# src/sync/queue_processor.py
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict
import os
import sys
import argparse
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database.connection import get_db
from src.shopify.api import ShopifyAPI
from src.utils.email import EmailSender
from sqlalchemy import text

logger = logging.getLogger(__name__)

class QueueProcessor:
    def __init__(self, shopify_api: ShopifyAPI, email_sender: EmailSender = None, 
                 batch_size: int = 100, max_retries: int = 3):
        self.shopify = shopify_api
        self.email_sender = email_sender
        self.db = next(get_db())
        self.batch_size = batch_size
        self.max_retries = max_retries

    def get_pending_price_updates(self) -> List[Dict]:
        """Obtiene un lote de actualizaciones de precio pendientes o con error"""
        result = self.db.execute(text("""
            SELECT 
                pq.id as queue_id,
                pq.variant_mapping_id,
                pq.new_price,
                vm.shopify_product_id,
                vm.shopify_variant_id,
                pq.status
            FROM price_updates_queue pq
            JOIN variant_mappings vm ON pq.variant_mapping_id = vm.id
            WHERE pq.status IN ('pending', 'error')
            ORDER BY pq.created_at
            LIMIT :limit
        """), {'limit': self.batch_size})
        
        return [
            {
                'queue_id': row[0],
                'variant_mapping_id': row[1],
                'new_price': row[2],
                'shopify_product_id': row[3],
                'shopify_variant_id': row[4],
                'status': row[5]
            } 
            for row in result
        ]

    def get_pending_stock_updates(self) -> List[Dict]:
        try:
            logger.info("Consultando actualizaciones de stock pendientes...")
            
            # Primero verificar integridad
            integrity_check = self.db.execute(text("""
                SELECT sq.id, sq.variant_mapping_id
                FROM stock_updates_queue sq
                LEFT JOIN variant_mappings vm ON sq.variant_mapping_id = vm.id
                WHERE vm.id IS NULL AND sq.status IN ('pending', 'error')
            """))
            
            orphaned = list(integrity_check)
            if orphaned:
                logger.error(f"Encontradas {len(orphaned)} actualizaciones sin variante asociada: {orphaned}")

            # Verificar variantes sin inventory_item_id
            invalid_variants = self.db.execute(text("""
                SELECT vm.id, vm.internal_sku
                FROM variant_mappings vm
                WHERE vm.inventory_item_id IS NULL
            """))
            
            invalid = list(invalid_variants)
            if invalid:
                logger.error(f"Variantes sin inventory_item_id: {invalid}")

            # Query original con más información
            result = self.db.execute(text("""
                SELECT 
                    sq.id as queue_id,
                    sq.variant_mapping_id,
                    sq.new_stock,
                    vm.inventory_item_id,
                    vm.internal_sku,
                    sq.status,
                    sq.created_at
                FROM stock_updates_queue sq
                JOIN variant_mappings vm ON sq.variant_mapping_id = vm.id
                WHERE sq.status IN ('pending', 'error')
                ORDER BY sq.created_at
                LIMIT :limit
            """), {'limit': self.batch_size})

            updates = []
            for row in result:
                logger.info(f"Stock update - Queue ID: {row[0]}, SKU: {row[4]}, Stock: {row[2]}, Inventory ID: {row[3]}")
                updates.append({ 
                    'queue_id': row[0],
                    'variant_mapping_id': row[1],
                    'new_stock': row[2],
                    'inventory_item_id': row[3],
                    'internal_sku': row[4],
                    'status': row[5],
                    'created_at': row[6]
                })

            return updates
                
        except Exception as e:
            logger.error(f"Error en get_pending_stock_updates: {e}")
            return []


    def process_price_updates(self):
            """Procesa la cola de actualizaciones de precio"""
            try:
                pending_updates = self.get_pending_price_updates()
                logger.info(f"Encontrados {len(pending_updates)} precios para procesar")
                
                if not pending_updates:
                    return

                # Obtener margen de .env o usar valor por defecto
                margin = float(os.getenv('PRICE_MARGIN', 2.5))
                
                # Agrupar por producto para usar bulk update
                products_updates = {}
                for update in pending_updates:
                    product_id = update['shopify_product_id']
                    if product_id not in products_updates:
                        products_updates[product_id] = []
                    products_updates[product_id].append({
                        'product_id': product_id,
                        'variant_id': update['shopify_variant_id'],
                        'cost': update['new_price'],  # Ahora new_price representa el coste
                        'queue_id': update['queue_id']
                    })
                    
                logger.info(f"Procesando actualizaciones para {len(products_updates)} productos con margen {margin}")

                # Procesar cada grupo
                for product_id, variants in products_updates.items():
                    try:
                        results = self.shopify.bulk_price_update(variants, margin=margin)
                        
                        # Actualizar estado en la cola
                        for variant in variants:
                            status = 'completed' if results.get(str(variant['variant_id'])) else 'error'
                            logger.info(f"Actualizando estado de variante {variant['variant_id']} a {status}")
                            
                            self.db.execute(text("""
                                UPDATE price_updates_queue
                                SET status = :status, 
                                    processed_at = CURRENT_TIMESTAMP
                                WHERE id = :queue_id
                            """), {
                                'status': status, 
                                'queue_id': variant['queue_id']
                            })
                        
                        self.db.commit()
                        time.sleep(1)  # Rate limiting entre productos
                        
                    except Exception as e:
                        logger.error(f"Error procesando actualización de precios para producto {product_id}: {str(e)}")
                        self.db.rollback()

            except Exception as e:
                logger.error(f"Error en proceso de actualización de precios: {str(e)}")

    def process_stock_updates(self):
        """Procesa la cola de actualizaciones de stock"""
        try:
            pending_updates = self.get_pending_stock_updates()
            if not pending_updates:
                return

            # Como las actualizaciones de stock son individuales, procesamos con más cuidado
            for update in pending_updates:
                try:
                    queue_id = update['queue_id']  # Guardamos el queue_id antes del update
                    success = self.shopify.update_inventory_quantity(
                        inventory_item_id=update['inventory_item_id'],
                        location_id=os.getenv('SHOPIFY_LOCATION_ID'),
                        desired_quantity=update['new_stock']
                    )

                    # Actualizar estado usando queue_id
                    status = 'completed' if success else 'error'
                    self.db.execute(text("""
                        UPDATE stock_updates_queue
                        SET status = :status,
                            processed_at = CURRENT_TIMESTAMP
                        WHERE id = :queue_id
                    """), {'status': status, 'queue_id': queue_id})
                    
                    self.db.commit()
                    time.sleep(0.5)  # Rate limiting entre actualizaciones
                    
                except Exception as e:
                    logger.error(f"Error procesando stock para item {update.get('inventory_item_id')}: {str(e)}")
                    self.db.rollback()

        except Exception as e:
            logger.error(f"Error en proceso de actualización de stock: {str(e)}")

    def process_queues(self, process_type='all'):
        """
        Procesa las colas según el tipo especificado
        Args:
            process_type: 'all', 'prices', 'stock'
        """
        logger.setLevel(logging.WARNING)
        logging.getLogger('src.shopify.api').setLevel(logging.WARNING)
        
        def format_time(seconds):
            """Formatea segundos en formato legible"""
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"

        while True:
            try:
                start_time = time.time()
                stats = self.get_queue_stats()
                
                pending_price = stats['pending_price'] + stats['error_price']
                pending_stock = stats['pending_stock'] + stats['error_stock']
                
                if process_type == 'prices':
                    initial_total = pending_price
                elif process_type == 'stock':
                    initial_total = pending_stock
                else:
                    initial_total = pending_price + pending_stock
                    
                if initial_total == 0:
                    print(f"\nNo hay registros pendientes para {process_type}")
                    time.sleep(60)
                    continue

                # Mostrar resumen inicial
                print("\n" + "="*50)
                print("COLA DE ACTUALIZACIONES")
                print("="*50)
                print(f"Inicio: {datetime.now().strftime('%H:%M:%S')}")
                if process_type in ['all', 'prices']:
                    print(f"\nPRECIOS:")
                    print(f"- Pendientes: {stats['pending_price']:,}")
                    print(f"- Con error: {stats['error_price']:,}")
                if process_type in ['all', 'stock']:
                    print(f"\nSTOCK:")
                    print(f"- Pendientes: {stats['pending_stock']:,}")
                    print(f"- Con error: {stats['error_stock']:,}")
                print("="*50)

                processed = 0
                processing_times = []
                
                while processed < initial_total:
                    cycle_start = time.time()
                    prev_processed = processed

                    if process_type in ['all', 'prices']:
                        self.process_price_updates()
                    if process_type in ['all', 'stock']:
                        self.process_stock_updates()

                    current_stats = self.get_queue_stats()
                    
                    if process_type == 'prices':
                        current_total = current_stats['pending_price'] + current_stats['error_price']
                    elif process_type == 'stock':
                        current_total = current_stats['pending_stock'] + current_stats['error_stock']
                    else:
                        current_total = (current_stats['pending_price'] + current_stats['error_price'] + 
                                    current_stats['pending_stock'] + current_stats['error_stock'])

                    processed = initial_total - current_total
                    
                    # Actualizar estadísticas si hubo progreso
                    if processed > prev_processed:
                        elapsed = time.time() - start_time
                        items_per_second = processed / elapsed if elapsed > 0 else 0
                        eta = (initial_total - processed) / items_per_second if items_per_second > 0 else 0
                        
                        print(
                            f"\rProcesados: {processed:,}/{initial_total:,} ({processed/initial_total*100:.1f}%) - "
                            f"Velocidad: {items_per_second:.1f} items/s - "
                            f"Tiempo transcurrido: {format_time(elapsed)} - "
                            f"Tiempo restante: {format_time(eta)}", 
                            end=""
                        )

                    if current_total == 0:
                        break

                    time.sleep(1)  # Dar tiempo para actualizaciones de BD

                # Resumen final
                total_time = time.time() - start_time
                print("\n" + "="*50)
                print("RESUMEN FINAL")
                print("="*50)
                print(f"Inicio: {datetime.fromtimestamp(start_time).strftime('%H:%M:%S')}")
                print(f"Fin: {datetime.now().strftime('%H:%M:%S')}")
                print(f"Tiempo total: {format_time(total_time)}")
                print(f"Items procesados: {processed:,}")
                print(f"Velocidad media: {processed/total_time:.1f} items/s")
                print("="*50)
                
                if self.email_sender:
                    self.send_processing_summary(
                        initial_total - current_stats['pending_price'] - current_stats['error_price'],
                        initial_total - current_stats['pending_stock'] - current_stats['error_stock'],
                        current_stats
                    )

                # Esperar antes de siguiente ciclo
                time.sleep(30)

            except Exception as e:
                logger.error(f"Error: {str(e)}")
                time.sleep(30)

    def get_queue_stats(self) -> Dict:
        """Obtiene estadísticas de las colas"""
        try:
            stats = {
                'pending_price': 0,
                'error_price': 0,
                'completed_price': 0,
                'pending_stock': 0,
                'error_stock': 0,
                'completed_stock': 0
            }
            
            # Estadísticas de precio
            price_result = self.db.execute(text("""
                SELECT status, COUNT(*) as count
                FROM price_updates_queue
                GROUP BY status
            """))
            for row in price_result:
                status = row[0]
                if status == 'pending':
                    stats['pending_price'] = row[1]
                elif status == 'error':
                    stats['error_price'] = row[1]
                elif status == 'completed':
                    stats['completed_price'] = row[1]
            
            # Estadísticas de stock
            stock_result = self.db.execute(text("""
                SELECT status, COUNT(*) as count
                FROM stock_updates_queue
                GROUP BY status
            """))
            for row in stock_result:
                status = row[0]
                if status == 'pending':
                    stats['pending_stock'] = row[1]
                elif status == 'error':
                    stats['error_stock'] = row[1]
                elif status == 'completed':
                    stats['completed_stock'] = row[1]
            
            return stats
                
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {str(e)}")
            return {
                'pending_price': 0,
                'error_price': 0,
                'completed_price': 0,
                'pending_stock': 0,
                'error_stock': 0,
                'completed_stock': 0
            }

    def send_processing_summary(self, processed_price: int, processed_stock: int, stats: Dict):
        """Envía resumen del procesamiento por email"""
        if not self.email_sender:
            return

        try:
            html_content = f"""
            <h2>Resumen Procesamiento de Colas</h2>
            <p>Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h3>Procesados en este ciclo:</h3>
            <ul>
                <li>Precios: {processed_price}</li>
                <li>Stock: {processed_stock}</li>
            </ul>
            
            <h3>Estado actual de las colas:</h3>
            <h4>Precios:</h4>
            <ul>
                <li>Pendientes: {stats['pending_price']}</li>
                <li>Con error: {stats['error_price']}</li>
                <li>Completados: {stats['completed_price']}</li>
            </ul>
            <h4>Stock:</h4>
            <ul>
                <li>Pendientes: {stats['pending_stock']}</li>
                <li>Con error: {stats['error_stock']}</li>
                <li>Completados: {stats['completed_stock']}</li>
            </ul>
            """
            
            self.email_sender.send_email(
                subject=f"Procesamiento de Colas {datetime.now().strftime('%Y-%m-%d')}",
                recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error enviando resumen: {str(e)}")

if __name__ == "__main__":
   load_dotenv()
   
   parser = argparse.ArgumentParser(description="Procesador de colas de Shopify")
   parser.add_argument(
       '--type', 
       choices=['all', 'prices', 'stock'],
       help='Tipo de procesamiento (por defecto: all):\n'
            'all: Procesa cambios de precio y stock\n'
            'prices: Solo procesa cambios de precio\n'
            'stock: Solo procesa cambios de stock'
   )
   args = parser.parse_args()

   process_type = args.type if args.type else 'all'
   
   shopify = ShopifyAPI(
       shop_url=os.getenv('SHOPIFY_SHOP_URL'),
       access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
   )
   email_sender = EmailSender()
   
   processor = QueueProcessor(shopify, email_sender)
   processor.process_queues(process_type)