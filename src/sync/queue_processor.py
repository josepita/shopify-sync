# src/sync/queue_processor.py
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict
import os
import sys
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
        """Obtiene un lote de actualizaciones de stock pendientes o con error"""
        result = self.db.execute(text("""
            SELECT 
                sq.id as queue_id,
                sq.variant_mapping_id,
                sq.new_stock,
                vm.inventory_item_id,
                sq.status 
            FROM stock_updates_queue sq
            JOIN variant_mappings vm ON sq.variant_mapping_id = vm.id
            WHERE sq.status IN ('pending', 'error')
            ORDER BY sq.created_at
            LIMIT :limit
        """), {'limit': self.batch_size})
        
        return [
            { 
                'queue_id': row[0],
                'variant_mapping_id': row[1],
                'new_stock': row[2],
                'inventory_item_id': row[3],
                'status': row[4]
            }
            for row in result
        ]


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

    def process_queues(self):
        """Procesa ambas colas continuamente"""
        while True:
            try:
                start_time = datetime.now()
                
                # Obtener estadísticas iniciales
                stats = self.get_queue_stats()
                total_pending = stats['pending_price'] + stats['error_price'] + stats['pending_stock'] + stats['error_stock']
                
                logger.info(f"Estado de las colas:")
                logger.info(f"Precios -> Pendientes: {stats['pending_price']}, Error: {stats['error_price']}")
                logger.info(f"Stock -> Pendientes: {stats['pending_stock']}, Error: {stats['error_stock']}")

                if total_pending > 0:
                    # Procesar colas
                    logger.info("Procesando cola de precios...")
                    self.process_price_updates()
                    
                    logger.info("Procesando cola de stock...")
                    self.process_stock_updates()

                    # Obtener estadísticas finales
                    end_stats = self.get_queue_stats()
                    processed_price = (stats['pending_price'] + stats['error_price']) - (end_stats['pending_price'] + end_stats['error_price'])
                    processed_stock = (stats['pending_stock'] + stats['error_stock']) - (end_stats['pending_stock'] + end_stats['error_stock'])

                    logger.info(f"Procesamiento completado:")
                    logger.info(f"Precios procesados: {processed_price}")
                    logger.info(f"Stock procesado: {processed_stock}")

                    # Enviar resumen si hay email configurado
                    if self.email_sender and (processed_price > 0 or processed_stock > 0):
                        self.send_processing_summary(processed_price, processed_stock, end_stats)
                else:
                    logger.info("No hay registros pendientes de procesar")
                    time.sleep(60)  # Esperar 1 minuto antes de siguiente check

            except Exception as e:
                logger.error(f"Error en proceso principal: {str(e)}")
                time.sleep(30)  # Esperar antes de reintentar

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
    
    # Inicializar componentes
    shopify = ShopifyAPI(
        shop_url=os.getenv('SHOPIFY_SHOP_URL'),
        access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
    )
    email_sender = EmailSender()
    
    # Crear y ejecutar procesador
    processor = QueueProcessor(shopify, email_sender)
    processor.process_queues()