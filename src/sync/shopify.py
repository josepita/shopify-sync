# src/sync/shopify.py
import logging
from typing import Dict, Tuple, Optional
import pandas as pd
import time
from datetime import datetime
import os
import sys
from sqlalchemy import text


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


logger = logging.getLogger(__name__)

class ShopifyCSVComparator:
    def __init__(self, shopify_api, queue_manager, email_sender=None):
        self.shopify = shopify_api
        self.queue_manager = queue_manager
        self.email_sender = email_sender

    def get_shopify_inventory(self) -> Dict[str, Dict]:
        """
        Obtiene el inventario actual de todas las variantes en Shopify
        """
        try:
            inventory = {}
            
            # Obtener mappings de variantes usando SQLAlchemy
            result = self.queue_manager.db.execute(text("""
                SELECT internal_sku, shopify_variant_id, shopify_product_id, inventory_item_id 
                FROM variant_mappings
            """))
            mappings = {row.shopify_variant_id: row.internal_sku for row in result}

            logger.info(f"Obteniendo datos de {len(mappings)} variantes de Shopify")

            # Query GraphQL para obtener todos los productos
            query = """
                query getInventory($cursor: String) {
                products(first: 250, after: $cursor) {
                    pageInfo {
                    hasNextPage
                    endCursor
                    }
                    edges {
                    node {
                        variants(first: 10) {
                        edges {
                            node {
                            id
                            price
                            inventoryItem {
                                id
                                inventoryLevels(first: 1) {
                                edges {
                                    node {
                                    quantities(names: "available") {
                                        quantity
                                        name
                                    }
                                    }
                                }
                                }
                            }
                            }
                        }
                        }
                    }
                    }
                }
                }
                """

            cursor = None
            while True:
                variables = {"cursor": cursor} if cursor else {}
                result = self.shopify._make_request(query, variables)
                
                products = result.get('products', {})
                
                # Procesar los productos de esta página
                for product_edge in products.get('edges', []):
                    for variant_edge in product_edge['node']['variants']['edges']:
                        variant = variant_edge['node']
                        variant_id = variant['id'].split('/')[-1]
                        
                        # Si tenemos el mapping para esta variante
                        if variant_id in mappings:
                            inventory[mappings[variant_id]] = {
                                'price': float(variant['price']),
                                'stock': (variant['inventoryItem']['inventoryLevels']['edges'][0]['node']['quantities'][0]['quantity']
                                    if variant['inventoryItem'].get('inventoryLevels', {}).get('edges') and 
                                        variant['inventoryItem']['inventoryLevels']['edges'][0]['node'].get('quantities')
                                    else 0)
                            }

                # Verificar si hay más páginas
                page_info = products.get('pageInfo', {})
                if not page_info.get('hasNextPage'):
                    break
                
                cursor = page_info.get('endCursor')
                logger.info(f"Procesadas {len(inventory)} variantes, obteniendo siguiente página...")

            logger.info(f"Datos de Shopify obtenidos: {len(inventory)} variantes procesadas")
            return inventory

        except Exception as e:
            logger.error(f"Error obteniendo inventario de Shopify: {str(e)}")
            return {}

    def compare_with_csv(self, csv_path: str) -> Tuple[Dict, Dict]:
        """
        Compara el inventario de Shopify con el CSV actual
        Returns:
            Tuple[price_changes, stock_changes]
        """
        try:
            # Leer CSV
            df = pd.read_csv(csv_path)
            df['PRECIO'] = pd.to_numeric(df['PRECIO'], errors='coerce')
            df['STOCK'] = pd.to_numeric(df['STOCK'], errors='coerce')

            # Obtener datos de Shopify
            shopify_data = self.get_shopify_inventory()
            
            price_changes = {}
            stock_changes = {}

            # Comparar cada producto
            for _, row in df.iterrows():
                ref = row['REFERENCIA']
                if ref not in shopify_data:
                    #logger.warning(f"Referencia {ref} no encontrada en Shopify")
                    continue

                shopify_item = shopify_data[ref]
                
                # Comparar precio
                if abs(row['PRECIO'] - shopify_item['price']) > 0.01:  # Tolerancia para decimales
                    price_changes[ref] = {
                        'old_price': shopify_item['price'],
                        'new_price': float(row['PRECIO']),
                        'descripcion': row['DESCRIPCION']
                    }
                
                # Comparar stock
                if row['STOCK'] != shopify_item['stock']:
                    stock_changes[ref] = {
                        'old_stock': shopify_item['stock'],
                        'new_stock': int(row['STOCK']),
                        'descripcion': row['DESCRIPCION']
                    }

            return price_changes, stock_changes

        except Exception as e:
            logger.error(f"Error comparando datos: {str(e)}")
            return {}, {}

    def sync(self, csv_path: str) -> bool:
        """
        Proceso completo de sincronización usando Shopify como fuente de verdad
        """
        try:
            start_time = datetime.now()
            logger.info("Iniciando sincronización con Shopify")

            # Detectar cambios
            price_changes, stock_changes = self.compare_with_csv(csv_path)
            
            # Registrar cambios en BD
            if price_changes:
                self.queue_manager.register_price_changes(price_changes)
            if stock_changes:
                self.queue_manager.register_stock_changes(stock_changes)

            # Enviar resumen por email si hay configurado email_sender
            if self.email_sender:
                elapsed_time = datetime.now() - start_time
                summary_html = f"""
                <h2>Resumen Sincronización con Shopify</h2>
                <p>Fecha: {start_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Duración: {elapsed_time.total_seconds()/60:.1f} minutos</p>
                <h3>Cambios detectados:</h3>
                <ul>
                    <li>Cambios de precio: {len(price_changes)}</li>
                    <li>Cambios de stock: {len(stock_changes)}</li>
                </ul>
                """
                
                self.email_sender.send_email(
                    subject=f"Sincronización Shopify {start_time.strftime('%Y-%m-%d')}",
                    recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
                    html_content=summary_html
                )

            logger.info("Sincronización con Shopify completada")
            return True

        except Exception as e:
            logger.error(f"Error en sincronización con Shopify: {str(e)}")
            if self.email_sender:
                self.email_sender.send_email(
                    subject=f"❌ ERROR Sincronización Shopify {datetime.now().strftime('%Y-%m-%d')}",
                    recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
                    html_content=f"<h2>Error en sincronización</h2><p>{str(e)}</p>"
                )
            return False