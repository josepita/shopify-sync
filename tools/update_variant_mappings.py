# src/tools/update_variant_mappings.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from src.shopify.api import ShopifyAPI
from sqlalchemy import text
import logging
from dotenv import load_dotenv
import time
from typing import Optional

def get_next_batch(db, batch_size: int) -> list:
   query = text("""
       SELECT pm.* FROM product_mappings pm
       LEFT JOIN variant_mappings vm ON pm.internal_reference = vm.parent_reference
       WHERE vm.id IS NULL
       LIMIT :limit
   """)
   return db.execute(query, {'limit': batch_size}).fetchall()

def process_product(shopify: ShopifyAPI, db, product, retries: int = 3) -> bool:
   for attempt in range(retries):
       try:
           shopify_product = shopify.get_product(product.shopify_product_id)
           if not shopify_product or not shopify_product.get('variants', {}).get('edges'):
               return False

           variant = shopify_product['variants']['edges'][0]['node']
           inventory_item = variant.get('inventoryItem', {})

           db.execute(text("""
               INSERT INTO variant_mappings 
               (internal_sku, shopify_variant_id, shopify_product_id, parent_reference, price, inventory_item_id)
               VALUES (:internal_sku, :shopify_variant_id, :shopify_product_id, :parent_reference, :price, :inventory_item_id)
           """), {
               'internal_sku': product.internal_reference,
               'shopify_variant_id': variant['id'].split('/')[-1],
               'shopify_product_id': product.shopify_product_id,
               'parent_reference': product.internal_reference,
               'price': float(variant['price']),
               'inventory_item_id': inventory_item['id'].split('/')[-1] if inventory_item else None
           })
           
           return True
           
       except Exception as e:
           if attempt == retries - 1:
               logging.error(f"Error procesando {product.internal_reference} después de {retries} intentos")
               return False
           time.sleep(5 * (attempt + 1))  # Backoff exponencial

def update_variant_mappings(batch_size: int = 10):
   db = next(get_db())
   shopify = ShopifyAPI(
       shop_url=os.getenv('SHOPIFY_SHOP_URL'),
       access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
   )

   try:
       # Obtener total de registros pendientes
       total_pending = db.execute(text("""
           SELECT COUNT(*) FROM product_mappings pm 
           LEFT JOIN variant_mappings vm ON pm.internal_reference = vm.parent_reference
           WHERE vm.id IS NULL
       """)).scalar()

       if total_pending == 0:
           logging.info("No hay registros pendientes")
           return

       processed = 0
       start_time = time.time()

       while True:
           batch = get_next_batch(db, batch_size)
           if not batch:
               break

           batch_start = time.time()
           for product in batch:
               if process_product(shopify, db, product):
                   logging.info(f"✓ {product.internal_reference}")
               else:
                   logging.error(f"✗ {product.internal_reference}")
               processed += 1

           db.commit()
           
           # Calcular progreso y estimaciones
           batch_time = time.time() - batch_start
           elapsed = time.time() - start_time
           progress = (processed / total_pending) * 100
           remaining = (total_pending - processed)
           eta = (remaining * elapsed) / processed if processed > 0 else 0

           logging.info(
               f"Progreso: {progress:.1f}% ({processed}/{total_pending}) "
               f"- ETA: {eta/60:.1f} minutos"
           )

           time.sleep(1)  # Rate limit

   except Exception as e:
       logging.error(f"Error en el proceso: {str(e)}")
       db.rollback()

if __name__ == "__main__":
   update_variant_mappings(batch_size=10) 