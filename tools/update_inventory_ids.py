# src/tools/update_inventory_ids.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from src.shopify.api import ShopifyAPI
from sqlalchemy import text
import logging
from dotenv import load_dotenv
import time
import argparse

logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def update_inventory_item_ids(batch_size: int = 10, limit: int = None):
   """
   Actualiza inventory_item_id en variant_mappings usando el SKU o referencia padre (parent_reference)
   para los productos que son la primera variante del producto 
   Parte de la tabla variant_mappings para seleccionar todos los que tengan inventory_item_id NULL
   Luego va buscando los inventory_item_id en Shopify usando el SKU o referencia padre
   Busca el inventory_item_id a partir del SKU en Shopify y lo almacena en la base de datos local
   
   Args:
       batch_size: Tamaño del lote para procesamiento
       limit: Número máximo de registros a procesar (None para todos)
   """
   db = next(get_db())
   shopify = ShopifyAPI(
       shop_url=os.getenv('SHOPIFY_SHOP_URL'),
       access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
   )

   try:
       query = """
           SELECT 
               vm.id, 
               vm.internal_sku, 
               vm.parent_reference,
               vm.shopify_variant_id 
           FROM variant_mappings vm
           WHERE vm.inventory_item_id IS NULL
       """
       if limit:
           query += f" LIMIT {limit}"
           
       pending = db.execute(text(query)).fetchall()

       total = len(pending)
       if not total:
           logger.info("No hay variantes pendientes de actualizar")
           return

       logger.info(f"Encontradas {total} variantes sin inventory_item_id")
       processed = 0
       success = 0
       start_time = time.time()
       processing_times = []  # Lista para almacenar tiempos de procesamiento

       for variant in pending:
           variant_start_time = time.time()
           
           try:
               # Primero intentar con el SKU original
               inventory_data = shopify.get_inventory_item_by_sku(variant.internal_sku)
               sku_used = variant.internal_sku
               
               # Si no se encuentra, intentar con la referencia padre
               if not inventory_data:
                   logger.info(f"SKU {variant.internal_sku} no encontrado, probando con referencia padre {variant.parent_reference}")
                   inventory_data = shopify.get_inventory_item_by_sku(variant.parent_reference)
                   sku_used = variant.parent_reference

               if inventory_data:
                   db.execute(text("""
                       UPDATE variant_mappings 
                       SET inventory_item_id = :inventory_id 
                       WHERE id = :id
                   """), {
                       'inventory_id': inventory_data['id'].split('/')[-1],
                       'id': variant.id
                   })
                   db.commit()
                   success += 1
                   logger.info(f"✓ Actualizado {variant.internal_sku} usando SKU {sku_used}")
               else:
                   logger.error(f"✗ No encontrado inventory_item_id para SKU {variant.internal_sku} ni para padre {variant.parent_reference}")

               processed += 1
               
               # Calcular y almacenar tiempo de procesamiento de esta variante
               variant_time = time.time() - variant_start_time
               processing_times.append(variant_time)
               
               # Calcular tiempo medio de procesamiento
               avg_time = sum(processing_times) / len(processing_times)
               
               # Calcular estimaciones
               elapsed = time.time() - start_time
               remaining = total - processed
               eta = remaining * avg_time
               
               # Velocidad de procesamiento (variantes por minuto)
               speed = processed / (elapsed / 60)

               print(
                   f"\rProgreso: {processed}/{total} ({processed/total*100:.1f}%) - "
                   f"Actualizados: {success} - "
                   f"Errores: {processed - success} - "
                   f"Velocidad: {speed:.1f} var/min - "
                   f"Tiempo transcurrido: {elapsed/60:.1f}min - "
                   f"ETA: {eta/60:.1f}min", 
                   end=""
               )

               time.sleep(0.5)  # Rate limiting

           except Exception as e:
               logger.error(f"Error procesando {variant.internal_sku}: {str(e)}")
               db.rollback()

       elapsed = time.time() - start_time
       print(f"\n\nResumen final:")
       print(f"Tiempo total: {elapsed/60:.1f} minutos")
       print(f"Total procesados: {total}")
       print(f"Actualizados: {success}")
       print(f"Errores: {total - success}")
       print(f"Velocidad media: {(total/elapsed)*60:.1f} variantes/minuto")

   except Exception as e:
       logger.error(f"Error en el proceso: {str(e)}")

if __name__ == "__main__":
   load_dotenv()
   parser = argparse.ArgumentParser()
   parser.add_argument('--limit', type=int, help='Número máximo de registros a procesar')
   args = parser.parse_args()
   
   update_inventory_item_ids(limit=args.limit)