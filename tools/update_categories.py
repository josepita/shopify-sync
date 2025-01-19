#!/usr/bin/env python3
import os
import sys
import logging
import argparse
from typing import Dict, Optional
import pandas as pd
from dotenv import load_dotenv

# SCRIPT PARA ACTUALIZAR CATEGORÍAS DE PRODUCTOS EN SHOPIFY
# LEE EL CSV QUE SE LE INDIQUE, BUSCA CADA REFERENCIA EN LA BD Y ACTUALIZA LA CATEGORÍA EN SHOPIFY

# Añadir el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from src.shopify.api import ShopifyAPI
from sqlalchemy import text

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Mapeo de tipos a categorías de Shopify
TIPO_TO_CATEGORY = {
    'PENDIENTES': 'gid://shopify/TaxonomyCategory/aa-6-6',
    'FORNITURA': 'gid://shopify/TaxonomyCategory/aa-6',
    'COLGANTE': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'ALFILER': 'gid://shopify/TaxonomyCategory/aa-6',
    'CADENA': 'gid://shopify/TaxonomyCategory/aa-6-8',
    'GARGANTILLA': 'gid://shopify/TaxonomyCategory/aa-6-8',
    'PULSERA': 'gid://shopify/TaxonomyCategory/aa-6-3',
    'SOLITARIO': 'gid://shopify/TaxonomyCategory/aa-6-9',
    'CRISTO': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'MEDALLA': 'gid://shopify/TaxonomyCategory/aa-6-6',
    'ESCLAVA': 'gid://shopify/TaxonomyCategory/aa-6-3',
    'ALIANZA': 'gid://shopify/TaxonomyCategory/aa-6-9',
    'COLLAR': 'gid://shopify/TaxonomyCategory/aa-6-8',
    'AROS': 'gid://shopify/TaxonomyCategory/aa-6-6',
    'SELLO': 'gid://shopify/TaxonomyCategory/aa-6-9',
    'CRUZ': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'SORTIJA': 'gid://shopify/TaxonomyCategory/aa-6-9',
    'ESCAPULARIO': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'CORDON': 'gid://shopify/TaxonomyCategory/aa-6-8',
    'PLACA': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'PERGAMINO': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'TRESILLO': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'BROCHE': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'PIERCING': 'gid://shopify/TaxonomyCategory/aa-6-2',
    'ROSARIO': 'gid://shopify/TaxonomyCategory/aa-6-8',
    'INICIAL': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'DISCO': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'HOROSCOPO': 'gid://shopify/TaxonomyCategory/aa-6-5',
    'GEMELOS': 'gid://shopify/TaxonomyCategory/aa-2',
    'PISACORBATA': 'gid://shopify/TaxonomyCategory/aa-2'
}

class CategoryUpdater:
    def __init__(self, shopify_api: ShopifyAPI, csv_path: str):
        self.shopify = shopify_api
        self.db = next(get_db())
        self.csv_path = csv_path

    def get_product_id_for_sku(self, sku: str) -> Optional[str]:
        """
        Busca el product_id en la BD para un SKU específico
        """
        try:
            result = self.db.execute(text("""
                SELECT shopify_product_id 
                FROM variant_mappings 
                WHERE internal_sku = :sku 
                AND shopify_product_id IS NOT NULL
                AND internal_sku NOT LIKE '%%/%%'
                LIMIT 1
            """), {"sku": sku}).fetchone()
            
            print(f"Buscando SKU {sku} -> {'Encontrado' if result else 'No encontrado'}")
            if result:
                print(f"Product ID: {result[0]}")
                
            return result[0] if result else None
            
        except Exception as e:
            print(f"Error buscando SKU {sku}: {str(e)}")
            return None

    def process_updates(self):
        """
        Procesa las actualizaciones de categoría
        """
        try:
            # Cargar CSV
            df = pd.read_csv(self.csv_path)
            
            # Asegurar que REFERENCIA es string y añadir ceros a la izquierda si es necesario
            df['REFERENCIA'] = df['REFERENCIA'].fillna('').astype(str).apply(lambda x: x.zfill(8))
            
            # Filtrar variantes
            df = df[~df['REFERENCIA'].str.contains('/', na=False)]
            df = df[df['REFERENCIA'] != '']
            
            logger.info(f"Cargados {len(df)} productos del CSV")
            
            print("\nProductos en CSV:")
            for _, row in df.iterrows():
                print(f"REFERENCIA: {row['REFERENCIA']}, TIPO: {row['TIPO']}")
            print("")
            
            # Contadores para estadísticas
            actualizados = 0
            no_encontrados = 0
            errores = 0
            
            # Procesar cada producto
            for _, row in df.iterrows():
                sku = row['REFERENCIA']
                tipo = row['TIPO'].strip().upper()
                
                print(f"\nProcesando SKU: {sku} (Tipo: {tipo})")
                
                # Buscar producto en BD
                product_id = self.get_product_id_for_sku(sku)
                if not product_id:
                    print(f"SKU {sku} no encontrado en BD")
                    no_encontrados += 1
                    continue
                
                # Obtener categoría de Shopify
                category_id = TIPO_TO_CATEGORY.get(tipo)
                if not category_id:
                    print(f"Tipo no reconocido en mapeo: '{tipo}'")
                    errores += 1
                    continue
                
                print(f"Intentando actualizar SKU {sku} con categoría {category_id}")
                
                # Actualizar categoría
                if self.shopify.update_product_category(product_id, category_id):
                    actualizados += 1
                    logger.info(f"✓ Actualizado: {sku} -> {tipo}")
                else:
                    errores += 1

            # Mostrar resumen final
            logger.info("\n" + "="*50)
            logger.info("RESUMEN DE ACTUALIZACIÓN")
            logger.info("="*50)
            logger.info(f"Total productos: {len(df)}")
            logger.info(f"No encontrados en BD: {no_encontrados}")
            logger.info(f"Actualizaciones exitosas: {actualizados}")
            if errores > 0:
                logger.info(f"Errores: {errores}")
            logger.info("="*50)

        except Exception as e:
            logger.error(f"Error en el proceso de actualización: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Actualiza las categorías de productos en Shopify")
    parser.add_argument(
        '--csv', 
        required=True,
        help='Ruta al archivo CSV con los datos de productos'
    )
    args = parser.parse_args()

    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    # Silenciar logs de bajo nivel
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

    # Cargar variables de entorno
    load_dotenv()

    # Inicializar API de Shopify
    shopify = ShopifyAPI(
        shop_url=os.getenv('SHOPIFY_SHOP_URL'),
        access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
    )

    # Crear y ejecutar el actualizador
    updater = CategoryUpdater(shopify, args.csv)
    updater.process_updates()

if __name__ == "__main__":
    main()