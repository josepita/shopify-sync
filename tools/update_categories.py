#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import time
from typing import Dict, Optional
import pandas as pd
from dotenv import load_dotenv

# Añadir el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from src.shopify.api import ShopifyAPI
from sqlalchemy import text

# Configurar logging
logging.basicConfig(level=logging.ERROR)
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
            
            return result[0] if result else None
            
        except Exception as e:
            return None

    def _format_time(self, seconds: float) -> str:
        """Formatea segundos en formato legible"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"

    def process_updates(self):
        """
        Procesa las actualizaciones de categoría
        """
        try:
            # Cargar CSV
            df = pd.read_csv(self.csv_path)
            df['REFERENCIA'] = df['REFERENCIA'].fillna('').astype(str).apply(lambda x: x.zfill(8))
            df['TIPO'] = df['TIPO'].fillna('').str.upper()
            
            # Filtrar variantes
            df = df[~df['REFERENCIA'].str.contains('/', na=False)]
            df = df[df['REFERENCIA'] != '']
            
            total_refs = len(df)
            
            # Lista para guardar las referencias no encontradas
            not_found_refs = []
            
            # Contadores para estadísticas
            actualizados = 0
            no_encontrados = 0
            errores = 0
            start_time = time.time()

            # Resumen inicial
            print("\n" + "="*50)
            print("ACTUALIZACIÓN DE CATEGORÍAS")
            print("="*50)
            print(f"Referencias a procesar: {total_refs:,}")
            print("="*50)
            
            # Procesar cada producto
            for index, row in df.iterrows():
                sku = row['REFERENCIA']
                tipo = row['TIPO'].strip().upper()
                
                # Buscar producto en BD
                product_id = self.get_product_id_for_sku(sku)
                if not product_id:
                    no_encontrados += 1
                    # Guardar la fila completa para las referencias no encontradas
                    not_found_refs.append(row)
                else:
                    # Obtener categoría
                    category_id = TIPO_TO_CATEGORY.get(tipo)
                    if not category_id:
                        errores += 1
                        continue
                    
                    # Actualizar categoría
                    if self.shopify.update_product_category(product_id, category_id):
                        actualizados += 1
                    else:
                        errores += 1

                # Mostrar progreso
                processed = index + 1
                elapsed = time.time() - start_time
                items_per_second = processed / elapsed if elapsed > 0 else 0
                remaining = total_refs - processed
                eta = remaining / items_per_second if items_per_second > 0 else 0

                print(
                    f"\rProcesando {processed:,}/{total_refs:,} ({processed/total_refs*100:.1f}%) - "
                    f"Actualizados: {actualizados:,} - "
                    f"No encontrados: {no_encontrados:,} - "
                    f"Errores: {errores:,} - "
                    f"Tiempo restante: {self._format_time(eta)}",
                    end="",
                    flush=True
                )

            # Guardar referencias no encontradas en CSV
            if not_found_refs:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                not_found_file = f'data/not_found_references_{timestamp}.csv'
                not_found_df = pd.DataFrame(not_found_refs)
                not_found_df.to_csv(not_found_file, index=False)

            total_time = time.time() - start_time

            # Resumen final
            print("\n\n" + "="*50)
            print("RESUMEN FINAL")
            print("="*50)
            print(f"Referencias procesadas: {total_refs:,}")
            print(f"Actualizados exitosamente: {actualizados:,}")
            print(f"No encontrados en BD: {no_encontrados:,}")
            if errores > 0:
                print(f"Errores: {errores:,}")
            if no_encontrados > 0:
                print(f"Referencias no encontradas guardadas en: {not_found_file}")
            print(f"Tiempo total: {self._format_time(total_time)}")
            print(f"Velocidad media: {total_refs/total_time:.1f} productos/s")
            print("="*50)

        except Exception as e:
            print(f"\nError en el proceso: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Actualiza las categorías de productos en Shopify")
    parser.add_argument(
        '--csv', 
        required=True,
        help='Ruta al archivo CSV con los datos de productos'
    )
    args = parser.parse_args()

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