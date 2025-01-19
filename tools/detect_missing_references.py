#!/usr/bin/env python3
"""
Utilidad para encontrar SKUs (referencias) que existen en el CSV pero no en la base de datos.

Este script ayuda a identificar qué productos del catálogo (CSV) aún no han sido 
sincronizados con la tienda Shopify. Es útil para:
- Detectar productos nuevos que necesitan ser dados de alta
- Verificar la completitud de la sincronización
- Identificar posibles problemas en la importación de productos

El script:
1. Lee todas las referencias del CSV (incluyendo variantes con '/')
2. Comprueba cuáles no existen en la tabla variant_mappings de la BD
3. Genera un archivo CSV con los registros completos de las referencias faltantes
4. Muestra un resumen por pantalla agrupado por TIPO de producto

Uso:
    python tools/find_unsynchronized_skus.py --csv ruta/al/archivo.csv

Salida:
    - Genera un archivo CSV en data/missing_references_[timestamp].csv
    - Muestra un resumen por pantalla con estadísticas
"""

import os
import sys
import logging
import argparse
from typing import Dict, Set
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Añadir el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from sqlalchemy import text

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class ReferenceFinder:
    def __init__(self, csv_path: str):
        self.db = next(get_db())
        self.csv_path = csv_path

    def get_db_references(self) -> Set[str]:
        """
        Obtiene todas las referencias de la base de datos.
        Returns:
            Set[str]: Conjunto de referencias (internal_sku) encontradas en la BD
        """
        try:
            result = self.db.execute(text("""
                SELECT DISTINCT internal_sku 
                FROM variant_mappings 
                WHERE internal_sku IS NOT NULL
            """))
            
            return {row[0] for row in result}
            
        except Exception as e:
            logger.error(f"Error obteniendo referencias de BD: {str(e)}")
            raise

    def find_missing_references(self):
        """
        Encuentra referencias que están en el CSV pero no en la BD.
        Genera un archivo CSV con las referencias faltantes y muestra
        un resumen por pantalla agrupado por tipo de producto.
        """
        try:
            # Cargar CSV
            df = pd.read_csv(self.csv_path)
            df['REFERENCIA'] = df['REFERENCIA'].fillna('').astype(str)
            total_refs = len(df)
            
            # Obtener referencias de BD
            db_refs = self.get_db_references()
            logger.info(f"Referencias en BD: {len(db_refs)}")
            
            # Identificar faltantes
            df['exists_in_db'] = df['REFERENCIA'].isin(db_refs)
            missing_df = df[~df['exists_in_db']].copy()
            
            # Generar resumen por tipo
            missing_by_type = missing_df.groupby('TIPO').size().reset_index()
            missing_by_type.columns = ['TIPO', 'CANTIDAD']
            missing_by_type = missing_by_type.sort_values('CANTIDAD', ascending=False)
            
            # Guardar faltantes en CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'data/missing_references_{timestamp}.csv'
            missing_df.drop('exists_in_db', axis=1).to_csv(output_file, index=False)
            
            # Mostrar resumen
            total_missing = len(missing_df)
            logger.info("\n" + "="*50)
            logger.info("RESUMEN DE REFERENCIAS FALTANTES")
            logger.info("="*50)
            logger.info(f"Total referencias en CSV: {total_refs:,}")
            logger.info(f"Total referencias faltantes: {total_missing:,}")
            logger.info(f"Porcentaje faltante: {(total_missing/total_refs*100):.1f}%")
            logger.info("\nDesglose por tipo:")
            logger.info("-" * 40)
            for _, row in missing_by_type.iterrows():
                logger.info(f"{row['TIPO']}: {row['CANTIDAD']:,}")
            logger.info("-" * 40)
            logger.info(f"\nArchivo generado: {output_file}")
            logger.info("="*50)

        except Exception as e:
            logger.error(f"Error en el proceso: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(
        description="Encuentra referencias del CSV que no existen en la BD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  %(prog)s --csv data/catalogo.csv
  %(prog)s --csv data/csv_archive/catalogo_20240119.csv
        """
    )
    parser.add_argument(
        '--csv', 
        required=True,
        help='Ruta al archivo CSV con los datos de productos'
    )
    args = parser.parse_args()

    # Cargar variables de entorno
    load_dotenv()

    # Ejecutar búsqueda
    finder = ReferenceFinder(args.csv)
    finder.find_missing_references()

if __name__ == "__main__":
    main()