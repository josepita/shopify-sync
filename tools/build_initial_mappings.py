#!/usr/bin/env python3
import os
import sys
import logging
from typing import Optional
import pandas as pd

from dotenv import load_dotenv

# Add repo root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from src.shopify.api import ShopifyAPI
from src.utils.file_manager import FileManager
from src.csv_processor.processor import CSVProcessor
from sqlalchemy import text


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def ensure_current_csv() -> Optional[str]:
    """Garantiza que exista data/current.csv. Si no, descarga desde CSV_URL."""
    fm = FileManager()
    if os.path.exists(fm.current_file):
        logger.info(f"Usando CSV existente: {fm.current_file}")
        return fm.current_file

    logger.info("No existe current.csv, intentando descargar del proveedor...")
    url = os.getenv('CSV_URL')
    auth = (os.getenv('CSV_USERNAME'), os.getenv('CSV_PASSWORD')) if os.getenv('CSV_USERNAME') else None
    processor = CSVProcessor(fm)
    ok = processor.download_and_process_file(url, auth)
    return fm.current_file if ok else None


def upsert_product_mapping(db, internal_reference: str, shopify_product_id: str, title: str = None):
    sql = text("""
        INSERT INTO product_mappings (internal_reference, shopify_product_id, title)
        VALUES (:internal_reference, :shopify_product_id, :title)
        ON DUPLICATE KEY UPDATE
          shopify_product_id = VALUES(shopify_product_id),
          title = VALUES(title),
          last_updated_at = CURRENT_TIMESTAMP
    """)
    db.execute(sql, {
        'internal_reference': internal_reference,
        'shopify_product_id': shopify_product_id,
        'title': title
    })


def upsert_variant_mapping(db, internal_sku: str, variant_id: str, product_id: str,
                           parent_reference: str, price: float = None, inventory_item_id: str = None):
    sql = text("""
        INSERT INTO variant_mappings (internal_sku, shopify_variant_id, shopify_product_id, parent_reference, price, inventory_item_id)
        VALUES (:internal_sku, :shopify_variant_id, :shopify_product_id, :parent_reference, :price, :inventory_item_id)
        ON DUPLICATE KEY UPDATE
          shopify_variant_id = VALUES(shopify_variant_id),
          shopify_product_id = VALUES(shopify_product_id),
          parent_reference = VALUES(parent_reference),
          price = VALUES(price),
          inventory_item_id = VALUES(inventory_item_id),
          last_updated_at = CURRENT_TIMESTAMP
    """)
    db.execute(sql, {
        'internal_sku': internal_sku,
        'shopify_variant_id': variant_id,
        'shopify_product_id': product_id,
        'parent_reference': parent_reference,
        'price': price,
        'inventory_item_id': inventory_item_id
    })


def build_initial_mappings():
    load_dotenv()
    db = next(get_db())
    shopify = ShopifyAPI(
        shop_url=os.getenv('SHOPIFY_SHOP_URL'),
        access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
    )

    csv_path = ensure_current_csv()
    if not csv_path or not os.path.exists(csv_path):
        logger.error("No se pudo obtener el CSV actual. Revisa CSV_URL y credenciales en .env")
        return 1

    df = pd.read_csv(csv_path)
    if 'REFERENCIA' not in df.columns:
        logger.error("El CSV no contiene la columna REFERENCIA")
        return 1

    # Normalizar columnas presentes
    df['REFERENCIA'] = df['REFERENCIA'].astype(str).str.strip()
    if 'PRECIO' in df.columns:
        df['PRECIO'] = pd.to_numeric(df['PRECIO'], errors='coerce')

    total = len(df)
    processed = 0
    found = 0
    not_found = 0

    logger.info(f"Iniciando mapeado inicial para {total} referencias del CSV")

    try:
        for _, row in df.iterrows():
            sku = str(row['REFERENCIA']).strip()
            if not sku:
                continue
            parent_reference = sku.split('/')[0]

            info = shopify.get_variant_info_by_sku(sku)
            if info and info.get('variant_id') and info.get('product_id'):
                # Upsert product mapping por referencia base
                upsert_product_mapping(
                    db,
                    internal_reference=parent_reference,
                    shopify_product_id=info['product_id'],
                    title=info.get('product_title')
                )

                # Upsert variant mapping por SKU completo
                upsert_variant_mapping(
                    db,
                    internal_sku=sku,
                    variant_id=info['variant_id'],
                    product_id=info['product_id'],
                    parent_reference=parent_reference,
                    price=float(row['PRECIO']) if 'PRECIO' in row and pd.notna(row['PRECIO']) else None,
                    inventory_item_id=info.get('inventory_item_id')
                )
                found += 1
            else:
                not_found += 1

            processed += 1
            if processed % 200 == 0 or processed == total:
                db.commit()
                logger.info(
                    f"Progreso: {processed:,}/{total:,} | Encontrados: {found:,} | No encontrados: {not_found:,}"
                )

        db.commit()
        logger.info("Mapeado inicial completado.")
        logger.info(f"Encontrados: {found:,} – No encontrados: {not_found:,}")
        if not_found > 0:
            logger.warning("Algunas referencias no se encontraron por SKU en Shopify. Revisa SKUs o crea productos.")
        return 0

    except Exception as e:
        db.rollback()
        logger.error(f"Error durante el mapeado: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(build_initial_mappings())

