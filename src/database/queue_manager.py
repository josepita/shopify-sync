# src/database/queue_manager.py
from sqlalchemy import text
from datetime import datetime
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR) 

class QueueManager:
   def __init__(self, db_session):
       self.db = db_session

   def register_price_changes(self, changes: Dict) -> bool:
    try:
        for ref, data in changes.items():
            variant_id = self.get_variant_id(ref)
            logger.info(f"Buscando variant_id para {ref}: {variant_id}")

            if not variant_id:
                logger.warning(f"No se encontró variant_id para referencia {ref}")
                continue

            # Log e inserción historial
            price_history = {
                'reference': ref,
                'price': data['new_price'],
                'date': datetime.now().date()
            }
            logger.info(f"Insertando en price_history: {price_history}")
            
            self.db.execute(text("""
                INSERT INTO price_history (reference, price, date) 
                VALUES (:reference, :price, :date)
            """), price_history)

            # Log e inserción cola
            price_update = {
                'variant_mapping_id': variant_id,
                'new_price': data['new_price'],
                'status': 'pending',
                'created_at': datetime.now()
            }
            logger.info(f"Insertando en price_updates_queue: {price_update}")
            
            self.db.execute(text("""
                INSERT INTO price_updates_queue 
                (variant_mapping_id, new_price, status, created_at) 
                VALUES (:variant_mapping_id, :new_price, :status, :created_at)
            """), price_update)

        self.db.commit()
        return True

    except Exception as e:
        logger.error(f"Error registrando cambios de precio: {str(e)}")
        self.db.rollback()
        return False

   def register_stock_changes(self, changes: Dict) -> bool:
    try:
        for ref, data in changes.items():
            variant_id = self.get_variant_id(ref)
            logger.info(f"Buscando variant_id para {ref}: {variant_id}")

            if not variant_id:
                logger.warning(f"No se encontró variant_id para referencia {ref}")
                continue

            # Log e inserción historial
            stock_history = {
                'reference': ref,
                'stock': data['new_stock'],
                'date': datetime.now().date()
            }
            logger.info(f"Insertando en stock_history: {stock_history}")
            
            self.db.execute(text("""
                INSERT INTO stock_history (reference, stock, date) 
                VALUES (:reference, :stock, :date)
            """), stock_history)

            # Log e inserción cola
            stock_update = {
                'variant_mapping_id': variant_id,
                'new_stock': data['new_stock'],
                'status': 'pending',
                'created_at': datetime.now()
            }
            logger.info(f"Insertando en stock_updates_queue: {stock_update}")
            
            self.db.execute(text("""
                INSERT INTO stock_updates_queue 
                (variant_mapping_id, new_stock, status, created_at) 
                VALUES (:variant_mapping_id, :new_stock, :status, :created_at)
            """), stock_update)

        self.db.commit()
        return True

    except Exception as e:
        logger.error(f"Error registrando cambios de stock: {str(e)}")
        self.db.rollback()
        return False

   def get_variant_id(self, reference: str) -> Optional[int]:
       result = self.db.execute(text("""
           SELECT id FROM variant_mappings 
           WHERE internal_sku = :reference
       """), {'reference': reference}).fetchone()
       
       return result[0] if result else None