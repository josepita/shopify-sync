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
    """
    Registra cambios de precio en el historial y cola de actualizaciones
    Args:
        changes: Diccionario con los cambios de precio {referencia: {new_price: float, descripcion: str}}
    Returns:
        bool: True si los cambios se registraron correctamente
    """
    try:
        for ref, data in changes.items():
            variant_id = self.get_variant_id(ref)
            if not variant_id:
                logger.warning(f"No se encontró variant_id para referencia {ref}")
                continue

            # Comprobar si ya existe un registro para hoy en el historial
            today = datetime.now().date()
            existing_history = self.db.execute(text("""
                SELECT id FROM price_history 
                WHERE reference = :reference 
                AND date = :date
            """), {
                'reference': ref,
                'date': today
            }).fetchone()

            if existing_history:
                # Actualizar el registro existente
                self.db.execute(text("""
                    UPDATE price_history 
                    SET price = :price
                    WHERE id = :id
                """), {
                    'price': data['new_price'],
                    'id': existing_history[0]
                })
                logger.info(f"Actualizado historial de precio para {ref} del día {today}")
            else:
                # Insertar nuevo registro en el historial
                self.db.execute(text("""
                    INSERT INTO price_history (reference, price, date) 
                    VALUES (:reference, :price, :date)
                """), {
                    'reference': ref,
                    'price': data['new_price'],
                    'date': today
                })
                logger.info(f"Creado nuevo registro en historial de precio para {ref}")

            # Gestionar cola de actualizaciones
            existing_queue = self.db.execute(text("""
                SELECT id FROM price_updates_queue 
                WHERE variant_mapping_id = :variant_id 
                AND status = 'pending'
            """), {'variant_id': variant_id}).fetchone()

            if existing_queue:
                # Actualizar precio en cola existente
                self.db.execute(text("""
                    UPDATE price_updates_queue 
                    SET new_price = :new_price,
                        created_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """), {
                    'new_price': data['new_price'],
                    'id': existing_queue[0]
                })
                logger.info(f"Actualizada cola de precio para {ref}")
            else:
                # Crear nueva entrada en la cola
                self.db.execute(text("""
                    INSERT INTO price_updates_queue 
                    (variant_mapping_id, new_price, status, created_at) 
                    VALUES (:variant_mapping_id, :new_price, :status, :created_at)
                """), {
                    'variant_mapping_id': variant_id,
                    'new_price': data['new_price'],
                    'status': 'pending',
                    'created_at': datetime.now()
                })
                logger.info(f"Creada nueva entrada en cola de precio para {ref}")

        self.db.commit()
        return True

    except Exception as e:
        logger.error(f"Error registrando cambios de precio: {str(e)}")
        self.db.rollback()
        return False

   def register_stock_changes(self, changes: Dict) -> bool:
    """
    Registra cambios de stock en el historial y cola de actualizaciones
    Args:
        changes: Diccionario con los cambios de stock {referencia: {new_stock: int, descripcion: str}}
    Returns:
        bool: True si los cambios se registraron correctamente
    """
    try:
        for ref, data in changes.items():
            variant_id = self.get_variant_id(ref)
            if not variant_id:
                logger.warning(f"No se encontró variant_id para referencia {ref}")
                continue

            # Comprobar si ya existe un registro para hoy en el historial
            today = datetime.now().date()
            existing_history = self.db.execute(text("""
                SELECT id FROM stock_history 
                WHERE reference = :reference 
                AND date = :date
            """), {
                'reference': ref,
                'date': today
            }).fetchone()

            if existing_history:
                # Actualizar el registro existente
                self.db.execute(text("""
                    UPDATE stock_history 
                    SET stock = :stock
                    WHERE id = :id
                """), {
                    'stock': data['new_stock'],
                    'id': existing_history[0]
                })
                logger.info(f"Actualizado historial de stock para {ref} del día {today}")
            else:
                # Insertar nuevo registro en el historial
                self.db.execute(text("""
                    INSERT INTO stock_history (reference, stock, date) 
                    VALUES (:reference, :stock, :date)
                """), {
                    'reference': ref,
                    'stock': data['new_stock'],
                    'date': today
                })
                logger.info(f"Creado nuevo registro en historial de stock para {ref}")

            # Gestionar cola de actualizaciones
            existing_queue = self.db.execute(text("""
                SELECT id FROM stock_updates_queue 
                WHERE variant_mapping_id = :variant_id 
                AND status = 'pending'
            """), {'variant_id': variant_id}).fetchone()

            if existing_queue:
                # Actualizar stock en cola existente
                self.db.execute(text("""
                    UPDATE stock_updates_queue 
                    SET new_stock = :new_stock,
                        created_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """), {
                    'new_stock': data['new_stock'],
                    'id': existing_queue[0]
                })
                logger.info(f"Actualizada cola de stock para {ref}")
            else:
                # Crear nueva entrada en la cola
                self.db.execute(text("""
                    INSERT INTO stock_updates_queue 
                    (variant_mapping_id, new_stock, status, created_at) 
                    VALUES (:variant_mapping_id, :new_stock, :status, :created_at)
                """), {
                    'variant_mapping_id': variant_id,
                    'new_stock': data['new_stock'],
                    'status': 'pending',
                    'created_at': datetime.now()
                })
                logger.info(f"Creada nueva entrada en cola de stock para {ref}")

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