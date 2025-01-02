# src/sync/catalog.py
import sys
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database.connection import get_db
from src.utils.file_manager import FileManager
from src.csv_processor.processor import CSVProcessor 
from src.database.queue_manager import QueueManager
from src.utils.email import EmailSender

logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
   handlers=[
       logging.FileHandler(f'logs/sync_catalog_{datetime.now().strftime("%Y%m%d")}.log'),
       logging.StreamHandler()
   ]
)
logger = logging.getLogger(__name__)

def sync_catalog(force: bool = False):
    """
    Sincroniza el catálogo con Shopify
    Args:
        force: Si es True, fuerza la actualización de todos los productos sin comparar
    """
    try:
        start_time = datetime.now()
        logger.info("Iniciando sincronización de catálogo" + (" (modo forzado)" if force else ""))

        file_manager = FileManager()
        processor = CSVProcessor(file_manager)
        db = next(get_db())
        queue_manager = QueueManager(db)
        email_sender = EmailSender()

        url = os.getenv('CSV_URL')
        auth = (os.getenv('CSV_USERNAME'), os.getenv('CSV_PASSWORD'))
        
        if not processor.download_and_process_file(url, auth):
            raise Exception("Error descargando/procesando catálogo")

        valid, message, df, stats = processor.validate_csv()
        if not valid:
            raise Exception(f"Error validando CSV: {message}")

        if force:
            # Modo forzado: procesar todos los productos
            total_processed = 0
            for _, row in df.iterrows():
                ref = row['REFERENCIA']
                queue_manager.register_price_changes({
                    ref: {
                        'new_price': float(row['PRECIO']),
                        'descripcion': row['DESCRIPCION']
                    }
                })
                queue_manager.register_stock_changes({
                    ref: {
                        'new_stock': int(row['STOCK']),
                        'descripcion': row['DESCRIPCION']
                    }
                })
                total_processed += 1
        else:
            # Modo normal: detectar y procesar cambios
            price_changes, stock_changes = processor.detect_changes()
            discontinued_products = processor.detect_discontinued_products()

            if price_changes:
                queue_manager.register_price_changes(price_changes)
            if stock_changes:
                queue_manager.register_stock_changes(stock_changes)

        file_manager.archive_current_file()

        # Preparar resumen según modo
        elapsed_time = datetime.now() - start_time
        if force:
            summary_html = f"""
            <h2>Resumen Sincronización Forzada de Catálogo</h2>
            <p>Fecha: {start_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Duración: {elapsed_time.total_seconds()/60:.1f} minutos</p>
            <h3>Resultados:</h3>
            <ul>
                <li>Total productos: {stats['total_products']}</li>
                <li>Productos procesados: {total_processed}</li>
                <li>Productos con precio 0: {stats['zero_prices']['count']} ({stats['zero_prices']['percent']:.1f}%)</li>
                <li>Productos con stock 0: {stats['zero_stock']['count']} ({stats['zero_stock']['percent']:.1f}%)</li>
            </ul>
            """
            subject = f"⚠️ Sincronización Forzada Catálogo {start_time.strftime('%Y-%m-%d')}"
        else:
            summary_html = f"""
            <h2>Resumen Sincronización Catálogo</h2>
            <p>Fecha: {start_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Duración: {elapsed_time.total_seconds()/60:.1f} minutos</p>
            <h3>Estadísticas:</h3>
            <ul>
                <li>Total productos: {stats['total_products']}</li>
                <li>Productos precio 0: {stats['zero_prices']['count']} ({stats['zero_prices']['percent']:.1f}%)</li>
                <li>Productos stock 0: {stats['zero_stock']['count']} ({stats['zero_stock']['percent']:.1f}%)</li>
                <li>Cambios de precio: {len(price_changes)}</li>
                <li>Cambios de stock: {len(stock_changes)}</li>
                <li>Productos descatalogados: {len(discontinued_products) if discontinued_products else 0}</li>
            </ul>
            """
            subject = f"Sincronización Catálogo {start_time.strftime('%Y-%m-%d')}"

        email_sender.send_email(
            subject=subject,
            recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
            html_content=summary_html
        )

        logger.info("Sincronización completada exitosamente")
        return True

    except Exception as e:
        logger.error(f"Error en sincronización: {str(e)}")
        email_sender.send_email(
            subject=f"❌ ERROR Sincronización Catálogo {datetime.now().strftime('%Y-%m-%d')}",
            recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
            html_content=f"<h2>Error en sincronización</h2><p>{str(e)}</p>"
        )
        return False

if __name__ == "__main__":
    load_dotenv()
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='Forzar sincronización completa')
    args = parser.parse_args()
    sync_catalog(args.force)