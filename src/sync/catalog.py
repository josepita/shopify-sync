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

def sync_catalog():
   try:
       start_time = datetime.now()
       logger.info("Iniciando sincronización de catálogo")

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
           if "Diferencia de productos" in message:
               email_sender.send_email(
                   subject=f"⚠️ 🔍 ALERTA: Diferencia significativa en catálogo {start_time.strftime('%Y-%m-%d')}",
                   recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
                   html_content=f"""
                       <h2>Alerta: Diferencia en número de productos</h2>
                       <p>Diferencia: {stats['products_diff']['count']} productos ({stats['products_diff']['percent']:.1f}%)</p>
                       <p>Actual: {stats['total_products']}</p>
                       <p>Anterior: {stats['total_products'] - stats['products_diff']['count']}</p>
                   """
               )
               return False

           raise Exception(message)

       price_changes, stock_changes = processor.detect_changes()
       discontinued_products = processor.detect_discontinued_products()

       # Solo registrar cambios si no hay diferencia significativa
       if abs(stats['products_diff']['percent']) <= 10:
           if price_changes:
               queue_manager.register_price_changes(price_changes)
           if stock_changes:
               queue_manager.register_stock_changes(stock_changes)

       file_manager.archive_current_file()

       elapsed_time = datetime.now() - start_time
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
       
       email_sender.send_email(
           subject=f"Sincronización Catálogo {start_time.strftime('%Y-%m-%d')}",
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
   sync_catalog()