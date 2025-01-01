# tests/test_queue_manager.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.queue_manager import QueueManager
from src.database.connection import get_db
from src.utils.file_manager import FileManager
from src.csv_processor.processor import CSVProcessor
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

def test_queue_manager():
    try:
        file_manager = FileManager()
        processor = CSVProcessor(file_manager)
        db = next(get_db())
        queue_manager = QueueManager(db)

        url = os.getenv('CSV_URL')
        auth = (os.getenv('CSV_USERNAME'), os.getenv('CSV_PASSWORD'))

        # Descargar y procesar nuevo CSV
        if processor.download_and_process_file(url, auth):
            print("✅ CSV descargado y procesado")
            price_changes, stock_changes = processor.detect_changes(limit=100)
            
            if price_changes:
                print(f"\nCambios de precio detectados: {len(price_changes)}")
                if queue_manager.register_price_changes(price_changes):
                    print("✅ Cambios de precio registrados en BD")
            
            if stock_changes:
                print(f"\nCambios de stock detectados: {len(stock_changes)}")
                if queue_manager.register_stock_changes(stock_changes):
                    print("✅ Cambios de stock registrados en BD")

            file_manager.archive_current_file()
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    test_queue_manager()