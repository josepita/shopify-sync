# tests/test_shopify_sync.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db
from src.database.queue_manager import QueueManager
from src.shopify.api import ShopifyAPI
from src.utils.email import EmailSender
from src.sync.shopify import ShopifyCSVComparator
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

def test_shopify_sync():
    try:
        # Inicializar componentes
        db = next(get_db())
        shopify = ShopifyAPI(
            shop_url=os.getenv('SHOPIFY_SHOP_URL'),
            access_token=os.getenv('SHOPIFY_ACCESS_TOKEN')
        )
        queue_manager = QueueManager(db)
        email_sender = EmailSender()

        # Crear comparador
        comparator = ShopifyCSVComparator(shopify, queue_manager, email_sender)

        # Ruta al CSV actual
        csv_path = 'data/current.csv'

        # Ejecutar sincronización
        if comparator.sync(csv_path):
            print("✅ Sincronización completada")
        else:
            print("❌ Error en sincronización")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    test_shopify_sync()