# tests/test_csv_processor.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.file_manager import FileManager
from src.csv_processor.processor import CSVProcessor
from src.utils.email import EmailSender
from dotenv import load_dotenv
import logging
import pandas as pd

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

def test_csv_processor():
    try:
        # Crear instancias
        file_manager = FileManager()
        processor = CSVProcessor(file_manager)
        email_sender = EmailSender()
        
        # Descargar y procesar archivo
        print("\n1. Descarga y procesamiento del archivo")
        print("-" * 50)
        
        url = os.getenv('CSV_URL')
        username = os.getenv('CSV_USERNAME')
        password = os.getenv('CSV_PASSWORD')
        
        if not url:
            print("❌ Error: URL no configurada en .env")
            return

        if processor.download_and_process_file(url, auth=(username, password)):
            print("✅ Archivo descargado y procesado correctamente")
            
            # Validar CSV
            print("\n2. Validación de datos")
            print("-" * 50)
            
            is_valid, message, df = processor.validate_csv()
            
            if df is not None:
                print(f"Total de productos procesados: {len(df)}")
                
                # Analizar productos con precio 0
                zero_prices = df[df['PRECIO'] == 0][['REFERENCIA', 'DESCRIPCION', 'PRECIO']]
                if not zero_prices.empty:
                    print("\nProductos con precio 0 detectados:")
                    print("-" * 50)
                    for _, row in zero_prices.iterrows():
                        print(f"REF: {row['REFERENCIA']} - {row['DESCRIPCION']}")
                    
                    # Enviar alerta por email
                    print("\nEnviando alerta por email...")
                    recipient = os.getenv('ALERT_EMAIL_RECIPIENT')
                    if not recipient:
                        print("❌ Error: ALERT_EMAIL_RECIPIENT no configurado en .env")
                    else:
                        if processor.send_price_alerts(zero_prices, email_sender):
                            print("✅ Alerta enviada correctamente")
                        else:
                            print("❌ Error enviando alerta")
                
                # Analizar stock
                zero_stock = len(df[df['STOCK'] == 0])
                zero_stock_percentage = (zero_stock / len(df)) * 100
                print(f"\nEstadísticas de stock:")
                print("-" * 50)
                print(f"Productos con stock 0: {zero_stock} ({zero_stock_percentage:.2f}%)")
                
                # Si tenemos archivo previo, detectar cambios
                if os.path.exists(file_manager.previous_file):
                    print("\n3. Detección de cambios")
                    print("-" * 50)
                    
                    price_changes, stock_changes = processor.detect_changes()
                    
                    if price_changes:
                        print(f"\nCambios de precio detectados: {len(price_changes)}")
                        print("Primeros 5 cambios de precio:")
                        for i, (ref, data) in enumerate(price_changes.items()):
                            if i >= 5: break
                            print(f"REF: {ref}")
                            print(f"  - Anterior: {data['old_price']:.2f}")
                            print(f"  - Nuevo: {data['new_price']:.2f}")
                    
                    if stock_changes:
                        print(f"\nCambios de stock detectados: {len(stock_changes)}")
                        print("Primeros 5 cambios de stock:")
                        for i, (ref, data) in enumerate(stock_changes.items()):
                            if i >= 5: break
                            print(f"REF: {ref}")
                            print(f"  - Anterior: {data['old_stock']}")
                            print(f"  - Nuevo: {data['new_stock']}")
                
                # Detectar productos descatalogados
                print("\n4. Detección de productos descatalogados")
                print("-" * 50)
                discontinued = processor.detect_discontinued_products()
                if discontinued:
                    print(f"Productos potencialmente descatalogados: {len(discontinued)}")
                    print("Primeros 5 productos:")
                    for i, (ref, data) in enumerate(discontinued.items()):
                        if i >= 5: break
                        print(f"REF: {ref}")
                        print(f"  - Días ausente: {data['dias_ausente']}")
                        print(f"  - Descripción: {data['descripcion']}")
                else:
                    print("No se detectaron productos descatalogados")
                
                if file_manager.archive_current_file():
                    print("\n✅ Archivo actual archivado correctamente")
                else:
                    print("\n❌ Error archivando archivo actual")
            else:
                print(f"❌ Error en la validación: {message}")
        else:
            print("❌ Error procesando archivo")
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {str(e)}")

if __name__ == "__main__":
    test_csv_processor()