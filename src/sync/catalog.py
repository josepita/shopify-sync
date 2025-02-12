# src/sync/catalog.py
import shutil
import sys
import os
from datetime import datetime
import logging
import argparse

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
      logging.FileHandler(f'/var/log/sync_catalog_{datetime.now().strftime("%Y%m%d")}.log'),
      logging.StreamHandler()
  ]
)
logger = logging.getLogger(__name__)

def generate_discontinued_report(discontinued_products):
  """
  Genera el HTML para el informe de productos descatalogados
  """
  html = """
  <h2>Productos Descatalogados</h2>
  <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
      <tr style="background-color: #f2f2f2;">
          <th>Imagen</th>
          <th>Referencia</th>
          <th>Descripción</th>
          <th>Días Ausente</th>
          <th>Último Precio</th>
          <th>Último Stock</th>
      </tr>
  """
  
  for product in discontinued_products:
      html += f"""
      <tr>
          <td style="width: 100px;"><img src="{product['image']}" style="width: 100px; height: 100px; object-fit: cover;"></td>
          <td>{product['reference']}</td>
          <td>{product['name']}</td>
          <td style="text-align: center;">{product['days_missing']}</td>
          <td style="text-align: right;">{product['last_price']:.2f} €</td>
          <td style="text-align: right;">{product['last_stock']}</td>
      </tr>
      """
  
  html += "</table>"
  return html

def generate_missing_variants_report(missing_variants):
   """
   Genera el HTML para el informe de productos no encontrados en CSV
   """
   html = """
   <h2>Productos en Shopify no encontrados en CSV</h2>
   <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
       <tr style="background-color: #f2f2f2;">
           <th>Referencia</th>
           <th>Último Precio</th>
           <th>Último Stock</th>
       </tr>
   """
   
   for product in missing_variants.values():
       html += f"""
       <tr>
           <td>{product['reference']}</td>
           <td style="text-align: right;">{product['last_price']:.2f} €</td>
           <td style="text-align: right;">{product['last_stock']}</td>
       </tr>
       """
   
   html += "</table>"
   return html

def generate_report_html(stats, elapsed_time, force):
    html = """
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; margin-bottom: 20px;">
        <tr style="background-color: #f2f2f2;">
            <th>Métrica</th>
            <th>Valor</th>
        </tr>
        <tr>
            <td>Total productos actual</td>
            <td>{current[total]:,}</td>
        </tr>
        <tr>
            <td>Total productos anterior</td>
            <td>{previous[total]:,} ({previous[difference]:+,})</td>
        </tr>
        <tr>
            <td>Productos mapeados</td>
            <td>{variants[mapped]:,} ({variants[percent]}%)</td>
        </tr>
    """.format(**stats)

    # Cambios de precio
    price_changes = stats.get('price_changes', {'count': 0, 'percent': 0})
    html += """
    <tr>
        <td>Cambios de precio</td>
        <td>{count:,} ({percent}%)</td>
    </tr>
    """.format(**price_changes)

    # Cambios de stock
    stock_changes = stats.get('stock_changes', {'count': 0, 'percent': 0})
    html += """
    <tr>
        <td>Cambios de stock</td>
        <td>{count:,} ({percent}%)</td>
    </tr>
    """.format(**stock_changes)

    # Variantes perdidas
    if 'missing_variants' in stats:
        html += """
        <tr>
            <td>Variantes no encontradas</td>
            <td>{count:,} ({percent}%)</td>
        </tr>
        """.format(**stats['missing_variants'])

    # altas y bajas
    if 'product_changes' in stats:
        html += """
        <tr>
            <td>Productos nuevos</td>
            <td>{new:,}</td>
        </tr>
        <tr>
            <td>Productos eliminados</td>
            <td>{removed:,}</td>
        </tr>
        """.format(**stats['product_changes'])

    html += """
        <tr>
            <td>Productos precio 0</td>
            <td>{current[zero_prices][count]:,} ({current[zero_prices][percent]}%)</td>
        </tr>
        <tr>
            <td>Productos stock 0</td>
            <td>{current[zero_stock][count]:,} ({current[zero_stock][percent]}%)</td>
        </tr>
    </table>
    """.format(**stats)
    
    return html

def sync_catalog(force_type: str = None):
    """
    Sincroniza el catálogo con Shopify
    Args:
        force_type: Tipo de sincronización forzada:
            - None: Modo normal, detecta cambios
            - 'all': Fuerza actualización de precio y stock
            - 'prices': Fuerza solo actualización de precios 
            - 'stock': Fuerza solo actualización de stock
    """
    try:
        email_sender = EmailSender() 
        start_time = datetime.now()
        print("\nIniciando sincronización de catálogo" + 
              (f" (modo forzado: {force_type})" if force_type else ""))

        file_manager = FileManager()
        processor = CSVProcessor(file_manager)
        db = next(get_db())
        queue_manager = QueueManager(db)
        email_sender = EmailSender()

        # Hacer backup del catálogo actual antes de procesar el nuevo
        print("\nPreparando archivos para comparación...")
        file_manager.backup_current_before_processing()

        url = os.getenv('CSV_URL')
        auth = (os.getenv('CSV_USERNAME'), os.getenv('CSV_PASSWORD'))
       
        today_path = os.path.join(
            file_manager.csv_dir,
            start_time.strftime('%Y%m%d')
        )
   
        today_files = sorted([f for f in os.listdir(today_path) if f.endswith('.csv')]) if os.path.exists(today_path) else []

        if today_files and not force_type:
            print(f"\nUsando catálogo existente de hoy: {today_files[-1]}")
            shutil.copy(
                os.path.join(today_path, today_files[-1]), 
                file_manager.current_file
            )
        else:
            print("\nDescargando nuevo catálogo...")
            if not processor.download_and_process_file(url, auth):
                raise Exception("Error descargando/procesando catálogo")

        valid, message, df, stats = processor.validate_csv()
        if not valid:
            raise Exception(f"Error validando CSV: {message}")

        # Detectar altas y bajas
        new_products, removed_products = processor.detect_new_and_removed_products()
        stats['product_changes'] = {
            'new': len(new_products),
            'removed': len(removed_products)
        } 

        # Detectar variantes ausentes
        missing_variants, total_variants = processor.detect_missing_variants(db)
        if missing_variants:
            stats['missing_variants'] = {
                'count': len(missing_variants),
                'total_variants': total_variants,
                'percent': round((len(missing_variants) / total_variants * 100), 1),
                'variants': missing_variants
            }

        # Calcular variantes mapeadas
        total = len(df)
        print(f"\nCatálogo actual: {total:,} productos")
        print("Calculando productos mapeados...")
       
        mapped_variants = sum(1 for ref in df['REFERENCIA'] if queue_manager.get_variant_id(ref) is not None)
        stats['variants'] = {
            'mapped': mapped_variants,
            'percent': round((mapped_variants / stats['current']['total']) * 100, 1)
        }

        if force_type:
            total_processed = 0
            print(f"\nProcesando {total:,} registros en modo forzado ({force_type})...")
           
            for _, row in df.iterrows():
                ref = row['REFERENCIA']
                if force_type in ['all', 'prices']:
                    queue_manager.register_price_changes({
                        ref: {
                            'new_price': float(row['PRECIO']),
                            'descripcion': row['DESCRIPCION']
                        }
                    })
                if force_type in ['all', 'stock']:
                    queue_manager.register_stock_changes({
                        ref: {
                            'new_stock': int(row['STOCK']),
                            'descripcion': row['DESCRIPCION']
                        }
                    })
                total_processed += 1
                if total_processed % 1000 == 0:
                    print(f"Procesados: {total_processed:,} ({(total_processed/total*100):.1f}%) - Pendientes: {total-total_processed:,}")
           
            stats['total_processed'] = total_processed
        else:
            print("\nDetectando cambios...")
            price_changes, stock_changes = processor.detect_changes()
            discontinued_products = processor.detect_discontinued_products()
            logger.info(f"Productos descatalogados encontrados: {discontinued_products}")
            total_changes = len(price_changes) + len(stock_changes)
            processed = 0

            if price_changes:
                print(f"Procesando {len(price_changes):,} cambios de precio...")
                queue_manager.register_price_changes(price_changes)
                processed += len(price_changes)
                stats['price_changes'] = {
                    'count': len(price_changes),
                    'percent': round(len(price_changes) / stats['current']['total'] * 100, 1)
                }
                print(f"Cambios de precio completados")
           
            if stock_changes:
                print(f"Procesando {len(stock_changes):,} cambios de stock...")
                queue_manager.register_stock_changes(stock_changes)
                processed += len(stock_changes)
                stats['stock_changes'] = {
                    'count': len(stock_changes),
                    'percent': round(len(stock_changes) / stats['current']['total'] * 100, 1)
                }
                print(f"Cambios de stock completados")

            if discontinued_products:
                stats['discontinued'] = []
                for ref, data in discontinued_products.items():
                    stats['discontinued'].append({
                        'reference': ref,
                        'name': data['descripcion'],
                        'image': data.get('imagen', ''),
                        'days_missing': data['dias_ausente'],
                        'last_price': data['last_price'],
                        'last_stock': data['last_stock']
                    })

        file_manager.archive_current_file()

        # Generar resumen e informe
        elapsed_time = datetime.now() - start_time
        summary_html = generate_report_html(stats, elapsed_time, force_type)

        # En la sección de mostrar resumen
        print("\n" + "="*50)
        print("RESUMEN DE SINCRONIZACIÓN")
        print("="*50)
        print(f"Tiempo total: {elapsed_time.total_seconds()/60:.1f} minutos")
        print(f"Total productos actual: {stats['current']['total']:,}")
        print(f"Total productos anterior: {stats['previous']['total']:,} ({stats['previous']['difference']:+,})")
        print(f"Productos mapeados: {stats['variants']['mapped']:,} ({stats['variants']['percent']}%)")

        if 'missing_variants' in stats:
            print(f"Variantes no encontradas: {stats['missing_variants']['count']:,} ({stats['missing_variants']['percent']}%)")

        if force_type:
            print(f"Productos procesados: {stats['total_processed']:,}")
        else:
            price_changes = stats.get('price_changes', {'count': 0, 'percent': 0})
            stock_changes = stats.get('stock_changes', {'count': 0, 'percent': 0})
            print(f"Cambios de precio: {price_changes['count']:,} ({price_changes['percent']}%)")
            print(f"Cambios de stock: {stock_changes['count']:,} ({stock_changes['percent']}%)")
            if 'discontinued' in stats:
                print(f"Productos descatalogados: {len(stats['discontinued']):,}")

        print(f"Productos precio 0: {stats['current']['zero_prices']['count']:,} ({stats['current']['zero_prices']['percent']}%)")
        print(f"Productos stock 0: {stats['current']['zero_stock']['count']:,} ({stats['current']['zero_stock']['percent']}%)")
        print("="*50)

        # Enviar emails
        subject = f"{'⚠️ Sincronización Forzada ' if force_type else ''}Catálogo {start_time.strftime('%Y-%m-%d')}"
        email_sender.send_email(
            subject=subject,
            recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
            html_content=summary_html
        )

        if 'discontinued' in stats and stats['discontinued']:
            discontinued_html = generate_discontinued_report(stats['discontinued'])
            email_sender.send_email(
                subject=f"🚫 Productos Descatalogados {start_time.strftime('%Y-%m-%d')}",
                recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
                html_content=discontinued_html
            )

        if 'missing_variants' in stats and stats['missing_variants']['variants']:
            missing_html = generate_missing_variants_report(stats['missing_variants']['variants'])
            email_sender.send_email(
                subject=f"⚠️ Productos no encontrados en CSV {start_time.strftime('%Y-%m-%d')}",
                recipients=[os.getenv('ALERT_EMAIL_RECIPIENT')],
                html_content=missing_html
            )
        if 'product_changes' in stats:
            print(f"Productos nuevos: {stats['product_changes']['new']:,}")
            print(f"Productos eliminados: {stats['product_changes']['removed']:,}")    

        print("\nSincronización completada exitosamente")
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
    parser = argparse.ArgumentParser(description="Sincronización de catálogo con Shopify")
    parser.add_argument(
        '--force', 
        choices=['all', 'prices', 'stock'],
        help='Tipo de sincronización forzada:\n'
             'all: Fuerza actualización de precio y stock\n'
             'prices: Fuerza solo actualización de precios\n'
             'stock: Fuerza solo actualización de stock'
    )
    args = parser.parse_args()
    sync_catalog(force_type=args.force)