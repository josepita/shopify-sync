import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Tuple, Dict
import requests
from bs4 import BeautifulSoup
import re
import os

logger = logging.getLogger(__name__)

class CSVProcessor:
    def __init__(self, file_manager):
        self.file_manager = file_manager
        self.base_dir = file_manager.base_dir
        self.csv_dir = file_manager.csv_dir
        self.previous_file = file_manager.previous_file
        self.current_file = file_manager.current_file
        # Estructura exacta del CSV
        self.required_columns = [
            'REFERENCIA', 'DESCRIPCION', 'PRECIO', 'STOCK', 
            'CATEGORIA', 'SUBCATEGORIA', 'METAL', 'COLOR ORO', 
            'TIPO', 'PESO G.', 'PIEDRA', 'CALIDAD PIEDRA', 
            'MEDIDAS', 'CIERRE', 'TALLA', 'GENERO',
            'IMAGEN 1', 'IMAGEN 2', 'IMAGEN 3'
        ]
        # Columnas que requieren validación numérica
        self.numeric_columns = {
            'PRECIO': {'min_value': 0.01, 'decimals': True},
            'STOCK': {'min_value': 0, 'decimals': False},
            'PESO G.': {'min_value': 0, 'decimals': True}
        }

    def download_and_process_file(self, url: str, auth: Tuple[str, str] = None) -> bool:
        """
        Descarga y procesa el archivo que contiene HTML directamente
        """
        try:
            logger.info(f"Intentando descargar archivo desde: {url}")
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            if not table:
                raise ValueError("No se encontró tabla en el HTML")

            # Extraer headers
            headers = []
            header_row = table.find('tr')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all('td')]
            
            logger.info(f"Headers encontrados: {headers}")

            # Verificar que tenemos todos los headers necesarios
            missing_columns = set(self.required_columns) - set(headers)
            if missing_columns:
                raise ValueError(f"Faltan columnas requeridas: {missing_columns}")

            # Extraer datos
            data = []
            for tr in table.find_all('tr')[1:]:
                row_data = {}
                cells = tr.find_all('td')
                for i, td in enumerate(cells):
                    if i < len(headers):
                        text = td.get_text(strip=True)
                        header = headers[i]
                        
                        # Limpieza específica por tipo de campo
                        if header == 'PRECIO':
                            text = re.sub(r'[^\d.,]', '', text)
                            text = text.replace(',', '.')
                        elif header == 'STOCK':
                            text = re.sub(r'[^\d]', '', text)
                            text = text if text else '0'
                        elif header == 'PESO G.':
                            text = re.sub(r'[^\d.,]', '', text)
                            text = text.replace(',', '.')
                        elif header == 'REFERENCIA':
                            text = text.strip()  # Asegurar que no hay espacios
                        
                        row_data[header] = text
                if row_data:
                    data.append(row_data)

            # Convertir a DataFrame
            df = pd.DataFrame(data)
            
            # Convertir tipos de datos
            for col, specs in self.numeric_columns.items():
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if specs['decimals']:
                        df[col] = df[col].round(2)
                    else:
                        df[col] = df[col].astype('Int64')  # Permite NaN en enteros

            # Guardar como CSV
            df.to_csv(self.file_manager.current_file, index=False)
            
            logger.info(f"Archivo procesado y convertido a CSV correctamente. {len(df)} filas procesadas")
            return True
            
        except Exception as e:
            logger.error(f"Error procesando archivo: {str(e)}")
            return False

    def validate_csv(self) -> Tuple[bool, str, pd.DataFrame, Dict]:
        """
        Valida el CSV y retorna también estadísticas
        """
        try:
            df = pd.read_csv(self.file_manager.current_file)
            stats = {}
            
            # Validar columnas básicas
            missing_columns = [col for col in self.required_columns if col not in df.columns]
            if missing_columns:
                return False, f"Faltan columnas: {', '.join(missing_columns)}", None, None

            # Convertir campos numéricos
            for col, specs in self.numeric_columns.items():
                df[col] = pd.to_numeric(df[col], errors='coerce')

            total_products = len(df)
            
            # Análisis de precios 0
            zero_prices = df[df['PRECIO'] == 0]
            zero_prices_count = len(zero_prices)
            zero_prices_percent = (zero_prices_count / total_products) * 100
            
            # Análisis de stock 0
            zero_stock = len(df[df['STOCK'] == 0])
            zero_stock_percent = (zero_stock / total_products) * 100

            # Comparar total de productos con archivo anterior
            if os.path.exists(self.file_manager.previous_file):
                prev_df = pd.read_csv(self.file_manager.previous_file)
                prev_total = len(prev_df)
                diff_percent = ((total_products - prev_total) / prev_total) * 100
                products_diff = total_products - prev_total
            else:
                diff_percent = 0
                products_diff = 0

            stats = {
                'current': {
                    'total': total_products,
                    'zero_prices': {
                        'count': zero_prices_count,
                        'percent': round(zero_prices_percent, 1)
                    },
                    'zero_stock': {
                        'count': zero_stock,
                        'percent': round(zero_stock_percent, 1)
                    }
                },
                'previous': {
                    'total': prev_total if os.path.exists(self.file_manager.previous_file) else 0,
                    'difference': products_diff
                }
            }

            # Verificar problema de stock masivo
            if zero_stock_percent > 40:
                return False, f"El {zero_stock_percent:.2f}% de productos tienen stock 0", None, stats

            # Verificar diferencia de productos
            if abs(diff_percent) > 10:
                return False, f"Diferencia de productos ({diff_percent:.2f}%) supera el 10% permitido", None, stats

            return True, "CSV válido", df, stats

        except Exception as e:
            logger.error(f"Error validando CSV: {str(e)}")
            return False, str(e), None, None

    def detect_changes(self, limit: int = None) -> Tuple[Dict, Dict]:
        """
        Detecta cambios en precios y stock entre el CSV actual y el anterior
        Args:
            limit: Número máximo de registros a procesar
        """
        try:
            current_df = pd.read_csv(self.file_manager.current_file)
            if limit:
                current_df = current_df.head(limit)
                
            if not os.path.exists(self.file_manager.previous_file):
                logger.warning("No existe archivo previo para comparar") 
                return {}, {}

            previous_df = pd.read_csv(self.file_manager.previous_file)

            # Convertir campos numéricos
            for col in ['PRECIO', 'STOCK']:
                current_df[col] = pd.to_numeric(current_df[col], errors='coerce')
                previous_df[col] = pd.to_numeric(previous_df[col], errors='coerce')

            price_changes = {}
            stock_changes = {}

            for _, row in current_df.iterrows():
                ref = row['REFERENCIA']
                prev_row = previous_df[previous_df['REFERENCIA'] == ref]
                
                if len(prev_row) > 0:
                    if row['PRECIO'] != prev_row.iloc[0]['PRECIO']:
                        price_changes[ref] = {
                            'old_price': prev_row.iloc[0]['PRECIO'],
                            'new_price': row['PRECIO'],
                            'descripcion': row['DESCRIPCION']
                        }
                    
                    if row['STOCK'] != prev_row.iloc[0]['STOCK']:
                        stock_changes[ref] = {
                            'old_stock': prev_row.iloc[0]['STOCK'],
                            'new_stock': row['STOCK'],
                            'descripcion': row['DESCRIPCION']
                        }

            logger.info(f"Detectados {len(price_changes)} cambios de precio y {len(stock_changes)} cambios de stock")
            return price_changes, stock_changes

        except Exception as e:
            logger.error(f"Error detectando cambios: {str(e)}")
            return {}, {}

    def detect_discontinued_products(self, days_threshold: int = 3) -> Dict:
        """
        Detecta productos que no aparecen en el catálogo actual pero sí en catálogos anteriores.
        
        Proceso:
        1. Obtiene referencias del catálogo actual
        2. Busca archivos CSV de los últimos X días (days_threshold)
        3. Por cada archivo histórico:
            - Compara referencias con catálogo actual
            - Registra productos ausentes
            - Acumula días de ausencia
        4. Filtra productos ausentes por días_threshold
        
        Args:
            days_threshold: Días consecutivos sin aparecer para considerar descatalogado
            
        Returns:
            Dict con productos descatalogados:
            {
                'referencia': {
                    'referencia': str,
                    'descripcion': str,
                    'first_missing_date': date,
                    'last_price': float,
                    'last_stock': int,
                    'dias_ausente': int
                }
            }
        """
        try:
            if not os.path.exists(self.current_file):
                logger.error("No existe archivo actual para comparar")
                return {}
                    
            current_df = pd.read_csv(self.current_file)
            current_refs = set(current_df['REFERENCIA'])
            
            logger.info(f"Referencias en catálogo actual: {len(current_refs)}")
            
            last_days_files = []
            last_days_dates = []
            
            logger.info(f"Buscando archivos de los últimos {days_threshold} días...")
            
            for i in range(1, days_threshold + 2):
                date = datetime.now() - timedelta(days=i)
                day_folder = os.path.join(
                    self.csv_dir,
                    date.strftime('%Y%m%d')
                )
                
                if os.path.exists(day_folder):
                    files = sorted([f for f in os.listdir(day_folder) if f.endswith('.csv')])
                    if files:
                        file_path = os.path.join(day_folder, files[-1])
                        last_days_files.append(file_path)
                        last_days_dates.append(date.strftime('%Y-%m-%d'))
                        logger.info(f"Día {date.strftime('%Y-%m-%d')}: usando archivo {files[-1]}")
                    else:
                        logger.info(f"Día {date.strftime('%Y-%m-%d')}: no hay archivos CSV")
                else:
                    logger.info(f"Día {date.strftime('%Y-%m-%d')}: carpeta no existe")

            if len(last_days_files) < days_threshold:
                logger.warning(
                    f"Insuficientes días con datos ({len(last_days_files)} de {days_threshold})"
                )
                return {}

            discontinued = {}
            
            for idx, file_path in enumerate(last_days_files):
                try:
                    df = pd.read_csv(file_path)
                    historic_refs = set(df['REFERENCIA'])
                    missing_refs = historic_refs - current_refs
                    
                    logger.info(f"Analizando {file_path}")
                    logger.info(f"- Referencias históricas: {len(historic_refs)}")
                    logger.info(f"- Referencias ausentes: {len(missing_refs)}")
                    
                    for ref in missing_refs:
                        product_data = df[df['REFERENCIA'] == ref].iloc[0]
                        
                        if ref not in discontinued:
                            discontinued[ref] = {
                                'referencia': ref,
                                'descripcion': product_data['DESCRIPCION'],
                                'imagen': product_data['IMAGEN 1'],
                                'first_missing_date': last_days_dates[idx],
                                'last_price': float(product_data['PRECIO']),
                                'last_stock': int(product_data['STOCK']),
                                'dias_ausente': 1
                            }
                        else:
                            discontinued[ref]['dias_ausente'] += 1
                            
                except Exception as e:
                    logger.error(f"Error procesando {file_path}: {str(e)}")

            final_discontinued = {
                ref: data for ref, data in discontinued.items() 
                if data['dias_ausente'] >= days_threshold
            }

            logger.info(
                f"Productos descatalogados encontrados: {len(final_discontinued)} "
                f"(ausentes {days_threshold} días o más)"
            )
            
            return final_discontinued

        except Exception as e:
            logger.error(f"Error en detect_discontinued_products: {str(e)}")
            return {}

    
    def send_price_alerts(self, zero_prices_df: pd.DataFrame, email_sender):
        """
        Envía alertas por email sobre productos con precio 0
        """
        try:
            # Obtener email del destinatario
            recipient = os.getenv('ALERT_EMAIL_RECIPIENT')
            if not recipient:
                logger.error("No se ha configurado ALERT_EMAIL_RECIPIENT en .env")
                return False

            # Preparar contenido del email
            products_list = ""
            for _, row in zero_prices_df.iterrows():
                products_list += f"<tr><td>{row['REFERENCIA']}</td><td>{row['DESCRIPCION']}</td></tr>"

            html_content = f"""
            <html>
                <body>
                    <h2>Alerta: Productos con Precio 0 Detectados</h2>
                    <p>Se han detectado {len(zero_prices_df)} productos con precio 0 en el catálogo:</p>
                    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
                        <tr style="background-color: #f2f2f2;">
                            <th>Referencia</th>
                            <th>Descripción</th>
                        </tr>
                        {products_list}
                    </table>
                    <p>Por favor, verificar estos productos con el proveedor.</p>
                </body>
            </html>
            """

            # Enviar email
            if email_sender.send_email(
                subject="⚠️ Alerta: Productos con Precio 0 Detectados",
                recipients=[recipient],
                html_content=html_content
            ):
                logger.info("Alerta de precios enviada por email correctamente")
                return True
            else:
                logger.error("Error al enviar el email de alerta")
                return False
                
        except Exception as e:
            logger.error(f"Error enviando alerta de precios: {str(e)}")
            return False
        
