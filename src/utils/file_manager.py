import os
from datetime import datetime
import shutil
import logging

logger = logging.getLogger(__name__)

class FileManager:
    def __init__(self):
        # Directorios base
        self.base_dir = 'data'
        self.csv_dir = os.path.join(self.base_dir, 'csv_archive')
        self.current_file = os.path.join(self.base_dir, 'current.csv')
        self.previous_file = os.path.join(self.base_dir, 'previous.csv')
        
        # Crear estructura de directorios
        self._create_directory_structure()

    def _create_directory_structure(self):
        """Crea la estructura de directorios necesaria"""
        try:
            os.makedirs(self.base_dir, exist_ok=True)
            os.makedirs(self.csv_dir, exist_ok=True)
            logger.info(f"Estructura de directorios creada en {self.base_dir}")
        except Exception as e:
            logger.error(f"Error creando directorios: {str(e)}")

    def backup_current_before_processing(self):
        """
        Guarda una copia del current.csv como previous.csv antes de procesar el nuevo catálogo
        Returns:
            bool: True si se realizó el backup, False si no había archivo que respaldar
        """
        try:
            if os.path.exists(self.current_file):
                logger.info("Guardando copia del catálogo actual antes de procesar el nuevo")
                if os.path.exists(self.previous_file):
                    os.remove(self.previous_file)
                shutil.copy2(self.current_file, self.previous_file)
                logger.info(f"Backup creado: {self.previous_file}")
                return True
            logger.warning("No existe current.csv para hacer backup")
            return False
        except Exception as e:
            logger.error(f"Error haciendo backup del current.csv: {str(e)}")
            return False

    def archive_current_file(self):
        """
        Archiva el archivo actual con fecha y hora en el directorio de archivo
        y actualiza last_successful.csv si corresponde
        
        Returns:
            bool: True si el archivo se archivó correctamente
        """
        try:
            if not os.path.exists(self.current_file):
                logger.warning(f"No se encontró archivo actual: {self.current_file}")
                return False

            # Crear nombre de archivo con timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Crear subcarpeta para el día actual
            day_folder = os.path.join(
                self.csv_dir,
                datetime.now().strftime('%Y%m%d')
            )
            os.makedirs(day_folder, exist_ok=True)
            
            # Guardar copia con timestamp
            daily_archive = os.path.join(
                day_folder,
                f'catalogo_{timestamp}.csv'
            )
            shutil.copy2(self.current_file, daily_archive)
            logger.info(f"Archivo guardado en histórico: {daily_archive}")
            
            # Actualizar last_successful si es la primera ejecución del día
            last_successful = os.path.join(self.csv_dir, 'last_successful.csv')
            if not os.path.exists(last_successful):
                shutil.copy2(self.current_file, last_successful)
                logger.info("Creado primer last_successful.csv del día")
            else:
                last_date = datetime.fromtimestamp(os.path.getmtime(last_successful))
                if last_date.date() < datetime.now().date():
                    shutil.copy2(self.current_file, last_successful)
                    logger.info("Actualizado last_successful.csv con primera ejecución del día")
            
            return True
                
        except Exception as e:
            logger.error(f"Error archivando archivo: {str(e)}")
            return False

    def get_latest_file_from_day(self, date: datetime) -> str:
        """
        Obtiene el último archivo CSV de un día específico
        
        Args:
            date: Fecha para buscar el archivo
            
        Returns:
            str: Ruta al último archivo del día o None si no se encuentra
        """
        try:
            day_folder = os.path.join(
                self.csv_dir,
                date.strftime('%Y%m%d')
            )
            
            if not os.path.exists(day_folder):
                logger.warning(f"No existe carpeta para la fecha {date.strftime('%Y-%m-%d')}")
                return None
                
            files = sorted([
                f for f in os.listdir(day_folder) 
                if f.endswith('.csv')
            ])
            
            if not files:
                logger.warning(f"No hay archivos CSV para la fecha {date.strftime('%Y-%m-%d')}")
                return None
                
            return os.path.join(day_folder, files[-1])
            
        except Exception as e:
            logger.error(f"Error buscando archivo del día {date.strftime('%Y-%m-%d')}: {str(e)}")
            return None

    def clean_old_files(self, days_to_keep: int = 30):
        """
        Elimina archivos más antiguos que el número de días especificado
        
        Args:
            days_to_keep: Número de días de archivos a mantener
        """
        try:
            cutoff_date = datetime.now().date() - timedelta(days=days_to_keep)
            
            for folder in os.listdir(self.csv_dir):
                if not folder.isdigit() or folder == 'last_successful.csv':
                    continue
                    
                folder_date = datetime.strptime(folder, '%Y%m%d').date()
                if folder_date < cutoff_date:
                    folder_path = os.path.join(self.csv_dir, folder)
                    shutil.rmtree(folder_path)
                    logger.info(f"Eliminada carpeta antigua: {folder_path}")
                    
        except Exception as e:
            logger.error(f"Error limpiando archivos antiguos: {str(e)}")

    def log_execution(self, success: bool, error_message: str = None):
        """
        Registra cada ejecución del proceso en un archivo de log mensual
        
        Args:
            success: Si la ejecución fue exitosa
            error_message: Mensaje de error si la ejecución falló
        """
        try:
            # Asegurar que existe el directorio de logs
            logs_dir = os.path.join(self.base_dir, 'logs')
            os.makedirs(logs_dir, exist_ok=True)
            
            # Crear nombre del archivo de log para el mes actual
            month_file = os.path.join(
                logs_dir, 
                f"executions_{datetime.now().strftime('%Y_%m')}.log"
            )
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            status = "SUCCESS" if success else "ERROR"
            
            # Crear la línea de log
            log_line = f"{timestamp}\t{status}"
            if error_message:
                # Limpiar el mensaje de error de tabulaciones y saltos de línea
                clean_error = error_message.replace('\n', ' ').replace('\t', ' ')
                log_line += f"\t{clean_error}"
            log_line += "\n"
            
            # Escribir en el archivo
            with open(month_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
                
            logger.info(f"Ejecución registrada en {month_file}")
            
        except Exception as e:
            logger.error(f"Error registrando la ejecución: {str(e)}")