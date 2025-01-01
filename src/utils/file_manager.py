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
            os.makedirs(self.csv_dir, exist_ok=True)
            logger.info(f"Estructura de directorios creada en {self.base_dir}")
        except Exception as e:
            logger.error(f"Error creando directorios: {str(e)}")

    def archive_current_file(self):
        """Archiva el archivo actual con la fecha"""
        try:
            if os.path.exists(self.current_file):
                # Crear nombre de archivo con fecha y hora
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # Crear subcarpeta para el día
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
                
                # Actualizar last_successful si es la primera ejecución del día
                last_successful = os.path.join(self.csv_dir, 'last_successful.csv')
                if not os.path.exists(last_successful):
                    shutil.copy2(self.current_file, last_successful)
                else:
                    last_date = datetime.fromtimestamp(os.path.getmtime(last_successful))
                    if last_date.date() < datetime.now().date():
                        shutil.copy2(self.current_file, last_successful)
                
                # Actualizar previous
                if os.path.exists(self.previous_file):
                    os.remove(self.previous_file)
                shutil.copy2(last_successful, self.previous_file)
                
                logger.info(f"Archivo archivado correctamente: {daily_archive}")
                return True
                
            logger.warning(f"No se encontró archivo actual: {self.current_file}")
            return False
            
        except Exception as e:
            logger.error(f"Error archivando archivo: {str(e)}")
            return False

    def log_execution(self, success: bool, error_message: str = None):
        """
        Registra cada ejecución del proceso
        
        Args:
            success (bool): Si la ejecución fue exitosa
            error_message (str, optional): Mensaje de error si la ejecución falló
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