import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def test_database_connection():
    try:
        # Crear URL de conexión
        DATABASE_URL = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        
        # Crear engine y probar conexión
        engine = create_engine(DATABASE_URL, echo=True)
        
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("¡Conexión básica exitosa!")
            print("Resultado de la consulta:", result.scalar())
            
        return True
            
    except Exception as e:
        print(f"Error en la conexión básica: {e}")
        return False

def test_database_full():
    if test_database_connection():
        try:
            from src.database.connection import engine, Base
            from src.database.models import ProductMapping, VariantMapping
            
            # Crear tablas
            Base.metadata.create_all(bind=engine)
            print("¡Tablas creadas correctamente!")
            
        except Exception as e:
            print(f"Error en la creación de tablas: {e}")
    
if __name__ == "__main__":
    test_database_full()