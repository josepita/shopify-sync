import logging
import sys

from dotenv import load_dotenv
from sqlalchemy import text


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Cargar variables de entorno
    load_dotenv()

    try:
        # Importar conexión y Base (connection ya hace autoimport de modelos)
        from src.database.connection import engine, Base

        logging.info(
            "Usando URL de conexión: %s",
            engine.url.render_as_string(hide_password=True),
        )

        # Probar conexión básica
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Conexión a base de datos OK")

        # Crear tablas si no existen
        Base.metadata.create_all(bind=engine)
        logging.info("Tablas creadas o ya existentes.")
        return 0

    except Exception as e:
        logging.error("Error inicializando base de datos: %s", str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())

