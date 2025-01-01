import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de Base de Datos
DATABASE = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}

# Configuración de Shopify
SHOPIFY = {
    'shop_url': os.getenv('SHOPIFY_SHOP_URL'),
    'access_token': os.getenv('SHOPIFY_ACCESS_TOKEN'),
}

# Configuración de CSV
CSV = {
    'url': os.getenv('CSV_URL'),
    'username': os.getenv('CSV_USERNAME'),
    'password': os.getenv('CSV_PASSWORD'),
}

# Configuración de Email
EMAIL = {
    'smtp_host': os.getenv('SMTP_HOST'),
    'smtp_port': int(os.getenv('SMTP_PORT', 587)),
    'smtp_user': os.getenv('SMTP_USER'),
    'smtp_password': os.getenv('SMTP_PASSWORD'),
}

# Configuración de rutas
PATHS = {
    'csv_archive': 'data/csv_archive',
    'logs': 'logs',
}