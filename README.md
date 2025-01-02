# Shopify Sync

Sistema de sincronización automática de catálogo entre proveedor y Shopify.

## Descripción

El sistema descarga diariamente un catálogo en formato CSV/HTML desde el proveedor, procesa los cambios de precio y stock, y los sincroniza con una tienda Shopify usando la Admin API.

## Características principales

- Descarga y procesamiento de catálogo del proveedor
- Detección de cambios en precios y stock 
- Sistema de colas para actualizaciones en Shopify
- Histórico de precios y stock
- Sistema de alertas por email
- Validaciones automáticas:
  - Productos con precio 0
  - Stock masivo a 0 (>40%)
  - Diferencias significativas en número de productos (>10%)
- Detección de productos descatalogados
- Modo de sincronización forzada

## Estructura del proyecto

.
├── config/               # Configuración del sistema
├── data/                # Datos y archivos CSV
│   └── csv_archive/     # Histórico de catálogos  
├── logs/                # Logs del sistema
├── src/
│   ├── csv_processor/   # Procesamiento de CSV
│   ├── database/        # Conexión y modelos BD
│   ├── shopify/         # Cliente API Shopify
│   ├── sync/           # Lógica de sincronización
│   └── utils/          # Utilidades (email, archivos)
├── tests/              # Tests del sistema
└── tools/              # Scripts de utilidad

## Base de datos

Tablas principales:
- `product_mappings`: Mapeo productos locales-Shopify
- `variant_mappings`: Mapeo variantes locales-Shopify 
- `price_updates_queue`: Cola de actualizaciones de precio
- `stock_updates_queue`: Cola de actualizaciones de stock
- `price_history`: Histórico de precios
- `stock_history`: Histórico de stock

## Uso

### Sincronización normal

python src/sync/catalog.py

### Sincronización forzada

python src/sync/catalog.py --force

## Configuración

Archivo `.env`:

# Database
DB_HOST=localhost
DB_USER=user
DB_PASSWORD=pass
DB_NAME=shopify_sync

# Shopify
SHOPIFY_SHOP_URL=tienda.myshopify.com
SHOPIFY_ACCESS_TOKEN=token

# CSV Source
CSV_URL=url_catalogo
CSV_USERNAME=user
CSV_PASSWORD=pass

# Email
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=email
SMTP_PASSWORD=pass
ALERT_EMAIL_RECIPIENT=email_alertas

## Instalación

1. Clonar repositorio
2. Instalar dependencias: `pip install -r requirements.txt`
3. Configurar variables de entorno
4. Ejecutar migraciones: `python scripts/run_migrations.py`

## Dependencias principales

- SQLAlchemy
- Pandas  
- Requests
- BeautifulSoup4
- Python-dotenv

## Ejecución programada

Se recomienda configurar un cron job para la ejecución diaria:

0 6 * * * /ruta/python /ruta/src/sync/catalog.py >> /ruta/logs/sync.log 2>&1