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
├── config/                                      # Configuración del sistema
├── data/                                        # Datos y archivos CSV
│   └── csv_archive/                             # Histórico de catálogos  
├── logs/                                        # Logs del sistema
├── src/
│   ├── csv_processor/                          # Procesamiento de CSV
│   ├── database/                               # Conexión y modelos BD
│   ├── shopify/                                # Cliente API Shopify
│   ├── sync/                                   # Lógica de sincronización
│   └── utils/                                  # Utilidades (email, archivos)
├── tests/                                      # Tests del sistema
└── tools/                                      # Scripts de utilidad
    └── update_variant_mappings.py              # Permite forzar la sincronización del mapeo de variantes con referencias del CSV

## Base de datos

Tablas principales:
- `product_mappings`: Mapeo productos locales-Shopify
- `variant_mappings`: Mapeo variantes locales-Shopify 
- `price_updates_queue`: Cola de actualizaciones de precio
- `stock_updates_queue`: Cola de actualizaciones de stock
- `price_history`: Histórico de precios
- `stock_history`: Histórico de stock


## Sincronización del catálogo (actualizar colas)

`python src/sync/catalog.py` o `python src/sync/catalog.py --force`

Es el sistema que detecta cambios de precios entre distintos CSV y los mete en la cola de procesamiento de stock y precios, así como en los históricos de precios y stock.

Este sistema descarga el catálogo de Perez Mora, lo procesa y lo compara con el ultimo de que disponga. 

Resumen 

- Descarga y procesamiento de CSV
- Detección de cambios (precio/stock)
- Informe de posibles productos descatalogados
- Mantiene sistema de colas para actualizaciones
- Reportes y alertas por email 

### Análisis Sistema de Sincronización de Catálogo

#### 1. ESTRUCTURA GENERAL
El sistema maneja la sincronización de un catálogo de productos entre un CSV fuente y Shopify, con estas funciones principales:
* Descarga y procesamiento de CSV 
* Detección de cambios (precio/stock)
* Gestión de productos descatalogados
* Sistema de colas para actualizaciones
* Reportes y alertas por email

#### 2. FLUJO PRINCIPAL (catalog.py)

##### a) Inicialización
* Carga configuración (.env)
* Inicializa servicios (FileManager, CSVProcessor, DB, QueueManager, EmailSender)

##### b) Obtención del Catálogo  
* Verifica si existe catálogo del día actual
* Si no existe/modo forzado: descarga nuevo catálogo
* Si existe: reutiliza el último archivo del día

##### c) Validación y Estadísticas
* Valida estructura del CSV
* Calcula estadísticas (productos totales, precios 0, stock 0)
* Verifica variantes mapeadas en Shopify

##### d) Procesamiento (dos modos)

###### Modo Normal
* Detecta cambios comparando con catálogo anterior
* Procesa cambios de precio y stock 
* Detecta productos descatalogados

###### Modo Forzado
* Procesa todos los productos sin comparar
* Actualiza precios y stock de todo el catálogo

##### e) Informes
* Genera reporte HTML con estadísticas
* Envía email con resumen
* Si hay productos descatalogados, envía reporte separado

#### 3. PROCESAMIENTO CSV (processor.py)

##### a) Validaciones
* Columnas requeridas
* Tipos de datos numéricos 
* Valores mínimos (precio > 0)
* Diferencias significativas con catálogo anterior

##### b) Detección de Cambios
* Compara precios y stock con versión anterior
* Registra diferencias con valores antiguos y nuevos

##### c) Productos Descatalogados
* Analiza últimos X días de catálogos
* Detecta productos ausentes consecutivamente 
* Guarda último precio/stock conocido

#### 4. CONTROLES Y SEGURIDAD

##### a) Validación de Datos
* Estructura CSV completa
* Tipos de datos correctos
* Valores numéricos válidos
* Límites en diferencias de productos (>10%)
* Alerta por stock masivo en 0 (>40%)

##### b) Control de Archivos
* Gestión de versiones por fecha
* Backup de catálogos anteriores 
* Verificación de existencia de archivos

##### c) Control de Errores
* Manejo de excepciones en cada etapa
* Rollback en operaciones de BD
* Logging detallado
* Notificaciones de error por email

##### d) Rate Limiting
* Control en actualizaciones Shopify
* Procesamiento por lotes
* Tiempos de espera entre operaciones

#### 5. REPORTES Y MONITOR
* Estadísticas detalladas de cambios
* Seguimiento de productos descatalogados
* Alertas por email para precios 0
* Logs detallados de operaciones
* Tiempos de procesamiento

##### Sincronización normal

python src/sync/catalog.py

##### Sincronización forzada

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

## Puesta en marcha

Requisitos previos:
- Python 3.10+
- MariaDB/MySQL accesible y una base de datos creada (p. ej. `shopify_sync`)

Pasos:
- Crear y activar entorno virtual
  - `python3 -m venv .venv && source .venv/bin/activate`
- Instalar dependencias
  - `pip install -r requirements.txt`
- Configurar variables de entorno
  - Copiar `.env.example` a `.env` y editar credenciales (`DB_*`, Shopify, email, CSV)
- Crear base de datos (si no existe)
  - En MariaDB: `CREATE DATABASE shopify_sync CHARACTER SET utf8mb4;`
- Inicializar tablas
  - Recomendado: `python tools/init_db.py`
  - Alternativas:
    - `python -c "from src.database.connection import engine, Base; Base.metadata.create_all(bind=engine)"`
    - `python tests/test_connection.py` (prueba conexión y crea tablas)

Ejecutar sincronización:
- Normal: `python src/sync/catalog.py`
- Forzada: `python src/sync/catalog.py --force`

Notas:
- La estructura `data/` y `data/csv_archive/` se crea automáticamente al ejecutar.
- Los logs se guardan en `data/logs/` con rotación mensual.

## Dependencias principales

- SQLAlchemy
- Pandas  
- Requests
- BeautifulSoup4
- Python-dotenv

## Ejecución programada

Se recomienda configurar un cron job para la ejecución diaria:

0 6 * * * /ruta/proyecto/.venv/bin/python /ruta/proyecto/src/sync/catalog.py >> /ruta/proyecto/data/logs/sync.log 2>&1

## Utilidades

- Inicializar base de datos: `python tools/init_db.py`
  - Carga `.env`, prueba la conexión y crea todas las tablas definidas por los modelos.
  - Usa la URL configurada en `src/database/connection.py`.
