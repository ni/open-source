# db_config.py
import os

# You can store DB credentials here, or use environment variables.
DB_HOST = os.environ.get("DB_HOST","localhost")
DB_USER = os.environ.get("DB_USER","root")
DB_PASSWORD = os.environ.get("DB_PASSWORD","root")
DB_DATABASE = os.environ.get("DB_DATABASE","my_kpis_analytics_sergio")
