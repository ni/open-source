"""
db_config.py
Stores DB credentials, possibly read from environment variables or hard-coded.

Adapt or override with environment variables for local debugging vs. GitHub Action usage.
"""

import os

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "root")
DB_DATABASE = os.environ.get("DB_DATABASE", "my_kpis_analytics_db2")

if not DB_HOST or not DB_USER or not DB_DATABASE:
    raise RuntimeError("Missing DB config. Please set DB_HOST, DB_USER, and DB_DATABASE.")
