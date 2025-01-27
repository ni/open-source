# db.py

import mysql.connector
from config import load_db_config

def get_connection():
    cfg = load_db_config()
    conn = mysql.connector.connect(
        host=cfg["localhost"],
        port=cfg["3306"],
        user=cfg["root"],
        password=cfg["root"],
        database=cfg["my_kpis_analytics_db2"]
    )
    return conn
