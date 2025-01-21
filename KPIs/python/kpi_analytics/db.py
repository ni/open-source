# db.py

import mysql.connector
from config import load_db_config

def get_connection():
    cfg = load_db_config()
    conn = mysql.connector.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"]
    )
    return conn
