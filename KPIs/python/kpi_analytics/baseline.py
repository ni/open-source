############################################
# baseline.py
############################################

import mysql.connector
from datetime import datetime, timedelta
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

def find_oldest_date_for_repo(repo_name):
    """
    Searches multiple tables (issues, pulls, forks, stars)
    for earliest known date for that repo.
    Returns None if no data found.
    """
    queries = [
      "SELECT MIN(created_at) FROM issues WHERE repo_name=%s",
      "SELECT MIN(created_at) FROM pulls  WHERE repo_name=%s",
      "SELECT MIN(created_at) FROM forks  WHERE repo_name=%s",
      "SELECT MIN(starred_at) FROM stars WHERE repo_name=%s"
    ]
    cnx= get_db_connection()
    cursor= cnx.cursor()
    earliest= None
    for q in queries:
        cursor.execute(q, (repo_name,))
        row= cursor.fetchone()
        if row and row[0]:
            dt= row[0]
            if earliest is None or dt< earliest:
                earliest= dt
    cursor.close()
    cnx.close()
    return earliest
