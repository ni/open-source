############################################
# baseline.py
############################################

import mysql.connector
from datetime import datetime, timedelta
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    """
    Returns a MySQL connection based on credentials from db_config.py.
    """
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

def find_oldest_date_for_repo(repo_name):
    """
    Finds the earliest known date for a given repo by scanning:
      - issues.created_at
      - pulls.created_at
      - forks.created_at
      - stars.starred_at

    If none found, returns None.
    """
    queries= [
      "SELECT MIN(created_at)   FROM issues WHERE repo_name=%s",
      "SELECT MIN(created_at)   FROM pulls  WHERE repo_name=%s",
      "SELECT MIN(created_at)   FROM forks  WHERE repo_name=%s",
      "SELECT MIN(starred_at)   FROM stars  WHERE repo_name=%s"
    ]

    cnx= get_db_connection()
    cursor= cnx.cursor()

    earliest= None

    # Check each query to find the minimum date among them
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
