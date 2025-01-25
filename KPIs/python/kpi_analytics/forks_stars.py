# analytics/forks_stars.py
"""
Queries for forks, stars if needed. 
But we've already done them in merges_issues if you'd prefer to unify. 
Here we do them similarly:
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    return mysql.connector.connect(
        host= DB_HOST,
        user= DB_USER,
        password= DB_PASSWORD,
        database= DB_DATABASE
    )

def count_forks(repo, start_dt, end_dt):
    query= """
        SELECT COUNT(*)
        FROM forks
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_stars(repo, start_dt, end_dt):
    query= """
        SELECT COUNT(*)
        FROM stars
        WHERE repo_name=%s
          AND starred_at >= %s
          AND starred_at < %s
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val
