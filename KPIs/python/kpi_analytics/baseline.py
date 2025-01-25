# baseline.py
"""
Finds the oldest creation date for a given repo from issues, pulls, forks, stars, etc.
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def find_oldest_date_for_repo(repo):
    """
    We want the earliest creation date from issues, pulls, forks, stars, etc.
    """
    query= """
        SELECT MIN(all_min) AS oldest_date
        FROM (
            SELECT MIN(created_at) AS all_min FROM issues      WHERE repo_name=%s
            UNION ALL
            SELECT MIN(created_at) FROM pulls                  WHERE repo_name=%s
            UNION ALL
            SELECT MIN(created_at) FROM forks                  WHERE repo_name=%s
            UNION ALL
            SELECT MIN(starred_at) FROM stars                  WHERE repo_name=%s
        ) subq
    """
    cnx= mysql.connector.connect(
        host= DB_HOST,
        user= DB_USER,
        password= DB_PASSWORD,
        database= DB_DATABASE
    )
    cursor= cnx.cursor()
    cursor.execute(query, (repo,repo,repo,repo))
    row= cursor.fetchone()
    cursor.close()
    cnx.close()
    if row and row[0]:
        return row[0]
    return None
