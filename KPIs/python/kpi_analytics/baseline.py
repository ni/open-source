# baseline.py
"""
Finds earliest creation date across multiple tables for a repo,
plus a helper to find earliest date for a single table/column if needed.
No lines omitted.
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def find_oldest_date_for_repo(repo):
    """
    Returns the absolute earliest date among issues, pulls, forks, stars for that repo.
    We'll omit any other tables for this function, if you prefer a broader approach, add them.
    """
    query= """
        SELECT MIN(all_min) AS oldest_date
        FROM (
            SELECT MIN(created_at) AS all_min FROM issues WHERE repo_name=%s
            UNION
            SELECT MIN(created_at) AS all_min FROM pulls WHERE repo_name=%s
            UNION
            SELECT MIN(created_at) AS all_min FROM forks WHERE repo_name=%s
            UNION
            SELECT MIN(starred_at) AS all_min FROM stars WHERE repo_name=%s
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

def get_earliest_date_for_table(repo, table_name, date_column):
    """
    For per-table earliest approach: returns earliest date in table_name's date_column for the given repo.
    If no data => returns None.
    """
    q= f"SELECT MIN({date_column}) FROM {table_name} WHERE repo_name=%s"
    cnx= mysql.connector.connect(
        host= DB_HOST,
        user= DB_USER,
        password= DB_PASSWORD,
        database= DB_DATABASE
    )
    cursor= cnx.cursor()
    cursor.execute(q, (repo,))
    row= cursor.fetchone()
    cursor.close()
    cnx.close()
    if row and row[0]:
        return row[0]
    return None
