# analytics/forks_stars.py

from db import get_connection

def count_forks(repo_name, start_dt, end_dt):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM forks
       WHERE repo_name=%s
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    cursor.close()
    conn.close()
    return cnt

def count_stars(repo_name, start_dt, end_dt):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM stars
       WHERE repo_name=%s
         AND starred_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    cursor.close()
    conn.close()
    return cnt
