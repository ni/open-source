# analytics/merges_issues.py

from db import get_connection

def count_merged_pulls(repo_name, start_dt, end_dt):
    """
    pull_events => raw_json->'$.event'='merged'
    """
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM pull_events
       WHERE repo_name=%s
         AND JSON_EXTRACT(raw_json, '$.event') = '\"merged\"'
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    merges = row[0] if row else 0
    cursor.close()
    conn.close()
    return merges

def count_closed_issues(repo_name, start_dt, end_dt):
    """
    issue_events => raw_json->'$.event'='closed'
    """
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM issue_events
       WHERE repo_name=%s
         AND JSON_EXTRACT(raw_json, '$.event') = '\"closed\"'
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    cursor.close()
    conn.close()
    return cnt

def count_new_pulls(repo_name, start_dt, end_dt):
    """
    'pulls' => created_at in [start_dt..end_dt]
    """
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM pulls
       WHERE repo_name=%s
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    c = row[0] if row else 0
    cursor.close()
    conn.close()
    return c

def count_new_issues(repo_name, start_dt, end_dt):
    """
    'issues' => created_at
    """
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM issues
       WHERE repo_name=%s
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    c = row[0] if row else 0
    cursor.close()
    conn.close()
    return c
