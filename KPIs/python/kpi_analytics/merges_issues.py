# analytics/merges_issues.py

from db import get_connection

def count_merged_pulls(repo_name, start_dt, end_dt):
    """
    Count merged pulls by scanning pull_events raw_json for
    \"event\": \"merged\" (with exactly one space).
    Filter on created_at in [start_dt..end_dt].
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Double-escape backslashes => \\\" => \" in final
    query = """
      SELECT COUNT(*)
        FROM pull_events
       WHERE repo_name=%s
         AND raw_json LIKE '%\\"event\\": \\"merged\\"%'
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
    Count closed issues by scanning issue_events raw_json for
    \"event\": \"closed\" (with exactly one space).
    Filter on created_at in [start_dt..end_dt].
    """
    conn = get_connection()
    cursor = conn.cursor()

    query = """
      SELECT COUNT(*)
        FROM issue_events
       WHERE repo_name=%s
         AND raw_json LIKE '%\\"event\\": \\"closed\\"%'
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    closed_count = row[0] if row else 0
    cursor.close()
    conn.close()
    return closed_count

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
