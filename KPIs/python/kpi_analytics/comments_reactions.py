# analytics/comments_reactions.py

from db import get_connection

def count_issue_comments(repo_name, start_dt, end_dt):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*) 
        FROM issue_comments
       WHERE repo_name=%s
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    cursor.close()
    conn.close()
    return cnt

def count_comment_reactions(repo_name, start_dt, end_dt):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*)
        FROM comment_reactions
       WHERE repo_name=%s
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    cursor.close()
    conn.close()
    return cnt

def count_issue_reactions(repo_name, start_dt, end_dt):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
      SELECT COUNT(*) 
        FROM issue_reactions
       WHERE repo_name=%s
         AND created_at BETWEEN %s AND %s
    """
    cursor.execute(query, (repo_name, start_dt, end_dt))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    cursor.close()
    conn.close()
    return cnt

def count_all_reactions(repo_name, start_dt, end_dt):
    ccr = count_comment_reactions(repo_name, start_dt, end_dt)
    cir = count_issue_reactions(repo_name, start_dt, end_dt)
    return ccr + cir
