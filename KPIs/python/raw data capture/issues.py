# issues.py

def get_issues_for_repo(conn, owner, repo):
    """
    Return a list of (owner, repo, issue_number) for issues in the 'issues' table.
    You can filter by open or recently updated if desired.
    """
    c = conn.cursor()
    reponame = f"{owner}/{repo}"
    c.execute("""
      SELECT issue_number
      FROM issues
      WHERE repo_name=%s
        AND (state='open' OR updated_at >= DATE_SUB(NOW(), INTERVAL 30 DAY))
    """, (reponame,))
    rows = c.fetchall()
    c.close()
    return [(owner, repo, r[0]) for r in rows]

def get_issue_last_id(conn, repo_name, issue_number):
    """
    Return last_event_id from 'issues' table for incremental fetch.
    """
    c = conn.cursor()
    c.execute("""
      SELECT last_event_id
      FROM issues
      WHERE repo_name=%s AND issue_number=%s
    """, (repo_name, issue_number))
    row = c.fetchone()
    c.close()
    if row:
        return row[0]
    return 0

def update_issue_last_id(conn, repo_name, issue_number, new_val):
    """
    Update last_event_id in 'issues' table.
    """
    c = conn.cursor()
    c.execute("""
      UPDATE issues
      SET last_event_id=%s
      WHERE repo_name=%s AND issue_number=%s
    """, (new_val, repo_name, issue_number))
    conn.commit()
    c.close()
