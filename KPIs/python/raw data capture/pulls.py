# pulls.py

def get_pulls_for_repo(conn, owner, repo):
    """
    Return a list of (owner, repo, pull_number) from 'pulls' table.
    Filter by open or recent if desired.
    """
    c = conn.cursor()
    reponame = f"{owner}/{repo}"
    c.execute("""
      SELECT pull_number
      FROM pulls
      WHERE repo_name=%s
        AND (state='open' OR updated_at >= DATE_SUB(NOW(), INTERVAL 30 DAY))
    """, (reponame,))
    rows = c.fetchall()
    c.close()
    return [(owner, repo, r[0]) for r in rows]

def get_pull_last_id(conn, repo_name, pull_number):
    """
    Return last_event_id from 'pulls' table for incremental fetch.
    """
    c = conn.cursor()
    c.execute("""
      SELECT last_event_id
      FROM pulls
      WHERE repo_name=%s AND pull_number=%s
    """, (repo_name, pull_number))
    row = c.fetchone()
    c.close()
    if row:
        return row[0]
    return 0

def update_pull_last_id(conn, repo_name, pull_number, new_val):
    """
    Update last_event_id in 'pulls' table.
    """
    c = conn.cursor()
    c.execute("""
      UPDATE pulls
      SET last_event_id=%s
      WHERE repo_name=%s AND pull_number=%s
    """, (new_val, repo_name, pull_number))
    conn.commit()
    c.close()
