# baseline.py

from db import get_connection

def find_oldest_date_for_repo(repo_name):
    """
    Return earliest creation date from issues, pulls, forks, stars,
    issue_comments, issue_events, pull_events, comment_reactions, issue_reactions.
    watchers = excluded.
    """
    conn = get_connection()
    cursor = conn.cursor()

    queries = [
        ("SELECT MIN(created_at) FROM issues WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM pulls WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM forks WHERE repo_name=%s", repo_name),
        ("SELECT MIN(starred_at) FROM stars WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM issue_comments WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM issue_events WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM pull_events WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM comment_reactions WHERE repo_name=%s", repo_name),
        ("SELECT MIN(created_at) FROM issue_reactions WHERE repo_name=%s", repo_name)
    ]

    oldest = None
    for q, prm in queries:
        cursor.execute(q, (prm,))
        row = cursor.fetchone()
        if row and row[0]:
            dt = row[0]
            if oldest is None or dt < oldest:
                oldest = dt

    cursor.close()
    conn.close()
    return oldest
