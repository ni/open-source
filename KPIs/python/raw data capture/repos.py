# repos.py

def get_enabled_repos(conn):
    """
    Return a list of (owner, repo) from 'repos' table where enabled=1.
    """
    c = conn.cursor()
    c.execute("SELECT owner, repo FROM repos WHERE enabled=1")
    rows = c.fetchall()
    c.close()
    results = []
    for (ow, rp) in rows:
        results.append((ow, rp))
    return results
