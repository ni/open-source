# fetch_comment_reactions.py

import logging
from datetime import datetime

def fetch_issue_comment_reactions(owner, repo, issue_number,
                                  session, conn,
                                  handle_rate_limit_func=None):
    """
    For each comment in /issues/{issue_number}/comments,
    fetch reactions => store in comment_reactions (resource_type='issue_comment').
    """
    c = conn.cursor()
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"
        params = {"page": page, "per_page": 50}
        resp = session.get(url, params=params)
        if handle_rate_limit_func:
            handle_rate_limit_func(resp)
        if resp.status_code != 200:
            logging.warning("Issue comment => HTTP %d => stop %s/%s#%d",
                            resp.status_code, owner, repo, issue_number)
            break

        data = resp.json()
        if not data:
            break

        for cmt in data:
            cmt_id = cmt["id"]
            reac_url = f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{cmt_id}/reactions"
            old_accept = session.headers.get("Accept","")
            session.headers["Accept"] = "application/vnd.github.squirrel-girl-preview+json"
            reac_resp = session.get(reac_url)
            if handle_rate_limit_func:
                handle_rate_limit_func(reac_resp)
            session.headers["Accept"] = old_accept

            if reac_resp.status_code == 200:
                reac_data = reac_resp.json()
                total_cnt = len(reac_data)
                sql = """
                INSERT INTO comment_reactions
                  (repo_name, resource_type, comment_id, total_reactions, raw_json)
                VALUES
                  (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  total_reactions=VALUES(total_reactions),
                  raw_json=VALUES(raw_json)
                """
                c.execute(sql, (
                    f"{owner}/{repo}",
                    "issue_comment",
                    cmt_id,
                    total_cnt,
                    reac_data
                ))
                conn.commit()

        if len(data) < 50:
            break
        page += 1
    c.close()
