# fetch_comment_reactions.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

def get_last_page(resp):
    link_header = resp.headers.get("Link")
    if not link_header:
        return None
    parts = link_header.split(',')
    import re
    for p in parts:
        if 'rel="last"' in p:
            m = re.search(r'[?&]page=(\d+)', p)
            if m:
                return int(m.group(1))
    return None

def fetch_issue_comment_reactions_for_all_comments(conn, owner, repo, enabled,
                                                   session, handle_rate_limit_func,
                                                   max_retries):
    """
    For each comment in 'issue_comments' => GET /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions
    Insert them into 'comment_reactions' or 'issue_comment_reactions' table.
    Accept header needed => 'application/vnd.github.squirrel-girl-preview+json'
    """
    if enabled==0:
        logging.info("[issue_comment_reactions] %s/%s => disabled => skip", owner, repo)
        return

    repo_name = f"{owner}/{repo}"

    # 1) Query DB for known issue comments
    c=conn.cursor()
    c.execute("""
      SELECT issue_number, comment_id
      FROM issue_comments
      WHERE repo_name=%s
      ORDER BY issue_number, comment_id
    """, (repo_name,))
    rows=c.fetchall()
    c.close()
    if not rows:
        logging.info("[issue_comment_reactions] No issue comments => skip => %s", repo_name)
        return

    logging.info("[issue_comment_reactions] Starting => we have %d issue comments => %s", len(rows), repo_name)

    # Must set Accept header => 'application/vnd.github.squirrel-girl-preview+json'
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"

    for (issue_num, cmt_id) in rows:
        fetch_comment_reactions_single_thread(conn, owner, repo, issue_num, cmt_id,
                                              session, handle_rate_limit_func,
                                              max_retries)
    # restore old Accept
    session.headers["Accept"]=old_accept

    logging.info("[issue_comment_reactions] Done => all known issue comment reactions => %s", repo_name)

def fetch_comment_reactions_single_thread(conn, owner, repo, issue_number, comment_id,
                                          session, handle_rate_limit_func,
                                          max_retries):
    """
    GET /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions
    Insert them => comment_reactions or issue_comment_reactions table.
    """
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{comment_id}/reactions"
        params={"page":page,"per_page":50}
        (resp, success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,
                                        endpoint="issue_comment_reactions")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for reac_obj in data:
            if insert_comment_reaction(conn, f"{owner}/{repo}", issue_number, comment_id, reac_obj):
                new_count+=1
        total_inserted+=new_count
        if len(data)<50:
            break
        page+=1
    if total_inserted>0:
        logging.debug("[issue_comment_reactions] issue #%d => comment_id=%d => inserted %d => %s",
                      issue_number, comment_id, total_inserted, f"{owner}/{repo}")

def insert_comment_reaction(conn, repo_name, issue_number, comment_id, reac_obj):
    """
    Insert or skip into 'comment_reactions' or 'issue_comment_reactions'.
    Suppose the table is 'issue_comment_reactions'.
    Fields => reaction_id, created_at, raw_json
    """
    c=conn.cursor()
    reaction_id = reac_obj.get("id")
    if not reaction_id:
        c.close()
        return False

    c.execute("""
      SELECT reaction_id
      FROM issue_comment_reactions
      WHERE repo_name=%s AND issue_number=%s AND comment_id=%s AND reaction_id=%s
    """,(repo_name, issue_number, comment_id, reaction_id))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        created_str = reac_obj.get("created_at")
        created_dt = None
        if created_str:
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
        raw_str = json.dumps(reac_obj, ensure_ascii=False)

        sql = """
        INSERT INTO issue_comment_reactions
         (repo_name, issue_number, comment_id, reaction_id, created_at, raw_json)
        VALUES
         (%s,%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name, issue_number, comment_id, reaction_id, created_dt, raw_str))
        conn.commit()
        c.close()
        return True
