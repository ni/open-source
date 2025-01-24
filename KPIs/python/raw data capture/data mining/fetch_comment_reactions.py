# fetch_comment_reactions.py

import logging
import time
import requests
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def get_last_page(resp):
    link_header = resp.headers.get("Link")
    if not link_header:
        return None
    parts = link_header.split(',')
    for part in parts:
        if 'rel="last"' in part:
            import re
            match = re.search(r'[?&]page=(\d+)', part)
            if match:
                return int(match.group(1))
    return None

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    mini_retry_attempts = 3
    for attempt in range(1, max_retries + 1):
        local_attempt = 1
        while local_attempt <= mini_retry_attempts:
            try:
                resp = session.get(url, params=params)
                handle_rate_limit_func(resp)
                if resp.status_code == 200:
                    return (resp, True)
                elif resp.status_code in (403, 429, 500, 502, 503, 504):
                    logging.warning(
                        "HTTP %d => attempt %d/%d => retry => %s",
                        resp.status_code, attempt, max_retries, url
                    )
                    time.sleep(5)
                else:
                    logging.warning(
                        "HTTP %d => attempt %d => break => %s",
                        resp.status_code, attempt, url
                    )
                    return (resp, False)
                break
            except requests.exceptions.ConnectionError:
                logging.warning("Connection error => local mini-retry => %s", url)
                time.sleep(3)
                local_attempt += 1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s", url)
            return (None, False)
    logging.warning("Exceeded max_retries => give up => %s", url)
    return (None, False)

def get_max_reaction_id_for_comment(conn, repo_name, issue_number, comment_id):
    c = conn.cursor()
    c.execute("""
        SELECT MAX(reaction_id)
        FROM comment_reactions
        WHERE repo_name=%s AND issue_number=%s AND comment_id=%s
    """, (repo_name, issue_number, comment_id))
    row = c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def fetch_comment_reactions_for_all_comments(conn, owner, repo, enabled,
                                            session, handle_rate_limit_func,
                                            max_retries):
    """
    Loops over all comments in 'issue_comments' table for this repo,
    fetches each comment's reactions, skipping older reaction_id.
    """
    if enabled == 0:
        logging.info("Repo %s/%s => disabled => skip comment_reactions", owner, repo)
        return
    repo_name = f"{owner}/{repo}"

    # get all known (issue_number, comment_id) from issue_comments
    c = conn.cursor()
    c.execute("""
        SELECT issue_number, comment_id
        FROM issue_comments
        WHERE repo_name=%s
    """,(repo_name,))
    rows = c.fetchall()
    c.close()

    for (issue_number, comment_id) in rows:
        fetch_comment_reactions_single_thread(
            conn, repo_name,
            issue_number, comment_id,
            enabled,
            session, handle_rate_limit_func,
            max_retries
        )

def fetch_comment_reactions_single_thread(conn, repo_name,
                                         issue_number, comment_id,
                                         enabled,
                                         session,
                                         handle_rate_limit_func,
                                         max_retries):
    if enabled == 0:
        logging.info("%s => disabled => skip => comment_reactions => issue #%d => comment_id=%d",
                     repo_name, issue_number, comment_id)
        return

    highest_rid = get_max_reaction_id_for_comment(conn, repo_name, issue_number, comment_id)
    page = 1
    last_page = None

    # The endpoint => GET /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions
    old_accept = session.headers.get("Accept","")
    session.headers["Accept"] = "application/vnd.github.squirrel-girl-preview+json"

    while True:
        url = f"https://api.github.com/repos/{repo_name}/issues/comments/{comment_id}/reactions"
        params = {"page": page, "per_page": 100}
        (resp, success) = robust_get_page(session, url, params, handle_rate_limit_func, max_retries)
        if not success:
            logging.warning(
                "Comment Reactions => skip => page=%d => comment_id=%d => %s => issue #%d",
                page, comment_id, repo_name, issue_number
            )
            break
        data = resp.json()
        if not data:
            break

        if last_page is None:
            last_page = get_last_page(resp)
        if last_page:
            progress = (page / last_page) * 100
            logging.debug(
                f"[DEBUG] comment_reactions => page={page}/{last_page} => {progress:.3f}%% => {repo_name} => issue #{issue_number} => comment_id={comment_id}"
            )

        new_count = 0
        for reac in data:
            reac_id = reac["id"]
            if reac_id <= highest_rid:
                continue
            cstr = reac.get("created_at")
            cdt = None
            if cstr:
                cdt = datetime.strptime(cstr, "%Y-%m-%dT%H:%M:%SZ")
            insert_comment_reaction(conn, repo_name, issue_number, comment_id, reac_id, cdt, reac)
            new_count += 1
            if reac_id > highest_rid:
                highest_rid = reac_id

        if len(data) < 100:
            break
        page += 1

    session.headers["Accept"] = old_accept

def insert_comment_reaction(conn, repo_name, issue_number, comment_id,
                            reac_id, created_dt, reac_json):
    import json
    raw_str = json.dumps(reac_json, ensure_ascii=False)
    c = conn.cursor()
    sql = """
    INSERT INTO comment_reactions
      (repo_name, issue_number, comment_id, reaction_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql, (repo_name, issue_number, comment_id, reac_id, created_dt, raw_str))
    conn.commit()
    c.close()
