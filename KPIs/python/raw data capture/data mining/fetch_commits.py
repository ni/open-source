# fetch_commits.py

import logging
import time
import requests
from datetime import datetime

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

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    """
    Reusable function to GET a page with retry logic + handle_rate_limit_func.
    """
    from requests.exceptions import ConnectionError
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
                    logging.warning("[deadbird/commits] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code, attempt, max_retries, url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/commits] HTTP %d => attempt %d => break => %s",
                                    resp.status_code, attempt, url)
                    return (resp, False)
                break
            except ConnectionError:
                logging.warning("[deadbird/commits] Connection error => local mini-retry => %s", url)
                time.sleep(3)
                local_attempt += 1
        if local_attempt > mini_retry_attempts:
            logging.warning("[deadbird/commits] Exhausted mini-retry => break => %s", url)
            return (None, False)
    logging.warning("[deadbird/commits] Exceeded max_retries => give up => %s", url)
    return (None, False)

def list_commits_single_thread(conn, owner, repo, enabled, baseline_dt,
                               session, handle_rate_limit_func, max_retries):
    """
    Single-thread fetch of all commits from oldest to newest, 
    with date-based skipping if baseline_dt is provided.
    Called by main.py as: from fetch_commits import list_commits_single_thread
    """
    if enabled == 0:
        logging.info("Repo %s/%s => disabled => skip commits", owner, repo)
        return
    repo_name = f"{owner}/{repo}"
    page = 1
    last_page = None
    total_inserted = 0

    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    params = {
        "page": page,
        "per_page": 50,
        "sort": "committer-date",
        "direction": "asc"
    }
    if baseline_dt:
        # date-based skip => 'since' param
        params["since"] = baseline_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        params["page"] = page
        (resp, success) = robust_get_page(session, url, params, handle_rate_limit_func, max_retries)
        if not success or not resp:
            break
        data = resp.json()
        if not data:
            break
        if last_page is None:
            last_page = get_last_page(resp)
        total_items = 0
        if last_page:
            total_items = last_page * 50

        new_count = 0
        for commit_obj in data:
            if store_commit_record(conn, repo_name, commit_obj):
                new_count += 1
        total_inserted += new_count

        if last_page:
            progress = (page / last_page) * 100.0
            logging.debug("[deadbird/commits] page=%d/%d => %.3f%% => inserted %d => %s",
                          page, last_page, progress, new_count, repo_name)
            if total_items > 0:
                logging.debug("[deadbird/commits] => total so far %d out of ~%d => %s",
                              total_inserted, total_items, repo_name)
        else:
            logging.debug("[deadbird/commits] page=%d => inserted %d => no last_page => %s",
                          page, new_count, repo_name)

        if len(data) < 50:
            break
        page += 1

    logging.info("[deadbird/commits] Done => total inserted %d => %s", total_inserted, repo_name)

def store_commit_record(conn, repo_name, commit_obj):
    """
    Insert or update a single commit. Return True if new inserted, False if updated/skipped.
    commit_obj has 'sha', 'commit' sub-object with 'committer' date, etc.
    """
    c = conn.cursor()
    sha = commit_obj["sha"]
    c.execute("SELECT sha FROM commits WHERE repo_name=%s AND sha=%s", (repo_name, sha))
    row = c.fetchone()
    if row:
        # update => not new
        update_commit_record(c, conn, repo_name, sha, commit_obj)
        c.close()
        return False
    else:
        insert_commit_record(c, conn, repo_name, sha, commit_obj)
        c.close()
        return True

def insert_commit_record(c, conn, repo_name, sha, commit_obj):
    import json
    commit_info = commit_obj.get("commit", {})
    author_login = (commit_obj.get("author") or {}).get("login")
    committer_login = (commit_obj.get("committer") or {}).get("login")
    message = commit_info.get("message", "")
    date_str = commit_info.get("committer", {}).get("date")
    commit_date = None
    if date_str:
        commit_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

    raw_str = json.dumps(commit_obj, ensure_ascii=False)
    sql = """
    INSERT INTO commits
      (repo_name, sha, author_login, committer_login,
       commit_message, commit_date, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sql, (repo_name, sha, author_login, committer_login, message, commit_date, raw_str))
    conn.commit()

def update_commit_record(c, conn, repo_name, sha, commit_obj):
    import json
    commit_info = commit_obj.get("commit", {})
    author_login = (commit_obj.get("author") or {}).get("login")
    committer_login = (commit_obj.get("committer") or {}).get("login")
    message = commit_info.get("message", "")
    date_str = commit_info.get("committer", {}).get("date")
    commit_date = None
    if date_str:
        commit_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

    raw_str = json.dumps(commit_obj, ensure_ascii=False)
    sql = """
    UPDATE commits
    SET author_login=%s, committer_login=%s,
        commit_message=%s, commit_date=%s, raw_json=%s
    WHERE repo_name=%s AND sha=%s
    """
    c.execute(sql, (author_login, committer_login, message, commit_date, raw_str, repo_name, sha))
    conn.commit()
