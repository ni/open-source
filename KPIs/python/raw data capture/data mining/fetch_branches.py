# fetch_branches.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

from etags import get_endpoint_state, update_endpoint_state

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

def list_branches_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries,
                                use_etags=True):
    """
    Final version that accepts `use_etags`.
    If `use_etags=False`, fallback to old approach scanning from page=1.
    """
    if enabled == 0:
        logging.info("[deadbird/branches] %s/%s => disabled => skip branches", owner, repo)
        return

    endpoint = "branches"
    repo_name = f"{owner}/{repo}"

    if not use_etags:
        branches_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn, owner, repo, endpoint)
    page = 1
    last_page = None
    total_inserted = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/branches"
        params = {"page": page, "per_page": 50}

        if etag_val:
            session.headers["If-None-Match"] = etag_val

        (resp, success) = robust_get_page(session, url, params,
                                          handle_rate_limit_func, max_retries,
                                          endpoint=endpoint)

        if "If-None-Match" in session.headers:
            del session.headers["If-None-Match"]

        if not success or not resp:
            break

        data = resp.json()
        if not data:
            break

        if last_page is None:
            last_page = get_last_page(resp)

        new_count = 0
        for br in data:
            if store_branch_record(conn, repo_name, br):
                new_count += 1
        total_inserted += new_count

        new_etag = resp.headers.get("ETag")
        if new_etag:
            etag_val = new_etag

        if len(data) < 50:
            break
        page += 1

    # branches doesn't track last_updated => store new ETag, keep last_upd
    update_endpoint_state(conn, owner, repo, endpoint, etag_val, last_upd)
    logging.info("[deadbird/branches-etag] Done => inserted %d => %s", total_inserted, repo_name)

def branches_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/branches-old] => scanning => %s/%s => from page=1", owner, repo)
    page = 1
    total_inserted = 0
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/branches"
        params = {"page": page, "per_page": 50}
        (resp, success) = robust_get_page(session, url, params,
                                          handle_rate_limit_func, max_retries,
                                          endpoint="branches-old")
        if not success or not resp:
            break
        data = resp.json()
        if not data:
            break

        new_count = 0
        for br in data:
            if store_branch_record(conn, f"{owner}/{repo}", br):
                new_count += 1
        total_inserted += new_count

        if len(data) < 50:
            break
        page += 1

    logging.info("[deadbird/branches-old] total inserted %d => %s/%s", total_inserted, owner, repo)

def store_branch_record(conn, repo_name, branch_obj):
    c = conn.cursor()
    branch_name = branch_obj["name"]
    c.execute("""
      SELECT branch_name FROM branches
      WHERE repo_name=%s AND branch_name=%s
    """,(repo_name, branch_name))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        commit_sha = None
        commit_obj = branch_obj.get("commit", {})
        if commit_obj:
            commit_sha = commit_obj.get("sha")
        protected = 1 if branch_obj.get("protected", False) else 0
        raw_str = json.dumps(branch_obj, ensure_ascii=False)

        sql = """
        INSERT INTO branches
          (repo_name, branch_name, commit_sha, protected, raw_json)
        VALUES
          (%s, %s, %s, %s, %s)
        """
        c.execute(sql,(repo_name, branch_name, commit_sha, protected, raw_str))
        conn.commit()
        c.close()
        return True
