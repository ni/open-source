# fetch_labels.py

import logging
import time
import requests
from datetime import datetime

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

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20, endpoint="labels"):
    from requests.exceptions import ConnectionError
    mini_retry_attempts = 3
    for attempt in range(1, max_retries + 1):
        local_attempt=1
        while local_attempt <= mini_retry_attempts:
            try:
                resp = session.get(url, params=params)
                handle_rate_limit_func(resp)
                if resp.status_code == 200:
                    return (resp, True)
                elif resp.status_code == 304:
                    logging.info("[deadbird/%s-etag] 304 => no new labels => skip" % endpoint)
                    return (resp, False)
                elif resp.status_code in (403, 429, 500, 502, 503, 504):
                    logging.warning("[deadbird/%s-etag] HTTP %d => attempt %d/%d => retry => %s",
                                    endpoint, resp.status_code, attempt, max_retries, url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/%s-etag] HTTP %d => attempt %d => break => %s",
                                    endpoint, resp.status_code, attempt, url)
                    return (resp, False)
                break
            except ConnectionError:
                logging.warning("[deadbird/%s-etag] Connection error => local mini-retry => %s", endpoint, url)
                time.sleep(3)
                local_attempt += 1
        if local_attempt > mini_retry_attempts:
            logging.warning("[deadbird/%s-etag] Exhausted mini => break => %s", endpoint, url)
            return (None, False)
    logging.warning("[deadbird/%s-etag] Exceeded max_retries => give up => %s", endpoint, url)
    return (None, False)

def fetch_labels_single_thread(conn, owner, repo, enabled,
                               session, handle_rate_limit_func,
                               max_retries,
                               use_etags=True):
    """
    Final version that accepts `use_etags`.
    If `use_etags=False`, fallback to old approach scanning from page=1.
    """
    if enabled == 0:
        logging.info("Repo %s/%s => disabled => skip labels", owner, repo)
        return

    endpoint = "labels"
    repo_name = f"{owner}/{repo}"

    if not use_etags:
        labels_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn, owner, repo, endpoint)
    page = 1
    last_page = None
    total_inserted = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/labels"
        params = {"page": page, "per_page": 50}
        if etag_val:
            session.headers["If-None-Match"] = etag_val

        (resp, success) = robust_get_page(
            session, url, params, handle_rate_limit_func,
            max_retries, endpoint=endpoint
        )

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
        for lbl_obj in data:
            if store_label(conn, repo_name, lbl_obj):
                new_count += 1
        total_inserted += new_count

        new_etag = resp.headers.get("ETag")
        if new_etag:
            etag_val = new_etag

        if len(data) < 50:
            break
        page += 1

    # no last_updated usage => pass it unchanged
    update_endpoint_state(conn, owner, repo, endpoint, etag_val, last_upd)
    logging.info("[deadbird/labels-etag] Done => inserted %d => %s", total_inserted, repo_name)

def labels_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/labels-old] => scanning => %s/%s from page=1", owner, repo)
    page = 1
    total_inserted = 0
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/labels"
        params = {"page": page, "per_page": 50}
        (resp, success) = robust_get_page(
            session, url, params, handle_rate_limit_func,
            max_retries, endpoint="labels-old"
        )
        if not success or not resp:
            break

        data = resp.json()
        if not data:
            break

        new_count = 0
        for lbl_obj in data:
            if store_label(conn, f"{owner}/{repo}", lbl_obj):
                new_count += 1
        total_inserted += new_count

        if len(data) < 50:
            break
        page += 1

    logging.info("[deadbird/labels-old] total inserted %d => %s/%s", total_inserted, owner, repo)

def store_label(conn, repo_name, lbl_obj):
    c = conn.cursor()
    lbl_name = lbl_obj["name"]
    c.execute("SELECT label_name FROM repo_labels WHERE repo_name=%s AND label_name=%s",
              (repo_name, lbl_name))
    row = c.fetchone()
    if row:
        update_label(c, conn, repo_name, lbl_obj)
        c.close()
        return False
    else:
        insert_label(c, conn, repo_name, lbl_obj)
        c.close()
        return True

def insert_label(c, conn, repo_name, lbl_obj):
    import json
    name = lbl_obj["name"]
    color = lbl_obj.get("color", "")
    desc = lbl_obj.get("description", "")

    sql = """
    INSERT INTO repo_labels
      (repo_name, label_name, color, description)
    VALUES
      (%s, %s, %s, %s)
    """
    c.execute(sql, (repo_name, name, color, desc))
    conn.commit()

def update_label(c, conn, repo_name, lbl_obj):
    name = lbl_obj["name"]
    color = lbl_obj.get("color", "")
    desc = lbl_obj.get("description", "")

    sql = """
    UPDATE repo_labels
    SET color=%s, description=%s
    WHERE repo_name=%s AND label_name=%s
    """
    c.execute(sql, (color, desc, repo_name, name))
    conn.commit()
