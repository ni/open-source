# fetch_milestones.py

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
    from requests.exceptions import ConnectionError
    mini_retry_attempts=3
    for attempt in range(1, max_retries + 1):
        local_attempt=1
        while local_attempt <= mini_retry_attempts:
            try:
                resp = session.get(url, params=params)
                handle_rate_limit_func(resp)
                if resp.status_code == 200:
                    return (resp, True)
                elif resp.status_code in (403, 429, 500, 502, 503, 504):
                    logging.warning(
                        "[deadbird/milestones] HTTP %d => attempt %d/%d => retry => %s",
                        resp.status_code, attempt, max_retries, url
                    )
                    time.sleep(5)
                else:
                    logging.warning(
                        "[deadbird/milestones] HTTP %d => attempt %d => break => %s",
                        resp.status_code, attempt, url
                    )
                    return (resp, False)
                break
            except ConnectionError:
                logging.warning("[deadbird/milestones] Connection error => local mini-retry => %s", url)
                time.sleep(3)
                local_attempt += 1
        if local_attempt > mini_retry_attempts:
            logging.warning("[deadbird/milestones] Exhausted mini-retry => break => %s", url)
            return (None, False)
    logging.warning("[deadbird/milestones] Exceeded max_retries => give up => %s", url)
    return (None, False)


def list_milestones_single_thread(conn, owner, repo, enabled,
                                  session, handle_rate_limit_func,
                                  max_retries):
    """
    Replaces the function name that main.py is trying to import.

    Single-thread approach: fetch all milestones (state=all),
    store them in the repo_milestones table, update if they exist.
    """
    if enabled == 0:
        logging.info("Repo %s/%s => disabled => skip milestones", owner, repo)
        return

    repo_name = f"{owner}/{repo}"
    page = 1
    last_page = None
    total_inserted = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/milestones"
        params = {"state": "all", "page": page, "per_page": 30}
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
            total_items = last_page * 30

        new_count = 0
        for mst in data:
            if store_milestone(conn, repo_name, mst):
                new_count += 1

        total_inserted += new_count

        if last_page:
            progress = (page / last_page) * 100.0
            logging.debug("[deadbird/milestones] page=%d/%d => %.3f%% => inserted %d => %s",
                          page, last_page, progress, new_count, repo_name)
            if total_items > 0:
                logging.debug("[deadbird/milestones] so far %d out of approx %d => %s",
                              total_inserted, total_items, repo_name)
        else:
            logging.debug("[deadbird/milestones] page=%d => inserted %d => no last_page => %s",
                          page, new_count, repo_name)

        if len(data) < 30:
            break

        page += 1

    logging.info("[deadbird/milestones] Done => total inserted %d => %s", total_inserted, repo_name)


def store_milestone(conn, repo_name, mst_obj):
    c = conn.cursor()
    milestone_id = mst_obj["id"]
    c.execute("""
      SELECT milestone_id FROM repo_milestones
      WHERE repo_name=%s AND milestone_id=%s
    """, (repo_name, milestone_id))
    row = c.fetchone()
    if row:
        update_milestone(c, conn, repo_name, milestone_id, mst_obj)
        c.close()
        return False
    else:
        insert_milestone(c, conn, repo_name, milestone_id, mst_obj)
        c.close()
        return True


def insert_milestone(c, conn, repo_name, milestone_id, mst_obj):
    import json

    title = mst_obj.get("title", "")
    state = mst_obj.get("state", "")
    desc = mst_obj.get("description", "")
    due_str = mst_obj.get("due_on")
    due_on = None
    if due_str:
        due_on = datetime.strptime(due_str, "%Y-%m-%dT%H:%M:%SZ")

    created_str = mst_obj.get("created_at")
    created_dt = None
    if created_str:
        created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

    updated_str = mst_obj.get("updated_at")
    updated_dt = None
    if updated_str:
        updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ")

    closed_str = mst_obj.get("closed_at")
    closed_dt = None
    if closed_str:
        closed_dt = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ")

    raw_str = json.dumps(mst_obj, ensure_ascii=False)

    sqlmst = """
    INSERT INTO repo_milestones
      (repo_name, milestone_id, title, state, description, due_on,
       created_at, updated_at, closed_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sqlmst, (repo_name, milestone_id, title, state, desc, due_on,
                       created_dt, updated_dt, closed_dt, raw_str))
    conn.commit()


def update_milestone(c, conn, repo_name, milestone_id, mst_obj):
    import json

    title = mst_obj.get("title", "")
    state = mst_obj.get("state", "")
    desc = mst_obj.get("description", "")
    due_str = mst_obj.get("due_on")
    due_on = None
    if due_str:
        due_on = datetime.strptime(due_str, "%Y-%m-%dT%H:%M:%SZ")

    created_str = mst_obj.get("created_at")
    created_dt = None
    if created_str:
        created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

    updated_str = mst_obj.get("updated_at")
    updated_dt = None
    if updated_str:
        updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ")

    closed_str = mst_obj.get("closed_at")
    closed_dt = None
    if closed_str:
        closed_dt = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ")

    raw_str = json.dumps(mst_obj, ensure_ascii=False)

    sqlmst = """
    UPDATE repo_milestones
    SET title=%s, state=%s, description=%s, due_on=%s,
        created_at=%s, updated_at=%s, closed_at=%s, raw_json=%s
    WHERE repo_name=%s AND milestone_id=%s
    """
    c.execute(sqlmst, (
        title, state, desc, due_on,
        created_dt, updated_dt, closed_dt, raw_str,
        repo_name, milestone_id
    ))
    conn.commit()
