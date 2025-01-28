# fetch_actions_runs.py

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

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20, endpoint="actions_runs"):
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
                    logging.info("[deadbird/%s-etag] 304 => no new actions runs => skip" % endpoint)
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
                logging.warning("[deadbird/%s-etag] Connection error => mini-retry => %s", endpoint, url)
                time.sleep(3)
                local_attempt += 1
        if local_attempt > mini_retry_attempts:
            logging.warning("[deadbird/%s-etag] Exhausted mini-retry => break => %s", endpoint, url)
            return (None, False)
    logging.warning("[deadbird/%s-etag] Exceeded max_retries => give up => %s", endpoint, url)
    return (None, False)

def list_actions_runs_single_thread(conn, owner, repo, enabled,
                                   session, handle_rate_limit_func,
                                   max_retries,
                                   use_etags=True):
    if enabled == 0:
        logging.info("[deadbird/actions_runs] %s/%s => disabled => skip", owner, repo)
        return
    endpoint = "actions_runs"
    repo_name = f"{owner}/{repo}"

    if not use_etags:
        actions_runs_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn, owner, repo, endpoint)
    page = 1
    last_page = None
    total_inserted = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        params = {"page": page, "per_page": 50}
        if etag_val:
            session.headers["If-None-Match"] = etag_val

        (resp, success) = robust_get_page(session, url, params, handle_rate_limit_func, max_retries, endpoint=endpoint)

        if "If-None-Match" in session.headers:
            del session.headers["If-None-Match"]
        if not success or not resp:
            break

        data = resp.json()
        if not data:
            break
        runs = data.get("workflow_runs", [])

        if last_page is None:
            last_page = get_last_page(resp)

        new_count = 0
        for run in runs:
            if store_action_run(conn, repo_name, run):
                new_count += 1
        total_inserted += new_count

        new_etag = resp.headers.get("ETag")
        if new_etag:
            etag_val = new_etag

        if len(runs) < 50:
            break
        page += 1

    # code doesn't store last_upd for actions_runs, so pass it unchanged
    update_endpoint_state(conn, owner, repo, endpoint, etag_val, last_upd)
    logging.info("[deadbird/actions_runs-etag] Done => inserted %d => %s", total_inserted, repo_name)

def actions_runs_old_approach(conn, owner, repo,
                              session, handle_rate_limit_func,
                              max_retries):
    logging.info("[deadbird/actions_runs-old] scanning => %s/%s => from page=1", owner, repo)
    page = 1
    total_inserted = 0
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        params = {"page": page, "per_page": 50}
        (resp, success) = robust_get_page(session, url, params, handle_rate_limit_func, max_retries, endpoint="actions_runs-old")
        if not success or not resp:
            break
        data = resp.json()
        if not data:
            break
        runs = data.get("workflow_runs", [])
        new_count = 0
        for run in runs:
            if store_action_run(conn, f"{owner}/{repo}", run):
                new_count += 1
        total_inserted += new_count
        if len(runs) < 50:
            break
        page += 1

    logging.info("[deadbird/actions_runs-old] total inserted %d => %s/%s", total_inserted, owner, repo)

def store_action_run(conn, repo_name, run_obj):
    c = conn.cursor()
    run_id = run_obj["id"]
    c.execute("""
      SELECT run_id FROM actions_runs
      WHERE repo_name=%s AND run_id=%s
    """, (repo_name, run_id))
    row = c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        head_branch = run_obj.get("head_branch", "")
        head_sha = run_obj.get("head_sha", "")
        event_type = run_obj.get("event", "")
        status = run_obj.get("status", "")
        conclusion = run_obj.get("conclusion", "")
        workflow_id = run_obj.get("workflow_id", 0)

        created_str = run_obj.get("created_at")
        created_dt = None
        if created_str:
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

        updated_str = run_obj.get("updated_at")
        updated_dt = None
        if updated_str:
            updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ")

        run_started_str = run_obj.get("run_started_at")
        run_started_dt = None
        if run_started_str:
            run_started_dt = datetime.strptime(run_started_str, "%Y-%m-%dT%H:%M:%SZ")

        raw_str = json.dumps(run_obj, ensure_ascii=False)
        sqlrun = """
        INSERT INTO actions_runs
          (repo_name, run_id, head_branch, head_sha, event_type, status,
           conclusion, workflow_id, created_at, updated_at, run_started_at, raw_json)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        c.execute(sqlrun, (repo_name, run_id, head_branch, head_sha,
                           event_type, status, conclusion, workflow_id,
                           created_dt, updated_dt, run_started_dt, raw_str))
        conn.commit()
        c.close()
        return True
