# fetch_issue_events.py

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

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20, endpoint="issue_events"):
    """
    Basic GET with partial re-try logic + handle_rate_limit_func calls.
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
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/%s] HTTP %d => attempt %d/%d => retry => %s",
                                    endpoint, resp.status_code, attempt, max_retries, url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/%s] HTTP %d => attempt %d => break => %s",
                                    endpoint, resp.status_code, attempt, url)
                    return (resp, False)
                break
            except ConnectionError:
                logging.warning("[deadbird/%s] Connection error => local mini => %s", endpoint, url)
                time.sleep(3)
                local_attempt += 1
        if local_attempt > mini_retry_attempts:
            logging.warning("[deadbird/%s] Exhausted mini => break => %s", endpoint, url)
            return (None, False)
    logging.warning("[deadbird/%s] Exceeded max_retries => give up => %s", endpoint, url)
    return (None, False)

def fetch_issue_events_for_all_issues(conn, owner, repo, enabled,
                                      session, handle_rate_limit_func,
                                      max_retries):
    """
    For each known issue => GET /repos/{owner}/{repo}/issues/{issue_number}/events
    Insert them into 'issue_events'.
    This matches the function your main.py tries to import:
      from fetch_issue_events import fetch_issue_events_for_all_issues
    """
    if enabled == 0:
        logging.info("[issue_events] %s/%s => disabled => skip events", owner, repo)
        return
    repo_name = f"{owner}/{repo}"

    # 1) find all issues from DB
    c=conn.cursor()
    c.execute("""
      SELECT issue_number
      FROM issues
      WHERE repo_name=%s
      ORDER BY issue_number ASC
    """,(repo_name,))
    rows=c.fetchall()
    c.close()

    if not rows:
        logging.info("[issue_events] no issues => skip => %s", repo_name)
        return

    logging.info("[issue_events] Starting => we have %d issues => %s", len(rows), repo_name)
    for (issue_num,) in rows:
        fetch_issue_events_for_one_issue(conn, owner, repo, issue_num,
                                         session, handle_rate_limit_func,
                                         max_retries)
    logging.info("[issue_events] Done => all known issue events => %s", repo_name)

def fetch_issue_events_for_one_issue(conn, owner, repo, issue_number,
                                     session, handle_rate_limit_func,
                                     max_retries):
    """
    Page through /repos/{owner}/{repo}/issues/{issue_number}/events
    Insert them into 'issue_events'.
    """
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/events"
        params={"page":page, "per_page":50}
        (resp, success)=robust_get_page(session, url, params, handle_rate_limit_func, max_retries,
                                        endpoint="issue_events")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for ev_obj in data:
            if store_issue_event(conn, f"{owner}/{repo}", issue_number, ev_obj):
                new_count+=1
        total_inserted+=new_count

        if len(data)<50:
            break
        page+=1

    if total_inserted>0:
        logging.debug("[issue_events] issue #%d => inserted %d events => %s",
                      issue_number, total_inserted, f"{owner}/{repo}")

def store_issue_event(conn, repo_name, issue_num, ev_obj):
    """
    Insert or skip in 'issue_events' table.
    Return True if newly inserted, False otherwise.
    Typically event_id is ev_obj["id"].
    """
    c=conn.cursor()
    event_id=ev_obj.get("id")
    if not event_id:
        c.close()
        return False

    c.execute("""
      SELECT event_id FROM issue_events
      WHERE repo_name=%s AND issue_number=%s AND event_id=%s
    """,(repo_name, issue_num, event_id))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        created_str=ev_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
        raw_str=json.dumps(ev_obj, ensure_ascii=False)

        sql="""
        INSERT INTO issue_events
          (repo_name, issue_number, event_id, created_at, raw_json)
        VALUES
          (%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name, issue_num, event_id, created_dt, raw_str))
        conn.commit()
        c.close()
        return True
