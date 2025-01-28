# fetch_pull_events.py

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
    Basic approach to GET a page with re-tries for 403,429,5xx.
    """
    from requests.exceptions import ConnectionError
    mini_retry_attempts=3
    for attempt in range(1,max_retries+1):
        local_attempt=1
        while local_attempt<=mini_retry_attempts:
            try:
                resp=session.get(url, params=params)
                handle_rate_limit_func(resp)
                if resp.status_code==200:
                    return (resp,True)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[pull_events] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code, attempt, max_retries, url)
                    time.sleep(5)
                else:
                    logging.warning("[pull_events] HTTP %d => attempt %d => break => %s",
                                    resp.status_code, attempt, url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[pull_events] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[pull_events] Exhausted mini => break => %s",url)
            return (None,False)
    logging.warning("[pull_events] Exceeded max_retries => give up => %s",url)
    return (None,False)

def fetch_pull_events_for_all_pulls(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries):
    """
    Single-thread approach:
    1) Query your `pulls` table => list known pull_number for the repo
    2) For each pull_number => fetch /repos/{owner}/{repo}/issues/{pull_number}/events
    3) Insert events into `pull_events` table
    """
    if enabled==0:
        logging.info("[pull_events] %s/%s => disabled => skip all pull events",owner,repo)
        return
    repo_name = f"{owner}/{repo}"

    # 1) find all pulls from DB
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s ORDER BY pull_number ASC",
              (repo_name,))
    pull_rows=c.fetchall()
    c.close()

    if not pull_rows:
        logging.info("[pull_events] no pulls found => skip => %s",repo_name)
        return

    logging.info("[pull_events] Starting => we have %d pulls => %s", len(pull_rows), repo_name)

    for pr_row in pull_rows:
        pr_number=pr_row[0]
        fetch_pull_events_for_one_pr(conn, owner, repo, pr_number,
                                     session, handle_rate_limit_func,
                                     max_retries)

    logging.info("[pull_events] Done => %s => all known pull events fetched", repo_name)

def fetch_pull_events_for_one_pr(conn, owner, repo, pr_number,
                                 session, handle_rate_limit_func,
                                 max_retries):
    """
    For a single PR => GET /repos/{owner}/{repo}/issues/{pr_number}/events
    Insert into pull_events.
    """
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{pr_number}/events"
        params={"page":page,"per_page":50}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for ev_obj in data:
            if store_pull_event(conn, f"{owner}/{repo}", pr_number, ev_obj):
                new_count+=1
        total_inserted+=new_count
        if len(data)<50:
            break
        page+=1
    if total_inserted>0:
        logging.debug("[pull_events] pr #%d => inserted %d events => %s",pr_number,total_inserted,f"{owner}/{repo}")

def store_pull_event(conn, repo_name, pull_num, ev_obj):
    """
    Insert into pull_events table => if not already present
    event_id is typically ev_obj["id"]
    """
    c=conn.cursor()
    event_id=ev_obj.get("id")
    if not event_id:
        c.close()
        return False
    c.execute("""
      SELECT event_id FROM pull_events
      WHERE repo_name=%s AND pull_number=%s AND event_id=%s
    """,(repo_name,pull_num,event_id))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        created_str=ev_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
        raw_str=json.dumps(ev_obj, ensure_ascii=False)

        sql="""
        INSERT INTO pull_events
          (repo_name, pull_number, event_id, created_at, raw_json)
        VALUES
          (%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,pull_num,event_id,created_dt,raw_str))
        conn.commit()
        c.close()
        return True
