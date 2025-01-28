# fetch_events.py

import logging
import time
import requests
from datetime import datetime

def get_last_page(resp):
    link_header=resp.headers.get("Link")
    if not link_header:
        return None
    parts=link_header.split(',')
    import re
    for p in parts:
        if 'rel="last"' in p:
            m=re.search(r'[?&]page=(\d+)', p)
            if m:
                return int(m.group(1))
    return None

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
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
                    logging.warning("[deadbird/events] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/events] HTTP %d => break => %s",resp.status_code,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/events] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/events] Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/events] Exceeded max_retries => give up => %s",url)
    return (None,False)

def fetch_issue_events_for_all_issues(conn, owner, repo, enabled,
                                      session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_events",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(repo_name,))
    rows=c.fetchall()
    c.close()

    for (issue_num,) in rows:
        fetch_issue_events_single_thread(conn,owner,repo,issue_num,enabled,
                                         session,handle_rate_limit_func,max_retries)

def fetch_issue_events_single_thread(conn, owner, repo, issue_num, enabled,
                                     session, handle_rate_limit_func,
                                     max_retries):
    if enabled==0:
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_num}/events"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*100

        new_count=0
        for ev in data:
            if store_issue_event(conn,repo_name,issue_num,ev):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/issue_events] issue#%d => page=%d/%d => %.4f%% => inserted %d => %s",
                          issue_num,page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/issue_events] => so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/issue_events] issue#%d => page=%d => inserted %d => no last_page => %s",
                          issue_num,page,new_count,repo_name)

        if len(data)<100:
            break
        page+=1

def store_issue_event(conn, repo_name, issue_num, ev_obj):
    c=conn.cursor()
    event_id=ev_obj["id"]
    c.execute("""
      SELECT event_id FROM issue_events
      WHERE repo_name=%s AND issue_number=%s AND event_id=%s
    """,(repo_name,issue_num,event_id))
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
        INSERT INTO issue_events
          (repo_name, issue_number, event_id, created_at, raw_json)
        VALUES
          (%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,issue_num,event_id,created_dt,raw_str))
        conn.commit()
        c.close()
        return True

def fetch_pull_events_for_all_pulls(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_events",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s",(repo_name,))
    rows=c.fetchall()
    c.close()

    for (pull_num,) in rows:
        fetch_pull_events_single_thread(conn,owner,repo,pull_num,enabled,
                                        session,handle_rate_limit_func,max_retries)

def fetch_pull_events_single_thread(conn, owner, repo, pull_num, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries):
    if enabled==0:
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{pull_num}/events"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*100

        new_count=0
        for ev in data:
            if store_pull_event(conn,repo_name,pull_num,ev):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/pull_events] PR#%d => page=%d/%d => %.4f%% => inserted %d => %s",
                          pull_num,page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/pull_events] => so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/pull_events] PR#%d => page=%d => inserted %d => no last_page => %s",
                          pull_num,page,new_count,repo_name)

        if len(data)<100:
            break
        page+=1

def store_pull_event(conn, repo_name, pull_num, ev_obj):
    c=conn.cursor()
    event_id=ev_obj["id"]
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
