# fetch_events.py
import logging
import time
import requests
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
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
                    logging.warning("HTTP %d => attempt %d/%d => re-try => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except requests.exceptions.ConnectionError:
                logging.warning("Conn error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def get_last_event_id_for_issue(conn, repo_name, issue_num):
    c=conn.cursor()
    c.execute("SELECT last_event_id FROM issues WHERE repo_name=%s AND issue_number=%s",
              (repo_name,issue_num))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def set_last_event_id_for_issue(conn, repo_name, issue_num, new_val):
    c=conn.cursor()
    c.execute("""
    UPDATE issues
    SET last_event_id=%s
    WHERE repo_name=%s AND issue_number=%s
    """,(new_val,repo_name,issue_num))
    conn.commit()
    c.close()

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
        fetch_issue_events_single_thread(conn,repo_name,issue_num,
                                         enabled,session,
                                         handle_rate_limit_func,max_retries)

def fetch_issue_events_single_thread(conn, repo_name, issue_num,
                                     enabled, session,
                                     handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s => disabled => skip => issue_events => #%d",repo_name,issue_num)
        return
    last_eid=get_last_event_id_for_issue(conn,repo_name,issue_num)
    highest_eid=last_eid
    page=1
    while True:
        url=f"https://api.github.com/repos/{repo_name}/issues/{issue_num}/events"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for evt in data:
            eid=evt["id"]
            if eid<=last_eid:
                continue
            cstr=evt.get("created_at")
            cdt=None
            if cstr:
                cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            insert_issue_event_record(conn,repo_name,issue_num,eid,cdt,evt)
            new_count+=1
            if eid>highest_eid:
                highest_eid=eid
        if new_count<100:
            break
        page+=1
    if highest_eid>last_eid:
        set_last_event_id_for_issue(conn,repo_name,issue_num,highest_eid)

def insert_issue_event_record(conn, repo_name, issue_num, event_id, created_dt, evt_json):
    import json
    raw_str=json.dumps(evt_json, ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO issue_events
      (repo_name, issue_number, event_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,issue_num,event_id,created_dt,raw_str))
    conn.commit()
    c.close()

# same approach for pull events => skip event_id <= last_event_id in 'pulls'
def get_last_event_id_for_pull(conn, repo_name, pull_num):
    c=conn.cursor()
    c.execute("SELECT last_event_id FROM pulls WHERE repo_name=%s AND pull_number=%s",(repo_name,pull_num))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def set_last_event_id_for_pull(conn, repo_name, pull_num, new_val):
    c=conn.cursor()
    c.execute("""
    UPDATE pulls
    SET last_event_id=%s
    WHERE repo_name=%s AND pull_number=%s
    """,(new_val,repo_name,pull_num))
    conn.commit()
    c.close()

def fetch_pull_events_for_all_pulls(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_events",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s",(repo_name,))
    rows=c.fetchall()
    c.close()
    for (pull_num,) in rows:
        fetch_pull_events_single_thread(conn,repo_name,pull_num,
                                        enabled,session,
                                        handle_rate_limit_func,max_retries)

def fetch_pull_events_single_thread(conn, repo_name, pull_num,
                                    enabled, session,
                                    handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s => disabled => skip => pull_events => #%d",repo_name,pull_num)
        return
    last_eid=get_last_event_id_for_pull(conn,repo_name,pull_num)
    highest_eid=last_eid
    page=1
    while True:
        url=f"https://api.github.com/repos/{repo_name}/issues/{pull_num}/events"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for evt in data:
            eid=evt["id"]
            if eid<=last_eid:
                continue
            cstr=evt.get("created_at")
            cdt=None
            if cstr:
                cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            insert_pull_event_record(conn,repo_name,pull_num,eid,cdt,evt)
            new_count+=1
            if eid>highest_eid:
                highest_eid=eid
        if new_count<100:
            break
        page+=1
    if highest_eid>last_eid:
        set_last_event_id_for_pull(conn,repo_name,pull_num,highest_eid)

def insert_pull_event_record(conn, repo_name, pull_num, event_id, created_dt, evt_json):
    import json
    raw_str=json.dumps(evt_json, ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO pull_events
      (repo_name, pull_number, event_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,pull_num,event_id,created_dt,raw_str))
    conn.commit()
    c.close()
