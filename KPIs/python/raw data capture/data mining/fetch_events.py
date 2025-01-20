# fetch_events.py
"""
Fetch issue_events => skip if event.created_at>baseline_date
Fetch pull_events => skip if event.created_at>baseline_date
We do NOT break on 403 => robust_get_page => we pass handle_rate_limit_func, max_retries => no 'NameError: max_retries'.
"""

import logging
import time
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    for attempt in range(1,max_retries+1):
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code==200:
            return (resp,True)
        elif resp.status_code in (403,429):
            logging.warning("HTTP %d => attempt %d/%d => will retry => %s",
                            resp.status_code,attempt,max_retries,url)
            time.sleep(5)
        else:
            logging.warning("HTTP %d => attempt %d => break => %s",
                            resp.status_code,attempt,url)
            return (resp,False)
    logging.warning("Exceeding max_retries => give up => url=%s",url)
    return (None,False)

def fetch_issue_events_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                      session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_events",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()

    for (issue_num,) in rows:
        fetch_issue_events_single_thread(
            conn, owner, repo, issue_num,
            baseline_date, enabled, session,
            handle_rate_limit_func,
            max_retries
        )

def fetch_issue_events_single_thread(conn, owner, repo, issue_number,
                                     baseline_date, enabled, session,
                                     handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_events => #%d",owner,repo,issue_number)
        return
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop issue_events => #%d mid-run",
                         owner,repo,issue_number)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (issue_events for #%d)",
                         owner,repo,baseline_date,issue_number)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/events"
        params={
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(
            session, url, params,
            handle_rate_limit_func,
            max_retries=max_retries
        )
        if not success:
            logging.warning("Issue Events => can't fetch page %d => stop => issue #%d => %s/%s",
                            page,issue_number,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for evt in data:
            cstr=evt.get("created_at")
            if not cstr:
                continue
            cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and cdt>baseline_date:
                continue
            insert_issue_event_record(conn,f"{owner}/{repo}",issue_number,evt,cdt)

        if len(data)<100:
            break
        page+=1

def insert_issue_event_record(conn, repo_name, issue_num, evt_json, created_dt):
    raw_str=json.dumps(evt_json, ensure_ascii=False)
    event_id=evt_json["id"]
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

def fetch_pull_events_for_all_pulls(conn, owner, repo, baseline_date, enabled,
                                    session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_events",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()
    for (pull_num,) in rows:
        fetch_pull_events_single_thread(
            conn, owner, repo, pull_num,
            baseline_date, enabled, session,
            handle_rate_limit_func,
            max_retries
        )

def fetch_pull_events_single_thread(conn, owner, repo, pull_number,
                                    baseline_date, enabled, session,
                                    handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_events => PR #%d",owner,repo,pull_number)
        return
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop pull_events => PR #%d mid-run",
                         owner,repo,pull_number)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (pull_events for PR #%d)",
                         owner,repo,baseline_date,pull_number)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{pull_number}/events"
        params={
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(
            session, url, params,
            handle_rate_limit_func,
            max_retries=max_retries
        )
        if not success:
            logging.warning("Pull Events => cannot get page %d => stop => PR #%d => %s/%s",
                            page,pull_number,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for evt in data:
            cstr=evt.get("created_at")
            if not cstr:
                continue
            cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and cdt>baseline_date:
                continue
            insert_pull_event_record(conn,f"{owner}/{repo}",pull_number,evt,cdt)

        if len(data)<100:
            break
        page+=1

def insert_pull_event_record(conn, repo_name, pull_num, evt_json, created_dt):
    raw_str=json.dumps(evt_json, ensure_ascii=False)
    event_id=evt_json["id"]
    c=conn.cursor()
    sql="""
    INSERT INTO pull_events
      (repo_name, pull_number, event_id, created_at, raw_json)
    VALUES
      (%s, %s, %s, %s, %s)
    """
    c.execute(sql,(repo_name,pull_num,event_id,created_dt,raw_str))
    conn.commit()
    c.close()
