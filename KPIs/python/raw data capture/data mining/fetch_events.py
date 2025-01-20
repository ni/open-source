# fetch_events.py
"""
Fetch issue events => skip if event.created_at>baseline_date
We do GET /repos/{owner}/{repo}/issues/{issue_number}/events
Similarly, we can do the same for pulls if we want to store them separately in pull_events.
But typically GitHub merges them in the same endpoint if you treat pulls as issues.

We'll do separate logic if you want 'pull_events' specifically from the same endpoint.
"""

import logging
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def fetch_issue_events_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                      session, handle_rate_limit_func):
    """
    For each issue in 'issues' table, fetch events => skip if new
    => populate issue_events
    """
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_events",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()

    for (issue_num,) in rows:
        fetch_issue_events_single_thread(conn, owner, repo, issue_num,
                                         baseline_date, enabled, session,
                                         handle_rate_limit_func)

def fetch_issue_events_single_thread(conn, owner, repo, issue_number,
                                     baseline_date, enabled, session,
                                     handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_events for #%d",owner,repo,issue_number)
        return
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop issue_events mid-run for #%d",
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
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Issue Events => HTTP %d => break for %s/%s#%d",
                            resp.status_code,owner,repo,issue_number)
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
    import json
    event_id=evt_json["id"]  # numeric ID
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

# for pull_events, you'd do a separate approach if you want them distinctly
def fetch_pull_events_for_all_pulls(conn, owner, repo, baseline_date, enabled,
                                    session, handle_rate_limit_func):
    """
    For each pull in 'pulls' table => we can do a similar approach if you want
    to store 'pull_events' from GET /repos/{owner}/{repo}/issues/{pull_number}/events
    But might be the same as issue events. We'll demonstrate anyway.
    """
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_events",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()

    for (pull_num,) in rows:
        fetch_pull_events_single_thread(conn, owner, repo, pull_num,
                                        baseline_date, enabled, session,
                                        handle_rate_limit_func)

def fetch_pull_events_single_thread(conn, owner, repo, pull_number,
                                    baseline_date, enabled, session,
                                    handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_events for PR #%d",owner,repo,pull_number)
        return
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop pull_events mid-run for PR #%d",
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
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Pull Events => HTTP %d => break for %s/%s#%d",
                            resp.status_code,owner,repo,pull_number)
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
    import json
    event_id=evt_json["id"]
    raw_str=json.dumps(evt_json,ensure_ascii=False)
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
