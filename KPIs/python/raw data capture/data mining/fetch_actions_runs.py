# fetch_actions_runs.py

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
                resp=session.get(url,params=params)
                handle_rate_limit_func(resp)
                if resp.status_code==200:
                    return (resp,True)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/actions] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/actions] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/actions] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/actions] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/actions] Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_actions_runs_single_thread(conn, owner, repo, enabled,
                                   session, handle_rate_limit_func,
                                   max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip actions_runs",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        params={"page":page,"per_page":50}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        # data might have "workflow_runs" or so
        runs=data.get("workflow_runs",[])
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*50
        new_count=0
        for run in runs:
            if store_action_run(conn,repo_name,run):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/actions] page=%d/%d => %.3f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/actions] => so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/actions] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(runs)<50:
            break
        page+=1

    logging.info("[deadbird/actions] Done => total inserted %d => %s",total_inserted,repo_name)

def store_action_run(conn, repo_name, run_obj):
    c=conn.cursor()
    run_id=run_obj["id"]
    c.execute("""
      SELECT run_id FROM actions_runs
      WHERE repo_name=%s AND run_id=%s
    """,(repo_name,run_id))
    row=c.fetchone()
    if row:
        update_action_run(c,conn,repo_name,run_id,run_obj)
        c.close()
        return False
    else:
        insert_action_run(c,conn,repo_name,run_id,run_obj)
        c.close()
        return True

def insert_action_run(c, conn, repo_name, run_id, run_obj):
    import json
    head_branch=run_obj.get("head_branch","")
    head_sha=run_obj.get("head_sha","")
    event_type=run_obj.get("event","")
    status=run_obj.get("status","")
    conclusion=run_obj.get("conclusion","")
    workflow_id=run_obj.get("workflow_id",0)

    created_str=run_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    updated_str=run_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

    run_started_str=run_obj.get("run_started_at")
    run_started_dt=None
    if run_started_str:
        run_started_dt=datetime.strptime(run_started_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(run_obj, ensure_ascii=False)
    sqlrun="""
    INSERT INTO actions_runs
      (repo_name, run_id, head_branch, head_sha, event_type, status,
       conclusion, workflow_id, created_at, updated_at, run_started_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sqlrun,(repo_name,run_id,head_branch,head_sha,event_type,status,
                      conclusion,workflow_id,created_dt,updated_dt,run_started_dt,
                      raw_str))
    conn.commit()

def update_action_run(c, conn, repo_name, run_id, run_obj):
    import json
    head_branch=run_obj.get("head_branch","")
    head_sha=run_obj.get("head_sha","")
    event_type=run_obj.get("event","")
    status=run_obj.get("status","")
    conclusion=run_obj.get("conclusion","")
    workflow_id=run_obj.get("workflow_id",0)

    created_str=run_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    updated_str=run_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

    run_started_str=run_obj.get("run_started_at")
    run_started_dt=None
    if run_started_str:
        run_started_dt=datetime.strptime(run_started_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(run_obj, ensure_ascii=False)
    sqlrun="""
    UPDATE actions_runs
    SET head_branch=%s, head_sha=%s, event_type=%s, status=%s, conclusion=%s,
        workflow_id=%s, created_at=%s, updated_at=%s, run_started_at=%s,
        raw_json=%s
    WHERE repo_name=%s AND run_id=%s
    """
    c.execute(sqlrun,(head_branch,head_sha,event_type,status,conclusion,
                      workflow_id,created_dt,updated_dt,run_started_dt,
                      raw_str,repo_name,run_id))
    conn.commit()
