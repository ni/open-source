# fetch_review_requests.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

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

def list_review_requests_single_thread(conn, owner, repo, enabled,
                                       session, handle_rate_limit_func,
                                       max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip specialized review requests",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    # for each known pull => we do /pulls/{pull_number}/reviews
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s",(repo_name,))
    pull_rows=c.fetchall()
    c.close()

    for (pull_num,) in pull_rows:
        fetch_review_requests_for_pull(conn, repo_name, pull_num, enabled,
                                       session, handle_rate_limit_func, max_retries)

def fetch_review_requests_for_pull(conn, repo_name, pull_num, enabled,
                                   session, handle_rate_limit_func, max_retries):
    if enabled==0:
        return
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{repo_name}/pulls/{pull_num}/reviews"
        params={"page":page,"per_page":50}
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
            total_items=last_page*50
        new_count=0
        for rv in data:
            if store_review_request_event(conn,repo_name,pull_num,rv):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/reviewreq] PR #%d => page=%d/%d => %.3f%% => inserted %d => %s",
                          pull_num,page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/reviewreq] => total so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/reviewreq] PR #%d => page=%d => inserted %d => no last_page => %s",
                          pull_num,page,new_count,repo_name)

        if len(data)<50:
            break
        page+=1
    logging.info("[deadbird/reviewreq] PR #%d => inserted total %d => %s",pull_num,total_inserted,repo_name)

def store_review_request_event(conn, repo_name, pull_num, rv_obj):
    """
    We store them in review_request_events if it indicates “review requested.”
    Actually, GitHub's /reviews endpoint might just show actual reviews. 
    If you want 'review_requested' events specifically, we might parse the timeline. 
    For demonstration, we store them all here as specialized events.
    """
    c=conn.cursor()
    request_event_id=rv_obj["id"]
    c.execute("""
      SELECT request_event_id FROM review_request_events
      WHERE repo_name=%s AND pull_number=%s AND request_event_id=%s
    """,(repo_name,pull_num,request_event_id))
    row=c.fetchone()
    if row:
        update_review_request_event(c,conn,repo_name,pull_num,request_event_id,rv_obj)
        c.close()
        return False
    else:
        insert_review_request_event(c,conn,repo_name,pull_num,request_event_id,rv_obj)
        c.close()
        return True

def insert_review_request_event(c, conn, repo_name, pull_num, request_event_id, rv_obj):
    import json
    # parse who was requested_reviewer, created_at, etc.
    created_str=rv_obj.get("submitted_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    requested_reviewer=""
    requested_team=""
    # 'user' field might be the reviewer
    user=rv_obj.get("user",{})
    if user:
        requested_reviewer=user.get("login","")

    raw_str=json.dumps(rv_obj, ensure_ascii=False)
    sql="""
    INSERT INTO review_request_events
      (repo_name, pull_number, request_event_id,
       created_at, requested_reviewer, requested_team, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,pull_num,request_event_id,
                   created_dt,requested_reviewer,requested_team,raw_str))
    conn.commit()

def update_review_request_event(c, conn, repo_name, pull_num, request_event_id, rv_obj):
    import json
    created_str=rv_obj.get("submitted_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    user=rv_obj.get("user",{})
    requested_reviewer=user.get("login","")
    requested_team=""

    raw_str=json.dumps(rv_obj, ensure_ascii=False)
    sql="""
    UPDATE review_request_events
    SET created_at=%s, requested_reviewer=%s,
        requested_team=%s, raw_json=%s
    WHERE repo_name=%s AND pull_number=%s AND request_event_id=%s
    """
    c.execute(sql,(created_dt,requested_reviewer,requested_team,raw_str,
                   repo_name,pull_num,request_event_id))
    conn.commit()
