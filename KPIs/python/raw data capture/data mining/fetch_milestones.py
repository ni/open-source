# fetch_milestones.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

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

def list_milestones_single_thread(conn, owner, repo, enabled,
                                  session, handle_rate_limit_func,
                                  max_retries,
                                  use_etags=True):
    """
    Final version that accepts `use_etags`.
    If `use_etags=False`, fallback to old approach scanning from page=1.
    """
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip milestones",owner,repo)
        return

    endpoint="milestones"
    repo_name=f"{owner}/{repo}"

    if not use_etags:
        milestones_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn, owner, repo, endpoint)
    page=1
    last_page=None
    total_inserted=0

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/milestones"
        params={"state":"all","page":page,"per_page":30}
        if etag_val:
            session.headers["If-None-Match"]=etag_val

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint=endpoint)
        if "If-None-Match" in session.headers:
            del session.headers["If-None-Match"]
        if not success or not resp:
            break

        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)

        new_count=0
        for mst in data:
            if store_milestone(conn,repo_name,mst):
                new_count+=1
        total_inserted+=new_count

        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if len(data)<30:
            break
        page+=1

    # no last_updated usage => keep it
    update_endpoint_state(conn,owner,repo,endpoint,etag_val,last_upd)
    logging.info("[deadbird/milestones-etag] Done => total inserted %d => %s",total_inserted,repo_name)

def milestones_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/milestones-old] => scanning => %s/%s from page=1", owner, repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/milestones"
        params={"state":"all","page":page,"per_page":30}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint="milestones-old")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break

        new_count=0
        for mst in data:
            if store_milestone(conn,f"{owner}/{repo}",mst):
                new_count+=1
        total_inserted+=new_count

        if len(data)<30:
            break
        page+=1

    logging.info("[deadbird/milestones-old] total inserted %d => %s/%s", total_inserted, owner, repo)

def store_milestone(conn, repo_name, mst_obj):
    c=conn.cursor()
    milestone_id=mst_obj["id"]
    c.execute("SELECT milestone_id FROM repo_milestones WHERE repo_name=%s AND milestone_id=%s",
              (repo_name,milestone_id))
    row=c.fetchone()
    if row:
        update_milestone(c,conn,repo_name,milestone_id,mst_obj)
        c.close()
        return False
    else:
        insert_milestone(c,conn,repo_name,milestone_id,mst_obj)
        c.close()
        return True

def insert_milestone(c, conn, repo_name, milestone_id, mst_obj):
    import json
    title=mst_obj.get("title","")
    state=mst_obj.get("state","")
    desc=mst_obj.get("description","")
    due_str=mst_obj.get("due_on")
    due_on=None
    if due_str:
        due_on=datetime.strptime(due_str,"%Y-%m-%dT%H:%M:%SZ")

    created_str=mst_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    updated_str=mst_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

    closed_str=mst_obj.get("closed_at")
    closed_dt=None
    if closed_str:
        closed_dt=datetime.strptime(closed_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(mst_obj, ensure_ascii=False)
    sqlmst = """
    INSERT INTO repo_milestones
      (repo_name, milestone_id, title, state, description, due_on,
       created_at, updated_at, closed_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sqlmst,(repo_name,milestone_id,title,state,desc,due_on,
                      created_dt,updated_dt,closed_dt,raw_str))
    conn.commit()

def update_milestone(c, conn, repo_name, milestone_id, mst_obj):
    import json
    title=mst_obj.get("title","")
    state=mst_obj.get("state","")
    desc=mst_obj.get("description","")
    due_str=mst_obj.get("due_on")
    due_on=None
    if due_str:
        due_on=datetime.strptime(due_str,"%Y-%m-%dT%H:%M:%SZ")

    created_str=mst_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    updated_str=mst_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

    closed_str=mst_obj.get("closed_at")
    closed_dt=None
    if closed_str:
        closed_dt=datetime.strptime(closed_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(mst_obj, ensure_ascii=False)

    sqlmst="""
    UPDATE repo_milestones
    SET title=%s, state=%s, description=%s, due_on=%s,
        created_at=%s, updated_at=%s, closed_at=%s, raw_json=%s
    WHERE repo_name=%s AND milestone_id=%s
    """
    c.execute(sqlmst,(title,state,desc,due_on,
                      created_dt,updated_dt,closed_dt,raw_str,
                      repo_name,milestone_id))
    conn.commit()
