# fetch_projects.py

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
                    logging.warning("[deadbird/projects] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/projects] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/projects] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/projects] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/projects] Exceeded max_retries => give up => %s",url)
    return (None,False)

def fetch_projects_single_thread(conn, owner, repo, enabled,
                                 session, handle_rate_limit_func,
                                 max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip projects",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/projects"
        params={"page":page,"per_page":30}
        # Must set Accept to inlude projects:
        old_accept=session.headers.get("Accept","")
        session.headers["Accept"]="application/vnd.github.inertia-preview+json"

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        session.headers["Accept"]=old_accept

        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*30

        new_count=0
        for pj in data:
            if store_project_and_columns(conn, repo_name, pj, session, handle_rate_limit_func, max_retries):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/projects] page=%d/%d => %.3f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/projects] => total so far %d out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/projects] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(data)<30:
            break
        page+=1

    logging.info("[deadbird/projects] Done => total inserted %d => %s",total_inserted,repo_name)

def store_project_and_columns(conn, repo_name, pj_obj, session, handle_rate_limit_func, max_retries):
    c=conn.cursor()
    project_id=pj_obj["id"]
    c.execute("""
      SELECT project_id FROM repo_projects
      WHERE repo_name=%s AND project_id=%s
    """,(repo_name,project_id))
    row=c.fetchone()
    if row:
        update_project_record(c,conn,repo_name,project_id,pj_obj)
        c.close()
        # fetch columns anyway
        fetch_project_columns(conn, repo_name, project_id, session, handle_rate_limit_func, max_retries)
        return False
    else:
        insert_project_record(c,conn,repo_name,project_id,pj_obj)
        c.close()
        fetch_project_columns(conn, repo_name, project_id, session, handle_rate_limit_func, max_retries)
        return True

def insert_project_record(c,conn,repo_name,project_id,pj_obj):
    import json
    name=pj_obj.get("name","")
    body=pj_obj.get("body","")
    state=pj_obj.get("state","")
    created_str=pj_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=pj_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(pj_obj, ensure_ascii=False)
    sql="""
    INSERT INTO repo_projects
     (repo_name, project_id, name, body, state,
      created_at, updated_at, raw_json)
    VALUES
     (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,project_id,name,body,state,created_dt,updated_dt,raw_str))
    conn.commit()

def update_project_record(c,conn,repo_name,project_id,pj_obj):
    import json
    name=pj_obj.get("name","")
    body=pj_obj.get("body","")
    state=pj_obj.get("state","")
    created_str=pj_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=pj_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(pj_obj, ensure_ascii=False)
    sql="""
    UPDATE repo_projects
    SET name=%s, body=%s, state=%s,
        created_at=%s, updated_at=%s,
        raw_json=%s
    WHERE repo_name=%s AND project_id=%s
    """
    c.execute(sql,(name,body,state,created_dt,updated_dt,raw_str,repo_name,project_id))
    conn.commit()

def fetch_project_columns(conn, repo_name, project_id, session, handle_rate_limit_func, max_retries):
    url=f"https://api.github.com/projects/{project_id}/columns"
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.inertia-preview+json"
    page=1
    while True:
        params={"page":page,"per_page":30}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for col in data:
            if store_project_column_and_cards(conn, repo_name, project_id, col, session, handle_rate_limit_func, max_retries):
                new_count+=1
        if len(data)<30:
            break
        page+=1

    session.headers["Accept"]=old_accept

def store_project_column_and_cards(conn, repo_name, project_id, col_obj,
                                   session, handle_rate_limit_func, max_retries):
    c=conn.cursor()
    column_id=col_obj["id"]
    c.execute("""
      SELECT column_id FROM project_columns
      WHERE repo_name=%s AND project_id=%s AND column_id=%s
    """,(repo_name,project_id,column_id))
    row=c.fetchone()
    if row:
        update_project_column(c,conn,repo_name,project_id,column_id,col_obj)
        c.close()
        fetch_project_cards(conn, repo_name, project_id, column_id, session, handle_rate_limit_func, max_retries)
        return False
    else:
        insert_project_column(c,conn,repo_name,project_id,column_id,col_obj)
        c.close()
        fetch_project_cards(conn, repo_name, project_id, column_id, session, handle_rate_limit_func, max_retries)
        return True

def insert_project_column(c, conn, repo_name, project_id, column_id, col_obj):
    import json
    name=col_obj.get("name","")
    created_str=col_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=col_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(col_obj,ensure_ascii=False)
    sql="""
    INSERT INTO project_columns
      (repo_name, project_id, column_id, name,
       created_at, updated_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,project_id,column_id,name,created_dt,updated_dt,raw_str))
    conn.commit()

def update_project_column(c, conn, repo_name, project_id, column_id, col_obj):
    import json
    name=col_obj.get("name","")
    created_str=col_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=col_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(col_obj,ensure_ascii=False)
    sql="""
    UPDATE project_columns
    SET name=%s, created_at=%s, updated_at=%s, raw_json=%s
    WHERE repo_name=%s AND project_id=%s AND column_id=%s
    """
    c.execute(sql,(name,created_dt,updated_dt,raw_str,
                   repo_name,project_id,column_id))
    conn.commit()

def fetch_project_cards(conn, repo_name, project_id, column_id,
                        session, handle_rate_limit_func, max_retries):
    url=f"https://api.github.com/projects/columns/{column_id}/cards"
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.inertia-preview+json"
    page=1
    while True:
        params={"page":page,"per_page":30}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for card in data:
            if store_project_card(conn,repo_name,project_id,column_id,card):
                new_count+=1
        if len(data)<30:
            break
        page+=1
    session.headers["Accept"]=old_accept

def store_project_card(conn, repo_name, project_id, column_id, card_obj):
    c=conn.cursor()
    card_id=card_obj["id"]
    c.execute("""
      SELECT card_id FROM project_cards
      WHERE repo_name=%s AND card_id=%s
    """,(repo_name,card_id))
    row=c.fetchone()
    if row:
        update_project_card(c,conn,repo_name,project_id,column_id,card_id,card_obj)
        c.close()
        return False
    else:
        insert_project_card(c,conn,repo_name,project_id,column_id,card_id,card_obj)
        c.close()
        return True

def insert_project_card(c, conn, repo_name, project_id, column_id, card_id, card_obj):
    import json
    note=card_obj.get("note","")
    ctype=card_obj.get("content_type","")
    cid=card_obj.get("content_id")
    created_str=card_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=card_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(card_obj,ensure_ascii=False)
    sql="""
    INSERT INTO project_cards
     (repo_name, project_id, column_id, card_id, note,
      content_type, content_id, created_at, updated_at, raw_json)
    VALUES
     (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,project_id,column_id,card_id,note,
                   ctype,cid,created_dt,updated_dt,raw_str))
    conn.commit()

def update_project_card(c, conn, repo_name, project_id, column_id, card_id, card_obj):
    import json
    note=card_obj.get("note","")
    ctype=card_obj.get("content_type","")
    cid=card_obj.get("content_id")
    created_str=card_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=card_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(card_obj,ensure_ascii=False)
    sql="""
    UPDATE project_cards
    SET note=%s, content_type=%s, content_id=%s,
        created_at=%s, updated_at=%s, raw_json=%s
    WHERE repo_name=%s AND card_id=%s
    """
    c.execute(sql,(note,ctype,cid,created_dt,updated_dt,raw_str,
                   repo_name,card_id))
    conn.commit()
