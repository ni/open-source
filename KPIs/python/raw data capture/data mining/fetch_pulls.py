# fetch_pulls.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

from etags import get_endpoint_state, update_endpoint_state

def get_last_page(resp):
    link_header=resp.headers.get("Link")
    if not link_header:
        return None
    parts=link_header.split(',')
    import re
    for p in parts:
        if 'rel="last"' in p:
            m = re.search(r'[?&]page=(\d+)', p)
            if m:
                return int(m.group(1))
    return None

def list_pulls_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries,
                             use_etags=True):
    if enabled==0:
        logging.info("[deadbird/pulls] %s/%s => disabled => skip",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    endpoint="pulls"

    if not use_etags:
        pulls_old_approach(conn,owner,repo,session,handle_rate_limit_func,max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn,owner,repo,endpoint)
    page=1
    last_page=None
    total_inserted=0
    max_updated=last_upd

    if max_updated:
        logging.info("[deadbird/pulls-etag] ?since=%s => incremental updated pulls" % max_updated)
    else:
        logging.info("[deadbird/pulls-etag] No last_updated => full fetch => sort=updated asc")

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={
            "state":"all",
            "sort":"updated",
            "direction":"asc",
            "page":page,
            "per_page":100
        }
        if max_updated:
            params["since"]=max_updated.isoformat()

        if etag_val:
            session.headers["If-None-Match"]=etag_val

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
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
        new_max_dt=max_updated
        for item in data:
            if "pull_request" not in item:
                # skip => it's not a pull
                continue
            if insert_pull_record(conn,repo_name,item):
                new_count+=1
            upd_str=item.get("updated_at")
            if upd_str:
                dt=datetime.strptime(upd_str,"%Y-%m-%dT%H:%M:%SZ")
                if not new_max_dt or dt>new_max_dt:
                    new_max_dt=dt

        total_inserted+=new_count
        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if new_max_dt and (not max_updated or new_max_dt>max_updated):
            max_updated=new_max_dt

        if len(data)<100:
            break
        page+=1

    update_endpoint_state(conn,owner,repo,endpoint,etag_val,max_updated)
    logging.info("[deadbird/pulls-etag] Done => inserted %d => %s => new last_updated=%s",
                 total_inserted, repo_name, max_updated)

def pulls_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/pulls-old] scanning => %s/%s => unlimited",owner,repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={"state":"all","sort":"created","direction":"asc","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for item in data:
            if "pull_request" not in item:
                continue
            if insert_pull_record(conn,f"{owner}/{repo}",item):
                new_count+=1
        total_inserted+=new_count
        if len(data)<100:
            break
        page+=1

    logging.info("[deadbird/pulls-old] inserted %d => %s/%s",total_inserted,owner,repo)

def insert_pull_record(conn, repo_name, pr_obj):
    c=conn.cursor()
    pull_num=pr_obj["number"]
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s AND pull_number=%s",
              (repo_name,pull_num))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        created_str=pr_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
        sql="""
        INSERT INTO pulls (repo_name, pull_number, created_at)
        VALUES (%s,%s,%s)
        """
        c.execute(sql,(repo_name,pull_num,created_dt))
        conn.commit()
        c.close()
        return True
