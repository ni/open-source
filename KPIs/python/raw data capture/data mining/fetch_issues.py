# fetch_issues.py

import logging
import time
import requests
from datetime import datetime

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
                elif resp.status_code==304:
                    logging.info("[deadbird/issues-etag] 304 => no changes => skip new data.")
                    return (resp,False)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/issues-etag] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/issues-etag] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/issues-etag] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/issues-etag] Exhausted mini => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/issues-etag] Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_issues_single_thread(conn, owner, repo, enabled,
                              session, handle_rate_limit_func,
                              max_retries,
                              use_etags=True):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issues",owner,repo)
        return

    repo_name=f"{owner}/{repo}"
    endpoint_name="issues"

    if not use_etags:
        # fallback => old approach
        old_list_issues_no_etag(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_value, last_updated = get_endpoint_state(conn, owner, repo, endpoint_name)
    page=1
    last_page=None
    total_inserted=0
    max_updated_so_far=last_updated

    if max_updated_so_far:
        logging.info("[deadbird/issues-etag] Using ?since=%s for updated issues" % max_updated_so_far)
    else:
        logging.info("[deadbird/issues-etag] No last_updated => full fetch from earliest => sorting by updated")

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={
            "state":"all",
            "sort":"updated",
            "direction":"asc",
            "page":page,
            "per_page":100
        }
        if max_updated_so_far:
            params["since"]=max_updated_so_far.isoformat()

        if etag_value:
            session.headers["If-None-Match"]=etag_value

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
        new_max_dt=max_updated_so_far
        for item in data:
            # skip if it's a PR
            if "pull_request" in item:
                continue
            if insert_issue_record(conn, repo_name, item):
                new_count+=1

            upd_str=item.get("updated_at")
            if upd_str:
                dt=datetime.strptime(upd_str,"%Y-%m-%dT%H:%M:%SZ")
                if not new_max_dt or dt>new_max_dt:
                    new_max_dt=dt

        total_inserted+=new_count
        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_value=new_etag

        if new_max_dt and (not max_updated_so_far or new_max_dt>max_updated_so_far):
            max_updated_so_far=new_max_dt

        if len(data)<100:
            break
        page+=1

    # store final ETag + final last_updated
    update_endpoint_state(conn, owner, repo, endpoint_name, etag_value, max_updated_so_far)
    logging.info("[deadbird/issues-etag] Done => inserted %d => %s => new last_updated=%s",
                 total_inserted, repo_name, max_updated_so_far)

def old_list_issues_no_etag(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/issues-old] scanning from page=1 => unlimited => %s/%s",owner,repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={
            "state":"all",
            "sort":"created",
            "direction":"asc",
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for item in data:
            if "pull_request" in item:
                continue
            if insert_issue_record(conn,f"{owner}/{repo}",item):
                new_count+=1
        total_inserted+=new_count
        if len(data)<100:
            break
        page+=1

    logging.info("[deadbird/issues-old] Done => total inserted %d => %s/%s",total_inserted,owner,repo)

def insert_issue_record(conn, repo_name, issue_obj):
    """
    Return True if newly inserted, False otherwise
    """
    c=conn.cursor()
    issue_num=issue_obj["number"]
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s AND issue_number=%s",
              (repo_name,issue_num))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        created_str=issue_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
        sql="""
        INSERT INTO issues (repo_name, issue_number, created_at)
        VALUES (%s,%s,%s)
        """
        c.execute(sql,(repo_name,issue_num,created_dt))
        conn.commit()
        c.close()
        return True
