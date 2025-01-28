# fetch_commits.py

import logging
import time
import requests
from datetime import datetime

from etags import get_endpoint_state, update_endpoint_state

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

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20, endpoint="commits"):
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
                    logging.info("[deadbird/%s-etag] 304 => skip => no new commits" % endpoint)
                    return (resp,False)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/%s-etag] HTTP %d => attempt %d/%d => retry => %s",
                                    endpoint, resp.status_code, attempt, max_retries, url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/%s-etag] HTTP %d => break => %s", endpoint, resp.status_code, url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/%s-etag] Connection error => local mini => %s",endpoint,url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/%s-etag] Exhausted mini => break => %s",endpoint,url)
            return (None,False)
    logging.warning("[deadbird/%s-etag] Exceeded max => give up => %s",endpoint,url)
    return (None,False)

def list_commits_single_thread(conn, owner, repo, enabled, baseline_dt,
                               session, handle_rate_limit_func, max_retries,
                               use_etags=True):
    if enabled==0:
        logging.info("[deadbird/commits] %s/%s => disabled => skip",owner,repo)
        return
    endpoint="commits"
    repo_name=f"{owner}/{repo}"

    if not use_etags:
        commits_old_approach(conn, owner, repo, baseline_dt, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_updated = get_endpoint_state(conn,owner,repo,endpoint)
    page=1
    last_page=None
    total_inserted=0
    max_updated=last_updated

    # some folks also do baseline_dt skip => if baseline_dt is older => we'd do partial logic
    # but let's do "since" = max(baseline_dt, last_updated) if that fits your scenario
    # We'll assume last_updated is the best incremental check for updated commits
    if max_updated:
        logging.info("[deadbird/commits-etag] ?since=%s => incremental commits" % max_updated)
    else:
        # if baseline_dt is used => you can do: if baseline_dt is not None => baseline_dt
        # but let's keep it simpler: if baseline_dt is older, we do a full fetch anyway
        logging.info("[deadbird/commits-etag] No last_updated => full fetch => ascending by date")

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/commits"
        params={
            "page":page,
            "per_page":50,
            "sort":"committer-date",
            "direction":"asc"
        }
        # if we want an incremental approach => ?since=some_date
        # let's let last_updated override baseline_dt for incremental
        final_since=max_updated if max_updated else baseline_dt
        if final_since:
            params["since"]=final_since.strftime("%Y-%m-%dT%H:%M:%SZ")

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
        new_max_dt=max_updated
        for cmt_obj in data:
            if store_commit_record(conn, repo_name, cmt_obj):
                new_count+=1
            cdt_str=cmt_obj.get("commit",{}).get("committer",{}).get("date")
            if cdt_str:
                dt=datetime.strptime(cdt_str,"%Y-%m-%dT%H:%M:%SZ")
                if not new_max_dt or dt>new_max_dt:
                    new_max_dt=dt
        total_inserted+=new_count

        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if new_max_dt and (not max_updated or new_max_dt>max_updated):
            max_updated=new_max_dt

        if len(data)<50:
            break
        page+=1

    update_endpoint_state(conn,owner,repo,endpoint,etag_val,max_updated)
    logging.info("[deadbird/commits-etag] Done => inserted %d => %s => new last_updated=%s",
                 total_inserted,repo_name,max_updated)

def commits_old_approach(conn, owner, repo, baseline_dt,
                         session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/commits-old] scanning => %s/%s => unlimited asc by date",owner,repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/commits"
        params={
            "page":page,
            "per_page":50,
            "sort":"committer-date",
            "direction":"asc"
        }
        # if baseline_dt => we can do ?since=...
        if baseline_dt:
            params["since"]=baseline_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint="commits-old")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for cmt_obj in data:
            if store_commit_record(conn,f"{owner}/{repo}",cmt_obj):
                new_count+=1
        total_inserted+=new_count
        if len(data)<50:
            break
        page+=1

    logging.info("[deadbird/commits-old] total inserted %d => %s/%s",total_inserted,owner,repo)

def store_commit_record(conn, repo_name, cmt_obj):
    c=conn.cursor()
    sha=cmt_obj["sha"]
    c.execute("SELECT sha FROM commits WHERE repo_name=%s AND sha=%s",(repo_name,sha))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        commit_info=cmt_obj.get("commit",{})
        author_login=(cmt_obj.get("author") or {}).get("login")
        committer_login=(cmt_obj.get("committer") or {}).get("login")
        message=commit_info.get("message","")
        date_str=commit_info.get("committer",{}).get("date")
        commit_date=None
        if date_str:
            commit_date=datetime.strptime(date_str,"%Y-%m-%dT%H:%M:%SZ")
        raw_str=json.dumps(cmt_obj, ensure_ascii=False)
        sql="""
        INSERT INTO commits
          (repo_name, sha, author_login, committer_login,
           commit_message, commit_date, raw_json)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,sha,author_login,committer_login,
                       message,commit_date,raw_str))
        conn.commit()
        c.close()
        return True
