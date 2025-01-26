# fetch_forks_stars_watchers.py
import logging
import time
import requests
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def get_last_page(resp):
    link_header=resp.headers.get("Link")
    if not link_header:
        return None
    parts=link_header.split(',')
    for p in parts:
        if 'rel="last"' in p:
            import re
            m=re.search(r'[?&]page=(\d+)',p)
            if m:
                return int(m.group(1))
    return None

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
                    logging.warning("HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except requests.exceptions.ConnectionError:
                logging.warning("Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return
    page=1
    last_page=None
    repo_name=f"{owner}/{repo}"
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Watchers => can't get page %d => skip => %s",page,repo_name)
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] watchers => page={page}/{last_page} => {progress:.3f}%% => {repo_name}")

        for user_obj in data:
            insert_watcher_record(conn,repo_name,user_obj)
        if len(data)<100:
            break
        page+=1

def insert_watcher_record(conn, repo_name, user_obj):
    import json
    raw_str=json.dumps(user_obj,ensure_ascii=False)
    user_login=user_obj["login"]
    c=conn.cursor()
    sql="""
    INSERT INTO watchers (repo_name, user_login, raw_json)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,user_login,raw_str))
    conn.commit()
    c.close()

def list_forks_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks",owner,repo)
        return
    page=1
    last_page=None
    repo_name=f"{owner}/{repo}"
    while True:
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,None,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop forks mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={"sort":"oldest","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Forks => can't get page %d => skip => %s",page,repo_name)
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] forks => page={page}/{last_page} => {progress:.3f}%% => {repo_name}")

        for fk in data:
            insert_fork_record(conn,repo_name,fk)
        if len(data)<100:
            break
        page+=1

def insert_fork_record(conn, repo_name, fork_obj):
    import json
    raw_str=json.dumps(fork_obj,ensure_ascii=False)
    fork_id=fork_obj["id"]
    cstr=fork_obj.get("created_at")
    cdt=None
    if cstr:
        cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
    c=conn.cursor()
    sql="""
    INSERT INTO forks (repo_name, fork_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,fork_id,cdt,raw_str))
    conn.commit()
    c.close()

def list_stars_single_thread(conn, owner, repo, enabled,
                             baseline_dt,
                             session, handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"
    page=1
    last_page=None
    while True:
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,None,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop stars mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Stars => can't get page %d => skip => %s",page,repo_name)
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] stars => page={page}/{last_page} => {progress:.3f}%% => {repo_name}")

        import json
        for stargazer in data:
            starred_at_str=stargazer.get("starred_at")
            if not starred_at_str:
                continue
            sdt=datetime.strptime(starred_at_str,"%Y-%m-%dT%H:%M:%SZ")
            if sdt<baseline_dt:
                # skip older
                continue
            user_login=stargazer["user"]["login"]
            raw_str=json.dumps(stargazer,ensure_ascii=False)
            insert_star_record(conn,repo_name,user_login,sdt,raw_str)

        if len(data)<100:
            break
        page+=1
    session.headers["Accept"]=old_accept

def insert_star_record(conn, repo_name, user_login, starred_dt, raw_str):
    c=conn.cursor()
    sql="""
    INSERT INTO stars (repo_name, user_login, starred_at, raw_json)
    VALUES (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      starred_at=VALUES(starred_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,user_login,starred_dt,raw_str))
    conn.commit()
    c.close()
