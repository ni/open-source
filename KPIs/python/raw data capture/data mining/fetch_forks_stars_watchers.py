# fetch_forks_stars_watchers.py
"""
Fetch watchers => no date => if enabled=1
Fetch forks => skip if created_at>baseline_date
Fetch stars => skip if starred_at>baseline_date
No break on 403 => robust_get_page => pass handle_rate_limit_func, max_retries => consistent usage.
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
    logging.warning("Exceeded max_retries => giving up => %s",url)
    return (None,False)

def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return
    page=1
    full_repo_name=f"{owner}/{repo}"
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Watchers => cannot get page %d => break => %s/%s",page,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for user_obj in data:
            insert_watcher_record(conn, full_repo_name, user_obj)

        if len(data)<100:
            break
        page+=1

def insert_watcher_record(conn, repo_name, user_json):
    user_login=user_json["login"]
    raw_str=json.dumps(user_json, ensure_ascii=False)
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

def list_forks_single_thread(conn, owner, repo, baseline_date, enabled,
                             session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks",owner,repo)
        return
    page=1
    full_repo_name=f"{owner}/{repo}"
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop forks mid-run",owner,repo)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (forks).",owner,repo,baseline_date)

        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={
            "sort":"oldest",
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Forks => can't get page %d => break => %s/%s",page,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for fork in data:
            cstr=fork.get("created_at")
            if not cstr:
                continue
            cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and cdt>baseline_date:
                continue
            insert_fork_record(conn, full_repo_name, fork)

        if len(data)<100:
            break
        page+=1

def insert_fork_record(conn, repo_name, fork_json):
    fork_id=fork_json["id"]
    created_str=fork_json["created_at"]
    created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(fork_json,ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO forks (repo_name, fork_id, created_at, raw_json)
    VALUES (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,fork_id,created_dt,raw_str))
    conn.commit()
    c.close()

def list_stars_single_thread(conn, owner, repo, baseline_date, enabled,
                             session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars",owner,repo)
        return

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"
    page=1
    full_repo_name=f"{owner}/{repo}"
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop stars mid-run",owner,repo)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (stars).",owner,repo,baseline_date)

        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Stars => can't get page %d => break => %s/%s",page,owner,repo)
            break

        data=resp.json()
        if not data:
            break

        for stargazer in data:
            sstr=stargazer.get("starred_at")
            if not sstr:
                continue
            sdt=datetime.strptime(sstr,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and sdt>baseline_date:
                continue
            raw_str=json.dumps(stargazer,ensure_ascii=False)
            insert_star_record(conn, full_repo_name, stargazer, sdt)

        if len(data)<100:
            break
        page+=1
    session.headers["Accept"]=old_accept

def insert_star_record(conn, repo_name, star_json, starred_dt):
    user_login=star_json["user"]["login"]
    raw_str=json.dumps(star_json, ensure_ascii=False)
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
