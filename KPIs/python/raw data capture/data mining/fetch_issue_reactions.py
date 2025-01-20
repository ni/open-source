# fetch_forks_stars_watchers.py
"""
Fetch forks => skip if created_at>baseline_date
Fetch stars => skip if starred_at>baseline_date
Fetch watchers => no date => fetch if enabled=1
"""

import logging
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def list_forks_single_thread(conn, owner, repo, baseline_date, enabled, session, handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks.",owner,repo)
        return
    page=1
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
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Forks => HTTP %d => break %s/%s", resp.status_code,owner,repo)
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
            insert_fork_record(conn, f"{owner}/{repo}", fork)

        if len(data)<100:
            break
        page+=1

def insert_fork_record(conn, repo_name, fork_json):
    fork_id=fork_json["id"]
    created_str=fork_json["created_at"]
    created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    c=conn.cursor()
    sql="""
    INSERT INTO forks (repo_name, fork_id, created_at, raw_json)
    VALUES (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name, fork_id, created_dt,fork_json))
    conn.commit()
    c.close()

def list_stars_single_thread(conn, owner, repo, baseline_date, enabled, session, handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars.",owner,repo)
        return

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop stars mid-run",owner,repo)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (stars)",owner,repo,baseline_date)

        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={
            "page":page,
            "per_page":100
        }
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Stars => HTTP %d => break %s/%s",resp.status_code,owner,repo)
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
            insert_star_record(conn, f"{owner}/{repo}", stargazer)

        if len(data)<100:
            break
        page+=1
    session.headers["Accept"]=old_accept

def insert_star_record(conn, repo_name, star_json):
    user_login=star_json["user"]["login"]
    starred_str=star_json["starred_at"]
    starred_dt=datetime.strptime(starred_str,"%Y-%m-%dT%H:%M:%SZ")
    c=conn.cursor()
    sql="""
    INSERT INTO stars (repo_name, user_login, starred_at, raw_json)
    VALUES (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      starred_at=VALUES(starred_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,user_login,starred_dt,star_json))
    conn.commit()
    c.close()

def list_watchers_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return
    page=1
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={
            "page":page,
            "per_page":100
        }
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Watchers => HTTP %d => break for %s/%s",resp.status_code,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for user_obj in data:
            user_login=user_obj["login"]
            insert_watcher_record(conn,f"{owner}/{repo}",user_login,user_obj)

        if len(data)<100:
            break
        page+=1

def insert_watcher_record(conn, repo_name, user_login, user_json):
    c=conn.cursor()
    sql="""
    INSERT INTO watchers (repo_name, user_login, raw_json)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,user_login,user_json))
    conn.commit()
    c.close()
