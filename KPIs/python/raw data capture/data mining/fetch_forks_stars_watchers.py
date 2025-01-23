# fetch_forks_stars_watchers.py
import logging
import time
import requests
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

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
                logging.warning("Conn error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

# watchers => full => no numeric skip
def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return
    page=1
    repo_name=f"{owner}/{repo}"
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            break
        data=resp.json()
        if not data:
            break
        for user_obj in data:
            insert_watcher_record(conn, repo_name, user_obj)
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

# numeric skip => forks => skip fork_id <= known
def get_max_fork_id(conn, repo_name):
    c=conn.cursor()
    c.execute("SELECT MAX(fork_id) FROM forks WHERE repo_name=%s",(repo_name,))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def list_forks_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    highest_fid=get_max_fork_id(conn,repo_name)
    page=1
    while True:
        old_val=highest_fid
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,None,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop forks mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={"sort":"oldest","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for fork_obj in data:
            fid=fork_obj["id"]
            if fid<=highest_fid:
                continue
            c_str=fork_obj.get("created_at")
            cdt=None
            if c_str:
                cdt=datetime.strptime(c_str,"%Y-%m-%dT%H:%M:%SZ")
            insert_fork_record(conn,repo_name,fork_obj,fid,cdt)
            new_count+=1
            if fid>highest_fid:
                highest_fid=fid
        if new_count<100:
            break
        page+=1

def insert_fork_record(conn, repo_name, fork_obj, fork_id, created_dt):
    import json
    raw_str=json.dumps(fork_obj,ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO forks (repo_name, fork_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,fork_id,created_dt,raw_str))
    conn.commit()
    c.close()

# numeric skip => stars => star_id
def get_max_star_id(conn, repo_name):
    c=conn.cursor()
    c.execute("SELECT MAX(star_id) FROM stars WHERE repo_name=%s",(repo_name,))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def list_stars_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    highest_sid=get_max_star_id(conn,repo_name)
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"
    page=1
    while True:
        old_val=highest_sid
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,None,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop stars mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        import json
        for stargazer in data:
            # ID from top-level stargazer or from stargazer["user"]["id"] if needed
            sid=stargazer["id"]
            if sid<=highest_sid:
                continue
            s_str=stargazer.get("starred_at")
            cdt=None
            if s_str:
                cdt=datetime.strptime(s_str,"%Y-%m-%dT%H:%M:%SZ")
            insert_star_record(conn,repo_name,stargazer,sid,cdt)
            new_count+=1
            if sid>highest_sid:
                highest_sid=sid
        if new_count<100:
            break
        page+=1
    session.headers["Accept"]=old_accept

def insert_star_record(conn, repo_name, star_obj, star_id, starred_dt):
    import json
    raw_str=json.dumps(star_obj,ensure_ascii=False)
    user_login=star_obj["user"]["login"]
    c=conn.cursor()
    sql="""
    INSERT INTO stars (repo_name, star_id, user_login, starred_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      starred_at=VALUES(starred_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,star_id,user_login,starred_dt,raw_str))
    conn.commit()
    c.close()
