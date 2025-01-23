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
    for part in parts:
        if 'rel="last"' in part:
            import re
            match=re.search(r'[?&]page=(\d+)',part)
            if match:
                return int(match.group(1))
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
                logging.warning("Conn error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1

        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)

    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries,
                                start_date, end_date):
    """
    watchers => no created_at => full fetch
    but we'll still show date range + progress
    """
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return

    # show date range
    logging.debug(f"[DEBUG] {owner}/{repo} watchers [{start_date} - {end_date}]")

    page=1
    last_page=None
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Watchers => can't get page %d => break => %s/%s",page,owner,repo)
            break

        data=resp.json()
        if not data:
            break

        # parse last_page => show progress
        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] {owner}/{repo} watchers => {progress:.4f}% done")

        # no date => full fetch
        for user_obj in data:
            insert_watcher_record(conn, f"{owner}/{repo}", user_obj)

        if len(data)<100:
            break
        page+=1

def insert_watcher_record(conn, repo_name, user_json):
    user_login=user_json["login"]
    import json
    raw_str=json.dumps(user_json,ensure_ascii=False)
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

def list_forks_single_thread(conn, owner, repo,
                             start_date, end_date, enabled,
                             session,
                             handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks",owner,repo)
        return

    # show date range
    logging.debug(f"[DEBUG] {owner}/{repo} forks [{start_date} - {end_date}]")

    page=1
    last_page=None
    while True:
        old_base=start_date
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,old_base,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop forks mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={"sort":"oldest","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Forks => can't get page %d => break => %s/%s",page,owner,repo)
            break

        data=resp.json()
        if not data:
            break

        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] {owner}/{repo} forks => {progress:.4f}% done")

        for fork in data:
            cstr=fork.get("created_at")
            if not cstr:
                continue
            cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            if cdt<start_date:
                continue
            if cdt>end_date:
                continue
            insert_fork_record(conn,f"{owner}/{repo}",fork)

        if len(data)<100:
            break
        page+=1

def insert_fork_record(conn, repo_name, fork_json):
    fork_id=fork_json["id"]
    cstr=fork_json["created_at"]
    cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
    import json
    raw_str=json.dumps(fork_json,ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO forks (repo_name, fork_id, created_at, raw_json)
    VALUES (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,fork_id,cdt,raw_str))
    conn.commit()
    c.close()

def list_stars_single_thread(conn, owner, repo,
                             start_date, end_date, enabled,
                             session, handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars",owner,repo)
        return

    logging.debug(f"[DEBUG] {owner}/{repo} stars [{start_date} - {end_date}]")

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"
    page=1
    last_page=None

    while True:
        old_base=start_date
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,old_base,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop stars mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Stars => can't get page %d => break => %s/%s",page,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] {owner}/{repo} stars => {progress:.4f}% done")

        import json
        for stargazer in data:
            sstr=stargazer.get("starred_at")
            if not sstr:
                continue
            sdt=datetime.strptime(sstr,"%Y-%m-%dT%H:%M:%SZ")
            if sdt<start_date:
                continue
            if sdt>end_date:
                continue
            raw_str=json.dumps(stargazer,ensure_ascii=False)
            insert_star_record(conn,f"{owner}/{repo}",stargazer,sdt)

        if len(data)<100:
            break
        page+=1

    session.headers["Accept"]=old_accept

def insert_star_record(conn, repo_name, star_json, starred_dt):
    user_login=star_json["user"]["login"]
    import json
    raw_str=json.dumps(star_json,ensure_ascii=False)
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
