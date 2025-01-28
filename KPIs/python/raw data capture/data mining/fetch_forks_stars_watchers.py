# fetch_forks_stars_watchers.py

import logging
import time
import requests
from datetime import datetime

def get_last_page(resp):
    link_header = resp.headers.get("Link")
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
                resp=session.get(url, params=params)
                handle_rate_limit_func(resp)
                if resp.status_code==200:
                    return (resp,True)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/forks_stars_watchers] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/forks_stars_watchers] HTTP %d => break => %s",
                                    resp.status_code,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/forks_stars_watchers] Connection error => local retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/forks_stars_watchers] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/forks_stars_watchers] Exceeded max_retries => give up => %s",url)
    return (None,False)

# 1) watchers => /repos/{owner}/{repo}/subscribers
def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*100

        new_count=0
        for w in data:
            if insert_watcher_record(conn,repo_name,w):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100
            logging.debug("[deadbird/watchers] page=%d/%d => %.4f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/watchers] => total so far %d out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/watchers] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)
        if len(data)<100:
            break
        page+=1
    logging.info("[deadbird/watchers] Done => total inserted %d => %s",total_inserted,repo_name)

def insert_watcher_record(conn, repo_name, user_obj):
    c=conn.cursor()
    user_login=user_obj.get("login","")
    c.execute("""
      SELECT user_login FROM watchers
      WHERE repo_name=%s AND user_login=%s
    """,(repo_name,user_login))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        user_json=json.dumps(user_obj, ensure_ascii=False)
        sql="""
        INSERT INTO watchers
          (repo_name, user_login, raw_json)
        VALUES
          (%s,%s,%s)
        """
        c.execute(sql,(repo_name,user_login,user_json))
        conn.commit()
        c.close()
        return True

# 2) forks => /repos/{owner}/{repo}/forks => sort=oldest
def list_forks_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={"page":page,"per_page":100,"sort":"oldest"}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*100

        new_count=0
        for fk in data:
            if insert_fork_record(conn,repo_name,fk):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100
            logging.debug("[deadbird/forks] page=%d/%d => %.4f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/forks] => total so far %d out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/forks] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)
        if len(data)<100:
            break
        page+=1
    logging.info("[deadbird/forks] Done => total inserted %d => %s",total_inserted,repo_name)

def insert_fork_record(conn, repo_name, fork_obj):
    c=conn.cursor()
    fork_id=fork_obj.get("id")
    c.execute("""
      SELECT fork_id FROM forks
      WHERE repo_name=%s AND fork_id=%s
    """,(repo_name,fork_id))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        raw_str=json.dumps(fork_obj, ensure_ascii=False)
        created_str=fork_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
        sql="""
        INSERT INTO forks
          (repo_name, fork_id, created_at, raw_json)
        VALUES
          (%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,fork_id,created_dt,raw_str))
        conn.commit()
        c.close()
        return True

# 3) stars => /repos/{owner}/{repo}/stargazers => Accept: star+json => skip if starred_at > baseline_date
def list_stars_single_thread(conn, owner, repo, enabled, baseline_dt,
                             session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*100

        new_count=0
        for st in data:
            starred_str=st.get("starred_at")
            if starred_str:
                st_dt=datetime.strptime(starred_str,"%Y-%m-%dT%H:%M:%SZ")
                if baseline_dt and st_dt>baseline_dt:
                    # skip
                    continue
                if insert_star_record(conn,repo_name,st,st_dt):
                    new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100
            logging.debug("[deadbird/stars] page=%d/%d => %.4f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/stars] => so far %d out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/stars] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)
        if len(data)<100:
            break
        page+=1

    session.headers["Accept"]=old_accept
    logging.info("[deadbird/stars] Done => total inserted %d => %s",total_inserted,repo_name)

def insert_star_record(conn, repo_name, star_obj, st_dt):
    c=conn.cursor()
    user=star_obj.get("user",{})
    user_login=user.get("login","")
    c.execute("""
      SELECT user_login FROM stars
      WHERE repo_name=%s AND user_login=%s AND starred_at=%s
    """,(repo_name,user_login,st_dt))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        raw_str=json.dumps(star_obj, ensure_ascii=False)
        sql="""
        INSERT INTO stars
          (repo_name, user_login, starred_at, raw_json)
        VALUES
          (%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,user_login,st_dt,raw_str))
        conn.commit()
        c.close()
        return True
