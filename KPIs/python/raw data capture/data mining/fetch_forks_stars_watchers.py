# fetch_forks_stars_watchers.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

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

def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func, max_retries,
                                use_etags=True):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip watchers",owner,repo)
        return
    endpoint="watchers"
    repo_name=f"{owner}/{repo}"
    if not use_etags:
        watchers_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn,owner,repo,endpoint)
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={"page":page,"per_page":100}
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
        for w in data:
            if insert_watcher_record(conn, repo_name, w):
                new_count+=1
        total_inserted+=new_count

        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if len(data)<100:
            break
        page+=1

    # watchers => no last_updated usage, store new ETag anyway
    update_endpoint_state(conn,owner,repo,endpoint,etag_val,last_upd)
    logging.info("[deadbird/watchers-etag] Done => total inserted %d => %s",total_inserted,repo_name)

def watchers_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/watchers-old] scanning from page=1 => unlimited => %s/%s",owner,repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint="watchers-old")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for w in data:
            if insert_watcher_record(conn,f"{owner}/{repo}",w):
                new_count+=1
        total_inserted+=new_count
        if len(data)<100:
            break
        page+=1
    logging.info("[deadbird/watchers-old] total inserted %d => %s/%s",total_inserted,owner,repo)

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

def list_forks_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func, max_retries,
                             use_etags=True):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip forks",owner,repo)
        return
    endpoint="forks"
    repo_name=f"{owner}/{repo}"
    if not use_etags:
        forks_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries)
        return

    etag_val, last_upd = get_endpoint_state(conn,owner,repo,endpoint)
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={"page":page,"per_page":100,"sort":"oldest"}
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
        for fk in data:
            if insert_fork_record(conn,repo_name,fk):
                new_count+=1
        total_inserted+=new_count

        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if len(data)<100:
            break
        page+=1

    update_endpoint_state(conn,owner,repo,endpoint,etag_val,last_upd)
    logging.info("[deadbird/forks-etag] Done => total inserted %d => %s",total_inserted,repo_name)

def forks_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/forks-old] scanning from page=1 => unlimited => %s/%s",owner,repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/forks"
        params={"page":page,"per_page":100,"sort":"oldest"}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint="forks-old")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for fk in data:
            if insert_fork_record(conn,f"{owner}/{repo}",fk):
                new_count+=1
        total_inserted+=new_count
        if len(data)<100:
            break
        page+=1
    logging.info("[deadbird/forks-old] total inserted %d => %s/%s",total_inserted,owner,repo)

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

def list_stars_single_thread(conn, owner, repo, enabled, baseline_dt,
                             session, handle_rate_limit_func, max_retries,
                             use_etags=True):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip stars",owner,repo)
        return
    endpoint="stars"
    repo_name=f"{owner}/{repo}"
    if not use_etags:
        stars_old_approach(conn, owner, repo, baseline_dt, session, handle_rate_limit_func, max_retries)
        return

    # ETag approach => stargazers => no ?since => rely on ETag + baseline skip
    etag_val, last_upd = get_endpoint_state(conn,owner,repo,endpoint)
    page=1
    last_page=None
    total_inserted=0
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={"page":page,"per_page":100}
        if etag_val:
            session.headers["If-None-Match"]=etag_val

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint=endpoint)

        if "If-None-Match" in session.headers:
            del session.headers["If-None-Match"]
        session.headers["Accept"]=old_accept

        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)

        new_count=0
        for st in data:
            starred_str=st.get("starred_at")
            if not starred_str:
                continue
            st_dt=datetime.strptime(starred_str,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_dt and st_dt>baseline_dt:
                # skip => older logic
                continue
            if insert_star_record(conn,repo_name,st,st_dt):
                new_count+=1
        total_inserted+=new_count

        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if len(data)<100:
            break
        page+=1

    update_endpoint_state(conn,owner,repo,endpoint,etag_val,last_upd)
    logging.info("[deadbird/stars-etag] Done => total inserted %d => %s",total_inserted,repo_name)

def stars_old_approach(conn, owner, repo, baseline_dt,
                       session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/stars-old] scanning => %s/%s => unlimited",owner,repo)
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.v3.star+json"
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params={"page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint="stars-old")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for st in data:
            starred_str=st.get("starred_at")
            if not starred_str:
                continue
            st_dt=datetime.strptime(starred_str,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_dt and st_dt>baseline_dt:
                continue
            if insert_star_record(conn,f"{owner}/{repo}",st,st_dt):
                new_count+=1
        total_inserted+=new_count
        if len(data)<100:
            break
        page+=1

    session.headers["Accept"]=old_accept
    logging.info("[deadbird/stars-old] total inserted %d => %s/%s",total_inserted,owner,repo)


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
