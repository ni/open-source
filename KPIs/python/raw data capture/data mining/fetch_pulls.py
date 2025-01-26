# fetch_pulls.py
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
                logging.warning("Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def get_max_pull_number(conn, repo_name):
    c=conn.cursor()
    c.execute("SELECT MAX(pull_number) FROM pulls WHERE repo_name=%s",(repo_name,))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def list_pulls_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pulls",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    highest_known=get_max_pull_number(conn,repo_name)
    page=1
    last_page=None
    while True:
        old_val=highest_known
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,None,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop pulls mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={"state":"all","sort":"created","direction":"asc","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Pulls => page %d => skip => %s",page,repo_name)
            break
        data=resp.json()
        if not data:
            break

        if last_page is None:
            last_page=get_last_page(resp)
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] pulls => page={page}/{last_page} => {progress:.3f}%% => {repo_name}")

        new_count=0
        for item in data:
            if "pull_request" not in item:
                continue
            pull_num=item["number"]
            if pull_num<=highest_known:
                continue
            cstr=item.get("created_at")
            cdt=None
            if cstr:
                cdt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            insert_pull_record(conn,repo_name,pull_num,cdt)
            new_count+=1
            if pull_num>highest_known:
                highest_known=pull_num
        if new_count<100:
            break
        page+=1

def insert_pull_record(conn, repo_name, pull_number, created_dt):
    c=conn.cursor()
    sql="""
    INSERT INTO pulls (repo_name, pull_number, created_at)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at)
    """
    c.execute(sql,(repo_name,pull_number,created_dt))
    conn.commit()
    c.close()
