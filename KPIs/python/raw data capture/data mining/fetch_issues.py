# fetch_issues.py

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
                    logging.warning("[deadbird/issues] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/issues] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/issues] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/issues] Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/issues] Exceeded max_retries => give up => %s",url)
    return (None,False)

def get_max_issue_number(conn, repo_name):
    c=conn.cursor()
    c.execute("SELECT MAX(issue_number) FROM issues WHERE repo_name=%s",(repo_name,))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def list_issues_single_thread(conn, owner, repo, enabled,
                              session, handle_rate_limit_func,
                              max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issues",owner,repo)
        return

    repo_name=f"{owner}/{repo}"
    highest_known=get_max_issue_number(conn,repo_name)
    page=1
    last_page=None
    total_inserted=0

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={"state":"all","sort":"created","direction":"asc","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            logging.warning("[deadbird/issues] page=%d => skip => %s",page,repo_name)
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
        for item in data:
            if "pull_request" in item:
                continue  # skip if it's a PR => in pulls table
            issue_num=item["number"]
            if issue_num<=highest_known:
                continue
            c_created_str=item.get("created_at")
            cdt=None
            if c_created_str:
                cdt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
            insert_issue_record(conn,repo_name,issue_num,cdt)
            new_count+=1
            if issue_num>highest_known:
                highest_known=issue_num

        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/issues] page=%d/%d => %.4f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/issues] => total so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/issues] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(data)<100:
            break
        page+=1

    logging.info("[deadbird/issues] Done => total inserted %d => %s",total_inserted,repo_name)

def insert_issue_record(conn, repo_name, issue_number, created_dt):
    c=conn.cursor()
    sql="""
    INSERT INTO issues (repo_name, issue_number, created_at)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at)
    """
    c.execute(sql,(repo_name,issue_number,created_dt))
    conn.commit()
    c.close()
