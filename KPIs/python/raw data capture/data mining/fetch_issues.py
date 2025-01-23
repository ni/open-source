# fetch_issues.py
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
                    logging.warning("HTTP %d => attempt %d/%d => will retry => %s",
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

def get_max_issue_number(conn, repo_name):
    c=conn.cursor()
    c.execute("SELECT MAX(issue_number) FROM issues WHERE repo_name=%s",(repo_name,))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def list_issues_single_thread(conn, owner, repo, enabled,
                              session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issues",owner,repo)
        return

    repo_name=f"{owner}/{repo}"
    highest_known=get_max_issue_number(conn,repo_name)
    logging.debug(f"[DEBUG] {repo_name} => highest_known_issue={highest_known}")

    page=1
    while True:
        old_val=highest_known
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,None,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop issues mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={"state":"all","sort":"created","direction":"asc","page":page,"per_page":100}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Issues => page %d => skip => %s",page,repo_name)
            break
        data=resp.json()
        if not data:
            break

        new_count=0
        for item in data:
            if "pull_request" in item:
                continue
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

        if new_count<100:
            break
        page+=1

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
