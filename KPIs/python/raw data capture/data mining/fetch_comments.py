# fetch_comments.py
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
                    logging.warning("HTTP %d => attempt %d/%d => re-try => %s",
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

def get_max_comment_id_for_issue(conn, repo_name, issue_num):
    c=conn.cursor()
    c.execute("""
        SELECT MAX(comment_id) FROM issue_comments
        WHERE repo_name=%s AND issue_number=%s
    """,(repo_name,issue_num))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def fetch_comments_for_all_issues(conn, owner, repo, enabled,
                                  session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip all comments",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(repo_name,))
    rows=c.fetchall()
    c.close()
    for (issue_num,) in rows:
        list_issue_comments_single_thread(conn, repo_name, issue_num,
                                          enabled, session,
                                          handle_rate_limit_func, max_retries)

def list_issue_comments_single_thread(conn, repo_name, issue_num,
                                      enabled, session,
                                      handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("%s => disabled => skip => issue #%d comments",repo_name,issue_num)
        return
    highest_cid=get_max_comment_id_for_issue(conn,repo_name,issue_num)
    page=1
    while True:
        url=f"https://api.github.com/repos/{repo_name}/issues/{issue_num}/comments"
        params={"page":page,"per_page":50,"sort":"created","direction":"asc"}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for cmt in data:
            c_id=cmt["id"]
            if c_id<=highest_cid:
                continue
            c_str=cmt.get("created_at")
            cdt=None
            if c_str:
                cdt=datetime.strptime(c_str,"%Y-%m-%dT%H:%M:%SZ")
            insert_comment_record(conn,repo_name,issue_num,c_id,cdt,cmt)
            new_count+=1
            if c_id>highest_cid:
                highest_cid=c_id
        if new_count<50:
            break
        page+=1

def insert_comment_record(conn, repo_name, issue_num, comment_id, created_dt, cmt_json):
    body=cmt_json.get("body","")
    import json
    c=conn.cursor()
    sql="""
    INSERT INTO issue_comments
      (repo_name, issue_number, comment_id, created_at, body)
    VALUES
      (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      body=VALUES(body)
    """
    c.execute(sql,(repo_name, issue_num, comment_id, created_dt, body))
    conn.commit()
    c.close()
