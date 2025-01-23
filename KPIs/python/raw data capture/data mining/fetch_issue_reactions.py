# fetch_issue_reactions.py
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

def get_max_reaction_id_for_issue(conn, repo_name, issue_num):
    c=conn.cursor()
    c.execute("""
       SELECT MAX(reaction_id) FROM issue_reactions
       WHERE repo_name=%s AND issue_number=%s
    """,(repo_name,issue_num))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return 0

def fetch_issue_reactions_for_all_issues(conn, owner, repo, enabled,
                                         session, handle_rate_limit_func,
                                         max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_reactions",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(repo_name,))
    rows=c.fetchall()
    c.close()
    for (issue_num,) in rows:
        fetch_issue_reactions_single_thread(conn, repo_name, issue_num,
                                            enabled, session,
                                            handle_rate_limit_func,
                                            max_retries)

def fetch_issue_reactions_single_thread(conn, repo_name, issue_num,
                                        enabled, session,
                                        handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("%s => disabled => skip => issue_reactions => #%d",repo_name,issue_num)
        return
    highest_rid=get_max_reaction_id_for_issue(conn,repo_name,issue_num)
    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"
    url=f"https://api.github.com/repos/{repo_name}/issues/{issue_num}/reactions"
    (resp,success)=robust_get_page(session,url,{},handle_rate_limit_func,max_retries)
    session.headers["Accept"]=old_accept
    if not success:
        return
    data=resp.json()
    if not data:
        return

    new_count=0
    for reac in data:
        reac_id=reac["id"]
        if reac_id<=highest_rid:
            continue
        c_str=reac.get("created_at")
        cdt=None
        if c_str:
            cdt=datetime.strptime(c_str,"%Y-%m-%dT%H:%M:%SZ")
        insert_issue_reaction(conn,repo_name,issue_num,reac_id,cdt,reac)
        new_count+=1
        if reac_id>highest_rid:
            highest_rid=reac_id

def insert_issue_reaction(conn, repo_name, issue_num, reac_id, created_dt, reac_json):
    import json
    raw_str=json.dumps(reac_json,ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO issue_reactions
      (repo_name, issue_number, reaction_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,issue_num,reac_id,created_dt,raw_str))
    conn.commit()
    c.close()
