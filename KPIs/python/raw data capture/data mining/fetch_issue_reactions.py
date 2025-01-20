# fetch_issue_reactions.py
import logging
import time
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    for attempt in range(1,max_retries+1):
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code==200:
            return (resp,True)
        elif resp.status_code in (403,429):
            logging.warning("HTTP %d => attempt %d/%d => will retry => %s",
                            resp.status_code,attempt,max_retries,url)
            time.sleep(5)
        else:
            logging.warning("HTTP %d => attempt %d => break => %s",resp.status_code,attempt,url)
            return (resp,False)
    logging.warning("Exceed max_retries => give up => url=%s",url)
    return (None,False)

def fetch_issue_reactions_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                         session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_reactions",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()
    for (issue_num,) in rows:
        fetch_issue_reactions_single_thread(
            conn, owner, repo, issue_num,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

def fetch_issue_reactions_single_thread(conn, owner, repo, issue_number,
                                        baseline_date, enabled, session,
                                        handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_reactions => #%d",owner,repo,issue_number)
        return
    new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
    if new_en==0:
        logging.info("Repo %s/%s => toggled disabled => skip issue_reactions mid-run => #%d",owner,repo,issue_number)
        return
    if new_base!=baseline_date:
        baseline_date=new_base
        logging.info("Repo %s/%s => baseline changed => now %s (issue_reactions for #%d)",
                     owner,repo,baseline_date,issue_number)

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"
    url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/reactions"
    (resp,success)=robust_get_page(
        session, url, params={},
        handle_rate_limit_func=handle_rate_limit_func,
        max_retries=max_retries
    )
    session.headers["Accept"]=old_accept

    if not success:
        logging.warning("Issue Reactions => skip => issue #%d => %s/%s",issue_number,owner,repo)
        return
    data=resp.json()
    for reac in data:
        reac_created_str=reac["created_at"]
        reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
        if baseline_date and reac_created_dt>baseline_date:
            continue
        insert_issue_reaction(conn,f"{owner}/{repo}",issue_number,reac)

def insert_issue_reaction(conn, repo_name, issue_num, reac_json):
    reac_id=reac_json["id"]
    reac_created_str=reac_json["created_at"]
    reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
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
    c.execute(sql,(repo_name,issue_num,reac_id,reac_created_dt,raw_str))
    conn.commit()
    c.close()
