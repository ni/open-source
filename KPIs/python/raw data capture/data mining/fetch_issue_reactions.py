# fetch_issue_reactions.py
"""
Fetch Reactions on the Issue object => skip if reaction.created_at>baseline_date
We do GET /repos/{owner}/{repo}/issues/{issue_number}/reactions (Squirrel-Girl preview).
"""

import logging
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def fetch_issue_reactions_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                         session, handle_rate_limit_func):
    """
    For each issue in 'issues' table for this repo, fetch issue-level reactions => skip if new.
    This ensures 'issue_reactions' is populated on first run.
    """
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_reactions",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()
    for (issue_num,) in rows:
        fetch_issue_reactions_single_thread(conn, owner, repo, issue_num,
                                            baseline_date, enabled, session,
                                            handle_rate_limit_func)

def fetch_issue_reactions_single_thread(conn, owner, repo, issue_number,
                                        baseline_date, enabled, session,
                                        handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issue_reactions for #%d",owner,repo,issue_number)
        return
    new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
    if new_en==0:
        logging.info("Repo %s/%s => toggled disabled => skip issue_reactions mid-run for #%d",owner,repo,issue_number)
        return
    if new_base!=baseline_date:
        baseline_date=new_base
        logging.info("Repo %s/%s => baseline changed => now %s (issue_reactions for #%d)",owner,repo,baseline_date,issue_number)

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"
    url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/reactions"
    resp=session.get(url)
    handle_rate_limit_func(resp)
    session.headers["Accept"]=old_accept

    if resp.status_code!=200:
        logging.warning("Issue Reactions => HTTP %d => skip for #%d in %s/%s",resp.status_code,issue_number,owner,repo)
        return
    data=resp.json()
    for reac in data:
        reac_created_str=reac["created_at"]
        reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
        if baseline_date and reac_created_dt>baseline_date:
            continue
        insert_issue_reaction(conn, f"{owner}/{repo}", issue_number, reac)

def insert_issue_reaction(conn, repo_name, issue_num, reac_json):
    reac_id=reac_json["id"]
    reac_created_str=reac_json["created_at"]
    reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
    import json
    raw_str=json.dumps(reac_json, ensure_ascii=False)
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
    c.execute(sql,(repo_name, issue_num, reac_id, reac_created_dt, raw_str))
    conn.commit()
    c.close()
