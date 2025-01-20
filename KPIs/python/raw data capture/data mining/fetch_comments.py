# fetch_comments.py
"""
Lists issue comments => skip if comment.created_at>baseline_date
Also fetch comment reactions => skip if reaction.created_at>baseline_date
We re-try 403,429,500,502,503,504 => skip after max_retries
handle_rate_limit_func => logs rate-limit
"""

import logging
import time
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    for attempt in range(1, max_retries+1):
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
    logging.warning("Exceeded max_retries => giving up => url=%s",url)
    return (None,False)

def fetch_comments_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                  session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip comments",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()
    for (issue_num,) in rows:
        list_issue_comments_single_thread(
            conn, owner, repo, issue_num,
            baseline_date, enabled, session,
            handle_rate_limit_func, max_retries
        )

def list_issue_comments_single_thread(conn, owner, repo, issue_number,
                                      baseline_date, enabled, session,
                                      handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip comments => issue #%d",owner,repo,issue_number)
        return
    page=1
    full_repo_name=f"{owner}/{repo}"
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop comments => issue #%d mid-run",
                         owner,repo,issue_number)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (comments for #%d)",
                         owner,repo,baseline_date,issue_number)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"
        params={
            "page": page,
            "per_page":50,
            "sort":"created",
            "direction":"asc"
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Comments => cannot get page %d => stop => issue #%d => %s/%s",
                            page,issue_number,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for cmt in data:
            c_created_str=cmt["created_at"]
            c_created_dt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and c_created_dt>baseline_date:
                continue
            insert_comment_record(conn, full_repo_name, issue_number, cmt)
            # also fetch comment reactions => skip if reaction.created_at>baseline_date
            fetch_comment_reactions_single_thread(
                conn, owner, repo, issue_number, cmt["id"],
                baseline_date, new_en, session,
                handle_rate_limit_func, max_retries
            )
        if len(data)<50:
            break
        page+=1

def insert_comment_record(conn, repo_name, issue_num, cmt_json):
    cmt_id=cmt_json["id"]
    c_created_str=cmt_json["created_at"]
    c_created_dt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
    body=cmt_json.get("body","")
    c=conn.cursor()
    sql="""
    INSERT INTO issue_comments (repo_name, issue_number, comment_id, created_at, body)
    VALUES (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      body=VALUES(body)
    """
    c.execute(sql,(repo_name,issue_num,cmt_id,c_created_dt,body))
    conn.commit()
    c.close()

def fetch_comment_reactions_single_thread(conn, owner, repo, issue_number, comment_id,
                                         baseline_date, enabled, session,
                                         handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip comment_reactions => #%d cmt=%d",
                     owner,repo,issue_number,comment_id)
        return
    new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
    if new_en==0:
        logging.info("Disabled mid-run => skip comment reactions => issue #%d cmt=%d",
                     issue_number,comment_id)
        return
    if new_base!=baseline_date:
        baseline_date=new_base

    url=f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{comment_id}/reactions"
    (resp,success)=robust_get_page(session,url,params={},
                                   handle_rate_limit_func=handle_rate_limit_func,
                                   max_retries=max_retries)
    if not success:
        logging.warning("Comment Reactions => skip => cmt_id=%d => issue #%d => %s/%s",
                        comment_id, issue_number, owner, repo)
        return
    data=resp.json()
    for reac in data:
        reac_created_str=reac["created_at"]
        reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
        if baseline_date and reac_created_dt>baseline_date:
            continue
        insert_comment_reaction(conn, f"{owner}/{repo}", issue_number, comment_id, reac)

def insert_comment_reaction(conn, repo_name, issue_num, comment_id, reac_json):
    reac_id=reac_json["id"]
    reac_created_str=reac_json["created_at"]
    reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(reac_json,ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO comment_reactions
      (repo_name, issue_number, comment_id, reaction_id, created_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name,issue_num,comment_id,reac_id,reac_created_dt,raw_str))
    conn.commit()
    c.close()
