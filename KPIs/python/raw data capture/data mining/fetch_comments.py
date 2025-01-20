# fetch_comments.py
"""
Lists issue comments => skip if comment.created_at>baseline_date
Also fetch comment reactions => skip if reaction.created_at>baseline_date
We store them so that they appear on the first run.
"""

import logging
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def fetch_comments_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                  session, handle_rate_limit_func):
    """
    For each issue in 'issues' table for this repo, fetch & store comments (skip new).
    Also fetch comment reactions for each comment.
    This ensures 'issue_comments' & 'comment_reactions' are populated on the first run.
    """
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip all comments",owner,repo)
        return
    c=conn.cursor()
    c.execute("SELECT issue_number FROM issues WHERE repo_name=%s",(f"{owner}/{repo}",))
    rows=c.fetchall()
    c.close()
    for (issue_num,) in rows:
        list_issue_comments_single_thread(conn, owner, repo, issue_num,
                                          baseline_date, enabled, session, handle_rate_limit_func)

def list_issue_comments_single_thread(conn, owner, repo, issue_number,
                                      baseline_date, enabled, session,
                                      handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip comments for issue #%d",owner,repo,issue_number)
        return
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop comments mid-run for #%d",owner,repo,issue_number)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (comments for #%d)",owner,repo,baseline_date,issue_number)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"
        params={
            "page":page,
            "per_page":50,
            "sort":"created",
            "direction":"asc"
        }
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Comments => HTTP %d => break for issue #%d in %s/%s",
                            resp.status_code,issue_number,owner,repo)
            break

        data=resp.json()
        if not data:
            break

        for cmt in data:
            c_created_str=cmt["created_at"]
            c_created_dt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and c_created_dt>baseline_date:
                continue
            insert_comment_record(conn,f"{owner}/{repo}",issue_number,cmt)

            # fetch comment reactions => skip if reaction.created_at>baseline_date
            fetch_comment_reactions_single_thread(conn, owner, repo, issue_number, cmt["id"],
                                                 baseline_date, new_en, session, handle_rate_limit_func)

        if len(data)<50:
            break
        page+=1

def insert_comment_record(conn, repo_name, issue_num, cmt_json):
    import json
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
                                         handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip comment_reactions for #%d cmt=%d",owner,repo,issue_number,comment_id)
        return
    new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
    if new_en==0:
        logging.info("Disabled mid-run => skip comment reactions.")
        return
    if new_base!=baseline_date:
        baseline_date=new_base

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"
    reac_url=f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{comment_id}/reactions"
    resp=session.get(reac_url)
    handle_rate_limit_func(resp)
    session.headers["Accept"]=old_accept

    if resp.status_code!=200:
        logging.warning("Comment Reactions => HTTP %d => skip cmt_id=%d in %s/%s",
                        resp.status_code,comment_id,owner,repo)
        return
    data=resp.json()
    from datetime import datetime
    import json
    for reac in data:
        reac_created_str=reac["created_at"]
        reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
        if baseline_date and reac_created_dt>baseline_date:
            continue
        insert_comment_reaction(conn, repo_name, issue_number, comment_id, reac)

def insert_comment_reaction(conn, repo_name, issue_num, comment_id, reac_json):
    import json
    reac_id=reac_json["id"]
    reac_created_str=reac_json["created_at"]
    from datetime import datetime
    reac_created_dt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(reac_json, ensure_ascii=False)
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
