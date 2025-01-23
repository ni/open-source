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
                    logging.warning("HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("HTTP %d => attempt %d => break => %s",
                                    resp.status_code, attempt, url)
                    return (resp,False)
                break
            except requests.exceptions.ConnectionError:
                logging.warning("Conn error => local mini-retry %d/%d => %s",
                                local_attempt,mini_retry_attempts,url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def fetch_comments_for_all_issues(conn, owner, repo, baseline_date, enabled,
                                  session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip all comments",owner,repo)
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
        old_base=baseline_date
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,old_base,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop => issue #%d mid-run",
                         owner,repo,issue_number)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (comments for #%d)",
                         owner,repo,baseline_date,issue_number)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"
        params={
            "page":page,
            "per_page":50,
            "sort":"created",
            "direction":"asc"
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Comments => can't get page %d => break => issue #%d => %s/%s",
                            page,issue_number,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for cmt in data:
            c_created_str=cmt.get("created_at")
            if not c_created_str:
                continue
            cdt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and cdt>baseline_date:
                continue
            insert_comment_record(conn, full_repo_name, issue_number, cmt)
            # fetch comment reactions => skip new
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
    cdt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
    body=cmt_json.get("body","")
    c=conn.cursor()
    sql="""
    INSERT INTO issue_comments (repo_name, issue_number, comment_id, created_at, body)
    VALUES (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      body=VALUES(body)
    """
    c.execute(sql,(repo_name,issue_num,cmt_id,cdt,body))
    conn.commit()
    c.close()

def fetch_comment_reactions_single_thread(conn, owner, repo, issue_number, comment_id,
                                         baseline_date, enabled, session,
                                         handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip comment_reactions => #%d cmt=%d",
                     owner,repo,issue_number,comment_id)
        return
    old_base=baseline_date
    old_en=enabled
    new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,old_base,old_en)
    if new_en==0:
        logging.info("Repo %s/%s => toggled disabled => skip => issue #%d cmt=%d",
                     owner,repo,issue_number,comment_id)
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
        reac_created_str=reac.get("created_at")
        if not reac_created_str:
            continue
        rdt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
        if baseline_date and rdt>baseline_date:
            continue
        insert_comment_reaction(conn,f"{owner}/{repo}",issue_number,comment_id,reac)

def insert_comment_reaction(conn, repo_name, issue_num, comment_id, reac_json):
    reac_id=reac_json["id"]
    reac_created_str=reac_json["created_at"]
    rdt=datetime.strptime(reac_created_str,"%Y-%m-%dT%H:%M:%SZ")
    import json
    raw_str=json.dumps(reac_json, ensure_ascii=False)
    c=conn.cursor()
    sql="""
    INSERT INTO comment_reactions
      (repo_name, issue_number, comment_id, reaction_id, created_at, raw_json)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at),
      raw_json=VALUES(raw_json)
    """
    c.execute(sql,(repo_name, issue_num, comment_id, reac_id, rdt, raw_str))
    conn.commit()
    c.close()
