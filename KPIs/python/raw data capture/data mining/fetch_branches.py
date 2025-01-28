# fetch_branches.py

import logging
import time
import requests
from datetime import datetime

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
                    logging.warning("[deadbird/branches] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/branches] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/branches] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/branches] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/branches] Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_branches_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip branches",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/branches"
        params={"page":page,"per_page":50}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)
        total_items=0
        if last_page:
            total_items=last_page*50

        new_count=0
        for br in data:
            if store_branch_record(conn,repo_name,br):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/branches] page=%d/%d => %.3f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/branches] => so far %d out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/branches] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(data)<50:
            break
        page+=1

    logging.info("[deadbird/branches] Done => total inserted %d => %s",total_inserted,repo_name)

def store_branch_record(conn, repo_name, br_obj):
    c=conn.cursor()
    branch_name=br_obj["name"]
    c.execute("""
      SELECT branch_name FROM branches
      WHERE repo_name=%s AND branch_name=%s
    """,(repo_name,branch_name))
    row=c.fetchone()
    if row:
        update_branch_record(c,conn,repo_name,branch_name,br_obj)
        c.close()
        return False
    else:
        insert_branch_record(c,conn,repo_name,branch_name,br_obj)
        c.close()
        return True

def insert_branch_record(c, conn, repo_name, branch_name, br_obj):
    import json
    commit_sha=br_obj.get("commit",{}).get("sha","")
    protected=1 if br_obj.get("protected",False) else 0
    raw_str=json.dumps(br_obj, ensure_ascii=False)
    sql="""
    INSERT INTO branches
      (repo_name, branch_name, commit_sha, protected, raw_json)
    VALUES
      (%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,branch_name,commit_sha,protected,raw_str))
    conn.commit()

def update_branch_record(c, conn, repo_name, branch_name, br_obj):
    import json
    commit_sha=br_obj.get("commit",{}).get("sha","")
    protected=1 if br_obj.get("protected",False) else 0
    raw_str=json.dumps(br_obj, ensure_ascii=False)
    sql="""
    UPDATE branches
    SET commit_sha=%s, protected=%s, raw_json=%s
    WHERE repo_name=%s AND branch_name=%s
    """
    c.execute(sql,(commit_sha,protected,raw_str,repo_name,branch_name))
    conn.commit()
