# fetch_pull_comment_reactions.py

import logging
import time
import requests
from datetime import datetime

def get_last_page(resp):
    link_header = resp.headers.get("Link")
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
                resp=session.get(url,params=params)
                handle_rate_limit_func(resp)
                if resp.status_code==200:
                    return (resp,True)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/pull_comment_reactions] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/pull_comment_reactions] HTTP %d => break => %s",
                                    resp.status_code,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/pull_comment_reactions] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/pull_comment_reactions] Exhausted => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/pull_comment_reactions] Exceeded => give up => %s",url)
    return (None,False)

def fetch_pull_comment_reactions_for_all_comments(conn, owner, repo, enabled,
                                                  session, handle_rate_limit_func,
                                                  max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_comment_reactions",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("""
      SELECT pull_number, comment_id
      FROM pull_review_comments
      WHERE repo_name=%s
    """,(repo_name,))
    rows=c.fetchall()
    c.close()

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"

    for (pull_num, cmt_id) in rows:
        list_pull_comment_reactions_single_thread(conn,repo_name,pull_num,cmt_id,
                                                  enabled,session,handle_rate_limit_func,
                                                  max_retries)

    session.headers["Accept"]=old_accept

def list_pull_comment_reactions_single_thread(conn, repo_name, pull_num, comment_id,
                                              enabled, session, handle_rate_limit_func,
                                              max_retries):
    if enabled==0:
        return
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{repo_name}/pulls/comments/{comment_id}/reactions"
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
        for reac in data:
            if store_pull_comment_reaction(conn,repo_name,pull_num,comment_id,reac):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/pull_comment_reactions] PR#%d cmt#%d => page=%d/%d => %.4f%% => inserted %d => %s",
                          pull_num,comment_id,page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/pull_comment_reactions] => so far %d of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/pull_comment_reactions] PR#%d cmt#%d => page=%d => inserted %d => no last_page => %s",
                          pull_num,comment_id,page,new_count,repo_name)

        if len(data)<50:
            break
        page+=1

    logging.info("[deadbird/pull_comment_reactions] PR#%d cmt#%d => total inserted %d => %s",
                 pull_num,comment_id,total_inserted,repo_name)

def store_pull_comment_reaction(conn, repo_name, pull_num, comment_id, reac_obj):
    c=conn.cursor()
    reac_id=reac_obj["id"]
    c.execute("""
      SELECT reaction_id FROM pull_comment_reactions
      WHERE repo_name=%s AND pull_number=%s AND comment_id=%s AND reaction_id=%s
    """,(repo_name,pull_num,comment_id,reac_id))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        created_str=reac_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
        raw_str=json.dumps(reac_obj, ensure_ascii=False)
        sql="""
        INSERT INTO pull_comment_reactions
          (repo_name, pull_number, comment_id, reaction_id, created_at, raw_json)
        VALUES
          (%s,%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,pull_num,comment_id,reac_id,created_dt,raw_str))
        conn.commit()
        c.close()
        return True
