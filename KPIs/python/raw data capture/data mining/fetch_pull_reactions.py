# fetch_pull_reactions.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

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

def fetch_pull_reactions_for_all_pulls(conn, owner, repo, enabled,
                                       session, handle_rate_limit_func,
                                       max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pull_reactions",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("SELECT pull_number FROM pulls WHERE repo_name=%s",(repo_name,))
    rows=c.fetchall()
    c.close()

    old_accept=session.headers.get("Accept","")
    session.headers["Accept"]="application/vnd.github.squirrel-girl-preview+json"

    for (pull_num,) in rows:
        list_pull_reactions_single_thread(conn, repo_name, pull_num, enabled,
                                          session, handle_rate_limit_func,
                                          max_retries)

    session.headers["Accept"]=old_accept

def list_pull_reactions_single_thread(conn, repo_name, pull_num, enabled,
                                      session, handle_rate_limit_func,
                                      max_retries):
    if enabled==0:
        return
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{repo_name}/issues/{pull_num}/reactions"
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
            if store_pull_reaction(conn,repo_name,pull_num,reac):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/pull_reactions] PR#%d => page=%d/%d => %.4f%% => inserted %d => %s",
                          pull_num,page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/pull_reactions] => so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/pull_reactions] PR#%d => page=%d => inserted %d => no last_page => %s",
                          pull_num,page,new_count,repo_name)

        if len(data)<50:
            break
        page+=1

    logging.info("[deadbird/pull_reactions] PR#%d => total inserted %d => %s",
                 pull_num,total_inserted,repo_name)

def store_pull_reaction(conn, repo_name, pull_num, reac_obj):
    c=conn.cursor()
    reac_id=reac_obj["id"]
    c.execute("""
      SELECT reaction_id FROM pull_reactions
      WHERE repo_name=%s AND pull_number=%s AND reaction_id=%s
    """,(repo_name,pull_num,reac_id))
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
        INSERT INTO pull_reactions
          (repo_name, pull_number, reaction_id, created_at, raw_json)
        VALUES
          (%s,%s,%s,%s,%s)
        """
        c.execute(sql,(repo_name,pull_num,reac_id,created_dt,raw_str))
        conn.commit()
        c.close()
        return True
