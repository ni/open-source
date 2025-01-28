# fetch_labels.py

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
                    logging.warning("[ossmining/labels] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[ossmining/labels] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[ossmining/labels] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[ossmining/labels] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[ossmining/labels] Exceeded max_retries => give up => %s",url)
    return (None,False)

def fetch_labels_single_thread(conn, owner, repo, enabled,
                               session, handle_rate_limit_func,
                               max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip labels",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/labels"
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
        for lbl in data:
            if store_label(conn,repo_name,lbl):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[ossmining/labels] page=%d/%d => %.3f%% => inserted %d new => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[ossmining/labels] => total so far %d out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[ossmining/labels] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(data)<50:
            break
        page+=1

    logging.info("[ossmining/labels] Done => total inserted %d => %s",total_inserted,repo_name)

def store_label(conn, repo_name, lbl_obj):
    c=conn.cursor()
    lbl_name=lbl_obj["name"]
    # check if existing
    c.execute("SELECT label_name FROM repo_labels WHERE repo_name=%s AND label_name=%s",
              (repo_name,lbl_name))
    row=c.fetchone()
    if row:
        # update
        update_label(c, conn, repo_name, lbl_obj)
        c.close()
        return False
    else:
        insert_label(c, conn, repo_name, lbl_obj)
        c.close()
        return True

def insert_label(c, conn, repo_name, lbl_obj):
    import json
    name=lbl_obj["name"]
    color=lbl_obj.get("color","")
    desc=lbl_obj.get("description","")
    raw_str=json.dumps(lbl_obj, ensure_ascii=False)
    sql="""
    INSERT INTO repo_labels
      (repo_name, label_name, color, description)
    VALUES
      (%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,name,color,desc))
    conn.commit()

def update_label(c, conn, repo_name, lbl_obj):
    import json
    name=lbl_obj["name"]
    color=lbl_obj.get("color","")
    desc=lbl_obj.get("description","")

    sql="""
    UPDATE repo_labels
    SET color=%s, description=%s
    WHERE repo_name=%s AND label_name=%s
    """
    c.execute(sql,(color,desc,repo_name,name))
    conn.commit()
