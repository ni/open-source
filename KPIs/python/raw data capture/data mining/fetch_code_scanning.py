# fetch_code_scanning.py

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
                elif resp.status_code==403:
                    # fallback => skip
                    logging.warning("[deadbird/code_scanning] 403 => skip => insufficient perms => %s",url)
                    return (resp,False)
                elif resp.status_code in (429,500,502,503,504):
                    logging.warning("[deadbird/code_scanning] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/code_scanning] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/code_scanning] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/code_scanning] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/code_scanning] Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_code_scanning_alerts_single_thread(conn, owner, repo, enabled,
                                            session, handle_rate_limit_func,
                                            max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip code_scanning",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/code-scanning/alerts"
        params={"page":page,"per_page":50}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if resp and resp.status_code==403:
            # fallback => skip
            logging.warning("[deadbird/code_scanning] 403 => insufficient permission => skip => %s/%s",owner,repo)
            break
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
        for alert in data:
            if store_code_scanning_alert(conn,repo_name,alert):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/code_scanning] page=%d/%d => %.3f%% => inserted %d => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/code_scanning] => so far %d out of ~%d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/code_scanning] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(data)<50:
            break
        page+=1

    logging.info("[deadbird/code_scanning] Done => total inserted %d => %s",total_inserted,repo_name)

def store_code_scanning_alert(conn, repo_name, alert_obj):
    c=conn.cursor()
    alert_number=alert_obj["number"]
    c.execute("""
      SELECT alert_number FROM code_scanning_alerts
      WHERE repo_name=%s AND alert_number=%s
    """,(repo_name,alert_number))
    row=c.fetchone()
    if row:
        update_code_scanning_alert(c,conn,repo_name,alert_number,alert_obj)
        c.close()
        return False
    else:
        insert_code_scanning_alert(c,conn,repo_name,alert_number,alert_obj)
        c.close()
        return True

def insert_code_scanning_alert(c, conn, repo_name, alert_number, alert_obj):
    import json
    state=alert_obj.get("state","")
    rule=alert_obj.get("rule",{})
    rule_id=rule.get("id","")
    rule_name=rule.get("name","")

    created_str=alert_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=alert_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(alert_obj, ensure_ascii=False)
    sql="""
    INSERT INTO code_scanning_alerts
     (repo_name, alert_number, state, rule_id, rule_name,
      created_at, updated_at, raw_json)
    VALUES
     (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sql,(repo_name,alert_number,state,rule_id,rule_name,
                   created_dt,updated_dt,raw_str))
    conn.commit()

def update_code_scanning_alert(c, conn, repo_name, alert_number, alert_obj):
    import json
    state=alert_obj.get("state","")
    rule=alert_obj.get("rule",{})
    rule_id=rule.get("id","")
    rule_name=rule.get("name","")
    created_str=alert_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")
    updated_str=alert_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")
    raw_str=json.dumps(alert_obj, ensure_ascii=False)

    sql="""
    UPDATE code_scanning_alerts
    SET state=%s, rule_id=%s, rule_name=%s,
        created_at=%s, updated_at=%s, raw_json=%s
    WHERE repo_name=%s AND alert_number=%s
    """
    c.execute(sql,(state,rule_id,rule_name,created_dt,updated_dt,raw_str,
                   repo_name,alert_number))
    conn.commit()
