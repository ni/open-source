# fetch_code_scanning.py

import logging
import time
import requests
from datetime import datetime
from robust_fetch import robust_get_page

from etags import get_endpoint_state, update_endpoint_state

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

def list_code_scanning_alerts_single_thread(conn, owner, repo, enabled,
                                            session, handle_rate_limit_func,
                                            max_retries,
                                            use_etags=True):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip code_scanning",owner,repo)
        return
    endpoint="code_scanning"
    repo_name=f"{owner}/{repo}"
    if not use_etags:
        code_scanning_old_approach(conn,owner,repo,session,handle_rate_limit_func,max_retries)
        return

    etag_val, last_upd=get_endpoint_state(conn,owner,repo,endpoint)
    page=1
    last_page=None
    total_inserted=0

    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/code-scanning/alerts"
        params={"page":page,"per_page":50}
        if etag_val:
            session.headers["If-None-Match"]=etag_val

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint=endpoint)
        if "If-None-Match" in session.headers:
            del session.headers["If-None-Match"]
        if not success or not resp:
            # maybe 403 => skip
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)

        new_count=0
        for alert in data:
            if store_code_scanning_alert(conn,repo_name,alert):
                new_count+=1
        total_inserted+=new_count

        new_etag=resp.headers.get("ETag")
        if new_etag:
            etag_val=new_etag

        if len(data)<50:
            break
        page+=1

    # store final ETag => no last_updated for code scanning?
    update_endpoint_state(conn,owner,repo,endpoint,etag_val,last_upd)
    logging.info("[deadbird/code_scanning-etag] Done => total inserted %d => %s",total_inserted,repo_name)

def code_scanning_old_approach(conn, owner, repo, session, handle_rate_limit_func, max_retries):
    logging.info("[deadbird/code_scanning-old] => scanning => %s/%s => might skip if 403",owner,repo)
    page=1
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/code-scanning/alerts"
        params={"page":page,"per_page":50}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries,endpoint="code_scanning-old")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        new_count=0
        for alert in data:
            if store_code_scanning_alert(conn,f"{owner}/{repo}",alert):
                new_count+=1
        total_inserted+=new_count
        if len(data)<50:
            break
        page+=1

    logging.info("[deadbird/code_scanning-old] total inserted %d => %s/%s",total_inserted,owner,repo)

def store_code_scanning_alert(conn, repo_name, alert_obj):
    c=conn.cursor()
    alert_number=alert_obj["number"]
    c.execute("""
      SELECT alert_number FROM code_scanning_alerts
      WHERE repo_name=%s AND alert_number=%s
    """,(repo_name,alert_number))
    row=c.fetchone()
    if row:
        c.close()
        return False
    else:
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
        c.execute(sql,(repo_name,alert_number,state,rule_id,rule_name,created_dt,updated_dt,raw_str))
        conn.commit()
        c.close()
        return True
