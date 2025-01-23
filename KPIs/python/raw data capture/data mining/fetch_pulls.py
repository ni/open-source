# fetch_pulls.py
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
            except requests.exceptions.ConnectionError as e:
                logging.warning("Conn error => local mini-retry %d/%d => %s",
                                local_attempt,mini_retry_attempts,url)
                time.sleep(3)
                local_attempt+=1

        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => give up => %s",url)
            return (None,False)

    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_pulls_single_thread(conn, owner, repo, baseline_date, enabled,
                             session, handle_rate_limit_func, max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip pulls",owner,repo)
        return
    page=1
    while True:
        old_base=baseline_date
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,old_base,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop pulls mid-run",owner,repo)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (pulls).",
                         owner,repo,baseline_date)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={
            "state":"all",
            "sort":"created",
            "direction":"asc",
            "page":page,
            "per_page":100
        }
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Pulls => cannot get page %d => break => %s/%s",page,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for item in data:
            if "pull_request" not in item:
                continue
            c_created_str=item.get("created_at")
            if c_created_str:
                cdt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
                if baseline_date and cdt>baseline_date:
                    continue
                insert_pull_record(conn, f"{owner}/{repo}", item["number"], cdt)

        if len(data)<100:
            break
        page+=1

def insert_pull_record(conn, repo_name, pull_number, created_dt):
    c=conn.cursor()
    sql="""
    INSERT INTO pulls (repo_name, pull_number, created_at)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at)
    """
    c.execute(sql,(repo_name,pull_number,created_dt))
    conn.commit()
    c.close()
