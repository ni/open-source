# fetch_issues.py
import logging
import time
import requests
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def get_last_page(resp):
    """
    Parse the Link header for rel="last" => return last_page as int or None
    """
    link_header=resp.headers.get("Link")
    if not link_header:
        return None
    parts=link_header.split(',')
    for part in parts:
        # example part: <https://api.github.com/...page=5>; rel="last"
        if 'rel="last"' in part:
            import re
            match=re.search(r'[?&]page=(\d+)',part)
            if match:
                return int(match.group(1))
    return None

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    mini_retry_attempts=3
    for attempt in range(1, max_retries+1):
        local_attempt=1
        while local_attempt<=mini_retry_attempts:
            try:
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
                break
            except requests.exceptions.ConnectionError:
                logging.warning("ConnectionError => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1

        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => give up => %s",url)
            return (None,False)

    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_issues_single_thread(conn, owner, repo,
                              start_date, end_date, enabled,
                              session, handle_rate_limit_func,
                              max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issues",owner,repo)
        return

    # log the date range
    logging.debug(f"[DEBUG] {owner}/{repo} issues [{start_date} - {end_date}]")

    page=1
    last_page=None
    while True:
        old_base=start_date
        old_en=enabled
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,old_base,old_en)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop issues mid-run",owner,repo)
            break

        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={
            "state":"all",
            "sort":"created",
            "direction":"asc",
            "page":page,
            "per_page":100
        }
        # skip items older than start_date
        # you could skip items beyond end_date if desired
        params["since"]=start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success:
            logging.warning("Issues => cannot get page %d => break => %s/%s",page,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        # attempt to parse last_page
        if last_page is None:
            last_page=get_last_page(resp)

        # display progress => 4 decimals
        if last_page:
            progress=(page/last_page)*100
            logging.debug(f"[DEBUG] {owner}/{repo} issues => {progress:.4f}% done")

        for item in data:
            # skip pulls
            if "pull_request" in item:
                continue
            c_created_str=item.get("created_at")
            if not c_created_str:
                continue
            cdt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")

            # skip older than start_date
            if cdt<start_date:
                continue
            # optionally skip if cdt>end_date => if you want a closed interval
            if cdt>end_date:
                continue

            insert_issue_record(conn,f"{owner}/{repo}",item["number"],cdt)

        if len(data)<100:
            # no more pages
            break
        page+=1

def insert_issue_record(conn, repo_name, issue_number, created_dt):
    c=conn.cursor()
    sql="""
    INSERT INTO issues (repo_name, issue_number, created_at)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      created_at=VALUES(created_at)
    """
    c.execute(sql,(repo_name,issue_number,created_dt))
    conn.commit()
    c.close()
