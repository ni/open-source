# fetch_issues.py
"""
Lists issues => skip if created_at>baseline_date => store in issues table.
We do not call comments or events here => separate modules handle that.
"""

import logging
import json
from datetime import datetime
from repo_baselines import refresh_baseline_info_mid_run

def list_issues_single_thread(conn, owner, repo, baseline_date, enabled, session, handle_rate_limit_func):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip issues",owner,repo)
        return
    page=1
    while True:
        new_base,new_en=refresh_baseline_info_mid_run(conn,owner,repo,baseline_date,enabled)
        if new_en==0:
            logging.info("Repo %s/%s => toggled disabled => stop issues mid-run",owner,repo)
            break
        if new_base!=baseline_date:
            baseline_date=new_base
            logging.info("Repo %s/%s => baseline changed => now %s (issues).",owner,repo,baseline_date)

        url=f"https://api.github.com/repos/{owner}/{repo}/issues"
        params={
            "state":"all",
            "sort":"created",
            "direction":"asc",
            "page":page,
            "per_page":100
        }
        resp=session.get(url, params=params)
        handle_rate_limit_func(resp)
        if resp.status_code!=200:
            logging.warning("Issues => HTTP %d => break for %s/%s", resp.status_code,owner,repo)
            break
        data=resp.json()
        if not data:
            break

        for item in data:
            if "pull_request" in item:
                continue
            c_created_str=item["created_at"]
            cdt=datetime.strptime(c_created_str,"%Y-%m-%dT%H:%M:%SZ")
            if baseline_date and cdt>baseline_date:
                continue
            insert_issue_record(conn, f"{owner}/{repo}", item["number"], cdt)

        if len(data)<100:
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
