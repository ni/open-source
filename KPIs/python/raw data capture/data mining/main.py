#!/usr/bin/env python
# main.py

import os
import sys
import time
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler
from fetch_events import (
    fetch_issue_events_for_all_issues,
    fetch_pull_events_for_all_pulls
)
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter, Retry
import mysql.connector

from db import connect_db, create_tables
from repo_baselines import get_baseline_info, set_baseline_date
from repos import get_repo_list

TOKENS = []
CURRENT_TOKEN_INDEX = 0
session = None
token_info = {}

def load_config():
    cfg = {}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    cfg.setdefault("mysql", {
        "host":"localhost","port":3306,
        "user":"root","password":"root","db":"my_kpis_db"
    })
    cfg.setdefault("tokens",[])
    cfg.setdefault("logging", {
        "file_name":"myapp.log",
        "rotate_when":"midnight",
        "backup_count":7,
        "console_level":"DEBUG",
        "file_level":"DEBUG"
    })
    # The number of days we add to earliest GH commit date => final baseline
    cfg.setdefault("days_to_capture",1)
    cfg.setdefault("max_retries",20)
    return cfg

def setup_logging(cfg):
    log_conf = cfg["logging"]
    log_file = log_conf["file_name"]
    rotate_when = log_conf["rotate_when"]
    backup_count = log_conf["backup_count"]
    console_level = log_conf["console_level"]
    file_level = log_conf["file_level"]

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level.upper())
    logger.addHandler(ch)

    fh = TimedRotatingFileHandler(log_file, when=rotate_when, backupCount=backup_count)
    fh.setLevel(file_level.upper())
    logger.addHandler(fh)

    f_console = logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(f_console)
    f_file = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(f_file)

def setup_session_with_retry():
    s = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=2,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("http://",adapter)
    s.mount("https://",adapter)
    return s

def rotate_token():
    global CURRENT_TOKEN_INDEX, TOKENS, session
    if not TOKENS:
        return
    old_idx = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
    new_token = TOKENS[CURRENT_TOKEN_INDEX]
    session.headers["Authorization"] = f"token {new_token}"
    logging.info("Rotated token from idx %d to %d => not showing partial token", old_idx, CURRENT_TOKEN_INDEX)

def main():
    global TOKENS, session, token_info, CURRENT_TOKEN_INDEX
    cfg = load_config()
    setup_logging(cfg)
    logging.info("Starting => watchers=full => numeric issues/pulls => skip older stars => single-thread => ALWAYS baseline=earliest GH commit + days_to_capture")

    conn = connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    TOKENS = cfg["tokens"]
    session = setup_session_with_retry()
    token_info = {}

    if TOKENS:
        session.headers["Authorization"] = f"token {TOKENS[0]}"

    max_retries = cfg["max_retries"]
    days_to_capture = cfg["days_to_capture"]

    summary_data = []
    all_repos = get_repo_list()
    for (owner,repo) in all_repos:
        # 1) get earliest GH commit => if none => skip entire
        earliest_gh_date = get_earliest_gh_commit_date(owner,repo,session,handle_rate_limit_func,max_retries)
        if not earliest_gh_date:
            logging.warning("Repo %s/%s => no earliest GH commit => skip",owner,repo)
            skip_reason="no_earliest_gh_commit"
            stats=gather_repo_stats(conn,owner,repo,skip_reason,None,None,False)
            stats["fetched_min_dt"]=None
            stats["fetched_max_dt"]=None
            summary_data.append(stats)
            continue

        # 2) baseline_date = earliestGhCommit + days_to_capture
        baseline_dt = earliest_gh_date + timedelta(days=days_to_capture)
        # store in repo_baselines => enabled=1
        update_repo_baseline(conn, owner, repo, baseline_dt)

        # 3) check earliest DB date => if it's beyond baseline => skip
        earliest_db_dt = get_minmax_earliest_db_date(conn,owner,repo)
        skip_reason="None"
        if earliest_db_dt and earliest_db_dt>baseline_dt:
            skip_reason="earliest_in_db_newer_than_baseline"
            logging.info("Repo %s/%s => earliest DB item %s > baseline=%s => skip entire repo",
                         owner,repo,earliest_db_dt,baseline_dt)
            stats=gather_repo_stats(conn,owner,repo,skip_reason,earliest_db_dt,baseline_dt,True)
            stats["fetched_min_dt"]=None
            stats["fetched_max_dt"]=None
            summary_data.append(stats)
            continue

        logging.info("Repo %s/%s => watchers => full => numeric issues/pulls => final baseline_date=%s => proceed",
                     owner,repo,baseline_dt)

        # normal fetch watchers/forks/stars/issues/pulls...
        from fetch_forks_stars_watchers import (
            list_watchers_single_thread,
            list_forks_single_thread,
            list_stars_single_thread
        )
        list_watchers_single_thread(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)
        list_forks_single_thread(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)
        list_stars_single_thread(conn,owner,repo,1,baseline_dt,session,handle_rate_limit_func,max_retries)

        from fetch_issues import list_issues_single_thread
        list_issues_single_thread(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)

        from fetch_pulls import list_pulls_single_thread
        list_pulls_single_thread(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)

        from fetch_events import (
            fetch_issue_events_for_all_issues,
            fetch_pull_events_for_all_pulls
        )
        fetch_issue_events_for_all_issues(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)
        fetch_pull_events_for_all_pulls(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)

        from fetch_comments import fetch_comments_for_all_issues
        fetch_comments_for_all_issues(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)

        from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
        fetch_issue_reactions_for_all_issues(conn,owner,repo,1,session,handle_rate_limit_func,max_retries)

        # if comment_reactions => do them here

        stats=gather_repo_stats(conn,owner,repo,"None",earliest_db_dt,baseline_dt,True)
        min_dt,max_dt = get_minmax_all_tables(conn,f"{owner}/{repo}")
        stats["fetched_min_dt"]=min_dt
        stats["fetched_max_dt"]=max_dt
        summary_data.append(stats)

    conn.close()
    logging.info("All done => printing final summary table & multiline details...\n")
    print_final_summary_table(summary_data)
    print_detailed_repo_summaries(summary_data)
    logging.info("Finished completely.")

def get_earliest_gh_commit_date(owner, repo, session, handle_rate_limit_func, max_retries):
    """
    Single call to /repos/{owner}/{repo}/commits?sort=committer-date&direction=asc&per_page=1
    Return datetime or None => skip
    """
    url=f"https://api.github.com/repos/{owner}/{repo}/commits"
    params={
        "sort":"committer-date",
        "direction":"asc",
        "per_page":1,
        "page":1
    }
    (resp, success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
    if not success or not resp:
        return None
    data=resp.json()
    if not data:
        return None
    cstr=data[0].get("commit",{}).get("committer",{}).get("date")
    if not cstr:
        return None
    try:
        dt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
        return dt
    except ValueError:
        return None

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    import requests
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
                    logging.warning("HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("Exhausted local mini-retry => break => %s",url)
            return (None,False)
    logging.warning("Exceeded max_retries => give up => %s",url)
    return (None,False)

def get_minmax_earliest_db_date(conn, owner, repo):
    """
    Return earliest date from union across forks/stars/issues/pulls/events/comments/reactions
    watchers => no date
    If none => None
    """
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    c.execute("""
    SELECT MIN(dt)
    FROM (
      SELECT created_at as dt FROM forks WHERE repo_name=%s
      UNION ALL
      SELECT starred_at as dt FROM stars WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issues WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM pulls WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issue_events WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM pull_events WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issue_comments WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM comment_reactions WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issue_reactions WHERE repo_name=%s
    ) sub
    """,(repo_name,repo_name,repo_name,repo_name,repo_name,repo_name,repo_name,repo_name,repo_name))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return None

def get_minmax_all_tables(conn, repo_name):
    """
    Return (min_dt, max_dt) across forks,stars,issues,pulls,events,comments,comment_reactions,issue_reactions
    watchers => no date
    If none => (None,None)
    """
    c=conn.cursor()
    c.execute("""
    SELECT MIN(dt), MAX(dt)
    FROM (
      SELECT created_at as dt FROM forks WHERE repo_name=%s
      UNION ALL
      SELECT starred_at as dt FROM stars WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issues WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM pulls WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issue_events WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM pull_events WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issue_comments WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM comment_reactions WHERE repo_name=%s
      UNION ALL
      SELECT created_at as dt FROM issue_reactions WHERE repo_name=%s
    ) sub
    """,(repo_name,repo_name,repo_name,repo_name,repo_name,repo_name,repo_name,repo_name,repo_name))
    row=c.fetchone()
    c.close()
    if row and (row[0] or row[1]):
        return (row[0], row[1])
    return (None,None)

def update_repo_baseline(conn, owner, repo, baseline_dt):
    """
    Insert or update (owner,repo, baseline_date, enabled=1)
    """
    c=conn.cursor()
    sql="""
    INSERT INTO repo_baselines (owner, repo, baseline_date, enabled, updated_at)
    VALUES (%s,%s,%s,1,NOW())
    ON DUPLICATE KEY UPDATE
      baseline_date=VALUES(baseline_date),
      enabled=1,
      updated_at=NOW()
    """
    c.execute(sql,(owner,repo,baseline_dt))
    conn.commit()
    c.close()

def gather_repo_stats(conn, owner, repo,
                      skip_reason, earliest_db_dt, baseline_dt,
                      enabled):
    """
    Summaries => if skip_reason!="None" or not enabled => zero counts
    else => actual counts from DB
    """
    stats_dict={
      "owner_repo": f"{owner}/{repo}",
      "skip_reason": skip_reason,
      "earliest_db_dt": earliest_db_dt,
      "baseline_dt": baseline_dt
    }
    if skip_reason!="None" or not enabled:
        stats_dict["reactions_in_issues"] = 0
        stats_dict["issues_count"] = 0
        stats_dict["reactions_in_pulls"] = 0
        stats_dict["pulls_count"] = 0
        stats_dict["comments_issues"] = 0
        stats_dict["comments_pulls"] = 0
        stats_dict["reactions_comments_issues"] = 0
        stats_dict["reactions_comments_pulls"] = 0
        stats_dict["stars_count"] = 0
        stats_dict["forks_count"] = 0
        stats_dict["watchers_count"] = 0
        stats_dict["opened_issues"] = 0
        stats_dict["opened_pulls"] = 0
        stats_dict["closed_issues"] = 0
        stats_dict["closed_pulls"] = 0
        stats_dict["merged_pulls"] = 0
        return stats_dict

    repo_name=f"{owner}/{repo}"
    c=conn.cursor()

    c.execute("SELECT COUNT(*) FROM issue_reactions WHERE repo_name=%s",(repo_name,))
    stats_dict["reactions_in_issues"]=c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM issues WHERE repo_name=%s",(repo_name,))
    stats_dict["issues_count"]=c.fetchone()[0]

    stats_dict["reactions_in_pulls"]=0

    c.execute("SELECT COUNT(*) FROM pulls WHERE repo_name=%s",(repo_name,))
    stats_dict["pulls_count"]=c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM issue_comments WHERE repo_name=%s",(repo_name,))
    stats_dict["comments_issues"]=c.fetchone()[0]

    stats_dict["comments_pulls"]=0

    c.execute("""
      SELECT COUNT(*)
      FROM comment_reactions cr
      JOIN issue_comments ic ON
        cr.repo_name=ic.repo_name
        AND cr.issue_number=ic.issue_number
        AND cr.comment_id=ic.comment_id
      WHERE cr.repo_name=%s
    """,(repo_name,))
    stats_dict["reactions_comments_issues"]=c.fetchone()[0]

    stats_dict["reactions_comments_pulls"]=0

    c.execute("SELECT COUNT(*) FROM stars WHERE repo_name=%s",(repo_name,))
    stats_dict["stars_count"]=c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM forks WHERE repo_name=%s",(repo_name,))
    stats_dict["forks_count"]=c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM watchers WHERE repo_name=%s",(repo_name,))
    stats_dict["watchers_count"]=c.fetchone()[0]

    stats_dict["opened_issues"]=stats_dict["issues_count"]
    stats_dict["opened_pulls"]=stats_dict["pulls_count"]

    c.execute("""
      SELECT COUNT(DISTINCT issue_number)
      FROM issue_events
      WHERE repo_name=%s
        AND JSON_EXTRACT(raw_json,'$.event')='closed'
    """,(repo_name,))
    row=c.fetchone()
    stats_dict["closed_issues"]=row[0] if row and row[0] else 0

    c.execute("""
      SELECT COUNT(DISTINCT pull_number)
      FROM pull_events
      WHERE repo_name=%s
        AND JSON_EXTRACT(raw_json,'$.event')='closed'
    """,(repo_name,))
    row=c.fetchone()
    stats_dict["closed_pulls"]=row[0] if row and row[0] else 0

    c.execute("""
      SELECT COUNT(DISTINCT pull_number)
      FROM pull_events
      WHERE repo_name=%s
        AND JSON_EXTRACT(raw_json,'$.event')='merged'
    """,(repo_name,))
    row=c.fetchone()
    stats_dict["merged_pulls"]=row[0] if row and row[0] else 0

    c.close()
    return stats_dict

def print_final_summary_table(summary_data):
    col_repo_width = 25
    col_skip_width = 12
    header_parts = [
        f"{'Repo':{col_repo_width}s}",
        f"{'SkipReason':{col_skip_width}s}",
        "Issues", "ReacIss", "Pulls", "ReacPull", "CmtIss", "CmtPull",
        "ReCmtIss", "ReCmtPull",
        "Stars", "Forks", "Watchers",
        "OpIss", "OpPull", "ClIss", "ClPull", "MrPull"
    ]
    header_line="  ".join([x.ljust(8) for x in header_parts])
    print("")
    print("========== FINAL ONE-LINE SUMMARY (Aligned Columns) ==========")
    print(header_line)
    print("-"*len(header_line))

    for row in summary_data:
        line_parts=[]
        repo_str = row["owner_repo"][:col_repo_width]
        skip_str = row["skip_reason"][:col_skip_width]
        line_parts.append(f"{repo_str:{col_repo_width}s}")
        line_parts.append(f"{skip_str:{col_skip_width}s}")
        line_parts.append(f"{row['issues_count']:>6d}")
        line_parts.append(f"{row['reactions_in_issues']:>7d}")
        line_parts.append(f"{row['pulls_count']:>5d}")
        line_parts.append(f"{row['reactions_in_pulls']:>8d}")
        line_parts.append(f"{row['comments_issues']:>6d}")
        line_parts.append(f"{row['comments_pulls']:>7d}")
        line_parts.append(f"{row['reactions_comments_issues']:>8d}")
        line_parts.append(f"{row['reactions_comments_pulls']:>9d}")
        line_parts.append(f"{row['stars_count']:>5d}")
        line_parts.append(f"{row['forks_count']:>5d}")
        line_parts.append(f"{row['watchers_count']:>8d}")
        line_parts.append(f"{row['opened_issues']:>5d}")
        line_parts.append(f"{row['opened_pulls']:>6d}")
        line_parts.append(f"{row['closed_issues']:>6d}")
        line_parts.append(f"{row['closed_pulls']:>7d}")
        line_parts.append(f"{row['merged_pulls']:>7d}")

        print("  ".join(line_parts))

    print("===============================================================")

def print_detailed_repo_summaries(summary_data):
    print("")
    print("========== DETAILED SUMMARY PER REPO ==========")
    for row in summary_data:
        print(f"--- Repo: {row['owner_repo']} ---")
        print(f"    SkipReason: {row['skip_reason']}")
        print(f"    EarliestInDB: {row['earliest_db_dt']}")
        print(f"    BaselineDate: {row['baseline_dt']}")
        print(f"    FetchedMinDt: {row.get('fetched_min_dt',None)}")
        print(f"    FetchedMaxDt: {row.get('fetched_max_dt',None)}")
        print("")

def handle_rate_limit_func(resp):
    global TOKENS, CURRENT_TOKEN_INDEX, session, token_info
    if not TOKENS:
        return
    update_token_info(CURRENT_TOKEN_INDEX, resp)
    info=token_info.get(CURRENT_TOKEN_INDEX)
    if info and info["remaining"]<5:
        old_idx=CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX==old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
    if resp.status_code in (403,429):
        logging.warning("HTTP %d => forcibly rotate or sleep", resp.status_code)
        old_idx=CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX==old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
            else:
                do_sleep_based_on_reset()

def update_token_info(token_idx, resp):
    global token_info
    rem_str = resp.headers.get("X-RateLimit-Remaining","")
    rst_str = resp.headers.get("X-RateLimit-Reset","")
    try:
        remaining=int(rem_str)
    except ValueError:
        remaining=None
    try:
        reset_ts=int(rst_str)
    except ValueError:
        reset_ts=None
    if remaining is not None and reset_ts is not None:
        token_info[token_idx]={"remaining":remaining,"reset":reset_ts}

def get_all_tokens_near_limit():
    global token_info,TOKENS
    if not TOKENS:
        return False
    for idx in range(len(TOKENS)):
        info=token_info.get(idx)
        if not info or info["remaining"]>=5:
            return False
    return True

def sleep_until_earliest_reset():
    import time
    global token_info,TOKENS
    if not TOKENS:
        return
    earliest=None
    now_ts=int(time.time())
    for idx in range(len(TOKENS)):
        info=token_info.get(idx)
        if info:
            rst=info.get("reset")
            if rst and (earliest is None or rst<earliest):
                earliest=rst
    if earliest is None:
        logging.warning("No valid reset => fallback => sleep 3600s")
        time.sleep(3600)
        return
    delta=earliest-now_ts+30
    if delta>0:
        logging.warning("Sleeping %d seconds => earliest token resets at %d (now=%d)",
                        delta,earliest,now_ts)
        time.sleep(delta)
    else:
        logging.warning("Earliest reset is in the past => no sleep needed")

def do_sleep_based_on_reset():
    import time
    logging.warning("Cannot parse reset => fallback => sleep 3600s")
    time.sleep(3600)

if __name__=="__main__":
    main()