#!/usr/bin/env python
# main.py

import os
import sys
import time
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from fetch_events import (
    fetch_issue_events_for_all_issues,
    fetch_pull_events_for_all_pulls
)

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
    cfg.setdefault("max_retries",20)
    # Fallback date => used if no baseline_date in DB
    cfg.setdefault("fallback_baseline_date","2015-01-01")
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

def check_if_repo_baselines_empty(conn):
    """
    Returns True if repo_baselines table has zero rows => brand-new DB
    """
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM repo_baselines")
    row = c.fetchone()
    c.close()
    return (row and row[0] == 0)

def insert_repo_baseline_if_missing(conn, owner, repo, fallback_dt):
    """
    Insert (owner,repo,baseline_date=fallback_dt, enabled=1, updated_at=NOW()) if missing.
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
    c.execute(sql,(owner,repo,fallback_dt))
    conn.commit()
    c.close()

def get_earliest_item_created_date_in_db(conn, repo_name):
    """
    If the earliest item is newer than baseline => skip entire repo approach
    """
    c = conn.cursor()
    c.execute("""
    SELECT MIN(created_at) 
    FROM (
      SELECT created_at FROM issues WHERE repo_name=%s
      UNION
      SELECT created_at FROM pulls WHERE repo_name=%s
    ) sub
    """,(repo_name,repo_name))
    row = c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return None

def get_minmax_all_tables(conn, repo_name):
    """
    Gathers the overall min and max created_at (or starred_at) across:
      forks.created_at
      stars.starred_at
      issues.created_at
      pulls.created_at
      issue_events.created_at
      pull_events.created_at
      issue_comments.created_at
      comment_reactions.created_at
      issue_reactions.created_at
    watchers => no date
    If no items, returns (None,None).
    """
    c = conn.cursor()
    # We'll union all date-time fields into one derived table, then find min and max
    c.execute("""
    SELECT MIN(dt), MAX(dt) FROM (
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
    row = c.fetchone()
    c.close()
    if row and (row[0] or row[1]):
        return (row[0], row[1])
    return (None,None)

def main():
    global TOKENS, session, token_info, CURRENT_TOKEN_INDEX
    cfg = load_config()
    setup_logging(cfg)
    logging.info("Starting => watchers=full => numeric issues/pulls => skip older stars => single-thread => final summary => if brand-new DB => populate baselines")

    conn = connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    # Check if brand-new => if so, populate repo_baselines with fallback baseline_date, enabled=1
    new_db = check_if_repo_baselines_empty(conn)
    if new_db:
        logging.info("Detected brand-new DB => populating repo_baselines with fallback date, enabled=1")
        fallback_str = cfg["fallback_baseline_date"]
        fallback_dt = datetime.strptime(fallback_str, "%Y-%m-%d")
        for (owner,repo) in get_repo_list():
            insert_repo_baseline_if_missing(conn, owner, repo, fallback_dt)

    TOKENS = cfg["tokens"]
    session = setup_session_with_retry()
    token_info = {}

    if TOKENS:
        session.headers["Authorization"] = f"token {TOKENS[0]}"

    max_retries = cfg["max_retries"]
    fallback_str = cfg["fallback_baseline_date"]
    fallback_dt = datetime.strptime(fallback_str, "%Y-%m-%d")

    summary_data = []

    all_repos = get_repo_list()
    for (owner,repo) in all_repos:
        baseline_dt, enabled = get_baseline_info(conn,owner,repo)
        if not baseline_dt:
            baseline_dt = fallback_dt

        skip_reason = "None"
        if enabled==0:
            skip_reason="disabled"

        repo_name=f"{owner}/{repo}"
        earliest_in_db=None
        if enabled==1:
            earliest_in_db = get_earliest_item_created_date_in_db(conn,repo_name)
            if earliest_in_db and earliest_in_db>baseline_dt:
                skip_reason="earliest_in_db_newer_than_baseline"

        if enabled==0:
            logging.info("Repo %s/%s => disabled => skip run",owner,repo)
        elif skip_reason=="earliest_in_db_newer_than_baseline":
            logging.info("Repo %s/%s => earliest DB item %s > baseline=%s => skip entire repo",
                         owner,repo,earliest_in_db,baseline_dt)
        else:
            logging.info("Repo %s/%s => watchers => full => numeric issues/pulls => skip stars older than baseline=%s => proceed",
                         owner,repo,baseline_dt)
            from fetch_forks_stars_watchers import (
                list_watchers_single_thread,
                list_forks_single_thread,
                list_stars_single_thread
            )
            list_watchers_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
            list_forks_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
            list_stars_single_thread(conn,owner,repo,enabled,baseline_dt,session,handle_rate_limit_func,max_retries)

            from fetch_issues import list_issues_single_thread
            list_issues_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

            from fetch_pulls import list_pulls_single_thread
            list_pulls_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

            from fetch_events import (
                fetch_issue_events_for_all_issues,
                fetch_pull_events_for_all_pulls
            )
            fetch_issue_events_for_all_issues(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
            fetch_pull_events_for_all_pulls(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

            from fetch_comments import fetch_comments_for_all_issues
            fetch_comments_for_all_issues(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

            from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
            fetch_issue_reactions_for_all_issues(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

            # If you have comment_reactions => do that here

        # gather stats => includes counting closed merges, etc.
        stats = gather_repo_stats(conn, owner, repo, skip_reason, earliest_in_db, baseline_dt, enabled)
        # also gather min/max of ALL data fetched
        min_dt, max_dt = get_minmax_all_tables(conn, repo_name)
        stats["fetched_min_dt"] = min_dt
        stats["fetched_max_dt"] = max_dt

        summary_data.append(stats)

    conn.close()
    logging.info("All done => printing final summary => also printing a separate summary per repo with min_dt, max_dt, baseline.\n")

    print_final_summary_table(summary_data)
    print_detailed_repo_summaries(summary_data)

    logging.info("Finished completely.")

def gather_repo_stats(conn, owner, repo,
                      skip_reason, earliest_db_dt, baseline_dt, enabled):
    repo_name = f"{owner}/{repo}"
    stats_dict = {
      "owner_repo": f"{owner}/{repo}",
      "skip_reason": skip_reason,
      "earliest_db_dt": earliest_db_dt,
      "baseline_dt": baseline_dt,
      "enabled": enabled
    }
    if skip_reason!="None":
        # zero them out
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

    c = conn.cursor()

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
    """
    Print an aligned one-line table with columns like skipReason, issues, pulls, watchers, etc.
    """
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
    """
    Print a separate multiline summary per repo, showing:
      - earliest_in_db
      - baseline_dt
      - skip_reason
      - fetched_min_dt, fetched_max_dt
        which are the earliest/ latest we actually have in all tables
    """
    print("")
    print("========== DETAILED SUMMARY PER REPO ==========")
    for row in summary_data:
        print(f"--- Repo: {row['owner_repo']} ---")
        print(f"    SkipReason: {row['skip_reason']}")
        print(f"    BaselineDate: {row['baseline_dt']}")
        print(f"    EarliestInDB (from old data): {row['earliest_db_dt']}")
        print(f"    FetchedMinDt (this session): {row.get('fetched_min_dt',None)}")
        print(f"    FetchedMaxDt (this session): {row.get('fetched_max_dt',None)}")
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
