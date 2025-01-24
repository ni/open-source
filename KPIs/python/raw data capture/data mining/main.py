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

from db import connect_db, create_tables
from repo_baselines import get_baseline_info, set_baseline_date
from repos import get_repo_list

TOKENS=[]
CURRENT_TOKEN_INDEX=0
session=None
token_info={}

def load_config():
    cfg={}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg=yaml.safe_load(f)
    cfg.setdefault("mysql",{
        "host":"localhost","port":3306,"user":"root","password":"root","db":"my_kpis_db"
    })
    cfg.setdefault("tokens",[])
    cfg.setdefault("logging",{
        "file_name":"myapp.log",
        "rotate_when":"midnight",
        "backup_count":7,
        "console_level":"DEBUG",
        "file_level":"DEBUG"
    })
    cfg.setdefault("max_retries",20)
    cfg.setdefault("fallback_baseline_date","2015-01-01")
    return cfg

def setup_logging(cfg):
    log_conf=cfg["logging"]
    log_file=log_conf["file_name"]
    rotate_when=log_conf["rotate_when"]
    backup_count=log_conf["backup_count"]
    console_level=log_conf["console_level"]
    file_level=log_conf["file_level"]

    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)

    ch=logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level.upper())
    logger.addHandler(ch)

    fh=TimedRotatingFileHandler(log_file, when=rotate_when, backupCount=backup_count)
    fh.setLevel(file_level.upper())
    logger.addHandler(fh)

    f_console=logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(f_console)
    f_file=logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(f_file)

def setup_session_with_retry():
    s=requests.Session()
    retry_strategy=Retry(
        total=10,
        backoff_factor=2,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS"]
    )
    adapter=HTTPAdapter(max_retries=retry_strategy)
    s.mount("http://",adapter)
    s.mount("https://",adapter)
    return s

def rotate_token():
    global CURRENT_TOKEN_INDEX, TOKENS, session
    if not TOKENS:
        return
    old_idx=CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX=(CURRENT_TOKEN_INDEX+1)%len(TOKENS)
    new_token=TOKENS[CURRENT_TOKEN_INDEX]
    session.headers["Authorization"]=f"token {new_token}"
    logging.info("Rotated token from idx %d to %d => not showing partial token string",
                 old_idx,CURRENT_TOKEN_INDEX)

def get_earliest_item_created_date_in_db(conn, repo_name):
    """
    If you want the earliest created_at among issues + pulls.
    """
    c=conn.cursor()
    c.execute("""
    SELECT MIN(created_at) 
    FROM (
      SELECT created_at FROM issues WHERE repo_name=%s
      UNION
      SELECT created_at FROM pulls WHERE repo_name=%s
    ) sub
    """,(repo_name,repo_name))
    row=c.fetchone()
    c.close()
    if row and row[0]:
        return row[0]
    return None

def main():
    global TOKENS,session,token_info,CURRENT_TOKEN_INDEX
    cfg=load_config()
    setup_logging(cfg)
    logging.info("Starting => watchers=full => numeric issues/pulls => skip older stars => single-thread => final summary of stats per repo")

    conn=connect_db(cfg,create_db_if_missing=True)
    create_tables(conn)

    TOKENS=cfg["tokens"]
    session=setup_session_with_retry()
    token_info={}

    if TOKENS:
        session.headers["Authorization"]=f"token {TOKENS[0]}"

    max_retries=cfg["max_retries"]
    fallback_str=cfg["fallback_baseline_date"]
    fallback_dt=datetime.strptime(fallback_str, "%Y-%m-%d")

    # We'll store stats in a list => print at end
    summary_data=[]

    all_repos=get_repo_list()
    for (owner,repo) in all_repos:
        bdt,en = get_baseline_info(conn,owner,repo)
        if not bdt:
            bdt=fallback_dt

        skip_reason="None"
        if en==0:
            skip_reason="disabled"

        repo_name=f"{owner}/{repo}"
        earliest_db_dt=None
        if en==1:
            earliest_db_dt=get_earliest_item_created_date_in_db(conn,repo_name)
            if earliest_db_dt and earliest_db_dt>bdt:
                skip_reason="earliest_in_db_newer_than_baseline"

        if en==0:
            logging.info("Repo %s/%s => disabled => skip run",owner,repo)
        elif skip_reason=="earliest_in_db_newer_than_baseline":
            logging.info("Repo %s/%s => earliest item in DB %s > baseline=%s => skip entire repo",
                         owner,repo,earliest_db_dt,bdt)
        else:
            logging.info("Repo %s/%s => watchers => full => numeric issues/pulls => skip stars older than baseline=%s => proceed",
                         owner,repo,bdt)

            from fetch_forks_stars_watchers import (
                list_watchers_single_thread,
                list_forks_single_thread,
                list_stars_single_thread
            )
            list_watchers_single_thread(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)
            list_forks_single_thread(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)
            list_stars_single_thread(conn,owner,repo,en,bdt,session,handle_rate_limit_func,max_retries)

            from fetch_issues import list_issues_single_thread
            list_issues_single_thread(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)

            from fetch_pulls import list_pulls_single_thread
            list_pulls_single_thread(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)

            from fetch_events import (
                fetch_issue_events_for_all_issues,
                fetch_pull_events_for_all_pulls
            )
            fetch_issue_events_for_all_issues(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)
            fetch_pull_events_for_all_pulls(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)

            from fetch_comments import fetch_comments_for_all_issues
            fetch_comments_for_all_issues(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)

            from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
            fetch_issue_reactions_for_all_issues(conn,owner,repo,en,session,handle_rate_limit_func,max_retries)

            # If you have fetch_comment_reactions => do it here:
            # from fetch_comment_reactions import fetch_comment_reactions_for_all_comments
            # fetch_comment_reactions_for_all_comments(...)

        # after fetching, gather stats for final summary
        stats= gather_repo_stats(conn, owner, repo, skip_reason, earliest_db_dt, bdt, en)
        summary_data.append(stats)

    conn.close()

    logging.info("All done => now printing final summary with advanced counts.\n")
    print_final_summary_table(summary_data)
    logging.info("Finished completely.")

def gather_repo_stats(conn, owner, repo,
                      skip_reason, earliest_db_dt, baseline_dt, enabled):
    """
    Returns a dict with all the counts you requested for the final summary:
      - Reactions in issues
      - Issues
      - Reactions in pulls => 0 (Q7 => not stored)
      - Pull requests
      - Comments to issues => # in issue_comments
      - Comments to pulls => 0 (pull comments not in the schema)
      - Reactions to comments from issues => join comment_reactions -> issue_comments
      - Reactions to comments from pulls => 0
      - stars => count(stars)
      - forks => count(forks)
      - watchers => count(watchers)
      - opened issues => same as total issues
      - opened pulls => same as total pulls
      - closed issues => event-based
      - closed pulls => event-based
      - merged pulls => event-based
    If skip_reason != "None", we might set them to 0 because we didn't fetch.
    """
    repo_name=f"{owner}/{repo}"
    stats_dict={
      "owner_repo":f"{owner}/{repo}",
      "skip_reason":skip_reason,
      "earliest_db_dt":earliest_db_dt,
      "baseline_dt":baseline_dt,
      "enabled":enabled
    }

    if skip_reason!="None":
        # everything => 0
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

    # we do queries for each count
    c=conn.cursor()

    # Reactions in issues => count(*) from issue_reactions
    c.execute("SELECT COUNT(*) FROM issue_reactions WHERE repo_name=%s",(repo_name,))
    stats_dict["reactions_in_issues"]=c.fetchone()[0]

    # Issues => total in issues
    c.execute("SELECT COUNT(*) FROM issues WHERE repo_name=%s",(repo_name,))
    stats_dict["issues_count"]=c.fetchone()[0]

    # Reactions in pulls => 0 => Q7 => not stored
    stats_dict["reactions_in_pulls"]=0

    # Pull requests => total in pulls
    c.execute("SELECT COUNT(*) FROM pulls WHERE repo_name=%s",(repo_name,))
    stats_dict["pulls_count"]=c.fetchone()[0]

    # Comments to issues => total rows in issue_comments => they are only for issues
    c.execute("SELECT COUNT(*) FROM issue_comments WHERE repo_name=%s",(repo_name,))
    stats_dict["comments_issues"]=c.fetchone()[0]

    # Comments to pulls => 0 => Q5 => not stored
    stats_dict["comments_pulls"]=0

    # Reactions to comments from issues => do a join comment_reactions -> issue_comments
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

    # Reactions to comments from pulls => 0 => no pull_comments table
    stats_dict["reactions_comments_pulls"]=0

    # stars => count(*)
    c.execute("SELECT COUNT(*) FROM stars WHERE repo_name=%s",(repo_name,))
    stats_dict["stars_count"]=c.fetchone()[0]

    # forks => count(*)
    c.execute("SELECT COUNT(*) FROM forks WHERE repo_name=%s",(repo_name,))
    stats_dict["forks_count"]=c.fetchone()[0]

    # watchers => count(*)
    c.execute("SELECT COUNT(*) FROM watchers WHERE repo_name=%s",(repo_name,))
    stats_dict["watchers_count"]=c.fetchone()[0]

    # opened issues => same as total issues
    stats_dict["opened_issues"]=stats_dict["issues_count"]

    # opened pulls => same as total pulls
    stats_dict["opened_pulls"]=stats_dict["pulls_count"]

    # closed issues => event-based => DISTINCT issue_number in issue_events with event='closed'
    c.execute("""
      SELECT COUNT(DISTINCT issue_number)
      FROM issue_events
      WHERE repo_name=%s
        AND JSON_EXTRACT(raw_json,'$.event')='closed'
    """,(repo_name,))
    row=c.fetchone()
    closed_issues_count=row[0] if row and row[0] else 0
    stats_dict["closed_issues"]=closed_issues_count

    # closed pulls => event-based => DISTINCT pull_number in pull_events with event='closed'
    c.execute("""
      SELECT COUNT(DISTINCT pull_number)
      FROM pull_events
      WHERE repo_name=%s
        AND JSON_EXTRACT(raw_json,'$.event')='closed'
    """,(repo_name,))
    row=c.fetchone()
    closed_pulls_count=row[0] if row and row[0] else 0
    stats_dict["closed_pulls"]=closed_pulls_count

    # merged pulls => treat event='merged' => DISTINCT pull_number
    c.execute("""
      SELECT COUNT(DISTINCT pull_number)
      FROM pull_events
      WHERE repo_name=%s
        AND JSON_EXTRACT(raw_json,'$.event')='merged'
    """,(repo_name,))
    row=c.fetchone()
    merged_pulls_count=row[0] if row and row[0] else 0
    stats_dict["merged_pulls"]=merged_pulls_count

    c.close()
    return stats_dict

def print_final_summary_table(summary_data):
    """
    Print one line per repo with all the counts requested:
    - Reactions in issues/pulls
    - issues, pulls
    - comments in issues/pulls
    - reactions to comments in issues/pulls
    - stars, forks, watchers
    - opened issues, opened pulls
    - closed issues, closed pulls
    - merged pulls
    plus skipReason if any
    """
    print("")
    print("===================== FINAL SUMMARY =====================")
    header=(
        "Repo".ljust(25)+"  SkipReason".ljust(20)+
        "  Issues  IssueReact  Pulls  PullReact  CmtIss  CmtPull  ReactCmtIss  ReactCmtPull  Stars  Forks  Watchers  OpenIss  OpenPull  ClsdIss  ClsdPull  MrgdPull"
    )
    print(header)
    print("-"*len(header))

    for row in summary_data:
        line_parts=[]
        line_parts.append(row["owner_repo"].ljust(25))
        line_parts.append(row["skip_reason"].ljust(20))
        line_parts.append(str(row["issues_count"]).rjust(7))
        line_parts.append(str(row["reactions_in_issues"]).rjust(11))
        line_parts.append(str(row["pulls_count"]).rjust(7))
        line_parts.append(str(row["reactions_in_pulls"]).rjust(10))
        line_parts.append(str(row["comments_issues"]).rjust(7))
        line_parts.append(str(row["comments_pulls"]).rjust(8))
        line_parts.append(str(row["reactions_comments_issues"]).rjust(12))
        line_parts.append(str(row["reactions_comments_pulls"]).rjust(13))
        line_parts.append(str(row["stars_count"]).rjust(6))
        line_parts.append(str(row["forks_count"]).rjust(6))
        line_parts.append(str(row["watchers_count"]).rjust(9))
        line_parts.append(str(row["opened_issues"]).rjust(8))
        line_parts.append(str(row["opened_pulls"]).rjust(9))
        line_parts.append(str(row["closed_issues"]).rjust(8))
        line_parts.append(str(row["closed_pulls"]).rjust(9))
        line_parts.append(str(row["merged_pulls"]).rjust(9))

        print("  ".join(line_parts))

    print("=========================================================")

def handle_rate_limit_func(resp):
    global TOKENS,CURRENT_TOKEN_INDEX,session,token_info
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
        logging.warning("HTTP %d => forcibly rotate or sleep",resp.status_code)
        old_idx=CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX==old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
            else:
                do_sleep_based_on_reset()

def update_token_info(token_idx, resp):
    global token_info
    rem_str=resp.headers.get("X-RateLimit-Remaining","")
    rst_str=resp.headers.get("X-RateLimit-Reset","")
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
