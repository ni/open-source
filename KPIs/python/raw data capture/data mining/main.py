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
    # fallback if no baseline in DB
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
    If the earliest item is newer than baseline => skip entire repo.
    We'll check MIN(created_at) from issues + pulls.
    """
    c=conn.cursor()
    c.execute("""
    SELECT MIN(created_at) 
    FROM (
      SELECT created_at FROM issues WHERE repo_name=%s
      UNION
      SELECT created_at FROM pulls WHERE repo_name=%s
    ) AS sub
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
    logging.info("Starting => watchers=full => numeric for issues/pulls => skip stars by starred_at => skip entire repo if earliest DB item>baseline => single-thread")

    conn=connect_db(cfg,create_db_if_missing=True)
    create_tables(conn)

    TOKENS=cfg["tokens"]
    session=setup_session_with_retry()
    token_info={}

    if TOKENS:
        session.headers["Authorization"]=f"token {TOKENS[0]}"

    max_retries=cfg["max_retries"]
    fallback_dt=datetime.strptime(cfg["fallback_baseline_date"],"%Y-%m-%d")

    all_repos=get_repo_list()
    for (owner,repo) in all_repos:
        bdt,en = get_baseline_info(conn,owner,repo)
        if en==0:
            logging.info("Repo %s/%s => enabled=0 => skip run",owner,repo)
            continue
        if not bdt:
            bdt=fallback_dt

        repo_name=f"{owner}/{repo}"
        earliest_in_db=get_earliest_item_created_date_in_db(conn,repo_name)
        if earliest_in_db and earliest_in_db>bdt:
            logging.info("Earliest item in DB for %s => %s, which is newer than baseline_date=%s => skip entire repo",
                         repo_name,earliest_in_db,bdt)
            continue

        logging.info("Repo %s => watchers => full => numeric issues/pulls => skip stars older than %s => proceed",
                     repo_name,bdt)

        from fetch_forks_stars_watchers import (
            list_watchers_single_thread,
            list_forks_single_thread,
            list_stars_single_thread
        )
        list_watchers_single_thread(
            conn,owner,repo,en,session,handle_rate_limit_func,
            max_retries
        )
        list_forks_single_thread(
            conn,owner,repo,en,session,handle_rate_limit_func,
            max_retries
        )
        # pass baseline_dt => skip stars with starred_at < bdt
        list_stars_single_thread(
            conn,owner,repo,en,
            bdt,
            session,handle_rate_limit_func,
            max_retries
        )

        from fetch_issues import list_issues_single_thread
        list_issues_single_thread(
            conn,owner,repo,en,
            session,handle_rate_limit_func,
            max_retries
        )

        from fetch_pulls import list_pulls_single_thread
        list_pulls_single_thread(
            conn,owner,repo,en,
            session,handle_rate_limit_func,
            max_retries
        )

        from fetch_events import (
            fetch_issue_events_for_all_issues,
            fetch_pull_events_for_all_pulls
        )
        fetch_issue_events_for_all_issues(
            conn,owner,repo,en,
            session,handle_rate_limit_func,
            max_retries
        )
        fetch_pull_events_for_all_pulls(
            conn,owner,repo,en,
            session,handle_rate_limit_func,
            max_retries
        )

        from fetch_comments import fetch_comments_for_all_issues
        fetch_comments_for_all_issues(
            conn,owner,repo,en,
            session,handle_rate_limit_func,
            max_retries
        )

        from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
        fetch_issue_reactions_for_all_issues(
            conn,owner,repo,en,
            session,handle_rate_limit_func,
            max_retries
        )

    conn.close()
    logging.info("All done => single-thread => watchers=full => numeric issues/pulls => skip stars older than baseline => skip entire repo if earliest DB item>baseline => complete")

def handle_rate_limit_func(resp):
    global TOKENS,CURRENT_TOKEN_INDEX,session,token_info
    if not TOKENS:
        return
    update_token_info(CURRENT_TOKEN_INDEX,resp)
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
