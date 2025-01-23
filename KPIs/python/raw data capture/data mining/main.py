#!/usr/bin/env python
# main.py
"""
Main orchestrator:
 1) Load config
 2) Connect DB, create tables
 3) Setup session w/ advanced Retry adapter (Solution #1)
 4) For each repo, compute baseline_date from 'days_to_capture' or from 'repo_baselines'
 5) Call watchers, forks, stars, issues, pulls, events, comments, issue reactions
 6) Freed from chain-of-thought, focusing on final integrated code
"""

import os
import sys
import time
import logging
import yaml
import requests
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

from requests.adapters import HTTPAdapter, Retry

from db import connect_db, create_tables
from repo_baselines import get_baseline_info, set_baseline_date
from repos import get_repo_list

CURRENT_TOKEN_INDEX = 0
TOKENS = []
session = None
token_info = {}

def load_config():
    cfg={}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    cfg.setdefault("mysql",{
        "host":"localhost",
        "port":3306,
        "user":"root",
        "password":"root",
        "db":"my_kpis_analytics_db"
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
    cfg.setdefault("days_to_capture",30)  # user can define how many days of history
    return cfg

def setup_logging(cfg):
    log_conf=cfg.get("logging",{})
    log_file=log_conf.get("file_name","myapp.log")
    rotate_when=log_conf.get("rotate_when","midnight")
    backup_count=log_conf.get("backup_count",7)
    console_level=log_conf.get("console_level","DEBUG")
    file_level=log_conf.get("file_level","DEBUG")

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
    """
    Creates a requests.Session that re-tries certain errors
    (Solution #1 => advanced backoff, up to 10 attempts)
    """
    s = requests.Session()
    retry_strategy = Retry(
        total=10,  # can raise or lower
        backoff_factor=2,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def main():
    global TOKENS, session, token_info, CURRENT_TOKEN_INDEX
    cfg=load_config()
    setup_logging(cfg)
    logging.info("Starting integrated solution => Solutions #1 + #3 => advanced re-try & local mini-retry => baseline_date from days_to_capture")

    conn=connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    TOKENS=cfg.get("tokens",[])
    session=setup_session_with_retry()   # advanced re-try
    token_info={}   # store remaining/reset for each token
    if TOKENS:
        session.headers["Authorization"]=f"token {TOKENS[0]}"

    max_retries=cfg.get("max_retries",20)
    days_to_capture=cfg.get("days_to_capture",30)

    from datetime import datetime, timedelta

    all_repos=get_repo_list()
    for (owner, repo) in all_repos:
        # We'll compute baseline_date => now - days_to_capture
        computed_base = datetime.now() - timedelta(days=days_to_capture)

        # read existing baseline from table
        old_base, enabled = get_baseline_info(conn, owner, repo)
        if enabled==0:
            logging.info("Repo %s/%s => disabled => skip entire run",owner,repo)
            continue

        # Optionally override baseline_date in the table, or just pass
        # If you want to store the computed date => set_baseline_date(conn,owner,repo,computed_base)
        baseline_date = computed_base

        logging.info("Repo %s/%s => final baseline_date=%s, enabled=%s => watchers,forks,stars,issues,pulls,events,comments,reactions",
                     owner,repo,baseline_date,enabled)

        # watchers, forks, stars
        from fetch_forks_stars_watchers import (
            list_watchers_single_thread,
            list_forks_single_thread,
            list_stars_single_thread
        )
        list_watchers_single_thread(
            conn, owner, repo,
            enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )
        list_forks_single_thread(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )
        list_stars_single_thread(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

        # issues => skip new
        from fetch_issues import list_issues_single_thread
        list_issues_single_thread(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

        # pulls => skip new
        from fetch_pulls import list_pulls_single_thread
        list_pulls_single_thread(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

        # events => issue_events + pull_events
        from fetch_events import (
            fetch_issue_events_for_all_issues,
            fetch_pull_events_for_all_pulls
        )
        fetch_issue_events_for_all_issues(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )
        fetch_pull_events_for_all_pulls(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

        # comments => skip new => plus comment reactions
        from fetch_comments import fetch_comments_for_all_issues
        fetch_comments_for_all_issues(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

        # issue reactions => skip new
        from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
        fetch_issue_reactions_for_all_issues(
            conn, owner, repo,
            baseline_date, enabled,
            session,
            handle_rate_limit_func,
            max_retries
        )

    conn.close()
    logging.info("All done => combined solutions => advanced re-try, local mini-retry, baseline_date from days => success.")

def handle_rate_limit_func(resp):
    """
    Called after each request in robust_get_page => handles token rotation,
    near-limit logic, earliest reset approach, etc.

    We won't show partial tokens in logs => skip it for security
    """
    global TOKENS, CURRENT_TOKEN_INDEX, session, token_info
    if not TOKENS:
        return

    # update token_info for current token
    update_token_info(CURRENT_TOKEN_INDEX, resp)

    info = token_info.get(CURRENT_TOKEN_INDEX)
    if info and info["remaining"] < 5:
        # try rotating
        old_idx = CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX == old_idx:
            # means only 1 token or ended up same => do we do earliest reset logic?
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()

    # if status code 403 => forcibly rotate or sleep
    if resp.status_code in (403,429):
        logging.warning("HTTP %d => forcibly rotate or sleep", resp.status_code)
        old_idx = CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX == old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
            else:
                # fallback if no data
                do_sleep_based_on_reset()

def update_token_info(token_idx, resp):
    global token_info
    rem_str = resp.headers.get("X-RateLimit-Remaining","")
    rst_str = resp.headers.get("X-RateLimit-Reset","")
    try:
        remaining = int(rem_str)
    except ValueError:
        remaining = None
    try:
        reset_ts = int(rst_str)
    except ValueError:
        reset_ts = None

    if remaining is not None and reset_ts is not None:
        token_info[token_idx] = {
            "remaining": remaining,
            "reset": reset_ts
        }

def get_all_tokens_near_limit():
    global token_info, TOKENS
    if not TOKENS:
        return False
    for idx in range(len(TOKENS)):
        info = token_info.get(idx)
        # if we lack data => assume not near-limit
        if (not info) or (info["remaining"]>=5):
            return False
    return True

def sleep_until_earliest_reset():
    global token_info, TOKENS
    if not TOKENS:
        return
    earliest = None
    import time
    now_ts = int(time.time())
    for idx in range(len(TOKENS)):
        info = token_info.get(idx)
        if not info:
            continue
        rst = info.get("reset")
        if rst and (earliest is None or rst<earliest):
            earliest=rst
    if earliest is None:
        # fallback => 1 hour
        logging.warning("No valid reset => fallback sleep 3600s")
        time.sleep(3600)
        return
    delta = earliest - now_ts + 30  # 30s buffer
    if delta>0:
        logging.warning("Sleeping %d seconds until the earliest token resets at %d (now=%d)",
                        delta, earliest, now_ts)
        time.sleep(delta)
    else:
        logging.warning("Earliest reset is in the past => no sleep needed")

def do_sleep_based_on_reset():
    # fallback 1 hour if no parse
    logging.warning("Cannot parse reset => fallback => sleep 3600s")
    import time
    time.sleep(3600)

if __name__=="__main__":
    main()
