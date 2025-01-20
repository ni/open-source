#!/usr/bin/env python
# main.py
"""
Final single-thread orchestrator with robust approach:
 - handle_rate_limit_func rotates tokens or sleeps for near-limit
 - pass max_retries from config.yaml
 - watchers, forks, stars, issues, pulls, events, comments, issue reactions => 
   each fetch uses robust_get_page, which now re-tries 500,502,503,504 as well.
"""

import os
import sys
import time
import logging
import yaml
import requests
from logging.handlers import TimedRotatingFileHandler

from db import connect_db, create_tables
from repo_baselines import get_baseline_info
from repos import get_repo_list

CURRENT_TOKEN_INDEX = 0
TOKENS = []
session = None

def load_config():
    cfg = {}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    cfg.setdefault("mysql", {
        "host": os.getenv("DB_HOST","localhost"),
        "port": int(os.getenv("DB_PORT","3306")),
        "user": os.getenv("DB_USER","root"),
        "password": os.getenv("DB_PASS","root"),
        "db": os.getenv("DB_NAME","my_kpis_analytics_db")
    })
    cfg.setdefault("tokens", [])
    cfg.setdefault("logging", {
        "file_name": "myapp.log",
        "rotate_when": "midnight",
        "backup_count": 7,
        "console_level": "DEBUG",
        "file_level": "DEBUG"
    })
    # define global max_retries
    cfg.setdefault("max_retries", 20)
    return cfg

def setup_logging(cfg):
    log_conf = cfg.get("logging", {})
    log_file = log_conf.get("file_name","myapp.log")
    rotate_when = log_conf.get("rotate_when","midnight")
    backup_count = log_conf.get("backup_count",7)
    console_level = log_conf.get("console_level","DEBUG")
    file_level = log_conf.get("file_level","DEBUG")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level.upper())
    logger.addHandler(ch)

    fh = TimedRotatingFileHandler(log_file, when=rotate_when, backupCount=backup_count)
    fh.setLevel(file_level.upper())
    logger.addHandler(fh)

    fmt_console = logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(fmt_console)
    f_file = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(f_file)

def rotate_token():
    global CURRENT_TOKEN_INDEX, TOKENS, session
    if not TOKENS:
        return
    old_idx = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX+1) % len(TOKENS)
    new_token = TOKENS[CURRENT_TOKEN_INDEX]
    session.headers["Authorization"] = f"token {new_token}"
    logging.info("Rotated token from idx %d to %d => now using token: %s...",
                 old_idx, CURRENT_TOKEN_INDEX, new_token[:10])

def do_sleep_based_on_reset(resp):
    reset_str=resp.headers.get("X-RateLimit-Reset","")
    if reset_str.isdigit():
        reset_ts=int(reset_str)
        now_ts=int(time.time())
        delta=reset_ts-now_ts+10
        if delta>0:
            logging.warning("Sleeping %d seconds for rate-limit reset...",delta)
            time.sleep(delta)
    else:
        logging.warning("Cannot parse reset => fallback => sleep 3600s")
        time.sleep(3600)

def handle_rate_limit_func(resp):
    global TOKENS, CURRENT_TOKEN_INDEX, session
    if not TOKENS:
        return
    if "X-RateLimit-Remaining" in resp.headers:
        try:
            rem_val=int(resp.headers["X-RateLimit-Remaining"])
            if rem_val<5:
                logging.warning("Near rate limit => rotate or sleep.")
                old_idx=CURRENT_TOKEN_INDEX
                rotate_token()
                if CURRENT_TOKEN_INDEX==old_idx:
                    do_sleep_based_on_reset(resp)
        except ValueError:
            pass
    if resp.status_code in (403,429):
        logging.warning("HTTP %d => forcibly rotate or sleep", resp.status_code)
        old_idx=CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX==old_idx:
            do_sleep_based_on_reset(resp)

def main():
    global TOKENS, session
    cfg=load_config()
    setup_logging(cfg)
    logging.info("Starting single-thread => robust approach => re-try 5xx => skip if fails repeatedly.")

    conn=connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    TOKENS=cfg.get("tokens",[])
    session=requests.Session()
    if TOKENS:
        session.headers["Authorization"] = f"token {TOKENS[0]}"

    max_retries = cfg.get("max_retries",20)
    logging.info("Global max_retries => %d",max_retries)

    all_repos = get_repo_list()
    for (owner, repo) in all_repos:
        baseline_date, enabled = get_baseline_info(conn, owner, repo)
        logging.info("Repo %s/%s => baseline_date=%s, enabled=%s", owner, repo, baseline_date, enabled)

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

        # events => issue_events & pull_events => skip if new
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
    logging.info("All done => watchers, forks, stars, issues, pulls, events, comments, issue reactions => robust => re-tries 5xx => success.")

if __name__=="__main__":
    main()
