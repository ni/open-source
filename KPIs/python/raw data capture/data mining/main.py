#!/usr/bin/env python
# main.py
"""
Final single-thread orchestrator with token rotation AND sleeping when all tokens are exhausted:
 - We fetch issues, pulls, forks, watchers, stars, comments, comment_reactions, issue_reactions, events
 - skip if created_at>baseline_date
 - watchers => fetch if enabled=1
 - mid-run baseline toggles => immediate effect
 - rotate tokens => if each token is near limit => eventually sleep until rate limit reset

We store raw_json => fix 'dict cannot be converted'. 
Auto-create DB => fix unknown DB error.
"""

import os
import sys
import logging
import yaml
import requests
import time
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

from db import connect_db, create_tables
from repo_baselines import get_baseline_info
from repos import get_repo_list
from fetch_issues import list_issues_single_thread
from fetch_pulls import list_pulls_single_thread
from fetch_comments import fetch_comments_for_all_issues
from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
from fetch_events import (
    fetch_issue_events_for_all_issues,
    fetch_pull_events_for_all_pulls
)
from fetch_forks_stars_watchers import (
    list_forks_single_thread,
    list_stars_single_thread,
    list_watchers_single_thread
)

CURRENT_TOKEN_INDEX = 0
USED_TOKEN_COUNT = 0

def load_config():
    cfg = {}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    cfg.setdefault("mysql",{
        "host":os.getenv("DB_HOST","localhost"),
        "port":int(os.getenv("DB_PORT","3306")),
        "user":os.getenv("DB_USER","root"),
        "password":os.getenv("DB_PASS","root"),
        "db":os.getenv("DB_NAME","my_kpis_analytics_db")
    })
    cfg.setdefault("tokens",[])
    cfg.setdefault("logging",{
        "file_name":"myapp.log",
        "rotate_when":"midnight",
        "backup_count":7,
        "console_level":"DEBUG",
        "file_level":"DEBUG"
    })
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

    fmt_console=logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(fmt_console)
    f_file=logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(f_file)

def rotate_token(session, tokens):
    """
    Move to the next token in the tokens list, updating session header.
    If there's only one token, rotating does nothing effectively.
    """
    global CURRENT_TOKEN_INDEX
    if not tokens:
        return
    old_idx = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(tokens)
    new_token = tokens[CURRENT_TOKEN_INDEX]
    session.headers.update({"Authorization": f"token {new_token}"})
    logging.info("Rotated token from idx %d to %d => now using token: %s...",
                 old_idx, CURRENT_TOKEN_INDEX, new_token[:10])

def handle_rate_limit(resp, session, tokens):
    """
    If near limit => rotate token or possibly sleep if all tokens are exhausted.
    We'll parse 'X-RateLimit-Remaining' & 'X-RateLimit-Reset'.
    If we do a full rotation back to the same token => means all are exhausted => sleep.
    """
    global CURRENT_TOKEN_INDEX
    if not tokens:
        return  # no tokens => can't rotate => user might get 403
    if "X-RateLimit-Remaining" not in resp.headers:
        return
    try:
        rem_val=int(resp.headers["X-RateLimit-Remaining"])
        if rem_val<5:
            logging.warning("Near rate limit => rotating token or possibly sleeping.")
            old_idx=CURRENT_TOKEN_INDEX
            rotate_token(session, tokens)
            # If after rotate, we end up with the same index => means we had only one token
            # or we came full circle => all tokens are near limit
            if CURRENT_TOKEN_INDEX==old_idx:
                # means we either have 1 token or ended back to old => all exhausted
                do_sleep_based_on_reset(resp)
    except ValueError:
        pass

def do_sleep_based_on_reset(resp):
    """
    Attempt to parse 'X-RateLimit-Reset'. If found, sleep until that time + 10s.
    If not found or parse error, sleep a default like 60s or 3600s.
    """
    import time
    reset_str=resp.headers.get("X-RateLimit-Reset","")
    if reset_str.isdigit():
        reset_ts=int(reset_str)
        now_ts=int(time.time())
        delta=reset_ts-now_ts+10
        if delta>0:
            logging.warning("All tokens appear exhausted => sleeping %d seconds for rate-limit reset",delta)
            time.sleep(delta)
    else:
        # fallback => sleep an hour if we can't parse
        logging.warning("All tokens appear exhausted, can't parse reset => sleeping 3600s")
        time.sleep(3600)

def main():
    cfg=load_config()
    setup_logging(cfg)
    logging.info("Starting single-thread script => skip if created_at>baseline_date. With token rotation & sleep if exhausted.")

    conn=connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    tokens=cfg.get("tokens",[])
    session=requests.Session()
    if tokens:
        session.headers.update({"Authorization":f"token {tokens[0]}"})

    all_repos=get_repo_list()
    for (owner,repo) in all_repos:
        from repo_baselines import get_baseline_info
        baseline_date, enabled=get_baseline_info(conn,owner,repo)
        logging.info("Repo %s/%s => baseline_date=%s, enabled=%s",owner,repo,baseline_date,enabled)

        # For convenience: short lambda for handle_rate_limit
        def rate_limit_hook(r):
            handle_rate_limit(r, session, tokens)

        # 1) issues => skip new
        list_issues_single_thread(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)
        # 2) pulls => skip new
        list_pulls_single_thread(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)

        # watchers => no date => if enabled=1
        from fetch_forks_stars_watchers import list_watchers_single_thread, list_forks_single_thread, list_stars_single_thread
        list_watchers_single_thread(conn,owner,repo,enabled,session,rate_limit_hook)
        # forks => skip if created_at>baseline_date
        list_forks_single_thread(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)
        # stars => skip if starred_at>baseline_date
        list_stars_single_thread(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)

        # events => issue_events & pull_events => skip new
        from fetch_events import (
            fetch_issue_events_for_all_issues,
            fetch_pull_events_for_all_pulls
        )
        fetch_issue_events_for_all_issues(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)
        fetch_pull_events_for_all_pulls(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)

        # comments => skip new => also comment reactions => skip new
        from fetch_comments import fetch_comments_for_all_issues
        fetch_comments_for_all_issues(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)

        # issue reactions => skip new
        from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
        fetch_issue_reactions_for_all_issues(conn,owner,repo,baseline_date,enabled,session,rate_limit_hook)

    conn.close()
    logging.info("All done => single-thread run with token rotation & sleeping on exhaustion => data stored in DB.")

if __name__=="__main__":
    main()
