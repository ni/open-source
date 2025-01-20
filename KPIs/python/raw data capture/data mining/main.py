#!/usr/bin/env python
# main.py
"""
Single-thread orchestrator with:
 - Preemptive checks if all tokens near-limit => parse X-RateLimit-Reset => earliest reset => sleep
 - Dictionary token_info => store each token's remaining, reset
 - No partial token strings in logs
 - re-check all tokens after sleeping
 - re-try logic for 403,429,500,502,503,504
 - single-run approach
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
token_info = {}  # e.g. token_info[token_idx] = {"remaining": ..., "reset": ...}

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

    f_console = logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(f_console)
    f_file = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(f_file)

def rotate_token():
    global CURRENT_TOKEN_INDEX, TOKENS, session
    if not TOKENS:
        return
    old_idx = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
    new_token = TOKENS[CURRENT_TOKEN_INDEX]
    session.headers["Authorization"] = f"token {new_token}"
    logging.info("Rotated token from index %d to %d => not showing partial token string for security",
                 old_idx, CURRENT_TOKEN_INDEX)

def update_token_info(token_idx, resp):
    """
    Parse X-RateLimit-Remaining, X-RateLimit-Reset from resp, store in token_info dict.
    If parse fails or not present => do nothing. We'll just handle that gracefully.
    """
    global token_info
    rem_str = resp.headers.get("X-RateLimit-Remaining","")
    rst_str = resp.headers.get("X-RateLimit-Reset","")
    if rem_str.isdigit():
        remaining = int(rem_str)
    else:
        remaining = None
    if rst_str.isdigit():
        reset_ts = int(rst_str)
    else:
        reset_ts = None

    if remaining is not None and reset_ts is not None:
        token_info[token_idx] = {
            "remaining": remaining,
            "reset": reset_ts
        }

def get_all_tokens_near_limit():
    """
    Return True if *all* tokens in token_info have 'remaining'<5
    Also, only check tokens if we actually have info for them
    If we don't have data for a token => assume not near limit
    """
    global token_info, TOKENS
    if not TOKENS:
        return False
    for idx in range(len(TOKENS)):
        info = token_info.get(idx)
        if (not info) or (info["remaining"] and info["remaining"] >= 5):
            return False
    return True

def sleep_until_earliest_reset():
    """
    Among all tokens, pick the earliest 'reset' time => sleep until then + 30sec buffer
    If can't parse => fallback 1 hour
    If earliest reset is in the past => skip sleep
    """
    global token_info, TOKENS
    if not TOKENS:
        return
    earliest = None
    for idx in range(len(TOKENS)):
        info = token_info.get(idx)
        if not info:
            # no data => skip
            continue
        rst = info.get("reset")
        if not rst:
            continue
        if earliest is None or rst < earliest:
            earliest = rst
    if earliest is None:
        # fallback => 1 hour
        logging.warning("No valid reset times => fallback to sleep 3600s")
        time.sleep(3600)
        return

    now_ts = int(time.time())
    delta = earliest - now_ts + 30  # 30-second buffer
    if delta<=0:
        logging.warning("Earliest reset is in the past => no sleep needed.")
        return
    logging.warning("Sleeping %d seconds until the earliest token resets at %d (now=%d)",
                    delta, earliest, now_ts)
    time.sleep(delta)

def do_sleep_based_on_reset():
    """
    If we can't parse => fallback 1 hour
    (this is used if we specifically see a 403 for the current token 
     and can't parse a reset. Or all tokens are near-limit but no reset found.)
    But we replaced logic with sleep_until_earliest_reset above.
    We'll keep fallback 1 hour if needed.
    """
    logging.warning("Cannot parse reset => fallback => sleep 3600s")
    time.sleep(3600)

def handle_rate_limit_func(resp):
    """
    Called after each request in robust_get_page. 
    1) update token_info with X-RateLimit-Remaining, X-RateLimit-Reset
    2) if current token near-limit => rotate
    3) if all tokens near-limit => compute earliest reset => sleep => re-check
    4) if 403 => forcibly rotate or sleep
    """
    global TOKENS, CURRENT_TOKEN_INDEX, session, token_info
    if not TOKENS:
        return

    # 1) Store info for CURRENT_TOKEN_INDEX
    update_token_info(CURRENT_TOKEN_INDEX, resp)

    # Check near-limit
    info = token_info.get(CURRENT_TOKEN_INDEX)
    if info and info["remaining"]<5:
        # try rotating
        old_idx = CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX == old_idx:
            # means only 1 token or we ended up same => check if all tokens near-limit
            if get_all_tokens_near_limit():
                # do earliest reset approach => preemptive
                sleep_until_earliest_reset()
                # after sleeping => re-check tokens => no special code needed here, 
                # next request we'll see updated times
    # if 403 => forcibly rotate or sleep
    if resp.status_code in (403,429):
        logging.warning("HTTP %d => forcibly rotate or sleep", resp.status_code)
        old_idx=CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX==old_idx:
            # all tokens near-limit => do earliest reset approach
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
            else:
                # fallback if no data
                do_sleep_based_on_reset()

def main():
    global TOKENS, session, token_info
    cfg = load_config()
    setup_logging(cfg)
    logging.info("Starting single-thread => robust approach => parse X-RateLimit-Reset, do preemptive checks, no partial token logs")

    conn = connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    TOKENS = cfg.get("tokens",[])
    session = requests.Session()
    token_info = {}  # reset this at start
    if TOKENS:
        session.headers["Authorization"] = f"token {TOKENS[0]}"

    max_retries = cfg.get("max_retries",20)
    logging.info("Global max_retries => %d", max_retries)

    from repos import get_repo_list
    all_repos = get_repo_list()
    from repo_baselines import get_baseline_info

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
    logging.info("All done => watchers, forks, stars, issues, pulls, events, comments, issue reactions => integrated preemptive approach with earliest reset => success.")

if __name__=="__main__":
    main()
