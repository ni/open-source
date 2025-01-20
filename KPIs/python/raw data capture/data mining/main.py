#!/usr/bin/env python
# main.py
"""
Final single-thread orchestrator with token rotation:
 - We fetch issues, pulls, forks, watchers, stars, comments, comment_reactions, issue_reactions, events
 - skip items if created_at>baseline_date
 - watchers => fetch if enabled=1
 - mid-run re-check => immediate effect if baseline changes
 - token rotation => if X-RateLimit-Remaining < threshold => switch to next token

We store raw_json with json.dumps => fix 'Python type dict cannot be converted'.
DB => auto-created => fix unknown DB error.
"""

import os
import sys
import json
import logging
import yaml
import requests
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

# Global to track current token index
CURRENT_TOKEN_INDEX = 0

def load_config():
    cfg = {}
    if os.path.isfile("config.yaml"):
        with open("config.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    cfg.setdefault("mysql", {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", "root"),
        "db": os.getenv("DB_NAME", "my_kpis_analytics_db")
    })
    cfg.setdefault("tokens", [])
    cfg.setdefault("logging", {
        "file_name": "myapp.log",
        "rotate_when": "midnight",
        "backup_count": 7,
        "console_level": "DEBUG",
        "file_level": "DEBUG"
    })
    return cfg

def setup_logging(cfg):
    log_conf = cfg.get("logging", {})
    log_file = log_conf.get("file_name", "myapp.log")
    rotate_when = log_conf.get("rotate_when", "midnight")
    backup_count = log_conf.get("backup_count", 7)
    console_level = log_conf.get("console_level", "DEBUG")
    file_level = log_conf.get("file_level", "DEBUG")

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

def rotate_token(session, tokens):
    """
    Move to the next token in the tokens list, update session header.
    Helps spread out calls across multiple tokens, preventing 403 rate-limits.
    """
    global CURRENT_TOKEN_INDEX
    if not tokens:
        return
    old_idx = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(tokens)
    new_token = tokens[CURRENT_TOKEN_INDEX]
    session.headers.update({"Authorization": f"token {new_token}"})
    logging.info("Rotated token from index %d to %d => now using token: %s...",
                 old_idx, CURRENT_TOKEN_INDEX, new_token[:10])

def handle_rate_limit(resp, session, tokens):
    """
    If near or at rate limit => rotate token or possibly sleep.
    By default, if X-RateLimit-Remaining < 5 => rotate.
    """
    if "X-RateLimit-Remaining" in resp.headers:
        try:
            rem_val = int(resp.headers["X-RateLimit-Remaining"])
            if rem_val < 5:
                logging.warning("Near rate limit => rotating token or skipping calls.")
                rotate_token(session, tokens)
        except ValueError:
            pass

def main():
    cfg = load_config()
    setup_logging(cfg)
    logging.info("Starting single-thread script => fetch data & skip if created_at>baseline_date, watchers => fetch if enabled=1. With token rotation.")

    conn = connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    tokens = cfg.get("tokens", [])
    session = requests.Session()

    # Use first token if available
    if tokens:
        session.headers.update({"Authorization": f"token {tokens[0]}"})

    all_repos = get_repo_list()
    for (owner, repo) in all_repos:
        from repo_baselines import get_baseline_info
        baseline_date, enabled = get_baseline_info(conn, owner, repo)
        logging.info("Repo %s/%s => baseline_date=%s, enabled=%s", owner, repo, baseline_date, enabled)

        # 1) issues => skip new
        list_issues_single_thread(conn, owner, repo, baseline_date, enabled, session,
                                  lambda r: handle_rate_limit(r, session, tokens))
        # 2) pulls => skip new
        list_pulls_single_thread(conn, owner, repo, baseline_date, enabled, session,
                                 lambda r: handle_rate_limit(r, session, tokens))

        # watchers => no date => fetch if enabled=1
        from fetch_forks_stars_watchers import list_watchers_single_thread, list_forks_single_thread, list_stars_single_thread
        list_watchers_single_thread(conn, owner, repo, enabled, session,
                                    lambda r: handle_rate_limit(r, session, tokens))

        # forks => skip if created_at>baseline_date
        list_forks_single_thread(conn, owner, repo, baseline_date, enabled, session,
                                 lambda r: handle_rate_limit(r, session, tokens))

        # stars => skip if starred_at>baseline_date
        list_stars_single_thread(conn, owner, repo, baseline_date, enabled, session,
                                 lambda r: handle_rate_limit(r, session, tokens))

        # events => issue_events & pull_events => skip new
        from fetch_events import (
            fetch_issue_events_for_all_issues,
            fetch_pull_events_for_all_pulls
        )
        fetch_issue_events_for_all_issues(conn, owner, repo, baseline_date, enabled, session,
                                          lambda r: handle_rate_limit(r, session, tokens))
        fetch_pull_events_for_all_pulls(conn, owner, repo, baseline_date, enabled, session,
                                        lambda r: handle_rate_limit(r, session, tokens))

        # comments => skip new, plus comment_reactions => skip new
        from fetch_comments import fetch_comments_for_all_issues
        fetch_comments_for_all_issues(conn, owner, repo, baseline_date, enabled, session,
                                      lambda r: handle_rate_limit(r, session, tokens))

        # issue reactions => skip if reaction.created_at>baseline_date
        from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
        fetch_issue_reactions_for_all_issues(conn, owner, repo, baseline_date, enabled, session,
                                             lambda r: handle_rate_limit(r, session, tokens))

    conn.close()
    logging.info("All done => single-thread run with token rotation => data stored in MySQL DB.")

if __name__=="__main__":
    main()
