#!/usr/bin/env python
# main.py
"""
Final orchestrator, single-thread approach:
 - skip items if created_at>baseline_date
 - watchers => no created_at => fetch if enabled=1
 - includes issues, pulls, forks, watchers, stars, comments, comment reactions, issue reactions
 - mid-run re-check => immediate effect if baseline changes
 - uses connect_db => create DB if missing => fix 'Unknown database'
 - uses json.dumps => fix "Python type dict cannot be converted" error
"""

import os
import sys
import json
import logging
import yaml
import requests
from logging.handlers import TimedRotatingFileHandler

from db import connect_db, create_tables
from repo_baselines import get_baseline_info
from repos import get_repo_list
from fetch_issues import list_issues_single_thread
from fetch_pulls import list_pulls_single_thread
from fetch_comments import list_issue_comments_single_thread, fetch_comment_reactions_single_thread
from fetch_issue_reactions import fetch_issue_reactions_single_thread
from fetch_forks_stars_watchers import (
    list_forks_single_thread,
    list_stars_single_thread,
    list_watchers_single_thread
)

def load_config():
    cfg={}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg=yaml.safe_load(f)
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

    f_console=logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(f_console)
    f_file=logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(f_file)

def handle_rate_limit(resp):
    if "X-RateLimit-Remaining" in resp.headers:
        try:
            rem_val=int(resp.headers["X-RateLimit-Remaining"])
            if rem_val<5:
                logging.warning("Near rate limit => might need to sleep or skip calls.")
        except ValueError:
            pass

def main():
    cfg=load_config()
    setup_logging(cfg)
    logging.info("Starting single-thread script => skip if created_at>baseline_date, watchers => fetch if enabled=1.")

    conn=connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    tokens=cfg.get("tokens",[])
    session=requests.Session()
    if tokens:
        session.headers.update({"Authorization":f"token {tokens[0]}"})

    # get repos
    all_repos=get_repo_list()
    for (owner,repo) in all_repos:
        baseline_date, enabled = get_baseline_info(conn,owner,repo)
        logging.info("Repo %s/%s => baseline_date=%s, enabled=%s",owner,repo,baseline_date,enabled)

        # 1) issues => skip new
        list_issues_single_thread(conn,owner,repo,baseline_date,enabled,session,handle_rate_limit)
        # 2) pulls => skip new
        list_pulls_single_thread(conn,owner,repo,baseline_date,enabled,session,handle_rate_limit)

        # 3) forks => skip if fork.created_at>baseline_date
        list_forks_single_thread(conn,owner,repo,baseline_date,enabled,session,handle_rate_limit)

        # 4) watchers => no created_at => fetch if enabled=1
        list_watchers_single_thread(conn,owner,repo,enabled,session,handle_rate_limit)

        # 5) stars => skip if starred_at>baseline_date
        list_stars_single_thread(conn,owner,repo,baseline_date,enabled,session,handle_rate_limit)

        # 6) optional => comments => skip new
        # for demonstration => might pick an issue # or do them all
        # list_issue_comments_single_thread(conn, owner, repo, 1, baseline_date, enabled, session, handle_rate_limit)

        # 7) optional => comment reactions => skip new
        # fetch_comment_reactions_single_thread(conn, owner, repo, 1, cmt_id, baseline_date, enabled, session, handle_rate_limit)

        # 8) optional => issue reactions => skip new
        # fetch_issue_reactions_single_thread(conn, owner, repo, issue_num, baseline_date, enabled, session, handle_rate_limit)

    conn.close()
    logging.info("All done => single-thread run complete => data in MySQL DB => skipping created_at>baseline_date as needed.")

if __name__=="__main__":
    main()
