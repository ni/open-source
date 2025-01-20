#!/usr/bin/env python
# main.py
"""
Final orchestrator. Single-thread. 
- Reads config, connect DB (creating it if missing).
- create_tables
- For each repo, load baseline_date & enabled => skip if baseline_date is given & item is newer than that date.
- List issues, list pulls, (optionally list comments, etc.).
- Provides instructions for local usage or GitHub action.

We fix the "Unknown database" error by ensuring the DB is created in connect_db.
"""

import os
import sys
import logging
import yaml
import requests
from logging.handlers import TimedRotatingFileHandler

from db import connect_db, create_tables
from repo_baselines import get_baseline_info
from repos import get_repo_list
from fetch_issues import list_issues_single_thread
from fetch_pulls import list_pulls_single_thread
from fetch_comments import list_issue_comments_single_thread

def load_config():
    cfg={}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg=yaml.safe_load(f)
    # fallback => environment 
    cfg.setdefault("mysql", {
        "host":os.getenv("DB_HOST","localhost"),
        "port":int(os.getenv("DB_PORT","3306")),
        "user":os.getenv("DB_USER","root"),
        "password":os.getenv("DB_PASS","root"),
        "db":os.getenv("DB_NAME","my_kpis_analytics_db")
    })
    cfg.setdefault("tokens", [])
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
    # minimal approach => if near limit => just log
    if "X-RateLimit-Remaining" in resp.headers:
        try:
            rem_val=int(resp.headers["X-RateLimit-Remaining"])
            if rem_val<5:
                logging.warning("Near rate limit => be cautious or sleep.")
        except ValueError:
            pass

def main():
    cfg=load_config()
    setup_logging(cfg)
    logging.info("Starting single-thread script with baseline skip logic (created_at>baseline_date => skip).")

    conn=connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    tokens=cfg.get("tokens",[])
    session=requests.Session()
    if tokens:
        session.headers.update({"Authorization":f"token {tokens[0]}"})

    # fetch all repos from a function
    all_repos=get_repo_list()
    for (owner,repo) in all_repos:
        # get baseline_date + enabled
        baseline_date, enabled = get_baseline_info(conn, owner, repo)
        logging.info("Repo %s/%s => baseline_date=%s, enabled=%s",
                     owner, repo, baseline_date, enabled)

        # 1) List issues => skip newer than baseline
        list_issues_single_thread(conn, owner, repo, baseline_date, enabled, session, handle_rate_limit)
        # 2) List pulls => skip newer than baseline
        list_pulls_single_thread(conn, owner, repo, baseline_date, enabled, session, handle_rate_limit)

        # optional => for each issue, we might list comments, skipping newer
        # in a real scenario you'd discover which issues are newly inserted 
        # or updated. For demonstration:
        # if we inserted issues table => we can do a pass for each row 
        # and call list_issue_comments_single_thread

    conn.close()
    logging.info("All done => single-thread run complete, skipping created_at>baseline_date if set.")

if __name__=="__main__":
    main()
