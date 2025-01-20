#!/usr/bin/env python
# main.py

import os
import sys
import logging
import yaml
import time
import requests
from logging.handlers import TimedRotatingFileHandler

from db import connect_db, create_tables
from repos import get_enabled_repos
from issues import get_issues_for_repo, get_issue_last_id, update_issue_last_id
from pulls import get_pulls_for_repo, get_pull_last_id, update_pull_last_id
from fetch_issue_events import fetch_issue_events
from fetch_pull_events import fetch_pull_events
from fetch_comment_reactions import fetch_issue_comment_reactions

TOKENS = []
CURRENT_TOKEN_INDEX = 0
LOCAL_MODE = True
SLEEP_ON_LIMIT = True

def load_config():
    global LOCAL_MODE
    cfg = {}
    if os.path.isfile("config.yaml"):
        LOCAL_MODE = True
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        cfg = {
            "mysql":{
                "host":os.getenv("DB_HOST","localhost"),
                "port":int(os.getenv("DB_PORT","3306")),
                "user":os.getenv("DB_USER","root"),
                "password":os.getenv("DB_PASS","root"),
                "db":os.getenv("DB_NAME","my_kpis_analytics_db")
            },
            "tokens":[
                os.getenv("GITHUB_TOKEN1",""),
                os.getenv("GITHUB_TOKEN2","")
            ],
            "logging":{
                "file_name":os.getenv("LOG_FILE_NAME","events.log"),
                "rotate_when":"midnight",
                "backup_count":7,
                "console_level":"DEBUG",
                "file_level":"DEBUG"
            }
        }
    cfg["tokens"] = [t for t in cfg.get("tokens",[]) if t]
    return cfg

def setup_logging(cfg):
    log_conf = cfg.get("logging",{})
    log_file = log_conf.get("file_name","events.log")
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
    fmt_file = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(fmt_file)

def load_tokens(cfg):
    global TOKENS
    TOKENS = cfg["tokens"]
    if TOKENS:
        logging.info("Loaded %d token(s).", len(TOKENS))
    else:
        logging.warning("No GitHub tokens => low rate limit.")

def get_session():
    s = requests.Session()
    if TOKENS:
        s.headers.update({"Authorization": f"token {TOKENS[0]}"})
    s.headers.update({"Accept":"application/vnd.github.v3+json"})
    return s

def rotate_token():
    global CURRENT_TOKEN_INDEX
    old = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
    logging.debug("Rotated token from %d to %d", old, CURRENT_TOKEN_INDEX)

def handle_rate_limit(resp):
    if "X-RateLimit-Remaining" not in resp.headers:
        return
    try:
        rem_val = int(resp.headers["X-RateLimit-Remaining"])
        if rem_val < 5:
            logging.warning("Approaching rate limit => rotate or sleep.")
            if len(TOKENS) > 1:
                rotate_token()
            else:
                if LOCAL_MODE and SLEEP_ON_LIMIT:
                    reset_str = resp.headers.get("X-RateLimit-Reset","0")
                    if reset_str.isdigit():
                        now_ts = int(time.time())
                        reset_ts = int(reset_str)
                        wait = reset_ts - now_ts + 5
                        if wait>0:
                            logging.warning("Sleeping %d sec for rate-limit reset...", wait)
                            time.sleep(wait)
                else:
                    logging.warning("No extra tokens => partial skip calls.")
    except ValueError:
        pass

def main():
    cfg = load_config()
    setup_logging(cfg)
    load_tokens(cfg)

    conn = connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    session = get_session()

    from repos import get_enabled_repos
    repos_list = get_enabled_repos(conn)

    # For each repo, single-thread approach: issues, then pulls
    from issues import get_issues_for_repo, get_issue_last_id, update_issue_last_id
    from pulls import get_pulls_for_repo, get_pull_last_id, update_pull_last_id
    from fetch_issue_events import fetch_issue_events
    from fetch_pull_events import fetch_pull_events
    from fetch_comment_reactions import fetch_issue_comment_reactions

    for (owner, repo) in repos_list:
        # 1) issues
        issues_list = get_issues_for_repo(conn, owner, repo)
        for (ow, rp, inum) in issues_list:
            reponame = f"{ow}/{rp}"
            old_id = get_issue_last_id(conn, reponame, inum)
            new_id = fetch_issue_events(ow, rp, inum,
                                        session, conn,
                                        last_event_id=old_id,
                                        overlap_pages=1,
                                        handle_rate_limit_func=handle_rate_limit)
            update_issue_last_id(conn, reponame, inum, new_id)

            # optional: fetch comment-level reactions for this issue
            # fetch_issue_comment_reactions(ow, rp, inum, session, conn, handle_rate_limit)

        # 2) pulls
        pulls_list = get_pulls_for_repo(conn, owner, repo)
        for (ow, rp, pnum) in pulls_list:
            reponame = f"{ow}/{rp}"
            old_id = get_pull_last_id(conn, reponame, pnum)
            new_id = fetch_pull_events(ow, rp, pnum,
                                       session, conn,
                                       last_event_id=old_id,
                                       overlap_pages=1,
                                       handle_rate_limit_func=handle_rate_limit)
            update_pull_last_id(conn, reponame, pnum, new_id)

    conn.close()
    logging.info("All done. Single-thread approach finished for enabled repos in 'repos' table.")

if __name__=="__main__":
    main()