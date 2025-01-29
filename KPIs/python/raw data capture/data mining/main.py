#!/usr/bin/env python
# main.py

import os
import sys
import time
import logging
import yaml
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter, Retry

from db import connect_db, create_tables
from repo_baselines import get_baseline_info, set_baseline_date
from repos import get_repo_list

TOKENS = []
CURRENT_TOKEN_INDEX = 0
session = None
token_info = {}

def load_config():
    cfg = {}
    if os.path.isfile("config.yaml"):
        with open("config.yaml","r",encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    # Default settings if not present
    cfg.setdefault("mysql", {
        "host":"localhost",
        "port":3306,
        "user":"root",
        "password":"root",
        "db":"my_kpis_db_etags"
    })
    cfg.setdefault("tokens", [])
    cfg.setdefault("logging", {
        "console_level":"INFO",    # We'll show only INFO+ on console
        "debug_file_mode":"daily", # or "each_run"
    })
    cfg.setdefault("days_to_capture",730)
    cfg.setdefault("max_retries",20)
    cfg.setdefault("use_etags",True)
    cfg.setdefault("use_old_approach",False)
    return cfg

def setup_logging(cfg):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # overall level

    # 1) Console Handler => only INFO+ messages
    console_level_str = cfg["logging"].get("console_level","INFO").upper()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level_str)
    console_formatter = logging.Formatter("[%(levelname)s] %(message)s")
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    # 2) File handler => date-based naming, daily overwrite approach
    # We'll define a name like debug_log_YYYYMMDD.txt

    today_str = datetime.now().strftime("%Y%m%d")  # e.g. "20231114"
    debug_filename = f"debug_log_{today_str}.txt"

    # If you truly want to overwrite each new run (even if same day),
    # use a run-based approach: datetime.now().strftime("%Y%m%d_%H%M%S")
    # Then you'd have a unique file for each run.

    # For daily approach, we use mode='a' so multiple runs in the same day
    # append to that day's file. If you prefer overwriting the entire day’s file
    # on each run, you can do mode='w'.
    debug_file_mode = 'a'
    if cfg["logging"].get("debug_file_mode") == "each_run":
        # Overwrite or unique naming
        # Overwrite => mode='w'
        debug_file_mode = 'w'
        # or unique naming => debug_filename = f"debug_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    # Default is daily approach => same day's file appended

    fh_debug = open_file_handler(debug_filename, debug_file_mode)
    fh_debug.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh_debug.setFormatter(file_formatter)
    logger.addHandler(fh_debug)

    logging.info("[deadbird-logging] Logging to %s (mode=%s), console=%s, file=DEBUG",
                 debug_filename, debug_file_mode, console_level_str)

def open_file_handler(filename, mode='a'):
    """
    Return a standard FileHandler for the debug log.
    """
    fh = logging.FileHandler(filename, mode=mode)
    return fh

def setup_session_with_retry():
    s = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=2,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def rotate_token():
    global CURRENT_TOKEN_INDEX, TOKENS, session
    if not TOKENS:
        return
    old_idx = CURRENT_TOKEN_INDEX
    CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
    new_token = TOKENS[CURRENT_TOKEN_INDEX]
    session.headers["Authorization"] = f"token {new_token}"
    logging.info("[deadbird] Rotated token from idx %d to %d => not showing partial token",
                 old_idx, CURRENT_TOKEN_INDEX)

def main():
    global TOKENS, session, token_info, CURRENT_TOKEN_INDEX
    cfg = load_config()
    setup_logging(cfg)

    logging.info("[deadbird] Starting orchestrator => hooking up all endpoints")

    conn = connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    TOKENS = cfg["tokens"]
    session = setup_session_with_retry()
    token_info = {}

    if TOKENS:
        session.headers["Authorization"] = f"token {TOKENS[0]}"

    days_to_capture = cfg["days_to_capture"]
    max_retries = cfg["max_retries"]
    use_etags = cfg["use_etags"]
    use_old = cfg["use_old_approach"]

    # watchers/forks/stars
    from fetch_forks_stars_watchers import (
        list_watchers_single_thread,
        list_forks_single_thread,
        list_stars_single_thread
    )
    # (In iteration #3, you'll see instructions to import robust_fetch, fetch scripts, etc.)
    # For now, let's just show the skeleton. We'll do the rest later.

    # issues/pulls
    from fetch_issues import list_issues_single_thread
    from fetch_pulls import list_pulls_single_thread

    # advanced endpoints
    from fetch_commits import list_commits_single_thread
    from fetch_code_scanning import list_code_scanning_alerts_single_thread
    from fetch_releases import list_releases_single_thread
    from fetch_labels import fetch_labels_single_thread
    from fetch_milestones import list_milestones_single_thread
    from fetch_projects import list_projects_single_thread
    from fetch_branches import list_branches_single_thread
    from fetch_actions_runs import list_actions_runs_single_thread

    # pull-based advanced
    from fetch_pull_events import fetch_pull_events_for_all_pulls
    from fetch_review_requests import list_review_requests_single_thread
    from fetch_pull_review_comments import fetch_pull_review_comments_for_all_pulls
    from fetch_pull_comment_reactions import fetch_pull_comment_reactions_for_all_comments
    from fetch_pull_reactions import fetch_pull_reactions_for_all_pulls

    # issue-based advanced => Comments, events, reactions
    from fetch_comments import fetch_issue_comments_for_all_issues
    from fetch_comment_reactions import fetch_issue_comment_reactions_for_all_comments
    from fetch_issue_events import fetch_issue_events_for_all_issues
    from fetch_issue_reactions import fetch_issue_reactions_for_all_issues

    all_repos = get_repo_list()
    for (owner, repo) in all_repos:
        baseline_dt, enabled = get_baseline_info(conn, owner, repo)
        if not baseline_dt:
            earliest_dt = get_earliest_gh_commit_date(owner, repo)
            if earliest_dt:
                new_base = earliest_dt + timedelta(days=days_to_capture)
                set_baseline_date(conn, owner, repo, new_base)
                baseline_dt = new_base
                enabled = 1

        if enabled == 0:
            logging.info("Repo %s/%s => disabled => skip everything", owner, repo)
            continue

        # watchers/forks/stars
        list_watchers_single_thread(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries,
                                    use_etags=(use_etags and not use_old))

        list_forks_single_thread(conn, owner, repo, enabled,
                                 session, handle_rate_limit_func,
                                 max_retries,
                                 use_etags=(use_etags and not use_old))

        list_stars_single_thread(conn, owner, repo, enabled,
                                 session, handle_rate_limit_func,
                                 max_retries,
                                 use_etags=(use_etags and not use_old))

        # issues
        list_issues_single_thread(conn, owner, repo, enabled,
                                  session, handle_rate_limit_func,
                                  max_retries,
                                  use_etags=(use_etags and not use_old))

        # => missing advanced issue data
        fetch_issue_comments_for_all_issues(conn, owner, repo, enabled,
                                            session, handle_rate_limit_func,
                                            max_retries)
        fetch_issue_comment_reactions_for_all_comments(conn, owner, repo, enabled,
                                                       session, handle_rate_limit_func,
                                                       max_retries)
        fetch_issue_events_for_all_issues(conn, owner, repo, enabled,
                                          session, handle_rate_limit_func,
                                          max_retries)
        fetch_issue_reactions_for_all_issues(conn, owner, repo, enabled,
                                             session, handle_rate_limit_func,
                                             max_retries)

        # pulls
        list_pulls_single_thread(conn, owner, repo, enabled,
                                 session, handle_rate_limit_func,
                                 max_retries,
                                 use_etags=(use_etags and not use_old))

        # advanced pull stuff => previously missing
        fetch_pull_events_for_all_pulls(conn, owner, repo, enabled,
                                        session, handle_rate_limit_func,
                                        max_retries)

        list_review_requests_single_thread(conn, owner, repo, enabled,
                                           session, handle_rate_limit_func,
                                           max_retries)

        fetch_pull_review_comments_for_all_pulls(conn, owner, repo, enabled,
                                                 session, handle_rate_limit_func,
                                                 max_retries)

        fetch_pull_comment_reactions_for_all_comments(conn, owner, repo, enabled,
                                                      session, handle_rate_limit_func,
                                                      max_retries)

        fetch_pull_reactions_for_all_pulls(conn, owner, repo, enabled,
                                           session, handle_rate_limit_func,
                                           max_retries)

        # advanced endpoints
        list_commits_single_thread(conn, owner, repo, enabled, baseline_dt,
                                   session, handle_rate_limit_func,
                                   max_retries,
                                   use_etags=(use_etags and not use_old))

        list_code_scanning_alerts_single_thread(conn, owner, repo, enabled,
                                                session, handle_rate_limit_func,
                                                max_retries,
                                                use_etags=(use_etags and not use_old))

        list_releases_single_thread(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries,
                                    use_etags=(use_etags and not use_old))

        fetch_labels_single_thread(conn, owner, repo, enabled,
                                   session, handle_rate_limit_func,
                                   max_retries,
                                   use_etags=(use_etags and not use_old))

        list_milestones_single_thread(conn, owner, repo, enabled,
                                      session, handle_rate_limit_func,
                                      max_retries,
                                      use_etags=(use_etags and not use_old))

        list_projects_single_thread(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries,
                                    use_etags=(use_etags and not use_old))

        list_branches_single_thread(conn, owner, repo, enabled,
                                    session, handle_rate_limit_func,
                                    max_retries,
                                    use_etags=(use_etags and not use_old))

        list_actions_runs_single_thread(conn, owner, repo, enabled,
                                        session, handle_rate_limit_func,
                                        max_retries,
                                        use_etags=(use_etags and not use_old))

        logging.info("[deadbird] Repo %s/%s => done => all endpoints called", owner, repo)

        logging.info("[deadbird] Repo %s/%s => done with endpoints (placeholder).", owner, repo)

    conn.close()
    logging.info("[deadbird] All done => watchers/forks/stars, issues/pulls, advanced => (placeholder).")

def handle_rate_limit_func(resp):
    global TOKENS, CURRENT_TOKEN_INDEX, session, token_info
    update_token_info(CURRENT_TOKEN_INDEX, resp)
    info = token_info.get(CURRENT_TOKEN_INDEX)
    if info and info["remaining"] < 5:
        old_idx = CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX == old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
    if resp.status_code in (403,429):
        logging.warning("[deadbird] HTTP %d => forcibly rotate or sleep",resp.status_code)
        old_idx = CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX == old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
            else:
                do_sleep_based_on_reset()

def get_all_tokens_near_limit():
    global token_info, TOKENS
    if not TOKENS:
        return False
    for idx in range(len(TOKENS)):
        info = token_info.get(idx)
        if not info or info["remaining"] >= 5:
            return False
    return True

def sleep_until_earliest_reset():
    import time
    global token_info, TOKENS
    if not TOKENS:
        return
    earliest = None
    now_ts = int(time.time())
    for idx in range(len(TOKENS)):
        info = token_info.get(idx)
        if info:
            rst = info.get("reset")
            if rst and (earliest is None or rst < earliest):
                earliest = rst
    if earliest is None:
        logging.warning("[deadbird] fallback => 1hr sleep => cannot parse earliest reset")
        time.sleep(3600)
        return
    delta = earliest - now_ts + 30
    if delta > 0:
        logging.warning("[deadbird] Sleeping %d sec => earliest token resets at %d (now=%d)",
                        delta, earliest, now_ts)
        time.sleep(delta)
    else:
        logging.warning("[deadbird] earliest reset is in the past => skip sleep")

def do_sleep_based_on_reset():
    import time
    logging.warning("[deadbird] fallback => 1hr sleep => cannot parse reset header")
    time.sleep(3600)

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
        token_info[token_idx] = {"remaining": remaining, "reset": reset_ts}

def get_earliest_gh_commit_date(owner, repo):
    import requests
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    params = {"sort":"committer-date","direction":"asc","per_page":1}
    try:
        r = requests.get(url,params=params)
        if r.status_code == 200:
            data = r.json()
            if not data:
                return None
            cstr = data[0].get("commit",{}).get("committer",{}).get("date")
            if not cstr:
                return None
            dt = datetime.strptime(cstr, "%Y-%m-%dT%H:%M:%SZ")
            return dt
        else:
            logging.warning("[deadbird] earliest GH commit => HTTP %d => skip => %s/%s",
                            r.status_code, owner, repo)
    except:
        logging.warning("[deadbird] earliest GH commit => error => skip => %s/%s", owner, repo)
    return None

if __name__=="__main__":
    main()
