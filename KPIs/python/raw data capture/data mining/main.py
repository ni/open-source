#!/usr/bin/env python
# main.py

import os
import sys
import time
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

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
        "host":"localhost",
        "port":3306,
        "user":"root",
        "password":"root",
        "db":"my_kpis_db"
    })
    cfg.setdefault("tokens",[])
    cfg.setdefault("logging",{
        "file_name":"myapp.log",
        "rotate_when":"midnight",
        "backup_count":7,
        "console_level":"DEBUG",
        "file_level":"DEBUG"
    })
    cfg.setdefault("days_to_capture",730)
    cfg.setdefault("max_retries",20)
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
    logging.info("[deadbird] Rotated token from idx %d to %d => not showing partial token",
                 old_idx,CURRENT_TOKEN_INDEX)

def main():
    global TOKENS, session, token_info, CURRENT_TOKEN_INDEX
    cfg=load_config()
    setup_logging(cfg)
    logging.info("[deadbird] Starting from scratch => single-thread => advanced endpoints first => watchers/forks/stars => issues/pulls => events => comments => done")

    conn=connect_db(cfg, create_db_if_missing=True)
    create_tables(conn)

    TOKENS=cfg["tokens"]
    session=setup_session_with_retry()
    token_info={}

    if TOKENS:
        session.headers["Authorization"]=f"token {TOKENS[0]}"

    days_to_capture=cfg["days_to_capture"]
    max_retries=cfg["max_retries"]

    # Import advanced fetch scripts
    from fetch_releases import list_releases_single_thread
    from fetch_labels import fetch_labels_single_thread
    from fetch_milestones import list_milestones_single_thread
    from fetch_projects import list_projects_single_thread
    from fetch_commits import list_commits_single_thread
    from fetch_branches import list_branches_single_thread
    from fetch_actions_runs import list_actions_runs_single_thread
    from fetch_code_scanning import list_code_scanning_alerts_single_thread
    from fetch_review_requests import list_review_requests_single_thread

    # Import original watchers/forks/stars, issues/pulls, events, comment-level
    from fetch_forks_stars_watchers import (
        list_watchers_single_thread, list_forks_single_thread, list_stars_single_thread
    )
    from fetch_issues import list_issues_single_thread
    from fetch_pulls import list_pulls_single_thread
    from fetch_events import fetch_issue_events_for_all_issues, fetch_pull_events_for_all_pulls
    from fetch_comments import fetch_comments_for_all_issues
    from fetch_issue_reactions import fetch_issue_reactions_for_all_issues
    from fetch_issue_comment_reactions import fetch_issue_comment_reactions_for_all_comments
    from fetch_pull_review_comments import fetch_pull_review_comments_for_all_pulls
    from fetch_pull_comment_reactions import fetch_pull_comment_reactions_for_all_comments
    from fetch_pull_reactions import fetch_pull_reactions_for_all_pulls

    all_repos=get_repo_list()

    advanced_summary=[]
    for (owner,repo) in all_repos:
        baseline_dt, enabled = get_baseline_info(conn,owner,repo)
        if not baseline_dt:
            earliest_dt=get_earliest_gh_commit_date(owner,repo)
            if earliest_dt:
                from datetime import timedelta
                new_base=earliest_dt+timedelta(days=days_to_capture)
                set_baseline_date(conn,owner,repo,new_base)
                baseline_dt=new_base
                enabled=1

        if enabled==0:
            logging.info("Repo %s/%s => disabled => skip everything",owner,repo)
            continue

        # 1) advanced endpoints first
        list_releases_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        fetch_labels_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        list_milestones_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        list_projects_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        list_commits_single_thread(conn, owner, repo, enabled, baseline_dt, session, handle_rate_limit_func, max_retries)
        list_branches_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        list_actions_runs_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        list_code_scanning_alerts_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)
        list_review_requests_single_thread(conn, owner, repo, enabled, session, handle_rate_limit_func, max_retries)

        # watchers/forks/stars
        list_watchers_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        list_forks_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        if baseline_dt:
            list_stars_single_thread(conn,owner,repo,enabled,baseline_dt,session,handle_rate_limit_func,max_retries)
        else:
            list_stars_single_thread(conn,owner,repo,enabled,None,session,handle_rate_limit_func,max_retries)

        # issues
        list_issues_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        # pulls
        list_pulls_single_thread(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

        # events
        fetch_issue_events_for_all_issues(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        fetch_pull_events_for_all_pulls(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

        # issue comments
        fetch_comments_for_all_issues(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        # issue top-level reactions
        fetch_issue_reactions_for_all_issues(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        # issue comment reactions
        fetch_issue_comment_reactions_for_all_comments(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

        # pull review comments
        fetch_pull_review_comments_for_all_pulls(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        # pull comment reactions
        fetch_pull_comment_reactions_for_all_comments(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)
        # pull top-level reactions
        fetch_pull_reactions_for_all_pulls(conn,owner,repo,enabled,session,handle_rate_limit_func,max_retries)

        stats=build_advanced_summary(conn, owner, repo)
        advanced_summary.append(stats)
        logging.info("[deadbird] Repo %s/%s => done => advanced summary built",owner,repo)

    print_advanced_summary_table(advanced_summary)

    conn.close()
    logging.info("[deadbird] All done => integrated solution => complete")

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
        logging.warning("[deadbird] HTTP %d => forcibly rotate or sleep",resp.status_code)
        old_idx=CURRENT_TOKEN_INDEX
        rotate_token()
        if CURRENT_TOKEN_INDEX==old_idx:
            if get_all_tokens_near_limit():
                sleep_until_earliest_reset()
            else:
                do_sleep_based_on_reset()

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
        logging.warning("[deadbird] No valid reset => fallback => 1hr sleep")
        time.sleep(3600)
        return
    delta=earliest-now_ts+30
    if delta>0:
        logging.warning("[deadbird] Sleeping %d seconds => earliest token resets at %d (now=%d)",
                        delta,earliest,now_ts)
        time.sleep(delta)
    else:
        logging.warning("[deadbird] earliest reset is in the past => skip sleep")

def do_sleep_based_on_reset():
    import time
    logging.warning("[deadbird] fallback => 1hr sleep => cannot parse reset")
    time.sleep(3600)

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

def get_earliest_gh_commit_date(owner,repo):
    # We do a minimal approach => fetch the first commit in ascending order
    import requests
    url=f"https://api.github.com/repos/{owner}/{repo}/commits"
    params={"sort":"committer-date","direction":"asc","per_page":1,"page":1}
    try:
        r=requests.get(url,params=params)
        if r.status_code==200:
            data=r.json()
            if not data:
                return None
            cstr=data[0].get("commit",{}).get("committer",{}).get("date")
            if not cstr:
                return None
            dt=datetime.strptime(cstr,"%Y-%m-%dT%H:%M:%SZ")
            return dt
        else:
            logging.warning("[deadbird] earliest GH commit => HTTP %d => skip => %s/%s",
                            r.status_code,owner,repo)
    except:
        logging.warning("[deadbird] earliest GH commit => error => skip => %s/%s",owner,repo)
    return None

def build_advanced_summary(conn, owner, repo):
    """
    Query advanced tables => create a dict of stats for 'owner/repo'.
    """
    repo_name=f"{owner}/{repo}"
    c=conn.cursor()
    stats={}
    # releases
    c.execute("SELECT COUNT(*) FROM releases WHERE repo_name=%s",(repo_name,))
    stats["releases_count"]=c.fetchone()[0]

    # release_assets
    c.execute("SELECT COUNT(*) FROM release_assets WHERE repo_name=%s",(repo_name,))
    stats["release_assets_count"]=c.fetchone()[0]

    # labels
    c.execute("SELECT COUNT(*) FROM repo_labels WHERE repo_name=%s",(repo_name,))
    stats["labels_count"]=c.fetchone()[0]

    # milestones
    c.execute("SELECT COUNT(*) FROM repo_milestones WHERE repo_name=%s",(repo_name,))
    stats["milestones_count"]=c.fetchone()[0]

    # projects
    c.execute("SELECT COUNT(*) FROM repo_projects WHERE repo_name=%s",(repo_name,))
    stats["projects_count"]=c.fetchone()[0]

    # commits
    c.execute("SELECT COUNT(*) FROM commits WHERE repo_name=%s",(repo_name,))
    stats["commits_count"]=c.fetchone()[0]

    # branches
    c.execute("SELECT COUNT(*) FROM branches WHERE repo_name=%s",(repo_name,))
    stats["branches_count"]=c.fetchone()[0]

    # actions_runs
    c.execute("SELECT COUNT(*) FROM actions_runs WHERE repo_name=%s",(repo_name,))
    stats["actions_runs_count"]=c.fetchone()[0]

    # code_scanning_alerts
    c.execute("SELECT COUNT(*) FROM code_scanning_alerts WHERE repo_name=%s",(repo_name,))
    stats["code_scanning_count"]=c.fetchone()[0]

    # specialized review requests
    c.execute("SELECT COUNT(*) FROM review_request_events WHERE repo_name=%s",(repo_name,))
    stats["review_requests_count"]=c.fetchone()[0]

    c.close()
    stats["owner_repo"]=repo_name
    return stats

def print_advanced_summary_table(advanced_summary):
    col_repo_width=25
    header_parts=[
      f"{'Repo':{col_repo_width}s}",
      "Releases","RelAssets","Labels","Mstones","Projects","Commits","Branches",
      "Actions","SecAlerts","ReviewReq"
    ]
    header_line="  ".join(header_parts)
    print("")
    print("========== ADVANCED SUMMARY (DEADBIRD) ==========")
    print(header_line)
    print("-"*len(header_line))

    for row in advanced_summary:
        line_parts=[]
        repo_str=row["owner_repo"][:col_repo_width]
        line_parts.append(f"{repo_str:{col_repo_width}s}")
        line_parts.append(f"{row['releases_count']:>7d}")
        line_parts.append(f"{row['release_assets_count']:>9d}")
        line_parts.append(f"{row['labels_count']:>6d}")
        line_parts.append(f"{row['milestones_count']:>7d}")
        line_parts.append(f"{row['projects_count']:>8d}")
        line_parts.append(f"{row['commits_count']:>7d}")
        line_parts.append(f"{row['branches_count']:>8d}")
        line_parts.append(f"{row['actions_runs_count']:>7d}")
        line_parts.append(f"{row['code_scanning_count']:>9d}")
        line_parts.append(f"{row['review_requests_count']:>10d}")
        print("  ".join(line_parts))

    print("=================================================")

if __name__=="__main__":
    main()
