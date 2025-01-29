# fetch_forks_stars_watchers.py

import logging
import time
import requests
from datetime import datetime

from robust_fetch import robust_get_page

def list_watchers_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries,
                                use_etags=True):
    """
    Watchers still do a full fetch because there's no watchers 'watched_at'.
    The code is unchanged from previous versions.
    """
    if enabled == 0:
        logging.info("[deadbird/watchers] %s/%s => disabled => skip watchers", owner, repo)
        return

    repo_name = f"{owner}/{repo}"
    endpoint = "watchers"

    logging.info("[deadbird/watchers] => calling watchers => %s", repo_name)
    page = 1
    total_inserted = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/subscribers"
        params = {"page": page, "per_page": 100}
        (resp, success) = robust_get_page(session, url, params,
                                          handle_rate_limit_func, max_retries,
                                          endpoint=endpoint)
        if not success or not resp:
            break

        data = resp.json()
        if not data:
            break

        new_count = 0
        for w in data:
            if insert_watcher_record(conn, repo_name, w):
                new_count += 1
        total_inserted += new_count

        if len(data) < 100:
            break
        page += 1

    logging.info("[deadbird/watchers] => done => inserted=%d => %s", total_inserted, repo_name)


def insert_watcher_record(conn, repo_name, user_obj):
    c = conn.cursor()
    user_login = user_obj.get("login","")
    c.execute("""
      SELECT user_login FROM watchers
      WHERE repo_name=%s AND user_login=%s
    """,(repo_name,user_login))
    row = c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        raw_str = json.dumps(user_obj, ensure_ascii=False)
        sql = """
        INSERT INTO watchers
          (repo_name, user_login, raw_json)
        VALUES
          (%s, %s, %s)
        """
        c.execute(sql, (repo_name, user_login, raw_str))
        conn.commit()
        c.close()
        return True


def list_forks_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries,
                             use_etags=True,
                             baseline_dt=None):
    """
    (Unchanged) If you want local skipping or baseline approach for forks,
    you can keep it. e.g.:

    - skip entire call if baseline_dt < now (optional)
    - local skip if fork.created_at > baseline_dt
    - or do a full fetch if you'd like

    We'll leave it as is from previous code.
    """
    if enabled == 0:
        logging.info("[deadbird/forks] %s/%s => disabled => skip forks", owner, repo)
        return

    repo_name = f"{owner}/{repo}"
    endpoint = "forks"

    # Example logic (unchanged from previous):
    if baseline_dt:
        now_dt = datetime.utcnow()
        if baseline_dt < now_dt:
            logging.debug("[deadbird/forks] baseline_dt < now => skip entire call => %s", repo_name)
            return

    logging.info("[deadbird/forks] => calling => local skip if fork.created_at > baseline_dt => %s", repo_name)

    page = 1
    total_inserted = 0
    total_skipped = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/forks"
        params = {"page": page, "per_page": 100, "sort": "oldest"}
        (resp, success) = robust_get_page(session, url, params,
                                          handle_rate_limit_func, max_retries,
                                          endpoint=endpoint)
        if not success or not resp:
            break

        data = resp.json()
        if not data:
            break

        for fk in data:
            fork_created_str = fk.get("created_at")
            if fork_created_str:
                fork_created_dt = datetime.strptime(fork_created_str, "%Y-%m-%dT%H:%M:%SZ")
                if baseline_dt and fork_created_dt > baseline_dt:
                    logging.debug("[deadbird/forks] skipping => fork_created_dt=%s => baseline_dt=%s => %s",
                                  fork_created_dt, baseline_dt, repo_name)
                    total_skipped += 1
                    continue

            if insert_fork_record(conn, repo_name, fk):
                total_inserted += 1

        if len(data) < 100:
            break
        page += 1

    logging.info("[deadbird/forks] => done => inserted=%d, skipped=%d => %s",
                 total_inserted, total_skipped, repo_name)


def insert_fork_record(conn, repo_name, fork_obj):
    c = conn.cursor()
    fork_id = fork_obj.get("id")
    c.execute("""
      SELECT fork_id FROM forks
      WHERE repo_name=%s AND fork_id=%s
    """, (repo_name, fork_id))
    row = c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        raw_str = json.dumps(fork_obj, ensure_ascii=False)
        created_str = fork_obj.get("created_at")
        created_dt = None
        if created_str:
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

        sql = """
        INSERT INTO forks
          (repo_name, fork_id, created_at, raw_json)
        VALUES
          (%s, %s, %s, %s)
        """
        c.execute(sql, (repo_name, fork_id, created_dt, raw_str))
        conn.commit()
        c.close()
        return True


def list_stars_single_thread(conn, owner, repo, enabled,
                             session, handle_rate_limit_func,
                             max_retries,
                             use_etags=True):
    """
    => ALWAYS a full fetch => no baseline skip or local skipping.
    => page-based approach until GitHub says no more pages.
    => robust for large repos (may need lots of tokens or wait for rate limit).
    """
    if enabled == 0:
        logging.info("[deadbird/stars] %s/%s => disabled => skip stars", owner, repo)
        return

    repo_name = f"{owner}/{repo}"
    endpoint = "stars"

    logging.info("[deadbird/stars] => ALWAYS full fetch => no baseline skip => %s", repo_name)

    old_accept = session.headers.get("Accept","")
    # needed to get 'starred_at'
    session.headers["Accept"] = "application/vnd.github.v3.star+json"

    page = 1
    total_inserted = 0

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        params = {"page": page, "per_page": 100}
        (resp, success) = robust_get_page(session, url, params,
                                          handle_rate_limit_func, max_retries,
                                          endpoint=endpoint)
        if not success or not resp:
            break

        data = resp.json()
        if not data:
            break

        new_count = 0
        for st in data:
            if insert_star_record(conn, repo_name, st):
                new_count += 1
        total_inserted += new_count

        # if we get less than 100, presumably no more
        # or we can parse Link header for next page
        if len(data) < 100:
            break
        page += 1

    session.headers["Accept"] = old_accept
    logging.info("[deadbird/stars] => done => inserted=%d => %s", total_inserted, repo_name)


def insert_star_record(conn, repo_name, star_obj):
    c = conn.cursor()
    user_login = star_obj.get("user",{}).get("login","")
    starred_str = star_obj.get("starred_at")
    starred_dt = None
    if starred_str:
        starred_dt = datetime.strptime(starred_str, "%Y-%m-%dT%H:%M:%SZ")

    c.execute("""
      SELECT id FROM stars
      WHERE repo_name=%s AND user_login=%s AND starred_at=%s
    """,(repo_name, user_login, starred_dt))
    row = c.fetchone()
    if row:
        c.close()
        return False
    else:
        import json
        raw_str = json.dumps(star_obj, ensure_ascii=False)
        sql = """
        INSERT INTO stars
          (repo_name, user_login, starred_at, raw_json)
        VALUES
          (%s, %s, %s, %s)
        """
        c.execute(sql, (repo_name, user_login, starred_dt, raw_str))
        conn.commit()
        c.close()
        return True
