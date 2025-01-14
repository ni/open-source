#!/usr/bin/env python
# fetch_data.py

import os
import re
import requests
import mysql.connector
from datetime import datetime, timedelta
from time import sleep

# For robust retry logic:
from requests.adapters import HTTPAdapter, Retry

############################
# DATABASE CONFIG
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# GITHUB TOKENS
############################
TOKENS = []
CURRENT_TOKEN_INDEX = 0
MAX_LIMIT_BUFFER = 50  # switch to next token if remaining < 50

def load_tokens():
    """
    Attempt to load tokens from 'tokens.txt' or env variables.
    If none found => run unauthenticated => low rate limit.
    """
    global TOKENS
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tokens_file = os.path.join(script_dir, "tokens.txt")

    if os.path.isfile(tokens_file):
        with open(tokens_file, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
            TOKENS = lines
    else:
        # fallback: environment variables
        t1 = os.getenv("GITHUB_TOKEN1", "")
        t2 = os.getenv("GITHUB_TOKEN2", "")
        TOKENS = [tk for tk in [t1, t2] if tk]

    if TOKENS:
        print(f"Loaded {len(TOKENS)} GitHub token(s).")
    else:
        print("No GitHub tokens found => unauthenticated => lower rate limits.")

def get_session():
    """
    Return a requests.Session configured with:
      1) GitHub token (if any)
      2) HTTPAdapter that retries on transient errors (ConnectionError, 5xx, etc.).
    """
    global TOKENS, CURRENT_TOKEN_INDEX

    # Create the base session
    session = requests.Session()

    # If we have multiple tokens, pick the current one
    if TOKENS:
        current_token = TOKENS[CURRENT_TOKEN_INDEX]
        session.headers.update({"Authorization": f"token {current_token}"})

    # Define a Retry strategy for robust handling of connection errors & certain HTTP codes
    retry_strategy = Retry(
        total=5,                  # total retries
        connect=5,                # how many times to retry on connection errors
        read=5,                   # how many times to retry on read errors
        backoff_factor=2,         # exponential backoff: 1st retry=2s, 2nd=4s, 3rd=8s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]
        # raise_on_status=False,   # optionally handle 4xx as well
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Mount the adapter for both http & https
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

def maybe_switch_token_if_needed(resp):
    """
    If near rate limit, switch to next token. 
    (In addition, we have a robust global retry approach for ConnectionErrors, etc.)
    """
    global TOKENS, CURRENT_TOKEN_INDEX
    if not TOKENS:
        return
    rem_str = resp.headers.get("X-RateLimit-Remaining")
    if rem_str:
        try:
            rem_val = int(rem_str)
            if rem_val < MAX_LIMIT_BUFFER and len(TOKENS) > 1:
                old_idx = CURRENT_TOKEN_INDEX
                CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
                print(f"Switching token from {old_idx} to {CURRENT_TOKEN_INDEX} (remaining={rem_val}).")
        except ValueError:
            pass

def handle_rate_limit(resp):
    """
    If we get 403 => attempt to sleep until reset. Return True if we actually sleep so we can re-try.
    """
    if resp.status_code == 403:
        reset_time = resp.headers.get("X-RateLimit-Reset")
        if reset_time:
            try:
                reset_ts = int(reset_time)
                now_ts = int(datetime.now().timestamp())
                sleep_seconds = reset_ts - now_ts + 5
                if sleep_seconds > 0:
                    print(f"Rate limit => sleeping {sleep_seconds} sec...")
                    sleep(sleep_seconds)
                    return True
            except ValueError:
                pass
    return False

def handle_rate_limit_and_switch(resp):
    """
    Combine handle_rate_limit + maybe_switch_token_if_needed.
    If we sleep => return True => calling code can re-try the same request.
    """
    if handle_rate_limit(resp):
        return True
    maybe_switch_token_if_needed(resp)
    return False

############################
# CONNECT TO MYSQL
############################
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

############################
# CREATE TABLES
############################
def create_tables():
    conn = connect_db()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS forks (
        repo_name VARCHAR(255) NOT NULL,
        creator_login VARCHAR(255),
        forked_at DATETIME NOT NULL,
        PRIMARY KEY (repo_name, forked_at, creator_login)
    ) ENGINE=InnoDB
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pulls (
        repo_name VARCHAR(255) NOT NULL,
        pr_number INT NOT NULL,
        created_at DATETIME NOT NULL,
        first_review_at DATETIME,
        merged_at DATETIME,
        creator_login VARCHAR(255),
        title TEXT,
        PRIMARY KEY (repo_name, pr_number)
    ) ENGINE=InnoDB
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS issues (
        repo_name VARCHAR(255) NOT NULL,
        issue_number INT NOT NULL,
        created_at DATETIME NOT NULL,
        closed_at DATETIME,
        first_comment_at DATETIME,
        comments INT,
        creator_login VARCHAR(255),
        PRIMARY KEY (repo_name, issue_number, created_at)
    ) ENGINE=InnoDB
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS stars (
        repo_name VARCHAR(255) NOT NULL,
        user_login VARCHAR(255),
        starred_at DATETIME NOT NULL,
        PRIMARY KEY (repo_name, starred_at, user_login)
    ) ENGINE=InnoDB
    """)

    conn.commit()
    c.close()
    conn.close()
    print("Tables created or already exist.")

############################
# INSERT HELPERS
############################
def db_insert_forks(rows):
    if not rows:
        return
    try:
        conn = connect_db()
        c = conn.cursor()
        sql = """
        INSERT IGNORE INTO forks (repo_name, creator_login, forked_at)
        VALUES (%s, %s, %s)
        """
        data = [(r["repo_name"], r["creator_login"], r["forked_at"]) for r in rows]
        c.executemany(sql, data)
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"DB insert error (forks): {e}")

def db_insert_pulls(rows):
    if not rows:
        return
    try:
        conn = connect_db()
        c = conn.cursor()
        sql = """
        INSERT IGNORE INTO pulls
         (repo_name, pr_number, created_at, first_review_at, merged_at, creator_login, title)
        VALUES
         (%s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (r["repo_name"], r["pr_number"], r["created_at"], r["first_review_at"],
             r["merged_at"], r["creator_login"], r["title"])
            for r in rows
        ]
        c.executemany(sql, data)
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"DB insert error (pulls): {e}")

def db_insert_issues(rows):
    if not rows:
        return
    try:
        conn = connect_db()
        c = conn.cursor()
        sql = """
        INSERT IGNORE INTO issues
         (repo_name, issue_number, created_at, closed_at, first_comment_at, comments, creator_login)
        VALUES
         (%s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (r["repo_name"], r["issue_number"], r["created_at"], r["closed_at"],
             r["first_comment_at"], r["comments"], r["creator_login"])
            for r in rows
        ]
        c.executemany(sql, data)
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"DB insert error (issues): {e}")

def db_insert_stars(rows):
    if not rows:
        return
    try:
        conn = connect_db()
        c = conn.cursor()
        sql = """
        INSERT IGNORE INTO stars (repo_name, user_login, starred_at)
        VALUES (%s, %s, %s)
        """
        data = [(r["repo_name"], r["user_login"], r["starred_at"]) for r in rows]
        c.executemany(sql, data)
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"DB insert error (stars): {e}")

############################
# DB coverage queries
############################
def db_get_max_forked_at(repo_name):
    conn = connect_db()
    c = conn.cursor()
    c.execute("SELECT MAX(forked_at) FROM forks WHERE repo_name=%s", (repo_name,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row and row[0] else None

def db_get_max_created_at_pulls(repo_name):
    conn = connect_db()
    c = conn.cursor()
    c.execute("SELECT MAX(created_at) FROM pulls WHERE repo_name=%s", (repo_name,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row and row[0] else None

def db_get_max_created_at_issues(repo_name):
    conn = connect_db()
    c = conn.cursor()
    c.execute("SELECT MAX(created_at) FROM issues WHERE repo_name=%s", (repo_name,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row and row[0] else None

def db_get_max_starred_at(repo_name):
    conn = connect_db()
    c = conn.cursor()
    c.execute("SELECT MAX(starred_at) FROM stars WHERE repo_name=%s", (repo_name,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row and row[0] else None

############################
# 365-day chunk approach
############################
def parse_date(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d")

def chunk_date_ranges_365(start_dt, end_dt):
    """
    Return list of inclusive date ranges (up to 365 days each), with no data lost at boundaries.
    """
    chunks = []
    cur = start_dt
    while cur <= end_dt:
        nxt = cur + timedelta(days=365)
        if nxt > end_dt:
            nxt = end_dt
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks

############################
# parse last_page from Link header
############################
def get_last_page(resp):
    link_header = resp.headers.get("Link")
    if not link_header:
        return None
    last_page = None
    links = link_header.split(',')
    for link in links:
        match = re.search(r'<([^>]+)>;\s*rel="([^"]+)"', link.strip())
        if match:
            url = match.group(1)
            rel = match.group(2)
            if rel == 'last':
                page_match = re.search(r'[?&]page=(\d+)', url)
                if page_match:
                    try:
                        last_page = int(page_match.group(1))
                    except ValueError:
                        pass
    return last_page

############################
# optional progress display
############################
def show_per_entry_progress(table_name, repo_name, item_index, total_items):
    if not total_items or total_items <= 0:
        return
    progress = float(item_index) / float(total_items)
    if progress > 1.0:
        progress = 1.0
    percent = progress * 100
    print(f"[{repo_name}/{table_name}] => {percent:.2f}% done with chunk items...")

##############################################################################
# 1) fetch_fork_data
##############################################################################
def fetch_fork_data(owner, repo, start_str, end_str=None):
    print(f"\n=== fetch_fork_data for {owner}/{repo} ===")

    start_dt = parse_date(start_str)
    end_dt = parse_date(end_str) if end_str else datetime.now()
    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    all_chunks = chunk_date_ranges_365(start_dt, end_dt)
    if not all_chunks:
        print("No chunks => done.")
        return

    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_forked_at(repo_name)
    table_name = "forks"

    total_chunks = len(all_chunks)
    for i, (chunk_start, chunk_end) in enumerate(all_chunks, start=1):
        print(f"[{table_name.upper()}] chunk {i}/{total_chunks}: range {chunk_start.date()}..{chunk_end.date()}")

        page = 1
        while True:
            session = get_session()  # now has robust retry
            url = f"https://api.github.com/repos/{owner}/{repo}/forks"
            params = {
                "sort": "oldest",
                "direction": "asc",
                "page": page,
                "per_page": 100
            }

            resp = session.get(url, params=params, timeout=30)
            # handle rate-limits
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{table_name.upper()}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print("    no data => end of chunk.")
                break

            last_page = get_last_page(resp)
            total_items = last_page * 100 if last_page else None

            skip_rest = False
            new_rows = []
            for idx, fork_info in enumerate(data):
                dt_str = fork_info.get("created_at")
                if not dt_str:
                    continue
                forked_dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")

                if forked_dt < chunk_start:
                    continue
                if forked_dt > chunk_end:
                    print(f"    fork {forked_dt} > chunk_end => break.")
                    skip_rest = True
                    break

                # if older/equal than we have => skip the chunk
                if db_max_dt and forked_dt <= db_max_dt:
                    print(f"    older fork {forked_dt} <= db_max_dt => skip chunk.")
                    skip_rest = True
                    break

                new_rows.append({
                    "repo_name": repo_name,
                    "creator_login": fork_info.get("owner", {}).get("login", ""),
                    "forked_at": forked_dt
                })

                item_index = (page - 1)*100 + (idx+1)
                show_per_entry_progress(table_name, repo_name, item_index, total_items)

            db_insert_forks(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} forks out of {len(data)}.")

            if skip_rest or len(data) < 100:
                print("    done page => break.")
                break
            page += 1

##############################################################################
# 2) fetch_pull_data
##############################################################################
def fetch_pull_data(owner, repo, start_str, end_str=None):
    print(f"\n=== fetch_pull_data for {owner}/{repo} ===")

    start_dt = parse_date(start_str)
    end_dt = parse_date(end_str) if end_str else datetime.now()
    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    all_chunks = chunk_date_ranges_365(start_dt, end_dt)
    if not all_chunks:
        print("No chunks => done.")
        return

    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_created_at_pulls(repo_name)
    table_name = "pulls"

    total_chunks = len(all_chunks)
    for i, (chunk_start, chunk_end) in enumerate(all_chunks, start=1):
        print(f"[{table_name.upper()}] chunk {i}/{total_chunks}: range {chunk_start.date()}..{chunk_end.date()}")

        page = 1
        while True:
            session = get_session()  # robust retry
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
            params = {
                "state": "all",
                "sort": "created",
                "direction": "asc",
                "page": page,
                "per_page": 50
            }
            resp = session.get(url, params=params, timeout=30)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{table_name.upper()}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print("    no data => end of chunk.")
                break

            last_page = get_last_page(resp)
            total_items = last_page * 50 if last_page else None

            skip_rest = False
            new_rows = []
            for idx, pr in enumerate(data):
                created_str = pr.get("created_at")
                if not created_str:
                    continue
                created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                if created_dt < chunk_start:
                    continue
                if created_dt > chunk_end:
                    print(f"    pull {created_dt} > chunk_end => break.")
                    skip_rest = True
                    break

                if db_max_dt and created_dt <= db_max_dt:
                    print(f"    older pull {created_dt} <= db_max_dt => skip chunk.")
                    skip_rest = True
                    break

                pr_number = pr["number"]
                merged_str = pr.get("merged_at")
                merged_dt = datetime.strptime(merged_str, "%Y-%m-%dT%H:%M:%SZ") if merged_str else None

                new_rows.append({
                    "repo_name": repo_name,
                    "pr_number": pr_number,
                    "created_at": created_dt,
                    "first_review_at": None,
                    "merged_at": merged_dt,
                    "creator_login": pr.get("user", {}).get("login", ""),
                    "title": pr.get("title", "")
                })

                item_index = (page - 1)*50 + (idx+1)
                show_per_entry_progress(table_name, repo_name, item_index, total_items)

            db_insert_pulls(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} pulls out of {len(data)}.")

            if skip_rest or len(data) < 50:
                print("    done page => break.")
                break
            page += 1

##############################################################################
# 3) fetch_issue_data
##############################################################################
def fetch_issue_data(owner, repo, start_str, end_str=None):
    print(f"\n=== fetch_issue_data for {owner}/{repo} ===")

    start_dt = parse_date(start_str)
    end_dt = parse_date(end_str) if end_str else datetime.now()
    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    all_chunks = chunk_date_ranges_365(start_dt, end_dt)
    if not all_chunks:
        print("No chunks => done.")
        return

    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_created_at_issues(repo_name)
    table_name = "issues"

    total_chunks = len(all_chunks)
    for i, (chunk_start, chunk_end) in enumerate(all_chunks, start=1):
        print(f"[{table_name.upper()}] chunk {i}/{total_chunks}: range {chunk_start.date()}..{chunk_end.date()}")

        page = 1
        while True:
            session = get_session()  # robust retry
            url = f"https://api.github.com/repos/{owner}/{repo}/issues"
            params = {
                "state": "all",
                "sort": "created",
                "direction": "asc",
                "page": page,
                "per_page": 50
            }
            resp = session.get(url, params=params, timeout=30)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{table_name.upper()}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print("    no data => end of chunk.")
                break

            last_page = get_last_page(resp)
            total_items = last_page * 50 if last_page else None

            skip_rest = False
            new_rows = []
            for idx, issue in enumerate(data):
                if "pull_request" in issue:
                    continue
                created_str = issue.get("created_at")
                if not created_str:
                    continue
                created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                if created_dt < chunk_start:
                    continue
                if created_dt > chunk_end:
                    print(f"    issue {created_dt} > chunk_end => break.")
                    skip_rest = True
                    break

                if db_max_dt and created_dt <= db_max_dt:
                    print(f"    older issue {created_dt} <= db_max_dt => skip chunk.")
                    skip_rest = True
                    break

                issue_num = issue["number"]
                closed_str = issue.get("closed_at")
                closed_dt = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ") if closed_str else None

                comments_count = issue.get("comments", 0)
                creator_login = issue.get("user", {}).get("login", "")

                new_rows.append({
                    "repo_name": repo_name,
                    "issue_number": issue_num,
                    "created_at": created_dt,
                    "closed_at": closed_dt,
                    "first_comment_at": None,
                    "comments": comments_count,
                    "creator_login": creator_login
                })

                item_index = (page - 1)*50 + (idx+1)
                show_per_entry_progress(table_name, repo_name, item_index, total_items)

            db_insert_issues(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} issues out of {len(data)}.")

            if skip_rest or len(data) < 50:
                print("    done page => break.")
                break
            page += 1

##############################################################################
# 4) fetch_star_data
##############################################################################
def fetch_star_data(owner, repo, start_str, end_str=None):
    print(f"\n=== fetch_star_data for {owner}/{repo} ===")

    start_dt = parse_date(start_str)
    end_dt = parse_date(end_str) if end_str else datetime.now()
    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    all_chunks = chunk_date_ranges_365(start_dt, end_dt)
    if not all_chunks:
        print("No chunks => done.")
        return

    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_starred_at(repo_name)
    table_name = "stars"

    total_chunks = len(all_chunks)
    for i, (chunk_start, chunk_end) in enumerate(all_chunks, start=1):
        print(f"[{table_name.upper()}] chunk {i}/{total_chunks}: range {chunk_start.date()}..{chunk_end.date()}")

        page = 1
        while True:
            session = get_session()  # robust retry
            session.headers["Accept"] = "application/vnd.github.v3.star+json"
            url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
            params = {
                "page": page,
                "per_page": 100
            }
            resp = session.get(url, params=params, timeout=30)

            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{table_name.upper()}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print("    no data => end of chunk.")
                break

            last_page = get_last_page(resp)
            total_items = last_page * 100 if last_page else None

            skip_rest = False
            new_rows = []
            for idx, star_info in enumerate(data):
                starred_str = star_info.get("starred_at")
                if not starred_str:
                    continue
                starred_dt = datetime.strptime(starred_str, "%Y-%m-%dT%H:%M:%SZ")

                if starred_dt < chunk_start:
                    continue
                if starred_dt > chunk_end:
                    print(f"    star {starred_dt} > chunk_end => break.")
                    skip_rest = True
                    break

                if db_max_dt and starred_dt <= db_max_dt:
                    print(f"    older star {starred_dt} <= db_max_dt => skip chunk.")
                    skip_rest = True
                    break

                user_login = star_info.get("user", {}).get("login", "")
                new_rows.append({
                    "repo_name": repo_name,
                    "user_login": user_login,
                    "starred_at": starred_dt
                })

                item_index = (page - 1)*100 + (idx+1)
                show_per_entry_progress(table_name, repo_name, item_index, total_items)

            db_insert_stars(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} stars out of {len(data)}.")

            if skip_rest or len(data) < 100:
                print("    done page => break.")
                break
            page += 1

############################
# If run directly
############################
if __name__ == "__main__":
    load_tokens()
    create_tables()
    print("fetch_data.py => to fetch data, call fetch_*_data(...) from code or from caller.py.")
