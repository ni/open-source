#!/usr/bin/env python
# fetch_data.py

import os
import re
import requests
import mysql.connector
from datetime import datetime, timedelta
from time import sleep, time

############################
# DB Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# GITHUB TOKENS (two tokens, round-robin)
############################
TOKENS = []
CURRENT_TOKEN_INDEX = 0
MAX_LIMIT_BUFFER = 50  # if remaining < 50 => switch token

def load_tokens():
    """
    Loads up to two tokens from 'tokens.txt' (one per line) if present,
    or from env variables GITHUB_TOKEN1, GITHUB_TOKEN2.
    We'll do naive round-robin when near-limit on one token.
    """
    global TOKENS
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tokens_file = os.path.join(script_dir, "tokens.txt")
    if os.path.isfile(tokens_file):
        with open(tokens_file, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
            TOKENS = lines
    else:
        t1 = os.getenv("GITHUB_TOKEN1", "")
        t2 = os.getenv("GITHUB_TOKEN2", "")
        TOKENS = [tk for tk in [t1, t2] if tk]

    if TOKENS:
        print(f"Loaded {len(TOKENS)} GitHub token(s).")
    else:
        print("No GitHub tokens found! Proceeding with no token => possible rate limit issues.")

def get_session():
    """
    Return a requests.Session using the CURRENT token (if any).
    """
    global TOKENS, CURRENT_TOKEN_INDEX
    s = requests.Session()
    if TOKENS:
        current_token = TOKENS[CURRENT_TOKEN_INDEX]
        s.headers.update({"Authorization": f"token {current_token}"})
    return s

def maybe_switch_token_if_needed(resp):
    """
    Check X-RateLimit-Remaining. If < MAX_LIMIT_BUFFER => switch token.
    """
    global TOKENS, CURRENT_TOKEN_INDEX
    if not TOKENS:
        return
    rem_str = resp.headers.get("X-RateLimit-Remaining")
    if rem_str is not None:
        try:
            rem_val = int(rem_str)
            if rem_val < MAX_LIMIT_BUFFER and len(TOKENS) > 1:
                old_idx = CURRENT_TOKEN_INDEX
                CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
                print(f"Switching token from index {old_idx} to {CURRENT_TOKEN_INDEX} (remaining={rem_val}).")
        except ValueError:
            pass

def handle_rate_limit(resp):
    """
    If we get 403 => attempt to sleep until reset. Return True if we actually slept so caller can re-try.
    """
    if resp.status_code == 403:
        reset_time = resp.headers.get("X-RateLimit-Reset")
        if reset_time:
            try:
                reset_ts = int(reset_time)
                now_ts = int(datetime.now().timestamp())
                sleep_seconds = reset_ts - now_ts + 5
                if sleep_seconds > 0:
                    print(f"Rate limit reached. Sleeping {sleep_seconds} seconds...")
                    sleep(sleep_seconds)
                    return True
            except ValueError:
                pass
    return False

def handle_rate_limit_and_switch(resp):
    """
    Combine handle_rate_limit + maybe_switch_token_if_needed logic.
    If we slept => return True => caller re-tries same request.
    """
    if handle_rate_limit(resp):
        return True
    maybe_switch_token_if_needed(resp)
    return False

############################
# DAYS PER CHUNK (VARIABLE)
############################
# Default chunk size in days (you can modify below or pass from caller)
DAYS_PER_CHUNK = 1

############################
# Connect to MySQL
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

############################
# Insert Helpers
############################
def db_insert_forks(rows):
    if not rows:
        return
    try:
        conn = connect_db()
        c = conn.cursor()
        sql = """
        INSERT IGNORE INTO forks
         (repo_name, creator_login, forked_at)
        VALUES
         (%s, %s, %s)
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
        INSERT IGNORE INTO stars
         (repo_name, user_login, starred_at)
        VALUES
         (%s, %s, %s)
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
# Chunk approach with user-modifiable days_per_chunk
############################
def parse_date(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d")

def chunk_date_ranges_variable(start_dt, end_dt, days_per_chunk=1):
    """
    Creates chunk intervals of 'days_per_chunk' from start_dt up to end_dt.
    e.g. if days_per_chunk=3, each chunk is 3 days wide.
    """
    chunks = []
    cur = start_dt
    while cur < end_dt:
        nxt = cur + timedelta(days=days_per_chunk)
        if nxt > end_dt:
            nxt = end_dt
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks

############################
# PROGRESS BAR
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

def show_text_progress(repo_name, table_name, current_page, last_page, new_items, start_time):
    """
    text-based progress bar:
      - repo_name, table_name
      - number of new_items inserted so far
      - progress bar with up to 2 decimals
      - approximate time left
    """
    if not last_page or last_page <= 0:
        # unknown total pages
        print(f"[{repo_name}/{table_name}] Page {current_page}: total unknown, new items so far={new_items}")
        return
    progress = current_page / last_page
    if progress > 1.0:
        progress = 1.0
    filled_length = int(20 * progress)
    bar = '=' * filled_length + '-' * (20 - filled_length)
    percent = progress * 100

    # naive ETA calc
    elapsed = time() - start_time
    pages_done = current_page
    if pages_done < 1:
        pages_done = 1
    avg_time_per_page = elapsed / pages_done
    pages_left = last_page - current_page
    eta_secs = avg_time_per_page * pages_left
    if eta_secs < 0:
        eta_secs = 0

    if eta_secs < 60:
        eta_str = f"{eta_secs:.1f}s"
    else:
        eta_str = f"{eta_secs/60:.1f}m"

    print(f"[{repo_name}/{table_name}] [{bar}] {percent:.2f}% (Page {current_page}/{last_page}), new items={new_items}, ETA~{eta_str}")

############################
# 1) fetch_fork_data with variable chunk size
############################
def fetch_fork_data(owner, repo, start_str, end_str, days_per_chunk=1):
    table_name = "forks"
    print(f"\n=== fetch_fork_data for {owner}/{repo}, chunk={days_per_chunk} days, from {start_str} -> {end_str or 'NOW'} ===")

    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    chunks = chunk_date_ranges_variable(start_dt, end_dt, days_per_chunk)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_forked_at(repo_name)
    total_inserted = 0

    for (chunk_start, chunk_end) in chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[{repo_name}/{table_name}] skipping entire chunk {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[{repo_name}/{table_name}] chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        last_page = None
        skip_chunk = False
        start_time = time()  # for ETA calc

        while not skip_chunk:
            session = get_session()
            url = f"https://api.github.com/repos/{owner}/{repo}/forks"
            params = {
                "page": page,
                "per_page": 100,
                "sort": "oldest",  # ascending
                "direction": "asc"
            }
            resp = session.get(url, params=params)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{repo_name}/{table_name}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last

            new_rows = []
            for fork_info in data:
                dt_str = fork_info.get("created_at")
                if not dt_str:
                    continue
                forked_dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
                if db_max_dt and forked_dt <= db_max_dt:
                    print(f"    partial skip => older item {forked_dt} <= db_max_dt={db_max_dt}")
                    skip_chunk = True
                    break

                new_rows.append({
                    "repo_name": repo_name,
                    "creator_login": fork_info.get("owner", {}).get("login", ""),
                    "forked_at": forked_dt
                })
            db_insert_forks(new_rows)
            total_inserted += len(new_rows)

            # Show progress bar
            show_text_progress(repo_name, table_name, page, last_page, total_inserted, start_time)

            if skip_chunk:
                break

            if len(data) < 100:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")

############################
# 2) fetch_pull_data, similarly
############################
def fetch_pull_data(owner, repo, start_str, end_str, days_per_chunk=1):
    table_name = "pulls"
    print(f"\n=== fetch_pull_data for {owner}/{repo}, chunk={days_per_chunk} days, from {start_str} -> {end_str or 'NOW'} ===")

    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    chunks = chunk_date_ranges_variable(start_dt, end_dt, days_per_chunk)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_created_at_pulls(repo_name)
    total_inserted = 0

    for (chunk_start, chunk_end) in chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[{repo_name}/{table_name}] skipping entire chunk {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[{repo_name}/{table_name}] chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        last_page = None
        skip_chunk = False
        start_time = time()

        while not skip_chunk:
            session = get_session()
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
            params = {
                "state": "all",
                "page": page,
                "per_page": 50,
                "sort": "created",
                "direction": "asc"
            }
            resp = session.get(url, params=params)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{repo_name}/{table_name}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last

            new_rows = []
            for pr in data:
                created_str = pr.get("created_at")
                if not created_str:
                    continue
                created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
                if db_max_dt and created_dt <= db_max_dt:
                    print(f"    partial skip => older item {created_dt} <= db_max_dt={db_max_dt}")
                    skip_chunk = True
                    break

                pr_number = pr["number"]
                merged_str = pr.get("merged_at")
                merged_dt_obj = datetime.strptime(merged_str, "%Y-%m-%dT%H:%M:%SZ") if merged_str else None

                new_rows.append({
                    "repo_name": repo_name,
                    "pr_number": pr_number,
                    "created_at": created_dt,
                    "first_review_at": None,
                    "merged_at": merged_dt_obj,
                    "creator_login": pr.get("user", {}).get("login", ""),
                    "title": pr.get("title", "")
                })
            db_insert_pulls(new_rows)
            total_inserted += len(new_rows)

            show_text_progress(repo_name, table_name, page, last_page, total_inserted, start_time)

            if skip_chunk:
                break

            if len(data) < 50:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")

############################
# 3) fetch_issue_data
############################
def fetch_issue_data(owner, repo, start_str, end_str, days_per_chunk=1):
    table_name = "issues"
    print(f"\n=== fetch_issue_data for {owner}/{repo}, chunk={days_per_chunk} days, from {start_str} -> {end_str or 'NOW'} ===")

    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    chunks = chunk_date_ranges_variable(start_dt, end_dt, days_per_chunk)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_created_at_issues(repo_name)
    total_inserted = 0

    for (chunk_start, chunk_end) in chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[{repo_name}/{table_name}] skipping entire chunk {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[{repo_name}/{table_name}] chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        last_page = None
        skip_chunk = False
        start_time = time()

        while not skip_chunk:
            session = get_session()
            url = f"https://api.github.com/repos/{owner}/{repo}/issues"
            params = {
                "state": "all",
                "page": page,
                "per_page": 50,
                "sort": "created",
                "direction": "asc"
            }
            resp = session.get(url, params=params)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{repo_name}/{table_name}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last

            new_rows = []
            for issue in data:
                if "pull_request" in issue:
                    continue
                created_str = issue.get("created_at")
                if not created_str:
                    continue
                created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
                if db_max_dt and created_dt <= db_max_dt:
                    print(f"    partial skip => older item {created_dt} <= db_max_dt={db_max_dt}")
                    skip_chunk = True
                    break

                issue_num = issue["number"]
                closed_str = issue.get("closed_at")
                closed_dt_obj = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ") if closed_str else None

                ccount = issue.get("comments", 0)
                creator_login = issue.get("user", {}).get("login", "")

                new_rows.append({
                    "repo_name": repo_name,
                    "issue_number": issue_num,
                    "created_at": created_dt,
                    "closed_at": closed_dt_obj,
                    "first_comment_at": None,
                    "comments": ccount,
                    "creator_login": creator_login
                })
            db_insert_issues(new_rows)
            total_inserted += len(new_rows)

            show_text_progress(repo_name, table_name, page, last_page, total_inserted, start_time)

            if skip_chunk:
                break

            if len(data) < 50:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")

############################
# 4) fetch_star_data
############################
def fetch_star_data(owner, repo, start_str, end_str, days_per_chunk=1):
    table_name = "stars"
    print(f"\n=== fetch_star_data for {owner}/{repo}, chunk={days_per_chunk} days, from {start_str} -> {end_str or 'NOW'} ===")

    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    chunks = chunk_date_ranges_variable(start_dt, end_dt, days_per_chunk)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_starred_at(repo_name)
    total_inserted = 0

    for (chunk_start, chunk_end) in chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[{repo_name}/{table_name}] skipping entire chunk {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[{repo_name}/{table_name}] chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        last_page = None
        skip_chunk = False
        start_time = time()

        while not skip_chunk:
            session = get_session()
            url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
            params = {
                "page": page,
                "per_page": 100
            }
            session.headers['Accept'] = 'application/vnd.github.v3.star+json'
            resp = session.get(url, params=params)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[{repo_name}/{table_name}] HTTP {resp.status_code}, stop page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last

            new_rows = []
            for star_info in data:
                starred_str = star_info.get("starred_at")
                if not starred_str:
                    continue
                starred_dt = datetime.strptime(starred_str, "%Y-%m-%dT%H:%M:%SZ")

                if db_max_dt and starred_dt <= db_max_dt:
                    print(f"    partial skip => older item {starred_dt} <= db_max_dt={db_max_dt}")
                    skip_chunk = True
                    break

                user_login = star_info.get("user", {}).get("login", "")
                new_rows.append({
                    "repo_name": repo_name,
                    "user_login": user_login,
                    "starred_at": starred_dt
                })
            db_insert_stars(new_rows)
            total_inserted += len(new_rows)

            show_text_progress(repo_name, table_name, page, last_page, total_inserted, start_time)

            if skip_chunk:
                break

            if len(data) < 100:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")
