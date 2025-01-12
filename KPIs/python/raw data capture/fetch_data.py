#!/usr/bin/env python
# fetch_data.py

import os
import re
import requests
import mysql.connector
from datetime import datetime, timedelta
from time import sleep

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
    We'll do naive round-robin whenever near-limit on one token.
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
# Daily chunk approach
############################
def parse_date(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d")

def chunk_date_ranges_daily(start_dt, end_dt):
    """
    Creates daily chunks: each (day_start, day_end)
    """
    chunks = []
    cur = start_dt
    while cur < end_dt:
        nxt = cur + timedelta(days=1)
        if nxt > end_dt:
            nxt = end_dt
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks

############################
# 1) fetch_fork_data
############################
def fetch_fork_data(owner, repo, start_str, end_str):
    print(f"\n=== fetch_fork_data (1-day chunk partial skip) for {owner}/{repo}, from {start_str} -> {end_str or 'NOW'} ===")
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    daily_chunks = chunk_date_ranges_daily(start_dt, end_dt)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_forked_at(repo_name)

    for (chunk_start, chunk_end) in daily_chunks:
        # skip entire day if chunk_end <= db_max_dt
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[FORKS] skipping entire day {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[FORKS] day chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        while True:
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
                # if we slept => re-try same page
                continue

            if resp.status_code != 200:
                print(f"[FORKS] HTTP {resp.status_code} => stop. page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of day chunk.")
                break

            skip_day = False
            new_rows = []
            for fork_info in data:
                dt_str = fork_info.get("created_at")
                if not dt_str:
                    continue
                forked_dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")

                # partial skip => if older => skip remainder of the day
                if db_max_dt and forked_dt <= db_max_dt:
                    print(f"    [FORKS partial skip] older item={forked_dt} <= db_max_dt={db_max_dt}, skipping remainder of day {chunk_start.date()}")
                    skip_day = True
                    break

                new_rows.append({
                    "repo_name": repo_name,
                    "creator_login": fork_info.get("owner", {}).get("login", ""),
                    "forked_at": forked_dt
                })

            db_insert_forks(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} forks (out of {len(data)}).")

            if skip_day:
                break

            if len(data) < 100:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")

############################
# 2) fetch_pull_data
############################
def fetch_pull_data(owner, repo, start_str, end_str):
    print(f"\n=== fetch_pull_data (1-day chunk partial skip) for {owner}/{repo}, from {start_str} -> {end_str or 'NOW'} ===")
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    daily_chunks = chunk_date_ranges_daily(start_dt, end_dt)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_created_at_pulls(repo_name)

    for (chunk_start, chunk_end) in daily_chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[PULLS] skipping entire day {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[PULLS] day chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        while True:
            session = get_session()
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
            params = {
                "page": page,
                "per_page": 50,
                "sort": "created",
                "direction": "asc"  # ascending
            }
            resp = session.get(url, params=params)
            if handle_rate_limit_and_switch(resp):
                continue

            if resp.status_code != 200:
                print(f"[PULLS] HTTP {resp.status_code} => stop. page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of day chunk.")
                break

            skip_day = False
            new_rows = []
            for pr in data:
                created_str = pr.get("created_at")
                if not created_str:
                    continue
                created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                if db_max_dt and created_dt <= db_max_dt:
                    print(f"    [PULLS partial skip] older item={created_dt} <= db_max_dt={db_max_dt}, skip remainder of {chunk_start.date()}")
                    skip_day = True
                    break

                # Prepare row
                pr_number = pr["number"]
                merged_str = pr["merged_at"]
                merged_dt_obj = datetime.strptime(merged_str, "%Y-%m-%dT%H:%M:%SZ") if merged_str else None

                new_rows.append({
                    "repo_name": repo_name,
                    "pr_number": pr_number,
                    "created_at": created_dt,
                    "first_review_at": None,  # not implemented here
                    "merged_at": merged_dt_obj,
                    "creator_login": pr.get("user", {}).get("login", ""),
                    "title": pr.get("title", "")
                })

            db_insert_pulls(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} pulls (out of {len(data)}).")

            if skip_day:
                break

            if len(data) < 50:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")

############################
# 3) fetch_issue_data
############################
def fetch_issue_data(owner, repo, start_str, end_str):
    print(f"\n=== fetch_issue_data (1-day chunk partial skip) for {owner}/{repo}, from {start_str} -> {end_str or 'NOW'} ===")
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    daily_chunks = chunk_date_ranges_daily(start_dt, end_dt)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_created_at_issues(repo_name)

    for (chunk_start, chunk_end) in daily_chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[ISSUES] skipping entire day {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[ISSUES] day chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        while True:
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
                print(f"[ISSUES] HTTP {resp.status_code} => stop. page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of day chunk.")
                break

            skip_day = False
            new_rows = []
            for issue in data:
                # skip if it's a pull_request crossover
                if "pull_request" in issue:
                    continue
                created_str = issue.get("created_at")
                if not created_str:
                    continue
                created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                if db_max_dt and created_dt <= db_max_dt:
                    print(f"    [ISSUES partial skip] older item={created_dt} <= db_max_dt={db_max_dt}, skip remainder {chunk_start.date()}")
                    skip_day = True
                    break

                issue_num = issue["number"]
                closed_str = issue.get("closed_at")
                closed_dt_obj = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ") if closed_str else None

                # first_comment_at, comments, creator_login
                comments_count = issue.get("comments", 0)
                creator_login = issue.get("user", {}).get("login", "")

                new_rows.append({
                    "repo_name": repo_name,
                    "issue_number": issue_num,
                    "created_at": created_dt,
                    "closed_at": closed_dt_obj,
                    "first_comment_at": None,  # not implemented here
                    "comments": comments_count,
                    "creator_login": creator_login
                })

            db_insert_issues(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} issues (out of {len(data)}).")

            if skip_day:
                break

            if len(data) < 50:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")

############################
# 4) fetch_star_data
############################
def fetch_star_data(owner, repo, start_str, end_str):
    print(f"\n=== fetch_star_data (1-day chunk partial skip) for {owner}/{repo}, from {start_str} -> {end_str or 'NOW'} ===")
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()

    if not start_dt or start_dt > end_dt:
        print("No valid date range => done.")
        return

    daily_chunks = chunk_date_ranges_daily(start_dt, end_dt)
    repo_name = f"{owner}/{repo}"
    db_max_dt = db_get_max_starred_at(repo_name)

    for (chunk_start, chunk_end) in daily_chunks:
        if db_max_dt and chunk_end <= db_max_dt:
            print(f"[STARS] skipping entire day {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
            continue

        print(f"[STARS] day chunk: {chunk_start.date()} => {chunk_end.date()}, db_max_dt={db_max_dt}")
        page = 1
        while True:
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
                print(f"[STARS] HTTP {resp.status_code} => stop. page={page}")
                break

            data = resp.json()
            if not data:
                print(f"    page={page}: no data => end of day chunk.")
                break

            skip_day = False
            new_rows = []
            for star_info in data:
                starred_str = star_info.get("starred_at")
                if not starred_str:
                    continue
                starred_dt = datetime.strptime(starred_str, "%Y-%m-%dT%H:%M:%SZ")

                if db_max_dt and starred_dt <= db_max_dt:
                    print(f"    [STARS partial skip] older item={starred_dt} <= db_max_dt={db_max_dt}, skipping remainder {chunk_start.date()}")
                    skip_day = True
                    break

                user_login = star_info.get("user", {}).get("login", "")
                new_rows.append({
                    "repo_name": repo_name,
                    "user_login": user_login,
                    "starred_at": starred_dt
                })

            db_insert_stars(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} stars (out of {len(data)}).")

            if skip_day:
                break

            if len(data) < 100:
                print("    last page => break.")
                break
            page += 1
            print(f"    next page => {page}")
