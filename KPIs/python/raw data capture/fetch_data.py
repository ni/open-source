#!/usr/bin/env python
# fetch_data.py

import os
import re
import json
import requests
import mysql.connector
from datetime import datetime, timedelta, timezone
from time import sleep

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# GITHUB TOKEN
############################
def get_github_token():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    token_file = os.path.join(current_dir, "github_token.txt")

    if os.path.isfile(token_file):
        with open(token_file, "r", encoding="utf-8") as f:
            file_token = f.read().strip()
            if file_token:
                print("Read GitHub token from 'github_token.txt'.")
                return file_token

    env_token = os.getenv("GITHUB_TOKEN")
    if env_token:
        print("Using GitHub token from environment variable GITHUB_TOKEN.")
        return env_token

    token = input("Enter your GitHub Personal Access Token: ").strip()
    return token

def handle_rate_limit(resp):
    """
    If 403, tries to sleep until GitHub resets the limit. Return True if we should retry.
    """
    if resp.status_code == 403:
        reset_time = resp.headers.get("X-RateLimit-Reset")
        if reset_time:
            reset_ts = int(reset_time)
            now_ts = int(datetime.now().timestamp())
            sleep_seconds = reset_ts - now_ts + 5
            if sleep_seconds > 0:
                print(f"Rate limit reached. Sleeping {sleep_seconds} seconds...")
                sleep(sleep_seconds)
                return True
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
# Safe index creation
############################
def create_index_safely(cursor, table, idx_name, columns):
    """
    Checks if 'idx_name' exists on 'table'. If not, create it (no IF NOT EXISTS).
    Avoids MySQL syntax errors if CREATE INDEX IF NOT EXISTS isn't supported.
    """
    try:
        cursor.execute(f"SHOW INDEX FROM {table}")
        all_idx = cursor.fetchall()
        index_exists = any(row[2] == idx_name for row in all_idx)
        if index_exists:
            print(f"Index '{idx_name}' on table '{table}' already exists.")
        else:
            sql = f"CREATE INDEX {idx_name} ON {table} ({columns})"
            cursor.execute(sql)
            print(f"Created index '{idx_name}' on table '{table}'.")
    except Exception as e:
        print(f"Warning: Could not create index '{idx_name}' on table '{table}': {e}")

############################
# CREATE TABLES
############################
def create_tables():
    """
    Creates forks, pulls, issues, stars with columns for 'creator_login', 'title', etc.
    Then calls create_index_safely(...) to avoid MySQL syntax errors.
    """
    conn = connect_db()
    cursor = conn.cursor()

    # forks
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forks (
        repo_name VARCHAR(255) NOT NULL,
        creator_login VARCHAR(255),
        forked_at DATETIME NOT NULL,
        PRIMARY KEY (repo_name, forked_at, creator_login)
    ) ENGINE=InnoDB
    """)
    create_index_safely(cursor, "forks", "idx_fork_repo_date", "repo_name, forked_at")

    # pulls
    cursor.execute("""
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
    create_index_safely(cursor, "pulls", "idx_pull_repo_date", "repo_name, created_at")

    # issues
    cursor.execute("""
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
    create_index_safely(cursor, "issues", "idx_issue_repo_date", "repo_name, created_at")

    # stars
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stars (
        repo_name VARCHAR(255) NOT NULL,
        user_login VARCHAR(255),
        starred_at DATETIME NOT NULL,
        PRIMARY KEY (repo_name, starred_at, user_login)
    ) ENGINE=InnoDB
    """)
    create_index_safely(cursor, "stars", "idx_stars_repo_date", "repo_name, starred_at")

    conn.commit()
    cursor.close()
    conn.close()

############################
# last_known_dates.json logic
############################
def ensure_last_known_dates_json():
    """
    If 'last_known_dates.json' doesn't exist, we create it by scanning the DB for each enabled repo
    to avoid re-fetching older data. We also won't skip data if DB coverage is older than the start_date.
    We'll do a minimal approach: store the "effective start" date for each resource.

    The 'effective start' date is:
      - if DB is empty => use repo_list's start_date
      - if DB date < repo_list's start_date => use repo_list's start_date
      - if DB date >= start_date => use that DB date (meaning we can skip older data up to db date)
    
    This ensures no data is lost, but we skip older pages if we truly have them in DB.
    We'll also print a message if we decide to skip or not.
    """
    fname = "last_known_dates.json"
    if os.path.isfile(fname):
        print(f"{fname} already exists, not recreating.")
        return

    # We'll import 'repo_list' here to see which repos are enabled
    try:
        from repo_list import repo_list
    except ImportError:
        print("repo_list.py not found or can't be imported! Creating an empty JSON.")
        basic_data = {"forks": {}, "pulls": {}, "issues": {}, "stars": {}}
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(basic_data, f, indent=2)
        return

    print(f"'{fname}' not found => creating by analyzing DB + repo_list.py ...")

    lkd = {
        "forks": {},
        "pulls": {},
        "issues": {},
        "stars": {}
    }

    # Helper to convert DB datetime -> iso8601Z
    def dt_to_iso_z(dt):
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return dt_utc.isoformat().replace("+00:00", "Z")

    # We'll define functions to get the DB's max date
    def get_db_max_for_forks(repo_name):
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT MAX(forked_at) FROM forks WHERE repo_name=%s", (repo_name,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None

    def get_db_max_for_pulls(repo_name):
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT MAX(created_at) FROM pulls WHERE repo_name=%s", (repo_name,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None

    def get_db_max_for_issues(repo_name):
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT MAX(created_at) FROM issues WHERE repo_name=%s", (repo_name,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None

    def get_db_max_for_stars(repo_name):
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT MAX(starred_at) FROM stars WHERE repo_name=%s", (repo_name,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None

    for r in repo_list:
        if not r.get("enabled", False):
            continue
        owner = r["owner"]
        repo = r["repo"]
        db_key = f"{owner}/{repo}"
        start_date_str = r["start_date"]
        code_start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")

        # 1) forks
        db_dt_forks = get_db_max_for_forks(db_key)
        if db_dt_forks is None:
            # no DB => fallback to code_start_dt
            print(f"[forks] {db_key}: DB empty => using repo_list start_date={start_date_str}")
            lkd["forks"][db_key] = dt_to_iso_z(code_start_dt)
        else:
            if db_dt_forks < code_start_dt:
                # DB is older => we might be missing data from db_dt_forks to code_start_dt => don't skip
                print(f"[forks] {db_key}: DB date={db_dt_forks} < {code_start_dt} => using start_date => no skip")
                lkd["forks"][db_key] = dt_to_iso_z(code_start_dt)
            else:
                # DB is newer => skip older data up to db_dt_forks
                print(f"[forks] {db_key}: DB date={db_dt_forks} >= start_date => skipping older than DB date")
                lkd["forks"][db_key] = dt_to_iso_z(db_dt_forks)

        # 2) pulls
        db_dt_pulls = get_db_max_for_pulls(db_key)
        if db_dt_pulls is None:
            print(f"[pulls] {db_key}: DB empty => using repo_list start_date={start_date_str}")
            lkd["pulls"][db_key] = dt_to_iso_z(code_start_dt)
        else:
            if db_dt_pulls < code_start_dt:
                print(f"[pulls] {db_key}: DB date={db_dt_pulls} < {code_start_dt} => using start_date => no skip")
                lkd["pulls"][db_key] = dt_to_iso_z(code_start_dt)
            else:
                print(f"[pulls] {db_key}: DB date={db_dt_pulls} >= start_date => skipping older than DB date")
                lkd["pulls"][db_key] = dt_to_iso_z(db_dt_pulls)

        # 3) issues
        db_dt_issues = get_db_max_for_issues(db_key)
        if db_dt_issues is None:
            print(f"[issues] {db_key}: DB empty => using repo_list start_date={start_date_str}")
            lkd["issues"][db_key] = dt_to_iso_z(code_start_dt)
        else:
            if db_dt_issues < code_start_dt:
                print(f"[issues] {db_key}: DB date={db_dt_issues} < {code_start_dt} => using start_date => no skip")
                lkd["issues"][db_key] = dt_to_iso_z(code_start_dt)
            else:
                print(f"[issues] {db_key}: DB date={db_dt_issues} >= start_date => skipping older than DB date")
                lkd["issues"][db_key] = dt_to_iso_z(db_dt_issues)

        # 4) stars
        db_dt_stars = get_db_max_for_stars(db_key)
        if db_dt_stars is None:
            print(f"[stars] {db_key}: DB empty => using repo_list start_date={start_date_str}")
            lkd["stars"][db_key] = dt_to_iso_z(code_start_dt)
        else:
            if db_dt_stars < code_start_dt:
                print(f"[stars] {db_key}: DB date={db_dt_stars} < {code_start_dt} => using start_date => no skip")
                lkd["stars"][db_key] = dt_to_iso_z(code_start_dt)
            else:
                print(f"[stars] {db_key}: DB date={db_dt_stars} >= start_date => skipping older than DB date")
                lkd["stars"][db_key] = dt_to_iso_z(db_dt_stars)

    # Write the file
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(lkd, f, indent=2)
    print(f"Created '{fname}' based on DB max dates + repo_list. Potential skipping is now recognized.\n")

############################
# Date utilities (continued)
############################
def parse_date(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d")

def add_months(dt, months):
    import calendar
    year = dt.year + (dt.month - 1 + months) // 12
    month = ((dt.month - 1 + months) % 12) + 1
    day = dt.day
    last_day = calendar.monthrange(year, month)[1]
    if day > last_day:
        day = last_day
    return dt.replace(year=year, month=month, day=day)

def chunk_date_ranges(start_dt, end_dt, months_per_chunk=12):
    if end_dt is None:
        end_dt = datetime.now()
    if start_dt and end_dt and start_dt > end_dt:
        return []
    chunks = []
    cur = start_dt
    while cur and cur < end_dt:
        nxt = add_months(cur, months_per_chunk)
        if nxt > end_dt:
            nxt = end_dt
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks

def is_in_range(dt, chunk_start, chunk_end):
    return (dt >= chunk_start) and (dt <= chunk_end)

############################
# Insert helpers
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
# Max date queries
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
# Link header + progress bar
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

def show_progress_bar(current_page, last_page, bar_length=20):
    if not last_page or last_page <= 0:
        print(f"    Page {current_page}: total unknown")
        return
    progress = current_page / last_page
    if progress > 1.0:
        progress = 1.0
    filled_length = int(bar_length * progress)
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    percent = progress * 100
    print(f"    [{bar}] {percent:.2f}% (Page {current_page} / {last_page})")

############################
# fetch functions
############################

def fetch_fork_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    Fetch forks chunk-by-chunk, reading entire pages. If DB has a bigger date, we skip older chunk windows,
    printing a message that we skip them. If DB is older, we re-fetch so we don't lose any data.
    """
    effective_start_dt = get_effective_start_dt("forks", owner, repo, start_str)
    if not effective_start_dt:
        print("No valid start date, skipping forks.")
        return

    end_dt = parse_date(end_str) if end_str else datetime.now()
    if effective_start_dt > end_dt:
        print(f"[forks] For {owner}/{repo}: effective start > end => no fetch.")
        return

    date_chunks = chunk_date_ranges(effective_start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n=== fetch_fork_data for {repo_name} from {effective_start_dt.date()} to {end_str or 'NOW'} ===")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Forks] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        page = 1
        per_page = 100
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/forks"
            params = {"page": page, "per_page": per_page, "sort": "newest", "direction": "desc"}
            resp = session.get(url, params=params)

            if handle_rate_limit(resp):
                continue

            if resp.status_code != 200:
                print(f"[Forks] HTTP {resp.status_code}, stopping. page={page}")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Forks] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    print(f"    Found last_page={possible_last}.")
                    last_page = possible_last
                else:
                    if len(data) < per_page:
                        last_page = 1
                        print("    Single-page => last_page=1.")
                    else:
                        print("    No 'rel=last' => unknown total pages.")
                        last_page = 0

            show_progress_bar(page, last_page)

            new_rows = []
            for fork_repo in data:
                forked_str = fork_repo.get("created_at")
                if not forked_str:
                    continue
                forked_dt = datetime.strptime(forked_str, "%Y-%m-%dT%H:%M:%SZ")
                creator_login = fork_repo.get("owner", {}).get("login", "")

                # We rely on DB skip at chunk-level, so we just store all found
                new_rows.append({
                    "repo_name": repo_name,
                    "creator_login": creator_login,
                    "forked_at": forked_dt
                })

            db_insert_forks(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} forks.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

def fetch_pull_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    Fetch pulls chunk by chunk. Skips older chunk windows if DB date is bigger, logs it.
    """
    effective_start_dt = get_effective_start_dt("pulls", owner, repo, start_str)
    if not effective_start_dt:
        print("No valid start date, skipping pulls.")
        return

    end_dt = parse_date(end_str) if end_str else datetime.now()
    if effective_start_dt > end_dt:
        print(f"[pulls] For {owner}/{repo}: effective start > end => no fetch.")
        return

    date_chunks = chunk_date_ranges(effective_start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n=== fetch_pull_data for {repo_name} from {effective_start_dt.date()} to {end_str or 'NOW'} ===")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Pulls] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        page = 1
        per_page = 50
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
            params = {"state": "all", "page": page, "per_page": per_page,
                      "sort": "created", "direction": "desc"}
            resp = session.get(url, params=params)

            if handle_rate_limit(resp):
                continue
            if resp.status_code != 200:
                print(f"[Pulls] HTTP {resp.status_code}, stopping. page={page}")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Pulls] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last
                    print(f"    Found last_page={last_page}.")
                else:
                    if len(data) < per_page:
                        last_page = 1
                        print("    Single-page => last_page=1.")
                    else:
                        print("    No 'rel=last' => unknown total pages.")
                        last_page = 0

            show_progress_bar(page, last_page)

            new_rows = []
            for pr in data:
                pr_number = pr["number"]
                created_str = pr["created_at"]
                merged_str  = pr["merged_at"]
                created_dt  = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                creator_login = pr.get("user", {}).get("login", "")
                pr_title = pr.get("title", "")

                # optional first review
                first_review_dt = None
                rev_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
                rev_resp = session.get(rev_url)
                if handle_rate_limit(rev_resp):
                    pass
                else:
                    if rev_resp.status_code == 200:
                        rev_data = rev_resp.json()
                        if rev_data:
                            sorted_revs = sorted(rev_data, key=lambda x: x["submitted_at"])
                            fr_str = sorted_revs[0].get("submitted_at")
                            if fr_str:
                                first_review_dt = datetime.strptime(fr_str, "%Y-%m-%dT%H:%M:%SZ")

                merged_dt = None
                if merged_str:
                    merged_dt = datetime.strptime(merged_str, "%Y-%m-%dT%H:%M:%SZ")

                new_rows.append({
                    "repo_name": repo_name,
                    "pr_number": pr_number,
                    "created_at": created_dt,
                    "first_review_at": first_review_dt,
                    "merged_at": merged_dt,
                    "creator_login": creator_login,
                    "title": pr_title
                })

            db_insert_pulls(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} pulls.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

def fetch_issue_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    Fetch issues chunk by chunk, skipping older windows if DB is bigger. No data lost.
    """
    effective_start_dt = get_effective_start_dt("issues", owner, repo, start_str)
    if not effective_start_dt:
        print("No valid start date, skipping issues.")
        return

    end_dt = parse_date(end_str) if end_str else datetime.now()
    if effective_start_dt > end_dt:
        print(f"[issues] For {owner}/{repo}: effective start > end => no fetch.")
        return

    date_chunks = chunk_date_ranges(effective_start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n=== fetch_issue_data for {repo_name} from {effective_start_dt.date()} to {end_str or 'NOW'} ===")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Issues] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        page = 1
        per_page = 50
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/issues"
            params = {
                "state": "all",
                "page": page,
                "per_page": per_page,
                "sort": "created",
                "direction": "desc"
            }
            resp = session.get(url, params=params)

            if handle_rate_limit(resp):
                continue

            if resp.status_code != 200:
                print(f"[Issues] HTTP {resp.status_code}, stopping. page={page}")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Issues] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last
                    print(f"    Found last_page={last_page}.")
                else:
                    if len(data) < per_page:
                        last_page = 1
                        print("    Single-page => last_page=1.")
                    else:
                        print("    No 'rel=last' => unknown total pages.")
                        last_page = 0

            show_progress_bar(page, last_page)

            new_rows = []
            for issue in data:
                # skip PR crossovers
                if "pull_request" in issue:
                    continue
                issue_num = issue["number"]
                created_str = issue["created_at"]
                closed_str  = issue["closed_at"]
                ccount      = issue["comments"]
                created_dt  = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                creator_login = issue.get("user", {}).get("login", "")
                closed_dt = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ") if closed_str else None

                first_comment_dt = None
                if ccount > 0:
                    com_url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_num}/comments"
                    com_resp = session.get(com_url)
                    if handle_rate_limit(com_resp):
                        pass
                    else:
                        if com_resp.status_code == 200:
                            com_data = com_resp.json()
                            if com_data:
                                sorted_coms = sorted(com_data, key=lambda c: c["created_at"])
                                fr_str = sorted_coms[0].get("created_at")
                                if fr_str:
                                    first_comment_dt = datetime.strptime(fr_str, "%Y-%m-%dT%H:%M:%SZ")

                new_rows.append({
                    "repo_name": repo_name,
                    "issue_number": issue_num,
                    "created_at": created_dt,
                    "closed_at": closed_dt,
                    "first_comment_at": first_comment_dt,
                    "comments": ccount,
                    "creator_login": creator_login
                })

            db_insert_issues(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} issues.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

def fetch_star_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    Fetch stargazers chunk by chunk, skipping older windows if DB date is bigger.
    """
    effective_start_dt = get_effective_start_dt("stars", owner, repo, start_str)
    if not effective_start_dt:
        print("No valid start date, skipping stars.")
        return

    end_dt = parse_date(end_str) if end_str else datetime.now()
    if effective_start_dt > end_dt:
        print(f"[stars] For {owner}/{repo}: effective start > end => no fetch.")
        return

    date_chunks = chunk_date_ranges(effective_start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n=== fetch_star_data for {repo_name} from {effective_start_dt.date()} to {end_str or 'NOW'} ===")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Stars] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        max_known_dt = db_get_max_starred_at(repo_name)
        print(f"    (Note) max_known_dt in DB: {max_known_dt or 'None'} (not used to partial skip in-chunk)")

        page = 1
        per_page = 100
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
            params = {"page": page, "per_page": per_page}
            # Must use star+json to get "starred_at"
            session.headers['Accept'] = 'application/vnd.github.v3.star+json'
            resp = session.get(url, params=params)

            if handle_rate_limit(resp):
                continue

            if resp.status_code != 200:
                print(f"[Stars] HTTP {resp.status_code}, stopping. page={page}")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Stars] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    page={page}: no data => end of chunk.")
                break

            if last_page is None:
                possible_last = get_last_page(resp)
                if possible_last:
                    last_page = possible_last
                    print(f"    Found last_page={last_page}.")
                else:
                    if len(data) < per_page:
                        last_page = 1
                        print("    Single-page => last_page=1.")
                    else:
                        print("    No 'rel=last' => unknown total pages.")
                        last_page = 0

            show_progress_bar(page, last_page)

            new_rows = []
            for star_info in data:
                starred_str = star_info.get("starred_at")
                if not starred_str:
                    continue
                starred_dt = datetime.strptime(starred_str, "%Y-%m-%dT%H:%M:%SZ")

                user_login = star_info.get("user", {}).get("login", "")
                new_rows.append({
                    "repo_name": repo_name,
                    "user_login": user_login,
                    "starred_at": starred_dt
                })

            db_insert_stars(new_rows)
            print(f"    page={page}: inserted={len(new_rows)} stars.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

############################
# Logic to pick effective start date to skip older data or not
############################

def get_effective_start_dt(resource, owner, repo, config_start_str):
    """
    Decide the 'effective start date' for chunking:
      - We fetch the DB max date for this resource & (owner/repo).
      - Compare to the config's start_date from repo_list.
      - If DB is None => fallback to config's start_date => no skip
      - If DB < config => pick config => no skip
      - If DB >= config => skip older data => pick DB date
    We'll print a message if we skip or if we fallback to ensure you see what's happening.

    resource in ("forks", "pulls", "issues", "stars").
    """
    config_dt = parse_date(config_start_str)
    db_key = f"{owner}/{repo}"
    
    if resource == "forks":
        db_dt = db_get_max_forked_at(db_key)
    elif resource == "pulls":
        db_dt = db_get_max_created_at_pulls(db_key)
    elif resource == "issues":
        db_dt = db_get_max_created_at_issues(db_key)
    else:
        # "stars"
        db_dt = db_get_max_starred_at(db_key)

    if not db_dt:
        print(f"[{resource}] {db_key}: No DB data => not skipping => use config start_date: {config_dt}")
        return config_dt

    if db_dt < config_dt:
        print(f"[{resource}] {db_key}: DB date={db_dt} < config start={config_dt} => no skip => use config date")
        return config_dt
    else:
        print(f"[{resource}] {db_key}: DB date={db_dt} >= config start={config_dt} => skip older => use DB date")
        return db_dt
