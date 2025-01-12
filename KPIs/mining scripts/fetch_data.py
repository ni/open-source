#!/usr/bin/env python
# fetch_data.py

import os
import re
import requests
import mysql.connector
from datetime import datetime, timedelta
from time import sleep

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"    # as requested
DB_PASS = "root"    # as requested
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

def handle_rate_limit(response):
    """
    If 403, tries to sleep until GitHub resets the limit. Return True if we should retry.
    """
    if response.status_code == 403:
        reset_time = response.headers.get("X-RateLimit-Reset")
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
# Date utilities
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
# Max date queries (we no longer skip older data in these fetchers, but we keep them if needed)
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
# fetch functions (without stop_early)
############################

def fetch_fork_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    Updated: removed 'stop_early' logic => we read the entire page, relying on
    len(data)<per_page or empty data to stop. We also chunk by date if needed.
    'creator_login' => who created the fork.
    """
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()
    date_chunks = chunk_date_ranges(start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n--- fetch_fork_data for {repo_name} from {start_str} to {end_str or 'NOW'} ---")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Forks] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        # We no longer skip older data, but we log max_known_dt if needed
        max_known_dt = db_get_max_forked_at(repo_name)
        print(f"    (Note) max_known_dt in DB: {max_known_dt or 'None'} (not used to skip)")

        page = 1
        per_page = 100
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/forks"
            params = {
                "page": page,
                "per_page": per_page,
                "sort": "newest",
                "direction": "desc"
            }
            resp = session.get(url, params=params)

            if handle_rate_limit(resp):
                print(f"    Page {page}: rate-limited => retry.")
                continue

            if resp.status_code != 200:
                print(f"[Forks] HTTP {resp.status_code} => stop. repo={repo_name}, page={page}.")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Forks] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    Page {page}: no data => done chunk.")
                break

            # Link-based progress bar
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

                # who created the fork => fork_repo["owner"]["login"]
                creator_login = fork_repo.get("owner", {}).get("login", "")

                new_rows.append({
                    "repo_name": repo_name,
                    "creator_login": creator_login,
                    "forked_at": forked_dt
                })

            db_insert_forks(new_rows)

            print(f"    Page={page}: len(data)={len(data)}, inserted={len(new_rows)} new forks.")

            # We'll only break if fewer than per_page => last page
            if len(data) < per_page:
                print("    <= last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

def fetch_pull_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    'creator_login' => pr["user"]["login"], 'title' => pr["title"].
    No stop_early => we fetch all pages fully.
    """
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()
    date_chunks = chunk_date_ranges(start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n--- fetch_pull_data for {repo_name} from {start_str} to {end_str or 'NOW'} ---")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Pulls] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        max_known_dt = db_get_max_created_at_pulls(repo_name)
        print(f"    (Note) max_known_dt in DB: {max_known_dt or 'None'} (not used to skip)")

        page = 1
        per_page = 50
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
            params = {
                "state": "all",
                "page": page,
                "per_page": per_page,
                "sort": "created",
                "direction": "desc"
            }
            resp = session.get(url, params=params)

            if handle_rate_limit(resp):
                print(f"    Page {page}: rate-limited => retry.")
                continue

            if resp.status_code != 200:
                print(f"[Pulls] HTTP {resp.status_code} => stop. repo={repo_name}, page={page}.")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Pulls] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    Page {page}: no data => done chunk.")
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
            for pr in data:
                pr_number = pr["number"]
                created_str = pr["created_at"]
                merged_str  = pr["merged_at"]
                created_dt  = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                # who created the PR => pr["user"]["login"]
                creator_login = pr.get("user", {}).get("login", "")
                # PR title => pr["title"]
                pr_title = pr.get("title", "")

                # first review => optional call
                first_review_dt = None
                rev_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
                rev_resp = session.get(rev_url)
                if handle_rate_limit(rev_resp):
                    print(f"      rate-limit inside PR reviews => page={page}. skip or retry.")
                else:
                    if rev_resp.status_code == 200:
                        rev_data = rev_resp.json()
                        if rev_data:
                            sorted_revs = sorted(rev_data, key=lambda r: r["submitted_at"])
                            fr_str = sorted_revs[0].get("submitted_at")
                            if fr_str:
                                first_review_dt = datetime.strptime(fr_str, "%Y-%m-%dT%H:%M:%SZ")

                merged_dt = datetime.strptime(merged_str, "%Y-%m-%dT%H:%M:%SZ") if merged_str else None

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

            print(f"    Page={page}: len(data)={len(data)}, inserted={len(new_rows)} new pulls.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

def fetch_issue_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    'creator_login' => issue["user"]["login"].
    No stop_early => we fetch all pages fully.
    """
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()
    date_chunks = chunk_date_ranges(start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n--- fetch_issue_data for {repo_name} from {start_str} to {end_str or 'NOW'} ---")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Issues] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        max_known_dt = db_get_max_created_at_issues(repo_name)
        print(f"    (Note) max_known_dt in DB: {max_known_dt or 'None'} (not used to skip)")

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
                print(f"    Page {page}: rate-limited => retry.")
                continue

            if resp.status_code != 200:
                print(f"[Issues] HTTP {resp.status_code} => stop. repo={repo_name}, page={page}.")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Issues] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    Page {page}: no data => done chunk.")
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
            for issue in data:
                # skip pull-request crossovers
                if "pull_request" in issue:
                    continue
                issue_num = issue["number"]
                created_str = issue["created_at"]
                closed_str  = issue["closed_at"]
                ccount      = issue["comments"]
                created_dt  = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

                # who opened => issue["user"]["login"]
                creator_login = issue.get("user", {}).get("login", "")

                closed_dt = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ") if closed_str else None

                first_comment_dt = None
                if ccount > 0:
                    com_url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_num}/comments"
                    com_resp = session.get(com_url)
                    if handle_rate_limit(com_resp):
                        print(f"      rate-limit inside issue comments => page={page}. skip or retry.")
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

            print(f"    Page={page}: len(data)={len(data)}, inserted={len(new_rows)} new issues.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")

def fetch_star_data(owner, repo, start_str, end_str, session, months_per_chunk=12):
    """
    star growth over time => 'user_login' and 'starred_at'.
    No stop_early => fetch all pages fully, rely on len(data)<per_page to break.
    """
    start_dt = parse_date(start_str)
    end_dt   = parse_date(end_str) if end_str else datetime.now()
    date_chunks = chunk_date_ranges(start_dt, end_dt, months_per_chunk)
    repo_name = f"{owner}/{repo}"

    print(f"\n--- fetch_star_data for {repo_name} from {start_str} to {end_str or 'NOW'} ---")

    for (chunk_start, chunk_end) in date_chunks:
        print(f"\n[Stars] Chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} for {repo_name}")
        max_known_dt = db_get_max_starred_at(repo_name)
        print(f"    (Note) max_known_dt in DB: {max_known_dt or 'None'} (not used to skip)")

        page = 1
        per_page = 100
        last_page = None

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
            params = {"page": page, "per_page": per_page}
            session.headers['Accept'] = 'application/vnd.github.v3.star+json'

            resp = session.get(url, params=params)
            if handle_rate_limit(resp):
                print(f"    Page {page}: rate-limited => retry.")
                continue

            if resp.status_code != 200:
                print(f"[Stars] HTTP {resp.status_code} => stop. repo={repo_name}, page={page}.")
                break

            try:
                data = resp.json()
            except ValueError as e:
                print(f"[Stars] JSON parse error => break. page={page}, repo={repo_name}: {e}")
                break

            if not data:
                print(f"    Page {page}: no data => done chunk.")
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
            for star_info in data:
                starred_at_str = star_info.get("starred_at")
                if not starred_at_str:
                    continue
                starred_dt = datetime.strptime(starred_at_str, "%Y-%m-%dT%H:%M:%SZ")

                user_login = star_info.get("user", {}).get("login", "")
                new_rows.append({
                    "repo_name": repo_name,
                    "user_login": user_login,
                    "starred_at": starred_dt
                })

            db_insert_stars(new_rows)

            print(f"    Page={page}: len(data)={len(data)}, inserted={len(new_rows)} stars.")

            if len(data) < per_page:
                print("    last page => break.")
                break

            page += 1
            print(f"    next page => {page}")
