#!/usr/bin/env python
# fetch_data.py

import os
import re
import requests
import mysql.connector
from datetime import datetime, timedelta
from time import sleep

###############################
# CONFIG
###############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"

EPHEMERAL_DB_PREFIX = "my_kpis_run_"
METADATA_DB = "my_kpis_metadata"

# If we see 5 consecutive "no new inserts" pages, we break
MAX_EMPTY_PAGES = 5

TOKENS = []
CURRENT_TOKEN_INDEX = 0
MAX_LIMIT_BUFFER = 50  # If near rate limit, switch token

GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

################################
# LOAD TOKENS
################################
def load_tokens():
    global TOKENS
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tokens_file = os.path.join(script_dir, "tokens.txt")

    if os.path.isfile(tokens_file):
        with open(tokens_file, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
            TOKENS = lines
    else:
        # Check environment vars: GITHUB_TOKEN1, GITHUB_TOKEN2, ...
        env_tokens = []
        i = 1
        while True:
            t = os.getenv(f"GITHUB_TOKEN{i}", "")
            if not t:
                break
            env_tokens.append(t)
            i += 1
        TOKENS = env_tokens

    if TOKENS:
        print(f"Loaded {len(TOKENS)} GitHub token(s) for GraphQL.")
    else:
        print("No GraphQL tokens found => unauthenticated => extremely limited rate limit.")

def get_session():
    """
    Returns a requests.Session for GraphQL. We'll manually attach tokens in each request
    because we might switch tokens as needed.
    """
    return requests.Session()

################################
# DB: METADATA & EPHEMERAL
################################
def connect_db(database=None):
    return mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=database)

def init_metadata_db():
    """
    Create (if not exists) the metadata DB to store last_date for each (owner, repo).
    """
    conn = connect_db()
    c = conn.cursor()
    c.execute(f"CREATE DATABASE IF NOT EXISTS {METADATA_DB}")
    conn.commit()
    c.execute(f"USE {METADATA_DB}")
    c.execute("""
        CREATE TABLE IF NOT EXISTS repo_start_dates (
            owner VARCHAR(255) NOT NULL,
            repo  VARCHAR(255) NOT NULL,
            last_date DATETIME NOT NULL,
            PRIMARY KEY (owner, repo)
        ) ENGINE=InnoDB
    """)
    conn.commit()
    c.close()
    return conn

def get_or_create_repo_start_date(mconn, owner, repo, fallback_str="2007-01-01"):
    c = mconn.cursor()
    c.execute(f"USE {METADATA_DB}")
    sel_sql = "SELECT last_date FROM repo_start_dates WHERE owner=%s AND repo=%s"
    c.execute(sel_sql, (owner, repo))
    row = c.fetchone()
    if row:
        c.close()
        return row[0]
    else:
        # Insert fallback
        fallback_dt = datetime.strptime(fallback_str, "%Y-%m-%d")
        ins_sql = "INSERT INTO repo_start_dates (owner, repo, last_date) VALUES (%s, %s, %s)"
        c.execute(ins_sql, (owner, repo, fallback_dt))
        mconn.commit()
        c.close()
        return fallback_dt

def update_repo_last_date(mconn, owner, repo, new_dt):
    c = mconn.cursor()
    c.execute(f"USE {METADATA_DB}")
    upd_sql = """
        UPDATE repo_start_dates
        SET last_date = GREATEST(last_date, %s)
        WHERE owner=%s AND repo=%s
    """
    c.execute(upd_sql, (new_dt, owner, repo))
    mconn.commit()
    c.close()

def create_ephemeral_db():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    db_name = f"{EPHEMERAL_DB_PREFIX}{timestamp}"

    temp_conn = connect_db()
    c = temp_conn.cursor()
    c.execute(f"CREATE DATABASE {db_name}")
    temp_conn.commit()
    c.close()
    temp_conn.close()
    print(f"Created ephemeral DB: {db_name}")

    conn = connect_db(database=db_name)
    return conn, db_name

def create_tables(conn):
    c = conn.cursor()

    c.execute("""
    CREATE TABLE forks (
        repo_name VARCHAR(255) NOT NULL,
        creator_login VARCHAR(255),
        forked_at DATETIME NOT NULL,
        PRIMARY KEY (repo_name, forked_at, creator_login)
    ) ENGINE=InnoDB
    """)

    c.execute("""
    CREATE TABLE stars (
        repo_name VARCHAR(255) NOT NULL,
        user_login VARCHAR(255),
        starred_at DATETIME NOT NULL,
        PRIMARY KEY (repo_name, starred_at, user_login)
    ) ENGINE=InnoDB
    """)

    c.execute("""
    CREATE TABLE issues (
        repo_name VARCHAR(255) NOT NULL,
        issue_number INT NOT NULL,
        created_at DATETIME NOT NULL,
        closed_at DATETIME,
        updated_at DATETIME NOT NULL,
        creator_login VARCHAR(255),
        title TEXT,
        PRIMARY KEY (repo_name, issue_number, created_at)
    ) ENGINE=InnoDB
    """)

    c.execute("""
    CREATE TABLE pulls (
        repo_name VARCHAR(255) NOT NULL,
        pr_number INT NOT NULL,
        created_at DATETIME NOT NULL,
        merged_at DATETIME,
        updated_at DATETIME NOT NULL,
        creator_login VARCHAR(255),
        title TEXT,
        PRIMARY KEY (repo_name, pr_number, created_at)
    ) ENGINE=InnoDB
    """)

    conn.commit()
    c.close()
    print("Created ephemeral tables in the ephemeral DB.")

################################
# Token Switching & Rate Limits
################################
def maybe_switch_token_if_needed(resp):
    """
    GraphQL does not return X-RateLimit-Remaining for every request, but sometimes it does.
    If near the limit, we switch to another token if available.
    """
    global TOKENS, CURRENT_TOKEN_INDEX
    if not TOKENS:
        return

    rem_str = resp.headers.get("X-RateLimit-Remaining")
    if rem_str:
        try:
            rem_val = int(rem_str)
            if rem_val < MAX_LIMIT_BUFFER and len(TOKENS) > 1:
                old = CURRENT_TOKEN_INDEX
                CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(TOKENS)
                print(f"Switching GraphQL token from {old} to {CURRENT_TOKEN_INDEX}, remaining={rem_val}")
        except ValueError:
            pass

def check_graphql_errors(resp_json):
    """
    If the GraphQL JSON has errors, print them. Return True if there's a blocking error.
    """
    if "errors" in resp_json:
        print(f"GraphQL Errors: {resp_json['errors']}")
        return True
    return False

def do_graphql_post(session, query_str, variables):
    """
    Makes a POST to the GitHub GraphQL endpoint. Attaches the current token.
    Returns the parsed JSON or None if error.
    """
    global TOKENS, CURRENT_TOKEN_INDEX
    headers = {"Content-Type": "application/json"}
    if TOKENS:
        token = TOKENS[CURRENT_TOKEN_INDEX]
        headers["Authorization"] = f"Bearer {token}"

    resp = session.post(
        GITHUB_GRAPHQL_ENDPOINT,
        json={"query": query_str, "variables": variables},
        headers=headers,
        timeout=30
    )
    # Possibly switch token if near limit
    maybe_switch_token_if_needed(resp)

    if resp.status_code != 200:
        print(f"GraphQL query failed, HTTP {resp.status_code}: {resp.text}")
        return None

    data = resp.json()
    if check_graphql_errors(data):
        return None
    return data

################################
# DB INSERT UTILS
################################
def insert_forks(conn, repo_name, fork_rows):
    if not fork_rows:
        return 0
    c = conn.cursor()
    sql = "INSERT IGNORE INTO forks (repo_name, creator_login, forked_at) VALUES (%s,%s,%s)"
    data = [(repo_name, f["creator_login"], f["forked_at"]) for f in fork_rows]
    c.executemany(sql, data)
    conn.commit()
    inserted = c.rowcount
    c.close()
    return inserted

def insert_stars(conn, repo_name, star_rows):
    if not star_rows:
        return 0
    c = conn.cursor()
    sql = "INSERT IGNORE INTO stars (repo_name, user_login, starred_at) VALUES (%s,%s,%s)"
    data = [(repo_name, s["user_login"], s["starred_at"]) for s in star_rows]
    c.executemany(sql, data)
    conn.commit()
    inserted = c.rowcount
    c.close()
    return inserted

def insert_issues(conn, repo_name, issue_rows):
    if not issue_rows:
        return 0
    c = conn.cursor()
    sql = """
    INSERT IGNORE INTO issues (
      repo_name, issue_number, created_at, closed_at, updated_at, creator_login, title
    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    data = [
        (
            repo_name,
            i["issue_number"],
            i["created_at"],
            i["closed_at"],
            i["updated_at"],
            i["creator_login"],
            i["title"]
        )
        for i in issue_rows
    ]
    c.executemany(sql, data)
    conn.commit()
    inserted = c.rowcount
    c.close()
    return inserted

def insert_pulls(conn, repo_name, pull_rows):
    if not pull_rows:
        return 0
    c = conn.cursor()
    sql = """
    INSERT IGNORE INTO pulls (
      repo_name, pr_number, created_at, merged_at, updated_at, creator_login, title
    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    data = [
        (
            repo_name,
            p["pr_number"],
            p["created_at"],
            p["merged_at"],
            p["updated_at"],
            p["creator_login"],
            p["title"]
        )
        for p in pull_rows
    ]
    c.executemany(sql, data)
    conn.commit()
    inserted = c.rowcount
    c.close()
    return inserted

################################
# GRAPHQL QUERIES
################################

#############################
# FORKS => createdAt ASC
#############################
FORKS_QUERY = """
query($owner:String!, $repo:String!, $pageSize:Int!, $after:String) {
  repository(owner:$owner, name:$repo) {
    forks(first:$pageSize, after:$after, orderBy:{field:CREATED_AT, direction:ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        owner {
          login
        }
        createdAt
      }
    }
  }
}
"""

def fetch_forks_graphql(conn, owner, repo, start_dt, end_dt):
    """
    1) Page through forks (createdAt ascending).
    2) If createdAt < start_dt => skip.
    3) If createdAt > end_dt => break out.
    4) Insert each chunk into DB.
    5) Return total inserted, max date.

    FIX: We use len(new_rows) == 0 to increment consecutive_empty
         so we don't prematurely skip pages if insert_forks => 0
         because they were duplicates.
    """
    repo_name = f"{owner}/{repo}"
    session = get_session()

    after_cursor = None
    page_size = 50
    total_inserted = 0
    global_max_dt = start_dt

    consecutive_empty = 0

    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "pageSize": page_size,
            "after": after_cursor
        }
        data = do_graphql_post(session, FORKS_QUERY, variables)
        if not data:
            print("GraphQL error or no data => done.")
            break

        forks_data = data["data"]["repository"]
        if not forks_data or not forks_data["forks"]:
            print("No forks data => done.")
            break

        page_info = forks_data["forks"]["pageInfo"]
        nodes = forks_data["forks"]["nodes"]
        has_next = page_info["hasNextPage"]
        end_cursor = page_info["endCursor"]

        new_rows = []
        skip_rest = False
        for f in nodes:
            created_str = f["createdAt"]
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")

            # Skip if older than start_dt
            if created_dt < start_dt:
                continue
            if created_dt > end_dt:
                skip_rest = True
                break

            new_rows.append({
                "creator_login": f["owner"]["login"],
                "forked_at": created_dt
            })
            if created_dt > global_max_dt:
                global_max_dt = created_dt

        inserted_count = insert_forks(conn, repo_name, new_rows)
        total_inserted += inserted_count

        # --- FIX: Use len(new_rows) to decide if page is "empty" ---
        if len(new_rows) == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0

        # If we found a fork > end_dt or no more pages, break
        if skip_rest or not has_next:
            break

        # If we got 5 consecutive pages with 0 new_rows, break
        if consecutive_empty >= MAX_EMPTY_PAGES:
            print(f"Forks => {consecutive_empty} consecutive empty pages => break.")
            break

        after_cursor = end_cursor

    print(f"Done fetching forks for {repo_name}, inserted={total_inserted}")
    return total_inserted, global_max_dt

#############################
# STARS => starredAt ASC
#############################
STARS_QUERY = """
query($owner:String!, $repo:String!, $pageSize:Int!, $after:String) {
  repository(owner:$owner, name:$repo) {
    stargazers(first:$pageSize, after:$after, orderBy:{field:STARRED_AT, direction:ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        starredAt
        node {
          login
        }
      }
    }
  }
}
"""

def fetch_stars_graphql(conn, owner, repo, start_dt, end_dt):
    """
    Similar logic => starredAt ascending.
    (No changes needed here for empty pages,
     but you could do the same fix if duplicates occur.)
    """
    repo_name = f"{owner}/{repo}"
    session = get_session()

    after_cursor = None
    page_size = 50
    total_inserted = 0
    global_max_dt = start_dt

    consecutive_empty = 0

    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "pageSize": page_size,
            "after": after_cursor
        }
        data = do_graphql_post(session, STARS_QUERY, variables)
        if not data:
            break

        star_data = data["data"]["repository"]
        if not star_data:
            print("No data for stargazers => done.")
            break

        stargazers = star_data["stargazers"]
        edges = stargazers["edges"]
        has_next = stargazers["pageInfo"]["hasNextPage"]
        end_cursor = stargazers["pageInfo"]["endCursor"]

        new_rows = []
        skip_rest = False

        for edge in edges:
            starred_at_str = edge["starredAt"]
            starred_dt = datetime.strptime(starred_at_str, "%Y-%m-%dT%H:%M:%SZ")
            if starred_dt < start_dt:
                continue
            if starred_dt > end_dt:
                skip_rest = True
                break

            user_login = edge["node"]["login"]
            new_rows.append({
                "user_login": user_login,
                "starred_at": starred_dt
            })
            if starred_dt > global_max_dt:
                global_max_dt = starred_dt

        inserted_count = insert_stars(conn, repo_name, new_rows)
        total_inserted += inserted_count

        # If you want to do the same fix for stars, you could:
        # if len(new_rows) == 0:
        #     consecutive_empty += 1
        # else:
        #     consecutive_empty = 0

        if skip_rest or not has_next:
            break
        if consecutive_empty >= MAX_EMPTY_PAGES:
            print(f"Consecutive empty star pages => break.")
            break

        after_cursor = end_cursor

    return total_inserted, global_max_dt

#############################
# ISSUES => updatedAt ASC
#############################
ISSUES_QUERY = """
query($owner:String!, $repo:String!, $pageSize:Int!, $after:String) {
  repository(owner:$owner, name:$repo) {
    issues(first:$pageSize, after:$after, orderBy:{field:UPDATED_AT, direction:ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        createdAt
        updatedAt
        closedAt
        author {
          login
        }
      }
    }
  }
}
"""

def fetch_issues_graphql(conn, owner, repo, start_dt, end_dt):
    """
    Similar approach for issues => updatedAt ascending.
    """
    repo_name = f"{owner}/{repo}"
    session = get_session()

    after_cursor = None
    page_size = 50
    total_inserted = 0
    global_max_dt = start_dt

    consecutive_empty = 0

    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "pageSize": page_size,
            "after": after_cursor
        }
        data = do_graphql_post(session, ISSUES_QUERY, variables)
        if not data:
            break

        repo_part = data["data"]["repository"]
        if not repo_part:
            print("No issue data => done.")
            break

        issues_part = repo_part["issues"]
        nodes = issues_part["nodes"]
        has_next = issues_part["pageInfo"]["hasNextPage"]
        end_cursor = issues_part["pageInfo"]["endCursor"]

        new_rows = []
        skip_rest = False

        for issue in nodes:
            updated_str = issue["updatedAt"]
            updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ")
            if updated_dt < start_dt:
                continue
            if updated_dt > end_dt:
                skip_rest = True
                break

            created_str = issue["createdAt"]
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
            closed_str = issue["closedAt"]
            closed_dt = datetime.strptime(closed_str, "%Y-%m-%dT%H:%M:%SZ") if closed_str else None

            user_login = issue["author"]["login"] if issue["author"] else ""
            number = issue["number"]
            title = issue["title"] or ""

            new_rows.append({
                "issue_number": number,
                "created_at": created_dt,
                "closed_at": closed_dt,
                "updated_at": updated_dt,
                "creator_login": user_login,
                "title": title
            })

            if updated_dt > global_max_dt:
                global_max_dt = updated_dt

        inserted_count = insert_issues(conn, repo_name, new_rows)
        total_inserted += inserted_count

        if len(new_rows) == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0

        if skip_rest or not has_next:
            break
        if consecutive_empty >= MAX_EMPTY_PAGES:
            print(f"Consecutive empty issue pages => break.")
            break

        after_cursor = end_cursor

    return total_inserted, global_max_dt

#############################
# PULLS => updatedAt ASC
#############################
PULLS_QUERY = """
query($owner:String!, $repo:String!, $pageSize:Int!, $after:String) {
  repository(owner:$owner, name:$repo) {
    pullRequests(first:$pageSize, after:$after, orderBy:{field:UPDATED_AT, direction:ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        createdAt
        updatedAt
        mergedAt
        author {
          login
        }
      }
    }
  }
}
"""

def fetch_pulls_graphql(conn, owner, repo, start_dt, end_dt):
    """
    For PRs => updatedAt ascending.
    """
    repo_name = f"{owner}/{repo}"
    session = get_session()

    after_cursor = None
    page_size = 50
    total_inserted = 0
    global_max_dt = start_dt

    consecutive_empty = 0

    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "pageSize": page_size,
            "after": after_cursor
        }
        data = do_graphql_post(session, PULLS_QUERY, variables)
        if not data:
            break

        repo_part = data["data"]["repository"]
        if not repo_part:
            print("No pull data => done.")
            break

        pulls_part = repo_part["pullRequests"]
        nodes = pulls_part["nodes"]
        has_next = pulls_part["pageInfo"]["hasNextPage"]
        end_cursor = pulls_part["pageInfo"]["endCursor"]

        new_rows = []
        skip_rest = False

        for pr in nodes:
            updated_str = pr["updatedAt"]
            updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ")
            if updated_dt < start_dt:
                continue
            if updated_dt > end_dt:
                skip_rest = True
                break

            created_str = pr["createdAt"]
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
            merged_str = pr["mergedAt"]
            merged_dt = datetime.strptime(merged_str, "%Y-%m-%dT%H:%M:%SZ") if merged_str else None

            user_login = pr["author"]["login"] if pr["author"] else ""
            number = pr["number"]
            title = pr["title"] or ""

            new_rows.append({
                "pr_number": number,
                "created_at": created_dt,
                "merged_at": merged_dt,
                "updated_at": updated_dt,
                "creator_login": user_login,
                "title": title
            })

            if updated_dt > global_max_dt:
                global_max_dt = updated_dt

        inserted_count = insert_pulls(conn, repo_name, new_rows)
        total_inserted += inserted_count

        if len(new_rows) == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0

        if skip_rest or not has_next:
            break
        if consecutive_empty >= MAX_EMPTY_PAGES:
            print(f"Consecutive empty pull pages => break.")
            break

        after_cursor = end_cursor

    return total_inserted, global_max_dt

################################
# MASTER FETCH
################################
def fetch_all_data_for_repo(ephemeral_conn, metadata_conn, owner, repo, fallback_str="", end_str=None):
    """
    1) Determine start_dt from metadata or fallback.
    2) End date => parse end_str or default to now.
    3) Fetch forks, stars, issues, pulls using GraphQL with ascending date sort.
    4) Insert them into ephemeral DB, track total inserts, track global max date.
    5) Update metadata with that global max date.
    6) Print summary.
    """
    if not fallback_str:
        fallback_str = "2007-01-01"
    start_dt = get_or_create_repo_start_date(metadata_conn, owner, repo, fallback_str)
    if end_str:
        try:
            end_dt = datetime.strptime(end_str, "%Y-%m-%d")
        except:
            end_dt = datetime.utcnow()
    else:
        end_dt = datetime.utcnow()

    overall_max_dt = start_dt

    # Forks
    forks_inserted, forks_max_dt = fetch_forks_graphql(ephemeral_conn, owner, repo, start_dt, end_dt)
    if forks_max_dt > overall_max_dt:
        overall_max_dt = forks_max_dt

    # Stars
    stars_inserted, stars_max_dt = fetch_stars_graphql(ephemeral_conn, owner, repo, start_dt, end_dt)
    if stars_max_dt > overall_max_dt:
        overall_max_dt = stars_max_dt

    # Issues
    issues_inserted, issues_max_dt = fetch_issues_graphql(ephemeral_conn, owner, repo, start_dt, end_dt)
    if issues_max_dt > overall_max_dt:
        overall_max_dt = issues_max_dt

    # Pulls
    pulls_inserted, pulls_max_dt = fetch_pulls_graphql(ephemeral_conn, owner, repo, start_dt, end_dt)
    if pulls_max_dt > overall_max_dt:
        overall_max_dt = pulls_max_dt

    # Update metadata
    update_repo_last_date(metadata_conn, owner, repo, overall_max_dt)

    # Print summary
    print(f"\n**** Summary for {owner}/{repo} ****")
    print(f"Forks inserted:  {forks_inserted}")
    print(f"Stars inserted:  {stars_inserted}")
    print(f"Issues inserted: {issues_inserted}")
    print(f"Pulls inserted:  {pulls_inserted}")
    print(f"Updated last_date => {overall_max_dt}")

if __name__ == "__main__":
    print("fetch_data.py (GraphQL) => Typically invoked via caller.py")
