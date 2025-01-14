#!/usr/bin/env python
# caller.py

from repo_list import repo_list
from fetch_data import (
    load_tokens,
    create_and_select_db,
    create_tables,
    fetch_fork_data,
    fetch_issue_data,
    fetch_pull_data,
    fetch_star_data,
    CURRENT_DB_NAME
)

def main():
    # 1) Load tokens
    load_tokens()

    # 2) Create brand-new DB each run => ephemeral approach
    create_and_select_db()

    # 3) Create tables in that new DB
    create_tables()

    print(f"\nUsing newly created DB: {CURRENT_DB_NAME}\n")

    # 4) Process each repo from repo_list
    for repo_info in repo_list:
        if not repo_info.get("enabled", False):
            continue

        owner = repo_info["owner"]
        repo  = repo_info["repo"]
        start_str = repo_info["start_date"]
        end_str   = repo_info["end_date"] or None

        print("\n========================================")
        print(f"Processing {owner}/{repo} from {start_str} to {end_str or 'NOW'}")

        # chunk-based => forks & stars
        fetch_fork_data(owner, repo, start_str, end_str)
        fetch_star_data(owner, repo, start_str, end_str)

        # incremental => issues & pulls
        fetch_issue_data(owner, repo, start_str, end_str)
        fetch_pull_data(owner, repo, start_str, end_str)

    print("\nAll done! Data stored in DB:", CURRENT_DB_NAME)

if __name__ == "__main__":
    main()
