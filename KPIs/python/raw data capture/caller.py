#!/usr/bin/env python
# caller.py

import os
from repo_list import repo_list
from fetch_data import (
    load_tokens,
    create_tables,
    fetch_fork_data,
    fetch_pull_data,
    fetch_issue_data,
    fetch_star_data
)

def main():
    # 1) Load tokens from 'tokens.txt' or env vars
    load_tokens()

    # 2) Create DB tables
    create_tables()

    # 3) For each repo, run the daily-chunk partial skip approach
    for cfg in repo_list:
        if not cfg["enabled"]:
            print(f"Skipping disabled: {cfg['owner']}/{cfg['repo']}")
            continue

        owner = cfg["owner"]
        repo = cfg["repo"]
        start_date = cfg["start_date"]
        end_date = cfg["end_date"] or ""

        print(f"\n=== Processing {owner}/{repo} from {start_date} to {end_date or 'NOW'} ===")

        # forks
        fetch_fork_data(owner, repo, start_date, end_date)
        # pulls
        fetch_pull_data(owner, repo, start_date, end_date)
        # issues
        fetch_issue_data(owner, repo, start_date, end_date)
        # stars
        fetch_star_data(owner, repo, start_date, end_date)

    print("\nAll done.")

if __name__ == "__main__":
    main()
