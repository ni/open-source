#!/usr/bin/env python
# caller.py

import os
from repo_list import repo_list
from fetch_data import (
    load_tokens,
    create_tables,
    DAYS_PER_CHUNK,  # default chunk size, can modify or override
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

    # Optionally override the default DAYS_PER_CHUNK here:
    # e.g. DAYS_PER_CHUNK = 2
    # or read from command-line arguments if you like.
    # For now, let's keep it as the default in fetch_data.py

    for cfg in repo_list:
        if not cfg["enabled"]:
            print(f"Skipping disabled: {cfg['owner']}/{cfg['repo']}")
            continue

        owner = cfg["owner"]
        repo = cfg["repo"]
        start_date = cfg["start_date"]
        end_date = cfg["end_date"] or ""

        print(f"\n=== Processing {owner}/{repo} from {start_date} to {end_date or 'NOW'} ===")

        # fetch_fork_data with user-defined days_per_chunk
        fetch_fork_data(owner, repo, start_date, end_date, days_per_chunk=DAYS_PER_CHUNK)
        fetch_pull_data(owner, repo, start_date, end_date, days_per_chunk=DAYS_PER_CHUNK)
        fetch_issue_data(owner, repo, start_date, end_date, days_per_chunk=DAYS_PER_CHUNK)
        fetch_star_data(owner, repo, start_date, end_date, days_per_chunk=DAYS_PER_CHUNK)

    print("\nAll done.")

if __name__ == "__main__":
    main()
