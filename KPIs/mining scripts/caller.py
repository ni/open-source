#!/usr/bin/env python
# caller.py

import requests
from repo_list import repo_list
from fetch_data import (
    get_github_token,
    create_tables,
    fetch_fork_data,
    fetch_pull_data,
    fetch_issue_data,
    fetch_star_data
)

def main():
    token = get_github_token()

    # Create tables if needed
    create_tables()

    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}"
    })

    months_per_chunk = 12

    for repo_cfg in repo_list:
        if not repo_cfg["enabled"]:
            print(f"Skipping disabled repo: {repo_cfg['owner']}/{repo_cfg['repo']}")
            continue

        owner = repo_cfg["owner"]
        rname = repo_cfg["repo"]
        start_date_str = repo_cfg["start_date"]
        end_date_str   = repo_cfg["end_date"]

        print(f"\n=== Fetching {owner}/{rname} from {start_date_str} to {end_date_str or 'NOW'} ===")

        fetch_fork_data(owner, rname, start_date_str, end_date_str, session, months_per_chunk)
        fetch_pull_data(owner, rname, start_date_str, end_date_str, session, months_per_chunk)
        fetch_issue_data(owner, rname, start_date_str, end_date_str, session, months_per_chunk)
        fetch_star_data(owner, rname, start_date_str, end_date_str, session, months_per_chunk)

    print("\nAll repositories processed. Done.")

if __name__ == "__main__":
    main()
