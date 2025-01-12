#!/usr/bin/env python
# caller.py

import requests
from repo_list import repo_list
from fetch_data import (
    get_github_token,
    create_tables,
    ensure_last_known_dates_json,
    fetch_fork_data,
    fetch_pull_data,
    fetch_issue_data,
    fetch_star_data
)

def main():
    # 1) Get GitHub token
    token = get_github_token()

    # 2) Create DB tables (safe index creation, etc.)
    create_tables()

    # 3) If last_known_dates.json doesn't exist, auto-populate from DB coverage
    ensure_last_known_dates_json()

    # 4) Prepare a single session for all requests
    session = requests.Session()
    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    })

    # How many months per chunk
    months_per_chunk = 12

    # 5) Iterate over each repo in repo_list.py
    for repo_cfg in repo_list:
        if not repo_cfg.get("enabled", False):
            print(f"Skipping disabled repo: {repo_cfg['owner']}/{repo_cfg['repo']}")
            continue

        owner = repo_cfg["owner"]
        repo = repo_cfg["repo"]
        start_date_str = repo_cfg["start_date"]
        end_date_str = repo_cfg.get("end_date", "")

        print(f"\n=== Fetching for {owner}/{repo} from {start_date_str} to {end_date_str or 'NOW'} ===")

        # 6) Call each fetch function (forks, pulls, issues, stars)
        fetch_fork_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)
        fetch_pull_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)
        fetch_issue_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)
        fetch_star_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)

    print("\nAll repositories processed. Done.")

if __name__ == "__main__":
    main()
