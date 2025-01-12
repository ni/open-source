#!/usr/bin/env python
# caller.py

import requests
from fetch_data import (
    get_github_token,
    create_tables,
    ensure_last_known_dates_json,
    fetch_fork_data,
    fetch_pull_data,
    fetch_issue_data,
    fetch_star_data
)
from repo_list import repo_list

def main():
    # 1) Obtain GitHub token
    token = get_github_token()

    # 2) Create/Update MySQL tables (forks, pulls, issues, stars), plus safe indexes
    create_tables()

    # 3) Ensure the last_known_dates.json file is created/updated based on DB coverage
    ensure_last_known_dates_json()

    # 4) Prepare a requests.Session with your GitHub token
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}"
    })

    # 5) Loop over each repo in repo_list
    #    We'll skip disabled ones, and run chunk-based fetch for each resource
    months_per_chunk = 12

    for repo_cfg in repo_list:
        if not repo_cfg.get("enabled", False):
            print(f"Skipping disabled repo: {repo_cfg['owner']}/{repo_cfg['repo']}")
            continue

        owner = repo_cfg["owner"]
        repo = repo_cfg["repo"]
        start_date_str = repo_cfg["start_date"]
        end_date_str   = repo_cfg["end_date"]

        print(f"\n=== Processing {owner}/{repo} from {start_date_str} to {end_date_str or 'NOW'} ===")

        # 6) For each resource, call the relevant fetch function
        fetch_fork_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)
        fetch_pull_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)
        fetch_issue_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)
        fetch_star_data(owner, repo, start_date_str, end_date_str, session, months_per_chunk)

    print("\nAll repositories processed. Done.")

if __name__ == "__main__":
    main()
