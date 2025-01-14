#!/usr/bin/env python
# caller.py

from repo_list import repo_list
from fetch_data import (
    load_tokens,
    create_tables,
    fetch_fork_data,
    fetch_issue_data,
    fetch_pull_data,
    fetch_star_data
)

def main():
    # 1) Load tokens
    load_tokens()

    # 2) Create tables => ephemeral => drop first
    create_tables()

    # 3) Loop over each repo
    for repo_info in repo_list:
        if not repo_info.get("enabled", False):
            continue

        owner = repo_info["owner"]
        repo  = repo_info["repo"]
        start_str = repo_info["start_date"]
        end_str   = repo_info["end_date"] or None

        print("\n================================================")
        print(f"Processing {owner}/{repo} from {start_str} to {end_str or 'NOW'}")

        # chunk-based for forks & stars
        fetch_fork_data(owner, repo, start_str, end_str)
        fetch_star_data(owner, repo, start_str, end_str)

        # incremental (but ephemeral => effectively full) for issues & pulls
        fetch_issue_data(owner, repo, start_str, end_str)
        fetch_pull_data(owner, repo, start_str, end_str)

    print("\nAll done — ephemeral approach.")

if __name__ == "__main__":
    main()
