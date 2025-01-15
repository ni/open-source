#!/usr/bin/env python
# caller.py

from repo_list import repo_list
from fetch_data import (
    load_tokens,
    init_metadata_db,
    create_ephemeral_db,
    create_tables,
    fetch_all_data_for_repo
)

def main():
    # 1) Load tokens
    load_tokens()

    # 2) Connect to persistent metadata DB
    metadata_conn = init_metadata_db()

    # 3) Create ephemeral DB for this run
    ephemeral_conn, ephemeral_name = create_ephemeral_db()
    create_tables(ephemeral_conn)

    # 4) Loop over repos
    for rinfo in repo_list:
        if not rinfo.get("enabled", False):
            continue

        owner = rinfo["owner"]
        repo  = rinfo["repo"]
        fallback_str = rinfo.get("start_date", "") or ""
        end_str = rinfo.get("end_date", "") or None

        print("\n=============================================")
        print(f"Processing {owner}/{repo} from {fallback_str or 'METADATA'} to {end_str or 'NOW'}")

        fetch_all_data_for_repo(
            ephemeral_conn,
            metadata_conn,
            owner,
            repo,
            fallback_str,
            end_str
        )

    ephemeral_conn.close()
    metadata_conn.close()

    print(f"\nAll done. Ephemeral DB was: {ephemeral_name}")

if __name__ == "__main__":
    main()
