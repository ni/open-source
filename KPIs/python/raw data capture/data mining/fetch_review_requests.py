# fetch_review_requests.py

import logging
import time
import requests
from datetime import datetime

from robust_fetch import robust_get_page

def list_review_requests_single_thread(conn, owner, repo, enabled,
                                       session, handle_rate_limit_func,
                                       max_retries):
    """
    For each known pull_number => fetch “review requested” details
    from your chosen endpoint. We'll show a generic approach:
    - Possibly use GET /repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers
    - Or parse special events from the pull’s timeline if you prefer
    """
    if enabled == 0:
        logging.info("[deadbird/reviewreq] %s/%s => disabled => skip", owner, repo)
        return

    repo_name = f"{owner}/{repo}"

    # 1) find all pulls from DB
    c=conn.cursor()
    c.execute("""
      SELECT pull_number
      FROM pulls
      WHERE repo_name=%s
      ORDER BY pull_number ASC
    """,(repo_name,))
    pull_rows = c.fetchall()
    c.close()

    if not pull_rows:
        logging.info("[deadbird/reviewreq] no pulls => skip => %s", repo_name)
        return

    logging.info("[deadbird/reviewreq] => listing pulls => then fetch review requests => %s", repo_name)

    # Suppose we fetch from a hypothetical approach:
    # for each pull => GET /repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers
    # We'll do a page=1 approach if needed, though typically it might not have pages.
    total_count=0

    for (pull_num,) in pull_rows:
        inserted_for_this_pr = fetch_review_requests_for_pull(conn, repo_name, pull_num,
                                                              enabled,
                                                              session,
                                                              handle_rate_limit_func,
                                                              max_retries)
        total_count += inserted_for_this_pr

    logging.info("[deadbird/reviewreq] => done => inserted total %d => %s", total_count, repo_name)


def fetch_review_requests_for_pull(conn, repo_name, pull_num,
                                   enabled,
                                   session, handle_rate_limit_func,
                                   max_retries):
    """
    Actually fetch the "review requested" data for one PR. 
    For demonstration, we'll do a single GET (no pages) 
    => GET /repos/{owner}/{repo}/pulls/{pull_num}/requested_reviewers
    Then we store them. If no data => inserted=0
    """
    page=1
    total_inserted=0

    owner_repo = repo_name.split("/",1)
    owner = owner_repo[0]
    repo = owner_repo[1]

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_num}/requested_reviewers"
        params = {"page":page,"per_page":50}
        (resp, success) = robust_get_page(session, url, params,
                                          handle_rate_limit_func, max_retries,
                                          endpoint="review_requests")
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break

        # 'data' might have "users" (requested reviewers) or "teams"
        users_list = data.get("users",[])
        teams_list = data.get("teams",[])

        # We'll store a "review request" for each user
        for user_obj in users_list:
            # each user => store an event row
            # you might treat "requested_reviewer" as user_obj
            if store_review_request_event(conn, repo_name, pull_num, {"requested_reviewer":user_obj}):
                total_inserted+=1

        # likewise for teams
        for team_obj in teams_list:
            # store if you want
            if store_review_request_event(conn, repo_name, pull_num, {"requested_team":team_obj}):
                total_inserted+=1

        if len(users_list)<50 and len(teams_list)<50:
            break
        page+=1

    logging.debug("[deadbird/reviewreq] PR #%d => inserted total %d => %s",pull_num,total_inserted,repo_name)
    return total_inserted


def store_review_request_event(conn, repo_name, pull_num, rv_obj):
    """
    Insert or update the table that records these "review requested" items.
    We'll do a single approach: generate an 'event_id' or use a random approach.
    """
    c=conn.cursor()
    import json

    # create a pseudo ID (though GitHub might not have a direct event_id for this)
    # or generate a random big int
    import random
    request_event_id = random.getrandbits(48)

    # Check if we already have something for that ID or a combination
    # We'll do a simple approach for demonstration
    c.execute("""
      SELECT id FROM pull_review_requests
      WHERE repo_name=%s AND pull_number=%s AND event_id=%s
    """,(repo_name,pull_num,request_event_id))
    row = c.fetchone()
    if row:
        c.close()
        return False
    else:
        # We'll do a function to finalize insert
        update_review_request_event(c, conn, repo_name, pull_num, request_event_id, rv_obj)
        c.close()
        return True


def update_review_request_event(c, conn, repo_name, pull_num, request_event_id, rv_obj):
    """
    Actually do the insert. The fix is here:
    Avoid 'NoneType' object has no attribute 'get' if 'requested_reviewer' is None.
    """
    # We parse the user
    user = rv_obj.get("requested_reviewer") or {}
    requested_reviewer = user.get("login","")  # if 'user' is None => we get {}

    # If there's a requested_team as well
    team_obj = rv_obj.get("requested_team") or {}
    requested_team = team_obj.get("slug","")

    raw_str = None
    try:
        import json
        raw_str = json.dumps(rv_obj, ensure_ascii=False)
    except:
        raw_str = None

    # We'll do a created_at, if we want a timestamp
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("""
      INSERT INTO pull_review_requests
        (repo_name, pull_number, event_id, requested_reviewer, requested_team, created_at, raw_json)
      VALUES
        (%s,%s,%s,%s,%s,%s,%s)
    """,(repo_name, pull_num, request_event_id, requested_reviewer, requested_team, now_str, raw_str))

    conn.commit()
