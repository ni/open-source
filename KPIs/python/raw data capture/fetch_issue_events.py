# fetch_issue_events.py

import logging
from datetime import datetime

def parse_assigned_user(evt, etype):
    if etype in ("assigned","unassigned"):
        assignee = evt.get("assignee", {})
        return assignee.get("login", None)
    elif etype == "review_requested":
        rr = evt.get("requested_reviewer", {})
        return rr.get("login", None)
    return None

def fetch_issue_events(owner, repo, issue_number,
                       session, conn,
                       last_event_id=0,
                       overlap_pages=1,
                       handle_rate_limit_func=None):
    c = conn.cursor()
    consecutive_old_pages = 0
    page = 1
    highest_id = last_event_id

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/events"
        params = {"page": page, "per_page": 100}
        resp = session.get(url, params=params)
        if handle_rate_limit_func:
            handle_rate_limit_func(resp)
        if resp.status_code != 200:
            logging.warning("Issue events => HTTP %d => stop %s/%s#%d",
                            resp.status_code, owner, repo, issue_number)
            break

        data = resp.json()
        if not data:
            break

        new_count = 0
        old_count = 0
        for evt in data:
            eid = evt["id"]
            if eid <= highest_id:
                old_count += 1
                continue

            etype = evt.get("event","")
            actor = evt.get("actor",{}).get("login",None)
            created_str = evt.get("created_at")
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ") if created_str else None

            label_name = None
            assigned_user = None
            if etype in ("labeled","unlabeled"):
                lbl = evt.get("label",{})
                label_name = lbl.get("name","")
            elif etype in ("assigned","unassigned","review_requested"):
                assigned_user = parse_assigned_user(evt, etype)

            # store advanced events (milestoned, locked, etc.) in raw_json
            insert_sql = """
            INSERT INTO issue_events
              (repo_name, issue_number, event_id, event_type, actor_login, created_at,
               label_name, assigned_user, raw_json)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            c.execute(insert_sql, (
                f"{owner}/{repo}",
                issue_number,
                eid,
                etype,
                actor,
                created_dt,
                label_name,
                assigned_user,
                evt
            ))
            new_count += 1
            if eid > highest_id:
                highest_id = eid

        conn.commit()

        if new_count == 0:
            consecutive_old_pages += 1
            if consecutive_old_pages >= overlap_pages:
                break
        else:
            consecutive_old_pages = 0

        if len(data) < 100:
            break
        page += 1

    c.close()
    return highest_id
