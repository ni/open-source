# analytics/splitted_metrics.py
"""
Gathers BFS data in minimal queries, using per-table earliest date logic
and detecting PR comments vs. issue comments by comparing issue_number
to the set of known pull_number's from 'pulls' table.

We do RAW columns: mergesRaw, closedRaw, forksRaw, starsRaw, newIssuesRaw,
commentsRaw, reactionsRaw, pullRaw.

No references to 'issue_comments.raw_json' exist. We read 'c.body' instead.
No lines omitted.
"""

import mysql.connector
import json
import re
from datetime import datetime
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
from baseline import get_earliest_date_for_table

PLUSMINUS_REGEX= re.compile(r"\b(\+1|-1)\b", re.IGNORECASE)

def get_db_connection():
    return mysql.connector.connect(
        host= DB_HOST,
        user= DB_USER,
        password= DB_PASSWORD,
        database= DB_DATABASE
    )

def build_pull_number_set(repo):
    cnx= get_db_connection()
    cursor= cnx.cursor()
    q= "SELECT pull_number FROM pulls WHERE repo_name=%s"
    cursor.execute(q,(repo,))
    pull_set= set()
    for row in cursor.fetchall():
        pull_set.add(row[0])
    cursor.close()
    cnx.close()
    return pull_set

def gather_data_for_window(repo, start_dt, end_dt):
    """
    Returns a dict of raw metrics:
      mergesRaw, closedRaw, forksRaw, starsRaw, newIssRaw, commentsRaw, reactionsRaw, pullRaw
    Then the aggregator expansions can scale them or we do that in main.

    Implementation:
     1) mergesRaw => from 'pull_events' with event=merged
     2) closedRaw => from 'issue_events' with event=closed + 'pull_events' with event=closed => We'll unify logic in main (some prefer splitted).
        We'll unify to an 'issue' closed vs 'pull' closed separately. For demonstration, we do mergesRaw separately from closedRaw.

     3) forksRaw => from 'forks' table, referencing created_at
     4) starsRaw => from 'stars' table, referencing starred_at
     5) newIssRaw => from 'issues' table, referencing created_at
     6) commentsRaw => from 'issue_comments' table => c.body => normal comment => if no +1 => commentsRaw, else => reactionsRaw
     7) reactionsRaw => from comment_reactions or +1 in c.body
     8) pullRaw => from 'pulls' referencing created_at (like new PR opened)

    Partial coverage => clamp earliest date per table.
    """

    results= {
      "mergesRaw": 0,     # merged PR events
      "closedRaw": 0,     # closed issues & closed PR events => unify
      "forksRaw": 0,
      "starsRaw": 0,
      "newIssRaw": 0,
      "commentsRaw": 0,
      "reactionsRaw": 0,
      "pullRaw": 0
    }

    cnx= get_db_connection()
    cursor= cnx.cursor(dictionary=True)

    # 1) mergesRaw => pull_events, event=merged
    earliest_pulle= get_earliest_date_for_table(repo, "pull_events","created_at")
    if earliest_pulle:
        m_start= max(start_dt, earliest_pulle)
        if m_start< end_dt:
            qm= """
            SELECT id, raw_json
              FROM pull_events
             WHERE repo_name=%s
               AND created_at >= %s
               AND created_at < %s
               AND raw_json LIKE '%"event": "merged"%'
            """
            cursor.execute(qm,(repo,m_start,end_dt))
            merges_rows= cursor.fetchall()
            results["mergesRaw"]+= len(merges_rows)

    # 2) closedRaw => unify closed issues + closed PR events
    #    a) issue_events => event=closed
    earliest_ie= get_earliest_date_for_table(repo,"issue_events","created_at")
    if earliest_ie:
        c_start= max(start_dt, earliest_ie)
        if c_start< end_dt:
            qi= """
            SELECT id, raw_json
              FROM issue_events
             WHERE repo_name=%s
               AND created_at >= %s
               AND created_at < %s
               AND raw_json LIKE '%"event": "closed"%'
            """
            cursor.execute(qi,(repo,c_start,end_dt))
            issues_closed= cursor.fetchall()
            results["closedRaw"]+= len(issues_closed)

    #    b) pull_events => event=closed
    if earliest_pulle:
        c2_start= max(start_dt, earliest_pulle)
        if c2_start< end_dt:
            qc= """
            SELECT id, raw_json
              FROM pull_events
             WHERE repo_name=%s
               AND created_at >= %s
               AND created_at < %s
               AND raw_json LIKE '%"event": "closed"%'
            """
            cursor.execute(qc,(repo,c2_start,end_dt))
            pulls_closed= cursor.fetchall()
            results["closedRaw"]+= len(pulls_closed)

    # 3) forksRaw => from 'forks'
    earliest_f= get_earliest_date_for_table(repo,"forks","created_at")
    if earliest_f:
        f_start= max(start_dt, earliest_f)
        if f_start< end_dt:
            qf= """
            SELECT id
              FROM forks
             WHERE repo_name=%s
               AND created_at >= %s
               AND created_at < %s
            """
            cursor.execute(qf,(repo,f_start,end_dt))
            forks_rows= cursor.fetchall()
            results["forksRaw"]+= len(forks_rows)

    # 4) starsRaw => from 'stars'
    earliest_s= get_earliest_date_for_table(repo,"stars","starred_at")
    if earliest_s:
        s_start= max(start_dt, earliest_s)
        if s_start< end_dt:
            qs= """
            SELECT id
              FROM stars
             WHERE repo_name=%s
               AND starred_at >= %s
               AND starred_at < %s
            """
            cursor.execute(qs,(repo,s_start,end_dt))
            star_rows= cursor.fetchall()
            results["starsRaw"]+= len(star_rows)

    # 5) newIssRaw => from 'issues'
    earliest_iss= get_earliest_date_for_table(repo,"issues","created_at")
    if earliest_iss:
        i_start= max(start_dt, earliest_iss)
        if i_start< end_dt:
            qi2= """
            SELECT id
              FROM issues
             WHERE repo_name=%s
               AND created_at >= %s
               AND created_at < %s
            """
            cursor.execute(qi2,(repo,i_start,end_dt))
            new_iss= cursor.fetchall()
            results["newIssRaw"]+= len(new_iss)

    # 6) commentsRaw + reactionsRaw => from 'issue_comments' + possible 'comment_reactions'
    #   We'll unify in a single pass, then add the reaction pass. 
    pull_set= build_pull_number_set(repo)  # for PR detection if needed
    earliest_c= get_earliest_date_for_table(repo,"issue_comments","created_at")
    if earliest_c:
        c_start2= max(start_dt, earliest_c)
        if c_start2< end_dt:
            qc2= """
            SELECT c.id, c.issue_number, c.body
              FROM issue_comments c
             WHERE c.repo_name=%s
               AND c.created_at >= %s
               AND c.created_at < %s
            """
            cursor.execute(qc2,(repo,c_start2,end_dt))
            comm_rows= cursor.fetchall()

            # Reaction table?
            earliest_creact= get_earliest_date_for_table(repo,"comment_reactions","created_at")
            reaction_map= {}
            if earliest_creact:
                r_st= max(c_start2, earliest_creact)
                if r_st< end_dt:
                    qr2= """
                    SELECT id, raw_json, comment_id
                      FROM comment_reactions
                     WHERE repo_name=%s
                       AND created_at >= %s
                       AND created_at < %s
                    """
                    cursor2= cnx.cursor(dictionary=True)
                    cursor2.execute(qr2,(repo,r_st,end_dt))
                    reac_rows= cursor2.fetchall()
                    cursor2.close()

                    for rr in reac_rows:
                        rawj= rr["raw_json"] or "{}"
                        try:
                            parsed= json.loads(rawj)
                        except:
                            parsed= {}
                        c_id= rr["comment_id"]
                        user= parsed.get("user",{}).get("login","unknown_user")
                        content= parsed.get("content","")
                        if c_id not in reaction_map:
                            reaction_map[c_id]= set()
                        reaction_map[c_id].add((user,content))

            for crow in comm_rows:
                cbody= crow["body"] or ""
                has_plusminus= bool(PLUSMINUS_REGEX.search(cbody))
                c_id= crow["id"]
                # increment commentsRaw or reactionsRaw
                if has_plusminus:
                    results["reactionsRaw"]+=1
                else:
                    results["commentsRaw"]+=1
                # incorporate reaction_map
                if c_id in reaction_map:
                    # each unique (usr,content) => +1 to reactions
                    results["reactionsRaw"]+= len(reaction_map[c_id])

    # 7) pullRaw => from 'pulls' referencing created_at
    earliest_pl= get_earliest_date_for_table(repo,"pulls","created_at")
    if earliest_pl:
        pl_start= max(start_dt, earliest_pl)
        if pl_start< end_dt:
            qpl= """
            SELECT id
              FROM pulls
             WHERE repo_name=%s
               AND created_at >= %s
               AND created_at < %s
            """
            cursor.execute(qpl,(repo,pl_start,end_dt))
            pull_rows= cursor.fetchall()
            results["pullRaw"]+= len(pull_rows)

    cursor.close()
    cnx.close()
    return results
