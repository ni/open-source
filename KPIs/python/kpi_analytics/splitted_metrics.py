############################################
# splitted_metrics.py
############################################

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

def gather_data_for_window(repo_name, start_dt, end_dt):
    """
    Returns a dict of splitted BFS raw variables for [start_dt..end_dt),
    PLUS a 'queriesUsed' sub-dict capturing the actual SQL queries & parameters.

    keys:
      mergesRaw
      closedIssRaw
      closedPRRaw
      forksRaw
      starsRaw
      newIssRaw
      commentsIssRaw
      commentsPRRaw
      reactIssRaw
      reactPRRaw
      pullRaw

    Example returned structure:
    {
      "mergesRaw": int,  "closedIssRaw": int, ...
      "queriesUsed": {
         "mergesRaw": ("<the SQL>", [params...]),
         ...
      }
    }
    """
    results= {
      "mergesRaw": 0,
      "closedIssRaw": 0,
      "closedPRRaw": 0,
      "forksRaw": 0,
      "starsRaw": 0,
      "newIssRaw": 0,
      "commentsIssRaw": 0,
      "commentsPRRaw": 0,
      "reactIssRaw": 0,
      "reactPRRaw": 0,
      "pullRaw": 0,
      "queriesUsed": {}
    }

    cnx= get_db_connection()
    cursor= cnx.cursor()

    # mergesRaw => from pull_events, event='merged'
    q_merges= """
      SELECT COUNT(*)
      FROM pull_events
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
        AND JSON_EXTRACT(raw_json,'$.event')='merged'
    """
    cursor.execute(q_merges,(repo_name, start_dt, end_dt))
    merges_val= cursor.fetchone()[0]
    results["mergesRaw"]= merges_val
    results["queriesUsed"]["mergesRaw"]= (q_merges, [repo_name, str(start_dt), str(end_dt)])

    # closedIssRaw => from issue_events, event='closed'
    q_closed_iss= """
      SELECT COUNT(*)
      FROM issue_events ie
      WHERE ie.repo_name=%s
        AND ie.created_at >= %s AND ie.created_at < %s
        AND JSON_EXTRACT(ie.raw_json,'$.event')='closed'
        AND ie.issue_number IN (
           SELECT i.issue_number FROM issues i WHERE i.repo_name=%s
        )
    """
    cursor.execute(q_closed_iss,(repo_name, start_dt, end_dt, repo_name))
    ci_val= cursor.fetchone()[0]
    results["closedIssRaw"]= ci_val
    results["queriesUsed"]["closedIssRaw"]= (q_closed_iss, [repo_name, str(start_dt), str(end_dt), repo_name])

    # closedPRRaw => from pull_events event in ('closed','merged')
    q_closed_pr= """
      SELECT COUNT(*)
      FROM pull_events
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
        AND JSON_EXTRACT(raw_json,'$.event') in ('closed','merged')
    """
    cursor.execute(q_closed_pr,(repo_name, start_dt, end_dt))
    cpr_val= cursor.fetchone()[0]
    results["closedPRRaw"]= cpr_val
    results["queriesUsed"]["closedPRRaw"]= (q_closed_pr, [repo_name, str(start_dt), str(end_dt)])

    # forksRaw
    q_forks= """
      SELECT COUNT(*)
      FROM forks
      WHERE repo_name=%s
        AND created_at >= %s
        AND created_at < %s
    """
    cursor.execute(q_forks,(repo_name, start_dt, end_dt))
    f_val= cursor.fetchone()[0]
    results["forksRaw"]= f_val
    results["queriesUsed"]["forksRaw"]= (q_forks, [repo_name, str(start_dt), str(end_dt)])

    # starsRaw
    q_stars= """
      SELECT COUNT(*)
      FROM stars
      WHERE repo_name=%s
        AND starred_at >= %s AND starred_at < %s
    """
    cursor.execute(q_stars,(repo_name, start_dt, end_dt))
    st_val= cursor.fetchone()[0]
    results["starsRaw"]= st_val
    results["queriesUsed"]["starsRaw"]= (q_stars, [repo_name, str(start_dt), str(end_dt)])

    # newIssRaw => issues.created_at
    q_new_iss= """
      SELECT COUNT(*)
      FROM issues
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    cursor.execute(q_new_iss,(repo_name,start_dt,end_dt))
    ni_val= cursor.fetchone()[0]
    results["newIssRaw"]= ni_val
    results["queriesUsed"]["newIssRaw"]= (q_new_iss, [repo_name, str(start_dt), str(end_dt)])

    # pullRaw => from pulls.created_at
    q_pull= """
      SELECT COUNT(*)
      FROM pulls
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    cursor.execute(q_pull,(repo_name,start_dt,end_dt))
    pr_val= cursor.fetchone()[0]
    results["pullRaw"]= pr_val
    results["queriesUsed"]["pullRaw"]= (q_pull, [repo_name, str(start_dt), str(end_dt)])

    # commentsIssRaw => ignoring +1/-1 => body not like
    q_c_iss= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN issues i ON (i.repo_name=ic.repo_name AND i.issue_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body NOT LIKE '%+1%' AND ic.body NOT LIKE '%-1%')
    """
    cursor.execute(q_c_iss,(repo_name,start_dt,end_dt))
    ciss_val= cursor.fetchone()[0]
    results["commentsIssRaw"]= ciss_val
    results["queriesUsed"]["commentsIssRaw"]= (q_c_iss, [repo_name, str(start_dt), str(end_dt)])

    # commentsPRRaw => ignoring +1/-1 => body not like
    q_c_pr= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN pulls p ON (p.repo_name=ic.repo_name AND p.pull_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body NOT LIKE '%+1%' AND ic.body NOT LIKE '%-1%')
    """
    cursor.execute(q_c_pr,(repo_name,start_dt,end_dt))
    cpr_val2= cursor.fetchone()[0]
    results["commentsPRRaw"]= cpr_val2
    results["queriesUsed"]["commentsPRRaw"]= (q_c_pr, [repo_name, str(start_dt), str(end_dt)])

    # reactIssRaw => +1/-1 in issues
    q_r_iss= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN issues i ON (i.repo_name=ic.repo_name AND i.issue_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body LIKE '%+1%' OR ic.body LIKE '%-1%')
    """
    cursor.execute(q_r_iss,(repo_name,start_dt,end_dt))
    ri_val= cursor.fetchone()[0]
    results["reactIssRaw"]= ri_val
    results["queriesUsed"]["reactIssRaw"]= (q_r_iss, [repo_name, str(start_dt), str(end_dt)])

    # reactPRRaw => +1/-1 in PRs
    q_r_pr= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN pulls p ON (p.repo_name=ic.repo_name AND p.pull_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body LIKE '%+1%' OR ic.body LIKE '%-1%')
    """
    cursor.execute(q_r_pr,(repo_name,start_dt,end_dt))
    rpr_val= cursor.fetchone()[0]
    results["reactPRRaw"]= rpr_val
    results["queriesUsed"]["reactPRRaw"]= (q_r_pr, [repo_name, str(start_dt), str(end_dt)])

    cursor.close()
    cnx.close()
    return results
