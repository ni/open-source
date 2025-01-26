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
    Gathers splitted BFS raw variables for [start_dt..end_dt).
    {
      "mergesRaw": int,
      "closedIssRaw": int,
      "closedPRRaw": int,
      "forksRaw": int,
      "starsRaw": int,
      "newIssRaw": int,
      "commentsIssRaw": int,
      "commentsPRRaw": int,
      "reactIssRaw": int,
      "reactPRRaw": int,
      "pullRaw": int
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
      "pullRaw": 0
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
    results["mergesRaw"]= cursor.fetchone()[0]

    # closedIssRaw => from issue_events, event='closed', must be an issue not a PR
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
    results["closedIssRaw"]= cursor.fetchone()[0]

    # closedPRRaw => from pull_events event in ('closed','merged')
    q_closed_pr= """
      SELECT COUNT(*)
      FROM pull_events
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
        AND JSON_EXTRACT(raw_json,'$.event') in ('closed','merged')
    """
    cursor.execute(q_closed_pr,(repo_name, start_dt, end_dt))
    results["closedPRRaw"]= cursor.fetchone()[0]

    # forksRaw => forks.created_at
    q_forks= """
      SELECT COUNT(*)
      FROM forks
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    cursor.execute(q_forks,(repo_name, start_dt, end_dt))
    results["forksRaw"]= cursor.fetchone()[0]

    # starsRaw => stars.starred_at
    q_stars= """
      SELECT COUNT(*)
      FROM stars
      WHERE repo_name=%s
        AND starred_at >= %s AND starred_at < %s
    """
    cursor.execute(q_stars,(repo_name, start_dt, end_dt))
    results["starsRaw"]= cursor.fetchone()[0]

    # newIssRaw => from issues.created_at
    q_new_iss= """
      SELECT COUNT(*)
      FROM issues
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    cursor.execute(q_new_iss,(repo_name,start_dt,end_dt))
    results["newIssRaw"]= cursor.fetchone()[0]

    # pullRaw => from pulls.created_at
    q_pull= """
      SELECT COUNT(*)
      FROM pulls
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    cursor.execute(q_pull,(repo_name,start_dt,end_dt))
    results["pullRaw"]= cursor.fetchone()[0]

    # commentsIssRaw => issue_comments + issues
    # ignoring +1/-1 => (ic.body not like '%+1%' and '%-1%')
    q_c_iss= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN issues i ON (i.repo_name=ic.repo_name AND i.issue_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body NOT LIKE '%+1%' AND ic.body NOT LIKE '%-1%')
    """
    cursor.execute(q_c_iss,(repo_name,start_dt,end_dt))
    results["commentsIssRaw"]= cursor.fetchone()[0]

    # commentsPRRaw => issue_comments + pulls
    q_c_pr= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN pulls p ON (p.repo_name=ic.repo_name AND p.pull_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body NOT LIKE '%+1%' AND ic.body NOT LIKE '%-1%')
    """
    cursor.execute(q_c_pr,(repo_name,start_dt,end_dt))
    results["commentsPRRaw"]= cursor.fetchone()[0]

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
    results["reactIssRaw"]= cursor.fetchone()[0]

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
    results["reactPRRaw"]= cursor.fetchone()[0]

    cursor.close()
    cnx.close()

    return results