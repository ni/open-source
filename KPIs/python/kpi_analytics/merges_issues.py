# analytics/merges_issues.py
"""
Queries for merges, closed issues, new pulls, new issues, plus open issues/pr logic.
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    cnx= mysql.connector.connect(
        host= DB_HOST,
        user= DB_USER,
        password= DB_PASSWORD,
        database= DB_DATABASE
    )
    return cnx

def count_merged_pulls(repo, start_dt, end_dt):
    """
    pull_events => raw_json LIKE '%"event": "merged"%' in range
    """
    query= """
        SELECT COUNT(*)
        FROM pull_events
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
          AND raw_json LIKE '%"event": "merged"%'
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_closed_issues(repo, start_dt, end_dt):
    """
    issue_events => raw_json LIKE '%"event": "closed"%' in range
    """
    query= """
        SELECT COUNT(*)
        FROM issue_events
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
          AND raw_json LIKE '%"event": "closed"%'
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_new_pulls(repo, start_dt, end_dt):
    """
    pulls => created_at in range
    """
    query= """
        SELECT COUNT(*)
        FROM pulls
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_new_issues(repo, start_dt, end_dt):
    """
    issues => created_at in range
    """
    query= """
        SELECT COUNT(*)
        FROM issues
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_open_issues_at_date(repo, dt):
    """
    # issues created < dt
    # not closed by dt
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()

    q_created= """
        SELECT issue_number
        FROM issues
        WHERE repo_name=%s
          AND created_at < %s
    """
    cursor.execute(q_created, (repo, dt))
    issues_list= [row[0] for row in cursor.fetchall()]
    if not issues_list:
        cursor.close()
        cnx.close()
        return 0
    str_nums= ",".join(str(x) for x in issues_list)
    q_closed= f"""
        SELECT issue_number
        FROM issue_events
        WHERE repo_name=%s
          AND created_at < %s
          AND raw_json LIKE '%"event": "closed"%'
          AND issue_number in ({str_nums})
    """
    closed_set= set()
    cursor.execute(q_closed, (repo, dt))
    for row in cursor.fetchall():
        closed_set.add(row[0])

    cursor.close()
    cnx.close()

    return len(issues_list)- len(closed_set)

def count_open_prs_at_date(repo, dt):
    cnx= get_db_connection()
    cursor= cnx.cursor()

    q_created= """
        SELECT pull_number
        FROM pulls
        WHERE repo_name=%s
          AND created_at < %s
    """
    cursor.execute(q_created, (repo, dt))
    pulls_list= [row[0] for row in cursor.fetchall()]
    if not pulls_list:
        cursor.close()
        cnx.close()
        return 0
    str_nums= ",".join(str(x) for x in pulls_list)

    q_closed= f"""
        SELECT pull_number
        FROM pull_events
        WHERE repo_name=%s
          AND created_at < %s
          AND raw_json LIKE '%"event": "closed"%'
          AND pull_number in ({str_nums})
    """
    closed_or_merged= set()
    cursor.execute(q_closed, (repo, dt))
    for row in cursor.fetchall():
        closed_or_merged.add(row[0])

    q_merged= f"""
        SELECT pull_number
        FROM pull_events
        WHERE repo_name=%s
          AND created_at < %s
          AND raw_json LIKE '%"event": "merged"%'
          AND pull_number in ({str_nums})
    """
    cursor.execute(q_merged, (repo, dt))
    for row in cursor.fetchall():
        closed_or_merged.add(row[0])

    cursor.close()
    cnx.close()

    return len(pulls_list)- len(closed_or_merged)
