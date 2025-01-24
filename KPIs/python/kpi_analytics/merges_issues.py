#!/usr/bin/env python3
"""
analytics/merges_issues.py

Implements real DB queries for merges, closed issues, new pulls, new issues,
plus logic to count how many issues/PRs are open at a certain date,
using JSON_EXTRACT(raw_json, '$.event') for 'merged'/'closed' events.
"""

import mysql.connector
import configparser

def _get_db_connection():
    config= configparser.ConfigParser()
    config.read('db_config.ini')
    db_cfg= config['mysql']
    cnx= mysql.connector.connect(
        host=db_cfg['host'],
        user=db_cfg['user'],
        password=db_cfg['password'],
        database=db_cfg['database']
    )
    return cnx

def count_merged_pulls(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM pull_events
     WHERE repo_name=?
       AND JSON_EXTRACT(raw_json, '$.event')='merged'
       AND created_at >= start_dt AND created_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM pull_events
    WHERE repo_name=%s
      AND JSON_EXTRACT(raw_json, '$.event')='merged'
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_closed_issues(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM issue_events
     WHERE repo_name=?
       AND JSON_EXTRACT(raw_json, '$.event')='closed'
       AND created_at >= start_dt AND created_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM issue_events
    WHERE repo_name=%s
      AND JSON_EXTRACT(raw_json, '$.event')='closed'
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_new_pulls(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM pulls
     WHERE repo_name=?
       AND created_at >= start_dt
       AND created_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM pulls
    WHERE repo_name=%s
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_new_issues(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM issues
     WHERE repo_name=?
       AND created_at >= start_dt
       AND created_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM issues
    WHERE repo_name=%s
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_open_issues_at_date(repo, at_date):
    """
    # open issues => issues.created_at <= at_date
      minus distinct issue_number that had event='closed' in issue_events before at_date
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()

    q1= """
    SELECT COUNT(*)
    FROM issues
    WHERE repo_name=%s
      AND created_at <= %s
    """
    cursor.execute(q1, (repo, at_date))
    total_created= cursor.fetchone()[0]

    q2= """
    SELECT COUNT(DISTINCT issue_number)
    FROM issue_events
    WHERE repo_name=%s
      AND JSON_EXTRACT(raw_json, '$.event')='closed'
      AND created_at < %s
    """
    cursor.execute(q2, (repo, at_date))
    total_closed= cursor.fetchone()[0]

    cursor.close()
    cnx.close()
    return max(0, total_created - total_closed)

def count_open_prs_at_date(repo, at_date):
    """
    # open PR => pulls.created_at <= at_date
      minus distinct pull_number with event in('merged','closed') from pull_events < at_date
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()

    q1= """
    SELECT COUNT(*)
    FROM pulls
    WHERE repo_name=%s
      AND created_at <= %s
    """
    cursor.execute(q1, (repo, at_date))
    total_created= cursor.fetchone()[0]

    q2= """
    SELECT COUNT(DISTINCT pull_number)
    FROM pull_events
    WHERE repo_name=%s
      AND JSON_EXTRACT(raw_json, '$.event') in ('merged','closed')
      AND created_at < %s
    """
    cursor.execute(q2, (repo, at_date))
    total_merged_or_closed= cursor.fetchone()[0]

    cursor.close()
    cnx.close()
    return max(0, total_created- total_merged_or_closed)
