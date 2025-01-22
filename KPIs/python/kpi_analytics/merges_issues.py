#!/usr/bin/env python3
"""
analytics/merges_issues.py

Implements real DB queries for merges, closed issues, new pulls, new issues,
plus logic to count how many issues/PRs are open at a certain date,
using JSON_EXTRACT on pull_events / issue_events,
matching the raw JSON structure you showed:

Example for closed issues:
  { "event": "closed", ... }

Example for merged pulls:
  { "event": "merged", ... }
"""

import mysql.connector
import configparser

def _get_db_connection():
    """
    Reads DB credentials from db_config.ini under section [mysql].
    """
    config = configparser.ConfigParser()
    config.read('db_config.ini')
    db_cfg = config['mysql']
    cnx = mysql.connector.connect(
        host=db_cfg['host'],
        user=db_cfg['user'],
        password=db_cfg['password'],
        database=db_cfg['database']
    )
    return cnx

def count_merged_pulls(repo, start_dt, end_dt):
    """
    Count # of 'merged' events from pull_events:
      SELECT COUNT(*) FROM pull_events
       WHERE repo_name=? 
         AND JSON_EXTRACT(raw_json, '$.event')='merged'
         AND created_at >= start_dt
         AND created_at < end_dt

    This matches your raw JSON:
      "event": "merged"
    """
    cnx = _get_db_connection()
    cursor = cnx.cursor()
    query = """
    SELECT COUNT(*)
    FROM pull_events
    WHERE repo_name=%s
      AND JSON_EXTRACT(raw_json, '$.event')='merged'
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_closed_issues(repo, start_dt, end_dt):
    """
    Count # of 'closed' events in issue_events:
      SELECT COUNT(*) FROM issue_events
       WHERE repo_name=? 
         AND JSON_EXTRACT(raw_json, '$.event')='closed'
         AND created_at >= start_dt
         AND created_at < end_dt

    Matches raw JSON:
      "event": "closed"
    """
    cnx = _get_db_connection()
    cursor = cnx.cursor()
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
    "Open" = issues created <= at_date
      minus distinct issues that had a 'closed' event < at_date

    This approach:
      1) total issues created <= at_date
      2) minus distinct issue_number in issue_events
         where JSON_EXTRACT(raw_json, '$.event')='closed' 
         AND created_at < at_date
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
    return max(0, total_created- total_closed)

def count_open_prs_at_date(repo, at_date):
    """
    "Open" = pulls created <= at_date
      minus distinct pull_number that had event='merged' or 'closed' in pull_events 
      created_at < at_date
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
    return max(0, total_created - total_merged_or_closed)
