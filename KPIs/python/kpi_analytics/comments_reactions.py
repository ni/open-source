#!/usr/bin/env python3
"""
analytics/comments_reactions.py

Real DB queries for issue_comments, plus possibly a 'reactions' table.
If you track them in 'issue_comments' or 'issue_reactions', adapt logic here.
Count issue comments, plus total reactions, from DB.
If 'issue_reactions' and 'comment_reactions' are separate, unify them if needed.
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

def count_issue_comments(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM issue_comments
    WHERE repo_name=? 
      AND created_at >= start_dt
      AND created_at < end_dt
     WHERE repo_name=?
       AND created_at >= start_dt
       AND created_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM issue_comments
    WHERE repo_name=%s
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_all_reactions(repo, start_dt, end_dt):
    """
    sum of:
     - comment_reactions
     - issue_reactions
    in [start_dt, end_dt]
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()

    q1= """
    SELECT COUNT(*)
    FROM comment_reactions
    WHERE repo_name=%s
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(q1, (repo, start_dt, end_dt))
    total_comment= cursor.fetchone()[0]
    cursor.close()

    cursor= cnx.cursor()
    q2= """
    SELECT COUNT(*)
    FROM issue_reactions
    WHERE repo_name=%s
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(q2, (repo, start_dt, end_dt))
    total_issue= cursor.fetchone()[0]
    cursor.close()
    cnx.close()

    return total_comment+ total_issue
