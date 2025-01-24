#!/usr/bin/env python3
"""
analytics/forks_stars.py

Count forks + stars from DB, given a date range, using the columns:
- forks.created_at
- stars.starred_at
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

def count_forks(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM forks
     WHERE repo_name=?
       AND created_at >= start_dt
       AND created_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM forks
    WHERE repo_name=%s
      AND created_at >= %s
      AND created_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val

def count_stars(repo, start_dt, end_dt):
    """
    SELECT COUNT(*) FROM stars
     WHERE repo_name=?
       AND starred_at >= start_dt
       AND starred_at < end_dt
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT COUNT(*)
    FROM stars
    WHERE repo_name=%s
      AND starred_at >= %s
      AND starred_at < %s
    """
    cursor.execute(query, (repo, start_dt, end_dt))
    val= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val
