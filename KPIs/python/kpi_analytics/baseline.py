#!/usr/bin/env python3
"""
baseline.py

Finds the oldest date from 'pulls' or 'issues' for each repo.
Uses db_config.ini for MySQL credentials.
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

def find_oldest_date_for_repo(repo):
    """
    SELECT MIN(all_min) FROM (
      SELECT MIN(created_at) FROM pulls WHERE repo_name=?
      UNION ALL
      SELECT MIN(created_at) FROM issues WHERE repo_name=?
    ) subq
    """
    cnx= _get_db_connection()
    cursor= cnx.cursor()
    query= """
    SELECT MIN(all_min) AS oldest_date
    FROM (
        SELECT MIN(created_at) AS all_min
        FROM pulls
        WHERE repo_name=%s

        UNION ALL

        SELECT MIN(created_at) AS all_min
        FROM issues
        WHERE repo_name=%s
    ) subq
    """
    cursor.execute(query, (repo, repo))
    row= cursor.fetchone()
    cursor.close()
    cnx.close()
    if row and row[0]:
        return row[0]
    return None
