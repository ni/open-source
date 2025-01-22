#!/usr/bin/env python3
"""
baseline.py

Finds the oldest date for each repo from the DB by looking at the earliest
pull/issue creation date. If no data is found, returns None.
"""

import mysql.connector
import configparser
import os

def _get_db_connection():
    """
    Reads DB credentials from db_config.ini under section [mysql].
    Adjust as needed.
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

def find_oldest_date_for_repo(repo):
    """
    Return the earliest creation date found in 'pulls' or 'issues' for this repo.
    If none found, return None.
    """
    cnx = _get_db_connection()
    cursor = cnx.cursor()
    query = """
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
    row = cursor.fetchone()
    cursor.close()
    cnx.close()
    if row and row[0]:
        return row[0]
    return None
