# analytics/comments_reactions.py
"""
Queries for issue_comments, plus comment_reactions + issue_reactions in [start_dt, end_dt]
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    return mysql.connector.connect(
        host= DB_HOST,
        user= DB_USER,
        password= DB_PASSWORD,
        database= DB_DATABASE
    )

def count_issue_comments(repo, start_dt, end_dt):
    query= """
        SELECT COUNT(*)
        FROM issue_comments
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

def count_all_reactions(repo, start_dt, end_dt):
    query_c= """
        SELECT COUNT(*)
        FROM comment_reactions
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
    """
    query_i= """
        SELECT COUNT(*)
        FROM issue_reactions
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at < %s
    """
    cnx= get_db_connection()
    cursor= cnx.cursor()
    cursor.execute(query_c, (repo, start_dt, end_dt))
    val_c= cursor.fetchone()[0]
    cursor.execute(query_i, (repo, start_dt, end_dt))
    val_i= cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return val_c+ val_i
