# db.py
"""
1) connect_db(cfg) => ensures the database is created if missing, 
   then returns a mysql.connector connection.

2) create_tables(conn) => creates repo_baselines, issues, pulls, issue_events, 
   pull_events, issue_comments, comment_reactions if not exist.

This fixes the error "Unknown database 'my_kpis_analytics_db'" by creating the DB.
"""

import logging
import mysql.connector

def connect_db(cfg, create_db_if_missing=True):
    """
    Connect to MySQL. If create_db_if_missing=True, we first connect 
    without specifying the database, do 'CREATE DATABASE IF NOT EXISTS', 
    then reconnect to the actual DB.

    :param cfg: dictionary loaded from config (cfg['mysql'] with host,port,user,pass,db)
    :param create_db_if_missing: bool
    """
    db_conf = cfg["mysql"]
    db_name = db_conf["db"]

    # Step 1: connect w/out specifying db => so we can create it if missing
    temp_conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=None
    )
    temp_cursor = temp_conn.cursor()
    if create_db_if_missing:
        logging.info("Ensuring database '%s' exists...", db_name)
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        temp_conn.commit()
    temp_cursor.close()
    temp_conn.close()

    # Step 2: now connect to the actual db
    conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=db_name
    )
    return conn

def create_tables(conn):
    """
    Create all necessary tables:
    - repo_baselines => per repo's baseline_date + enabled
    - issues, pulls => minimal placeholders
    - issue_events, pull_events => placeholders for event data
    - issue_comments => store comments
    - comment_reactions => store comment reactions
    """
    c = conn.cursor()

    # baseline table
    c.execute("""
    CREATE TABLE IF NOT EXISTS repo_baselines (
      id INT AUTO_INCREMENT PRIMARY KEY,
      owner VARCHAR(255) NOT NULL,
      repo  VARCHAR(255) NOT NULL,
      baseline_date DATETIME,
      enabled TINYINT DEFAULT 1,
      updated_at DATETIME,
      UNIQUE KEY (owner, repo)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS issues (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      created_at DATETIME,
      last_event_id BIGINT UNSIGNED DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pulls (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT NOT NULL,
      created_at DATETIME,
      last_event_id BIGINT UNSIGNED DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_events (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255),
      issue_number INT,
      event_id BIGINT UNSIGNED,
      created_at DATETIME,
      raw_json JSON
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pull_events (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255),
      pull_number INT,
      event_id BIGINT UNSIGNED,
      created_at DATETIME,
      raw_json JSON
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_comments (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name    VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      comment_id   BIGINT UNSIGNED NOT NULL,
      created_at   DATETIME,
      body         TEXT,
      UNIQUE KEY (repo_name, issue_number, comment_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS comment_reactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name    VARCHAR(255) NOT NULL,
      issue_number INT,
      comment_id   BIGINT UNSIGNED NOT NULL,
      reaction_id  BIGINT UNSIGNED NOT NULL,
      created_at   DATETIME,
      raw_json     JSON,
      UNIQUE KEY (repo_name, issue_number, comment_id, reaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    c.close()
    logging.info("All tables created/verified.")
