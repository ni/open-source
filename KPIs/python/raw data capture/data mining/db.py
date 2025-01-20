# db.py
"""
1) connect_db(cfg, create_db_if_missing=True):
   - Connects to MySQL
   - Creates the DB if missing => avoids unknown DB error

2) create_tables(conn):
   - Builds all relevant tables. 
   - We specifically fix 'body' in issue_comments to LONGTEXT 
     to handle very large GitHub comment bodies, preventing "Data too long" errors.
"""

import logging
import mysql.connector

def connect_db(cfg, create_db_if_missing=True):
    db_conf = cfg["mysql"]
    db_name = db_conf["db"]

    # Connect w/o specifying db => create if missing
    tmp_conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=None
    )
    tmp_cursor = tmp_conn.cursor()
    if create_db_if_missing:
        logging.info("Ensuring database '%s' exists...", db_name)
        tmp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        tmp_conn.commit()
    tmp_cursor.close()
    tmp_conn.close()

    # Now connect to actual DB
    conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=db_name
    )
    return conn

def create_tables(conn):
    c = conn.cursor()

    # 1) repo_baselines
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

    # 2) issues
    c.execute("""
    CREATE TABLE IF NOT EXISTS issues (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      created_at DATETIME,
      last_event_id BIGINT UNSIGNED DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # 3) pulls
    c.execute("""
    CREATE TABLE IF NOT EXISTS pulls (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT NOT NULL,
      created_at DATETIME,
      last_event_id BIGINT UNSIGNED DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # 4) issue_events
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

    # 5) pull_events
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

    # 6) issue_comments => Fix 'body' to LONGTEXT or MEDIUMTEXT
    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_comments (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name    VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      comment_id   BIGINT UNSIGNED NOT NULL,
      created_at   DATETIME,
      body LONGTEXT,  -- changed from TEXT => LONGTEXT to support large comment bodies
      UNIQUE KEY (repo_name, issue_number, comment_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # 7) comment_reactions
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

    # 8) watchers
    c.execute("""
    CREATE TABLE IF NOT EXISTS watchers (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name  VARCHAR(255) NOT NULL,
      user_login VARCHAR(255) NOT NULL,
      raw_json   JSON,
      UNIQUE KEY (repo_name, user_login)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # 9) stars
    c.execute("""
    CREATE TABLE IF NOT EXISTS stars (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name  VARCHAR(255) NOT NULL,
      user_login VARCHAR(255) NOT NULL,
      starred_at DATETIME,
      raw_json   JSON,
      UNIQUE KEY (repo_name, user_login, starred_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # 10) forks
    c.execute("""
    CREATE TABLE IF NOT EXISTS forks (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name   VARCHAR(255) NOT NULL,
      fork_id     BIGINT UNSIGNED NOT NULL,
      created_at  DATETIME,
      raw_json    JSON,
      UNIQUE KEY (repo_name, fork_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # 11) issue_reactions
    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_reactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name    VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      reaction_id  BIGINT UNSIGNED NOT NULL,
      created_at   DATETIME,
      raw_json     JSON,
      UNIQUE KEY (repo_name, issue_number, reaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    c.close()
    logging.info("All tables created/verified (including LONGTEXT for issue_comments.body).")
