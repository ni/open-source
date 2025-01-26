# db.py

import logging
import mysql.connector

def connect_db(cfg, create_db_if_missing=True):
    db_conf = cfg["mysql"]
    db_name = db_conf["db"]
    conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=None if create_db_if_missing else db_name
    )
    c = conn.cursor()
    if create_db_if_missing:
        logging.info("Ensuring database '%s' exists...", db_name)
        c.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
    c.close()
    conn.close()

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

    # repos => scalable approach for many repos
    c.execute("""
    CREATE TABLE IF NOT EXISTS repos (
      id INT AUTO_INCREMENT PRIMARY KEY,
      owner VARCHAR(255) NOT NULL,
      repo  VARCHAR(255) NOT NULL,
      enabled TINYINT DEFAULT 1
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # issues => store last_event_id for incremental approach
    c.execute("""
    CREATE TABLE IF NOT EXISTS issues (
      repo_name     VARCHAR(255) NOT NULL,
      issue_number  INT NOT NULL,
      title         TEXT,
      state         VARCHAR(50),
      creator_login VARCHAR(255),
      created_at    DATETIME,
      updated_at    DATETIME,
      closed_at     DATETIME,
      last_event_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
      PRIMARY KEY (repo_name, issue_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # pulls => store last_event_id
    c.execute("""
    CREATE TABLE IF NOT EXISTS pulls (
      repo_name     VARCHAR(255) NOT NULL,
      pull_number   INT NOT NULL,
      title         TEXT,
      state         VARCHAR(50),
      creator_login VARCHAR(255),
      created_at    DATETIME,
      updated_at    DATETIME,
      closed_at     DATETIME,
      merged_at     DATETIME,
      last_event_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
      PRIMARY KEY (repo_name, pull_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # issue_events => timeline of events for issues
    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_events (
      repo_name      VARCHAR(255) NOT NULL,
      issue_number   INT NOT NULL,
      event_id       BIGINT UNSIGNED NOT NULL,
      event_type     VARCHAR(100),
      actor_login    VARCHAR(255),
      created_at     DATETIME NOT NULL,
      label_name     TEXT,
      assigned_user  VARCHAR(255),
      raw_json       JSON,
      PRIMARY KEY (repo_name, issue_number, event_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # pull_events => timeline of events for PRs
    c.execute("""
    CREATE TABLE IF NOT EXISTS pull_events (
      repo_name      VARCHAR(255) NOT NULL,
      pull_number    INT NOT NULL,
      event_id       BIGINT UNSIGNED NOT NULL,
      event_type     VARCHAR(100),
      actor_login    VARCHAR(255),
      created_at     DATETIME NOT NULL,
      label_name     TEXT,
      assigned_user  VARCHAR(255),
      raw_json       JSON,
      PRIMARY KEY (repo_name, pull_number, event_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # comment_reactions => store reaction data for issue comments, review comments, etc.
    c.execute("""
    CREATE TABLE IF NOT EXISTS comment_reactions (
      repo_name      VARCHAR(255) NOT NULL,
      resource_type  VARCHAR(50),
      comment_id     BIGINT UNSIGNED NOT NULL,
      total_reactions INT,
      raw_json       JSON,
      PRIMARY KEY (repo_name, resource_type, comment_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    c.close()
    logging.info("All tables created/verified.")
