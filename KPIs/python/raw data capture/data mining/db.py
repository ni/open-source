# db.py
import logging
import mysql.connector

def connect_db(cfg, create_db_if_missing=True):
    db_conf = cfg["mysql"]
    db_name = db_conf["db"]

    # Connect to server-level to ensure DB is created if missing
    tmp_conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf["port"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=None
    )
    tmp_cursor = tmp_conn.cursor()
    if create_db_if_missing:
        logging.info("[ossmining] Ensuring database '%s' exists...", db_name)
        tmp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        tmp_conn.commit()
    tmp_cursor.close()
    tmp_conn.close()

    # Now connect directly to that DB
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
    Creates tables from the original Revision A (watchers/forks/stars,
    issues/pulls, events, comment-level data) AND the advanced endpoints
    (Releases, Release Assets, Labels, Milestones, Projects, Commits,
    Branches, Actions, Code Scanning, Specialized Review Requests).
    """
    c = conn.cursor()

    # ============= Original / Revision A Tables =============

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

    # watchers
    c.execute("""
    CREATE TABLE IF NOT EXISTS watchers (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      user_login VARCHAR(255) NOT NULL,
      raw_json JSON,
      UNIQUE KEY (repo_name, user_login)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # stars
    c.execute("""
    CREATE TABLE IF NOT EXISTS stars (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      user_login VARCHAR(255) NOT NULL,
      starred_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, user_login, starred_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # forks
    c.execute("""
    CREATE TABLE IF NOT EXISTS forks (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      fork_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, fork_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # issues
    c.execute("""
    CREATE TABLE IF NOT EXISTS issues (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      created_at DATETIME,
      last_event_id BIGINT UNSIGNED DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # pulls
    c.execute("""
    CREATE TABLE IF NOT EXISTS pulls (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT NOT NULL,
      created_at DATETIME,
      last_event_id BIGINT UNSIGNED DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # issue_events
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

    # pull_events
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

    # issue_comments
    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_comments (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      comment_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      body LONGTEXT,
      UNIQUE KEY (repo_name, issue_number, comment_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # issue_reactions
    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_reactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      issue_number INT NOT NULL,
      reaction_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, issue_number, reaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # pull_reactions
    c.execute("""
    CREATE TABLE IF NOT EXISTS pull_reactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT NOT NULL,
      reaction_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, pull_number, reaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # issue_comment_reactions
    c.execute("""
    CREATE TABLE IF NOT EXISTS issue_comment_reactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      issue_number INT,
      comment_id BIGINT UNSIGNED NOT NULL,
      reaction_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, issue_number, comment_id, reaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # pull_review_comments
    c.execute("""
    CREATE TABLE IF NOT EXISTS pull_review_comments (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT NOT NULL,
      comment_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      body LONGTEXT,
      UNIQUE KEY (repo_name, pull_number, comment_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # pull_comment_reactions
    c.execute("""
    CREATE TABLE IF NOT EXISTS pull_comment_reactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT,
      comment_id BIGINT UNSIGNED NOT NULL,
      reaction_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, pull_number, comment_id, reaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # ============= Advanced Endpoints Tables =============

    # releases
    c.execute("""
    CREATE TABLE IF NOT EXISTS releases (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      release_id BIGINT UNSIGNED NOT NULL,
      tag_name VARCHAR(255),
      name VARCHAR(255),
      draft TINYINT DEFAULT 0,
      prerelease TINYINT DEFAULT 0,
      published_at DATETIME,
      created_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, release_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # release_assets
    c.execute("""
    CREATE TABLE IF NOT EXISTS release_assets (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      release_id BIGINT UNSIGNED NOT NULL,
      asset_id BIGINT UNSIGNED NOT NULL,
      name VARCHAR(255),
      content_type VARCHAR(255),
      size BIGINT UNSIGNED,
      download_count BIGINT UNSIGNED,
      created_at DATETIME,
      updated_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, release_id, asset_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # labels
    c.execute("""
    CREATE TABLE IF NOT EXISTS repo_labels (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      label_name VARCHAR(255) NOT NULL,
      color VARCHAR(255),
      description TEXT,
      UNIQUE KEY (repo_name, label_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # milestones
    c.execute("""
    CREATE TABLE IF NOT EXISTS repo_milestones (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      milestone_id BIGINT UNSIGNED NOT NULL,
      title VARCHAR(255),
      state VARCHAR(50),
      description TEXT,
      due_on DATETIME,
      created_at DATETIME,
      updated_at DATETIME,
      closed_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, milestone_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # projects => boards, columns, cards
    c.execute("""
    CREATE TABLE IF NOT EXISTS repo_projects (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      project_id BIGINT UNSIGNED NOT NULL,
      name VARCHAR(255),
      body TEXT,
      state VARCHAR(50),
      created_at DATETIME,
      updated_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, project_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS project_columns (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      project_id BIGINT UNSIGNED NOT NULL,
      column_id BIGINT UNSIGNED NOT NULL,
      name VARCHAR(255),
      created_at DATETIME,
      updated_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, project_id, column_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS project_cards (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      project_id BIGINT UNSIGNED NOT NULL,
      column_id BIGINT UNSIGNED NOT NULL,
      card_id BIGINT UNSIGNED NOT NULL,
      note TEXT,
      content_type VARCHAR(255),
      content_id BIGINT UNSIGNED,
      created_at DATETIME,
      updated_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, card_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # commits
    c.execute("""
    CREATE TABLE IF NOT EXISTS commits (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      sha CHAR(40) NOT NULL,
      author_login VARCHAR(255),
      committer_login VARCHAR(255),
      commit_message TEXT,
      commit_date DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, sha)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


    # branches
    c.execute("""
    CREATE TABLE IF NOT EXISTS branches (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      branch_name VARCHAR(255) NOT NULL,
      commit_sha CHAR(40),
      protected TINYINT DEFAULT 0,
      raw_json JSON,
      UNIQUE KEY (repo_name, branch_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # actions_runs
    c.execute("""
    CREATE TABLE IF NOT EXISTS actions_runs (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      run_id BIGINT UNSIGNED NOT NULL,
      head_branch VARCHAR(255),
      head_sha CHAR(40),
      event_type VARCHAR(255),
      status VARCHAR(255),
      conclusion VARCHAR(255),
      workflow_id BIGINT UNSIGNED,
      created_at DATETIME,
      updated_at DATETIME,
      run_started_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # code_scanning_alerts
    c.execute("""
    CREATE TABLE IF NOT EXISTS code_scanning_alerts (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      alert_number BIGINT UNSIGNED NOT NULL,
      state VARCHAR(50),
      rule_id VARCHAR(255),
      rule_name VARCHAR(255),
      created_at DATETIME,
      updated_at DATETIME,
      raw_json JSON,
      UNIQUE KEY (repo_name, alert_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # specialized review requests
    c.execute("""
    CREATE TABLE IF NOT EXISTS review_request_events (
      id INT AUTO_INCREMENT PRIMARY KEY,
      repo_name VARCHAR(255) NOT NULL,
      pull_number INT NOT NULL,
      request_event_id BIGINT UNSIGNED NOT NULL,
      created_at DATETIME,
      requested_reviewer VARCHAR(255),
      requested_team VARCHAR(255),
      raw_json JSON,
      UNIQUE KEY (repo_name, pull_number, request_event_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    c.close()
    logging.info("[ossmining] All tables created or verified (advanced).")
