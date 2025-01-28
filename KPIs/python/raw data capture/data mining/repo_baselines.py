# repo_baselines.py
import logging

def get_baseline_info(conn, owner, repo):
    c=conn.cursor()
    c.execute("SELECT baseline_date, enabled FROM repo_baselines WHERE owner=%s AND repo=%s",
              (owner,repo))
    row=c.fetchone()
    c.close()
    if row is None:
        return (None,1)
    return (row[0], row[1])

def refresh_baseline_info_mid_run(conn, owner, repo, old_base, old_en):
    new_base,new_en=get_baseline_info(conn,owner,repo)
    if new_en!=old_en or new_base!=old_base:
        logging.info("Repo %s/%s => baseline changed mid-run from (%s,%s) to (%s,%s)",
                     owner,repo,old_base,old_en,new_base,new_en)
    return (new_base,new_en)

def set_baseline_date(conn, owner, repo, new_date):
    c=conn.cursor()
    c.execute("""
    INSERT INTO repo_baselines (owner, repo, baseline_date, enabled, updated_at)
    VALUES (%s,%s,%s,1,NOW())
    ON DUPLICATE KEY UPDATE
      baseline_date=VALUES(baseline_date),
      enabled=1,
      updated_at=NOW()
    """,(owner,repo,new_date))
    conn.commit()
    c.close()
