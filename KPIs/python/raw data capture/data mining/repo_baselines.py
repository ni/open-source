# repo_baselines.py
import logging

def get_baseline_info(conn, owner, repo):
    """
    Return (baseline_date, enabled)
    If not found, default is (None, 1)
    """
    c=conn.cursor()
    c.execute("SELECT baseline_date, enabled FROM repo_baselines WHERE owner=%s AND repo=%s",(owner,repo))
    row=c.fetchone()
    c.close()
    if row is None:
        return (None,1)
    return (row[0], row[1])

def refresh_baseline_info_mid_run(conn, owner, repo, old_base, old_en):
    """
    We only check if 'enabled' changed mid-run.
    baseline_date is not used for numeric skipping, but we track if it changed for completeness.
    """
    new_base,new_en=get_baseline_info(conn,owner,repo)
    if new_en!=old_en:
        logging.info("Repo %s/%s => enabled changed mid-run from %s to %s",
                     owner,repo,old_en,new_en)
    return (new_base,new_en)

def set_baseline_date(conn, owner, repo, new_date):
    """
    Store earliest commit date in baseline_date if you want to see it in DB for reference
    """
    c=conn.cursor()
    c.execute("""
    INSERT INTO repo_baselines (owner, repo, baseline_date, enabled, updated_at)
    VALUES (%s,%s,%s,1,NOW())
    ON DUPLICATE KEY UPDATE
      baseline_date=VALUES(baseline_date),
      updated_at=NOW()
    """,(owner,repo,new_date))
    conn.commit()
    c.close()
