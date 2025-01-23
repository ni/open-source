# repo_baselines.py
import logging

def get_baseline_info(conn, owner, repo):
    """
    SELECT baseline_date, enabled => fallback (None,1) if not found
    We do still store baseline_date here, but we may override it from days_to_capture in main.py
    """
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
    if new_base!=old_base or new_en!=old_en:
        logging.info("Repo %s/%s => baseline changed mid-run from (%s,%s) to (%s,%s)",
                     owner,repo,old_base,old_en,new_base,new_en)
    return (new_base,new_en)
