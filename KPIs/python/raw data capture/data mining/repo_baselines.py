# repo_baselines.py

import logging

def get_baseline_info(conn, owner, repo):
    """
    Return (baseline_date, enabled). If not found => (None,1) => fetch everything, enabled=1
    """
    c=conn.cursor()
    c.execute("SELECT baseline_date, enabled FROM repo_baselines WHERE owner=%s AND repo=%s",(owner,repo))
    row=c.fetchone()
    c.close()
    if row is None:
        return (None,1)
    return (row[0],row[1])

def refresh_baseline_info_mid_run(conn, owner, repo, old_base, old_en):
    new_base,new_en=get_baseline_info(conn,owner,repo)
    if new_base!=old_base or new_en!=old_en:
        logging.info("Repo %s/%s => baseline changed mid-run from (%s,%s) to (%s,%s)",
                     owner,repo,old_base,old_en,new_base,new_en)
    return (new_base,new_en)
