# repo_baselines.py
"""
DB functions to get/update baseline_date + enabled for each (owner, repo).
We skip items if created_at > baseline_date. If enabled=0 => skip entire repo.
We also do mid-run re-check to see if baseline_date changed or enabled toggled.
"""

import logging

def get_baseline_info(conn, owner, repo):
    """
    Returns (baseline_date, enabled).
    If not found => (None, 1) meaning no skip + enabled by default.
    """
    c = conn.cursor()
    c.execute("""
      SELECT baseline_date, enabled
      FROM repo_baselines
      WHERE owner=%s AND repo=%s
    """, (owner, repo))
    row = c.fetchone()
    c.close()
    if row is None:
        return (None, 1)
    return (row[0], row[1])

def refresh_baseline_info_mid_run(conn, owner, repo, old_baseline, old_enabled):
    """
    Re-check DB => see if baseline_date or enabled changed. If so, log + return new.
    """
    new_base, new_en = get_baseline_info(conn, owner, repo)
    if new_base != old_baseline or new_en != old_enabled:
        logging.info("Repo %s/%s => baseline changed mid-run from (%s, %s) to (%s, %s)",
                     owner, repo, old_baseline, old_enabled, new_base, new_en)
    return (new_base, new_en)
