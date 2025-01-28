# etags.py

import logging
from datetime import datetime

def get_endpoint_state(conn, owner, repo, endpoint_name):
    """
    Load the existing etag_value and last_updated for a given (owner, repo, endpoint_name)
    from the repo_endpoints table. Return (etag_value, last_updated_dt).
    If none found, return (None, None).
    """
    c = conn.cursor()
    sql = """
    SELECT etag_value, last_updated
    FROM repo_endpoints
    WHERE owner=%s AND repo=%s AND endpoint_name=%s
    """
    c.execute(sql, (owner, repo, endpoint_name))
    row = c.fetchone()
    c.close()
    if row:
        etag_value = row[0]
        last_updated = row[1]  # a datetime or None
        return (etag_value, last_updated)
    else:
        return (None, None)

def update_endpoint_state(conn, owner, repo, endpoint_name,
                          new_etag_value, new_last_updated):
    """
    Insert or update row in repo_endpoints. 
    If new_etag_value is None => keep old one. 
    If new_last_updated is None => keep old date or set it to that if no row existed.
    """
    c = conn.cursor()
    # We do UPSERT logic => if row doesn't exist => create, else update.
    # We'll handle them carefully. The easiest might be:
    #   1) Attempt a SELECT for the existing row.
    #   2) If found => update fields that are not None.
    #   3) If not found => insert a fresh row.
    c.execute("""
    SELECT etag_value, last_updated
    FROM repo_endpoints
    WHERE owner=%s AND repo=%s AND endpoint_name=%s
    """,(owner,repo,endpoint_name))
    row=c.fetchone()
    if row:
        old_etag, old_last_updated=row
        final_etag=new_etag_value if new_etag_value else old_etag
        final_last_updated=old_last_updated
        if new_last_updated:
            # compare if new_last_updated is actually later
            if not final_last_updated or new_last_updated>final_last_updated:
                final_last_updated=new_last_updated

        upd_sql="""
        UPDATE repo_endpoints
        SET etag_value=%s, last_updated=%s
        WHERE owner=%s AND repo=%s AND endpoint_name=%s
        """
        c.execute(upd_sql,(final_etag,final_last_updated,owner,repo,endpoint_name))
        conn.commit()
        c.close()
    else:
        # insert new row
        ins_sql="""
        INSERT INTO repo_endpoints
         (owner, repo, endpoint_name, etag_value, last_updated)
        VALUES
         (%s,%s,%s,%s,%s)
        """
        c.execute(ins_sql,(owner,repo,endpoint_name,new_etag_value,new_last_updated))
        conn.commit()
        c.close()

    logging.debug("[etags] Updated endpoint_state => %s/%s/%s => etag=%s, last_updated=%s",
                  owner,repo,endpoint_name,new_etag_value,new_last_updated)
