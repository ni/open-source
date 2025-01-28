# fetch_releases.py

import logging
import time
import requests
from datetime import datetime

def get_last_page(resp):
    link_header = resp.headers.get("Link")
    if not link_header:
        return None
    parts = link_header.split(',')
    import re
    for p in parts:
        if 'rel="last"' in p:
            m = re.search(r'[?&]page=(\d+)', p)
            if m:
                return int(m.group(1))
    return None

def robust_get_page(session, url, params, handle_rate_limit_func, max_retries=20):
    from requests.exceptions import ConnectionError
    mini_retry_attempts=3
    for attempt in range(1,max_retries+1):
        local_attempt=1
        while local_attempt<=mini_retry_attempts:
            try:
                resp=session.get(url, params=params)
                handle_rate_limit_func(resp)
                if resp.status_code==200:
                    return (resp,True)
                elif resp.status_code in (403,429,500,502,503,504):
                    logging.warning("[deadbird/releases] HTTP %d => attempt %d/%d => retry => %s",
                                    resp.status_code,attempt,max_retries,url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/releases] HTTP %d => attempt %d => break => %s",
                                    resp.status_code,attempt,url)
                    return (resp,False)
                break
            except ConnectionError:
                logging.warning("[deadbird/releases] Connection error => local mini-retry => %s",url)
                time.sleep(3)
                local_attempt+=1
        if local_attempt>mini_retry_attempts:
            logging.warning("[deadbird/releases] Exhausted mini-retry => break => %s",url)
            return (None,False)
    logging.warning("[deadbird/releases] Exceeded max_retries => give up => %s",url)
    return (None,False)

def list_releases_single_thread(conn, owner, repo, enabled,
                                session, handle_rate_limit_func,
                                max_retries):
    if enabled==0:
        logging.info("Repo %s/%s => disabled => skip releases",owner,repo)
        return
    repo_name=f"{owner}/{repo}"
    page=1
    last_page=None
    total_inserted=0
    while True:
        url=f"https://api.github.com/repos/{owner}/{repo}/releases"
        params={"page":page,"per_page":20}
        (resp,success)=robust_get_page(session,url,params,handle_rate_limit_func,max_retries)
        if not success or not resp:
            break
        data=resp.json()
        if not data:
            break
        if last_page is None:
            last_page=get_last_page(resp)

        # approximate total if we have last_page
        total_items=0
        if last_page:
            total_items=last_page*20

        new_count=0
        for rel_obj in data:
            if store_release_and_assets(conn,repo_name,rel_obj):
                new_count+=1
        total_inserted+=new_count

        if last_page:
            progress=(page/last_page)*100.0
            logging.debug("[deadbird/releases] page=%d/%d => %.3f%% => inserted %d new => %s",
                          page,last_page,progress,new_count,repo_name)
            if total_items>0:
                logging.debug("[deadbird/releases] => so far inserted %d total new rows out of approx %d => %s",
                              total_inserted,total_items,repo_name)
        else:
            logging.debug("[deadbird/releases] page=%d => inserted %d => no last_page => %s",
                          page,new_count,repo_name)

        if len(data)<20:
            break
        page+=1

    logging.info("[deadbird/releases] Done => total inserted %d => %s",total_inserted,repo_name)

def store_release_and_assets(conn, repo_name, rel_obj):
    """
    Return True if we inserted a brand-new release row,
    False if it existed (we still update).
    """
    c=conn.cursor()
    release_id=rel_obj["id"]
    c.execute("""
      SELECT release_id FROM releases
      WHERE repo_name=%s AND release_id=%s
    """,(repo_name,release_id))
    row=c.fetchone()
    if row:
        # update => no new row
        update_release_record(c,conn,repo_name,release_id,rel_obj)
        c.close()
        return False
    else:
        insert_release_record(c,conn,repo_name,release_id,rel_obj)
        c.close()
        return True

def insert_release_record(c, conn, repo_name, release_id, rel_obj):
    import json
    tag_name=rel_obj.get("tag_name","")
    name=rel_obj.get("name","")
    draft=1 if rel_obj.get("draft",False) else 0
    prerelease=1 if rel_obj.get("prerelease",False) else 0
    published_str=rel_obj.get("published_at")
    published_dt=None
    if published_str:
        published_dt=datetime.strptime(published_str,"%Y-%m-%dT%H:%M:%SZ")
    created_str=rel_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(rel_obj,ensure_ascii=False)
    sqlrel="""
    INSERT INTO releases
     (repo_name, release_id, tag_name, name, draft, prerelease,
      published_at, created_at, raw_json)
    VALUES
     (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sqlrel,(repo_name,release_id,tag_name,name,
                      draft,prerelease,published_dt,created_dt,raw_str))
    conn.commit()
    # store assets
    assets=rel_obj.get("assets",[])
    for asset in assets:
        insert_release_asset(c,conn,repo_name,release_id,asset)

def update_release_record(c, conn, repo_name, release_id, rel_obj):
    import json
    tag_name=rel_obj.get("tag_name","")
    name=rel_obj.get("name","")
    draft=1 if rel_obj.get("draft",False) else 0
    prerelease=1 if rel_obj.get("prerelease",False) else 0
    published_str=rel_obj.get("published_at")
    published_dt=None
    if published_str:
        published_dt=datetime.strptime(published_str,"%Y-%m-%dT%H:%M:%SZ")
    created_str=rel_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(rel_obj,ensure_ascii=False)
    sqlrel="""
    UPDATE releases
    SET tag_name=%s, name=%s, draft=%s, prerelease=%s,
        published_at=%s, created_at=%s, raw_json=%s
    WHERE repo_name=%s AND release_id=%s
    """
    c.execute(sqlrel,(tag_name,name,draft,prerelease,
                      published_dt,created_dt,raw_str,repo_name,release_id))
    conn.commit()
    assets=rel_obj.get("assets",[])
    for asset in assets:
        upsert_release_asset(c,conn,repo_name,release_id,asset)

def insert_release_asset(c,conn,repo_name,release_id,asset_obj):
    import json
    asset_id=asset_obj["id"]
    name=asset_obj.get("name","")
    content_type=asset_obj.get("content_type","")
    size=asset_obj.get("size",0)
    download_count=asset_obj.get("download_count",0)

    created_str=asset_obj.get("created_at")
    created_dt=None
    if created_str:
        created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

    updated_str=asset_obj.get("updated_at")
    updated_dt=None
    if updated_str:
        updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

    raw_str=json.dumps(asset_obj,ensure_ascii=False)
    sqlast="""
    INSERT INTO release_assets
      (repo_name, release_id, asset_id, name, content_type,
       size, download_count, created_at, updated_at, raw_json)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    c.execute(sqlast,(repo_name,release_id,asset_id,name,content_type,size,
                      download_count,created_dt,updated_dt,raw_str))
    conn.commit()

def upsert_release_asset(c,conn,repo_name,release_id,asset_obj):
    import json
    asset_id=asset_obj["id"]
    c.execute("""
      SELECT asset_id FROM release_assets
      WHERE repo_name=%s AND release_id=%s AND asset_id=%s
    """,(repo_name,release_id,asset_id))
    row=c.fetchone()
    if row:
        name=asset_obj.get("name","")
        content_type=asset_obj.get("content_type","")
        size=asset_obj.get("size",0)
        download_count=asset_obj.get("download_count",0)

        created_str=asset_obj.get("created_at")
        created_dt=None
        if created_str:
            created_dt=datetime.strptime(created_str,"%Y-%m-%dT%H:%M:%SZ")

        updated_str=asset_obj.get("updated_at")
        updated_dt=None
        if updated_str:
            updated_dt=datetime.strptime(updated_str,"%Y-%m-%dT%H:%M:%SZ")

        raw_str=json.dumps(asset_obj, ensure_ascii=False)
        sql="""
        UPDATE release_assets
        SET name=%s, content_type=%s, size=%s,
            download_count=%s, created_at=%s, updated_at=%s,
            raw_json=%s
        WHERE repo_name=%s AND release_id=%s AND asset_id=%s
        """
        c.execute(sql,(name,content_type,size,download_count,
                       created_dt,updated_dt,raw_str,
                       repo_name,release_id,asset_id))
        conn.commit()
    else:
        insert_release_asset(c,conn,repo_name,release_id,asset_obj)
