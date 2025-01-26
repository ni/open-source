############################################################
# splitted_metrics.py
# Gathers BFS splitted variables from DB for [start_dt..end_dt)
# and logs EXACT queries (no placeholders) for easy MySQL Workbench usage.
############################################################

import mysql.connector
import re
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

def _inject_params_into_sql(query_str, param_list):
    """
    Transforms a query with '%s' placeholders into a final
    literal SQL statement, substituting param values with basic
    string escaping. This allows copy/paste into MySQL Workbench.
    Very naive approach: we replace placeholders in order.

    e.g. "SELECT ... WHERE repo_name=%s AND created_at> %s" => 
         "SELECT ... WHERE repo_name='facebook/react' AND created_at>'2024-06-05 14:49:03'"
    """
    final_sql= query_str
    idx=0
    for p in param_list:
        # naive single-quote wrap
        if p is None:
            p_str= "NULL"
        else:
            p_str= str(p)
            # basic escaping of single quotes
            p_str= p_str.replace("'","\\'")
            p_str= f"'{p_str}'"
        final_sql= final_sql.replace("%s", p_str, 1)
        idx+=1
    return final_sql

def gather_data_for_window(repo_name, start_dt, end_dt):
    """
    Returns a dict of splitted BFS raw variables for [start_dt..end_dt),
    PLUS a 'queriesUsed' sub-dict capturing the actual SQL queries & parameters.

    We now produce an additional 'finalSQL' that has param placeholders
    replaced with literal values, so you can copy/paste directly into MySQL.

    keys:
      mergesRaw, closedIssRaw, closedPRRaw, forksRaw, starsRaw,
      newIssRaw, commentsIssRaw, commentsPRRaw, reactIssRaw, reactPRRaw,
      pullRaw
    """
    results= {
      "mergesRaw": 0,
      "closedIssRaw": 0,
      "closedPRRaw": 0,
      "forksRaw": 0,
      "starsRaw": 0,
      "newIssRaw": 0,
      "commentsIssRaw": 0,
      "commentsPRRaw": 0,
      "reactIssRaw": 0,
      "reactPRRaw": 0,
      "pullRaw": 0,
      "queriesUsed": {}
    }

    cnx= get_db_connection()
    cursor= cnx.cursor()

    # mergesRaw => from pull_events, event='merged'
    q_merges= """
      SELECT COUNT(*)
      FROM pull_events
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
        AND JSON_EXTRACT(raw_json,'$.event')='merged'
    """
    params_merges= (repo_name, start_dt, end_dt)
    cursor.execute(q_merges, params_merges)
    merges_val= cursor.fetchone()[0]
    results["mergesRaw"]= merges_val
    final_merges_sql= _inject_params_into_sql(q_merges, params_merges)
    results["queriesUsed"]["mergesRaw"]= {
       "originalSQL": q_merges.strip(),
       "params": params_merges,
       "finalSQL": final_merges_sql
    }

    # closedIssRaw => from issue_events, event='closed'
    q_closed_iss= """
      SELECT COUNT(*)
      FROM issue_events ie
      WHERE ie.repo_name=%s
        AND ie.created_at >= %s AND ie.created_at < %s
        AND JSON_EXTRACT(ie.raw_json,'$.event')='closed'
        AND ie.issue_number IN (
           SELECT i.issue_number FROM issues i WHERE i.repo_name=%s
        )
    """
    params_ci= (repo_name, start_dt, end_dt, repo_name)
    cursor.execute(q_closed_iss, params_ci)
    ci_val= cursor.fetchone()[0]
    results["closedIssRaw"]= ci_val
    final_ci_sql= _inject_params_into_sql(q_closed_iss, params_ci)
    results["queriesUsed"]["closedIssRaw"]= {
       "originalSQL": q_closed_iss.strip(),
       "params": params_ci,
       "finalSQL": final_ci_sql
    }

    # closedPRRaw => from pull_events event in ('closed','merged')
    q_closed_pr= """
      SELECT COUNT(*)
      FROM pull_events
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
        AND JSON_EXTRACT(raw_json,'$.event') in ('closed','merged')
    """
    params_cpr= (repo_name, start_dt, end_dt)
    cursor.execute(q_closed_pr, params_cpr)
    cpr_val= cursor.fetchone()[0]
    results["closedPRRaw"]= cpr_val
    final_cpr_sql= _inject_params_into_sql(q_closed_pr, params_cpr)
    results["queriesUsed"]["closedPRRaw"]= {
       "originalSQL": q_closed_pr.strip(),
       "params": params_cpr,
       "finalSQL": final_cpr_sql
    }

    # forksRaw
    q_forks= """
      SELECT COUNT(*)
      FROM forks
      WHERE repo_name=%s
        AND created_at >= %s
        AND created_at < %s
    """
    params_f= (repo_name, start_dt, end_dt)
    cursor.execute(q_forks, params_f)
    f_val= cursor.fetchone()[0]
    results["forksRaw"]= f_val
    final_forks_sql= _inject_params_into_sql(q_forks, params_f)
    results["queriesUsed"]["forksRaw"]= {
       "originalSQL": q_forks.strip(),
       "params": params_f,
       "finalSQL": final_forks_sql
    }

    # starsRaw
    q_stars= """
      SELECT COUNT(*)
      FROM stars
      WHERE repo_name=%s
        AND starred_at >= %s AND starred_at < %s
    """
    params_s= (repo_name, start_dt, end_dt)
    cursor.execute(q_stars, params_s)
    st_val= cursor.fetchone()[0]
    results["starsRaw"]= st_val
    final_stars_sql= _inject_params_into_sql(q_stars, params_s)
    results["queriesUsed"]["starsRaw"]= {
       "originalSQL": q_stars.strip(),
       "params": params_s,
       "finalSQL": final_stars_sql
    }

    # newIssRaw => issues.created_at
    q_new_iss= """
      SELECT COUNT(*)
      FROM issues
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    params_ni= (repo_name, start_dt, end_dt)
    cursor.execute(q_new_iss, params_ni)
    ni_val= cursor.fetchone()[0]
    results["newIssRaw"]= ni_val
    final_ni_sql= _inject_params_into_sql(q_new_iss, params_ni)
    results["queriesUsed"]["newIssRaw"]= {
       "originalSQL": q_new_iss.strip(),
       "params": params_ni,
       "finalSQL": final_ni_sql
    }

    # pullRaw => from pulls.created_at
    q_pull= """
      SELECT COUNT(*)
      FROM pulls
      WHERE repo_name=%s
        AND created_at >= %s AND created_at < %s
    """
    params_pull= (repo_name, start_dt, end_dt)
    cursor.execute(q_pull, params_pull)
    pr_val= cursor.fetchone()[0]
    results["pullRaw"]= pr_val
    final_pull_sql= _inject_params_into_sql(q_pull, params_pull)
    results["queriesUsed"]["pullRaw"]= {
       "originalSQL": q_pull.strip(),
       "params": params_pull,
       "finalSQL": final_pull_sql
    }

    # commentsIssRaw => ignoring +1/-1 => body not like
    q_c_iss= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN issues i ON (i.repo_name=ic.repo_name AND i.issue_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body NOT LIKE '%+1%' AND ic.body NOT LIKE '%-1%')
    """
    params_ciss= (repo_name, start_dt, end_dt)
    cursor.execute(q_c_iss, params_ciss)
    ciss_val= cursor.fetchone()[0]
    results["commentsIssRaw"]= ciss_val
    final_ciss_sql= _inject_params_into_sql(q_c_iss, params_ciss)
    results["queriesUsed"]["commentsIssRaw"]= {
       "originalSQL": q_c_iss.strip(),
       "params": params_ciss,
       "finalSQL": final_ciss_sql
    }

    # commentsPRRaw => ignoring +1/-1 => body not like
    q_c_pr= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN pulls p ON (p.repo_name=ic.repo_name AND p.pull_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body NOT LIKE '%+1%' AND ic.body NOT LIKE '%-1%')
    """
    params_cpr2= (repo_name, start_dt, end_dt)
    cursor.execute(q_c_pr, params_cpr2)
    cpr_val2= cursor.fetchone()[0]
    results["commentsPRRaw"]= cpr_val2
    final_cpr2_sql= _inject_params_into_sql(q_c_pr, params_cpr2)
    results["queriesUsed"]["commentsPRRaw"]= {
       "originalSQL": q_c_pr.strip(),
       "params": params_cpr2,
       "finalSQL": final_cpr2_sql
    }

    # reactIssRaw => +1/-1 in issues
    q_r_iss= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN issues i ON (i.repo_name=ic.repo_name AND i.issue_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body LIKE '%+1%' OR ic.body LIKE '%-1%')
    """
    params_riss= (repo_name, start_dt, end_dt)
    cursor.execute(q_r_iss, params_riss)
    ri_val= cursor.fetchone()[0]
    results["reactIssRaw"]= ri_val
    final_riss_sql= _inject_params_into_sql(q_r_iss, params_riss)
    results["queriesUsed"]["reactIssRaw"]= {
       "originalSQL": q_r_iss.strip(),
       "params": params_riss,
       "finalSQL": final_riss_sql
    }

    # reactPRRaw => +1/-1 in PRs
    q_r_pr= """
      SELECT COUNT(*)
      FROM issue_comments ic
      JOIN pulls p ON (p.repo_name=ic.repo_name AND p.pull_number=ic.issue_number)
      WHERE ic.repo_name=%s
        AND ic.created_at >= %s AND ic.created_at < %s
        AND (ic.body LIKE '%+1%' OR ic.body LIKE '%-1%')
    """
    params_rpr= (repo_name, start_dt, end_dt)
    cursor.execute(q_r_pr, params_rpr)
    rpr_val= cursor.fetchone()[0]
    results["reactPRRaw"]= rpr_val
    final_rpr_sql= _inject_params_into_sql(q_r_pr, params_rpr)
    results["queriesUsed"]["reactPRRaw"]= {
       "originalSQL": q_r_pr.strip(),
       "params": params_rpr,
       "finalSQL": final_rpr_sql
    }

    cursor.close()
    cnx.close()
    return results
