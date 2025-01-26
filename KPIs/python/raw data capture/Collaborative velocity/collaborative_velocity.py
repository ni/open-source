#!/usr/bin/env python3
"""
Quarter-Based Collaborative Velocity Analysis, now with Forks/Stars (User Interest Growth)
-----------------------------------------------------------------------------------------
This script outputs FOUR tables:

1) Merges/Issues Main Table
2) Merges/Issues Comparison Table
3) Forks/Stars (UIG) Main Table
4) Forks/Stars (UIG) Comparison Table

Where "UIG" = 0.4 × (scaled forks) + 0.6 × (scaled stars).

Forks/Stars are counted per quarter (like merges/issues),
and also have scale factors computed from the same "first N days" sum approach:
- scaleFactorF[r], scaleFactorS[r]
- If scaling repo sum>0 but a non-scaling repo sum=0 => "cannot scale" for that metric.
- If both=0 => factor=1.0.
- Then each quarter's raw forks/stars is multiplied by that factor => scaled.

Usage Example:
--------------
  python collaborative_velocity.py \
    --scaling-repo ni/labview-icon-editor \
    --start-fy 2024 \
    --end-fy 2025 \
    --scaling-window 210 \
    --global-offset -32
"""

import argparse
import configparser
import mysql.connector
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import numpy as np
import sys

###############################################################################
# 1) Quarter & DB Helpers
###############################################################################

def get_fy_quarter_boundaries(fy):
    """
    Return Q1..Q4 boundaries for the given FY:
      Q1: (FY-1)-10-01..(FY-1)-12-31
      Q2: (FY)-01-01..(FY)-03-31
      Q3: (FY)-04-01..(FY)-06-30
      Q4: (FY)-07-01..(FY)-09-30
    We'll replace any '\n' in the labels with space for single-line output.
    """
    q1_label = f"Q1\nFY{fy}"
    q1_start = datetime(fy - 1, 10, 1, 0, 0, 0)
    q1_end   = datetime(fy - 1, 12, 31, 23, 59, 59)

    q2_label = f"Q2\nFY{fy}"
    q2_start = datetime(fy, 1, 1, 0, 0, 0)
    q2_end   = datetime(fy, 3, 31, 23, 59, 59)

    q3_label = f"Q3\nFY{fy}"
    q3_start = datetime(fy, 4, 1, 0, 0, 0)
    q3_end   = datetime(fy, 6, 30, 23, 59, 59)

    q4_label = f"Q4\nFY{fy}"
    q4_start = datetime(fy, 7, 1, 0, 0, 0)
    q4_end   = datetime(fy, 9, 30, 23, 59, 59)

    return [
        (q1_label, q1_start, q1_end),
        (q2_label, q2_start, q2_end),
        (q3_label, q3_start, q3_end),
        (q4_label, q4_start, q4_end)
    ]

def get_oldest_date(cursor, repo_name):
    """
    Return earliest creation date across merges/issues for a given repo.
    """
    q= """
        SELECT MIN(all_min) AS oldest_date
        FROM (
            SELECT MIN(created_at) AS all_min
            FROM pulls
            WHERE repo_name=%s
            UNION ALL
            SELECT MIN(created_at) AS all_min
            FROM issues
            WHERE repo_name=%s
        ) subq
    """
    cursor.execute(q, (repo_name, repo_name))
    row= cursor.fetchone()
    return row[0] if row and row[0] else None

def get_last_date(cursor, repo_name):
    """
    Return latest creation date across merges/issues for a given repo.
    """
    q= """
        SELECT MAX(all_max)
        FROM (
            SELECT MAX(created_at) AS all_max
            FROM pulls
            WHERE repo_name=%s
            UNION ALL
            SELECT MAX(created_at) AS all_max
            FROM issues
            WHERE repo_name=%s
        ) subq
    """
    cursor.execute(q, (repo_name, repo_name))
    row= cursor.fetchone()
    return row[0] if row and row[0] else None

def format_scale_factor_3dec(f):
    """
    Return a string with up to 3 decimals for factor or "N/A" if None.
    """
    if f is None:
        return "N/A"
    return f"{f:.3f}"

###############################################################################
# 2) Merges/Issues (like original)
###############################################################################

def get_merges_and_issues(cursor, repo, start_dt, end_dt):
    """
    Return (# merges, # issues closed) in [start_dt, end_dt].
    """
    if not start_dt or not end_dt:
        return (0,0)
    qm= """
        SELECT COUNT(*)
        FROM pulls
        WHERE repo_name=%s
          AND merged_at IS NOT NULL
          AND merged_at >= %s
          AND merged_at <= %s
    """
    cursor.execute(qm, (repo, start_dt, end_dt))
    merges_count= cursor.fetchone()[0]

    qi= """
        SELECT COUNT(*)
        FROM issues
        WHERE repo_name=%s
          AND closed_at IS NOT NULL
          AND closed_at >= %s
          AND closed_at <= %s
    """
    cursor.execute(qi, (repo, start_dt, end_dt))
    issues_count= cursor.fetchone()[0]
    return (merges_count, issues_count)

def compute_merges_issues_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    For merges/issues:
      - We sum merges & issues in [oldest+offset, oldest+offset+window_days] for scaling
      - Compare with each non-scaling repo => merges factor (M_sSum / M_rSum), issues factor, etc.
    """
    scaleFactorM={}
    scaleFactorI={}
    partialRepo={}
    cannotScale={}

    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        # fallback => no data
        for r in all_repos:
            scaleFactorM[r]=1.0
            scaleFactorI[r]=1.0
            partialRepo[r]=False
            cannotScale[r]=False
        return scaleFactorM, scaleFactorI, partialRepo, cannotScale

    s_end= s_old+ timedelta(days=window_days)
    s_last= get_last_date(cursor, scaling_repo)
    partial_s= False
    used_end= s_end
    if s_last and s_last< s_end:
        partial_s= True
        used_end= s_last

    # merges/issues for scaling
    M_sSum,I_sSum= get_merges_and_issues(cursor, scaling_repo, s_old, used_end)
    scaleFactorM[scaling_repo]=1.0
    scaleFactorI[scaling_repo]=1.0
    partialRepo[scaling_repo]= partial_s
    cannotScale[scaling_repo]= False

    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            scaleFactorM[r]=None
            scaleFactorI[r]=None
            partialRepo[r]=False
            cannotScale[r]= True
            continue
        rend= rold+ timedelta(days=window_days)
        rlast= get_last_date(cursor, r)
        isPartial= False
        used_end2= rend
        if rlast and rlast< rend:
            isPartial= True
            used_end2= rlast
        MrSum,IrSum= get_merges_and_issues(cursor, r, rold, used_end2)

        cScale=False
        sfm=None
        sfi=None
        # merges factor
        if (M_sSum>0 and MrSum==0):
            cScale=True
        elif M_sSum==0 and MrSum>0:
            sfm=0.0
        else:
            if MrSum==0:
                sfm=1.0
            else:
                sfm= M_sSum/MrSum
        # issues factor
        if (I_sSum>0 and IrSum==0):
            cScale=True
        elif I_sSum==0 and IrSum>0:
            sfi=0.0
        else:
            if IrSum==0:
                sfi=1.0
            else:
                sfi= I_sSum/IrSum

        scaleFactorM[r]= sfm
        scaleFactorI[r]= sfi
        partialRepo[r]= isPartial
        cannotScale[r]= cScale

    return scaleFactorM, scaleFactorI, partialRepo, cannotScale

###############################################################################
# 3) Forks/Stars => "User Interest Growth" (UIG = 0.4*F + 0.6*S)
###############################################################################

def get_forks_and_stars(cursor, repo, start_dt, end_dt):
    """
    Return (# forks, # stars) in [start_dt, end_dt].
    """
    if not start_dt or not end_dt:
        return (0,0)
    q_f= """
        SELECT COUNT(*)
        FROM forks
        WHERE repo_name=%s
          AND forked_at >= %s
          AND forked_at <= %s
    """
    cursor.execute(q_f, (repo, start_dt, end_dt))
    forks_count= cursor.fetchone()[0]

    q_s= """
        SELECT COUNT(*)
        FROM stars
        WHERE repo_name=%s
          AND starred_at >= %s
          AND starred_at <= %s
    """
    cursor.execute(q_s, (repo, start_dt, end_dt))
    stars_count= cursor.fetchone()[0]
    return (forks_count, stars_count)

def compute_forks_stars_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    For the "User Interest Growth" metric => we define scaleFactorF, scaleFactorS
    from the first N-day sum of forks/stars in the scaling repo vs. each non-scaling.
    If scaling>0 but repo=0 => cannot scale, etc.
    """
    scaleFactorF={}
    scaleFactorS={}
    partialUI={}
    cannotScaleUI={}

    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        # fallback => no data
        for r in all_repos:
            scaleFactorF[r]=1.0
            scaleFactorS[r]=1.0
            partialUI[r]=False
            cannotScaleUI[r]=False
        return scaleFactorF, scaleFactorS, partialUI, cannotScaleUI

    s_end= s_old+ timedelta(days=window_days)
    s_last= get_last_date(cursor, scaling_repo)
    partial_s= False
    used_end= s_end
    if s_last and s_last< s_end:
        partial_s= True
        used_end= s_last

    F_sSum, S_sSum= get_forks_and_stars(cursor, scaling_repo, s_old, used_end)
    scaleFactorF[scaling_repo]=1.0
    scaleFactorS[scaling_repo]=1.0
    partialUI[scaling_repo]= partial_s
    cannotScaleUI[scaling_repo]= False

    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            scaleFactorF[r]=None
            scaleFactorS[r]=None
            partialUI[r]=False
            cannotScaleUI[r]= True
            continue
        rend= rold+ timedelta(days=window_days)
        rlast= get_last_date(cursor, r)
        isPartial= False
        used_end2= rend
        if rlast and rlast< rend:
            isPartial= True
            used_end2= rlast
        F_rSum, S_rSum= get_forks_and_stars(cursor, r, rold, used_end2)

        cScale= False
        ff=None
        fs=None
        # forks factor
        if (F_sSum>0 and F_rSum==0):
            cScale=True
        elif F_sSum==0 and F_rSum>0:
            ff=0.0
        else:
            if F_rSum==0:
                ff=1.0
            else:
                ff= F_sSum/F_rSum
        # stars factor
        if (S_sSum>0 and S_rSum==0):
            cScale=True
        elif S_sSum==0 and S_rSum>0:
            fs=0.0
        else:
            if S_rSum==0:
                fs=1.0
            else:
                fs= S_sSum/S_rSum

        scaleFactorF[r]= ff
        scaleFactorS[r]= fs
        partialUI[r]= isPartial
        cannotScaleUI[r]= cScale

    return scaleFactorF, scaleFactorS, partialUI, cannotScaleUI

###############################################################################
# 4) MAC => 0.8*(IC scaled) + 2*(PRC scaled)
###############################################################################

def get_issues_and_comments(cursor, repo, start_dt, end_dt):
    """
    Return (# newly created issues, # newly created comments) in [start_dt, end_dt].
    Adapt this to your actual 'comments' table schema.
    """
    if not start_dt or not end_dt:
        return (0,0)
    # example for newly created issues
    q_iss= """
        SELECT COUNT(*)
        FROM issues
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at <= %s
    """
    cursor.execute(q_iss, (repo, start_dt, end_dt))
    new_issues= cursor.fetchone()[0]

    # example for new comments
    q_com= """
        SELECT COUNT(*)
        FROM comments
        WHERE repo_name=%s
          AND commented_at >= %s
          AND commented_at <= %s
    """
    cursor.execute(q_com, (repo, start_dt, end_dt))
    new_comments= cursor.fetchone()[0]

    return (new_issues, new_comments)

def get_pull_requests_created(cursor, repo, start_dt, end_dt):
    """
    Return # new PR creations (pulls.created_at in [start_dt, end_dt]).
    """
    if not start_dt or not end_dt:
        return 0
    q_pr= """
        SELECT COUNT(*)
        FROM pulls
        WHERE repo_name=%s
          AND created_at >= %s
          AND created_at <= %s
    """
    cursor.execute(q_pr, (repo, start_dt, end_dt))
    pr_count= cursor.fetchone()[0]
    return pr_count

def compute_mac_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    sub-metrics: IC => new issues + new comments,
                 PRC => new PR creations
    scaleFactorIC[r], scaleFactorPRC[r],
    partialMAC[r], cannotScaleMAC[r].
    If scaling>0 but repo=0 => cannot scale. If both=0 => factor=1.0
    """
    scaleFactorIC={}
    scaleFactorPRC={}
    partialMAC={}
    cannotScaleMAC={}

    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        for r in all_repos:
            scaleFactorIC[r]=1.0
            scaleFactorPRC[r]=1.0
            partialMAC[r]=False
            cannotScaleMAC[r]=False
        return scaleFactorIC, scaleFactorPRC, partialMAC, cannotScaleMAC

    s_end= s_old+ timedelta(days=window_days)
    s_last= get_last_date(cursor, scaling_repo)
    partial_s= False
    used_end= s_end
    if s_last and s_last< s_end:
        partial_s= True
        used_end= s_last

    iss_scal, com_scal= get_issues_and_comments(cursor, scaling_repo, s_old, used_end)
    ic_scal= iss_scal+ com_scal
    prc_scal= get_pull_requests_created(cursor, scaling_repo, s_old, used_end)

    scaleFactorIC[scaling_repo]=1.0
    scaleFactorPRC[scaling_repo]=1.0
    partialMAC[scaling_repo]= partial_s
    cannotScaleMAC[scaling_repo]= False

    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            scaleFactorIC[r]=None
            scaleFactorPRC[r]=None
            partialMAC[r]=False
            cannotScaleMAC[r]=True
            continue
        rend= rold+ timedelta(days=window_days)
        rlast= get_last_date(cursor, r)
        isPart= False
        used_end2= rend
        if rlast and rlast< rend:
            isPart= True
            used_end2= rlast
        iss_r, com_r= get_issues_and_comments(cursor, r, rold, used_end2)
        ic_r= iss_r+ com_r
        prc_r= get_pull_requests_created(cursor, r, rold, used_end2)

        cScale= False
        sf_ic=None
        sf_prc=None

        # IC factor
        if (ic_scal>0 and ic_r==0):
            cScale= True
        elif ic_scal==0 and ic_r>0:
            sf_ic=0.0
        else:
            if ic_r==0:
                sf_ic=1.0
            else:
                sf_ic= ic_scal/ic_r

        # PRC factor
        if (prc_scal>0 and prc_r==0):
            cScale= True
        elif prc_scal==0 and prc_r>0:
            sf_prc=0.0
        else:
            if prc_r==0:
                sf_prc=1.0
            else:
                sf_prc= prc_scal/prc_r

        scaleFactorIC[r]= sf_ic
        scaleFactorPRC[r]= sf_prc
        partialMAC[r]= isPart
        cannotScaleMAC[r]= cScale

    return scaleFactorIC, scaleFactorPRC, partialMAC, cannotScaleMAC

###############################################################################
def main():
    parser= argparse.ArgumentParser(
        description="Complete script with merges/issues, forks/stars, and MAC, producing 6 total tables."
    )
    parser.add_argument("--scaling-repo", required=True)
    parser.add_argument("--start-fy", type=int, required=True)
    parser.add_argument("--end-fy", type=int, required=True)
    parser.add_argument("--global-offset", type=int, default=0)
    parser.add_argument("--scaling-window", type=int, default=120,
                        help="Days from oldest date for merges/issues/forks/stars/MAC. Default=120.")
    args= parser.parse_args()

    scaling_repo= args.scaling_repo
    start_fy= args.start_fy
    end_fy= args.end_fy
    offset_days= args.global_offset
    window_days= args.scaling_window

    # Example repos
    all_repos= [
        "tensorflow/tensorflow",
        "facebook/react",
        "ni/labview-icon-editor"
    ]
    if scaling_repo not in all_repos:
        print(f"[ERROR] scaling-repo {scaling_repo} not recognized in {all_repos}.")
        sys.exit(1)

    # DB Connect
    config= configparser.ConfigParser()
    config.read("db_config.ini")
    db_params= config["mysql"]

    cnx= mysql.connector.connect(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        database=db_params["database"]
    )
    cursor= cnx.cursor()

    # 1) merges/issues scale factors
    sfM, sfI, partialMI, cannotScaleMI= compute_merges_issues_scale_factors(cursor, scaling_repo, all_repos, window_days)
    # 2) forks/stars scale factors
    sfF, sfS, partialUI, cannotScaleUI= compute_forks_stars_scale_factors(cursor, scaling_repo, all_repos, window_days)
    # 3) MAC scale factors
    sfIC, sfPRC, partialMAC, cannotScaleMAC= compute_mac_scale_factors(cursor, scaling_repo, all_repos, window_days)

    # Build quarter boundaries & partial logic
    def gather_quarters(st_fy, ed_fy):
        out=[]
        for fy in range(st_fy, ed_fy+1):
            qset= get_fy_quarter_boundaries(fy)
            out.extend(qset)
        out.sort(key=lambda x:x[1])
        return out

    all_quarters= gather_quarters(start_fy, end_fy)
    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        print("[WARN] No data for scaling => no tables.")
        return
    s_adjust= s_old+ timedelta(days=offset_days)

    scaling_quarters=[]
    started= False
    for (qlabel,qstart,qend) in all_quarters:
        label_fixed= qlabel.replace("\n"," ")
        is_partial= (qstart<= s_adjust<= qend)
        if not started:
            if s_adjust< qstart:
                scaling_quarters.append((label_fixed,qstart,qend,False))
                started=True
            elif is_partial:
                new_label= f"{label_fixed} (partial)"
                scaling_quarters.append((new_label,qstart,qend,False))
                started=True
            else:
                if qend< s_adjust:
                    pass
                else:
                    scaling_quarters.append((label_fixed,qstart,qend,False))
                    started=True
        else:
            now= datetime.utcnow()
            if now< qend:
                lbl2= f"{label_fixed} (partial/in-progress)"
                scaling_quarters.append((lbl2,qstart,qend,False))
            else:
                scaling_quarters.append((label_fixed,qstart,qend,False))

    n_quarters= len(scaling_quarters)
    sblocks=[]
    for (_,qs,qe,_) in scaling_quarters:
        sblocks.append((qs,qe))

    ############################################################################
    # Merges/Issues Row Map
    ############################################################################
    mi_row_map= {r:{} for r in all_repos}

    def produce_mi_row(qi, qlbl, repo_nm, st_dt, ed_dt,
                       m_raw, m_scl, m_fac,
                       i_raw, i_scl, i_fac, velocity):
        return [
          qi, qlbl, repo_nm,
          str(st_dt) if st_dt else "SKIPPED",
          str(ed_dt) if ed_dt else "SKIPPED",
          m_raw, m_scl, m_fac,
          i_raw, i_scl, i_fac,
          velocity
        ]

    def mi_format_quarter(r, merges_raw, issues_raw):
        """
        merges => scaleFactorM[r], issues => scaleFactorI[r].
        velocity= 0.4*(merged scaled)+0.6*(issues scaled).
        """
        if cannotScaleMI[r]:
            return (str(merges_raw),"cannot scale","N/A",
                    str(issues_raw),"cannot scale","N/A",
                    "N/A")
        fm= sfM[r]
        fi= sfI[r]
        if fm is None:
            M_s= "cannot scale"
            M_f= "N/A"
        else:
            mm= merges_raw*(fm if fm else 0)
            M_s= str(int(round(mm)))
            M_f= format_scale_factor_3dec(fm)
        if fi is None:
            I_s= "cannot scale"
            I_f= "N/A"
        else:
            ii= issues_raw*(fi if fi else 0)
            I_s= str(int(round(ii)))
            I_f= format_scale_factor_3dec(fi)
        if "cannot scale" in M_s or "cannot scale" in I_s:
            v_s= "N/A"
        else:
            # velocity= 0.4*M + 0.6*I
            try:
                m_i= int(M_s)
                i_i= int(I_s)
                v_calc= 0.4*m_i + 0.6*i_i
                v_s= f"{v_calc:.1f}"
            except:
                v_s= "N/A"
            v_s= v_s
        return (str(merges_raw), M_s, M_f, str(issues_raw), I_s, I_f, v_s)

    # Build quarter windows per repo
    def build_quarter_windows(r_old, count):
        arr=[]
        cur= r_old
        for _ in range(count):
            cend= cur+ relativedelta(months=3)- timedelta(seconds=1)
            arr.append((cur,cend))
            cur= cend+ timedelta(seconds=1)
        return arr

    # fill merges/issues for scaling
    for q_i,(qlabel,qs,qe,_) in enumerate(scaling_quarters, start=1):
        s_st, s_ed= sblocks[q_i-1]
        m_val, i_val= (0,0)
        if s_st and s_ed:
            m_val, i_val= get_merges_and_issues(cursor, scaling_repo, s_st, s_ed)
        if cannotScaleMI[scaling_repo]:
            row_s= produce_mi_row(
              q_i, qlabel, scaling_repo,
              s_st, s_ed,
              str(m_val),"cannot scale","N/A",
              str(i_val),"cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => scaled= raw
            vel= 0.4*m_val+ 0.6*i_val
            row_s= produce_mi_row(
              q_i, qlabel, scaling_repo,
              s_st, s_ed,
              str(m_val),str(m_val), f"{1.000:.3f}",
              str(i_val),str(i_val), f"{1.000:.3f}",
              f"{vel:.1f}"
            )
        mi_row_map[scaling_repo][q_i]= row_s

    # fill merges/issues for non-scaling
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            # no data => fill with blank
            for q_i in range(1,n_quarters+1):
                mi_row_map[r][q_i]= produce_mi_row(
                  q_i,"N/A",r,None,None,
                  "0","cannot scale","N/A",
                  "0","cannot scale","N/A","N/A"
                )
            continue
        radj= rold+ timedelta(days= offset_days)
        blocks= build_quarter_windows(radj, n_quarters)
        for q_i,(qlabel,qs,qe,_) in enumerate(scaling_quarters, start=1):
            (r_st,r_ed)= blocks[q_i-1]
            mm_val, ii_val= (0,0)
            if r_st and r_ed:
                mm_val, ii_val= get_merges_and_issues(cursor, r, r_st,r_ed)
            M_raw, M_s, M_f, I_raw, I_s, I_f, V_ = mi_format_quarter(r, mm_val, ii_val)
            row_n= produce_mi_row(
              q_i, qlabel, r, r_st, r_ed,
              M_raw, M_s, M_f,
              I_raw, I_s, I_f,
              V_
            )
            mi_row_map[r][q_i]= row_n

    ############################################################################
    # Now build the forks/stars => "User Interest Growth" row maps
    ############################################################################
    ui_row_map= {r:{} for r in all_repos}

    def produce_ui_row(qi, qlabel, repo_nm, st_dt, ed_dt,
                       f_raw, f_s, f_fac, s_raw, s_s, s_fac,
                       uig_val):
        return [
          qi, qlabel, repo_nm,
          str(st_dt) if st_dt else "SKIPPED",
          str(ed_dt) if ed_dt else "SKIPPED",
          f_raw, f_s, f_fac,
          s_raw, s_s, s_fac,
          uig_val
        ]

    def ui_format_quarter(r, f_raw, s_raw):
        """
        scale => scaleFactorF[r], scaleFactorS[r]
        UIG= 0.4*(F scaled)+0.6*(S scaled).
        """
        if cannotScaleUI[r]:
            return (str(f_raw),"cannot scale","N/A",
                    str(s_raw),"cannot scale","N/A",
                    "N/A")
        ff= sfF[r]
        fs= sfS[r]
        if ff is None:
            F_scl= "cannot scale"
            F_fac= "N/A"
        else:
            f_val= f_raw*(ff if ff else 0)
            F_scl= str(int(round(f_val)))
            F_fac= format_scale_factor_3dec(ff)
        if fs is None:
            S_scl= "cannot scale"
            S_fac= "N/A"
        else:
            s_val= s_raw*(fs if fs else 0)
            S_scl= str(int(round(s_val)))
            S_fac= format_scale_factor_3dec(fs)
        if "cannot scale" in F_scl or "cannot scale" in S_scl:
            uig_s= "N/A"
        else:
            try:
                f_i= int(F_scl)
                s_i= int(S_scl)
                uig_val= 0.4*f_i + 0.6*s_i
                uig_s= f"{uig_val:.1f}"
            except:
                uig_s= "N/A"
        return (str(f_raw), F_scl, F_fac, str(s_raw), S_scl, S_fac, uig_s)

    # fill forks/stars for scaling
    for q_i,(qlabel,qs,qe,_) in enumerate(scaling_quarters, start=1):
        (s_st,s_ed)= sblocks[q_i-1]
        f_val, s_val= (0,0)
        if s_st and s_ed:
            f_val, s_val= get_forks_and_stars(cursor, scaling_repo, s_st,s_ed)
        if cannotScaleUI[scaling_repo]:
            row_ui= produce_ui_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(f_val),"cannot scale","N/A",
              str(s_val),"cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => scaled= raw
            uigv= 0.4*f_val + 0.6*s_val
            row_ui= produce_ui_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(f_val),str(f_val),f"{1.000:.3f}",
              str(s_val),str(s_val),f"{1.000:.3f}",
              f"{uigv:.1f}"
            )
        ui_row_map[scaling_repo][q_i]= row_ui

    # fill forks/stars for non-scaling
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            # fill can't scale
            for q_i in range(1,n_quarters+1):
                ui_row_map[r][q_i]= produce_ui_row(
                  q_i,"N/A",r,None,None,
                  "0","cannot scale","N/A",
                  "0","cannot scale","N/A",
                  "N/A"
                )
            continue
        radj= rold+ timedelta(days=offset_days)
        blocks= build_quarter_windows(radj, n_quarters)
        for q_i,(qlabel,qs,qe,_) in enumerate(scaling_quarters, start=1):
            (r_st,r_ed)= blocks[q_i-1]
            ff_val, ss_val= (0,0)
            if r_st and r_ed:
                ff_val, ss_val= get_forks_and_stars(cursor, r, r_st,r_ed)
            F_raw, F_s, F_fac, S_raw, S_s, S_fac, UIG_ = ui_format_quarter(r, ff_val, ss_val)
            row_ui= produce_ui_row(
              q_i, qlabel, r, r_st,r_ed,
              F_raw, F_s, F_fac,
              S_raw, S_s, S_fac,
              UIG_
            )
            ui_row_map[r][q_i]= row_ui

    ############################################################################
    # 5) MAC => 0.8*(IC scaled) + 2*(PRC scaled)
    ############################################################################
    mac_row_map= {r:{} for r in all_repos}

    def get_IC_PRC(cursor, repo, start_dt, end_dt):
        """
        Return (IC_raw, PRC_raw), 
         where IC= new issues + new comments,
               PRC= new pull requests created.
        """
        if not start_dt or not end_dt:
            return (0,0)
        iss,com= get_issues_and_comments(cursor, repo, start_dt, end_dt)
        ic_val= iss+com
        prc_val= get_pull_requests_created(cursor, repo, start_dt, end_dt)
        return (ic_val, prc_val)

    def produce_mac_row(qi, qlabel, repo_nm, st_dt, ed_dt,
                        ic_raw, ic_s, ic_fac,
                        prc_raw, prc_s, prc_fac,
                        mac_val):
        return [
          qi, qlabel, repo_nm,
          str(st_dt) if st_dt else "SKIPPED",
          str(ed_dt) if ed_dt else "SKIPPED",
          ic_raw, ic_s, ic_fac,
          prc_raw, prc_s, prc_fac,
          mac_val
        ]

    def mac_format_quarter(r, ic_raw, prc_raw):
        """
        scale => sfIC[r], sfPRC[r].
        MAC= 0.8*(IC scaled)+2*(PRC scaled).
        """
        if cannotScaleMAC[r]:
            return (str(ic_raw),"cannot scale","N/A",
                    str(prc_raw),"cannot scale","N/A",
                    "N/A")
        fic= sfIC[r]
        fprc= sfPRC[r]
        if fic is None:
            ic_scl= "cannot scale"
            ic_fac= "N/A"
        else:
            ic_val= ic_raw*(fic if fic else 0)
            ic_scl= str(int(round(ic_val)))
            ic_fac= format_scale_factor_3dec(fic)
        if fprc is None:
            prc_scl= "cannot scale"
            prc_fac= "N/A"
        else:
            prc_val= prc_raw*(fprc if fprc else 0)
            prc_scl= str(int(round(prc_val)))
            prc_fac= format_scale_factor_3dec(fprc)
        if "cannot scale" in ic_scl or "cannot scale" in prc_scl:
            mac_s= "N/A"
        else:
            try:
                ic_i= int(ic_scl)
                prc_i= int(prc_scl)
                mac_val= 0.8*ic_i + 2.0*prc_i
                mac_s= f"{mac_val:.1f}"
            except:
                mac_s= "N/A"
        return (str(ic_raw), ic_scl, ic_fac, str(prc_raw), prc_scl, prc_fac, mac_s)

    # compute scale factors for MAC
    sfIC, sfPRC, partialMAC, cannotScaleMAC= compute_mac_scale_factors(cursor, scaling_repo, all_repos, window_days)

    # fill MAC data => scaling
    for q_i,(qlabel,qs,qe,_) in enumerate(scaling_quarters, start=1):
        (s_st,s_ed)= sblocks[q_i-1]
        ic_val, prc_val= (0,0)
        if s_st and s_ed:
            ic_val, prc_val= get_IC_PRC(cursor, scaling_repo, s_st,s_ed)
        if cannotScaleMAC[scaling_repo]:
            row_m= produce_mac_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(ic_val),"cannot scale","N/A",
              str(prc_val),"cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => scaled= raw
            mac_v= 0.8*ic_val + 2.0*prc_val
            row_m= produce_mac_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(ic_val), str(ic_val), f"{1.000:.3f}",
              str(prc_val), str(prc_val), f"{1.000:.3f}",
              f"{mac_v:.1f}"
            )
        mac_row_map[scaling_repo][q_i]= row_m

    # fill MAC data => non-scaling
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            # no data => fill can't scale
            for q_i in range(1,n_quarters+1):
                mac_row_map[r][q_i]= produce_mac_row(
                  q_i,"N/A",r,None,None,
                  "0","cannot scale","N/A",
                  "0","cannot scale","N/A",
                  "N/A"
                )
            continue
        radj= rold+ timedelta(days=offset_days)
        blocks= build_quarter_windows(radj, n_quarters)
        for q_i,(qlabel,qs,qe,_) in enumerate(scaling_quarters, start=1):
            (r_st,r_ed)= blocks[q_i-1]
            ic_v, prc_v= (0,0)
            if r_st and r_ed:
                ic_v, prc_v= get_IC_PRC(cursor, r, r_st,r_ed)
            (IC_raw, IC_scl, IC_fac, PRC_raw, PRC_scl, PRC_fac, MAC_)= mac_format_quarter(r, ic_v, prc_v)
            row_m= produce_mac_row(
              q_i, qlabel, r, r_st,r_ed,
              IC_raw, IC_scl, IC_fac,
              PRC_raw, PRC_scl, PRC_fac,
              MAC_
            )
            mac_row_map[r][q_i]= row_m

    ############################################################################
    # Now we produce 6 total tables:
    # 1) merges/issues main
    # 2) merges/issues comparison
    # 3) forks/stars main
    # 4) forks/stars comparison
    # 5) MAC main
    # 6) MAC comparison
    ############################################################################

    def compute_widths(table):
        w=[]
        for col_i in range(len(table[0])):
            mx=0
            for row in table:
                cell=str(row[col_i])
                if len(cell)>mx:
                    mx=len(cell)
            w.append(mx+2)
        return w

    def center_line(vals, widths):
        out=[]
        for v,wd in zip(vals,widths):
            out.append(str(v).center(wd))
        return " | ".join(out)

    ################### 1) Merges/Issues Main ###################
    mi_main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "M-raw","M","M-fact","I-raw","I","I-fact","V"
    ]
    mi_final_rows=[]
    for rr in [scaling_repo]+ [r for r in all_repos if r!=scaling_repo]:
        for q_i in range(1,n_quarters+1):
            if q_i in mi_row_map[rr]:
                mi_final_rows.append(mi_row_map[rr][q_i])

    comb_mi_main= [mi_main_header]+ mi_final_rows
    mi_main_widths= compute_widths(comb_mi_main)

    print(f"\n=== (1) MERGES/ISSUES MAIN TABLE (Window={window_days} days) ===\n")
    print(center_line(mi_main_header, mi_main_widths))
    print("-+-".join("-"*w for w in mi_main_widths))
    for row in mi_final_rows:
        print(center_line(row, mi_main_widths))

    ################### 2) Merges/Issues Comparison ###################
    mi_comp_header= [
      "QIdx","QuarterLabel",
      "M-target","M-scaling","M%Target",
      "I-target","I-scaling","I%Target",
      "V-target","V-scaling","V%Target"
    ]
    mi_comp_rows=[]
    M_raw_idx=5
    I_raw_idx=8
    V_idx=11

    def parse_int(s):
        try: return int(s)
        except: return None
    def parse_float(s):
        try: return float(s)
        except: return None
    def ratio_str(val,avg):
        if avg>0 and val is not None:
            return f"{(100.0*val/avg):.1f}%"
        elif avg==0 and val==0:
            return "100.0%"
        return "N/A"

    for q_i,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
        ns_count=0
        sum_m=0
        sum_i=0
        sum_v=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_i in mi_row_map[r]:
                rowd= mi_row_map[r][q_i]
                m_str= rowd[M_raw_idx]
                i_str= rowd[I_raw_idx]
                v_str= rowd[V_idx]
                mm= parse_int(m_str)
                ii= parse_int(i_str)
                vv= parse_float(v_str)
                if mm is not None and ii is not None and vv is not None:
                    ns_count+=1
                    sum_m+= mm
                    sum_i+= ii
                    sum_v+= vv
        if ns_count>0:
            m_avg= sum_m/ns_count
            i_avg= sum_i/ns_count
            v_avg= sum_v/ns_count
        else:
            m_avg=0
            i_avg=0
            v_avg=0

        if q_i not in mi_row_map[scaling_repo]:
            continue
        sc_row= mi_row_map[scaling_repo][q_i]
        sc_m_str= sc_row[M_raw_idx]
        sc_i_str= sc_row[I_raw_idx]
        sc_v_str= sc_row[V_idx]

        sc_m= parse_int(sc_m_str)
        sc_i= parse_int(sc_i_str)
        sc_v= parse_float(sc_v_str)

        row2= [
          q_i,
          qlabel,
          f"{m_avg:.1f}",
          sc_m_str,
          ratio_str(sc_m, m_avg),
          f"{i_avg:.1f}",
          sc_i_str,
          ratio_str(sc_i, i_avg),
          f"{v_avg:.1f}",
          sc_v_str if sc_v_str else "N/A",
          ratio_str(sc_v, v_avg)
        ]
        mi_comp_rows.append(row2)

    comb_mi_comp= [mi_comp_header]+ mi_comp_rows
    mi_comp_widths= compute_widths(comb_mi_comp)

    print("\n=== (2) MERGES/ISSUES COMPARISON TABLE ===\n")
    print(center_line(mi_comp_header, mi_comp_widths))
    print("-+-".join("-"*w for w in mi_comp_widths))
    for row in mi_comp_rows:
        print(center_line(row, mi_comp_widths))

    ################### 3) Forks/Stars (UIG) Main ###################
    ui_main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "F-raw","F","F-fact","S-raw","S","S-fact","UIG"
    ]
    ui_final_rows=[]
    for rr in [scaling_repo]+ [r for r in all_repos if r!=scaling_repo]:
        for q_i in range(1,n_quarters+1):
            if q_i in ui_row_map[rr]:
                ui_final_rows.append(ui_row_map[rr][q_i])

    comb_ui_main= [ui_main_header]+ ui_final_rows
    ui_main_widths= compute_widths(comb_ui_main)

    print(f"\n=== (3) FORKS/STARS (UIG) MAIN TABLE (Window={window_days} days) ===\n")
    print(center_line(ui_main_header, ui_main_widths))
    print("-+-".join("-"*w for w in ui_main_widths))
    for row in ui_final_rows:
        print(center_line(row, ui_main_widths))

    ################### 4) Forks/Stars (UIG) Comparison ###################
    ui_comp_header= [
      "QIdx","QuarterLabel",
      "F-target","F-scaling","F%Target",
      "S-target","S-scaling","S%Target",
      "UIG-target","UIG-scaling","UIG%Target"
    ]
    ui_comp_rows=[]
    F_raw_idx=5
    S_raw_idx=8
    UIG_idx=11

    for q_i,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
        ns_count=0
        sum_f=0
        sum_s=0
        sum_uig=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_i in ui_row_map[r]:
                rowd= ui_row_map[r][q_i]
                f_str= rowd[F_raw_idx]
                s_str= rowd[S_raw_idx]
                uig_str= rowd[UIG_idx]
                ff= parse_int(f_str)
                ss= parse_int(s_str)
                uu= parse_float(uig_str)
                if ff is not None and ss is not None and uu is not None:
                    ns_count+=1
                    sum_f+= ff
                    sum_s+= ss
                    sum_uig+= uu
        if ns_count>0:
            f_avg= sum_f/ns_count
            s_avg= sum_s/ns_count
            uig_avg= sum_uig/ns_count
        else:
            f_avg=0
            s_avg=0
            uig_avg=0

        if q_i not in ui_row_map[scaling_repo]:
            continue
        sc_row= ui_row_map[scaling_repo][q_i]
        sc_f_str= sc_row[F_raw_idx]
        sc_s_str= sc_row[S_raw_idx]
        sc_uig_str= sc_row[UIG_idx]

        sc_f= parse_int(sc_f_str)
        sc_s= parse_int(sc_s_str)
        sc_uig= parse_float(sc_uig_str)

        row_ui2= [
          q_i,
          qlabel,
          f"{f_avg:.1f}",
          sc_f_str,
          ratio_str(sc_f, f_avg),
          f"{s_avg:.1f}",
          sc_s_str,
          ratio_str(sc_s, s_avg),
          f"{uig_avg:.1f}",
          sc_uig_str if sc_uig_str else "N/A",
          ratio_str(sc_uig, uig_avg)
        ]
        ui_comp_rows.append(row_ui2)

    comb_ui_comp= [ui_comp_header]+ ui_comp_rows
    ui_comp_widths= compute_widths(comb_ui_comp)

    print("\n=== (4) FORKS/STARS (UIG) COMPARISON TABLE ===\n")
    print(center_line(ui_comp_header, ui_comp_widths))
    print("-+-".join("-"*w for w in ui_comp_widths))
    for row in ui_comp_rows:
        print(center_line(row, ui_comp_widths))

    ################### 5) MAC Main ###################
    mac_main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "IC-raw","IC","IC-fact","PRC-raw","PRC","PRC-fact","MAC"
    ]
    mac_final_rows=[]
    for rr in [scaling_repo]+ [r for r in all_repos if r!=scaling_repo]:
        for q_i in range(1,n_quarters+1):
            if q_i in mac_row_map[rr]:
                mac_final_rows.append(mac_row_map[rr][q_i])

    comb_mac_main= [mac_main_header]+ mac_final_rows
    mac_main_widths= compute_widths(comb_mac_main)

    print(f"\n=== (5) MAC MAIN TABLE (Window={window_days} days) ===\n")
    print(center_line(mac_main_header, mac_main_widths))
    print("-+-".join("-"*w for w in mac_main_widths))
    for row in mac_final_rows:
        print(center_line(row, mac_main_widths))

    ################### 6) MAC Comparison ###################
    mac_comp_header= [
      "QIdx","QuarterLabel",
      "IC-target","IC-scaling","IC%Target",
      "PRC-target","PRC-scaling","PRC%Target",
      "MAC-target","MAC-scaling","MAC%Target"
    ]
    mac_comp_rows=[]
    IC_raw_idx=5
    PRC_raw_idx=8
    MAC_idx=11

    for q_i,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
        ns_count=0
        sum_ic=0
        sum_prc=0
        sum_mac=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_i in mac_row_map[r]:
                rowm= mac_row_map[r][q_i]
                ic_str= rowm[IC_raw_idx]
                prc_str= rowm[PRC_raw_idx]
                mac_str= rowm[MAC_idx]
                ic_i= parse_int(ic_str)
                prc_i= parse_int(prc_str)
                mac_f= parse_float(mac_str)
                if ic_i is not None and prc_i is not None and mac_f is not None:
                    ns_count+=1
                    sum_ic+= ic_i
                    sum_prc+= prc_i
                    sum_mac+= mac_f
        if ns_count>0:
            ic_avg= sum_ic/ns_count
            prc_avg= sum_prc/ns_count
            mac_avg= sum_mac/ns_count
        else:
            ic_avg=0
            prc_avg=0
            mac_avg=0

        if q_i not in mac_row_map[scaling_repo]:
            continue
        sc_row= mac_row_map[scaling_repo][q_i]
        sc_ic_str= sc_row[IC_raw_idx]
        sc_prc_str= sc_row[PRC_raw_idx]
        sc_mac_str= sc_row[MAC_idx]

        sc_ic= parse_int(sc_ic_str)
        sc_prc= parse_int(sc_prc_str)
        sc_mac= parse_float(sc_mac_str)

        def ratio_str2(val, avg):
            if avg>0 and val is not None:
                return f"{(100.0* val/avg):.1f}%"
            elif avg==0 and val==0:
                return "100.0%"
            return "N/A"

        row_c= [
          q_i,
          qlabel,
          f"{ic_avg:.1f}",
          sc_ic_str,
          ratio_str2(sc_ic, ic_avg),
          f"{prc_avg:.1f}",
          sc_prc_str,
          ratio_str2(sc_prc, prc_avg),
          f"{mac_avg:.1f}",
          sc_mac_str if sc_mac_str else "N/A",
          ratio_str2(sc_mac, mac_avg)
        ]
        mac_comp_rows.append(row_c)

    comb_mac_comp= [mac_comp_header]+ mac_comp_rows
    mac_comp_widths= compute_widths(comb_mac_comp)

    print("\n=== (6) MAC COMPARISON TABLE ===\n")
    print(center_line(mac_comp_header, mac_comp_widths))
    print("-+-".join("-"*w for w in mac_comp_widths))
    for row in mac_comp_rows:
        print(center_line(row, mac_comp_widths))

    print("\n=== Done. ===")

if __name__=="__main__":
    main()

