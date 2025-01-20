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
# DB & Quarter Helpers
###############################################################################

def get_fy_quarter_boundaries(fy):
    q1_label = f"Q1\nFY{fy}"
    q1_start = datetime(fy - 1, 10, 1)
    q1_end   = datetime(fy - 1, 12, 31, 23, 59, 59)

    q2_label = f"Q2\nFY{fy}"
    q2_start = datetime(fy, 1, 1)
    q2_end   = datetime(fy, 3, 31, 23, 59, 59)

    q3_label = f"Q3\nFY{fy}"
    q3_start = datetime(fy, 4, 1)
    q3_end   = datetime(fy, 6, 30, 23, 59, 59)

    q4_label = f"Q4\nFY{fy}"
    q4_start = datetime(fy, 7, 1)
    q4_end   = datetime(fy, 9, 30, 23, 59, 59)

    return [
        (q1_label, q1_start, q1_end),
        (q2_label, q2_start, q2_end),
        (q3_label, q3_start, q3_end),
        (q4_label, q4_start, q4_end)
    ]

def get_oldest_date(cursor, repo_name):
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

def get_merges_and_issues(cursor, repo, start_dt, end_dt):
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

def format_scale_factor_3dec(f):
    if f is None:
        return "N/A"
    return f"{f:.3f}"

###############################################################################
# Merges/Issues scale factor
###############################################################################
def compute_merges_issues_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    Return scaleFactorM, scaleFactorI, partialRepo, cannotScale.
    - scaleFactorM[r]: merges factor
    - scaleFactorI[r]: issues factor
    - partialRepo[r]: True if partial
    - cannotScale[r]: True if scaling>0 but repo=0
    """
    scaleFactorM={}
    scaleFactorI={}
    partialRepo={}
    cannotScale={}

    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        for r in all_repos:
            scaleFactorM[r]=1.0
            scaleFactorI[r]=1.0
            partialRepo[r]=False
            cannotScale[r]=False
        return scaleFactorM, scaleFactorI, partialRepo, cannotScale

    s_end= s_old+ timedelta(days=window_days)
    s_last= get_last_date(cursor, scaling_repo)
    partial_s= False
    use_end= s_end
    if s_last and s_last< s_end:
        partial_s= True
        use_end= s_last

    M_sSum,I_sSum= get_merges_and_issues(cursor, scaling_repo, s_old, use_end)
    scaleFactorM[scaling_repo]=1.0
    scaleFactorI[scaling_repo]=1.0
    partialRepo[scaling_repo]= partial_s
    cannotScale[scaling_repo]=False

    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            scaleFactorM[r]=None
            scaleFactorI[r]=None
            partialRepo[r]=False
            cannotScale[r]=True
            continue
        rend= rold+ timedelta(days=window_days)
        rlast= get_last_date(cursor, r)
        isPartial= False
        used_end= rend
        if rlast and rlast< rend:
            isPartial= True
            used_end= rlast
        MrSum,IrSum= get_merges_and_issues(cursor, r, rold, used_end)

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
# Forks/Stars scale factor for User Interest Growth
###############################################################################
def compute_forks_stars_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    Return scaleFactorF, scaleFactorS, partialRepoUI, cannotScaleUI
    for forks/stars. Same logic as merges/issues:

    scaleFactorF[r], scaleFactorS[r]
    partialRepoUI[r] => True if partial
    cannotScaleUI[r] => True if scaling>0 but repo=0, etc.
    """
    scaleFactorF={}
    scaleFactorS={}
    partialRepoUI={}
    cannotScaleUI={}

    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        for r in all_repos:
            scaleFactorF[r]=1.0
            scaleFactorS[r]=1.0
            partialRepoUI[r]=False
            cannotScaleUI[r]=False
        return scaleFactorF, scaleFactorS, partialRepoUI, cannotScaleUI

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
    partialRepoUI[scaling_repo]= partial_s
    cannotScaleUI[scaling_repo]= False

    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            scaleFactorF[r]=None
            scaleFactorS[r]=None
            partialRepoUI[r]=False
            cannotScaleUI[r]=True
            continue
        rend= rold+ timedelta(days=window_days)
        rlast= get_last_date(cursor, r)
        isPartial= False
        used_end2= rend
        if rlast and rlast< rend:
            isPartial= True
            used_end2= rlast
        F_rSum, S_rSum= get_forks_and_stars(cursor, r, rold, used_end2)

        cScale=False
        sff=None
        sfs=None

        # forks factor
        if (F_sSum>0 and F_rSum==0):
            cScale=True
        elif F_sSum==0 and F_rSum>0:
            sff=0.0
        else:
            if F_rSum==0:
                sff=1.0
            else:
                sff= F_sSum/F_rSum

        # stars factor
        if (S_sSum>0 and S_rSum==0):
            cScale=True
        elif S_sSum==0 and S_rSum>0:
            sfs=0.0
        else:
            if S_rSum==0:
                sfs=1.0
            else:
                sfs= S_sSum/S_rSum

        scaleFactorF[r]= sff
        scaleFactorS[r]= sfs
        partialRepoUI[r]= isPartial
        cannotScaleUI[r]= cScale

    return scaleFactorF, scaleFactorS, partialRepoUI, cannotScaleUI

###############################################################################
def main():
    parser= argparse.ArgumentParser(
        description="""Quarter-based velocity with merges/issues and now forks/stars for user interest growth,
with separate main & comparison tables for each."""
    )
    parser.add_argument("--scaling-repo", required=True)
    parser.add_argument("--start-fy", type=int, required=True)
    parser.add_argument("--end-fy", type=int, required=True)
    parser.add_argument("--global-offset", type=int, default=0)
    parser.add_argument("--scaling-window", type=int, default=120,
                        help="Days to sum merges/issues/forks/stars from oldest date. Default=120.")
    args= parser.parse_args()

    scaling_repo= args.scaling_repo
    start_fy= args.start_fy
    end_fy= args.end_fy
    offset_days= args.global_offset
    window_days= args.scaling_window

    all_repos= [
        "tensorflow/tensorflow",
        "facebook/react",
        "ni/labview-icon-editor"
    ]
    if scaling_repo not in all_repos:
        print(f"[ERROR] scaling-repo {scaling_repo} not recognized in {all_repos}.")
        sys.exit(1)

    # DB
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
    sfM, sfI, partialRepoMI, cannotScaleMI= compute_merges_issues_scale_factors(
        cursor, scaling_repo, all_repos, window_days
    )
    # 2) forks/stars scale factors
    sfF, sfS, partialRepoUI, cannotScaleUI= compute_forks_stars_scale_factors(
        cursor, scaling_repo, all_repos, window_days
    )

    # Build quarter definitions
    def gather_quarters(st_fy, ed_fy):
        arr=[]
        for fy in range(st_fy, ed_fy+1):
            blocks= get_fy_quarter_boundaries(fy)
            arr.extend(blocks)
        arr.sort(key=lambda x:x[1])
        return arr

    all_quarters= gather_quarters(start_fy, end_fy)

    # offset
    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        print("[WARN] scaling repo has no data => no merges/issues/forks/stars table.")
        return
    s_adjust= s_old+ timedelta(days= offset_days)

    # same partial logic as your merges/issues approach
    scaling_quarters=[]
    started=False
    skip_label="(partial)"
    for (q_label, q_start, q_end) in all_quarters:
        lbl_fixed= q_label.replace("\n"," ")
        is_partial= (q_start <= s_adjust <= q_end)
        if not started:
            if s_adjust< q_start:
                scaling_quarters.append((lbl_fixed, q_start,q_end,False))
                started= True
            elif is_partial:
                new_label= f"{lbl_fixed} {skip_label}"
                scaling_quarters.append((new_label,q_start,q_end,False))
                started= True
            else:
                if q_end< s_adjust:
                    pass
                else:
                    scaling_quarters.append((lbl_fixed, q_start,q_end,False))
                    started= True
        else:
            now= datetime.utcnow()
            if now< q_end:
                new_label= f"{lbl_fixed} (partial/in-progress)"
                scaling_quarters.append((new_label,q_start,q_end,False))
            else:
                scaling_quarters.append((lbl_fixed,q_start,q_end,False))

    sblocks=[]
    for (_,qs,qe,_) in scaling_quarters:
        sblocks.append((qs,qe))
    n_quarters= len(scaling_quarters)

    def build_quarter_windows(st,n):
        arr=[]
        cur= st
        for _ in range(n):
            cend= cur+ relativedelta(months=3)- timedelta(seconds=1)
            arr.append((cur,cend))
            cur= cend+ timedelta(seconds=1)
        return arr

    # merges/issues row map
    mi_row_map= {r:{} for r in all_repos}
    # forks/stars row map
    ui_row_map= {r:{} for r in all_repos}

    # build non-scaling blocks
    non_scaling_data_merges={}
    non_scaling_data_uig={}
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            non_scaling_data_merges[r]= [(None,None)]* n_quarters
            non_scaling_data_uig[r]= [(None,None)]* n_quarters
            continue
        radj= rold+ timedelta(days= offset_days)
        blocks= build_quarter_windows(radj,n_quarters)
        # same blocks for merges/issues or forks/stars
        non_scaling_data_merges[r]= blocks
        non_scaling_data_uig[r]= blocks

    # merges/issues for scaling
    def produce_mi_row(qi, qlabel, repo_nm, sdt, edt,
                       m_raw, m_scl, m_fac, i_raw, i_scl, i_fac, vel):
        return [
          qi, qlabel, repo_nm,
          str(sdt) if sdt else "SKIPPED",
          str(edt) if edt else "SKIPPED",
          m_raw, m_scl, m_fac,
          i_raw, i_scl, i_fac,
          vel
        ]

    def mi_format_quarter(r, merges_raw, issues_raw):
        # merges => scaleFactorM, issues => scaleFactorI
        # partial => partialRepoMI, cannotScale => cannotScaleMI
        if cannotScaleMI[r]:
            return (str(merges_raw),"cannot scale","N/A",
                    str(issues_raw),"cannot scale","N/A","N/A")
        fm= sfM[r]
        fi= sfI[r]
        if fm is None:
            M_scl= "cannot scale"
            M_fac= "N/A"
        else:
            mm= merges_raw*(fm if fm else 0)
            M_scl= str(int(round(mm)))
            M_fac= format_scale_factor_3dec(fm)
        if fi is None:
            I_scl= "cannot scale"
            I_fac= "N/A"
        else:
            ii= issues_raw*(fi if fi else 0)
            I_scl= str(int(round(ii)))
            I_fac= format_scale_factor_3dec(fi)
        if "cannot scale" in M_scl or "cannot scale" in I_scl:
            v_s= "N/A"
        else:
            vcalc= 0.4*(merges_raw*(fm if fm else 0)) + 0.6*(issues_raw*(fi if fi else 0))
            v_s= f"{vcalc:.1f}"
        return (str(merges_raw), M_scl, M_fac, str(issues_raw), I_scl, I_fac, v_s)

    # forks/stars for user interest
    def produce_ui_row(qi, qlabel, repo_nm, sdt, edt,
                       f_raw, f_scl, f_fac, s_raw, s_scl, s_fac, uig):
        return [
          qi, qlabel, repo_nm,
          str(sdt) if sdt else "SKIPPED",
          str(edt) if edt else "SKIPPED",
          f_raw, f_scl, f_fac,
          s_raw, s_scl, s_fac,
          uig
        ]

    def ui_format_quarter(r, forks_raw, stars_raw):
        # forks => scaleFactorF, stars => scaleFactorS
        # partial => partialRepoUI, cannotScale => cannotScaleUI
        if cannotScaleUI[r]:
            return (str(forks_raw),"cannot scale","N/A",
                    str(stars_raw),"cannot scale","N/A","N/A")
        ff= sfF[r]
        fs= sfS[r]
        if ff is None:
            F_scl= "cannot scale"
            F_fac= "N/A"
        else:
            fval= forks_raw*(ff if ff else 0)
            F_scl= str(int(round(fval)))
            F_fac= format_scale_factor_3dec(ff)
        if fs is None:
            S_scl= "cannot scale"
            S_fac= "N/A"
        else:
            sval= stars_raw*(fs if fs else 0)
            S_scl= str(int(round(sval)))
            S_fac= format_scale_factor_3dec(fs)
        # UIG => 0.4*F + 0.6*S (scaled)
        if "cannot scale" in F_scl or "cannot scale" in S_scl:
            uig_s= "N/A"
        else:
            # parse the int from F_scl, S_scl
            try:
                f_int= int(F_scl)
                s_int= int(S_scl)
                uig_val= 0.4*f_int + 0.6*s_int
                uig_s= f"{uig_val:.1f}"
            except:
                uig_s= "N/A"
        return (str(forks_raw), F_scl, F_fac, str(stars_raw), S_scl, S_fac, uig_s)

    # Fill merges/issues for scaling
    for q_i,(qlabel,qstart,qend,xx) in enumerate(scaling_quarters, start=1):
        (s_st,s_ed)= sblocks[q_i-1]
        m_val, i_val= (0,0)
        if s_st and s_ed:
            m_val, i_val= get_merges_and_issues(cursor, scaling_repo, s_st,s_ed)
        if cannotScaleMI[scaling_repo]:
            row_s= produce_mi_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(m_val),"cannot scale","N/A",
              str(i_val),"cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => scaled=raw
            vel= 0.4*m_val + 0.6*i_val
            row_s= produce_mi_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(m_val), str(m_val), f"{1.000:.3f}",
              str(i_val), str(i_val), f"{1.000:.3f}",
              f"{vel:.1f}"
            )
        mi_row_map[scaling_repo][q_i]= row_s

    # Fill merges/issues for non-scaling
    for r in all_repos:
        if r==scaling_repo:
            continue
        blocks= build_quarter_windows(get_oldest_date(cursor, r)+ timedelta(days=offset_days), n_quarters)
        for q_i,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
            (r_st,r_ed)= blocks[q_i-1]
            mm_val, ii_val= (0,0)
            if r_st and r_ed:
                mm_val, ii_val= get_merges_and_issues(cursor, r, r_st,r_ed)
            # format
            M_raw, M_scl, M_fac, I_raw, I_scl, I_fac, V_ = mi_format_quarter(r, mm_val, ii_val)
            row_n= produce_mi_row(
              q_i, qlabel, r, r_st,r_ed,
              M_raw, M_scl, M_fac,
              I_raw, I_scl, I_fac,
              V_
            )
            mi_row_map[r][q_i]= row_n

    # Fill forks/stars for scaling
    for q_i,(qlabel,qstart,qend,xx) in enumerate(scaling_quarters, start=1):
        (s_st,s_ed)= sblocks[q_i-1]
        f_val, s_val= (0,0)
        if s_st and s_ed:
            f_val, s_val= get_forks_and_stars(cursor, scaling_repo, s_st, s_ed)
        if cannotScaleUI[scaling_repo]:
            row_ui= produce_ui_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(f_val), "cannot scale","N/A",
              str(s_val), "cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => scaled= raw
            uig= 0.4*f_val + 0.6*s_val
            row_ui= produce_ui_row(
              q_i, qlabel, scaling_repo, s_st,s_ed,
              str(f_val), str(f_val), f"{1.000:.3f}",
              str(s_val), str(s_val), f"{1.000:.3f}",
              f"{uig:.1f}"
            )
        ui_row_map[scaling_repo][q_i]= row_ui

    # Fill forks/stars for non-scaling
    for r in all_repos:
        if r==scaling_repo:
            continue
        blocks= build_quarter_windows(get_oldest_date(cursor, r)+ timedelta(days=offset_days), n_quarters)
        for q_i,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
            (r_st,r_ed)= blocks[q_i-1]
            ff_val, ss_val= (0,0)
            if r_st and r_ed:
                ff_val, ss_val= get_forks_and_stars(cursor, r, r_st,r_ed)
            F_raw, F_scl, F_fac, S_raw, S_scl, S_fac, UIG_ = ui_format_quarter(r, ff_val, ss_val)
            row_ui= produce_ui_row(
              q_i, qlabel, r, r_st, r_ed,
              F_raw, F_scl, F_fac,
              S_raw, S_scl, S_fac,
              UIG_
            )
            ui_row_map[r][q_i]= row_ui

    cursor.close()
    cnx.close()

    # ==================== Print Merges/Issues Main Table ====================
    def compute_widths(table):
        widths=[]
        for col_i in range(len(table[0])):
            mx=0
            for row in table:
                cell=str(row[col_i])
                if len(cell)>mx:
                    mx=len(cell)
            widths.append(mx+2)
        return widths

    def center_line(vals, widths):
        out=[]
        for v,w in zip(vals,widths):
            out.append(str(v).center(w))
        return " | ".join(out)

    mi_main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "M-raw","M","M-fact","I-raw","I","I-fact","V"
    ]
    mi_final_rows=[]
    # order => scaling first
    for rr in [scaling_repo]+[r for r in all_repos if r!=scaling_repo]:
        for q_i in range(1,n_quarters+1):
            if q_i in mi_row_map[rr]:
                mi_final_rows.append(mi_row_map[rr][q_i])

    comb_mi= [mi_main_header]+ mi_final_rows
    mi_widths= compute_widths(comb_mi)

    print(f"\n=== MERGES/ISSUES MAIN TABLE (Window={window_days} days) ===\n")
    print(center_line(mi_main_header, mi_widths))
    print("-+-".join("-"*w for w in mi_widths))
    for row in mi_final_rows:
        print(center_line(row, mi_widths))

    # ================= Merges/Issues Comparison ===========================
    mi_second_header= [
      "QIdx","QuarterLabel",
      "M-target","M-scaling","M%Target",
      "I-target","I-scaling","I%Target",
      "V-target","V-scaling","V%Target"
    ]
    mi_second_rows=[]
    M_raw_idx=5
    I_raw_idx=8
    V_idx=11

    def parse_int(s):
        try: return int(s)
        except: return None
    def parse_float(s):
        try: return float(s)
        except: return None
    def ratio_str(val, avg):
        if avg>0 and val is not None:
            return f"{(100.0*val/avg):.1f}%"
        elif avg==0 and val==0:
            return "100.0%"
        return "N/A"

    for q_idx,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
        # gather non-scaling raw merges/issues => average
        ns_count=0
        sum_m=0
        sum_i=0
        sum_v=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_idx in mi_row_map[r]:
                rowd= mi_row_map[r][q_idx]
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
        # scaling row
        if q_idx not in mi_row_map[scaling_repo]:
            continue
        sc_row= mi_row_map[scaling_repo][q_idx]
        sc_m_str= sc_row[M_raw_idx]
        sc_i_str= sc_row[I_raw_idx]
        sc_v_str= sc_row[V_idx]
        sc_m= parse_int(sc_m_str)
        sc_i= parse_int(sc_i_str)
        sc_v= parse_float(sc_v_str)

        row2= [
          q_idx,
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
        mi_second_rows.append(row2)

    comb_mi2= [mi_second_header]+ mi_second_rows
    mi2_widths= compute_widths(comb_mi2)

    print("\n=== MERGES/ISSUES COMPARISON TABLE (Raw) ===\n")
    print(center_line(mi_second_header, mi2_widths))
    print("-+-".join("-"*w for w in mi2_widths))
    for row in mi_second_rows:
        print(center_line(row, mi2_widths))

    # ================== FORKS/STARS (UIG) MAIN TABLE ======================
    ui_main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "F-raw","F","F-fact","S-raw","S","S-fact","UIG"
    ]
    ui_final_rows=[]
    for rr in [scaling_repo]+[r for r in all_repos if r!=scaling_repo]:
        for q_i in range(1,n_quarters+1):
            if q_i in ui_row_map[rr]:
                ui_final_rows.append(ui_row_map[rr][q_i])

    comb_ui= [ui_main_header]+ ui_final_rows
    ui_widths= compute_widths(comb_ui)

    print(f"\n=== USER INTEREST GROWTH (FORKS/STARS) MAIN TABLE (Window={window_days} days) ===\n")
    print(center_line(ui_main_header, ui_widths))
    print("-+-".join("-"*w for w in ui_widths))
    for row in ui_final_rows:
        print(center_line(row, ui_widths))

    # ================ FORKS/STARS (UIG) COMPARISON TABLE ==================
    ui_second_header= [
      "QIdx","QuarterLabel",
      "F-target","F-scaling","F%Target",
      "S-target","S-scaling","S%Target",
      "UIG-target","UIG-scaling","UIG%Target"
    ]
    ui_second_rows=[]
    F_raw_idx=5
    S_raw_idx=8
    UIG_idx=11

    for q_idx,(qlabel,qs,qe,xx) in enumerate(scaling_quarters, start=1):
        ns_count=0
        sum_f=0
        sum_s=0
        sum_uig=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_idx in ui_row_map[r]:
                rowd= ui_row_map[r][q_idx]
                f_str= rowd[F_raw_idx]   # e.g. "3"
                s_str= rowd[S_raw_idx]   # e.g. "5"
                uig_str= rowd[UIG_idx]   # e.g. "7.2"
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

        if q_idx not in ui_row_map[scaling_repo]:
            continue
        sc_row= ui_row_map[scaling_repo][q_idx]
        sc_f_str= sc_row[F_raw_idx]
        sc_s_str= sc_row[S_raw_idx]
        sc_uig_str= sc_row[UIG_idx]
        sc_f= parse_int(sc_f_str)
        sc_s= parse_int(sc_s_str)
        sc_uig= parse_float(sc_uig_str)

        row_ui2= [
          q_idx,
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
        ui_second_rows.append(row_ui2)

    comb_ui2= [ui_second_header]+ ui_second_rows
    ui2_widths= compute_widths(comb_ui2)

    print("\n=== USER INTEREST GROWTH (FORKS/STARS) COMPARISON TABLE (Raw) ===\n")
    print(center_line(ui_second_header, ui2_widths))
    print("-+-".join("-"*w for w in ui2_widths))
    for row in ui_second_rows:
        print(center_line(row, ui2_widths))

    print("\n=== Done. ===")

if __name__=="__main__":
    main()
