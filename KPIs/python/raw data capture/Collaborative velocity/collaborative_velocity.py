#!/usr/bin/env python3

"""
Quarter-Based Collaborative Velocity Analysis
with 120-Day Scaling Factor, Single-Line Quarter Labels, Aligned Columns.

Special Request:
---------------
- The "QIdx" column header is left-aligned ("QIdx".ljust(width))
- All other column headers are center-aligned
- The data rows remain left-aligned
- This way, QIdx appears exactly as your example.

Usage Example:
--------------
  python collaborative_velocity.py --scaling-repo ni/labview-icon-editor --start-fy 2025 --end-fy 2026
"""

import argparse
import configparser
import mysql.connector
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import numpy as np
import sys

def get_fy_quarter_boundaries(fy):
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
    query = """
        SELECT MIN(all_min) AS oldest_date
        FROM (
            SELECT MIN(created_at) AS all_min
            FROM pulls
            WHERE repo_name = %s
            
            UNION ALL
            
            SELECT MIN(created_at) AS all_min
            FROM issues
            WHERE repo_name = %s
        ) AS subq
    """
    cursor.execute(query, (repo_name, repo_name))
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

def get_last_date(cursor, repo_name):
    query = """
        SELECT MAX(all_max)
        FROM (
            SELECT MAX(created_at) AS all_max
            FROM pulls
            WHERE repo_name = %s
            UNION ALL
            SELECT MAX(created_at) AS all_max
            FROM issues
            WHERE repo_name = %s
        ) subq
    """
    cursor.execute(query, (repo_name, repo_name))
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

def get_merges_and_issues(cursor, repo, start_dt, end_dt):
    if not start_dt or not end_dt:
        return (0,0)
    qm = """
        SELECT COUNT(*)
        FROM pulls
        WHERE repo_name=%s
          AND merged_at IS NOT NULL
          AND merged_at >= %s
          AND merged_at <= %s
    """
    cursor.execute(qm, (repo, start_dt, end_dt))
    merges_count = cursor.fetchone()[0]

    qi = """
        SELECT COUNT(*)
        FROM issues
        WHERE repo_name=%s
          AND closed_at IS NOT NULL
          AND closed_at >= %s
          AND closed_at <= %s
    """
    cursor.execute(qi, (repo, start_dt, end_dt))
    issues_count = cursor.fetchone()[0]
    return (merges_count, issues_count)

def format_scale_factor(factor):
    if factor is None:
        return "None"
    if factor==0:
        return "0.0"
    s = f"{factor:.15g}"
    if '.' in s:
        s = s.rstrip('0')
        if s.endswith('.'):
            s+="0"
    return s

def compute_120day_scale_factors(cursor, scaling_repo, all_repos):
    scaleFactorM = {}
    scaleFactorI = {}
    partialRepo  = {}
    cannotScale  = {}

    s_old = get_oldest_date(cursor, scaling_repo)
    if not s_old:
        for r in all_repos:
            scaleFactorM[r]=1.0
            scaleFactorI[r]=1.0
            partialRepo[r]=False
            cannotScale[r]=False
        return scaleFactorM, scaleFactorI, partialRepo, cannotScale

    s_end= s_old+ timedelta(days=120)
    s_last= get_last_date(cursor, scaling_repo)
    partial_s= False
    actual_sdays=120
    if s_last and s_last< s_end:
        partial_s= True
        actual_sdays= max((s_last- s_old).days,1)
    M_s,I_s= get_merges_and_issues(cursor, scaling_repo, s_old, s_end)
    M_sAvg= M_s/float(actual_sdays)
    I_sAvg= I_s/float(actual_sdays)

    scaleFactorM[scaling_repo]=1.0
    scaleFactorI[scaling_repo]=1.0
    partialRepo[scaling_repo]= partial_s
    cannotScale[scaling_repo]= False

    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            scaleFactorM[r]= None
            scaleFactorI[r]= None
            partialRepo[r]= False
            cannotScale[r]= True
            continue
        rend= rold+ timedelta(days=120)
        rlast= get_last_date(cursor, r)
        isPartial=False
        actual_rdays=120
        if rlast and rlast< rend:
            isPartial=True
            actual_rdays= max((rlast- rold).days,1)
        Mr,Ir= get_merges_and_issues(cursor, r, rold,rend)
        MrAvg= Mr/float(actual_rdays)
        IrAvg= Ir/float(actual_rdays)

        cScale=False
        sf_m=None
        sf_i=None
        # merges
        if (M_sAvg>0 and MrAvg==0):
            cScale=True
        elif M_sAvg==0 and MrAvg>0:
            sf_m=0.0
        else:
            if MrAvg==0:
                sf_m=1.0
            else:
                sf_m= M_sAvg/MrAvg
        # issues
        if (I_sAvg>0 and IrAvg==0):
            cScale=True
        elif I_sAvg==0 and IrAvg>0:
            sf_i=0.0
        else:
            if IrAvg==0:
                sf_i=1.0
            else:
                sf_i= I_sAvg/IrAvg

        scaleFactorM[r]= sf_m
        scaleFactorI[r]= sf_i
        partialRepo[r]= isPartial
        cannotScale[r]= cScale
    return scaleFactorM, scaleFactorI, partialRepo, cannotScale

def main():
    parser= argparse.ArgumentParser(
        description="Quarter-based velocity + 120-day scaling factor, single-line quarter labels, aligned columns."
    )
    parser.add_argument("--scaling-repo",required=True)
    parser.add_argument("--start-fy", type=int,required=True)
    parser.add_argument("--end-fy", type=int,required=True)
    parser.add_argument("--global-offset",type=int,default=0)
    args= parser.parse_args()

    scaling_repo= args.scaling_repo
    start_fy= args.start_fy
    end_fy= args.end_fy
    offset_days= args.global_offset

    all_repos=[
        "ni/actor-framework",
        "tensorflow/tensorflow",
        "facebook/react",
        "dotnet/core",
        "ni/labview-icon-editor"
    ]
    if scaling_repo not in all_repos:
        print("[ERROR] scaling-repo not recognized.")
        sys.exit(1)

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

    # 1) scale factors
    sfM, sfI, partialRepo, cannotScale= compute_120day_scale_factors(cursor, scaling_repo, all_repos)

    # 2) gather scaling quarters
    def in_quarter(dt,s,e):
        return dt>=s and dt<= e

    s_old= get_oldest_date(cursor,scaling_repo)
    if not s_old:
        print("[WARNING] no data for scaling => no table.")
        return
    s_adjust= s_old+ timedelta(days= offset_days)

    min_fy= min(start_fy, s_adjust.year-1)
    quarter_list=[]
    for fy in range(min_fy, end_fy+1):
        blocks= get_fy_quarter_boundaries(fy)
        quarter_list.extend(blocks)
    quarter_list.sort(key=lambda x:x[1])

    scaling_quarters=[]
    found_first=False
    skip_label="(skipped partial at start)"

    for (qlabel, qst, qend) in quarter_list:
        # fix multiline => single line
        qlabel_fixed= qlabel.replace("\n"," ")
        # parse out fy from "Q3 FY2024"
        parts= qlabel_fixed.split("FY")
        if len(parts)==2:
            # e.g. "Q3 " and "2024"
            # strip non-digits from second part
            fy_digits=""
            for ch in parts[1]:
                if ch.isdigit():
                    fy_digits+=ch
                else:
                    break
            if fy_digits:
                fval= int(fy_digits)
                if fval> end_fy:
                    break

        if not found_first:
            if in_quarter(s_adjust,qst,qend):
                partial_lbl= f"{qlabel_fixed} {skip_label}"
                scaling_quarters.append((partial_lbl,qst,qend,True))
                found_first=True
            elif qend< s_adjust:
                pass
            else:
                scaling_quarters.append((qlabel_fixed,qst,qend,False))
                found_first=True
        else:
            scaling_quarters.append((qlabel_fixed,qst,qend,False))

    sblocks=[]
    for (_,qs,qe,sk) in scaling_quarters:
        sblocks.append((None,None) if sk else (qs,qe))
    n_quarters= len(scaling_quarters)

    # build non-scaling blocks
    def build_quarter_windows(st,n):
        out=[]
        cur= st
        for _ in range(n):
            cend= cur+ relativedelta(months=3)-timedelta(seconds=1)
            out.append((cur,cend))
            cur= cend+ timedelta(seconds=1)
        return out

    non_scaling_data={}
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            non_scaling_data[r]= [(None,None)]*n_quarters
            continue
        adj= rold+ timedelta(days= offset_days)
        blocks= build_quarter_windows(adj,n_quarters)
        non_scaling_data[r]= blocks

    def format_cell(r, merges_raw, issues_raw):
        if cannotScale[r]:
            return ("cannot scale","cannot scale")
        fm= sfM[r]
        fi= sfI[r]
        if fm is None:
            M_str="cannot scale"
        else:
            mm_s= merges_raw* fm
            fac_str= format_scale_factor(fm)
            M_str= f"{int(round(mm_s))} (sf={fac_str})"
        if fi is None:
            I_str="cannot scale"
        else:
            ii_s= issues_raw* fi
            fac_str= format_scale_factor(fi)
            I_str= f"{int(round(ii_s))} (sf={fac_str})"
        return (M_str,I_str)

    col_header= ["QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate","M","I","V"]
    rows=[]

    for q_idx,(qlabel,qs,qe,skip_flag) in enumerate(scaling_quarters, start=1):
        s_st,s_ed= sblocks[q_idx-1]
        M_s,I_s= (0,0)
        if s_st and s_ed:
            M_s,I_s= get_merges_and_issues(cursor, scaling_repo, s_st,s_ed)
        if skip_flag:
            M_col= "cannot scale"
            I_col= "cannot scale"
            V_col= "N/A"
        else:
            M_col= f"{M_s} (sf=1.0)"
            I_col= f"{I_s} (sf=1.0)"
            vel= 0.4*M_s+ 0.6*I_s
            V_col= f"{vel:.1f}"
        p_str= " (partial)" if partialRepo[scaling_repo] else ""
        row_s= [
          q_idx,
          qlabel,
          scaling_repo+p_str,
          str(s_st) if s_st else "SKIPPED",
          str(s_ed) if s_ed else "SKIPPED",
          M_col,I_col,V_col
        ]
        rows.append(row_s)

        # non-scaling
        for r in all_repos:
            if r==scaling_repo:
                continue
            (r_st,r_ed)= (None,None)
            if r in non_scaling_data:
                b= non_scaling_data[r]
                if q_idx-1< len(b):
                    r_st,r_ed= b[q_idx-1]
            merges_raw, issues_raw=(0,0)
            if skip_flag:
                M_o="cannot scale"
                I_o="cannot scale"
                V_o="N/A"
            else:
                if r_st and r_ed:
                    merges_raw, issues_raw= get_merges_and_issues(cursor, r, r_st, r_ed)
                M_o,I_o= format_cell(r, merges_raw, issues_raw)
                if "cannot scale" in M_o or "cannot scale" in I_o:
                    V_o="N/A"
                else:
                    fm= sfM[r]
                    fi= sfI[r]
                    mm_s= merges_raw*(fm if fm else 0)
                    ii_s= issues_raw*(fi if fi else 0)
                    vv= 0.4*mm_s+ 0.6*ii_s
                    V_o= f"{vv:.1f}"
            part_r= " (partial)" if partialRepo[r] else ""
            row_n= [
              q_idx, qlabel,
              r+ part_r,
              str(r_st) if r_st else "N/A",
              str(r_ed) if r_ed else "N/A",
              M_o, I_o, V_o
            ]
            rows.append(row_n)

    cursor.close()
    cnx.close()

    # align columns
    combined= [col_header]+ rows
    col_widths=[]
    for col_i in range(len(col_header)):
        maxlen=0
        for row in combined:
            cell= str(row[col_i])
            if len(cell)> maxlen:
                maxlen= len(cell)
        col_widths.append(maxlen+2)

    def print_header(vals):
        out_cells=[]
        for i,(v,w) in enumerate(zip(vals, col_widths)):
            if i==0:
                # QIdx => left align
                out_cells.append(v.ljust(w))
            else:
                # all others => center
                out_cells.append(v.center(w))
        print(" | ".join(out_cells))

    def print_row(vals):
        out_cells=[]
        for v,w in zip(vals,col_widths):
            out_cells.append(str(v).ljust(w))
        print(" | ".join(out_cells))

    print("\n=== Quarter-Based Table with QIdx left-aligned in header ===\n")
    print_header(col_header)
    print("-+-".join("-"*cw for cw in col_widths))
    for row in rows:
        print_row(row)

    print("\n=== Done ===")

if __name__=="__main__":
    main()
