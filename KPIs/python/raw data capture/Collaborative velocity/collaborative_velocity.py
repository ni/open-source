#!/usr/bin/env python3
# python collaborative_velocity.py --scaling-repo ni/labview-icon-editor --global-offset -32 --start-fy 2024 --end-fy 2025 --scaling-window 210
"""
Quarter-Based Velocity Analysis
with user-defined scaling window (sum-based factor), main quarter table, and
an ADDITIONAL "Comparison Table" that compares the average (target) of all
non-scaling repos' raw M/I to the scaling repo's raw M/I, plus velocity.

Main Table Columns:
-------------------
  1) QIdx
  2) QuarterLabel
  3) Repo(Partial?)
  4) StartDate
  5) EndDate
  6) M-raw
  7) M (scaled)
  8) M-fact
  9) I-raw
  10) I (scaled)
  11) I-fact
  12) V

Second Table Columns (Comparison):
----------------------------------
  1) QIdx
  2) QuarterLabel
  3) M-target   (avg M-raw of all non-scaling repos)
  4) M-scaling  (scaling repo's M-raw)
  5) M% target  (100 * M-scaling / M-target) or N/A if target=0 or missing
  6) I-target   (avg I-raw)
  7) I-scaling
  8) I% target
  9) V-target   (avg velocity = avg(0.4*M-raw + 0.6*I-raw) among non-scaling)
  10) V-scaling (0.4*M-raw + 0.6*I-raw for scaling)
  11) V% target

All columns are center-aligned in both tables.
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

def format_scale_factor_3dec(f):
    if f is None:
        return "N/A"
    return f"{f:.3f}"

###############################################################################
# Compute scale factors with a user-defined window
###############################################################################
def compute_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    Sums merges/issues in [oldest, oldest+window_days].
    Possibly partial if last < that range. Then factor= scalingSum / repoSum, etc.
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
    use_end= s_end
    if s_last and s_last< s_end:
        partial_s= True
        use_end= s_last

    M_sSum,I_sSum= get_merges_and_issues(cursor, scaling_repo, s_old, use_end)

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
        rend= rold+ timedelta(days= window_days)
        rlast= get_last_date(cursor, r)
        isPartial= False
        used_end= rend
        if rlast and rlast< rend:
            isPartial= True
            used_end= rlast
        MrSum, IrSum= get_merges_and_issues(cursor, r, rold, used_end)

        cScale=False
        sfm=None
        sfi=None
        # merges
        if (M_sSum>0 and MrSum==0):
            cScale= True
        elif M_sSum==0 and MrSum>0:
            sfm=0.0
        else:
            if MrSum==0:
                sfm=1.0
            else:
                sfm= M_sSum/MrSum
        # issues
        if (I_sSum>0 and IrSum==0):
            cScale= True
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
def main():
    parser= argparse.ArgumentParser(
        description="""
Quarter-based velocity with user-defined scaling window, main table + second table comparing scaling vs. average of non-scaling.
        """
    )
    parser.add_argument("--scaling-repo", required=True)
    parser.add_argument("--start-fy", type=int, required=True)
    parser.add_argument("--end-fy",   type=int, required=True)
    parser.add_argument("--global-offset", type=int, default=0)
    parser.add_argument("--scaling-window", type=int, default=120,
                        help="Days to sum merges/issues from oldest date. Default=120.")
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

    # 1) compute scale factors
    sfM, sfI, partialRepo, cannotScale= compute_scale_factors(
        cursor, scaling_repo, all_repos, window_days
    )

    # 2) gather quarter definitions for scaling
    def get_fy_quarter_boundaries_local(fy):
        return get_fy_quarter_boundaries(fy)

    def in_quarter(dt,s,e):
        return (dt and s and e and dt>=s and dt<=e)

    s_old= get_oldest_date(cursor, scaling_repo)
    if not s_old:
        print("[WARN] no data => no table.")
        return
    s_adjust= s_old+ timedelta(days= offset_days)

    # gather quarters
    def gather_quarters():
        out=[]
        min_fy= min(start_fy, s_adjust.year-1)
        for fy in range(min_fy, end_fy+1):
            out.extend(get_fy_quarter_boundaries_local(fy))
        out.sort(key=lambda x:x[1])
        return out

    all_quarters= gather_quarters()
    scaling_quarters=[]
    found_first=False
    skip_label="(skipped partial at start)"

    for (qlabel,qst,qed) in all_quarters:
        lbl_fixed= qlabel.replace("\n"," ")
        # parse out fy
        parts= lbl_fixed.split("FY")
        if len(parts)==2:
            fy_digits=""
            for ch in parts[1]:
                if ch.isdigit():
                    fy_digits+= ch
                else:
                    break
            if fy_digits:
                if int(fy_digits)> end_fy:
                    break
        if not found_first:
            if qst<= s_adjust<= qed:
                partial_lbl= f"{lbl_fixed} {skip_label}"
                scaling_quarters.append((partial_lbl,qst,qed,True))
                found_first=True
            elif qed< s_adjust:
                pass
            else:
                scaling_quarters.append((lbl_fixed,qst,qed,False))
                found_first=True
        else:
            scaling_quarters.append((lbl_fixed,qst,qed,False))

    sblocks=[]
    for (_,qs,qe,sk) in scaling_quarters:
        if sk: sblocks.append((None,None))
        else:  sblocks.append((qs,qe))
    n_quarters= len(scaling_quarters)

    # build quarter windows for non-scaling
    def build_quarter_windows(st,n):
        arr=[]
        cur= st
        for _ in range(n):
            cend= cur+ relativedelta(months=3)- timedelta(seconds=1)
            arr.append((cur,cend))
            cur= cend+ timedelta(seconds=1)
        return arr

    non_scaling_data={}
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold= get_oldest_date(cursor, r)
        if not rold:
            non_scaling_data[r]= [(None,None)]*n_quarters
            continue
        radj= rold+ timedelta(days= offset_days)
        blocks= build_quarter_windows(radj,n_quarters)
        non_scaling_data[r]= blocks

    # main table columns
    main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "M-raw","M","M-fact","I-raw","I","I-fact","V"
    ]
    row_map= {rr:{} for rr in all_repos}

    def produce_row(qi, lbl, rep, pstr, sdt, edt,
                    m_raw, m_scl, m_fac,
                    i_raw, i_scl, i_fac,
                    vel):
        return [
          qi,
          lbl,
          rep+pstr,
          str(sdt) if sdt else "SKIPPED",
          str(edt) if edt else "SKIPPED",
          m_raw,
          m_scl,
          m_fac,
          i_raw,
          i_scl,
          i_fac,
          vel
        ]

    # get merges/issues scaled
    def format_quarter(r, merges_raw, issues_raw, skip_flag):
        if skip_flag:
            return (str(merges_raw),"cannot scale","N/A", str(issues_raw),"cannot scale","N/A","N/A")
        if cannotScale[r]:
            return (str(merges_raw),"cannot scale","N/A", str(issues_raw),"cannot scale","N/A","N/A")
        fm= sfM[r]
        fi= sfI[r]
        # M-raw => merges_raw
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
            vcalc= 0.4*(merges_raw*(fm if fm else 0)) + 0.6*(issues_raw*(fi if fi else 0))
            v_s= f"{vcalc:.1f}"
        return (str(merges_raw),M_s,M_f, str(issues_raw), I_s,I_f, v_s)

    # fill scaling
    for q_i,(qlbl,qs,qe,sk) in enumerate(scaling_quarters, start=1):
        (s_st,s_ed)= sblocks[q_i-1]
        merges_val, issues_val=(0,0)
        if s_st and s_ed:
            merges_val, issues_val= get_merges_and_issues(cursor, scaling_repo, s_st,s_ed)
        if sk:
            row_s= produce_row(
              q_i, qlbl, scaling_repo,
              " (partial)" if partialRepo[scaling_repo] else "",
              s_st, s_ed,
              str(merges_val),
              "cannot scale","N/A",
              str(issues_val),
              "cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => scaled= raw
            vcalc= 0.4* merges_val +0.6* issues_val
            row_s= produce_row(
              q_i, qlbl, scaling_repo,
              " (partial)" if partialRepo[scaling_repo] else "",
              s_st, s_ed,
              str(merges_val),
              str(merges_val),
              f"{1.000:.3f}",
              str(issues_val),
              str(issues_val),
              f"{1.000:.3f}",
              f"{vcalc:.1f}"
            )
        row_map[scaling_repo][q_i]= row_s

    # fill non-scaling
    for r in all_repos:
        if r==scaling_repo:
            continue
        blocks= non_scaling_data[r]
        for q_i,(qlbl,qs,qe,sk) in enumerate(scaling_quarters, start=1):
            (r_st,r_ed)= blocks[q_i-1]
            merges_val, issues_val= (0,0)
            if (not sk) and r_st and r_ed:
                merges_val, issues_val= get_merges_and_issues(cursor, r, r_st,r_ed)
            m_raw,m_s,m_f, i_raw,i_s,i_f, v_= format_quarter(r, merges_val, issues_val, sk)
            row_n= produce_row(
              q_i, qlbl, r, 
              " (partial)" if partialRepo[r] else "",
              r_st,r_ed,
              m_raw,m_s,m_f,
              i_raw,i_s,i_f,
              v_
            )
            row_map[r][q_i]= row_n

    cursor.close()
    cnx.close()

    # produce the main table
    main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "M-raw","M","M-fact","I-raw","I","I-fact","V"
    ]
    final_rows=[]
    sorted_repos= [scaling_repo] + [rr for rr in all_repos if rr!=scaling_repo]
    for rr in sorted_repos:
        for q_i in range(1,len(scaling_quarters)+1):
            if q_i in row_map[rr]:
                final_rows.append(row_map[rr][q_i])

    # center align
    def compute_widths(table_data):
        widths=[]
        for col_i in range(len(table_data[0])):
            mx=0
            for row in table_data:
                cell= str(row[col_i])
                if len(cell)> mx:
                    mx= len(cell)
            widths.append(mx+2)
        return widths

    combined_main= [main_header]+ final_rows
    main_widths= compute_widths(combined_main)

    def center_line(vals, widths):
        out=[]
        for v,w in zip(vals,widths):
            out.append(str(v).center(w))
        return " | ".join(out)

    print(f"\n=== MAIN QUARTER TABLE (Window={window_days} days) ===\n")
    print(center_line(main_header, main_widths))
    print("-+-".join("-"*w for w in main_widths))
    for row in final_rows:
        row_strs= [str(x) for x in row]
        print(center_line(row_strs, main_widths))

    ############################################################################
    # 2) Additional Table => average per quarter for non-scaling => target,
    #    scaling repo raw => percentage of target
    ############################################################################
    # columns => QIdx, QuarterLabel,
    #            M-target, M-scaling, M%,
    #            I-target, I-scaling, I%,
    #            V-target, V-scaling, V%
    second_cols= [
      "QIdx","QuarterLabel",
      "M-target","M-scaling","M%Target",
      "I-target","I-scaling","I%Target",
      "V-target","V-scaling","V%Target"
    ]
    second_rows=[]

    # We'll gather from row_map. The raw merges => col=5, raw issues => col=8, velocity => col=11
    M_raw_idx=5
    I_raw_idx=8
    V_idx=11

    for q_i, (qlabel,_,_,_) in enumerate(scaling_quarters, start=1):
        # 1) gather non-scaling's M-raw, I-raw => sum => average
        ns_count=0
        sum_m=0
        sum_i=0
        sum_v=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_i not in row_map[r]:
                continue
            row_dat= row_map[r][q_i]
            mraw_str= row_dat[M_raw_idx]  # e.g. "3"
            iraw_str= row_dat[I_raw_idx]  # e.g. "9"
            v_str   = row_dat[V_idx]      # e.g. "5.4"
            def parse_int(s):
                try:
                    return int(s)
                except:
                    return None
            def parse_float(s):
                try:
                    return float(s)
                except:
                    return None
            m_int= parse_int(mraw_str)
            i_int= parse_int(iraw_str)
            v_val= parse_float(v_str)
            if m_int is not None and i_int is not None and v_val is not None:
                ns_count+=1
                sum_m+= m_int
                sum_i+= i_int
                sum_v+= v_val
        if ns_count>0:
            m_avg= sum_m/ns_count
            i_avg= sum_i/ns_count
            v_avg= sum_v/ns_count
        else:
            m_avg=0
            i_avg=0
            v_avg=0

        # 2) scaling row => parse M-raw, I-raw => velocity
        if q_i not in row_map[scaling_repo]:
            # skip
            continue
        sc_row= row_map[scaling_repo][q_i]
        sc_m_str= sc_row[M_raw_idx]  # might be "cannot scale" or int
        sc_i_str= sc_row[I_raw_idx]
        sc_v_str= sc_row[V_idx]
        def parse_int(s):
            try:
                return int(s)
            except:
                return None
        def parse_float(s):
            try:
                return float(s)
            except:
                return None
        sc_m= parse_int(sc_m_str)
        sc_i= parse_int(sc_i_str)
        sc_v= parse_float(sc_v_str)
        # ratio => scaling/avg => if avg=0 => "N/A" unless scaling also=0 => ratio=1
        def ratio_str(val, avg):
            if avg>0 and val is not None:
                return f"{(100.0*(val/avg)):.1f}%"
            elif avg==0 and val==0:
                return "100.0%"
            return "N/A"

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
          ratio_str(sc_v, v_avg if v_avg else 0)
        ]
        second_rows.append(row2)

    # center align that second table
    second_header= [
      "QIdx","QuarterLabel",
      "M-target","M-scaling","M%Target",
      "I-target","I-scaling","I%Target",
      "V-target","V-scaling","V%Target"
    ]
    comb2= [second_header]+ second_rows
    sec_widths=[]
    for c_i in range(len(second_header)):
        mx=0
        for row in comb2:
            cell= str(row[c_i])
            if len(cell)> mx:
                mx= len(cell)
        sec_widths.append(mx+2)

    def center_line(vals, widths):
        out=[]
        for v,w in zip(vals,widths):
            out.append(str(v).center(w))
        return " | ".join(out)

    print("\n=== SECOND TABLE: Non-scaling average (raw) vs. Scaling Repo (raw) => % of target ===\n")
    print(center_line(second_header, sec_widths))
    print("-+-".join("-"*w for w in sec_widths))
    for row in second_rows:
        row_strs= [str(x) for x in row]
        print(center_line(row_strs, sec_widths))

    print("\n=== Done. ===")

if __name__=="__main__":
    main()
