#!/usr/bin/env python3
"""
Quarter-Based Collaborative Velocity Analysis
--------------------------------------------
This script:
1) Connects to a MySQL database (using db_config.ini).
2) Accepts command-line arguments:
   --scaling-repo <repo>   (which repo is the "reference" for scaling factors)
   --start-fy <year>       (first fiscal year to consider)
   --end-fy <year>         (last fiscal year to consider)
   --global-offset <days>  (shift each repo's oldest date by this many days)
   --scaling-window <days> (how many days from each repo's oldest date to sum merges/issues for factor)

3) Computes a “scaling factor” for merges/issues for each non-scaling repo by summing merges/issues
   in [oldestDate+offset, oldestDate+offset+scaling_window], partial if the repo ends earlier.
   - If the scaling repo’s sum>0 but the non-scaling sum=0 => we treat merges/issues as “cannot scale.”
   - If both sums=0 => factor=1.0, etc.

4) Builds fiscal-year quarters from --start-fy to --end-fy. Normally, if the offset lands inside a quarter,
   we used to skip it. Now we DO NOT SKIP. We label that quarter as "(partial)" if the offset starts mid-quarter
   or if it’s in progress. The same if the quarter extends beyond the last known data.

5) Produces Two Tables:
   A) Main Table:
      QIdx, QuarterLabel, Repo(Partial?), StartDate, EndDate,
      M-raw, M (scaled), M-fact, I-raw, I (scaled), I-fact, V
   B) Comparison Table:
      For each quarter, compute the average raw merges/issues/velocity among all non-scaling repos => "target."
      Compare scaling repo's raw merges/issues => a percentage of that target.

All columns are center-aligned. Partial quarters are included rather than skipped,
and flagged as "(partial)" next to the quarter label or the repo name.

Usage Example:
--------------
  python collaborative_velocity.py --scaling-repo ni/labview-icon-editor \
      --start-fy 2024 --end-fy 2025 --scaling-window 210 --global-offset -32
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
# 1) Utilities: Quarter Boundaries, DB Queries, Format
###############################################################################

def get_fy_quarter_boundaries(fy):
    """
    Return Q1..Q4 for a given FY. Each label might include a newline (Q1\\nFY2025)
    which we will replace with a space for single-line display.
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
    Return the oldest creation date (pulls/issues) for this repo.
    """
    q = """
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
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

def get_last_date(cursor, repo_name):
    """
    Return the latest creation date (pulls/issues) for this repo.
    """
    q = """
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
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

def get_merges_and_issues(cursor, repo, start_dt, end_dt):
    """
    Return (# merges, # issues) in the interval [start_dt, end_dt].
    If start_dt/end_dt is None, or any partial logic, the queries might yield 0.
    """
    if not start_dt or not end_dt:
        return (0, 0)
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

def format_scale_factor_3dec(f):
    """
    If factor is None => "N/A"
    Otherwise => up to 3 decimals, e.g. 0.123
    """
    if f is None:
        return "N/A"
    return f"{f:.3f}"

###############################################################################
# 2) Compute scale factors (sum-based) with user-defined window
###############################################################################

def compute_scale_factors(cursor, scaling_repo, all_repos, window_days):
    """
    For each repo r:
      1) from oldest+0 => oldest+window_days (partial if last < that range)
      2) merges/issues => sum
      3) scalingFactorM(r) = scaling_sums.merges / r_sums.merges, with zero checks
         scalingFactorI(r) = ...
    partialRepo[r] => True if that range is partial
    cannotScale[r] => True if scaling>0 but repo=0, or similar
    """
    scaleFactorM = {}
    scaleFactorI = {}
    partialRepo  = {}
    cannotScale  = {}

    # 1) Get the scaling repo's sum
    s_old = get_oldest_date(cursor, scaling_repo)
    if not s_old:
        # fallback => no data
        for r in all_repos:
            scaleFactorM[r] = 1.0
            scaleFactorI[r] = 1.0
            partialRepo[r]  = False
            cannotScale[r]  = False
        return scaleFactorM, scaleFactorI, partialRepo, cannotScale

    s_end = s_old + timedelta(days=window_days)
    s_last = get_last_date(cursor, scaling_repo)
    partial_s = False
    use_end = s_end
    if s_last and s_last < s_end:
        partial_s = True
        use_end   = s_last

    M_sSum, I_sSum = get_merges_and_issues(cursor, scaling_repo, s_old, use_end)
    # scaling => factor=1.0
    scaleFactorM[scaling_repo] = 1.0
    scaleFactorI[scaling_repo] = 1.0
    partialRepo[scaling_repo]  = partial_s
    cannotScale[scaling_repo]  = False

    # 2) For each non-scaling
    for r in all_repos:
        if r == scaling_repo:
            continue
        rold = get_oldest_date(cursor, r)
        if not rold:
            scaleFactorM[r] = None
            scaleFactorI[r] = None
            partialRepo[r]  = False
            cannotScale[r]  = True
            continue
        rend = rold + timedelta(days=window_days)
        rlast = get_last_date(cursor, r)
        isPartial = False
        used_end  = rend
        if rlast and rlast < rend:
            isPartial = True
            used_end  = rlast
        MrSum, IrSum = get_merges_and_issues(cursor, r, rold, used_end)

        cScale = False
        sfm     = None
        sfi     = None

        # M factor
        if (M_sSum>0 and MrSum==0):
            # scaling>0 but repo=0 => cannot scale
            cScale = True
        elif M_sSum==0 and MrSum>0:
            sfm = 0.0
        else:
            if MrSum==0:
                sfm = 1.0
            else:
                sfm = M_sSum / MrSum

        # I factor
        if (I_sSum>0 and IrSum==0):
            cScale = True
        elif I_sSum==0 and IrSum>0:
            sfi = 0.0
        else:
            if IrSum==0:
                sfi=1.0
            else:
                sfi = I_sSum / IrSum

        scaleFactorM[r] = sfm
        scaleFactorI[r] = sfi
        partialRepo[r]  = isPartial
        cannotScale[r]  = cScale

    return scaleFactorM, scaleFactorI, partialRepo, cannotScale

###############################################################################
# 3) Main Script
###############################################################################

def main():
    parser = argparse.ArgumentParser(
        description="""
Quarter-based velocity with user-defined scaling window.
No quarters are skipped; partial quarters are included and flagged.
Also includes a second table comparing scaling repo's raw merges/issues to
the average of non-scaling repos' raw merges/issues, plus velocity.
"""
    )
    parser.add_argument("--scaling-repo", required=True)
    parser.add_argument("--start-fy", type=int, required=True)
    parser.add_argument("--end-fy",   type=int, required=True)
    parser.add_argument("--global-offset", type=int, default=0)
    parser.add_argument("--scaling-window", type=int, default=120,
                        help="Days to sum merges/issues from oldest date. Default=120.")
    args = parser.parse_args()

    scaling_repo = args.scaling_repo
    start_fy     = args.start_fy
    end_fy       = args.end_fy
    offset_days  = args.global_offset
    window_days  = args.scaling_window

    # define your repos
    all_repos = [
        "tensorflow/tensorflow",
        "facebook/react",
        "ni/labview-icon-editor"
    ]
    if scaling_repo not in all_repos:
        print(f"[ERROR] scaling-repo {scaling_repo} not recognized in {all_repos}.")
        sys.exit(1)

    # 1) Connect to DB
    config = configparser.ConfigParser()
    config.read("db_config.ini")
    db_params = config["mysql"]

    cnx = mysql.connector.connect(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        database=db_params["database"]
    )
    cursor = cnx.cursor()

    # 2) Compute scaling factors using the chosen window
    sfM, sfI, partialRepo, cannotScale = compute_scale_factors(
        cursor, scaling_repo, all_repos, window_days
    )

    # 3) Build the quarter definitions from --start-fy..--end-fy
    def gather_quarters():
        quarters = []
        # We'll define a min_fy that is 1 year behind if offset leads us earlier,
        # but for simplicity:
        # we just do from start_fy to end_fy
        for fy in range(start_fy, end_fy+1):
            blocks = get_fy_quarter_boundaries(fy)
            quarters.extend(blocks)
        # sort by the quarter's start date
        quarters.sort(key=lambda x:x[1])
        return quarters

    all_quarters = gather_quarters()

    # We'll build a list (scaling_quarters) of (qlabel, qstart, qend, partial?)
    # The difference is we do NOT skip partial quarters. Instead, we label them partial if needed.

    # Let's define "partial" if:
    # - the offset is in the middle of that quarter, or
    # - the quarter extends beyond the current date, or
    # - any other logic you want for partial. For now, let's define partial if offset is inside.
    # We'll also do "found_first" so we start listing from the first quarter that ends after offset, etc.
    # But we won't skip any; we'll label them partial.

    s_old = get_oldest_date(cursor, scaling_repo)
    if not s_old:
        print(f"[WARNING] scaling repo {scaling_repo} has no data => no table.")
        return
    s_adjust = s_old + timedelta(days=offset_days)

    scaling_quarters = []
    started = False
    skip_label = "(partial)"

    # We'll define "found_first" to ensure we start at the quarter that includes or is after offset.
    # But we won't skip partial. We just label it partial if offset is inside it. Then we continue
    # with all subsequent quarters up to end_fy.
    for (q_label, q_start, q_end) in all_quarters:
        # single-line label
        q_label_single = q_label.replace("\n"," ")
        # check if offset is inside this quarter:
        is_partial = (q_start <= s_adjust <= q_end)  # or any other logic
        if not started:
            if s_adjust < q_start:
                # offset is before the quarter entirely => not partial, but we do start here
                scaling_quarters.append((q_label_single, q_start, q_end, False))
                started = True
            elif is_partial:
                # offset is in the middle => label partial
                new_label = f"{q_label_single} {skip_label}"
                scaling_quarters.append((new_label, q_start, q_end, False))
                started = True
            else:
                # offset is after q_end => skip this quarter
                if q_end < s_adjust:
                    # do we want to include earlier quarters? The user might want them anyway.
                    # If we REALLY want to include them, do so, but typically you'd skip them if they're fully before offset.
                    pass
                else:
                    # we've found the first quarter that is after offset
                    scaling_quarters.append((q_label_single, q_start, q_end, False))
                    started = True
        else:
            # we already started => keep all subsequent quarters
            # If offset is in the middle => label partial
            # If the current date is before the quarter end => we might also label partial. etc.
            # We'll do a simpler approach: if s_adjust > q_start AND s_adjust < q_end => partial
            # but typically s_adjust won't matter for subsequent quarters. 
            # If you want to label "in-progress" if q_end> now => we can do that:
            now = datetime.utcnow()
            # if now < q_end => partial?
            if now < q_end:
                new_label = f"{q_label_single} (partial/in-progress)"
                scaling_quarters.append((new_label, q_start, q_end, False))
            else:
                scaling_quarters.append((q_label_single, q_start, q_end, False))

    # Now we have a list of all quarters from the first that occurs after offset to the end_fy, none are skipped.
    # Next we define quarter windows for scaling, but we don't skip them. We'll keep them all:
    # We'll store them in an array sblocks => same length => sblocks[i] = (start, end)

    sblocks = []
    for (lbl, qs, qe, x) in scaling_quarters:
        # we won't skip => so just store (qs, qe)
        sblocks.append((qs, qe))

    n_quarters = len(scaling_quarters)

    # We do the same for non-scaling repos: build quarter windows from their oldest + offset
    def build_quarter_windows(st, count):
        arr=[]
        cur= st
        for _ in range(count):
            cend= cur+ relativedelta(months=3)- timedelta(seconds=1)
            arr.append((cur,cend))
            cur= cend+ timedelta(seconds=1)
        return arr

    non_scaling_data={}
    for r in all_repos:
        if r==scaling_repo:
            continue
        rold = get_oldest_date(cursor, r)
        if not rold:
            non_scaling_data[r] = [(None,None)]* n_quarters
            continue
        radj= rold+ timedelta(days=offset_days)
        blocks= build_quarter_windows(radj, n_quarters)
        non_scaling_data[r] = blocks

    # We'll store final table data in row_map[repo][quarter_idx].
    # columns:
    # (1) QIdx, (2) QuarterLabel, (3) Repo(Partial?), (4) StartDate, (5) EndDate,
    # (6) M-raw, (7) M, (8) M-fact, (9) I-raw, (10) I, (11) I-fact, (12) V
    main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "M-raw","M","M-fact","I-raw","I","I-fact","V"
    ]
    row_map= {r:{} for r in all_repos}

    def produce_row(qi, label, repo_nm, start_dt, end_dt,
                    m_raw, m_scl, m_fac,
                    i_raw, i_scl, i_fac, vel):
        return [
          qi,
          label,
          repo_nm,      # if partial, we might add " (partial)" in the label or the repo
          str(start_dt) if start_dt else "SKIPPED",
          str(end_dt)   if end_dt   else "SKIPPED",
          m_raw,
          m_scl,
          m_fac,
          i_raw,
          i_scl,
          i_fac,
          vel
        ]

    # 4) For scaling repo => fill data
    for q_idx, (qlabel,qstart,qend, xflag) in enumerate(scaling_quarters, start=1):
        s_st, s_ed= sblocks[q_idx-1]
        merges_val, issues_val= (0,0)
        if s_st and s_ed:
            merges_val, issues_val= get_merges_and_issues(cursor, scaling_repo, s_st,s_ed)
        # Retrieve the merges factor from scaleFactorM
        # but scaling always => factor=1.0
        # if partialRepo[scaling_repo]? we can label the repo or the label as partial
        # Actually we already appended "(partial)" in qlabel if needed
        if cannotScale[scaling_repo]:
            # Typically scaling can't scale is false => but let's do a fallback
            row_s= produce_row(
              q_idx, qlabel, scaling_repo,
              s_st, s_ed,
              str(merges_val),
              "cannot scale","N/A",
              str(issues_val),
              "cannot scale","N/A",
              "N/A"
            )
        else:
            # factor=1 => M scaled= merges_val, I scaled= issues_val
            vel= 0.4* merges_val + 0.6* issues_val
            row_s= produce_row(
              q_idx, qlabel, scaling_repo,
              s_st, s_ed,
              str(merges_val),
              str(merges_val),
              f"{1.0:.3f}",
              str(issues_val),
              str(issues_val),
              f"{1.0:.3f}",
              f"{vel:.1f}"
            )
        row_map[scaling_repo][q_idx]= row_s

    # 5) For non-scaling repos => build row data
    def format_quarter(r, merges_raw, issues_raw):
        # merges => factor => merges scaled
        # if cannot scale => merges= "cannot scale"
        if cannotScale[r]:
            return (
              str(merges_raw), "cannot scale", "N/A",
              str(issues_raw), "cannot scale", "N/A",
              "N/A"
            )
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

    for r in all_repos:
        if r==scaling_repo:
            continue
        blocks= non_scaling_data[r]
        for q_idx, (qlabel, qstart, qend, xflag) in enumerate(scaling_quarters, start=1):
            # retrieve that quarter window
            (r_st, r_ed)= blocks[q_idx-1]
            merges_val, issues_val= (0,0)
            if r_st and r_ed:
                merges_val, issues_val= get_merges_and_issues(cursor, r, r_st, r_ed)
            # format => produce row
            M_raw, M_s, M_f, I_raw, I_s, I_f, V_s= format_quarter(r, merges_val, issues_val)
            row_n= produce_row(
              q_idx, qlabel, r,
              r_st, r_ed,
              M_raw, M_s, M_f,
              I_raw, I_s, I_f,
              V_s
            )
            row_map[r][q_idx]= row_n

    cursor.close()
    cnx.close()

    # 6) Print the main table => center align
    main_header= [
      "QIdx","QuarterLabel","Repo(Partial?)","StartDate","EndDate",
      "M-raw","M","M-fact","I-raw","I","I-fact","V"
    ]
    final_rows=[]
    # sorted => scaling first
    sorted_repos= [scaling_repo] + [rr for rr in all_repos if rr!=scaling_repo]
    for rr in sorted_repos:
        for q_i in range(1, n_quarters+1):
            if q_i in row_map[rr]:
                final_rows.append(row_map[rr][q_i])

    def compute_col_widths(table):
        widths=[]
        for col_i in range(len(table[0])):
            mx=0
            for row in table:
                cell_str= str(row[col_i])
                if len(cell_str)> mx:
                    mx= len(cell_str)
            widths.append(mx+2)
        return widths

    combined_main= [main_header]+ final_rows
    col_widths= compute_col_widths(combined_main)

    def center_line(vals, widths):
        out=[]
        for v,w in zip(vals,widths):
            out.append(str(v).center(w))
        return " | ".join(out)

    print(f"\n=== MAIN QUARTER TABLE (Window={window_days} days, no skip of partial) ===\n")
    print(center_line(main_header, col_widths))
    print("-+-".join("-"*cw for cw in col_widths))
    for row in final_rows:
        print(center_line(row, col_widths))

    # 7) Additional Comparison Table => average across non-scaling for M-raw, I-raw, velocity => compare scaling
    second_header= [
      "QIdx","QuarterLabel",
      "M-target","M-scaling","M%Target",
      "I-target","I-scaling","I%Target",
      "V-target","V-scaling","V%Target"
    ]
    second_rows=[]

    M_raw_idx=5
    I_raw_idx=8
    V_idx=11

    for q_idx, (qlabel,qstart,qend,xflag) in enumerate(scaling_quarters, start=1):
        # gather M-raw, I-raw, V from non-scaling
        ns_count=0
        sum_m=0
        sum_i=0
        sum_v=0.0
        for r in all_repos:
            if r==scaling_repo:
                continue
            if q_idx in row_map[r]:
                row_dat= row_map[r][q_idx]
                m_str= row_dat[M_raw_idx]
                i_str= row_dat[I_raw_idx]
                v_str= row_dat[V_idx]

                def parse_int(s):
                    try: return int(s)
                    except: return None
                def parse_float(s):
                    try: return float(s)
                    except: return None
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
        if q_idx not in row_map[scaling_repo]:
            continue
        sc_row= row_map[scaling_repo][q_idx]
        sc_m_str= sc_row[M_raw_idx]
        sc_i_str= sc_row[I_raw_idx]
        sc_v_str= sc_row[V_idx]

        def parse_int(s):
            try: return int(s)
            except: return None
        def parse_float(s):
            try: return float(s)
            except: return None
        sc_m= parse_int(sc_m_str)
        sc_i= parse_int(sc_i_str)
        sc_v= parse_float(sc_v_str)
        
        def ratio_str(val, avg):
            if avg>0 and val is not None:
                return f"{(100.0* val/avg):.1f}%"
            elif avg==0 and val==0:
                return "100.0%"
            return "N/A"

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
        second_rows.append(row2)

    # center align second table
    comb2= [second_header]+ second_rows
    sec_widths=[]
    for c_i in range(len(second_header)):
        mx=0
        for row in comb2:
            c= str(row[c_i])
            if len(c)> mx:
                mx= len(c)
        sec_widths.append(mx+2)

    print("\n=== SECOND TABLE: Non-scaling average (raw) vs. Scaling Repo (raw), % of target ===\n")
    print(center_line(second_header, sec_widths))
    print("-+-".join("-"*w for w in sec_widths))
    for row in second_rows:
        print(center_line(row, sec_widths))

    print("\n=== Done. ===")

if __name__=="__main__":
    main()
