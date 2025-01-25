#!/usr/bin/env python3
"""
main.py - Final Integrated BFS + Aggregator + Non-scaling Target + Partial Coverage
-----------------------------------------------------------------------------------
- Avoids KeyError by defining empty raw-data dicts for 'TARGET(avgNonScaling)'
- Ensures 'largest_overlap_quarter' is defined above 'produce_chart_with_table'
- BFS includes raw merges/issues/forks/stars + aggregator columns (Velocity, UIG, MAC, SEI)
- Generates separate .png files for each raw variable + aggregator metrics
- Partial quarters labeled "(partial)" if the quarter extends beyond 'now'

Dependencies (must be present in your environment):
  1) aggregator.py => aggregator logic (velocity, user_interest_growth, monthly_active_contributors)
  2) scale_factors.py => mergesFactor, closedFactor, etc. plus compute_scale_factors
  3) analytics/ => merges_issues.py, forks_stars.py, comments_reactions.py, etc. that do real SQL queries
  4) baseline.py => find_oldest_date_for_repo
  5) quarters.py => generate_quarter_windows
  6) config.py => get_scaling_repo, get_num_fiscal_quarters or environment
"""

import sys
import os
import io
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

# 1) config.py => user’s environment & config retrieval
from config import get_scaling_repo, get_num_fiscal_quarters

# 2) aggregator.py => aggregator weights & formulas
from aggregator import (
    load_aggregator_weights,
    velocity as aggregator_velocity,
    user_interest_growth as aggregator_uig,
    monthly_active_contributors as aggregator_mac
)

# 3) scale_factors.py => mergesFactor, closedFactor, etc.
from scale_factors import (
    compute_scale_factors,
    compute_sei_data
)

# 4) baseline.py => earliest date
from baseline import find_oldest_date_for_repo

# 5) quarters.py => quarter windows
from quarters import generate_quarter_windows

# 6) analytics/ => merges_issues, forks_stars, comments_reactions
from merges_issues import (
    count_merged_pulls,
    count_closed_issues,
    count_new_pulls,
    count_new_issues,
    count_open_issues_at_date,
    count_open_prs_at_date
)
from forks_stars import count_forks, count_stars
from comments_reactions import count_issue_comments, count_all_reactions

###############################################################################
# We'll capture console => debug_log
###############################################################################
original_stdout= sys.stdout
log_capture= io.StringIO()

class DualOutput:
    def write(self, text):
        original_stdout.write(text)
        log_capture.write(text)
    def flush(self):
        original_stdout.flush()
        log_capture.flush()

# Route all stdout => the dual output
sys.stdout= DualOutput()

###############################################################################
# BFS Print Helpers
###############################################################################
def print_aligned_table(table_data, alignments=None):
    """
    Utility to print a 2D array (list of lists) with aligned columns.
    """
    if not table_data:
        return
    num_cols = len(table_data[0])
    if alignments is None:
        alignments= ['left']* num_cols
    if len(alignments)< num_cols:
        alignments+= ['left']* (num_cols- len(alignments))

    # compute column widths
    col_widths= [0]* num_cols
    for row in table_data:
        for c_idx, cell in enumerate(row):
            c_str= str(cell)
            if len(c_str)> col_widths[c_idx]:
                col_widths[c_idx]= len(c_str)

    def format_cell(cell_str, width, alignment):
        if alignment=='left':
            return cell_str.ljust(width)
        elif alignment=='center':
            pad= width- len(cell_str)
            left_pad= pad//2
            right_pad= pad- left_pad
            return ' '*left_pad+ cell_str+ ' '*right_pad
        else:
            # right
            return cell_str.rjust(width)

    # header line
    header_line = " | ".join(
        format_cell(str(table_data[0][i]), col_widths[i], alignments[i])
        for i in range(num_cols)
    )
    print(header_line)

    # separator
    sep_line= "-+-".join("-"*col_widths[i] for i in range(num_cols))
    print(sep_line)

    # content rows
    for row in table_data[1:]:
        row_line= " | ".join(
            format_cell(str(row[i]), col_widths[i], alignments[i])
            for i in range(num_cols)
        )
        print(row_line)

###############################################################################
# Helper to identify a quarter label based on largest overlap with Q1..Q4
###############################################################################
def find_fy(d):
    """Return the fiscal year for date d. If month>=10 => fy=d.year+1, else d.year."""
    if d.month >= 10:
        return d.year + 1
    return d.year

def quarter_fy_ranges(fy):
    import datetime
    return {
      "Q1": (datetime.datetime(fy-1,10,1), datetime.datetime(fy-1,12,31,23,59,59)),
      "Q2": (datetime.datetime(fy,1,1), datetime.datetime(fy,3,31,23,59,59)),
      "Q3": (datetime.datetime(fy,4,1), datetime.datetime(fy,6,30,23,59,59)),
      "Q4": (datetime.datetime(fy,7,1), datetime.datetime(fy,9,30,23,59,59)),
    }

def largest_overlap_quarter(st, ed):
    """
    Return Q1..Q4 for the date range [st,ed] based on largest time overlap 
    with those quarters in the same fiscal year.
    """
    fy = find_fy(st)
    Q = quarter_fy_ranges(fy)
    best_lbl = "Q?"
    best_ov = 0
    for qlbl,(qs,qe) in Q.items():
        overlap_s= max(st, qs)
        overlap_e= min(ed, qe)
        overlap_sec= (overlap_e - overlap_s).total_seconds()
        if overlap_sec> best_ov:
            best_ov= overlap_sec
            best_lbl= qlbl
    return best_lbl

###############################################################################
# produce_chart_with_table => single variable bar chart + side table
###############################################################################
def produce_chart_with_table(
    quarter_ranges, bar_values, 
    scaling_repo, all_repos, oldest_map,
    chart_title, filename
):
    """
    Creates a figure with:
     1) A bar chart for 'bar_values'
     2) A side table listing each repo, oldest_date, and last window end
     3) X-axis labeled by quarter from 'largest_overlap_quarter'
    Saves to 'filename' as .png
    """
    import matplotlib.pyplot as plt
    from matplotlib.table import Table

    fig= plt.figure(figsize=(14,8))
    ax_chart= fig.add_axes([0.05,0.1,0.55,0.8])  # left half
    ax_table= fig.add_axes([0.65,0.1,0.3,0.8])   # right half
    ax_table.set_axis_off()

    # build X labels from partial overlap
    x_labels=[]
    for (st,ed,part_f) in quarter_ranges:
        qlbl= largest_overlap_quarter(st, ed)
        x_labels.append(qlbl)
    x= np.arange(len(quarter_ranges))

    ax_chart.bar(x, bar_values, 0.6, color='steelblue')
    ax_chart.set_title(chart_title)
    ax_chart.set_xticks(x)
    ax_chart.set_xticklabels(x_labels, rotation=0)

    # build the small table
    tbl= Table(ax_table, bbox=[0,0,1,1])
    col_labels= ["Repo","OldestDate","WindowEnd"]
    table_data= [col_labels]

    # top => scaling
    if scaling_repo in oldest_map:
        (odt, wend, pf)= oldest_map[scaling_repo]
        od_str= odt.strftime("%Y-%m-%d %H:%M")
        we_str= wend.strftime("%Y-%m-%d %H:%M")
        if pf:
            we_str+= " (partial)"
        table_data.append([scaling_repo+" (scaling)", od_str, we_str])

    # non-scaling
    for rp in all_repos:
        if rp== scaling_repo:
            continue
        if rp not in oldest_map:
            continue
        (odt, wend, pff)= oldest_map[rp]
        od_str= odt.strftime("%Y-%m-%d %H:%M")
        we_str= wend.strftime("%Y-%m-%d %H:%M")
        if pff:
            we_str+= " (partial)"
        table_data.append([rp, od_str, we_str])

    # layout table cells
    nrows= len(table_data)
    ncols= len(table_data[0])
    row_h= 1.0/ nrows
    col_w= 1.0/ ncols

    for irow in range(nrows):
        for icol in range(ncols):
            cell_txt= table_data[irow][icol]
            cell= tbl.add_cell(
                row=irow, col=icol,
                width=col_w, height=row_h,
                text= cell_txt,
                loc='center',
                facecolor='white'
            )
            if irow==0:
                cell.set_facecolor('lightgray')
                cell._text.set_weight('bold')
            cell._text.set_fontsize(9)

    ax_table.add_table(tbl)
    ax_table.set_xlim(0,1)
    ax_table.set_ylim(0,1)

    fig.savefig(filename)
    plt.close(fig)
    print(f"[INFO] Created {filename}")

###############################################################################
# BFS_print_repo => BFS console prints for raw & aggregator
###############################################################################
def BFS_print_repo(
    repo, 
    mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates
):
    """
    BFS console prints for:
      1) Raw BFS => mergesRaw, closedRaw, forksRaw, etc.
      2) Aggregator BFS => openIssueRatio, openPRRatio, velocity, uig, mac, sei
    """
    print(f"=== BFS for Repo: {repo} ===")

    # If the repo is not in mergesFactor => pseudo-repo => skip factor line
    if repo in mergesFactor:
        print(f"[RAW BFS] (mergesFactor={mergesFactor[repo]:.4f}, closedFactor={closedFactor[repo]:.4f}, etc.)")
    else:
        print("[RAW BFS] (pseudo-repo, no scale-factors)")

    BFS_data= [[
       "Q-Range","mergesRaw","closedRaw","forksRaw","starsRaw",
       "newIssRaw","commentsRaw","reactRaw","pullRaw"
    ]]
    BFS_align= ["left"]+ ["center"]*8

    if repo not in quarter_dates:
        print(f"[WARN] BFS_print_repo: No quarter_dates for {repo}\n")
        return

    sorted_quarters= sorted(quarter_dates[repo].keys())
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"

        mg= merges_data.get(repo,{}).get(q_idx,0.0)
        cl= closed_data.get(repo,{}).get(q_idx,0.0)
        fo= forks_data.get(repo,{}).get(q_idx,0.0)
        st= stars_data.get(repo,{}).get(q_idx,0.0)
        ni= newIss_data.get(repo,{}).get(q_idx,0.0)
        co= comm_data.get(repo,{}).get(q_idx,0.0)
        re= reac_data.get(repo,{}).get(q_idx,0.0)
        pu= pull_data.get(repo,{}).get(q_idx,0.0)

        BFS_data.append([
           label_str,
           f"{mg}", f"{cl}", f"{fo}", f"{st}",
           f"{ni}", f"{co}", f"{re}", f"{pu}"
        ])

    print_aligned_table(BFS_data, BFS_align)
    print()

    # aggregator BFS => velocity, uig, mac, sei
    print(f"[AGGREGATOR BFS]")
    ABFS= [[
      "Q-Range","openIssRatio","openPRRatio","Velocity","UIG","MAC","SEI"
    ]]
    ABFS_align= ["left"]+ ["center"]*6

    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"

        oi= issueRatio_data.get(repo,{}).get(q_idx,1.0)
        op= prRatio_data.get(repo,{}).get(q_idx,1.0)
        vel= velocity_data.get(repo,{}).get(q_idx,0.0)
        ui= uig_data.get(repo,{}).get(q_idx,0.0)
        mc= mac_data.get(repo,{}).get(q_idx,0.0)
        se= sei_data.get(repo,{}).get(q_idx,0.0)
        ABFS.append([
            label_str,
            f"{oi:.3f}", f"{op:.3f}",
            f"{vel:.3f}", f"{ui:.3f}", f"{mc:.3f}", f"{se:.3f}"
        ])

    print_aligned_table(ABFS, ABFS_align)
    print("------------------------------------------------------\n")

###############################################################################
# compute_non_scaling_target => aggregator target for BFS
###############################################################################
def compute_non_scaling_target(
    scaling_repo, all_repos,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data
):
    """
    Builds a pseudo-repo "TARGET(avgNonScaling)" that average aggregator metrics across non-scaling.
    Also defines empty merges_data[...] so BFS_print_repo won't KeyError.
    """
    target_name= "TARGET(avgNonScaling)"
    velocity_data[target_name]= {}
    uig_data[target_name]= {}
    mac_data[target_name]= {}
    sei_data[target_name]= {}
    issueRatio_data[target_name]= {}
    prRatio_data[target_name]= {}

    merges_data[target_name]= {}
    closed_data[target_name]= {}
    forks_data[target_name]= {}
    stars_data[target_name]= {}
    newIss_data[target_name]= {}
    comm_data[target_name]= {}
    reac_data[target_name]= {}
    pull_data[target_name]= {}

    quarter_dates[target_name]= {}

    union_q_idx= set()
    non_scalers= [r for r in all_repos if r!=scaling_repo]

    for nr in non_scalers:
        if nr in quarter_dates:
            union_q_idx.update(quarter_dates[nr].keys())

    union_q_idx= sorted(union_q_idx)
    for q_idx in union_q_idx:
        sum_v=0.0; sum_u=0.0; sum_m=0.0; sum_s=0.0
        sum_oi=0.0; sum_pr=0.0
        ccount=0
        partial_any=False
        st_list= []
        ed_list= []
        for nr in non_scalers:
            if nr not in quarter_dates:
                continue
            if q_idx in quarter_dates[nr]:
                (qs,qe,pf)= quarter_dates[nr][q_idx]
                st_list.append(qs)
                ed_list.append(qe)
                if pf:
                    partial_any= True

                val_v= velocity_data[nr].get(q_idx,0.0)
                val_u= uig_data[nr].get(q_idx,0.0)
                val_m= mac_data[nr].get(q_idx,0.0)
                val_s= sei_data[nr].get(q_idx,0.0)
                val_oi= issueRatio_data[nr].get(q_idx,1.0)
                val_pr= prRatio_data[nr].get(q_idx,1.0)

                sum_v+= val_v
                sum_u+= val_u
                sum_m+= val_m
                sum_s+= val_s
                sum_oi+= val_oi
                sum_pr+= val_pr
                ccount+=1

        if ccount>0 and st_list and ed_list:
            avg_v= sum_v/ ccount
            avg_u= sum_u/ ccount
            avg_m= sum_m/ ccount
            avg_s= sum_s/ ccount
            avg_oi= sum_oi/ ccount
            avg_pr= sum_pr/ ccount

            sdt= min(st_list)
            edt= max(ed_list)
            quarter_dates[target_name][q_idx]= (sdt, edt, partial_any)
            velocity_data[target_name][q_idx]= avg_v
            uig_data[target_name][q_idx]= avg_u
            mac_data[target_name][q_idx]= avg_m
            sei_data[target_name][q_idx]= avg_s
            issueRatio_data[target_name][q_idx]= avg_oi
            prRatio_data[target_name][q_idx]= avg_pr

    return target_name

###############################################################################
def build_ranges_for_repo(r, data_dict, quarter_dates):
    if r not in quarter_dates:
        return [],[]
    qk= sorted(quarter_dates[r].keys())
    qr=[]; vals=[]
    for qi in qk:
        (qs,qe,pf)= quarter_dates[r][qi]
        qr.append((qs,qe,pf))
        vals.append(data_dict[r].get(qi,0.0))
    return qr, vals

###############################################################################
def main():
    # Environment reading
    env_scaling= os.environ.get("SCALING_REPO","<not set>")
    env_quarters= os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
    print("=== ENVIRONMENT VARIABLES ===")
    print(f"SCALING_REPO={env_scaling}")
    print(f"NUM_FISCAL_QUARTERS={env_quarters}\n")
    print("=== CAPTURED CONSOLE OUTPUT ===\n")

    # Potentially user-defined
    repos= [
        "ni/labview-icon-editor",
        "facebook/react",
        "tensorflow/tensorflow",
        "dotnet/core"
    ]
    scaling_repo= get_scaling_repo() or "ni/labview-icon-editor"
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    q_count= get_num_fiscal_quarters() or 4
    aggregator_weights= load_aggregator_weights()

    # scale_factors => mergesFactor, closedFactor, etc.
    (sfM,sfCl,sfF,sfS,sfNi,sfCo,sfRe,sfP)= compute_scale_factors(scaling_repo, repos)

    now= datetime.utcnow()

    # aggregator data
    velocity_data={} ; uig_data={} ; mac_data={} ; sei_data={}
    issueRatio_data={} ; prRatio_data={}

    # raw data
    merges_data={} ; closed_data={} ; forks_data={} ; stars_data={}
    newIss_data={} ; comm_data={} ; reac_data={} ; pull_data={}

    quarter_dates={}
    oldest_map={}

    # 1) Gather BFS + aggregator
    for r in repos:
        # aggregator
        velocity_data[r]= {}
        uig_data[r]= {}
        mac_data[r]= {}
        sei_data[r]= {}
        issueRatio_data[r]= {}
        prRatio_data[r]= {}

        # raw
        merges_data[r]= {}
        closed_data[r]= {}
        forks_data[r]= {}
        stars_data[r]= {}
        newIss_data[r]= {}
        comm_data[r]= {}
        reac_data[r]= {}
        pull_data[r]= {}

        oldest= find_oldest_date_for_repo(r)
        if not oldest:
            print(f"[WARN] No data => {r}")
            continue

        raw_quarters= generate_quarter_windows(oldest, q_count)
        quarter_dates[r]={}
        idx=1
        final_end= oldest
        partial_any=False

        for (qs,qe) in raw_quarters:
            if qs> now:
                break
            p_flag= False
            if qe> now:
                p_flag= True
                qe= now
            if qs>=qe:
                continue

            mg= count_merged_pulls(r, qs, qe)
            cl= count_closed_issues(r, qs, qe)
            fo= count_forks(r, qs, qe)
            st= count_stars(r, qs, qe)
            ni= count_new_issues(r, qs, qe)
            co= count_issue_comments(r, qs, qe)
            re= count_all_reactions(r, qs, qe)
            pu= count_new_pulls(r, qs, qe)

            merges_data[r][idx]= mg
            closed_data[r][idx]= cl
            forks_data[r][idx]= fo
            stars_data[r][idx]= st
            newIss_data[r][idx]= ni
            comm_data[r][idx]= co
            reac_data[r][idx]= re
            pull_data[r][idx]= pu

            # scaled
            mg_s= mg* sfM[r]
            cl_s= cl* sfCl[r]
            fo_s= fo* sfF[r]
            st_s= st* sfS[r]
            ni_s= ni* sfNi[r]
            co_s= co* sfCo[r]
            re_s= re* sfRe[r]
            pu_s= pu* sfP[r]

            # openIssueRatio
            oIss_start= count_open_issues_at_date(r, qs)
            oIss_end= count_open_issues_at_date(r, qe)
            oIss_avg= (oIss_start + oIss_end)/2
            denom_iss= oIss_avg + cl
            if denom_iss< 1e-9:
                issRatio= 1.0
            else:
                issRatio= oIss_avg/ denom_iss
            issueRatio_data[r][idx]= issRatio

            # openPRRatio
            oPR_start= count_open_prs_at_date(r, qs)
            oPR_end= count_open_prs_at_date(r, qe)
            oPR_avg= (oPR_start + oPR_end)/2
            denom_pr= oPR_avg + mg
            if denom_pr<1e-9:
                prRat= 1.0
            else:
                prRat= oPR_avg/ denom_pr
            prRatio_data[r][idx]= prRat

            # aggregator
            vel= aggregator_velocity(mg_s, cl_s, issRatio, prRat, aggregator_weights)
            ui= aggregator_uig(fo_s, st_s)
            ma= aggregator_mac(ni_s, co_s, re_s, pu_s, aggregator_weights)
            se= 0.5* ma + 0.3* vel + 0.2* ui  # standard for SEI

            velocity_data[r][idx]= vel
            uig_data[r][idx]= ui
            mac_data[r][idx]= ma
            sei_data[r][idx]= se

            quarter_dates[r][idx]= (qs,qe,p_flag)
            final_end= qe
            if p_flag:
                partial_any= True

            idx+=1

        oldest_map[r]= (oldest, final_end, partial_any)

    # 2) BFS for each repo
    for r in repos:
        if r not in quarter_dates:
            continue
        if not quarter_dates[r]:
            continue
        BFS_print_repo(
            repo=r,
            mergesFactor= sfM, closedFactor= sfCl, forksFactor= sfF, starsFactor= sfS,
            newIssuesFactor= sfNi, commentsFactor= sfCo, reactionsFactor= sfRe, pullsFactor= sfP,
            merges_data= merges_data, closed_data= closed_data, forks_data= forks_data, stars_data= stars_data,
            newIss_data= newIss_data, comm_data= comm_data, reac_data= reac_data, pull_data= pull_data,
            velocity_data= velocity_data, uig_data= uig_data, mac_data= mac_data, sei_data= sei_data,
            issueRatio_data= issueRatio_data, prRatio_data= prRatio_data,
            quarter_dates= quarter_dates
        )

    # 3) build "TARGET" aggregator
    target_name= compute_non_scaling_target(
        scaling_repo, repos,
        velocity_data, uig_data, mac_data, sei_data,
        issueRatio_data, prRatio_data,
        quarter_dates,
        merges_data, closed_data, forks_data, stars_data,
        newIss_data, comm_data, reac_data, pull_data
    )

    # BFS for pseudo-repo => "TARGET(avgNonScaling)"
    BFS_print_repo(
        repo= target_name,
        mergesFactor= sfM, closedFactor= sfCl, forksFactor= sfF, starsFactor= sfS,
        newIssuesFactor= sfNi, commentsFactor= sfCo, reactionsFactor= sfRe, pullsFactor= sfP,
        merges_data= merges_data, closed_data= closed_data, forks_data= forks_data, stars_data= stars_data,
        newIss_data= newIss_data, comm_data= comm_data, reac_data= reac_data, pull_data= pull_data,
        velocity_data= velocity_data, uig_data= uig_data, mac_data= mac_data, sei_data= sei_data,
        issueRatio_data= issueRatio_data, prRatio_data= prRatio_data,
        quarter_dates= quarter_dates
    )

    print("\n=== Done BFS aggregator. Now produce separate PNGs for raw & aggregator. ===\n")

    # 4) produce separate PNG for each raw variable
    raw_vars= {
       "Merges": merges_data,
       "Closed": closed_data,
       "Forks": forks_data,
       "Stars": stars_data,
       "NewIssues": newIss_data,
       "Comments": comm_data,
       "Reactions": reac_data,
       "Pulls": pull_data
    }

    scaling_repo_final= scaling_repo

    def build_ranges_for_r(r, datadict):
        if r not in quarter_dates:
            return [],[]
        qk= sorted(quarter_dates[r].keys())
        qr=[]; vals=[]
        for qi in qk:
            (qs,qe,pf)= quarter_dates[r][qi]
            qr.append((qs,qe,pf))
            vals.append(datadict[r].get(qi,0.0))
        return qr, vals

    for rv_label, rv_dict in raw_vars.items():
        qrng, barv= build_ranges_for_r(scaling_repo_final, rv_dict)
        produce_chart_with_table(
          quarter_ranges= qrng,
          bar_values= barv,
          scaling_repo= scaling_repo_final,
          all_repos= repos,
          oldest_map= oldest_map,
          chart_title= f"{rv_label} ({scaling_repo_final}) - Fiscal Quarters",
          filename= f"{rv_label.lower()}_fiscal.png"
        )

    # 5) aggregator => velocity, uig, mac, sei
    def aggregator_build(r, dct):
        if r not in quarter_dates:
            return [], []
        qk= sorted(quarter_dates[r].keys())
        Qs=[]; Vs=[]
        for qi in qk:
            (qs,qe,pf)= quarter_dates[r][qi]
            Qs.append((qs,qe,pf))
            Vs.append(dct[r].get(qi,0.0))
        return Qs,Vs

    # velocity
    vQ,vV= aggregator_build(scaling_repo_final, velocity_data)
    produce_chart_with_table(
      quarter_ranges= vQ, bar_values= vV,
      scaling_repo= scaling_repo_final, all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"Velocity ({scaling_repo_final}) - Fiscal",
      filename= "velocity_fiscal.png"
    )

    # mac
    mQ,mV= aggregator_build(scaling_repo_final, mac_data)
    produce_chart_with_table(
      quarter_ranges= mQ, bar_values= mV,
      scaling_repo= scaling_repo_final, all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"MAC ({scaling_repo_final}) - Fiscal",
      filename= "mac_fiscal.png"
    )

    # uig
    uQ,uV= aggregator_build(scaling_repo_final, uig_data)
    produce_chart_with_table(
      quarter_ranges= uQ, bar_values= uV,
      scaling_repo= scaling_repo_final, all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"UIG ({scaling_repo_final}) - Fiscal",
      filename= "uig_fiscal.png"
    )

    # sei
    sQ,sV= aggregator_build(scaling_repo_final, sei_data)
    produce_chart_with_table(
      quarter_ranges= sQ, bar_values= sV,
      scaling_repo= scaling_repo_final, all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"SEI ({scaling_repo_final}) - Fiscal",
      filename= "sei_fiscal.png"
    )

    print("\n=== Done. BFS aggregator + target + raw & aggregator charts. ===")

    # finalize => flush & write debug log
    sys.stdout.flush()
    console_out= log_capture.getvalue()
    sys.stdout= original_stdout

    debug_file= "debug_log.txt"
    if os.path.exists(debug_file):
        os.remove(debug_file)
    with open(debug_file,"w",encoding="utf-8") as f:
        f.write(console_out)

    print(f"[INFO] Overwrote debug_log => {debug_file}")


if __name__=="__main__":
    main()
