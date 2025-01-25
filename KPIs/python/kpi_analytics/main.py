#!/usr/bin/env python3
"""
main.py - BFS aggregator + side-by-side raw variable charts (scaling vs. non-scaling target)
with a ratio label above the scaling bar.

We keep BFS aggregator prints in the console unchanged. For raw variable PNGs,
we now produce side-by-side bars (scaling vs. 'TARGET(avgNonScaling)') and label partial coverage.
We also place (scaling/target)*100.0 above scaling bar, or 'N/A' if target=0.

We keep the same filenames: merges_raw.png, closed_raw.png, etc., overwriting the old single-bar style.
"""

import sys
import os
import io
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

# We assume these modules exist
from config import get_scaling_repo, get_num_fiscal_quarters
from aggregator import (
    load_aggregator_weights,
    velocity as aggregator_velocity,
    user_interest_growth as aggregator_uig,
    monthly_active_contributors as aggregator_mac
)
from scale_factors import (
    compute_scale_factors,
    compute_sei_data
)
from baseline import find_oldest_date_for_repo
from quarters import generate_quarter_windows
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


############################################
# CAPTURE CONSOLE => debug_log
############################################
original_stdout= sys.stdout
log_capture= io.StringIO()

class DualOutput:
    def write(self, text):
        original_stdout.write(text)
        log_capture.write(text)
    def flush(self):
        original_stdout.flush()
        log_capture.flush()

sys.stdout= DualOutput()

############################################
# PRINT HELPERS
############################################
def print_aligned_table(table_data, alignments=None):
    if not table_data:
        return
    num_cols= len(table_data[0])
    if alignments is None:
        alignments= ['left']* num_cols
    if len(alignments)< num_cols:
        alignments+= ['left']* (num_cols- len(alignments))

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
            return cell_str.rjust(width)

    # header
    header_line= " | ".join(
        format_cell(str(table_data[0][i]), col_widths[i], alignments[i])
        for i in range(num_cols)
    )
    print(header_line)
    sep_line= "-+-".join("-"*col_widths[i] for i in range(num_cols))
    print(sep_line)

    # rows
    for row in table_data[1:]:
        row_line= " | ".join(
            format_cell(str(row[i]), col_widths[i], alignments[i])
            for i in range(num_cols)
        )
        print(row_line)

def find_fy(d):
    if d.month>=10:
        return d.year+1
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
    fy= find_fy(st)
    Q= quarter_fy_ranges(fy)
    best_lbl="Q?"
    best_ov= 0
    for qlbl,(qs,qe) in Q.items():
        overlap_s= max(st, qs)
        overlap_e= min(ed, qe)
        overlap_sec= (overlap_e - overlap_s).total_seconds()
        if overlap_sec> best_ov:
            best_ov= overlap_sec
            best_lbl= qlbl
    return best_lbl

############################################
# BFS PRINT REPO
############################################
def BFS_print_repo(
    repo, mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates
):
    print(f"=== BFS for Repo: {repo} ===")

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


############################################
# compute_non_scaling_target for aggregator
############################################
def compute_non_scaling_target(
    scaling_repo, all_repos,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data
):
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

############################################
# Additional: compute_non_scaling_raw for merges, forks, etc.
############################################
def compute_non_scaling_raw_target(
    scaling_repo, all_repos,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    quarter_dates
):
    """
    Creates a pseudo-repo "TARGETraw(avgNonScaling)" for each raw variable 
    by averaging the non-scaling repos. We'll unify quarter indexes => if 
    a quarter is missing for a repo => skip. Then BFS average.

    We'll store merges_data["TARGETraw(avgNonScaling)"][q_idx] = average merges etc.
    Return that target name.
    """
    target_name= "TARGETraw(avgNonScaling)"
    merges_data[target_name]= {}
    closed_data[target_name]= {}
    forks_data[target_name]= {}
    stars_data[target_name]= {}
    newIss_data[target_name]= {}
    comm_data[target_name]= {}
    reac_data[target_name]= {}
    pull_data[target_name]= {}
    quarter_dates[target_name]= {}

    union_q= set()
    non_scalers= [r for r in all_repos if r!=scaling_repo]
    # gather all q_idx from non-scaling
    for nr in non_scalers:
        if nr in quarter_dates:
            union_q.update(quarter_dates[nr].keys())

    union_q= sorted(union_q)
    for q_idx in union_q:
        sumM=0.0; sumC=0.0; sumF=0.0; sumS=0.0
        sumNi=0.0; sumCo=0.0; sumRe=0.0; sumPu=0.0
        ccount=0
        partial_any= False
        st_list=[]
        ed_list=[]
        for nr in non_scalers:
            if nr not in quarter_dates:
                continue
            if q_idx in quarter_dates[nr]:
                (qs,qe,pf)= quarter_dates[nr][q_idx]
                st_list.append(qs)
                ed_list.append(qe)
                if pf: partial_any= True
                mg= merges_data[nr].get(q_idx,0.0)
                cl= closed_data[nr].get(q_idx,0.0)
                fo= forks_data[nr].get(q_idx,0.0)
                stv= stars_data[nr].get(q_idx,0.0)
                ni= newIss_data[nr].get(q_idx,0.0)
                co= comm_data[nr].get(q_idx,0.0)
                re= reac_data[nr].get(q_idx,0.0)
                pu= pull_data[nr].get(q_idx,0.0)
                sumM+= mg
                sumC+= cl
                sumF+= fo
                sumS+= stv
                sumNi+= ni
                sumCo+= co
                sumRe+= re
                sumPu+= pu
                ccount+=1

        if ccount>0 and st_list and ed_list:
            avgM= sumM/ ccount
            avgC= sumC/ ccount
            avgF= sumF/ ccount
            avgS= sumS/ ccount
            avgNi= sumNi/ ccount
            avgCo= sumCo/ ccount
            avgRe= sumRe/ ccount
            avgPu= sumPu/ ccount

            sdt= min(st_list)
            edt= max(ed_list)
            quarter_dates[target_name][q_idx]= (sdt, edt, partial_any)
            merges_data[target_name][q_idx]= avgM
            closed_data[target_name][q_idx]= avgC
            forks_data[target_name][q_idx]= avgF
            stars_data[target_name][q_idx]= avgS
            newIss_data[target_name][q_idx]= avgNi
            comm_data[target_name][q_idx]= avgCo
            reac_data[target_name][q_idx]= avgRe
            pull_data[target_name][q_idx]= avgPu

    return target_name

############################################
def produce_side_by_side_raw(
    var_label, # e.g. "Merges"
    scaling_repo, target_repo,
    raw_data,   # e.g. merges_data
    quarter_dates,
    all_repos,
    oldest_map,
    filename
):
    """
    Side-by-side bar chart for a raw variable. 
    2 bars: scaling vs. target for each quarter. 
    With ratio label above scaling bar. If target=0 => 'N/A'.
    partial coverage labeling => unify quarter indexes.
    """
    import matplotlib.pyplot as plt

    # unify quarter indexes
    qset= set()
    if scaling_repo in quarter_dates:
        qset.update(quarter_dates[scaling_repo].keys())
    if target_repo in quarter_dates:
        qset.update(quarter_dates[target_repo].keys())
    sorted_q= sorted(qset)

    scaling_vals=[]
    target_vals=[]
    labels=[]

    for q_idx in sorted_q:
        sc_val= raw_data[scaling_repo].get(q_idx,0.0) if scaling_repo in raw_data else 0.0
        tg_val= raw_data[target_repo].get(q_idx,0.0) if target_repo in raw_data else 0.0
        # partial coverage?
        sp= False
        # gather intervals
        (sqs,sqe,spf)= (None,None,False)
        if scaling_repo in quarter_dates and q_idx in quarter_dates[scaling_repo]:
            (sqs,sqe,spf)= quarter_dates[scaling_repo][q_idx]
        (tqs,tqe,tpf)= (None,None,False)
        if target_repo in quarter_dates and q_idx in quarter_dates[target_repo]:
            (tqs,tqe,tpf)= quarter_dates[target_repo][q_idx]
        if spf or tpf:
            sp= True
        # unify intervals
        allS= [];allE=[]
        if sqs: allS.append(sqs)
        if tqs: allS.append(tqs)
        if sqe: allE.append(sqe)
        if tqe: allE.append(tqe)
        if allS and allE:
            st= min(allS)
            ed= max(allE)
        else:
            st= datetime(2000,1,1)
            ed= datetime(2000,1,1)

        # label
        qlbl= largest_overlap_quarter(st,ed)
        if sp:
            qlbl+= "(partial)"

        scaling_vals.append(sc_val)
        target_vals.append(tg_val)
        labels.append(qlbl)

    x= np.arange(len(sorted_q))
    width=0.35

    fig,ax= plt.subplots(figsize=(12,6))
    ax.set_title(f"{var_label} Compare: Scaling vs. Target")
    bar_s= ax.bar(x - width/2, scaling_vals, width, label=scaling_repo, color='steelblue')
    bar_t= ax.bar(x + width/2, target_vals, width, label=target_repo, color='orange')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    # ratio label on scaling bars
    for i,rect in enumerate(bar_s):
        scv= scaling_vals[i]
        tgv= target_vals[i]
        if abs(tgv)<1e-9:
            ratio_str= "N/A"
        else:
            ratio_f= (scv/tgv)*100.0
            ratio_str= f"{ratio_f:.2f}%"
        height= rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2, height+0.05*max(1.0,height),
            ratio_str, ha='center', va='bottom', fontsize=9)

    # Make a side table if you want, or replicate aggregator logic
    # For simplicity, let's replicate aggregator approach
    # We'll show scaling + non-scaling repos date range
    from matplotlib.table import Table
    ax_table= fig.add_axes([0.7,0.1,0.28,0.8])
    ax_table.set_axis_off()

    tbl= Table(ax_table,bbox=[0,0,1,1])
    col_labels= ["Repo","OldestDate","WindowEnd"]
    table_data= [col_labels]
    # top => scaling
    if scaling_repo in oldest_map:
        (odt, wend, pf)= oldest_map[scaling_repo]
        od_str= odt.strftime("%Y-%m-%d %H:%M")
        we_str= wend.strftime("%Y-%m-%d %H:%M")
        if pf:
            we_str+= " (partial)"
        table_data.append([scaling_repo+"(scaling)", od_str, we_str])
    # non-scaling
    # or just show the entire set of repos?
    for rp in all_repos:
        if rp== scaling_repo: 
            continue
        if rp not in oldest_map:
            continue
        (odt, wend, pf)= oldest_map[rp]
        od_str= odt.strftime("%Y-%m-%d %H:%M")
        we_str= wend.strftime("%Y-%m-%d %H:%M")
        if pf:
            we_str+= " (partial)"
        table_data.append([rp, od_str, we_str])

    nrows= len(table_data)
    ncols= len(table_data[0])
    row_h= 1.0/ nrows
    col_w= 1.0/ ncols

    for irow in range(nrows):
        for icol in range(ncols):
            ctxt= table_data[irow][icol]
            cell= tbl.add_cell(irow, icol,
               width=col_w, height=row_h,
               text= ctxt, loc='center', facecolor='white'
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
    print(f"[INFO] Created side-by-side raw => {filename}")

############################################
def main():
    env_scaling= os.environ.get("SCALING_REPO","<not set>")
    env_quarters= os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
    print("=== ENVIRONMENT VARIABLES ===")
    print(f"SCALING_REPO={env_scaling}")
    print(f"NUM_FISCAL_QUARTERS={env_quarters}\n")
    print("=== CAPTURED CONSOLE OUTPUT ===\n")

    repos= ["ni/labview-icon-editor","facebook/react","tensorflow/tensorflow","dotnet/core"]
    scaling_repo= get_scaling_repo() or "ni/labview-icon-editor"
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    q_count= get_num_fiscal_quarters() or 4
    aggregator_weights= load_aggregator_weights()
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

    # gather BFS + aggregator
    for r in repos:
        velocity_data[r]= {}
        uig_data[r]= {}
        mac_data[r]= {}
        sei_data[r]= {}
        issueRatio_data[r]= {}
        prRatio_data[r]= {}

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
            se= 0.5* ma + 0.3* vel + 0.2* ui

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

    # BFS for each repo
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

    # aggregator target for aggregator metrics
    target_name= "TARGET(avgNonScaling)"
    target_name= compute_non_scaling_target(
        scaling_repo, repos,
        velocity_data, uig_data, mac_data, sei_data,
        issueRatio_data, prRatio_data,
        quarter_dates,
        merges_data, closed_data, forks_data, stars_data,
        newIss_data, comm_data, reac_data, pull_data
    )

    # raw target
    raw_target_name= "TARGETraw(avgNonScaling)"
    raw_target_name= compute_non_scaling_raw_target(
        scaling_repo, repos,
        merges_data, closed_data, forks_data, stars_data,
        newIss_data, comm_data, reac_data, pull_data,
        quarter_dates
    )

    print("\n=== BFS aggregator done. Now produce side-by-side raw & aggregator charts. ===\n")

    ########## side-by-side raw => merges, closed, forks, etc.
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

    for rv_label, rv_dict in raw_vars.items():
        # produce side-by-side
        fname= f"{rv_label.lower()}_raw.png"
        produce_side_by_side_raw(
          var_label= rv_label,
          scaling_repo= scaling_repo,
          target_repo= raw_target_name,
          raw_data= rv_dict,
          quarter_dates= quarter_dates,
          all_repos= repos,
          oldest_map= oldest_map,
          filename= fname
        )

    # aggregator side-by-side => velocity, mac, uig, sei => omitted here if you only want raw side-by-side
    # but if you still want aggregator side by side:
    # [similar approach as before, or unify aggregator approach, etc.]

    print("\n=== Done all side-by-side raw variable charts. ===")

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
