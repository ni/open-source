#!/usr/bin/env python3
"""
main.py

Orchestrates quarter-based data, aggregator logic, partial coverage, and prints
detailed BFS-style debug prints like the sample provided.

Restores:
 - ENVIRONMENT VARIABLES print
 - BFS style 'Existing Quarter Data for {repo} | (mergesFactor=..., ...)' 
 - Detailed aggregator tables for velocity, UIG, MAC

Generates final SEI chart & prints a final summary.
"""

import sys
import os
import io
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

from config import get_scaling_repo, get_num_fiscal_quarters
from aggregator import (
    load_aggregator_weights,
    velocity,
    user_interest_growth,
    monthly_active_contributors
)
from scale_factors import (
    compute_scale_factors,
    compute_target_reached_data,
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

###############################################################################
# Capture console => debug_log
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

sys.stdout= DualOutput()

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

    # Print header
    header_line= " | ".join(
        format_cell(str(table_data[0][i]), col_widths[i], alignments[i])
        for i in range(num_cols)
    )
    print(header_line)
    sep_line= "-+-".join("-"*col_widths[i] for i in range(num_cols))
    print(sep_line)
    # Print rows
    for row in table_data[1:]:
        row_line= " | ".join(
            format_cell(str(row[i]), col_widths[i], alignments[i])
            for i in range(num_cols)
        )
        print(row_line)

def BFS_print_repo_table(
    repo, mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    velocity_data, uig_data, mac_data,
    quarter_dates
    ):
    """
    Prints:
     Existing Quarter Data for {repo} | (mergesFactor=..., closedFactor=..., etc.)
     Then a BFS table with columns:
      Q-Range, mergesRaw, closedRaw, forksRaw, starsRaw, newIssRaw, newCommRaw, newReactRaw, newPullRaw,
      mergesScaled, closedScaled, forksScaled, starsScaled, newIssScaled, newCommScaled, newReactScaled, newPullScaled,
      Velocity, UIG, MAC
    Then a "detailed aggregator" subtable for velocity, uig, mac.

    partial_flag => if partial, label with (partial).
    """
    # Print factor line
    fac_str= (f"(mergesFactor={mergesFactor[repo]:.4f}, closedFactor={closedFactor[repo]:.4f}, "
              f"forksFactor={forksFactor[repo]:.4f}, starsFactor={starsFactor[repo]:.4f}, "
              f"newIssuesFactor={newIssuesFactor[repo]:.4f}, commentsFactor={commentsFactor[repo]:.4f}, "
              f"reactionsFactor={reactionsFactor[repo]:.4f}, pullsFactor={pullsFactor[repo]:.4f})"
             )
    print(f"Existing Quarter Data for {repo} | {fac_str}")

    # BFS big table
    BFS_data= [[
        "Q-Range","mergesRaw","closedRaw","forksRaw","starsRaw",
        "newIssRaw","newCommRaw","newReactRaw","newPullRaw",
        "mergesScaled","closedScaled","forksScaled","starsScaled",
        "newIssScaled","newCommScaled","newReactScaled","newPullScaled",
        "Velocity","UIG","MAC"
    ]]
    BFS_align= ["left"]+ ["center"]*18

    sorted_quarters= sorted(quarter_dates[repo].keys())
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"

        mergesRaw= merges_data[repo].get(q_idx,0.0)
        closedRaw= closed_data[repo].get(q_idx,0.0)
        forksRaw= forks_data[repo].get(q_idx,0.0)
        starsRaw= stars_data[repo].get(q_idx,0.0)
        newIssRaw= newIss_data[repo].get(q_idx,0.0)
        commRaw= comm_data[repo].get(q_idx,0.0)
        reacRaw= reac_data[repo].get(q_idx,0.0)
        pullRaw= pull_data[repo].get(q_idx,0.0)

        vel= velocity_data[repo].get(q_idx,0.0)
        uig= uig_data[repo].get(q_idx,0.0)
        mac= mac_data[repo].get(q_idx,0.0)

        mergesScale= mergesRaw* mergesFactor[repo]
        closedScale= closedRaw* closedFactor[repo]
        forksScale= forksRaw* forksFactor[repo]
        starsScale= starsRaw* starsFactor[repo]
        newIssScale= newIssRaw* newIssuesFactor[repo]
        commScale= commRaw* commentsFactor[repo]
        reacScale= reacRaw* reactionsFactor[repo]
        pullScale= pullRaw* pullsFactor[repo]

        BFS_data.append([
            label_str,
            f"{mergesRaw}", f"{closedRaw}", f"{forksRaw}", f"{starsRaw}",
            f"{newIssRaw}", f"{commRaw}", f"{reacRaw}", f"{pullRaw}",

            f"{mergesScale:.4f}", f"{closedScale:.4f}", f"{forksScale:.4f}", f"{starsScale:.4f}",
            f"{newIssScale:.4f}", f"{commScale:.4f}", f"{reacScale:.4f}", f"{pullScale:.4f}",

            f"{vel:.4f}", f"{uig:.4f}", f"{mac:.4f}"
        ])

    print_aligned_table(BFS_data, BFS_align)
    print()  # spacing

    # Additional aggregator detail for velocity
    print(f"--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")

    # velocity detail table
    print(f"=== Detailed Calculations for {repo}: Velocity ===")
    vtab= [["Q-Range","mergesScaled","closedScaled","Velocity=0.4*M+0.6*C"]]
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"
        mergesScale= merges_data[repo].get(q_idx,0.0)* mergesFactor[repo]
        closedScale= closed_data[repo].get(q_idx,0.0)* closedFactor[repo]
        vel= velocity_data[repo].get(q_idx,0.0)
        vtab.append([
            label_str, f"{mergesScale:.4f}", f"{closedScale:.4f}", f"{vel:.4f}"
        ])
    print_aligned_table(vtab, ["left","center","center","center"])
    print()

    # uig detail table
    print(f"=== Detailed Calculations for {repo}: UIG ===")
    uitab= [["Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"]]
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"
        forksScale= forks_data[repo].get(q_idx,0.0)* forksFactor[repo]
        starsScale= stars_data[repo].get(q_idx,0.0)* starsFactor[repo]
        uigv= uig_data[repo].get(q_idx,0.0)
        uitab.append([
            label_str, f"{forksScale:.4f}", f"{starsScale:.4f}", f"{uigv:.4f}"
        ])
    print_aligned_table(uitab, ["left","center","center","center"])
    print()

    # mac detail table
    print(f"=== Detailed Calculations for {repo}: MAC ===")
    mctab= [["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]]
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"
        issScale= newIss_data[repo].get(q_idx,0.0)* newIssuesFactor[repo]
        comScale= comm_data[repo].get(q_idx,0.0)* commentsFactor[repo]
        reaScale= reac_data[repo].get(q_idx,0.0)* reactionsFactor[repo]
        su= issScale+ comScale+ reaScale
        pullScale= pull_data[repo].get(q_idx,0.0)* pullsFactor[repo]
        macv= mac_data[repo].get(q_idx,0.0)
        mctab.append([
            label_str,
            f"{su:.4f}",
            f"{pullScale:.4f}",
            f"{macv:.4f}"
        ])
    print_aligned_table(mctab, ["left","center","center","center"])
    print()

def produce_raw_comparison_chart(scaling_repo, metricName, quarter_data, quarter_dates, repos):
    """
    Summaries => compute_target_reached_data, then produce a bar chart for
    'target' vs. scaling repo's metric
    """
    from scale_factors import compute_target_reached_data
    t_dict= compute_target_reached_data(repos, scaling_repo, quarter_data)
    union_q= set()
    for r in repos:
        union_q |= set(quarter_data[r].keys())
    union_q= sorted(union_q)

    x_labels=[]
    target_vals=[]
    scaling_vals=[]

    def quarter_label(rp,q_idx):
        if rp in quarter_dates and q_idx in quarter_dates[rp]:
            (st_dt, en_dt, partial_flag)= quarter_dates[rp][q_idx]
            label_str= f"Q{q_idx}({st_dt:%Y-%m-%d}-{en_dt:%Y-%m-%d})"
            if partial_flag:
                label_str+= "(partial)"
            return label_str
        else:
            return f"Q{q_idx}(No data)"

    for q_idx in union_q:
        avgv, scv, ratio= t_dict.get(q_idx,(0,0,0))
        x_labels.append(quarter_label(scaling_repo,q_idx))
        target_vals.append(avgv)
        scaling_vals.append(scv)

    out_file= f"{metricName.lower()}_comparison_{scaling_repo.replace('/','_')}.png"
    if os.path.exists(out_file):
        os.remove(out_file)

    import numpy as np
    x= np.arange(len(x_labels))
    barw=0.4
    fig, ax= plt.subplots(figsize=(10,6))
    ax.bar(x- barw/2, target_vals, barw, label=f"{metricName} Target", color='steelblue')
    ax.bar(x+ barw/2, scaling_vals, barw, label=f"{metricName} {scaling_repo}", color='orange')
    ax.set_title(f"{metricName} Target vs. {metricName} {scaling_repo}")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha='right')
    ax.set_ylabel(metricName)
    ax.legend()

    plt.tight_layout()
    plt.savefig(out_file)
    plt.close()

    print(f"\n=== {metricName} Target vs. {metricName} {scaling_repo} ===")
    table_data= [
      ["Quarter", f"{metricName} Target", f"{metricName} ({scaling_repo})"]
    ]
    for i,q_idx in enumerate(union_q):
        lbl= x_labels[i]
        tv= f"{target_vals[i]:.2f}"
        sv= f"{scaling_vals[i]:.2f}"
        table_data.append([lbl, tv, sv])
    print_aligned_table(table_data, ["left","center","center"])

def produce_sei_comparison_chart(scaling_repo, velocity_data, uig_data, mac_data, quarter_dates, repos):
    from scale_factors import compute_target_reached_data, compute_sei_data
    vel_tr= compute_target_reached_data(repos, scaling_repo, velocity_data)
    uig_tr= compute_target_reached_data(repos, scaling_repo, uig_data)
    mac_tr= compute_target_reached_data(repos, scaling_repo, mac_data)
    sei_d= compute_sei_data(vel_tr, uig_tr, mac_tr)

    union_q= set()
    for rr in repos:
        union_q |= set(velocity_data[rr].keys())| set(uig_data[rr].keys())| set(mac_data[rr].keys())
    union_q= sorted(union_q)

    # build "seiScaled" for each repo => 0.5*(mac) +0.3*(velocity)+ 0.2*(uig)
    sei_scaled={}
    for r in repos:
        sei_scaled[r]= {}
        uq= set(velocity_data[r].keys())| set(uig_data[r].keys())| set(mac_data[r].keys())
        for q_idx in uq:
            v_s= velocity_data[r].get(q_idx,0.0)
            u_s= uig_data[r].get(q_idx,0.0)
            m_s= mac_data[r].get(q_idx,0.0)
            val= 0.5*m_s + 0.3*v_s + 0.2*u_s
            sei_scaled[r][q_idx]= val

    non_scaling= [rr for rr in repos if rr!=scaling_repo]
    sei_target={}
    for q_idx in union_q:
        sumv= 0.0
        cc=0
        for nr in non_scaling:
            if q_idx in sei_scaled[nr]:
                sumv+= sei_scaled[nr][q_idx]
                cc+=1
        avgv=0.0
        if cc>0:
            avgv= sumv/ cc
        sei_target[q_idx]= avgv

    scaling_sei={}
    for q_idx in union_q:
        scaling_sei[q_idx]= sei_scaled[scaling_repo].get(q_idx,0.0)

    out_file= f"sei_comparison_{scaling_repo.replace('/','_')}.png"
    if os.path.exists(out_file):
        os.remove(out_file)

    x_labels=[]
    t_vals=[]
    s_vals=[]

    def quarter_label(rp,qx):
        if rp in quarter_dates and qx in quarter_dates[rp]:
            (st_dt,en_dt,part)= quarter_dates[rp][qx]
            label_str= f"Q{qx}({st_dt:%Y-%m-%d}-{en_dt:%Y-%m-%d})"
            if part:
                label_str+= "(partial)"
            return label_str
        return f"Q{qx}(No data)"

    sorted_q= sorted(union_q)
    for q_idx in sorted_q:
        x_labels.append( quarter_label(scaling_repo,q_idx) )
        t_vals.append( sei_target[q_idx] )
        s_vals.append( scaling_sei[q_idx] )

    import numpy as np
    x= np.arange(len(x_labels))
    barw=0.4
    fig, ax= plt.subplots(figsize=(10,6))
    ax.bar(x- barw/2, t_vals, barw, label="SEI Target", color='steelblue')
    ax.bar(x+ barw/2, s_vals, barw, label=f"SEI {scaling_repo}", color='orange')
    ax.set_title(f"SEI Target vs. SEI {scaling_repo}")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha='right')
    ax.set_ylabel("SEI Value")
    ax.legend()

    plt.tight_layout()
    plt.savefig(out_file)
    plt.close()

    print(f"\n=== SEI Target vs. SEI {scaling_repo} ===")
    table_data= [
      ["Quarter","SEI Target", f"SEI ({scaling_repo})"]
    ]
    for i,q_idx in enumerate(sorted_q):
        lbl= x_labels[i]
        tv= f"{t_vals[i]:.4f}"
        sv= f"{s_vals[i]:.4f}"
        table_data.append([lbl, tv, sv])
    print_aligned_table(table_data, ["left","center","center"])

def main():
    # 1) Print ENV variables at top
    env_scaling= os.environ.get("SCALING_REPO","<not set>")
    env_quarters= os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
    print("=== ENVIRONMENT VARIABLES ===")
    print(f"SCALING_REPO={env_scaling}")
    print(f"NUM_FISCAL_QUARTERS={env_quarters}\n")

    print("=== CAPTURED CONSOLE OUTPUT ===\n")

    from aggregator import load_aggregator_weights, velocity, user_interest_growth, monthly_active_contributors
    from scale_factors import compute_scale_factors

    # default repos
    repos= ["ni/labview-icon-editor","facebook/react","tensorflow/tensorflow","dotnet/core"]
    scaling_repo= get_scaling_repo() or "ni/labview-icon-editor"
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    q_count= get_num_fiscal_quarters() or 4
    aggregator_weights= load_aggregator_weights()

    # compute scale factors
    (sfM, sfCl, sfF, sfS, sfNi, sfCo, sfRe, sfP)= compute_scale_factors(scaling_repo, repos)

    now= datetime.utcnow()

    # aggregator data structures
    velocity_data={}; uig_data={}; mac_data={}
    merges_data={}; closed_data={}; forks_data={}; stars_data={}
    newIss_data={}; comm_data={}; reac_data={}; pull_data={}
    quarter_dates={}

    for r in repos:
        velocity_data[r]= {}
        uig_data[r]= {}
        mac_data[r]= {}
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
            print(f"[WARN] No data for {r}, skipping aggregator for this repo.\n")
            continue

        raw_quarters= generate_quarter_windows(oldest, q_count)
        quarter_dates[r]={}
        idx=1
        for (qs,qe) in raw_quarters:
            if qs> now:
                break
            partial_flag=False
            if qe> now:
                partial_flag=True
                qe= now
            if qs>=qe:
                continue

            mergesRaw= count_merged_pulls(r, qs, qe)
            closedRaw= count_closed_issues(r, qs, qe)
            forksRaw= count_forks(r, qs, qe)
            starsRaw= count_stars(r, qs, qe)
            newIssRaw= count_new_issues(r, qs, qe)
            commRaw= count_issue_comments(r, qs, qe)
            reacRaw= count_all_reactions(r, qs, qe)
            pullRaw= count_new_pulls(r, qs, qe)

            merges_s= mergesRaw* sfM[r]
            closed_s= closedRaw* sfCl[r]
            forks_s= forksRaw* sfF[r]
            stars_s= starsRaw* sfS[r]
            newIss_s= newIssRaw* sfNi[r]
            comm_s= commRaw* sfCo[r]
            reac_s= reacRaw* sfRe[r]
            pull_s= pullRaw* sfP[r]

            # compute openIssueRatio / openPRRatio
            openIssStart= count_open_issues_at_date(r, qs)
            openIssEnd= count_open_issues_at_date(r, qe)
            openIssAvg= (openIssStart+ openIssEnd)/2
            denom_iss= openIssAvg+ closedRaw
            if denom_iss<1e-9:
                openIssueRatio= 1.0
            else:
                openIssueRatio= openIssAvg/ denom_iss

            openPRStart= count_open_prs_at_date(r, qs)
            openPREnd= count_open_prs_at_date(r, qe)
            openPRAvg= (openPRStart+ openPREnd)/2
            # treat mergesRaw as "closed pr" for ratio
            denom_pr= openPRAvg+ mergesRaw
            if denom_pr<1e-9:
                openPRRatio=1.0
            else:
                openPRRatio= openPRAvg/ denom_pr

            vel= velocity(merges_s, closed_s, openIssueRatio, openPRRatio, aggregator_weights)
            uigv= user_interest_growth(forks_s, stars_s)
            macv= monthly_active_contributors(newIss_s, comm_s, reac_s, pull_s, aggregator_weights)

            velocity_data[r][idx]= vel
            uig_data[r][idx]= uigv
            mac_data[r][idx]= macv

            merges_data[r][idx]= mergesRaw
            closed_data[r][idx]= closedRaw
            forks_data[r][idx]= forksRaw
            stars_data[r][idx]= starsRaw
            newIss_data[r][idx]= newIssRaw
            comm_data[r][idx]= commRaw
            reac_data[r][idx]= reacRaw
            pull_data[r][idx]= pullRaw

            quarter_dates[r][idx]= (qs,qe,partial_flag)
            idx+=1

    # BFS print for each repo
    for r in repos:
        if not quarter_dates[r]:
            continue
        BFS_print_repo_table(
            repo=r,
            mergesFactor= sfM, closedFactor= sfCl, forksFactor= sfF, starsFactor= sfS,
            newIssuesFactor= sfNi, commentsFactor= sfCo, reactionsFactor= sfRe, pullsFactor= sfP,
            merges_data= merges_data, closed_data= closed_data, forks_data= forks_data,
            stars_data= stars_data, newIss_data= newIss_data, comm_data= comm_data,
            reac_data= reac_data, pull_data= pull_data,
            velocity_data= velocity_data, uig_data= uig_data, mac_data= mac_data,
            quarter_dates= quarter_dates
        )

    def produce_all_raw_charts():
        produce_raw_comparison_chart(scaling_repo,"Merges", merges_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"Closed", closed_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"Forks", forks_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"Stars", stars_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"NewIssues", newIss_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"Comments", comm_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"Reactions", reac_data, quarter_dates, repos)
        produce_raw_comparison_chart(scaling_repo,"Pulls", pull_data, quarter_dates, repos)

    produce_all_raw_charts()

    # aggregator => velocity, MAC, UIG, SEI
    produce_raw_comparison_chart(scaling_repo,"Velocity", velocity_data, quarter_dates, repos)
    produce_raw_comparison_chart(scaling_repo,"MAC", mac_data, quarter_dates, repos)
    produce_raw_comparison_chart(scaling_repo,"UIG", uig_data, quarter_dates, repos)
    produce_sei_comparison_chart(scaling_repo, velocity_data, uig_data, mac_data, quarter_dates, repos)

    print("\n=== Final: 12 charts (8 raw + 4 aggregator) generated. ===\n")

    # End-of-script => flush + restore stdout
    sys.stdout.flush()
    console_text= log_capture.getvalue()
    sys.stdout= original_stdout

    # Overwrite debug_log
    debug_file= "debug_log.txt"
    if os.path.exists(debug_file):
        os.remove(debug_file)

    with open(debug_file,"w",encoding="utf-8") as f:
        f.write("=== ENVIRONMENT VARIABLES ===\n")
        env_scaling= os.environ.get("SCALING_REPO","<not set>")
        env_quarters= os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
        f.write(f"SCALING_REPO={env_scaling}\n")
        f.write(f"NUM_FISCAL_QUARTERS={env_quarters}\n\n")

        f.write("=== CAPTURED CONSOLE OUTPUT ===\n")
        f.write(console_text)

    print(f"[INFO] Overwrote debug_log => {debug_file}")

if __name__=="__main__":
    main()
