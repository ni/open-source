#!/usr/bin/env python3
"""
main.py

A production-style script that references scale_factors.py for the 8 separate
raw variable factors, then uses quarter-based analytics (Velocity, UIG, MAC).
Finally, it produces a 'SEI Target vs. SEI' chart with partial coverage.

Requires:
- scale_factors.py in the same folder
- aggregator modules, merges_issues, forks_stars, comments_reactions, config, baseline, quarters
"""

import sys
import os
import io
import math
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

from baseline import find_oldest_date_for_repo
from quarters import generate_quarter_windows
from merges_issues import (
    count_merged_pulls,
    count_closed_issues,
    count_new_pulls,
    count_new_issues
)
from forks_stars import count_forks, count_stars
from comments_reactions import (
    count_issue_comments,
    count_all_reactions
)
from aggregator import (
    velocity,
    user_interest_growth,
    monthly_active_contributors
)
from config import (
    get_scaling_repo,
    get_num_fiscal_quarters
)

# Now we import from scale_factors.py
from scale_factors import (
    compute_scale_factors,
    compute_target_reached_data,
    compute_sei_data
)

###############################################################################
# Capture console output for debug_log
###############################################################################
original_stdout = sys.stdout
log_capture = io.StringIO()

class DualOutput:
    def write(self, text):
        original_stdout.write(text)
        log_capture.write(text)
    def flush(self):
        original_stdout.flush()
        log_capture.flush()

sys.stdout = DualOutput()

###############################################################################
# Utility table printing
###############################################################################
def print_aligned_table(table_data, alignments=None):
    if not table_data:
        return
    num_cols = len(table_data[0])
    if alignments is None:
        alignments = ['left'] * num_cols
    if len(alignments) < num_cols:
        alignments += ['left'] * (num_cols - len(alignments))

    col_widths = [0]*num_cols
    for row in table_data:
        for c_idx, cell in enumerate(row):
            cell_str = str(cell)
            if len(cell_str) > col_widths[c_idx]:
                col_widths[c_idx] = len(cell_str)

    def format_cell(cell_str, width, alignment):
        if alignment == 'left':
            return cell_str.ljust(width)
        elif alignment == 'center':
            pad = width - len(cell_str)
            left_pad = pad // 2
            right_pad = pad - left_pad
            return ' '*left_pad + cell_str + ' '*right_pad
        else:
            return cell_str.rjust(width)

    header_line = " | ".join(
        format_cell(str(table_data[0][i]), col_widths[i], alignments[i])
        for i in range(num_cols)
    )
    print(header_line)
    sep_line = "-+-".join("-"*col_widths[i] for i in range(num_cols))
    print(sep_line)

    for row in table_data[1:]:
        row_line = " | ".join(
            format_cell(str(row[i]), col_widths[i], alignments[i])
            for i in range(num_cols)
        )
        print(row_line)

###############################################################################
# Print existing quarter data with new factors
###############################################################################
def print_existing_quarter_data_table(
    repo,
    mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    quarter_list
):
    """
    Print quarter data: raw merges, closed, forks, stars, new issues, new comments,
    new reactions, new pulls, plus aggregator (Velocity, UIG, MAC) with newly computed scale.
    """
    title_line = [
        f"Existing Quarter Data for {repo}",
        "(mergesFactor={:.4f}, closedFactor={:.4f}, forksFactor={:.4f}, starsFactor={:.4f}, "
        "newIssuesFactor={:.4f}, commentsFactor={:.4f}, reactionsFactor={:.4f}, pullsFactor={:.4f})".format(
            mergesFactor[repo],
            closedFactor[repo],
            forksFactor[repo],
            starsFactor[repo],
            newIssuesFactor[repo],
            commentsFactor[repo],
            reactionsFactor[repo],
            pullsFactor[repo]
        )
    ]
    print(" | ".join(title_line))
    print("=" * (len(" | ".join(title_line))))

    header = [
      "Q-Range",
      "mergesRaw","closedRaw","forksRaw","starsRaw",
      "newIssRaw","newCommRaw","newReactRaw","newPullRaw",
      "mergesScaled","closedScaled","forksScaled","starsScaled",
      "newIssScaled","newCommScaled","newReactScaled","newPullScaled",
      "Velocity","UIG","MAC"
    ]
    align = ["left"] + ["center"]*(len(header)-1)
    table_data = [header]

    from aggregator import velocity, user_interest_growth, monthly_active_contributors

    def no_decimals(x): return f"{x:.0f}"
    def f4(x): return f"{x:.4f}"

    for row_data in quarter_list:
        (q_idx, q_start, q_end,
         mergesRaw, closedRaw, forksRawValue, starsRawValue,
         newIssRawValue, newCommRawValue, newReactRawValue, newPullRawValue,
         old_m_s, old_c_s, old_f_s, old_st_s,
         old_ni_s, old_co_s, old_re_s, old_pu_s,
         old_vel, old_uig, old_mac
        ) = row_data

        # re-scale:
        merges_s= mergesRaw* mergesFactor[repo]
        closed_s= closedRaw* closedFactor[repo]
        forks_s= forksRawValue* forksFactor[repo]
        stars_s= starsRawValue* starsFactor[repo]
        newIss_s= newIssRawValue* newIssuesFactor[repo]
        newComm_s= newCommRawValue* commentsFactor[repo]
        newReact_s= newReactRawValue* reactionsFactor[repo]
        newPull_s= newPullRawValue* pullsFactor[repo]

        vel= velocity(merges_s, closed_s)
        uigv= user_interest_growth(forks_s, stars_s)
        macv= monthly_active_contributors(newIss_s, newComm_s, newReact_s, newPull_s)

        qrange_str= f"Q{q_idx}({q_start:%Y-%m-%d}-{q_end:%Y-%m-%d})"
        row= [
            qrange_str,
            no_decimals(mergesRaw), no_decimals(closedRaw),
            no_decimals(forksRawValue), no_decimals(starsRawValue),
            no_decimals(newIssRawValue), no_decimals(newCommRawValue),
            no_decimals(newReactRawValue), no_decimals(newPullRawValue),

            f4(merges_s), f4(closed_s), f4(forks_s), f4(stars_s),
            f4(newIss_s), f4(newComm_s), f4(newReact_s), f4(newPull_s),
            f4(vel), f4(uigv), f4(macv)
        ]
        table_data.append(row)

    print_aligned_table(table_data, align)

def print_calculation_details(repo, quarter_calcs):
    """
    Print a detailed breakdown for velocity, UIG, MAC computations, using scaled merges, closed, etc.
    """
    header_vel= ["Q-Range","mergesScaled","closedScaled","Velocity=0.4*M+0.6*C"]
    table_vel= [header_vel]
    header_uig= ["Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"]
    table_uig= [header_uig]
    header_mac= ["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]
    table_mac= [header_mac]

    def f4(x): return f"{x:.4f}"

    for row_data in quarter_calcs:
        (q_idx,q_label, merges_s, closed_s, vel,
         forks_s, stars_s, uigv,
         sumICR_s, pull_s, macv) = row_data

        table_vel.append([q_label, f4(merges_s), f4(closed_s), f4(vel)])
        table_uig.append([q_label, f4(forks_s), f4(stars_s), f4(uigv)])
        table_mac.append([q_label, f4(sumICR_s), f4(pull_s), f4(macv)])

    print(f"=== Detailed Calculations for {repo}: Velocity ===")
    print_aligned_table(table_vel, ["left","center","center","center"])

    print(f"\n=== Detailed Calculations for {repo}: UIG ===")
    print_aligned_table(table_uig, ["left","center","center","center"])

    print(f"\n=== Detailed Calculations for {repo}: MAC ===")
    print_aligned_table(table_mac, ["left","center","center","center"])


def generate_oss_sei_chart_and_table(
    scaling_repo,
    velocity_scaled,
    uig_scaled,
    mac_scaled,
    quarter_dates,
    repos
):
    """
    Produce oss_sei_target_{scaling_repo}.png showing SEI Target vs. SEI(scaling_repo).
    Also prints console table "SEI Target vs. SEI {scaling_repo}".
    """
    from scale_factors import compute_sei_data

    # 1) build a "sei_scaled" for each repo => 0.5*mac +0.3*vel +0.2*uig
    sei_scaled={}
    for r in repos:
        sei_scaled[r]= {}
        union_q= set(velocity_scaled[r].keys())| set(uig_scaled[r].keys())| set(mac_scaled[r].keys())
        for q_idx in union_q:
            v_val= velocity_scaled[r].get(q_idx,0.0)
            u_val= uig_scaled[r].get(q_idx,0.0)
            m_val= mac_scaled[r].get(q_idx,0.0)
            val= 0.5*m_val + 0.3*v_val + 0.2*u_val
            sei_scaled[r][q_idx]= val

    # 2) average => "SEI Target"
    non_scaling= [rr for rr in repos if rr!=scaling_repo]
    union_all= set()
    for rr in repos:
        union_all |= set(sei_scaled[rr].keys())
    union_all= sorted(union_all)
    sei_target_dict={}
    for q_idx in union_all:
        sum_v=0.0
        ccount=0
        for r in non_scaling:
            if q_idx in sei_scaled[r]:
                sum_v+= sei_scaled[r][q_idx]
                ccount+=1
        avg_v=0.0
        if ccount>0:
            avg_v= sum_v/ ccount
        sei_target_dict[q_idx]= avg_v

    scaling_sei={}
    for q_idx in union_all:
        scaling_sei[q_idx]= sei_scaled[scaling_repo].get(q_idx,0.0)

    # 3) build chart
    import matplotlib.pyplot as plt
    import numpy as np

    out_file= f"oss_sei_target_{scaling_repo.replace('/','_')}.png"
    if os.path.exists(out_file):
        os.remove(out_file)

    x_labels=[]
    target_vals=[]
    scaling_vals=[]

    def quarter_label(q_idx):
        if scaling_repo in quarter_dates and q_idx in quarter_dates[scaling_repo]:
            (st_dt, en_dt)= quarter_dates[scaling_repo][q_idx]
            return f"Q{q_idx}(FY25)({st_dt:%Y-%m-%d}..{en_dt:%Y-%m-%d})"
        else:
            return f"Q{q_idx}(No data)"

    all_q= sorted(union_all)
    for q_idx in all_q:
        x_labels.append( quarter_label(q_idx) )
        target_vals.append( sei_target_dict[q_idx] )
        scaling_vals.append( scaling_sei[q_idx] )

    fig, ax= plt.subplots(figsize=(10,6))
    x= np.arange(len(x_labels))
    barw= 0.4

    ax.bar(x - barw/2, target_vals, barw, label="SEI Target", color='steelblue')
    ax.bar(x + barw/2, scaling_vals, barw, label=f"SEI {scaling_repo}", color='orange')
    ax.set_title(f"SEI Target vs. SEI {scaling_repo}")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha='right')
    ax.set_ylabel("SEI Value")
    ax.legend()

    # small table at bottom, up to 4 rows
    table_data= [
        ["Quarter","Date Range","SEI Goal"]
    ]
    max_rows= min(4, len(all_q))
    for i in range(max_rows):
        qx= all_q[i]
        if scaling_repo in quarter_dates and qx in quarter_dates[scaling_repo]:
            (s_dt, e_dt)= quarter_dates[scaling_repo][qx]
            dr_str= f"{s_dt:%m/%d}..{e_dt:%m/%d}"
        else:
            dr_str= "N/A"
        goal_val= sei_target_dict[qx]
        table_data.append([ quarter_label(qx), dr_str, f"{goal_val:.2f}" ])

    plt.subplots_adjust(bottom=0.28)
    tb= ax.table(
        cellText= table_data[1:],
        colLabels= table_data[0],
        loc='bottom',
        cellLoc='center'
    )
    tb.set_fontsize(8)
    tb.scale(1,1.2)

    plt.tight_layout()
    plt.savefig(out_file)
    plt.close()

    # console table
    print(f"\n=== SEI Target vs. SEI {scaling_repo} ===")
    console_tbl= [
        ["Quarter","SEI Target", f"SEI ({scaling_repo})"]
    ]
    for i,qx in enumerate(all_q):
        lbl= x_labels[i]
        tv= f"{sei_target_dict[qx]:.4f}"
        sv= f"{scaling_sei[qx]:.4f}"
        console_tbl.append([lbl, tv, sv])
    print_aligned_table(console_tbl, ["left","center","center"])


def main():
    from config import get_scaling_repo, get_num_fiscal_quarters
    repos= [
        "tensorflow/tensorflow",
        "facebook/react"
    ]
    scaling_repo= get_scaling_repo()
    if not scaling_repo:
        print("[ERROR] No scaling_repo. Exiting.")
        sys.exit(1)
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    from scale_factors import (
        compute_scale_factors,
        compute_target_reached_data,
        compute_sei_data
    )

    # compute the 8 factors
    (sfM, sfCl, sfF, sfS, sfNi, sfCo, sfRe, sfP)= compute_scale_factors(scaling_repo, repos)
    num_quarters= get_num_fiscal_quarters()
    now= datetime.utcnow()

    velocity_scaled={}
    uig_scaled={}
    mac_scaled={}
    quarter_dates={}
    existing_data_dict={}
    detail_calc_dict={}

    from baseline import find_oldest_date_for_repo
    from quarters import generate_quarter_windows

    # aggregator
    from aggregator import (
        velocity, user_interest_growth,
        monthly_active_contributors
    )

    for repo in repos:
        velocity_scaled[repo]= {}
        uig_scaled[repo]= {}
        mac_scaled[repo]= {}
        existing_data_dict[repo]= []
        detail_calc_dict[repo]= []

        oldest_dt= find_oldest_date_for_repo(repo)
        if not oldest_dt:
            print(f"[INFO] No data for {repo}, skip.")
            continue

        raw_quarters= generate_quarter_windows(oldest_dt, num_quarters)
        quarter_ranges= []
        for (qs,qe) in raw_quarters:
            if qs> now:
                break
            if qe> now:
                qe= now
            if qs<qe:
                quarter_ranges.append((qs,qe))
        if not quarter_ranges:
            print(f"[INFO] No valid windows for {repo}.")
            continue

        q_idx=1
        quarter_dates[repo]={}

        from merges_issues import (
            count_merged_pulls, count_closed_issues,
            count_new_pulls, count_new_issues
        )
        from forks_stars import (
            count_forks, count_stars
        )
        from comments_reactions import (
            count_issue_comments, count_all_reactions
        )

        for (q_start,q_end) in quarter_ranges:
            mergesRaw= count_merged_pulls(repo, q_start, q_end)
            closedRaw= count_closed_issues(repo, q_start, q_end)
            forksRaw= count_forks(repo, q_start, q_end)
            starsRaw= count_stars(repo, q_start, q_end)
            newIssRaw= count_new_issues(repo, q_start, q_end)
            newCommRaw= count_issue_comments(repo, q_start, q_end)
            newReactRaw= count_all_reactions(repo, q_start, q_end)
            newPullRaw= count_new_pulls(repo, q_start, q_end)

            merges_s= mergesRaw * sfM[repo]
            closed_s= closedRaw * sfCl[repo]
            forks_s= forksRaw * sfF[repo]
            stars_s= starsRaw * sfS[repo]
            newIss_s= newIssRaw * sfNi[repo]
            newComm_s= newCommRaw * sfCo[repo]
            newReact_s= newReactRaw * sfRe[repo]
            newPull_s= newPullRaw * sfP[repo]

            vel= velocity(merges_s, closed_s)
            uigv= user_interest_growth(forks_s, stars_s)
            macv= monthly_active_contributors(newIss_s, newComm_s, newReact_s, newPull_s)

            quarter_dates[repo][q_idx]= (q_start,q_end)

            existing_data_dict[repo].append((
                q_idx, q_start, q_end,
                mergesRaw, closedRaw, forksRaw, starsRaw,
                newIssRaw, newCommRaw, newReactRaw, newPullRaw,

                merges_s, closed_s, forks_s, stars_s,
                newIss_s, newComm_s, newReact_s, newPull_s,

                vel, uigv, macv
            ))
            velocity_scaled[repo][q_idx]= vel
            uig_scaled[repo][q_idx]= uigv
            mac_scaled[repo][q_idx]= macv

            sumICR= newIss_s+ newComm_s+ newReact_s
            detail_calc_dict[repo].append((
                q_idx,
                f"Q{q_idx}({q_start:%Y-%m-%d}-{q_end:%Y-%m-%d})",
                merges_s, closed_s, vel,
                forks_s, stars_s, uigv,
                sumICR, newPull_s, macv
            ))

            q_idx+=1

    # print quarter-based data
    for repo in repos:
        data_list= existing_data_dict[repo]
        if not data_list:
            continue
        print_existing_quarter_data_table(
            repo,
            sfM, sfCl, sfF, sfS, sfNi, sfCo, sfRe, sfP,
            data_list
        )
        print(f"\n--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")
        print_calculation_details(repo, detail_calc_dict[repo])

    # compute "target reached" for velocity, uig, mac => produce SEI
    velocity_target= compute_target_reached_data(repos, scaling_repo, velocity_scaled)
    uig_target= compute_target_reached_data(repos, scaling_repo, uig_scaled)
    mac_target= compute_target_reached_data(repos, scaling_repo, mac_scaled)
    sei_data= compute_sei_data(velocity_target, uig_target, mac_target)

    # now produce the final SEI chart
    generate_oss_sei_chart_and_table(
        scaling_repo,
        velocity_scaled,
        uig_scaled,
        mac_scaled,
        quarter_dates,
        repos
    )

    print(f"\n=== Final Summary for {scaling_repo} ===\n")
    print("All quarter-based tables & SEI chart generated.\n")

    # finalize debug log
    sys.stdout.flush()
    console_text = log_capture.getvalue()
    sys.stdout = original_stdout

    debug_file= "debug_log.txt"
    if os.path.exists(debug_file):
        os.remove(debug_file)
    with open(debug_file, "w", encoding="utf-8") as f:
        f.write("=== ENVIRONMENT VARIABLES ===\n")
        env_scaling= os.environ.get("SCALING_REPO","<not set>")
        env_quarters= os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
        f.write(f"SCALING_REPO={env_scaling}\n")
        f.write(f"NUM_FISCAL_QUARTERS={env_quarters}\n")
        f.write("\n=== CAPTURED CONSOLE OUTPUT ===\n")
        f.write(console_text)

    print(f"[INFO] Debug log saved to {debug_file}")

if __name__=="__main__":
    main()
