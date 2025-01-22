#!/usr/bin/env python3

import sys
import os
import io
import math
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# Your existing imports:
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
from scale_factors import compute_scale_factors
from config import (
    get_scaling_repo,
    get_num_fiscal_quarters
)

###############################################################################
# Capture console output so we also write it to debug_log.txt
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
# Utility for printing tables without changing variable names
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
            if len(cell_str)> col_widths[c_idx]:
                col_widths[c_idx] = len(cell_str)

    def fmt_cell(cell_str, width, alignment):
        if alignment == 'left':
            return cell_str.ljust(width)
        elif alignment == 'center':
            pad = width - len(cell_str)
            left_pad = pad//2
            right_pad = pad - left_pad
            return ' '*left_pad + cell_str + ' '*right_pad
        else:
            return cell_str.rjust(width)

    header_line= " | ".join(
        fmt_cell(str(table_data[0][i]), col_widths[i], alignments[i])
        for i in range(num_cols)
    )
    print(header_line)
    sep_line= "-+-".join("-"*col_widths[i] for i in range(num_cols))
    print(sep_line)

    for row in table_data[1:]:
        row_line= " | ".join(
            fmt_cell(str(row[i]), col_widths[i], alignments[i])
            for i in range(num_cols)
        )
        print(row_line)


###############################################################################
# Print existing quarter data with original variable names
###############################################################################
def print_existing_quarter_data_table(
    repo,
    sfM, sfI, sfF, sfS, sfP,
    quarter_list
):
    title_line = [
        f"Existing Quarter Data for {repo}",
        f"(mergesFactor={sfM[repo]:.4f}, issuesFactor={sfI[repo]:.4f}, forksFactor={sfF[repo]:.4f}, starsFactor={sfS[repo]:.4f}, pullsFactor={sfP[repo]:.4f})"
    ]
    print(" | ".join(title_line))
    print("="*(len(" | ".join(title_line))))

    header = [
      "Q-Range",
      "mergesRaw","closedRaw","forksRaw","starsRaw",
      "newIssRaw","newCommRaw","newReactRaw","newPullRaw",
      "mergesScaled","closedScaled","forksScaled","starsScaled",
      "newIssScaled","newCommScaled","newReactScaled","newPullScaled",
      "Velocity","UIG","MAC"
    ]
    align = ["left"]+["center"]*(len(header)-1)
    table_data= [header]

    for row_data in quarter_list:
        (q_idx,q_start,q_end,
         mergesRaw, issuesRaw, forksRaw, starsRaw,
         newIssRaw, newCommRaw, newReactRaw, newPullRaw,
         merges_s, closed_s, forks_s, stars_s,
         newIss_s, newComm_s, newReact_s, newPull_s,
         vel, uigv, macv) = row_data

        qrange_str= f"Q{q_idx}({q_start:%Y-%m-%d}-{q_end:%Y-%m-%d})"
        mergesRaw_str= f"{mergesRaw:.0f}"
        closedRaw_str= f"{issuesRaw:.0f}"
        forksRaw_str= f"{forksRaw:.0f}"
        starsRaw_str= f"{starsRaw:.0f}"
        newIssRaw_str= f"{newIssRaw:.0f}"
        newCommRaw_str= f"{newCommRaw:.0f}"
        newReactRaw_str= f"{newReactRaw:.0f}"
        newPullRaw_str= f"{newPullRaw:.0f}"

        def f4(x):
            return f"{x:.4f}"
        row= [
            qrange_str,
            mergesRaw_str, closedRaw_str, forksRaw_str, starsRaw_str,
            newIssRaw_str, newCommRaw_str, newReactRaw_str, newPullRaw_str,
            f4(merges_s), f4(closed_s), f4(forks_s), f4(stars_s),
            f4(newIss_s), f4(newComm_s), f4(newReact_s), f4(newPull_s),
            f4(vel), f4(uigv), f4(macv)
        ]
        table_data.append(row)

    print_aligned_table(table_data, align)


###############################################################################
# Detailed calculations for velocity, UIG, MAC
###############################################################################
def print_calculation_details(repo, quarter_calcs):
    header_vel= ["Q-Range","mergesScaled","closedScaled","Velocity=0.4*M+0.6*C"]
    table_vel= [header_vel]
    header_uig= ["Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"]
    table_uig= [header_uig]
    header_mac= ["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]
    table_mac= [header_mac]

    for row_data in quarter_calcs:
        (q_idx,q_range, merges_s, closed_s, velocity_val,
         forks_s, stars_s, uig_val,
         sum_icr_s, pull_s, mac_val)= row_data

        def f4(x): return f"{x:.4f}"
        table_vel.append([q_range, f4(merges_s), f4(closed_s), f4(velocity_val)])
        table_uig.append([q_range, f4(forks_s), f4(stars_s), f4(uig_val)])
        table_mac.append([q_range, f4(sum_icr_s), f4(pull_s), f4(mac_val)])

    print(f"=== Detailed Calculations for {repo}: Velocity ===")
    print_aligned_table(table_vel, ["left","center","center","center"])

    print(f"\n=== Detailed Calculations for {repo}: UIG ===")
    print_aligned_table(table_uig, ["left","center","center","center"])

    print(f"\n=== Detailed Calculations for {repo}: MAC ===")
    print_aligned_table(table_mac, ["left","center","center","center"])


###############################################################################
# compute_target_reached_data & compute_sei_data
###############################################################################
def compute_target_reached_data(repos, scaling_repo, quarter_data_dict):
    target_data= {}
    union_q= set()
    for r in repos:
        union_q |= set(quarter_data_dict[r].keys())
    union_q= sorted(union_q)

    non_scaling= [x for x in repos if x != scaling_repo]

    for q_idx in union_q:
        sum_val= 0.0
        cnt=0
        for r in non_scaling:
            if q_idx in quarter_data_dict[r]:
                sum_val+= quarter_data_dict[r][q_idx]
                cnt+=1
        avg_val= 0.0
        if cnt>0:
            avg_val= sum_val/cnt
        scaling_val= quarter_data_dict[scaling_repo].get(q_idx, 0.0)
        ratio_val= 0.0
        if abs(avg_val)>1e-9:
            ratio_val= (scaling_val/avg_val)*100.0
        target_data[q_idx]= (avg_val, scaling_val, ratio_val)
    return target_data

def compute_sei_data(vel_dict, uig_dict, mac_dict):
    sei_dict= {}
    all_q= set(vel_dict.keys())| set(uig_dict.keys())| set(mac_dict.keys())
    all_q= sorted(all_q)
    for q_idx in all_q:
        vT,vS,vR= vel_dict.get(q_idx,(0,0,0))
        uT,uS,uR= uig_dict.get(q_idx,(0,0,0))
        mT,mS,mR= mac_dict.get(q_idx,(0,0,0))

        ratio_weights=[]
        ratio_values=[]
        if abs(vT)>1e-9:
            ratio_weights.append(0.3)
            ratio_values.append(vR)
        if abs(uT)>1e-9:
            ratio_weights.append(0.2)
            ratio_values.append(uR)
        if abs(mT)>1e-9:
            ratio_weights.append(0.5)
            ratio_values.append(mR)

        if len(ratio_weights)==0:
            scaled_sei= 0.5*mS +0.3*vS +0.2*uS
            sei_dict[q_idx]= (100.0, scaled_sei, 0.0)
            continue
        w_sum= sum(ratio_weights)
        partial_sum= 0.0
        for i in range(len(ratio_weights)):
            partial_sum += ratio_weights[i]* ratio_values[i]
        sei_ratio= partial_sum/ w_sum
        scaled_sei= 0.5*mS + 0.3*vS +0.2*uS
        sei_dict[q_idx]= (100.0, scaled_sei, sei_ratio)
    return sei_dict


###############################################################################
#  New function to produce the SEI chart: oss_sei_target_{scaling_repo}.png
###############################################################################
def generate_oss_sei_chart_and_table(
    scaling_repo,
    velocity_scaled,
    uig_scaled,
    mac_scaled,
    quarter_dates,
    repos
):
    """
    Creates oss_sei_target_{scaling_repo}.png comparing:
      - SEI Target (avg of non-scaling repos)
      - SEI (scaling_repo)
    Overwrites old file if it exists. Also places a small table at bottom
    listing Q1 => 10/01..12/31, etc. + numeric SEI goals from that quarter's average.
    Prints a console table "SEI Target vs. SEI {scaling_repo}" as well.
    """
    # 1) Compute SEI for each repo => sei_scaled[repo][q_idx]
    sei_scaled= {}
    for r in repos:
        sei_scaled[r]= {}
        union_q= set(velocity_scaled[r].keys())| set(uig_scaled[r].keys())| set(mac_scaled[r].keys())
        for q_idx in union_q:
            v_val= velocity_scaled[r].get(q_idx, 0.0)
            u_val= uig_scaled[r].get(q_idx, 0.0)
            m_val= mac_scaled[r].get(q_idx, 0.0)
            sei_val= 0.5*m_val +0.3*v_val +0.2*u_val
            sei_scaled[r][q_idx]= sei_val

    # 2) Compute average => "SEI Target"
    non_scaling= [rr for rr in repos if rr!=scaling_repo]
    all_q_idx= set()
    for rr in repos:
        all_q_idx |= set(sei_scaled[rr].keys())
    all_q_idx= sorted(all_q_idx)

    sei_target_dict= {}
    for q_idx in all_q_idx:
        sum_val=0.0
        cnt=0
        for rr in non_scaling:
            if q_idx in sei_scaled[rr]:
                sum_val+= sei_scaled[rr][q_idx]
                cnt+=1
        avg_val=0.0
        if cnt>0:
            avg_val= sum_val/cnt
        sei_target_dict[q_idx]= avg_val

    # scaling's SEI
    scaling_sei_dict= {}
    for q_idx in all_q_idx:
        scaling_sei_dict[q_idx]= sei_scaled[scaling_repo].get(q_idx, 0.0)

    # 3) build chart data
    x_labels=[]
    target_vals=[]
    scaling_vals=[]

    def quarter_label(q_idx):
        """
        Return e.g. Q{q_idx}(FY25)(YYYY-MM-DD..YYYY-MM-DD) if known
        else Q{q_idx}(No data).
        """
        if scaling_repo in quarter_dates and q_idx in quarter_dates[scaling_repo]:
            (start_dt, end_dt)= quarter_dates[scaling_repo][q_idx]
            return f"Q{q_idx}(FY25)({start_dt:%Y-%m-%d}..{end_dt:%Y-%m-%d})"
        else:
            return f"Q{q_idx}(No data)"

    for q_idx in all_q_idx:
        lbl= quarter_label(q_idx)
        x_labels.append(lbl)
        target_vals.append(sei_target_dict[q_idx])
        scaling_vals.append(scaling_sei_dict[q_idx])

    # remove old file
    out_file= f"oss_sei_target_{scaling_repo.replace('/','_')}.png"
    if os.path.exists(out_file):
        os.remove(out_file)

    fig, ax= plt.subplots(figsize=(10,6))
    x_pos= np.arange(len(x_labels))
    bar_w= 0.4

    ax.bar(x_pos - bar_w/2, target_vals, bar_w, label="SEI Target", color='steelblue')
    ax.bar(x_pos + bar_w/2, scaling_vals, bar_w, label=f"SEI {scaling_repo}", color='orange')

    ax.set_title(f"SEI Target vs. SEI {scaling_repo}")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels, rotation=45, ha='right')
    ax.set_ylabel("SEI Value")
    ax.legend()

    # 4) place a small table at bottom => show up to 4 quarters
    table_data= [
        ["Quarter","Date Range","SEI Goal"]
    ]
    max_rows= min(4, len(all_q_idx))
    for i in range(max_rows):
        q_idx= all_q_idx[i]
        if scaling_repo in quarter_dates and q_idx in quarter_dates[scaling_repo]:
            (st_dt, en_dt)= quarter_dates[scaling_repo][q_idx]
            date_range_str= f"{st_dt:%m/%d}..{en_dt:%m/%d}"
        else:
            date_range_str= "N/A"
        # "SEI goal" => same as the average for that quarter
        sei_goal_val= sei_target_dict[q_idx]
        quarter_lbl= quarter_label(q_idx)
        table_data.append([
            quarter_lbl,
            date_range_str,
            f"{sei_goal_val:.2f}"
        ])

    plt.subplots_adjust(bottom=0.28)
    the_table= ax.table(
        cellText= table_data[1:],
        colLabels= table_data[0],
        loc='bottom',
        cellLoc='center'
    )
    the_table.set_fontsize(8)
    the_table.scale(1,1.2)

    plt.tight_layout()
    plt.savefig(out_file)
    plt.close()

    # 5) print console table => "SEI Target vs. SEI {scaling_repo}"
    print(f"\n=== SEI Target vs. SEI {scaling_repo} ===")
    console_table= [
        ["Quarter","SEI Target", f"SEI ({scaling_repo})"]
    ]
    for idx, q_idx in enumerate(all_q_idx):
        label_str= x_labels[idx]
        t_val= f"{sei_target_dict[q_idx]:.4f}"
        s_val= f"{scaling_sei_dict[q_idx]:.4f}"
        console_table.append([label_str, t_val, s_val])

    print_aligned_table(console_table, ["left","center","center"])


def main():
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

    # 1) compute scale factors
    (sfM, sfI, sfF, sfS, sfP)= compute_scale_factors(scaling_repo, repos)
    num_quarters= get_num_fiscal_quarters()
    now= datetime.utcnow()

    velocity_scaled= {}
    uig_scaled= {}
    mac_scaled= {}
    quarter_dates= {}
    existing_data_dict= {}
    detail_calc_dict= {}

    for repo in repos:
        velocity_scaled[repo]= {}
        uig_scaled[repo]= {}
        mac_scaled[repo]= {}
        existing_data_dict[repo]= []
        detail_calc_dict[repo]= []

        oldest_dt= find_oldest_date_for_repo(repo)
        if not oldest_dt:
            print(f"[INFO] No data for {repo}, skipping.")
            continue

        raw_quarters= generate_quarter_windows(oldest_dt, num_quarters)
        quarter_ranges= []
        for (qs,qe) in raw_quarters:
            if qs>now:
                break
            if qe>now:
                qe= now
            if qs<qe:
                quarter_ranges.append((qs,qe))
        if not quarter_ranges:
            print(f"[INFO] No valid windows for {repo}.")
            continue

        q_idx=1
        quarter_dates[repo]= {}

        for (w_start,w_end) in quarter_ranges:
            mergesRaw= count_merged_pulls(repo, w_start, w_end)
            closedRaw= count_closed_issues(repo, w_start, w_end)
            forksRaw= count_forks(repo, w_start, w_end)
            starsRaw= count_stars(repo, w_start, w_end)
            newIssRaw= count_new_issues(repo, w_start, w_end)
            newCommRaw= count_issue_comments(repo, w_start, w_end)
            newReactRaw= count_all_reactions(repo, w_start, w_end)
            newPullRaw= count_new_pulls(repo, w_start, w_end)

            merges_s= mergesRaw* sfM[repo]
            closed_s= closedRaw* sfI[repo]
            forks_s= forksRaw* sfF[repo]
            stars_s= starsRaw* sfS[repo]
            newIss_s= newIssRaw* sfI[repo]
            newComm_s= newCommRaw* sfI[repo]
            newReact_s= newReactRaw* sfI[repo]
            newPull_s= newPullRaw* sfP[repo]

            vel= velocity(merges_s, closed_s)
            uigv= user_interest_growth(forks_s, stars_s)
            macv= monthly_active_contributors(newIss_s, newComm_s, newReact_s, newPull_s)

            quarter_dates[repo][q_idx]= (w_start,w_end)

            existing_data_dict[repo].append((
                q_idx, w_start, w_end,
                mergesRaw, closedRaw, forksRaw, starsRaw,
                newIssRaw, newCommRaw, newReactRaw, newPullRaw,
                merges_s, closed_s, forks_s, stars_s,
                newIss_s, newComm_s, newReact_s, newPull_s,
                vel, uigv, macv
            ))

            velocity_scaled[repo][q_idx]= vel
            uig_scaled[repo][q_idx]= uigv
            mac_scaled[repo][q_idx]= macv

            sum_icr= newIss_s+ newComm_s+ newReact_s
            detail_calc_dict[repo].append((
                q_idx,
                f"Q{q_idx}({w_start:%Y-%m-%d}-{w_end:%Y-%m-%d})",
                merges_s, closed_s, vel,
                forks_s, stars_s, uigv,
                sum_icr, newPull_s, macv
            ))

            q_idx+=1

    # Print existing data
    for repo in repos:
        if repo not in existing_data_dict or len(existing_data_dict[repo])==0:
            continue
        print_existing_quarter_data_table(
            repo, sfM, sfI, sfF, sfS, sfP,
            existing_data_dict[repo]
        )
        print(f"\n--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")
        print_calculation_details(repo, detail_calc_dict[repo])

    # compute target reached for velocity, uig, mac => then sei
    velocity_target= compute_target_reached_data(repos, scaling_repo, velocity_scaled)
    uig_target= compute_target_reached_data(repos, scaling_repo, uig_scaled)
    mac_target= compute_target_reached_data(repos, scaling_repo, mac_scaled)
    sei_data= compute_sei_data(velocity_target, uig_target, mac_target)

    # you might produce other combined charts here

    # now produce the new SEI chart
    generate_oss_sei_chart_and_table(
        scaling_repo,
        velocity_scaled,
        uig_scaled,
        mac_scaled,
        quarter_dates,
        repos
    )

    print(f"\n=== Final Summary for {scaling_repo} ===\n")
    print("All tables & bar charts generated with quarter-based data.\n")

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
