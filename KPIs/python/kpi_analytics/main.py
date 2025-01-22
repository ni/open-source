#!/usr/bin/env python3

import sys
import os
import io
import math
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# Existing modules (unchanged)
from baseline import find_oldest_date_for_repo
from quarters import generate_quarter_windows
from merges_issues import (
    count_merged_pulls, count_closed_issues,
    count_new_pulls, count_new_issues
)
from forks_stars import count_forks, count_stars
from comments_reactions import (
    count_issue_comments,
    count_all_reactions
)
from aggregator import (
    velocity, user_interest_growth, monthly_active_contributors
)
from scale_factors import compute_scale_factors
from config import (
    get_scaling_repo,
    get_num_fiscal_quarters
)

###############################################################################
# 1) Capture console output for debug_log
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
# 2) Print table utility
###############################################################################
def print_aligned_table(table_data, alignments=None):
    if not table_data:
        return
    num_cols = len(table_data[0])
    if alignments is None:
        alignments = ['left']*num_cols
    if len(alignments) < num_cols:
        alignments += ['left']*(num_cols - len(alignments))

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
            left_pad = pad//2
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
# 3) Existing quarter data in table form (raw => no decimals, scaled => 4 decimals)
###############################################################################
def print_existing_quarter_data_table(
    repo, sfM, sfI, sfF, sfS, sfP,
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
    align = ["left"] + ["center"]*(len(header)-1)
    table_data = [header]

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

        def f4(x): return f"{x:.4f}"
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
# 4) Detailed Calculation Tables (Velocity, UIG, MAC)
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
         sumIssCommReact_s, pull_s, mac_val)= row_data
        def f4(x): return f"{x:.4f}"
        table_vel.append([q_range, f4(merges_s), f4(closed_s), f4(velocity_val)])
        table_uig.append([q_range, f4(forks_s), f4(stars_s), f4(uig_val)])
        table_mac.append([q_range, f4(sumIssCommReact_s), f4(pull_s), f4(mac_val)])

    print(f"=== Detailed Calculations for {repo}: Velocity ===")
    print_aligned_table(table_vel, ["left","center","center","center"])
    print(f"\n=== Detailed Calculations for {repo}: UIG ===")
    print_aligned_table(table_uig, ["left","center","center","center"])
    print(f"\n=== Detailed Calculations for {repo}: MAC ===")
    print_aligned_table(table_mac, ["left","center","center","center"])

###############################################################################
# 5) compute_target_reached_data + compute_sei_data, same as before
###############################################################################
def compute_target_reached_data(repo_list, scaling_repo, quarter_data_dict):
    """
    This aggregates the scaled metric (like velocity) from each non-scaling repo,
    computing an average => targetVal, then comparing scalingVal => ratio.
    We skip if scaling doesn't exist, or if no non-scaling data. But user wants
    partial coverage, so we keep it if the non-scaler has data.
    Actually to handle "rows that have no data from the scaling repo," we still
    produce an entry if the non-scaler has data. We'll do a union of indexes.
    """
    target_data={}
    # gather union of quarter indexes across scaling + non-scaling
    union_q_idx= set()
    for r in repo_list:
        union_q_idx |= set(quarter_data_dict[r].keys())
    union_q_idx= sorted(union_q_idx)

    for q_idx in union_q_idx:
        # gather sum from non-scaling
        sum_val=0.0
        count_val=0
        for r in repo_list:
            if r==scaling_repo:
                continue
            if q_idx in quarter_data_dict[r]:
                sum_val += quarter_data_dict[r][q_idx]
                count_val+=1
        avg_val= 0.0
        if count_val>0:
            avg_val= sum_val/count_val

        # scalingVal => if missing => 0.0
        if q_idx in quarter_data_dict[scaling_repo]:
            scaling_val= quarter_data_dict[scaling_repo][q_idx]
        else:
            scaling_val= 0.0  # user wants to see projection if scaling is missing

        ratio= 0.0
        if abs(avg_val)>1e-9:
            ratio= (scaling_val/avg_val)*100.0
        else:
            ratio=0.0

        target_data[q_idx]= (avg_val, scaling_val, ratio)

    return target_data

def compute_sei_data(vel_dict, uig_dict, mac_dict):
    """
    We'll do a union of quarter indexes from velocity, uig, mac dict
    so that we produce a row for partial coverage too.
    If scaling is missing, we treat that portion as 0 => skip? or use 0.
    For clarity, we'll do the same approach => if there's no velocity entry,
    that portion is 0 => in ratio, it won't matter.
    """
    sei_data={}
    all_q = set(vel_dict.keys())|set(uig_dict.keys())|set(mac_dict.keys())
    all_q= sorted(all_q)
    for q_idx in all_q:
        vT,vS,vR = vel_dict.get(q_idx,(0,0,0))
        uT,uS,uR = uig_dict.get(q_idx,(0,0,0))
        mT,mS,mR = mac_dict.get(q_idx,(0,0,0))

        # Weighted ratio => 0.3 velocity ratio, 0.2 uig ratio, 0.5 mac ratio if >0 target
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
            # no metric => produce something
            # let's define ratio=0 if scaling=0, or partial
            # scaledSei => 0.5*mS +0.3*vS +0.2*uS
            scaled_sei= (0.5*mS +0.3*vS +0.2*uS)
            sei_data[q_idx]= (100.0, scaled_sei, 0.0)
            continue

        wsum= sum(ratio_weights)
        partial_sum=0.0
        for i in range(len(ratio_weights)):
            partial_sum+= ratio_weights[i]* ratio_values[i]
        sei_ratio= partial_sum/wsum
        scaled_sei= 0.5*mS +0.3*vS +0.2*uS
        sei_data[q_idx]= (100.0, scaled_sei, sei_ratio)
    return sei_data

###############################################################################
# 6) Additional bar charts for each metric per non-scaling repo
#    comparing that repo vs. scaling repo side by side.
###############################################################################
def plot_metric_comparison_for_repo(
    repo, scaling_repo,
    metric_name,
    metric_dict_repo,  # {quarterIdx: (avg, scalingVal, ratio)} if we store them or direct scaled?
    metric_dict_scaling # same structure for scaling, or we do direct scaled
):
    """
    We'll unify quarter indexes from both dicts. Then produce 2 bars per quarter:
    - 'repo' bar
    - 'scaling' bar
    If missing => 0. We'll call the output: {metric_name}_comparison_{repo.replace('/','_')}.png
    """
    # gather union of q_idx
    union_q_idx= set(metric_dict_repo.keys()) | set(metric_dict_scaling.keys())
    union_q_idx= sorted(union_q_idx)

    q_labels=[]
    repo_vals=[]
    scaling_vals=[]

    for q_idx in union_q_idx:
        q_labels.append(f"Q{q_idx}")
        # we might store data as (avgVal, scalVal, ratio) => but we just want the 'scalVal' portion if it's a direct store
        # or in your code, you might store "scaled" in index 1. We'll adapt if the structure is (val, scalingVal, ratio)
        # Actually for "this repo" we want the second item from metric_dict_repo? Wait, we might have used target structure?
        # Let's assume we stored => metric_dict_repo[q_idx] = scaledValue. We'll do that approach for simpler code.
        # But you currently store => target_data => (avgVal, scalingVal, ratio). That is for "target" perspective, not direct scaled?
        # If we want direct scaled velocity, we might better pass velocity_scaled[repo], velocity_scaled[scaling_repo].
        # We'll do the simpler approach: pass direct scaled dict => i.e. velocity_scaled[repo], velocity_scaled[scaling_repo].
        # Then if missing => 0. We'll update the code to do that for clarity.

        if q_idx in metric_dict_repo:
            repo_val= metric_dict_repo[q_idx]
        else:
            repo_val= 0.0
        if q_idx in metric_dict_scaling:
            scaling_val= metric_dict_scaling[q_idx]
        else:
            scaling_val= 0.0

        repo_vals.append(repo_val)
        scaling_vals.append(scaling_val)

    x= np.arange(len(union_q_idx))
    barw=0.4
    plt.figure(figsize=(9,5))
    plt.bar(x - barw/2, repo_vals, barw, label=f"{repo}", color='orange')
    plt.bar(x + barw/2, scaling_vals, barw, label=f"{scaling_repo}", color='gray')
    plt.xticks(x, q_labels, rotation=45, ha='right')
    plt.title(f"{metric_name} Comparison: {repo} vs. {scaling_repo}")
    plt.legend()
    plt.tight_layout()
    out_file= f"{metric_name.lower()}_comparison_{repo.replace('/','_')}.png"
    plt.savefig(out_file)
    plt.close()

###############################################################################
# 7) Provide a short "stakeholder summary" function
###############################################################################
def produce_stakeholder_summary(
    velocity_target, uig_target, mac_target, sei_data,
    quarter_dates, scaling_repo
):
    print("\n=== STAKEHOLDER SUMMARY & RECOMMENDATIONS ===\n")
    if scaling_repo not in quarter_dates:
        print("[No quarters for scaling repo, no summary available.]")
        return
    q_idxs= sorted(quarter_dates[scaling_repo].keys())
    if not q_idxs:
        print("[No quarter indexes, no summary available.]")
        return

    last_q= q_idxs[-1]
    (vT, vS, vR)= velocity_target.get(last_q,(0,0,0))
    (uT, uS, uR)= uig_target.get(last_q,(0,0,0))
    (mT, mS, mR)= mac_target.get(last_q,(0,0,0))
    (sT, sS, sRatio)= sei_data.get(last_q,(0,0,0))

    def rating(ratio):
        if ratio<70.0: return "Below target"
        elif ratio>120.0: return "Above target"
        else: return "Near target"

    v_rating= rating(vR)
    u_rating= rating(uR)
    m_rating= rating(mR)
    sei_rating= rating(sRatio)

    print(f"Final Quarter (Q{last_q}) Performance for {scaling_repo}:")
    print(f"  Velocity Ratio = {vR:.2f}% => {v_rating}")
    print(f"  UIG Ratio      = {uR:.2f}% => {u_rating}")
    print(f"  MAC Ratio      = {mR:.2f}% => {m_rating}")
    print(f"  SEI Ratio      = {sRatio:.2f}% => {sei_rating}")

    print("\nSuggestions for Stakeholders:")
    if vR<70:
        print(" - Velocity is below normal. Consider rebalancing tasks or analyzing bottlenecks.")
    if mR<70:
        print(" - MAC is below normal. Encourage more contributor involvement or reduce friction.")
    if sRatio>120:
        print(" - SEI is significantly above target. Good job—sustain this performance!")
    if (vR>120 and uR>120 and mR>120):
        print(" - All metrics are above target. This might indicate extraordinary performance.\n")

###############################################################################
def main():
    repos= [
        #"ni/labview-icon-editor",
        "facebook/react",
        "tensorflow/tensorflow"
    ]
    scaling_repo= get_scaling_repo()
    if not scaling_repo:
        print("[ERROR] No scaling_repo. Exiting.")
        sys.exit(1)
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    sfM, sfI, sfF, sfS, sfP= compute_scale_factors(scaling_repo, repos)
    num_quarters= get_num_fiscal_quarters()
    now= datetime.utcnow()

    velocity_scaled={}
    uig_scaled={}
    mac_scaled={}
    quarter_dates={}
    existing_data_dict={}
    detail_calc_dict={}

    # (We'll define SEI "scaled" dict after we compute target data.)
    # But if you want direct "sei" scaled, we can do it. We'll do it after the target step.

    for repo in repos:
        velocity_scaled[repo]={}
        uig_scaled[repo]={}
        mac_scaled[repo]={}
        existing_data_dict[repo]=[]
        detail_calc_dict[repo]=[]

        oldest_dt= find_oldest_date_for_repo(repo)
        if not oldest_dt:
            print(f"[INFO] No data for {repo}, skip.")
            continue

        raw_quarters= generate_quarter_windows(oldest_dt, num_quarters)
        quarter_ranges=[]
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
        quarter_dates[repo]={}

        for (q_start,q_end) in quarter_ranges:
            mergesRaw= count_merged_pulls(repo, q_start, q_end)
            closedRaw= count_closed_issues(repo, q_start, q_end)
            forksRaw= count_forks(repo, q_start, q_end)
            starsRaw= count_stars(repo, q_start, q_end)
            newIssRaw= count_new_issues(repo, q_start, q_end)
            newCommRaw= count_issue_comments(repo, q_start, q_end)
            newReactRaw= count_all_reactions(repo, q_start, q_end)
            newPullRaw= count_new_pulls(repo, q_start, q_end)

            merges_s= mergesRaw*sfM[repo]
            closed_s= closedRaw*sfI[repo]
            forks_s= forksRaw*sfF[repo]
            stars_s= starsRaw*sfS[repo]
            newIss_s= newIssRaw*sfI[repo]
            newComm_s= newCommRaw*sfI[repo]
            newReact_s= newReactRaw*sfI[repo]
            newPull_s= newPullRaw*sfP[repo]

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

            sumIssCommReact_s= newIss_s+ newComm_s+ newReact_s
            detail_calc_dict[repo].append((
                q_idx,
                f"Q{q_idx}({q_start:%Y-%m-%d}-{q_end:%Y-%m-%d})",
                merges_s, closed_s, vel,
                forks_s, stars_s, uigv,
                sumIssCommReact_s, newPull_s, macv
            ))

            q_idx+=1

    # Print existing data tables
    for repo in repos:
        if repo not in existing_data_dict or len(existing_data_dict[repo])==0:
            continue
        print_existing_quarter_data_table(
            repo, sfM, sfI, sfF, sfS, sfP,
            existing_data_dict[repo]
        )
        print(f"\n--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")
        print_calculation_details(repo, detail_calc_dict[repo])

    # compute "Target Reached"
    non_scaling = [r for r in repos if r!=scaling_repo]

    # We'll define a dictionary for velocity/mac/uig that we can use to produce "target" data
    velocity_target= compute_target_reached_data(repos, scaling_repo, velocity_scaled)
    uig_target= compute_target_reached_data(repos, scaling_repo, uig_scaled)
    mac_target= compute_target_reached_data(repos, scaling_repo, mac_scaled)
    sei_data= compute_sei_data(velocity_target, uig_target, mac_target)

    # We'll produce the combined table, separate tables, etc. same as before
    def print_combined_target_table():
        header= [
          "Quarter",
          "Velocity Target","Scaled Velocity","Velocity Ratio",
          "UIG Target","Scaled UIG","UIG Ratio",
          "MAC Target","Scaled MAC","MAC Ratio",
          "SEI Target","Scaled SEI","SEI Ratio"
        ]
        align= ["left"]+["center"]*(len(header)-1)
        table= [header]
        if scaling_repo not in quarter_dates:
            return
        # union of quarter indexes if you want partial coverage or just scaling
        q_idxs= sorted(set(quarter_dates[scaling_repo].keys()) | set(velocity_target.keys()) | set(uig_target.keys()) | set(mac_target.keys()) | set(sei_data.keys()))
        for q_idx in q_idxs:
            # if scaling_repo doesn't have that quarter => label partial
            if q_idx in quarter_dates[scaling_repo]:
                (qs,qe)= quarter_dates[scaling_repo][q_idx]
                q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            else:
                q_label= f"Q{q_idx}(No scaling data)"

            vT,vS,vR= velocity_target.get(q_idx,(0,0,0))
            uT,uS,uR= uig_target.get(q_idx,(0,0,0))
            mT,mS,mR= mac_target.get(q_idx,(0,0,0))
            sT,sS,sRatio= sei_data.get(q_idx,(0,0,0))
            def f4(x): return f"{x:.4f}"
            row= [
                q_label,
                f4(vT), f4(vS), f4(vR),
                f4(uT), f4(uS), f4(uR),
                f4(mT), f4(mS), f4(mR),
                f4(sT), f4(sS), f4(sRatio)
            ]
            table.append(row)
        print_aligned_table(table, align)

    def print_metric_table(metric_name, data_dict):
        header= [ "Quarter", f"{metric_name} Target", f"Scaled {metric_name}", f"{metric_name} Ratio"]
        align= ["left","center","center","center"]
        table= [header]
        if scaling_repo not in quarter_dates:
            pass
        # union of indexes
        all_q = sorted(set(quarter_dates.get(scaling_repo,{}).keys())|set(data_dict.keys()))
        for q_idx in all_q:
            if q_idx in data_dict:
                (tVal,sVal,rVal)= data_dict[q_idx]
            else:
                (tVal,sVal,rVal)= (0,0,0)
            if q_idx in quarter_dates.get(scaling_repo,{}):
                (qs,qe)= quarter_dates[scaling_repo][q_idx]
                q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            else:
                q_label= f"Q{q_idx}(No scaling data)"

            table.append([
                q_label,
                f"{tVal:.4f}",
                f"{sVal:.4f}",
                f"{rVal:.4f}"
            ])
        print_aligned_table(table, align)

    def print_sei_table():
        header= ["Quarter","SEI Target","Scaled SEI","SEI Ratio"]
        align= ["left","center","center","center"]
        table= [header]
        # union
        all_q= sorted(set(quarter_dates.get(scaling_repo,{}).keys())| set(sei_data.keys()))
        for q_idx in all_q:
            (tVal,sVal,ratio)= sei_data.get(q_idx,(0,0,0))
            if q_idx in quarter_dates.get(scaling_repo,{}):
                (qs,qe)= quarter_dates[scaling_repo][q_idx]
                q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            else:
                q_label= f"Q{q_idx}(No scaling data)"
            table.append([
                q_label,
                f"{tVal:.4f}",
                f"{sVal:.4f}",
                f"{ratio:.4f}"
            ])
        print_aligned_table(table, align)

    print(f"\n===== [TARGET REACHED] for Scaling Repo = {scaling_repo} =====\n")
    print("=== Combined Table (Velocity, UIG, MAC, SEI) ===")
    print_combined_target_table()

    print("\n=== Velocity Target Reached (Separate) ===")
    print_metric_table("Velocity", velocity_target)
    print("\n=== UIG Target Reached (Separate) ===")
    print_metric_table("UIG", uig_target)
    print("\n=== MAC Target Reached (Separate) ===")
    print_metric_table("MAC", mac_target)
    print("\n=== SEI Target Reached (Separate) ===")
    print_sei_table()

    # existing "combined" bar charts remain
    # We also produce the new "individual" plots
    print("\n=== Creating Individual Metric Comparison Plots (Non-scaling vs Scaling) ===\n")

    # We'll produce direct scaled dict => velocity_scaled[repo], velocity_scaled[scaling_repo], etc.
    # For each non-scaling repo, for each metric => produce 1 chart
    for r in repos:
        if r==scaling_repo:
            continue
        print(f"Generating Velocity/UIG/MAC/SEI charts for {r} vs. {scaling_repo}...")

        # velocity => velocity_scaled[r], velocity_scaled[scaling_repo]
        # if missing => 0, union of q_idx
        plot_metric_comparison_for_repo(
            r, scaling_repo,
            "Velocity",
            velocity_scaled[r],
            velocity_scaled.get(scaling_repo, {})
        )
        plot_metric_comparison_for_repo(
            r, scaling_repo,
            "UIG",
            uig_scaled[r],
            uig_scaled.get(scaling_repo, {})
        )
        plot_metric_comparison_for_repo(
            r, scaling_repo,
            "MAC",
            mac_scaled[r],
            mac_scaled.get(scaling_repo, {})
        )

        # For SEI => we didn't store direct scaled SEI per repo, only for scaling. But user wants "compare them?"
        # If we want to define a "SEI" for that non-scaling repo, we can do the aggregator approach or skip. The user only asked
        # for "SEI" per repo? We can do it if we define the same formula for non-scaling. We'll do a quick approach:
        # eq => SEI = 0.5*(mac) +0.3*(velocity) +0.2*(uig). We'll just produce it from velocity_scaled[r], etc.
        # Then compare that to the scaling repo's "SEI" from your compute. We'll define a small function now:
        non_scaling_sei = {}
        # union of q_idx => velocity_scaled[r], uig_scaled[r], mac_scaled[r]
        union_q = set(velocity_scaled[r].keys())|set(uig_scaled[r].keys())|set(mac_scaled[r].keys())
        for q_idx in union_q:
            vel_val = velocity_scaled[r].get(q_idx,0.0)
            uig_val = uig_scaled[r].get(q_idx,0.0)
            mac_val = mac_scaled[r].get(q_idx,0.0)
            # weighting => 0.3 velocity +0.2 uig +0.5 mac
            non_scaling_sei[q_idx]= 0.5*mac_val +0.3*vel_val +0.2*uig_val

        # for scaling => we do the same approach or we might do the partial approach from the "sei_data"
        # but "sei_data" is a dict => (100, scaledSEI, ratio). The scaledSEI is index 1 => we'll parse that out
        scaling_sei_dict= {}
        for q_idx,(targetVal, scaledSeiVal, ratioVal) in sei_data.items():
            scaling_sei_dict[q_idx] = scaledSeiVal

        plot_metric_comparison_for_repo(
            r, scaling_repo,
            "SEI",
            non_scaling_sei,
            scaling_sei_dict
        )

    print(f"\n=== Final Summary for {scaling_repo} ===\n")
    print("All tables & bar charts generated with quarter-based data in table form.\n")

    # "stakeholder summary" function => optional
    def produce_stakeholder_summary(
        velocity_target, uig_target, mac_target, sei_data,
        quarter_dates, scaling_repo
    ):
        print("\n=== STAKEHOLDER SUMMARY & RECOMMENDATIONS ===\n")
        if scaling_repo not in quarter_dates:
            print("[No quarters for scaling repo, no summary available.]")
            return
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
        if not q_idxs:
            print("[No quarter indexes, no summary available.]")
            return

        last_q= q_idxs[-1]
        (vT, vS, vR)= velocity_target.get(last_q,(0,0,0))
        (uT, uS, uR)= uig_target.get(last_q,(0,0,0))
        (mT, mS, mR)= mac_target.get(last_q,(0,0,0))
        (sT, sS, sRatio)= sei_data.get(last_q,(0,0,0))

        def rating(ratio):
            if ratio<70.0: return "Below target"
            elif ratio>120.0: return "Above target"
            else: return "Near target"

        v_rating= rating(vR)
        u_rating= rating(uR)
        m_rating= rating(mR)
        sei_rating= rating(sRatio)

        print(f"Final Quarter (Q{last_q}) Performance for {scaling_repo}:")
        print(f"  Velocity Ratio = {vR:.2f}% => {v_rating}")
        print(f"  UIG Ratio      = {uR:.2f}% => {u_rating}")
        print(f"  MAC Ratio      = {mR:.2f}% => {m_rating}")
        print(f"  SEI Ratio      = {sRatio:.2f}% => {sei_rating}")

        print("\nSuggestions for Stakeholders:")
        if vR<70:
            print(" - Velocity below normal => re-check merges & PR flow.")
        if mR<70:
            print(" - MAC below normal => reduce friction or encourage more contributor engagement.")
        if sRatio>120:
            print(" - SEI well above target => momentum is strong, keep the strategy!")
        if (vR>120 and uR>120 and mR>120):
            print(" - All metrics are above target => performance is outstanding.\n")

    # produce final summary / recommendations
    produce_stakeholder_summary(
        velocity_target, uig_target, mac_target, sei_data,
        quarter_dates, scaling_repo
    )

    sys.stdout.flush()
    console_text = log_capture.getvalue()
    sys.stdout = original_stdout

    debug_file = "debug_log.txt"
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
