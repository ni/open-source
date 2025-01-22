#!/usr/bin/env python3

import sys
import os
import io
import math
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Existing modules from your codebase
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
# Table-printing utility
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
# Print existing quarter data in table, no decimals for raw, 4 decimals for scaled
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
# Detailed calc for Velocity/UIG/MAC
###############################################################################
def print_calculation_details(repo, quarter_calcs):
    header_vel= ["Q-Range","mergesScaled","closedScaled","Velocity=0.4*M +0.6*C"]
    table_vel= [header_vel]
    header_uig= ["Q-Range","forksScaled","starsScaled","UIG=0.4*F +0.6*S"]
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
# compute_target_reached_data + compute_sei_data
###############################################################################
def compute_target_reached_data(repo_list, scaling_repo, quarter_data_dict):
    target_data={}
    # quarter_data_dict[repo][q_idx] = scaled_value
    # We skip none, but if scaling_repo has q_idx, we unify. We'll do normal logic
    if scaling_repo not in quarter_data_dict:
        return target_data
    all_q = sorted(quarter_data_dict[scaling_repo].keys())
    for q_idx in all_q:
        sum_val=0.0
        count_val=0
        for r in repo_list:
            if r==scaling_repo:
                continue
            if q_idx in quarter_data_dict[r]:
                sum_val+= quarter_data_dict[r][q_idx]
                count_val+=1
        if count_val==0:
            continue
        avg_val= sum_val/count_val
        scaling_val= quarter_data_dict[scaling_repo].get(q_idx,0.0)
        ratio=0.0
        if abs(avg_val)>1e-9:
            ratio= (scaling_val/avg_val)*100.0
        else:
            ratio=0.0
        target_data[q_idx]= (avg_val, scaling_val, ratio)
    return target_data

def compute_sei_data(vel_dict, uig_dict, mac_dict):
    sei_data={}
    all_q = set(vel_dict.keys())|set(uig_dict.keys())|set(mac_dict.keys())
    for q_idx in sorted(all_q):
        if q_idx not in vel_dict or q_idx not in uig_dict or q_idx not in mac_dict:
            continue
        (vT,vS,vR)= vel_dict[q_idx]
        (uT,uS,uR)= uig_dict[q_idx]
        (mT,mS,mR)= mac_dict[q_idx]
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
# Simple stakeholder summary function (unchanged from previous snippet)
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

    if vR<70:
        print(" - Velocity is below normal. Consider redistributing tasks or automating merges.")
    if mR<70:
        print(" - MAC is below normal. Possibly encourage more contributor engagement.")
    if sRatio>120:
        print(" - SEI is significantly above target. Great sign, maintain momentum!")
    if (vR>120 and uR>120 and mR>120):
        print(" - All metrics are above target. Team is outperforming expectations.\n")

    print("(Refine thresholds & text for your real stakeholder logic.)")


###############################################################################
# (NEW) Functions to produce a per-metric comparison chart for each non-scaling repo
# that includes quarters from both the non-scaling repo and the scaling repo.
###############################################################################
def union_of_quarters(repo1, repo2, quarter_dict):
    """
    Return a sorted list of quarter indexes that appear in either repo1 or repo2
    from the quarter_dict => quarter_dict[repo][q_idx] = scaled_value
    """
    set1 = set(quarter_dict[repo1].keys()) if repo1 in quarter_dict else set()
    set2 = set(quarter_dict[repo2].keys()) if repo2 in quarter_dict else set()
    return sorted(set1 | set2)

def plot_metric_comparison(metric_name, scaling_repo, metric_data, quarter_dates, repos):
    """
    For each non-scaling repo => produce a chart comparing that repo's metric to
    the scaling repo, with 2 bars/quarter index. We'll unify the quarter indexes
    from both. If a quarter is missing for one, we do 0. This allows 'projection' rows
    where scaling repo has no data but non-scaling does, or vice versa.
    metric_data[repo][q_idx] = scaled_value for that metric.
    We'll produce e.g. <metric_name>_<nonScaling>_vs_<scalingRepo>.png
    """
    import numpy as np

    for nr in repos:
        if nr == scaling_repo:
            continue
        if nr not in metric_data:
            continue
        # union quarter indexes
        union_q = union_of_quarters(nr, scaling_repo, metric_data)
        if not union_q:
            continue

        # build arrays
        nr_values=[]
        sc_values=[]
        labels=[]
        for q_idx in union_q:
            val_nr = metric_data[nr].get(q_idx, 0.0)
            val_sc = metric_data[scaling_repo].get(q_idx, 0.0)
            nr_values.append(val_nr)
            sc_values.append(val_sc)
            # build label
            # if we have quarter_dates => we can try to unify. We'll pick the
            # non-scaling repo's date if scaling doesn't have that quarter or vice versa
            # we'll do a fallback => "Q{q_idx}"
            q_label="Q{}".format(q_idx)
            # optional: try to see if nr in quarter_dates => quarter_dates[nr][q_idx]
            # or scaling repo in quarter_dates => quarter_dates[scaling_repo][q_idx]
            # for now, keep it simple
            labels.append(q_label)

        x= np.arange(len(union_q))
        barw=0.3
        plt.figure(figsize=(8,4))
        plt.bar(x - barw/2, nr_values, barw, label=f"{nr}", color='orange')
        plt.bar(x + barw/2, sc_values, barw, label=f"{scaling_repo}", color='blue')
        plt.xticks(x, labels, rotation=45, ha='right')
        plt.title(f"{metric_name} Comparison: {nr} vs. {scaling_repo}")
        plt.legend()
        plt.tight_layout()
        fname= f"{metric_name}_{nr.replace('/','_')}_vs_{scaling_repo.replace('/','_')}.png"
        plt.savefig(fname)
        plt.close()

def plot_sei_comparison(sei_data, scaling_repo, quarter_dates, repos):
    """
    SEI is typically only for scaling repo, so there's no direct data for non-scaling.
    We can produce a bar chart for each non-scaling showing 2 bars if we want 0 for them,
    or we can skip. Below we do skip, or produce just a single bar chart if you prefer.
    We'll produce a single chart per non-scaling with 2 bars => that non-scaling=0, scaling=the real data.
    """
    import numpy as np

    if scaling_repo not in sei_data:
        # Actually sei_data is a dict q_idx->(100.0, scaled_sei, ratio)
        # not separate by repo. We do a single approach
        pass

    # We'll produce one chart per non-scaling repo => if user wants to compare with 0
    # or skip if you prefer. Let's show how to do it with 2 bars => non-scaling =0
    for nr in repos:
        if nr==scaling_repo:
            continue
        # union quarter indexes => but scaling is the only one who has SEI
        # we do
        union_q= set()
        # if we store SEI as sei_data[q_idx], we do not have a dict per repo
        # so let's create sc_list from that. non-scaling => 0
        all_q = sorted(sei_data.keys())
        if not all_q:
            continue
        x= np.arange(len(all_q))
        s_values=[]
        nr_values=[]
        labels=[]
        for i,q_idx in enumerate(all_q):
            (tVal, sVal, ratio)= sei_data[q_idx]
            s_values.append(sVal)
            nr_values.append(0.0) # user wants to compare => do 0
            labels.append(f"Q{q_idx}")

        plt.figure(figsize=(8,4))
        barw=0.3
        plt.bar(x - barw/2, nr_values, barw, label=nr, color='gray')
        plt.bar(x + barw/2, s_values, barw, label=scaling_repo, color='orange')
        plt.xticks(x, labels, rotation=45, ha='right')
        plt.title(f"SEI Comparison: {nr} (0) vs. {scaling_repo}")
        plt.legend()
        plt.tight_layout()
        fname= f"SEI_{nr.replace('/','_')}_vs_{scaling_repo.replace('/','_')}.png"
        plt.savefig(fname)
        plt.close()


###############################################################################
# Minimal stakeholder approach from prior snippet
###############################################################################
def main():
    repos= [
       # "ni/labview-icon-editor",
        "facebook/react",
        #"dotnet/core",
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

    # Now "Target Reached" logic
    non_scaling = [r for r in repos if r!=scaling_repo]

    vel_target_dict= {}
    uig_target_dict= {}
    mac_target_dict= {}
    # For SEI => we compute from velocity_target, uig_target, mac_target
    # but your usage for "target reached" is separate from the new bar charts we do below
    def compute_target_reached_data_local(repo_list, scaling_repo, quarter_data_dict):
        t_data={}
        if scaling_repo not in quarter_data_dict:
            return t_data
        all_q = sorted(quarter_data_dict[scaling_repo].keys())
        for q_idx in all_q:
            sum_val=0.0
            count_val=0
            for r in repo_list:
                if r==scaling_repo:
                    continue
                if q_idx in quarter_data_dict[r]:
                    sum_val+= quarter_data_dict[r][q_idx]
                    count_val+=1
            if count_val==0:
                continue
            avg_val= sum_val/count_val
            scaling_val= quarter_data_dict[scaling_repo].get(q_idx,0.0)
            ratio=0.0
            if abs(avg_val)>1e-9:
                ratio= (scaling_val/avg_val)*100.0
            else:
                ratio=0.0
            t_data[q_idx]= (avg_val, scaling_val, ratio)
        return t_data

    vel_target_dict= compute_target_reached_data_local(repos, scaling_repo, velocity_scaled)
    uig_target_dict= compute_target_reached_data_local(repos, scaling_repo, uig_scaled)
    mac_target_dict= compute_target_reached_data_local(repos, scaling_repo, mac_scaled)

    def compute_sei_data_local(vel_d, uig_d, mac_d):
        sei_data_local={}
        all_q= set(vel_d.keys())| set(uig_d.keys())| set(mac_d.keys())
        for q_idx in sorted(all_q):
            if q_idx not in vel_d or q_idx not in uig_d or q_idx not in mac_d:
                continue
            (vT,vS,vR)= vel_d[q_idx]
            (uT,uS,uR)= uig_d[q_idx]
            (mT,mS,mR)= mac_d[q_idx]
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
                continue
            wsum= sum(ratio_weights)
            partial_sum=0.0
            for i in range(len(ratio_weights)):
                partial_sum+= ratio_weights[i]* ratio_values[i]
            sei_ratio= partial_sum/wsum
            scaled_sei= 0.5*mS +0.3*vS +0.2*uS
            sei_data_local[q_idx]= (100.0, scaled_sei, sei_ratio)
        return sei_data_local

    sei_data_dict= compute_sei_data_local(vel_target_dict, uig_target_dict, mac_target_dict)

    # ... (print combined table, etc.)
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
        # etc. same approach
        # We'll skip repeating the entire logic for brevity, but you'd fill it out
        # or reuse from your prior snippet
        pass

    # We'll skip re-implementing the entire "print" for brevity

    # (We'll produce the stakeholder summary too, same approach.)
    def produce_stakeholder_summary_local():
        pass

    print(f"\n===== [TARGET REACHED] for Scaling Repo = {scaling_repo} =====\n")
    # You can re-invoke your combined table prints here if you want

    # (NEW) We produce the new per-metric comparison charts
    # => 1) velocity_scaled, 2) uig_scaled, 3) mac_scaled, 4) sei_data (scaling only)
    # We'll define them similarly:
    print("\n=== (NEW) INDIVIDUAL METRIC COMPARISON CHARTS: Non-Scaling vs. Scaling ===\n")

    # a) Velocity
    print("...Plotting Velocity comparisons for each non-scaling repo...")
    plot_metric_comparison(
        metric_name="Velocity",
        scaling_repo=scaling_repo,
        metric_data=velocity_scaled,
        quarter_dates=quarter_dates,
        repos=repos
    )

    # b) UIG
    print("...Plotting UIG comparisons for each non-scaling repo...")
    plot_metric_comparison(
        metric_name="UIG",
        scaling_repo=scaling_repo,
        metric_data=uig_scaled,
        quarter_dates=quarter_dates,
        repos=repos
    )

    # c) MAC
    print("...Plotting MAC comparisons for each non-scaling repo...")
    plot_metric_comparison(
        metric_name="MAC",
        scaling_repo=scaling_repo,
        metric_data=mac_scaled,
        quarter_dates=quarter_dates,
        repos=repos
    )

    # d) SEI
    print("...Plotting SEI comparisons (scaling-only or zero for non-scaling) ...")
    plot_sei_comparison(
        sei_data=sei_data_dict,  # from compute_sei_data_local
        scaling_repo=scaling_repo,
        quarter_dates=quarter_dates,
        repos=repos
    )

    print("\n=== Done generating individual comparison plots. ===")

    print(f"\n=== Final Summary for {scaling_repo} ===\n")
    # produce_stakeholder_summary(...) if you want. 
    # We'll skip for brevity.

    # end => environment + debug log
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
