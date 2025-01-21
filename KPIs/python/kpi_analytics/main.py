#!/usr/bin/env python3

import sys
import os
import math
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Existing modules (must be in the same package/folder structure)
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
# Console table printer with basic alignment
###############################################################################
def print_aligned_table(table_data, alignments=None):
    """
    Print a 2D list (table_data) with optional alignment for each column.
      alignments is a list of 'left','center','right', default='left'.
    """
    if not table_data:
        return
    num_cols = len(table_data[0])
    if alignments is None:
        alignments = ['left']*num_cols
    if len(alignments) < num_cols:
        alignments += ['left']*(num_cols - len(alignments))

    # compute max widths
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

    # print header
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
# Compute "Target Reached" for a given metric:
# We'll gather scaled data from each repo, skip the scaling repo, average them,
# then produce ratio for the scaling repo's scaled data vs. that average.
###############################################################################
def compute_target_reached_data(repo_list, scaling_repo, quarter_data_dict):
    """
    quarter_data_dict[repo][q_idx] = scaled_value_for_that metric (Velocity, etc.)
    Return a dict => target_data[q_idx] = (targetAvg, scalingVal, ratio)
    ratio in percentage => (scalingVal / targetAvg)*100
    skip quarters where no non-scaling repo has data => skip them entirely
    """
    target_data = {}
    for q_idx in sorted(quarter_data_dict[scaling_repo].keys()):
        sum_val = 0.0
        count_val=0
        for r in repo_list:
            if r==scaling_repo:
                continue
            if q_idx in quarter_data_dict[r]:
                sum_val += quarter_data_dict[r][q_idx]
                count_val+=1
        if count_val==0:
            continue
        avg_val = sum_val/count_val
        scaling_val = quarter_data_dict[scaling_repo].get(q_idx,0.0)
        ratio = 0.0
        if abs(avg_val)<1e-9:
            ratio=0.0
        else:
            ratio= (scaling_val/avg_val)*100.0
        target_data[q_idx] = (avg_val, scaling_val, ratio)
    return target_data

###############################################################################
# We'll define a function to compute "SEI ratio" for the scaling repo only:
# SEI ratio = 0.5*MAC_ratio + 0.3*Velocity_ratio + 0.2*UIG_ratio,
# ignoring metrics whose target=0 => skip that portion, renormalize weights
###############################################################################
def compute_sei_data(velocity_data, uig_data, mac_data):
    """
    velocity_data[q_idx] = (vT, vS, vRatio)
    returns => sei_data[q_idx] = (sei_target, scaled_sei, sei_ratio)
       sei_target can be 100.0
       scaled_sei = 0.5*mS + 0.3*vS + 0.2*uS (the absolute scaled sum)
       ratio => skip metrics with 0 target, renormalize weights
    """
    sei_data={}
    all_q = sorted(set(velocity_data.keys())|set(uig_data.keys())|set(mac_data.keys()))
    for q_idx in all_q:
        if q_idx not in velocity_data or q_idx not in uig_data or q_idx not in mac_data:
            continue
        (vT,vS,vR) = velocity_data[q_idx]
        (uT,uS,uR) = uig_data[q_idx]
        (mT,mS,mR) = mac_data[q_idx]

        # gather
        ratio_weights=[]
        ratio_values=[]
        # velocity => weight=0.3
        if abs(vT)>1e-9:
            ratio_weights.append(0.3)
            ratio_values.append(vR)
        # uig => weight=0.2
        if abs(uT)>1e-9:
            ratio_weights.append(0.2)
            ratio_values.append(uR)
        # mac => weight=0.5
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

        # scaled SEI => 0.5*mS +0.3*vS +0.2*uS
        scaled_sei= 0.5*mS + 0.3*vS + 0.2*uS
        # we'll define "SEI target"=100.0
        sei_data[q_idx]= (100.0, scaled_sei, sei_ratio)
    return sei_data

def main():
    repos= [
        "ni/labview-icon-editor",
        "facebook/react",
        "tensorflow/tensorflow",
        "dotnet/core"
        # add more if needed
    ]
    scaling_repo= get_scaling_repo()
    if not scaling_repo:
        print("[ERROR] No scaling repo found. Exiting.")
        sys.exit(1)
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    # get scale factors => merges(M), issues(I), forks(F), stars(S), pulls(P)
    sfM, sfI, sfF, sfS, sfP = compute_scale_factors(scaling_repo, repos)
    num_quarters= get_num_fiscal_quarters()
    now= datetime.utcnow()

    # We'll store quarter-based scaled velocity, uig, mac
    velocity_scaled={}
    uig_scaled={}
    mac_scaled={}
    # also store (start,end) for each quarter => quarter_dates[repo][q_idx]=(start,end)
    quarter_dates={}
    # We'll do your existing console prints, gather data
    for repo in repos:
        velocity_scaled[repo]={}
        uig_scaled[repo]={}
        mac_scaled[repo]={}
        oldest_dt= find_oldest_date_for_repo(repo)
        if not oldest_dt:
            print(f"[INFO] No data for {repo}, skipping.")
            continue

        # generate index-based quarters
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

        print(f"\n---------- {repo} EXISTING QUARTER PRINTS ----------")
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

            # scale them
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

            # store in velocity_scaled
            velocity_scaled[repo][q_idx]= vel
            uig_scaled[repo][q_idx]= uigv
            mac_scaled[repo][q_idx]= macv
            quarter_dates[repo][q_idx]= (q_start,q_end)

            print(f"Q{q_idx}({q_start:%Y-%m-%d}..{q_end:%Y-%m-%d}): merges={mergesRaw}, closed={closedRaw}, forks={forksRaw}, stars={starsRaw}")
            # keep any other debug prints
            q_idx+=1

    # Next => compute "Target Reached" for velocity, uig, mac
    non_scaling_repos = [r for r in repos if r!=scaling_repo]
    velocity_target_data= compute_target_reached_data(repos, scaling_repo, velocity_scaled)
    uig_target_data= compute_target_reached_data(repos, scaling_repo, uig_scaled)
    mac_target_data= compute_target_reached_data(repos, scaling_repo, mac_scaled)

    # Then => compute SEI => 0.5*(MAC ratio) +0.3*(Velocity ratio)+0.2*(UIG ratio),
    # ignoring any metric with 0 target => re-normalize
    sei_data= compute_sei_data(velocity_target_data, uig_target_data, mac_target_data)

    # -----------
    # Step: Print combined table => (Velocity, UIG, MAC, SEI) side by side
    # plus separate tables => velocity, uig, mac, sei
    # 4 decimals for everything
    # We'll define a few functions:
    # -----------
    def print_combined_target_table():
        header= [
          "Quarter",
          "Velocity Target","Scaled Velocity","Velocity Ratio",
          "UIG Target","Scaled UIG","UIG Ratio",
          "MAC Target","Scaled MAC","MAC Ratio",
          "SEI Target","Scaled SEI","SEI Ratio"
        ]
        align= ["left"] + ["center"]*(len(header)-1)
        table= [header]
        # gather all quarter indexes from scaling repo
        if scaling_repo not in quarter_dates:
            return
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
        for q_idx in q_idxs:
            # form row
            qs,qe= quarter_dates[scaling_repo][q_idx]
            q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            # velocity
            if q_idx in velocity_target_data:
                (vT,vS,vR)= velocity_target_data[q_idx]
            else:
                vT,vS,vR=(0,0,0)
            # uig
            if q_idx in uig_target_data:
                (uT,uS,uR)= uig_target_data[q_idx]
            else:
                uT,uS,uR=(0,0,0)
            # mac
            if q_idx in mac_target_data:
                (mT,mS,mR)= mac_target_data[q_idx]
            else:
                mT,mS,mR=(0,0,0)
            # sei
            if q_idx in sei_data:
                (sT,sS,sR)= sei_data[q_idx]
            else:
                sT,sS,sR=(0,0,0)

            def f4(x):
                return f"{x:.4f}"
            row= [
                q_label,
                f4(vT), f4(vS), f4(vR),
                f4(uT), f4(uS), f4(uR),
                f4(mT), f4(mS), f4(mR),
                f4(sT), f4(sS), f4(sR)
            ]
            table.append(row)
        print_aligned_table(table, align)

    def print_metric_table(metric_name, data_dict):
        # Q, target, scalingVal, ratio
        header= [ "Quarter", f"{metric_name} Target", f"Scaled {metric_name}", f"{metric_name} Ratio"]
        align= ["left","center","center","center"]
        table= [header]
        if scaling_repo not in quarter_dates:
            return
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
        for q_idx in q_idxs:
            if q_idx not in data_dict:
                continue
            (tVal,sVal,rVal)= data_dict[q_idx]
            (qs,qe)= quarter_dates[scaling_repo][q_idx]
            q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            row= [
                q_label,
                f"{tVal:.4f}",
                f"{sVal:.4f}",
                f"{rVal:.4f}"
            ]
            table.append(row)
        print_aligned_table(table, align)

    def print_sei_table():
        # Q, target=100.0, scaledSEI, ratio
        header= ["Quarter","SEI Target","Scaled SEI","SEI Ratio"]
        align= ["left","center","center","center"]
        table= [header]
        if scaling_repo not in quarter_dates:
            return
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
        for q_idx in q_idxs:
            if q_idx not in sei_data:
                continue
            (tVal,sVal,rVal)= sei_data[q_idx]
            (qs,qe)= quarter_dates[scaling_repo][q_idx]
            q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            table.append([
                q_label,
                f"{tVal:.4f}",
                f"{sVal:.4f}",
                f"{rVal:.4f}"
            ])
        print_aligned_table(table, align)

    print(f"\n===== [TARGET REACHED] for Scaling Repo = {scaling_repo} =====\n")

    print("=== Combined Table (Velocity, UIG, MAC, SEI) ===")
    print_combined_target_table()

    print("\n=== Velocity Target Reached (Separate) ===")
    print_metric_table("Velocity", velocity_target_data)
    print("\n=== UIG Target Reached (Separate) ===")
    print_metric_table("UIG", uig_target_data)
    print("\n=== MAC Target Reached (Separate) ===")
    print_metric_table("MAC", mac_target_data)
    print("\n=== SEI Target Reached (Separate) ===")
    print_sei_table()

    # -----------
    # Step: produce bar charts
    # For Velocity/UIG/MAC => 6 bars/quarter => target vs. scaling for each metric
    # A separate bar chart for SEI => 2 bars/quarter
    # -----------
    import numpy as np

    # gather the quarter indexes from scaling repo
    if scaling_repo in quarter_dates:
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
    else:
        q_idxs=[]

    # build data lists
    q_labels=[]
    velT_list=[]
    velS_list=[]
    uigT_list=[]
    uigS_list=[]
    macT_list=[]
    macS_list=[]
    for q_idx in q_idxs:
        if q_idx in quarter_dates[scaling_repo]:
            (qs,qe)= quarter_dates[scaling_repo][q_idx]
            q_labels.append(f"Q{q_idx}")
        else:
            q_labels.append(f"Q{q_idx}")
        # velocity
        if q_idx in velocity_target_data:
            (vT,vS,vR)= velocity_target_data[q_idx]
        else:
            vT,vS= (0,0)
        velT_list.append(vT)
        velS_list.append(vS)

        # uig
        if q_idx in uig_target_data:
            (uT,uS,uR)= uig_target_data[q_idx]
        else:
            uT,uS=(0,0)
        uigT_list.append(uT)
        uigS_list.append(uS)

        # mac
        if q_idx in mac_target_data:
            (mT,mS,mR)= mac_target_data[q_idx]
        else:
            mT,mS=(0,0)
        macT_list.append(mT)
        macS_list.append(mS)

    x= np.arange(len(q_idxs))
    barw=0.12
    plt.figure(figsize=(10,5))
    # velocity => 2 bars
    plt.bar(x - 2*barw, velT_list, barw, label="Velocity Target", color='lightblue')
    plt.bar(x - 1*barw, velS_list, barw, label="Velocity Scaling", color='blue')
    # uig => 2 bars
    plt.bar(x + 0*barw, uigT_list, barw, label="UIG Target", color='lightgreen')
    plt.bar(x + 1*barw, uigS_list, barw, label="UIG Scaling", color='green')
    # mac => 2 bars
    plt.bar(x + 2*barw, macT_list, barw, label="MAC Target", color='lightcoral')
    plt.bar(x + 3*barw, macS_list, barw, label="MAC Scaling", color='red')

    plt.xticks(x, q_labels, rotation=45, ha='right')
    plt.title(f"Target vs. Scaling (Velocity, UIG, MAC) for {scaling_repo}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"combined_V_U_M_{scaling_repo.replace('/','_')}.png")
    plt.close()

    # separate bar chart for SEI => 2 bars/quarter => target vs. scaled
    # gather SEI
    sei_q_idxs= sorted(sei_data.keys())
    if sei_q_idxs:
        s_labels=[]
        s_target=[]
        s_scaled=[]
        for q_idx in sei_q_idxs:
            (tVal,sVal,rVal)= sei_data[q_idx]
            s_labels.append(f"Q{q_idx}")
            s_target.append(tVal)
            s_scaled.append(sVal)
        x2= np.arange(len(sei_q_idxs))
        barw2= 0.3
        plt.figure(figsize=(8,4))
        plt.bar(x2 - barw2/2, s_target, barw2, label="SEI Target", color='gray')
        plt.bar(x2 + barw2/2, s_scaled, barw2, label=f"SEI {scaling_repo}", color='orange')
        # date range => from earliest to latest q_idx
        if len(sei_q_idxs)>0:
            first_q= sei_q_idxs[0]
            last_q= sei_q_idxs[-1]
            if first_q in quarter_dates[scaling_repo] and last_q in quarter_dates[scaling_repo]:
                (qs1,qe1)= quarter_dates[scaling_repo][first_q]
                (qsN,qeN)= quarter_dates[scaling_repo][last_q]
                date_range_str= f"{qs1:%Y-%m-%d} to {qeN:%Y-%m-%d}"
            else:
                date_range_str= "N/A"
        else:
            date_range_str= "N/A"

        plt.xticks(x2, s_labels, rotation=45, ha='right')
        plt.title(f"SEI Target for {scaling_repo} {date_range_str}")
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"sei_chart_{scaling_repo.replace('/','_')}.png")
        plt.close()

    print(f"\n=== Final Summary for {scaling_repo} ===")
    print("All tables & bar charts have been generated successfully.\n")

if __name__=="__main__":
    main()
