#!/usr/bin/env python3

import sys
import os
import io
import math
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Our existing modules:
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
# 1) Setup console capture so we can also write to debug_log.txt at the end
###############################################################################
original_stdout = sys.stdout  # keep a reference to original
log_capture = io.StringIO()

class DualOutput:
    """
    A file-like class that writes to both original stdout and a StringIO buffer.
    """
    def write(self, text):
        original_stdout.write(text)   # show on screen
        log_capture.write(text)       # capture in memory

    def flush(self):
        original_stdout.flush()
        log_capture.flush()

sys.stdout = DualOutput()

###############################################################################
# 2) Table-Printing Utility
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
# 3) Print existing quarter data in a table, raw cols with no decimals
#    scaled/derived at 4 decimals, plus scaling factor info
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
# 4) Standard "Target Reached" + SEI logic from prior snippet
###############################################################################
def compute_target_reached_data(repo_list, scaling_repo, quarter_data_dict):
    target_data={}
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
            partial_sum+= ratio_weights[i]*ratio_values[i]
        sei_ratio= partial_sum/wsum
        scaled_sei= 0.5*mS +0.3*vS +0.2*uS
        sei_data[q_idx]= (100.0, scaled_sei, sei_ratio)
    return sei_data

def print_combined_target_table(scaling_repo, quarter_dates, velocity_target, uig_target, mac_target, sei_data):
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
    q_idxs= sorted(quarter_dates[scaling_repo].keys())
    for q_idx in q_idxs:
        (qs,qe)= quarter_dates[scaling_repo][q_idx]
        q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
        (vT,vS,vR)= velocity_target.get(q_idx,(0,0,0))
        (uT,uS,uR)= uig_target.get(q_idx,(0,0,0))
        (mT,mS,mR)= mac_target.get(q_idx,(0,0,0))
        (sT,sS,sR)= sei_data.get(q_idx,(0,0,0))
        def f4(x): return f"{x:.4f}"
        row= [
            q_label,
            f4(vT), f4(vS), f4(vR),
            f4(uT), f4(uS), f4(uR),
            f4(mT), f4(mS), f4(mR),
            f4(sT), f4(sS), f4(sR)
        ]
        table.append(row)
    print_aligned_table(table, align)

def print_metric_table(metric_name, data_dict, scaling_repo, quarter_dates):
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
        table.append([
            q_label,
            f"{tVal:.4f}",
            f"{sVal:.4f}",
            f"{rVal:.4f}"
        ])
    print_aligned_table(table, align)

def print_sei_table(sei_data, scaling_repo, quarter_dates):
    header= ["Quarter","SEI Target","Scaled SEI","SEI Ratio"]
    align= ["left","center","center","center"]
    table= [header]
    if scaling_repo not in quarter_dates:
        return
    q_idxs= sorted(quarter_dates[scaling_repo].keys())
    for q_idx in q_idxs:
        if q_idx not in sei_data:
            continue
        (tVal, sVal, ratio)= sei_data[q_idx]
        (qs,qe)= quarter_dates[scaling_repo][q_idx]
        q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
        table.append([
            q_label,
            f"{tVal:.4f}",
            f"{sVal:.4f}",
            f"{ratio:.4f}"
        ])
    print_aligned_table(table, align)


def main():
    repos= [
        "tensorflow/tensorflow",
 #       "dotnet/core",
        "facebook/react",
        "ni/labview-icon-editor"
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

    # Print each repo's quarter data in a table
    for repo in repos:
        if repo not in existing_data_dict or len(existing_data_dict[repo])==0:
            continue
        print_existing_quarter_data_table(
            repo, sfM, sfI, sfF, sfS, sfP,
            existing_data_dict[repo]
        )
        print(f"\n--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")
        print_calculation_details(repo, detail_calc_dict[repo])

    # Next => compute "Target Reached" ...
    non_scaling = [r for r in repos if r!=scaling_repo]

    def compute_target_reached_data(repo_list, scaling_repo, quarter_data_dict):
        target_data={}
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

    velocity_target= compute_target_reached_data(repos, scaling_repo, velocity_scaled)
    uig_target= compute_target_reached_data(repos, scaling_repo, uig_scaled)
    mac_target= compute_target_reached_data(repos, scaling_repo, mac_scaled)
    sei_data= compute_sei_data(velocity_target, uig_target, mac_target)

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
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
        for q_idx in q_idxs:
            qs,qe= quarter_dates[scaling_repo][q_idx]
            q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
            (vT,vS,vR)= velocity_target.get(q_idx,(0,0,0))
            (uT,uS,uR)= uig_target.get(q_idx,(0,0,0))
            (mT,mS,mR)= mac_target.get(q_idx,(0,0,0))
            (sT,sS,sR)= sei_data.get(q_idx,(0,0,0))
            def f4(x): return f"{x:.4f}"
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
        if scaling_repo not in quarter_dates:
            return
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
        for q_idx in q_idxs:
            if q_idx not in sei_data:
                continue
            (tVal, sVal, ratio)= sei_data[q_idx]
            (qs,qe)= quarter_dates[scaling_repo][q_idx]
            q_label= f"Q{q_idx}({qs:%Y-%m-%d}-{qe:%Y-%m-%d})"
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

    import numpy as np

    if scaling_repo in quarter_dates:
        q_idxs= sorted(quarter_dates[scaling_repo].keys())
    else:
        q_idxs=[]
    q_labels=[]
    velT_list=[]
    velS_list=[]
    uigT_list=[]
    uigS_list=[]
    macT_list=[]
    macS_list=[]

    for q_idx in q_idxs:
        (qs,qe)= quarter_dates[scaling_repo][q_idx]
        q_labels.append(f"Q{q_idx}")
        (vT,vS,vR)= velocity_target.get(q_idx,(0,0,0))
        velT_list.append(vT)
        velS_list.append(vS)
        (uT,uS,uR)= uig_target.get(q_idx,(0,0,0))
        uigT_list.append(uT)
        uigS_list.append(uS)
        (mT,mS,mR)= mac_target.get(q_idx,(0,0,0))
        macT_list.append(mT)
        macS_list.append(mS)

    barw=0.12
    x= np.arange(len(q_idxs))
    plt.figure(figsize=(10,5))
    plt.bar(x - 2*barw, velT_list, barw, label="Velocity Target", color='lightblue')
    plt.bar(x - 1*barw, velS_list, barw, label="Velocity Scaling", color='blue')
    plt.bar(x + 0*barw, uigT_list, barw, label="UIG Target", color='lightgreen')
    plt.bar(x + 1*barw, uigS_list, barw, label="UIG Scaling", color='green')
    plt.bar(x + 2*barw, macT_list, barw, label="MAC Target", color='lightcoral')
    plt.bar(x + 3*barw, macS_list, barw, label="MAC Scaling", color='red')
    plt.xticks(x, q_labels, rotation=45, ha='right')
    plt.title(f"Target vs. Scaling (Velocity, UIG, MAC) for {scaling_repo}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"combined_V_U_M_{scaling_repo.replace('/','_')}.png")
    plt.close()

    # separate bar chart for SEI => 2 bars/quarter => target vs. scaled
    # gather from sei_data
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
        barw2=0.3
        plt.figure(figsize=(8,4))
        plt.bar(x2 - barw2/2, s_target, barw2, label="SEI Target", color='gray')
        plt.bar(x2 + barw2/2, s_scaled, barw2, label=f"SEI {scaling_repo}", color='orange')
        if len(sei_q_idxs)>0:
            first_q=sei_q_idxs[0]
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
    print("All tables & bar charts generated with quarter-based data in table form.\n")

    ############################################################################
    # 5) End of main => let's restore stdout and write the captured console
    #    plus environment variables to debug_log.txt
    ############################################################################
    sys.stdout.flush()
    global log_capture
    console_text = log_capture.getvalue()

    # restore original stdout
    sys.stdout = original_stdout

    # environment variables that might be relevant
    scaling_repo_env = os.environ.get("SCALING_REPO","<not set>")
    fiscal_q_env     = os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
    # add more if needed

    debug_file = "debug_log.txt"
    with open(debug_file, "w", encoding="utf-8") as f:
        f.write("=== ENVIRONMENT VARIABLES ===\n")
        f.write(f"SCALING_REPO={scaling_repo_env}\n")
        f.write(f"NUM_FISCAL_QUARTERS={fiscal_q_env}\n")
        f.write("\n=== CAPTURED CONSOLE OUTPUT ===\n")
        f.write(console_text)

    print(f"[INFO] Debug log saved to {debug_file}")

if __name__=="__main__":
    main()
