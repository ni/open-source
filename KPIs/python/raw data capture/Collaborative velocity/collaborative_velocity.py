#!/usr/bin/env python3
#python collaborative_velocity.py --scaling-repo ni/labview-icon-editor --global-offset -32

import argparse
import configparser
import mysql.connector
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import numpy as np
import sys

def get_fiscal_quarter(dt):
    """
    Given a date dt, return which fiscal quarter (Q1/Q2/Q3/Q4) it belongs to,
    assuming the fiscal year runs Oct 1 - Sep 30.
    
    Example: 
      - If dt.month >= 10 => fiscal year = dt.year + 1
      - Else => fiscal year = dt.year
    Then we compare dt to the known date ranges for Q1, Q2, Q3, Q4 in that FY.
    """
    # Determine the fiscal year
    # e.g., if date is 2025-11-10, then FY=2026
    if dt.month >= 10:
        fy = dt.year + 1
    else:
        fy = dt.year
    
    # Build the 4 quarter ranges (start <= dt <= end)
    q1_start = datetime(fy - 1, 10, 1)  # e.g. 2026 => Q1 starts 10/1/2025
    q1_end   = datetime(fy - 1, 12, 31)
    q2_start = datetime(fy, 1, 1)
    q2_end   = datetime(fy, 3, 31)
    q3_start = datetime(fy, 4, 1)
    q3_end   = datetime(fy, 6, 30)
    q4_start = datetime(fy, 7, 1)
    q4_end   = datetime(fy, 9, 30)
    
    if q1_start <= dt <= q1_end:
        return "Q1"
    elif q2_start <= dt <= q2_end:
        return "Q2"
    elif q3_start <= dt <= q3_end:
        return "Q3"
    else:
        return "Q4"

def main():
    # --- 1. Parse Command-Line Arguments ---
    parser = argparse.ArgumentParser(
        description="Compute collaborative velocity with a user-selected scaling repo, compare to average, center bar groups, allow a global time offset, and label the fiscal quarter + date ranges."
    )
    parser.add_argument("--scaling-repo", required=True,
                        help="Name of the repo to use for scaling, e.g. 'facebook/react'")
    parser.add_argument("--global-offset", type=int, default=0,
                        help="Global offset in days to shift all oldest dates (can be negative). Default=0")
    
    args = parser.parse_args()
    scaling_repo = args.scaling_repo
    global_offset_days = args.global_offset  # integer (can be positive or negative)
    
    # --- 2. Define Repos List ---
    repos = [
        "ni/actor-framework",
        "tensorflow/tensorflow",
        "facebook/react",
        "dotnet/core",
        "ni/labview-icon-editor"
    ]
    
    if scaling_repo not in repos:
        print(f"[ERROR] The chosen scaling repo '{scaling_repo}' is not in our known repos:")
        print(repos)
        sys.exit(1)
    
    # --- 3. Read Config for DB Connection ---
    config = configparser.ConfigParser()
    config.read('db_config.ini')  # Make sure this file exists with a [mysql] section
    db_params = config['mysql']
    
    # --- 4. Connect to MySQL ---
    cnx = mysql.connector.connect(
        host=db_params['host'],
        user=db_params['user'],
        password=db_params['password'],
        database=db_params['database']
    )
    cursor = cnx.cursor()
    
    # We measure up to X years from each repo's (offset) oldest date
    X = 2  # 2 years => ~8 windows of 3 months
    
    # Data structures to store raw M/I per repo
    repo_windows_data = {}  # {repo: (list_of_windows, list_of_Mraw, list_of_Iraw)}
    
    # Global offset as a timedelta
    offset_delta = timedelta(days=global_offset_days)
    
    # ---------- QUERIES ----------
    query_oldest_date = """
        SELECT MIN(all_min) AS oldest_date
        FROM (
            SELECT MIN(created_at) AS all_min
            FROM pulls
            WHERE repo_name = %s
            
            UNION ALL
            
            SELECT MIN(created_at) AS all_min
            FROM issues
            WHERE repo_name = %s
        ) AS subq
    """
    
    query_m = """
        SELECT COUNT(*)
        FROM pulls
        WHERE repo_name = %s
          AND merged_at IS NOT NULL
          AND merged_at >= %s
          AND merged_at < %s
    """
    
    query_i = """
        SELECT COUNT(*)
        FROM issues
        WHERE repo_name = %s
          AND closed_at IS NOT NULL
          AND closed_at >= %s
          AND closed_at < %s
    """
    
    # --- 5. Collect Raw Data for Each Repo ---
    for repo in repos:
        # 5A. Find the oldest date
        cursor.execute(query_oldest_date, (repo, repo))
        result = cursor.fetchone()
        if not result or not result[0]:
            # No data for this repo
            repo_windows_data[repo] = ([], [], [])
            continue
        
        oldest_date = result[0]  # datetime object
        
        # --- APPLY THE GLOBAL OFFSET ---
        oldest_date = oldest_date + offset_delta
        
        cutoff = oldest_date + relativedelta(years=X)
        
        # 5B. Build 3-month windows
        windows = []
        current_start = oldest_date
        while current_start < cutoff:
            current_end = current_start + relativedelta(months=3)
            if current_end > cutoff:
                current_end = cutoff
            windows.append((current_start, current_end))
            current_start = current_end
        
        M_raw_list = []
        I_raw_list = []
        
        # 5C. Query merges/issues per window
        for (w_start, w_end) in windows:
            cursor.execute(query_m, (repo, w_start, w_end))
            merged_count = cursor.fetchone()[0]
            
            cursor.execute(query_i, (repo, w_start, w_end))
            closed_count = cursor.fetchone()[0]
            
            M_raw_list.append(merged_count)
            I_raw_list.append(closed_count)
        
        repo_windows_data[repo] = (windows, M_raw_list, I_raw_list)
    
    # --- 6. Identify the Scaling Repo's First Window (M, I) ---
    scaling_windows, scaling_M_raw_list, scaling_I_raw_list = repo_windows_data.get(scaling_repo, ([], [], []))
    
    if not scaling_M_raw_list:
        # If no data for scaling repo, treat first-window as 1,1 to avoid dividing by zero
        print(f"[WARNING] The chosen scaling repo '{scaling_repo}' has no data in the first window.")
        print("We'll skip scaling (use factor=1.0 for everything).")
        scaling_repo_M1 = 1
        scaling_repo_I1 = 1
    else:
        # Actual first-window values
        scaling_repo_M1 = scaling_M_raw_list[0]
        scaling_repo_I1 = scaling_I_raw_list[0]
        if scaling_repo_M1 == 0:
            scaling_repo_M1 = 1
        if scaling_repo_I1 == 0:
            scaling_repo_I1 = 1
    
    # --- 7. Compute Scale Factors for Each Repo (First Window Only) ---
    scaleFactor_M = {}
    scaleFactor_I = {}
    
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data[repo]
        
        if repo == scaling_repo:
            # No scaling needed for the reference repo
            scaleFactor_M[repo] = 1.0
            scaleFactor_I[repo] = 1.0
        else:
            if M_raw_list and M_raw_list[0] > 0:
                scaleFactor_M[repo] = float(scaling_repo_M1) / float(M_raw_list[0])
            else:
                scaleFactor_M[repo] = 1.0
            
            if I_raw_list and I_raw_list[0] > 0:
                scaleFactor_I[repo] = float(scaling_repo_I1) / float(I_raw_list[0])
            else:
                scaleFactor_I[repo] = 1.0
    
    # --- 8. Apply Scale Factors & Compute Velocity ---
    # velocity = 0.4*M_scaled + 0.6*I_scaled
    scaled_M = {}
    scaled_I = {}
    velocity_data = {}
    
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data[repo]
        m_list_scaled = []
        i_list_scaled = []
        v_list = []
        
        for idx in range(len(M_raw_list)):
            m_scaled = M_raw_list[idx] * scaleFactor_M[repo]
            i_scaled = I_raw_list[idx] * scaleFactor_I[repo]
            vel = 0.4*m_scaled + 0.6*i_scaled
            
            m_list_scaled.append(m_scaled)
            i_list_scaled.append(i_scaled)
            v_list.append(vel)
        
        scaled_M[repo] = m_list_scaled
        scaled_I[repo] = i_list_scaled
        velocity_data[repo] = v_list
    
    # --- 9. Print Data for Debugging ---
    print(f"\n[INFO] Scaling repo: {scaling_repo}")
    print(f"[INFO]   Global offset (days) = {global_offset_days}")
    if scaling_windows:
        print(f"[INFO]   First window merges (scaled repo) = {scaling_repo_M1}, issues = {scaling_repo_I1}")
        print(f"[INFO]   Window 1 date range (scaling repo): {scaling_windows[0]}")
    else:
        print(f"[INFO]   Scaling repo has no windows after offset")
    
    for repo in repos:
        print(f"--- Repo: {repo} ---")
        print(f"  scaleFactor_M={scaleFactor_M[repo]:.3f}, scaleFactor_I={scaleFactor_I[repo]:.3f}")
        
        windows, M_raw_list, I_raw_list = repo_windows_data[repo]
        m_scaled_list = scaled_M[repo]
        i_list_scaled = scaled_I[repo]
        v_list = velocity_data[repo]
        
        for w_idx, (w_start, w_end) in enumerate(windows):
            print(f"    Window {w_idx+1} [{w_start} to {w_end}]")
            print(f"      M_raw={M_raw_list[w_idx]}, I_raw={I_raw_list[w_idx]}")
            print(f"      M_scaled={m_scaled_list[w_idx]:.2f}, I_scaled={i_list_scaled[w_idx]:.2f}")
            print(f"      Velocity={v_list[w_idx]:.2f}")
        print()
    
    # --- 10. Pad Repos to Have Same # of Windows ---
    max_windows = max(len(repo_windows_data[r][0]) for r in repos)
    
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data[repo]
        needed = max_windows - len(M_raw_list)
        if needed > 0:
            M_raw_list.extend([0]*needed)
            I_raw_list.extend([0]*needed)
            scaled_M[repo].extend([0]*needed)
            scaled_I[repo].extend([0]*needed)
            velocity_data[repo].extend([0]*needed)
    
    # === Build x_labels with (Quarter, Start Date, End Date) on separate lines ===
    x_labels = []
    for i in range(max_windows):
        if i < len(scaling_windows):
            w_start = scaling_windows[i][0]
            w_end   = scaling_windows[i][1]
            
            # 1) Which fiscal quarter does w_start belong to?
            quarter = get_fiscal_quarter(w_start)
            
            # 2) Format the dates
            start_date_str = w_start.strftime("%m/%d/%Y")
            end_date_str   = w_end.strftime("%m/%d/%Y")
            
            # 3) Build the multi-line label
            label = f"{quarter}\n{start_date_str}\n{end_date_str}"
        else:
            label = "N/A"
        
        x_labels.append(label)
    
    # =============================
    # 11. Original Grouped Bar Charts
    # =============================
    x = np.arange(max_windows)
    bar_width = 0.15
    n_repos = len(repos)
    
    # A. Velocity Chart
    plt.figure(figsize=(10, 6))
    for i_repo, repo in enumerate(repos):
        x_positions = x + (i_repo - (n_repos - 1)/2)*bar_width
        v_list = velocity_data[repo]
        label_str = f"{repo} (M_fact={scaleFactor_M[repo]:.2f}, I_fact={scaleFactor_I[repo]:.2f})"
        plt.bar(x_positions, v_list, bar_width, label=label_str)
    
    plt.title(f"Velocity (Scaled using {scaling_repo}'s First Window) - Offset={global_offset_days} days")
    plt.xlabel("Window Index")
    plt.ylabel("Velocity (0.4*M + 0.6*I)")
    plt.text(
        0.5, 0.90,
        "Velocity = 0.4 × M_scaled + 0.6 × I_scaled",
        transform=plt.gca().transAxes,
        fontsize=10,
        ha='center'
    )
    plt.xticks(x, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig("velocity.png")
    plt.show()
    
    # B. Scaled M Chart
    plt.figure(figsize=(10, 6))
    for i_repo, repo in enumerate(repos):
        x_positions = x + (i_repo - (n_repos - 1)/2)*bar_width
        m_list = scaled_M[repo]
        label_str = f"{repo} (scaleFactorM={scaleFactor_M[repo]:.2f})"
        plt.bar(x_positions, m_list, bar_width, label=label_str)
    
    plt.title(f"Scaled M (PR merges) - Baseline: {scaling_repo}'s First Window\nOffset={global_offset_days} days")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled # of PR Merges")
    plt.xticks(x, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig("scaled_m.png")
    plt.show()
    
    # C. Scaled I Chart
    plt.figure(figsize=(10, 6))
    for i_repo, repo in enumerate(repos):
        x_positions = x + (i_repo - (n_repos - 1)/2)*bar_width
        i_list = scaled_I[repo]
        label_str = f"{repo} (scaleFactorI={scaleFactor_I[repo]:.2f})"
        plt.bar(x_positions, i_list, bar_width, label=label_str)
    
    plt.title(f"Scaled I (Issues Closed) - Baseline: {scaling_repo}'s First Window\nOffset={global_offset_days} days")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled # of Issues Closed")
    plt.xticks(x, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig("scaled_i.png")
    plt.show()
    
    # =============================
    # 12. Compare Scaling Repo vs. Average
    # =============================
    other_repos = [r for r in repos if r != scaling_repo]
    
    avg_M_list = []
    avg_I_list = []
    avg_vel_list = []
    
    for w_idx in range(max_windows):
        sum_m = sum(scaled_M[r][w_idx] for r in other_repos)
        sum_i = sum(scaled_I[r][w_idx] for r in other_repos)
        sum_v = sum(velocity_data[r][w_idx] for r in other_repos)
        
        count = len(other_repos)
        
        if count > 0:
            avg_m = sum_m / count
            avg_i = sum_i / count
            avg_v = sum_v / count
        else:
            avg_m = 0
            avg_i = 0
            avg_v = 0
        
        avg_M_list.append(avg_m)
        avg_I_list.append(avg_i)
        avg_vel_list.append(avg_v)
    
    # 12B. Compare M
    plt.figure(figsize=(10, 6))
    bar_width_compare = 0.3
    
    x_scaling = x - bar_width_compare/2
    x_avg     = x + bar_width_compare/2
    
    plt.bar(x_scaling,
            scaled_M[scaling_repo],
            bar_width_compare,
            label=f"{scaling_repo} (Scaling Repo)",
            color='tab:blue')
    plt.bar(x_avg,
            avg_M_list,
            bar_width_compare,
            label="Other Repos Average",
            color='tab:orange')
    
    plt.title(f"Comparing Scaled Merges\n{scaling_repo} vs. Others (Offset={global_offset_days} days)")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled # of PR Merges")
    plt.xticks(x, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig("compare_m.png")
    plt.show()
    
    # 12C. Compare I
    plt.figure(figsize=(10, 6))
    plt.bar(x_scaling,
            scaled_I[scaling_repo],
            bar_width_compare,
            label=f"{scaling_repo} (Scaling Repo)",
            color='tab:blue')
    plt.bar(x_avg,
            avg_I_list,
            bar_width_compare,
            label="Other Repos Average",
            color='tab:orange')
    
    plt.title(f"Comparing Scaled Issues\n{scaling_repo} vs. Others (Offset={global_offset_days} days)")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled # of Issues Closed")
    plt.xticks(x, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig("compare_i.png")
    plt.show()
    
    # 12D. Compare Velocity
    plt.figure(figsize=(10, 6))
    plt.bar(x_scaling,
            velocity_data[scaling_repo],
            bar_width_compare,
            label=f"{scaling_repo} (Scaling Repo)",
            color='tab:blue')
    plt.bar(x_avg,
            avg_vel_list,
            bar_width_compare,
            label="Other Repos Average",
            color='tab:orange')
    
    plt.title(f"Comparing Velocity\n{scaling_repo} vs. Others (Offset={global_offset_days} days)")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled Velocity (0.4*M + 0.6*I)")
    plt.xticks(x, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig("compare_velocity.png")
    plt.show()
    
    # 12E. Print comparison table
    print("\n=== Comparison Table: Scaling Repo vs. Other Repos Average (Offset={} days) ===".format(global_offset_days))
    print("Window |   M%    |   I%    | Velocity%")
    print("-------+---------+---------+----------")
    
    for w_idx in range(max_windows):
        sr_m = scaled_M[scaling_repo][w_idx]
        sr_i = scaled_I[scaling_repo][w_idx]
        sr_v = velocity_data[scaling_repo][w_idx]
        
        avg_m = avg_M_list[w_idx]
        avg_i = avg_I_list[w_idx]
        avg_v = avg_vel_list[w_idx]
        
        if avg_m != 0:
            ratio_m = 100.0*(sr_m/avg_m)
        else:
            ratio_m = 0
        if avg_i != 0:
            ratio_i = 100.0*(sr_i/avg_i)
        else:
            ratio_i = 0
        if avg_v != 0:
            ratio_v = 100.0*(sr_v/avg_v)
        else:
            ratio_v = 0
        
        print(f"W{w_idx+1:<5} | {ratio_m:7.2f}% | {ratio_i:7.2f}% | {ratio_v:8.2f}%")
    
    # 13. Close DB Connection
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
