#!/usr/bin/env python3

import configparser
import mysql.connector
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from datetime import datetime
import numpy as np

def main():
    # --- 1. Read Config ---
    config = configparser.ConfigParser()
    config.read('db_config.ini')
    db_params = config['mysql']
    
    # --- 2. Connect to DB ---
    cnx = mysql.connector.connect(
        host=db_params['host'],
        user=db_params['user'],
        password=db_params['password'],
        database=db_params['database']
    )
    cursor = cnx.cursor()
    
    # Repos to analyze
    repos = [
        "ni/actor-framework",
        "tensorflow/tensorflow",
        "facebook/react",
        "dotnet/core", 
        "ni/labview-icon-editor"
    ]
    
    # Number of years to measure from each repo's oldest date
    X = 1  # e.g., 2 years => ~8 windows of 3 months each
    
    # Data structures to store the raw data
    repo_windows_data = {}  # {repo: (windows_list, M_raw_list, I_raw_list)}
    
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
    
    # --- 3. For Each Repo, find oldest date and build windows ---
    for repo in repos:
        print(f"\n[DEBUG] Finding oldest date for repo: {repo}")
        print("[DEBUG] Query (oldest_date):")
        print(query_oldest_date.strip())
        
        cursor.execute(query_oldest_date, (repo, repo))
        result = cursor.fetchone()
        if not result or not result[0]:
            # No data for this repo
            print(f"[DEBUG] No oldest_date found for repo={repo}")
            repo_windows_data[repo] = ([], [], [])
            continue
        
        oldest_date = result[0]  # datetime object
        print(f"[DEBUG] oldest_date => {oldest_date}")
        
        # 3-month windows up to X years from oldest_date
        windows = []
        cutoff = oldest_date + relativedelta(years=X)
        
        current_start = oldest_date
        while current_start < cutoff:
            current_end = current_start + relativedelta(months=3)
            if current_end > cutoff:
                current_end = cutoff
            windows.append((current_start, current_end))
            current_start = current_end
        
        M_raw_list = []
        I_raw_list = []
        
        # Gather M, I for each window
        for (w_start, w_end) in windows:
            # PR merges
            cursor.execute(query_m, (repo, w_start, w_end))
            merged_count = cursor.fetchone()[0]
            
            # Issues closed
            cursor.execute(query_i, (repo, w_start, w_end))
            closed_count = cursor.fetchone()[0]
            
            M_raw_list.append(merged_count)
            I_raw_list.append(closed_count)
        
        repo_windows_data[repo] = (windows, M_raw_list, I_raw_list)
    
    # --- 4. Compute max values over FIRST 4 windows ONLY ---
    # We'll collect:
    #   M_max_4[repo] = maximum M in the first 4 windows
    #   I_max_4[repo] = maximum I in the first 4 windows
    M_max_4 = {}
    I_max_4 = {}
    
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data.get(repo, ([], [], []))
        
        # We only look at the first 4 windows, or fewer if the repo has <4
        limit = min(4, len(M_raw_list))
        
        if limit > 0:
            M_max_4[repo] = max(M_raw_list[:limit])
            I_max_4[repo] = max(I_raw_list[:limit])
        else:
            # No data
            M_max_4[repo] = 0
            I_max_4[repo] = 0
    
    # --- 5. Find the global smallest max among repos, restricted to the first 4 windows
    if len(M_max_4) > 0:
        global_M_max = [M_max_4[r] for r in repos if M_max_4[r] > 0]
        if global_M_max:
            M_min_of_max = min(global_M_max)
        else:
            M_min_of_max = 0
    else:
        M_min_of_max = 0
    
    if len(I_max_4) > 0:
        global_I_max = [I_max_4[r] for r in repos if I_max_4[r] > 0]
        if global_I_max:
            I_min_of_max = min(global_I_max)
        else:
            I_min_of_max = 0
    else:
        I_min_of_max = 0
    
    print("\n[DEBUG] Max Alignment (based on first 4 windows)")
    print(f"  M_min_of_max = {M_min_of_max}")
    print(f"  I_min_of_max = {I_min_of_max}")
    
    # --- 6. Compute scale factors for each repo ---
    # scaleFactor_M[repo] = M_min_of_max / M_max_4[repo]
    # scaleFactor_I[repo] = I_min_of_max / I_max_4[repo]
    scaleFactor_M = {}
    scaleFactor_I = {}
    
    for repo in repos:
        m4 = M_max_4.get(repo, 0)
        i4 = I_max_4.get(repo, 0)
        
        if m4 > 0:
            scaleFactor_M[repo] = M_min_of_max / m4
        else:
            scaleFactor_M[repo] = 1.0
        
        if i4 > 0:
            scaleFactor_I[repo] = I_min_of_max / i4
        else:
            scaleFactor_I[repo] = 1.0
        
        print(f"[DEBUG] Repo={repo}, M_max_4={m4}, I_max_4={i4}, "
              f"scaleFactor_M={scaleFactor_M[repo]:.4f}, scaleFactor_I={scaleFactor_I[repo]:.4f}")
    
    # --- 7. Apply scaling & compute velocity ---
    # velocity = 0.4 * M_scaled + 0.6 * I_scaled  (example weights)
    scaled_M = {}
    scaled_I = {}
    velocity_data = {}
    
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data.get(repo, ([], [], []))
        m_scaled_list = []
        i_scaled_list = []
        v_list = []
        
        for idx in range(len(M_raw_list)):
            m_scaled = M_raw_list[idx] * scaleFactor_M[repo]
            i_scaled = I_raw_list[idx] * scaleFactor_I[repo]
            vel = 0.4 * m_scaled + 0.6 * i_scaled
            
            m_scaled_list.append(m_scaled)
            i_scaled_list.append(i_scaled)
            v_list.append(vel)
        
        scaled_M[repo] = m_scaled_list
        scaled_I[repo] = i_scaled_list
        velocity_data[repo] = v_list
    
    # --- 8. Print Magnitudes on Command Line ---
    print("\n[DEBUG] Final Raw + Scaled Data + Velocity per Window:\n")
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data.get(repo, ([], [], []))
        m_scaled_list = scaled_M[repo]
        i_scaled_list = scaled_I[repo]
        v_list = velocity_data[repo]
        
        for w_idx, (w_start, w_end) in enumerate(windows):
            print(f"Repo: {repo}, Window {w_idx+1} ({w_start} to {w_end})")
            print(f"  M_raw = {M_raw_list[w_idx]}, I_raw = {I_raw_list[w_idx]}")
            print(f"  M_scaled = {m_scaled_list[w_idx]:.2f}, I_scaled = {i_scaled_list[w_idx]:.2f}")
            print(f"  Velocity = {v_list[w_idx]:.2f}")
            print("---")
    
    # --- 9. Padding: ensure same # of windows for all repos
    max_windows = max(len(repo_windows_data[r][0]) for r in repos)
    
    for repo in repos:
        windows, M_raw_list, I_raw_list = repo_windows_data.get(repo, ([], [], []))
        current_len = len(M_raw_list)
        needed = max_windows - current_len
        
        if needed > 0:
            M_raw_list.extend([0]*needed)
            I_raw_list.extend([0]*needed)
            scaled_M[repo].extend([0]*needed)
            scaled_I[repo].extend([0]*needed)
            velocity_data[repo].extend([0]*needed)
    
    # --- 10. Plot the Graphs ---
    x = np.arange(max_windows)
    bar_width = 0.15  # adjust as needed
    
    # ---------- Graph 1: Velocity ----------
    plt.figure(figsize=(10, 6))
    for i, repo in enumerate(repos):
        v_list = velocity_data[repo]
        x_positions = x + i*bar_width
        
        label_str = (f"{repo} "
                     f"(M_max_4={M_max_4[repo]}, "
                     f"I_max_4={I_max_4[repo]})")
        
        plt.bar(x_positions, v_list, bar_width, label=label_str)
    
    plt.title("Velocity (Max Alignment from First 4 Windows)")
    plt.xlabel("Window Index")
    plt.ylabel("Velocity Value (scaled)")
    
    # Show velocity formula on the plot (optional)
    plt.text(
        0.5, 0.90,
        "Velocity = 0.4 × M_scaled + 0.6 × I_scaled",
        transform=plt.gca().transAxes,
        fontsize=11,
        ha='center'
    )
    
    plt.xticks(x + bar_width*(len(repos)/2), [f"W{i+1}" for i in range(max_windows)])
    plt.legend()
    plt.tight_layout()
    plt.savefig("velocity.png")
    plt.show()
    
    # ---------- Graph 2: Scaled M ----------
    plt.figure(figsize=(10, 6))
    for i, repo in enumerate(repos):
        m_list = scaled_M[repo]
        x_positions = x + i*bar_width
        label_str = f"{repo} (scaleFactorM={scaleFactor_M[repo]:.3f})"
        
        plt.bar(x_positions, m_list, bar_width, label=label_str)
    
    plt.title("Scaled M (PR merges)")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled # of PR Merges")
    plt.xticks(x + bar_width*(len(repos)/2), [f"W{i+1}" for i in range(max_windows)])
    plt.legend()
    plt.tight_layout()
    plt.savefig("scaled_m.png")
    plt.show()
    
    # ---------- Graph 3: Scaled I ----------
    plt.figure(figsize=(10, 6))
    for i, repo in enumerate(repos):
        i_list = scaled_I[repo]
        x_positions = x + i*bar_width
        label_str = f"{repo} (scaleFactorI={scaleFactor_I[repo]:.3f})"
        
        plt.bar(x_positions, i_list, bar_width, label=label_str)
    
    plt.title("Scaled I (Issues Closed)")
    plt.xlabel("Window Index")
    plt.ylabel("Scaled # of Issues Closed")
    plt.xticks(x + bar_width*(len(repos)/2), [f"W{i+1}" for i in range(max_windows)])
    plt.legend()
    plt.tight_layout()
    plt.savefig("scaled_i.png")
    plt.show()
    
    # --- 11. Close DB Connection ---
    cursor.close()
    cnx.close()


if __name__ == "__main__":
    main()
