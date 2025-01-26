############################################
# main.py
############################################
import os
import math
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
from baseline import find_oldest_date_for_repo
from splitted_metrics import gather_data_for_window
from aggregator import compute_velocity, compute_uig, compute_mac, compute_sei
from scale_factors import ratio_vs_group_average

def main():
    # 1) aggregator weighting from environment or fallback
    AGG_CONFIG= {
      'velocity_merges': float(os.environ.get('V_MERGES','0.4')),
      'velocity_closedIss': float(os.environ.get('V_CISS','0.2')),
      'velocity_closedPR': float(os.environ.get('V_CPR','0.4')),

      'uig_forks': float(os.environ.get('U_FORKS','0.4')),
      'uig_stars': float(os.environ.get('U_STARS','0.6')),

      'mac_mainWeight': float(os.environ.get('MAC_MAIN','0.8')),
      'mac_subWeight': float(os.environ.get('MAC_SUB','0.2')),

      'sei_velocity': float(os.environ.get('SEI_V','0.3')),
      'sei_uig': float(os.environ.get('SEI_U','0.2')),
      'sei_mac': float(os.environ.get('SEI_M','0.5'))
    }

    # 2) read environment for BFS config
    NUM_FISCAL_QUARTERS= int(os.environ.get('NUM_FISCAL_QUARTERS','8'))
    GLOBAL_OFFSET= int(os.environ.get('GLOBAL_OFFSET','0'))

    # define your repos
    all_repos= [
      "ni/labview-icon-editor",
      "facebook/react",
      "tensorflow/tensorflow",
      "dotnet/core"
    ]
    scaling_repo= os.environ.get("SCALING_REPO","ni/labview-icon-editor")

    # 3) find earliest date (plus offset)
    oldest_dates={}
    for r in all_repos:
        od= find_oldest_date_for_repo(r)
        if not od:
            # if there's no data at all, artificially skip
            od= datetime(2099,1,1)
        od= od + timedelta(days=GLOBAL_OFFSET)
        oldest_dates[r]= od

    # BFS_data[repo][qIndex] -> { 'start','end','partial', 'raw', 'agg', 'ratio' }
    BFS_data= {}
    # build 3-month windows up to NUM_FISCAL_QUARTERS for each repo
    for r in all_repos:
        BFS_data[r]= {}
        startdt= oldest_dates[r]
        for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
            q_start= startdt
            q_end= q_start+ timedelta(days=90)  # approximate 3 mo
            BFS_data[r][q_idx]= {
              'start': q_start,
              'end': q_end,
              'partial': False,
              'raw': {},
              'agg': {},
              'ratio': {}
            }
            startdt= q_end

    # 4) gather splitted raw data
    for r in all_repos:
        for q_idx in BFS_data[r]:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            splitted= gather_data_for_window(r, st, ed)
            BFS_data[r][q_idx]['raw']= splitted

    # 5) aggregator expansions => mergesScaled= mergesRaw (factor=1), etc.
    for r in all_repos:
        for q_idx in BFS_data[r]:
            rawd= BFS_data[r][q_idx]['raw']
            # splitted raw
            mergesS= rawd["mergesRaw"]
            cIssS= rawd["closedIssRaw"]
            cPRS= rawd["closedPRRaw"]
            forksS= rawd["forksRaw"]
            starsS= rawd["starsRaw"]
            newIssS= rawd["newIssRaw"]
            commIssS= rawd["commentsIssRaw"]
            commPRS= rawd["commentsPRRaw"]
            reacIssS= rawd["reactIssRaw"]
            reacPRS= rawd["reactPRRaw"]
            pullS= rawd["pullRaw"]

            velocityVal= compute_velocity(mergesS, cIssS, cPRS, AGG_CONFIG)
            uigVal= compute_uig(forksS, starsS, AGG_CONFIG)
            macVal= compute_mac(newIssS, commIssS, commPRS, reacIssS, reacPRS, pullS, AGG_CONFIG)
            seiVal= compute_sei(velocityVal, uigVal, macVal, AGG_CONFIG)

            BFS_data[r][q_idx]['agg']= {
              'mergesScaled': mergesS,
              'closedIssScaled': cIssS,
              'closedPRScaled': cPRS,
              'forksScaled': forksS,
              'starsScaled': starsS,
              'newIssScaled': newIssS,
              'commentsIssScaled': commIssS,
              'commentsPRScaled': commPRS,
              'reactIssScaled': reacIssS,
              'reactPRScaled': reacPRS,
              'pullScaled': pullS,
              'velocity': velocityVal,
              'uig': uigVal,
              'mac': macVal,
              'sei': seiVal
            }

    # 6) ratio vs group average
    splitted_keys= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw","newIssRaw",
      "commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_keys= ["velocity","uig","mac","sei"]

    def compute_avg_of_var(q_idx, var, BFS_data, skip_repo=None):
        sum_v=0
        count=0
        for orp in BFS_data:
            if orp== skip_repo:
                continue
            if q_idx in BFS_data[orp]:
                if var in splitted_keys:
                    val= BFS_data[orp][q_idx]['raw'].get(var,0)
                else:
                    val= BFS_data[orp][q_idx]['agg'].get(var,0)
                sum_v+= val
                count+=1
        if count>0:
            return sum_v/count
        else:
            return 0.0

    for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
        for var in splitted_keys+ aggregator_keys:
            groupAvg= compute_avg_of_var(q_idx, var, BFS_data, skip_repo=None)
            # store ratio= BFS_value / groupAvg
            for r in BFS_data:
                if q_idx not in BFS_data[r]:
                    continue
                if var in splitted_keys:
                    rawVal= BFS_data[r][q_idx]['raw'][var]
                else:
                    rawVal= BFS_data[r][q_idx]['agg'][var]
                ratioVal= 0.0
                if groupAvg>0:
                    ratioVal= rawVal / groupAvg
                BFS_data[r][q_idx]['ratio'][var]= ratioVal

    # 7) BFS aggregator console print
    for r in all_repos:
        print(f"=== BFS for Repo: {r} ===")
        print(f"Existing Quarter Data for {r} | (mergesFactor=1.0000, closedIssFactor=1.0000, closedPRFactor=1.0000, forksFactor=1.0000, starsFactor=1.0000, newIssuesFactor=1.0000, commentsIssFactor=1.0000, commentsPRFactor=1.0000, reactIssFactor=1.0000, reactPRFactor=1.0000, pullFactor=1.0000)")

        header_cols= [
          "Q-Range",
          "mergesRaw","mRatio",
          "closedIssRaw","cIRatio",
          "closedPRRaw","cPRRatio",
          "forksRaw","fRatio",
          "starsRaw","sRatio",
          "newIssRaw","nIRatio",
          "commentsIssRaw","comIssR",
          "commentsPRRaw","comPRR",
          "reactIssRaw","rIssR",
          "reactPRRaw","rPRR",
          "pullRaw","pullRatio",
          "velocity","velRatio",
          "uig","uigRatio",
          "mac","macRatio",
          "sei","seiRatio"
        ]
        hline= " | ".join(header_cols)
        print(hline)
        print("-"*len(hline))

        for q_idx in sorted(BFS_data[r].keys()):
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            # you can label partial if e.g. ed> datetime.now() or if BFS_data[r][q_idx]['partial']
            lab= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"

            rawd= BFS_data[r][q_idx]['raw']
            aggd= BFS_data[r][q_idx]['agg']
            ratio= BFS_data[r][q_idx]['ratio']

            mergesR= rawd["mergesRaw"]
            mergesRat= ratio["mergesRaw"]

            cIssR= rawd["closedIssRaw"]
            cIssRat= ratio["closedIssRaw"]

            cPRR= rawd["closedPRRaw"]
            cPRRat= ratio["closedPRRaw"]

            forksR= rawd["forksRaw"]
            forksRat= ratio["forksRaw"]

            starsR= rawd["starsRaw"]
            starsRat= ratio["starsRaw"]

            nIssR= rawd["newIssRaw"]
            nIssRat= ratio["newIssRaw"]

            comIssR= rawd["commentsIssRaw"]
            comIssRat= ratio["commentsIssRaw"]

            comPRR= rawd["commentsPRRaw"]
            comPRRat= ratio["commentsPRRaw"]

            reactIssR= rawd["reactIssRaw"]
            rIssRat= ratio["reactIssRaw"]

            reactPRR= rawd["reactPRRaw"]
            rPRRat= ratio["reactPRRaw"]

            pullR= rawd["pullRaw"]
            pullRat= ratio["pullRaw"]

            vel= aggd["velocity"]
            velRat= ratio["velocity"]

            ug= aggd["uig"]
            ugRat= ratio["uig"]

            mc= aggd["mac"]
            mcRat= ratio["mac"]

            se= aggd["sei"]
            seRat= ratio["sei"]

            row= f"{lab} | {mergesR} | {mergesRat:.3f} | {cIssR} | {cIssRat:.3f} | {cPRR} | {cPRRat:.3f} | {forksR} | {forksRat:.3f} | {starsR} | {starsRat:.3f} | {nIssR} | {nIssRat:.3f} | {comIssR} | {comIssRat:.3f} | {comPRR} | {comPRRat:.3f} | {reactIssR} | {rIssRat:.3f} | {reactPRR} | {rPRRat:.3f} | {vel:.3f} | {velRat:.3f} | {ug:.3f} | {ugRat:.3f} | {mc:.3f} | {mcRat:.3f} | {se:.3f} | {seRat:.3f}"
            print(row)
        print()

        # Additional aggregator expansions for velocity, uig, mac, sei
        print(f"--- Additional Calculation Details for {r} (Velocity, UIG, MAC, SEI) ---")
        # you can expand the same style if you want. For brevity, let's just finalize.
        print()

    # 8) produce side-by-side scaled charts for splitted variables, aggregator expansions, etc.
    splitted_vars= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw","newIssRaw",
      "commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    # aggregator expansions
    aggregator_vars= ["velocity","uig","mac","sei"]

    # Example function to produce a chart: scaling repo vs. group average
    def produce_side_by_side_chart(variableName, BFS_data, all_repos, scaling_repo):
        # for each BFS quarter, get the scaling repo's raw => bar, group avg => second bar
        x_vals=[]
        scale_vals=[]
        group_vals=[]
        q_labels=[]
        # We assume BFS_data[scaling_repo] has sorted Q indexes
        for q_idx in sorted(BFS_data[scaling_repo].keys()):
            x_vals.append(q_idx)
            if variableName in splitted_vars:
                s_val= BFS_data[scaling_repo][q_idx]['raw'][variableName]
            else:
                s_val= BFS_data[scaling_repo][q_idx]['agg'][variableName]
            group_avg= 0.0
            count=0
            sum_v=0.0
            for r2 in all_repos:
                if r2== scaling_repo:
                    continue
                if q_idx in BFS_data[r2]:
                    if variableName in splitted_vars:
                        val2= BFS_data[r2][q_idx]['raw'][variableName]
                    else:
                        val2= BFS_data[r2][q_idx]['agg'][variableName]
                    sum_v+= val2
                    count+=1
            if count>0:
                group_avg= sum_v/count

            scale_vals.append(s_val)
            group_vals.append(group_avg)
            qstart= BFS_data[scaling_repo][q_idx]['start'].strftime("%Y-%m-%d")
            qend= BFS_data[scaling_repo][q_idx]['end'].strftime("%Y-%m-%d")
            q_labels.append(f"Q{q_idx}\n({qstart}..{qend})")

        bar_width=0.35
        x_arr= np.arange(len(x_vals))

        plt.figure(figsize=(10,6))
        plt.bar(x_arr - bar_width/2, scale_vals, bar_width, label=f"{scaling_repo}")
        plt.bar(x_arr + bar_width/2, group_vals, bar_width, label="NonScalingAvg")

        plt.title(f"{variableName} (Scaled Repo vs. Group Average)")
        plt.xlabel("Quarter Index")
        plt.ylabel(variableName)
        plt.xticks(x_arr, q_labels, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        out_filename= f"{variableName}_scaled.png"
        plt.savefig(out_filename)
        plt.close()
        print(f"[INFO] Created {out_filename}")

    # produce charts
    for var in splitted_vars:
        produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)
    for var in aggregator_vars:
        produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)

    print("=== Done BFS aggregator + side-by-side scaled charts. ===")


if __name__=="__main__":
    main()
