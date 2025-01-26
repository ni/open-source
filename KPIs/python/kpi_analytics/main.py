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

    # BFS config
    NUM_FISCAL_QUARTERS= int(os.environ.get('NUM_FISCAL_QUARTERS','8'))
    GLOBAL_OFFSET= int(os.environ.get('GLOBAL_OFFSET','0'))
    scaling_repo= os.environ.get("SCALING_REPO","ni/labview-icon-editor")

    # define your repos
    all_repos= [
      "ni/labview-icon-editor",
      "facebook/react",
      "tensorflow/tensorflow",
      "dotnet/core"
    ]
    # find earliest date + offset
    oldest_dates={}
    for r in all_repos:
        od= find_oldest_date_for_repo(r)
        if not od:
            # no data
            od= datetime(2099,1,1)
        od= od+ timedelta(days=GLOBAL_OFFSET)
        oldest_dates[r]= od

    # BFS_data => BFS_data[repo][quarterIndex] => { start, end, partial, raw, agg, ratio }
    BFS_data= {}
    for r in all_repos:
        BFS_data[r]= {}
        startdt= oldest_dates[r]
        for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
            q_start= startdt
            q_end= q_start+ timedelta(days=90)  # approximate 3 months
            BFS_data[r][q_idx]= {
              'start': q_start,
              'end': q_end,
              'partial': False,
              'raw': {},
              'agg': {},
              'ratio': {}
            }
            startdt= q_end

    # 2) gather splitted raw data
    for r in all_repos:
        for q_idx in BFS_data[r]:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            splitted= gather_data_for_window(r, st, ed)
            BFS_data[r][q_idx]['raw']= splitted

    # aggregator expansions
    for r in all_repos:
        for q_idx in BFS_data[r]:
            rawd= BFS_data[r][q_idx]['raw']

            # splitted raw
            merges= rawd["mergesRaw"]
            cIss = rawd["closedIssRaw"]
            cPR  = rawd["closedPRRaw"]
            forks= rawd["forksRaw"]
            stars= rawd["starsRaw"]
            newIss= rawd["newIssRaw"]
            comIss= rawd["commentsIssRaw"]
            comPR = rawd["commentsPRRaw"]
            reaIss= rawd["reactIssRaw"]
            reaPR = rawd["reactPRRaw"]
            pull=  rawd["pullRaw"]

            # aggregator
            velocityVal= compute_velocity(merges, cIss, cPR, AGG_CONFIG)
            uigVal= compute_uig(forks, stars, AGG_CONFIG)
            macVal= compute_mac(newIss, comIss, comPR, reaIss, reaPR, pull, AGG_CONFIG)
            seiVal= compute_sei(velocityVal, uigVal, macVal, AGG_CONFIG)

            BFS_data[r][q_idx]['agg']= {
              'mergesScaled': merges,
              'closedIssScaled': cIss,
              'closedPRScaled': cPR,
              'forksScaled': forks,
              'starsScaled': stars,
              'newIssScaled': newIss,
              'commentsIssScaled': comIss,
              'commentsPRScaled': comPR,
              'reactIssScaled': reaIss,
              'reactPRScaled': reaPR,
              'pullScaled': pull,
              'velocity': velocityVal,
              'uig': uigVal,
              'mac': macVal,
              'sei': seiVal
            }

    # ratio vs group average
    splitted_keys= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_keys= ["velocity","uig","mac","sei"]

    def compute_avg_of_var(q_idx, var, BFS_data, skip_repo=None):
        sum_v= 0.0
        count= 0
        for orp in BFS_data:
            if orp== skip_repo:
                continue
            if q_idx not in BFS_data[orp]:
                continue
            if var in splitted_keys:
                val= BFS_data[orp][q_idx]['raw'][var]
            else:
                val= BFS_data[orp][q_idx]['agg'][var]
            sum_v+= val
            count+=1
        if count>0:
            return sum_v/count
        else:
            return 0.0

    for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
        for var in splitted_keys+ aggregator_keys:
            groupAvg= compute_avg_of_var(q_idx, var, BFS_data, skip_repo=None)
            # store ratio => BFS_data[r][q_idx]['ratio'][var]
            for r in BFS_data:
                if q_idx not in BFS_data[r]:
                    continue
                if var in splitted_keys:
                    val= BFS_data[r][q_idx]['raw'][var]
                else:
                    val= BFS_data[r][q_idx]['agg'][var]
                if groupAvg>0:
                    BFS_data[r][q_idx]['ratio'][var]= val/groupAvg
                else:
                    BFS_data[r][q_idx]['ratio'][var]= 0.0

    # a helper to align console columns
    def monospaced_table(rows):
        # rows is a list of lists
        col_count= len(rows[0])
        col_widths= [0]* col_count
        for row in rows:
            for i,cell in enumerate(row):
                clen= len(str(cell))
                if clen> col_widths[i]:
                    col_widths[i]= clen
        aligned= []
        for row in rows:
            line_parts= []
            for i,cell in enumerate(row):
                c_str= str(cell)
                line_parts.append( c_str.ljust(col_widths[i]) )
            aligned.append(" | ".join(line_parts))
        return "\n".join(aligned)

    # BFS console print
    for r in all_repos:
        print(f"=== BFS for Repo: {r} ===")
        print(f"Existing Quarter Data for {r} | (mergesFactor=1.0000, closedIssFactor=1.0000, closedPRFactor=1.0000, forksFactor=1.0000, starsFactor=1.0000, newIssuesFactor=1.0000, commentsIssFactor=1.0000, commentsPRFactor=1.0000, reactIssFactor=1.0000, reactPRFactor=1.0000, pullFactor=1.0000)")

        # build a table
        header= [
          "Q-Range",
          "mergesRaw","mRatio",
          "cIssRaw","cIssRat",
          "cPRRaw","cPRRat",
          "forks","fRatio",
          "stars","sRatio",
          "newIss","nIRat",
          "comIss","ciRat",
          "comPR","cpRat",
          "reaIss","riRat",
          "reaPR","rpRat",
          "pull","pRatio",
          "velocity","vRatio",
          "uig","uRatio",
          "mac","mRatio",
          "sei","sRatio"
        ]
        rows= []
        rows.append(header)

        for q_idx in sorted(BFS_data[r].keys()):
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
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

            row= [
              lab,
              str(mergesR),
              f"{mergesRat:.3f}",
              str(cIssR),
              f"{cIssRat:.3f}",
              str(cPRR),
              f"{cPRRat:.3f}",
              str(forksR),
              f"{forksRat:.3f}",
              str(starsR),
              f"{starsRat:.3f}",
              str(nIssR),
              f"{nIssRat:.3f}",
              str(comIssR),
              f"{comIssRat:.3f}",
              str(comPRR),
              f"{comPRRat:.3f}",
              str(reactIssR),
              f"{rIssRat:.3f}",
              str(reactPRR),
              f"{rPRRat:.3f}",
              str(pullR),
              f"{pullRat:.3f}",
              f"{vel:.3f}",
              f"{velRat:.3f}",
              f"{ug:.3f}",
              f"{ugRat:.3f}",
              f"{mc:.3f}",
              f"{mcRat:.3f}",
              f"{se:.3f}",
              f"{seRat:.3f}"
            ]
            rows.append(row)

        table_str= monospaced_table(rows)
        print(table_str)
        print()

        # Additional aggregator expansions
        print(f"--- Additional Calculation Details for {r} (Velocity, UIG, MAC, SEI) ---")

        # Example expansions for velocity
        # build a sub-table
        sub_header= ["Q-Range","mergesScaled","closedIssScaled","closedPRScaled","Velocity=..."]
        sub_rows= [sub_header]
        for q_idx in sorted(BFS_data[r].keys()):
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lab= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            aggd= BFS_data[r][q_idx]['agg']
            mergesS= aggd["mergesScaled"]
            cIssS= aggd["closedIssScaled"]
            cPRS= aggd["closedPRScaled"]
            vel= aggd["velocity"]
            sub_rows.append([lab, f"{mergesS:.1f}", f"{cIssS:.1f}", f"{cPRS:.1f}", f"{vel:.3f}"])
        sub_table= monospaced_table(sub_rows)
        print(sub_table)
        print()

        # Similarly for uig
        sub_header2= ["Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"]
        sub_rows2= [sub_header2]
        for q_idx in sorted(BFS_data[r].keys()):
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lab= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            aggd= BFS_data[r][q_idx]['agg']
            fS= aggd["forksScaled"]
            sS= aggd["starsScaled"]
            uVal= aggd["uig"]
            sub_rows2.append([lab, f"{fS:.1f}", f"{sS:.1f}", f"{uVal:.3f}"])
        print(monospaced_table(sub_rows2))
        print()

        # mac
        sub_header3= ["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]
        sub_rows3= [sub_header3]
        for q_idx in sorted(BFS_data[r].keys()):
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lab= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            aggd= BFS_data[r][q_idx]['agg']
            issComRea= (aggd["newIssScaled"]+ aggd["commentsIssScaled"]+ aggd["commentsPRScaled"]+ aggd["reactIssScaled"]+ aggd["reactPRScaled"])
            pullS= aggd["pullScaled"]
            macVal= aggd["mac"]
            sub_rows3.append([lab, f"{issComRea:.1f}", f"{pullS:.1f}", f"{macVal:.3f}"])
        print(monospaced_table(sub_rows3))
        print()

        # sei
        sub_header4= ["Q-Range","Velocity","UIG","MAC","SEI= wv*vel + wu*uig + wm*mac"]
        sub_rows4= [sub_header4]
        for q_idx in sorted(BFS_data[r].keys()):
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lab= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            aggd= BFS_data[r][q_idx]['agg']
            vv= aggd["velocity"]
            uu= aggd["uig"]
            mm= aggd["mac"]
            ss= aggd["sei"]
            sub_rows4.append([lab, f"{vv:.3f}", f"{uu:.3f}", f"{mm:.3f}", f"{ss:.3f}"])
        print(monospaced_table(sub_rows4))
        print()

    # produce side-by-side scaled charts
    splitted_vars= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_vars= ["velocity","uig","mac","sei"]

    def compute_avg_for_var(q_idx, var, BFS_data, skip_repo=None):
        sum_v= 0.0
        count= 0
        for rp in BFS_data:
            if rp== skip_repo:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_vars:
                val= BFS_data[rp][q_idx]['raw'][var]
            else:
                val= BFS_data[rp][q_idx]['agg'][var]
            sum_v+= val
            count+=1
        if count>0:
            return sum_v/count
        else:
            return 0.0

    def produce_side_by_side_chart(variableName, BFS_data, all_repos, scaling_repo):
        """
        Chart with x-axis= quarters in scaling_repo,
        2 bars: scaling_repo's value & the group average (excluding scaling repo).
        We'll label x-ticks with QX plus the actual date range.
        """
        x_vals=[]
        scale_vals=[]
        group_vals=[]
        q_labels=[]
        if scaling_repo not in BFS_data:
            return
        sorted_quarters= sorted(BFS_data[scaling_repo].keys())
        for q_idx in sorted_quarters:
            st= BFS_data[scaling_repo][q_idx]['start'].strftime("%Y-%m-%d")
            ed= BFS_data[scaling_repo][q_idx]['end'].strftime("%Y-%m-%d")
            qlbl= f"Q{q_idx}\n({st}..{ed})"
            q_labels.append(qlbl)
            if variableName in splitted_vars:
                s_val= BFS_data[scaling_repo][q_idx]['raw'][variableName]
            else:
                s_val= BFS_data[scaling_repo][q_idx]['agg'][variableName]
            ga= compute_avg_for_var(q_idx, variableName, BFS_data, skip_repo=scaling_repo)
            x_vals.append(q_idx)
            scale_vals.append(s_val)
            group_vals.append(ga)

        bar_width= 0.35
        x_arr= np.arange(len(x_vals))

        plt.figure(figsize=(10,6))
        plt.bar(x_arr - bar_width/2, scale_vals, bar_width, label=scaling_repo)
        plt.bar(x_arr + bar_width/2, group_vals, bar_width, label="NonScalingAvg")
        plt.title(f"{variableName} (Scaling vs. Group Avg)")
        plt.xlabel("Quarter Index (Index-based BFS)")
        plt.ylabel(variableName)
        plt.xticks(x_arr, q_labels, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()

        out_name= f"{variableName}_scaled.png"
        plt.savefig(out_name)
        plt.close()
        print(f"[INFO] Created {out_name}")

    # produce charts
    for var in splitted_vars:
        produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)
    for var in aggregator_vars:
        produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)

    print("=== Done BFS aggregator + side-by-side scaled charts. ===")

if __name__=="__main__":
    main()
