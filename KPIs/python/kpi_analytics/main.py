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

    NUM_FISCAL_QUARTERS= int(os.environ.get('NUM_FISCAL_QUARTERS','8'))
    GLOBAL_OFFSET= int(os.environ.get('GLOBAL_OFFSET','0'))
    scaling_repo= os.environ.get("SCALING_REPO","ni/labview-icon-editor")

    # define repos
    all_repos= [
        "tensorflow/tensorflow",
        "facebook/react", 
        "ni/grpc-labview",
        "dotnet/core",
        "facebook/react",
        "EPICS/reconos",
        "OpenFOAM/OpenFOAM-dev",
        "FreeCAD/freecad",
        "fritzing/fritzing-app",
        "qucs/qucs",
        "OpenSCAD/openscad",
        "OpenPLC/OpenPLC-IDE",
        "Eclipse/mraa",
    ]

    # Find oldest dates + global offset
    oldest_dates={}
    for r in all_repos:
        od= find_oldest_date_for_repo(r)
        if not od:
            # no data => future date
            od= datetime(2100,1,1)
        od= od+ timedelta(days=GLOBAL_OFFSET)
        oldest_dates[r]= od

    # BFS_data => BFS_data[repo][q_idx] => { start, end, partial, raw{}, agg{}, ratio{} }
    BFS_data={}
    for r in all_repos:
        BFS_data[r]= {}
        startdt= oldest_dates[r]
        for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
            q_start= startdt
            q_end= q_start+ timedelta(days=90) # approximate 3 months
            BFS_data[r][q_idx]= {
              'start': q_start,
              'end': q_end,
              'partial': False,
              'raw': {},
              'agg': {},
              'ratio': {}
            }
            startdt= q_end

    # Gather splitted data
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
            merges= rawd["mergesRaw"]
            cIss=  rawd["closedIssRaw"]
            cPR=   rawd["closedPRRaw"]
            forks= rawd["forksRaw"]
            stars= rawd["starsRaw"]
            newIss= rawd["newIssRaw"]
            comIss= rawd["commentsIssRaw"]
            comPR=  rawd["commentsPRRaw"]
            reactIss= rawd["reactIssRaw"]
            reactPR= rawd["reactPRRaw"]
            pull=   rawd["pullRaw"]

            velocityVal= compute_velocity(merges, cIss, cPR, AGG_CONFIG)
            uigVal= compute_uig(forks, stars, AGG_CONFIG)
            # sumAll => newIss + comIss + comPR + reactIss + reactPR
            macVal= compute_mac(newIss, comIss, comPR, reactIss, reactPR, pull, AGG_CONFIG)
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
              'reactIssScaled': reactIss,
              'reactPRScaled': reactPR,
              'pullScaled': pull,
              'velocity': velocityVal,
              'uig': uigVal,
              'mac': macVal,
              'sei': seiVal
            }

    splitted_keys= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_keys= ["velocity","uig","mac","sei"]

    def compute_avg_var(q_idx, var, BFS_data, skip_repo=None):
        sum_v= 0.0
        count= 0
        for rp in BFS_data:
            if rp== skip_repo:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_keys:
                val= BFS_data[rp][q_idx]['raw'][var]
            else:
                val= BFS_data[rp][q_idx]['agg'][var]
            sum_v+= val
            count+=1
        if count>0:
            return sum_v/count
        else:
            return 0.0

    # ratio vs group
    for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
        for var in splitted_keys+ aggregator_keys:
            groupAvg= compute_avg_var(q_idx, var, BFS_data, skip_repo=None)
            for r in BFS_data:
                if q_idx not in BFS_data[r]:
                    continue
                if var in splitted_keys:
                    myVal= BFS_data[r][q_idx]['raw'][var]
                else:
                    myVal= BFS_data[r][q_idx]['agg'][var]
                if groupAvg>0:
                    BFS_data[r][q_idx]['ratio'][var]= myVal/groupAvg
                else:
                    BFS_data[r][q_idx]['ratio'][var]= 0.0

    # a helper to align columns + put a row of dashes after header
    def monospaced_table(rows):
        if not rows:
            return ""
        col_count= len(rows[0])
        col_widths= [0]* col_count
        for row in rows:
            for i,cell in enumerate(row):
                clen= len(str(cell))
                if clen> col_widths[i]:
                    col_widths[i]= clen
        lines= []
        for idx,row in enumerate(rows):
            line_parts= []
            for i,cell in enumerate(row):
                c_str= str(cell)
                # left-justify
                line_parts.append(c_str.ljust(col_widths[i]))
            line_str= " | ".join(line_parts)
            lines.append(line_str)
            if idx==0:
                # insert a row of dashes after header
                dash_parts= []
                for width in col_widths:
                    dash_parts.append("-"* width)
                dash_line= " | ".join(dash_parts)
                lines.append(dash_line)
        return "\n".join(lines)

    # BFS console prints
    for r in all_repos:
        print(f"=== BFS for Repo: {r} ===")
        # Hardcode scale factor? or produce logic. We'll just show mergesFactor=1.0 etc. for demonstration
        print(f"Existing Quarter Data for {r} | (mergesFactor=1.0000, closedIssFactor=1.0000, closedPRFactor=1.0000, forksFactor=1.0000, starsFactor=1.0000, newIssuesFactor=1.0000, commentsIssFactor=1.0000, commentsPRFactor=1.0000, reactIssFactor=1.0000, reactPRFactor=1.0000, pullFactor=1.0000)")

        header= [
          "Q-Range",
          "mRaw","mRat",
          "cIss","cIRt",
          "cPR","cPRt",
          "fork","fRat",
          "star","sRat",
          "nIss","nIRt",
          "comI","comIR",
          "comP","comPR",
          "reaI","reaIR",
          "reaP","reaPR",
          "pull","pRat",
          "vel","vRat",
          "uig","uRat",
          "mac","mRat",
          "sei","sRat"
        ]
        rows= [header]

        # BFS data
        all_quarters= sorted(BFS_data[r].keys())
        for q_idx in all_quarters:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            label= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"

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

            cIssCommR= rawd["commentsIssRaw"]
            cIssCommRat= ratio["commentsIssRaw"]

            cPRCommR= rawd["commentsPRRaw"]
            cPRCommRat= ratio["commentsPRRaw"]

            rIss= rawd["reactIssRaw"]
            rIssRat= ratio["reactIssRaw"]

            rPR= rawd["reactPRRaw"]
            rPRRat= ratio["reactPRRaw"]

            pullR= rawd["pullRaw"]
            pullRat= ratio["pullRaw"]

            vel= aggd["velocity"]
            velRat= ratio["velocity"]

            ui= aggd["uig"]
            uiRat= ratio["uig"]

            mac= aggd["mac"]
            macRat= ratio["mac"]

            sei= aggd["sei"]
            seiRat= ratio["sei"]

            row= [
              label,
              str(mergesR), f"{mergesRat:.3f}",
              str(cIssR),   f"{cIssRat:.3f}",
              str(cPRR),    f"{cPRRat:.3f}",
              str(forksR),  f"{forksRat:.3f}",
              str(starsR),  f"{starsRat:.3f}",
              str(nIssR),   f"{nIssRat:.3f}",
              str(cIssCommR), f"{cIssCommRat:.3f}",
              str(cPRCommR), f"{cPRCommRat:.3f}",
              str(rIss), f"{rIssRat:.3f}",
              str(rPR),  f"{rPRRat:.3f}",
              str(pullR), f"{pullRat:.3f}",
              f"{vel:.3f}", f"{velRat:.3f}",
              f"{ui:.3f}",  f"{uiRat:.3f}",
              f"{mac:.3f}", f"{macRat:.3f}",
              f"{sei:.3f}", f"{seiRat:.3f}"
            ]
            rows.append(row)

        table_str= monospaced_table(rows)
        print(table_str)
        print()

        # aggregator expansions
        print(f"--- Additional Calculation Details for {r} (Velocity, UIG, MAC, SEI) ---\n")

        # velocity
        h2= ["Q-Range","mergesScaled","closedIssScaled","closedPRScaled","Velocity=..."]
        r2= [h2]
        for q_idx in all_quarters:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            label= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            row= [
              label,
              f"{a['mergesScaled']:.1f}",
              f"{a['closedIssScaled']:.1f}",
              f"{a['closedPRScaled']:.1f}",
              f"{a['velocity']:.3f}"
            ]
            r2.append(row)
        print(monospaced_table(r2))
        print()

        # uig
        h3= ["Q-Range","forksScaled","starsScaled","UIG=0.4F+0.6S"]
        r3= [h3]
        for q_idx in all_quarters:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            label= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            row= [
              label,
              f"{a['forksScaled']:.1f}",
              f"{a['starsScaled']:.1f}",
              f"{a['uig']:.3f}"
            ]
            r3.append(row)
        print(monospaced_table(r3))
        print()

        # mac
        h4= ["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]
        r4= [h4]
        for q_idx in all_quarters:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            label= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            sumAll= (a["newIssScaled"]+ a["commentsIssScaled"]+ a["commentsPRScaled"]+ a["reactIssScaled"]+ a["reactPRScaled"])
            row= [
              label,
              f"{sumAll:.1f}",
              f"{a['pullScaled']:.1f}",
              f"{a['mac']:.3f}"
            ]
            r4.append(row)
        print(monospaced_table(r4))
        print()

        # sei
        h5= ["Q-Range","Velocity","UIG","MAC","SEI= (wv*vel + wu*uig + wm*mac)"]
        r5= [h5]
        for q_idx in all_quarters:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            label= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            row= [
              label,
              f"{a['velocity']:.3f}",
              f"{a['uig']:.3f}",
              f"{a['mac']:.3f}",
              f"{a['sei']:.3f}"
            ]
            r5.append(row)
        print(monospaced_table(r5))
        print()

    # produce side-by-side scaled charts for splitted and aggregator
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
        # We'll create 2 bars per BFS quarter => scaling's value and group average
        if scaling_repo not in BFS_data:
            return
        sorted_quarters= sorted(BFS_data[scaling_repo].keys())
        x_vals= []
        scale_vals=[]
        group_vals=[]
        q_labels=[]
        for i,q_idx in enumerate(sorted_quarters):
            st= BFS_data[scaling_repo][q_idx]['start'].strftime("%Y-%m-%d")
            ed= BFS_data[scaling_repo][q_idx]['end'].strftime("%Y-%m-%d")
            qlbl= f"Q{q_idx}\n({st}..{ed})"
            q_labels.append(qlbl)
            # scaling
            if variableName in splitted_vars:
                s_val= BFS_data[scaling_repo][q_idx]['raw'][variableName]
            else:
                s_val= BFS_data[scaling_repo][q_idx]['agg'][variableName]
            # group average
            g_val= compute_avg_for_var(q_idx, variableName, BFS_data, skip_repo=scaling_repo)
            scale_vals.append(s_val)
            group_vals.append(g_val)
            x_vals.append(i)

        bar_width= 0.4
        x_arr= np.arange(len(x_vals))

        plt.figure(figsize=(10,6))
        plt.bar(x_arr- bar_width/2, scale_vals, bar_width, label=scaling_repo, color='tab:blue')
        plt.bar(x_arr+ bar_width/2, group_vals, bar_width, label='NonScalingAvg', color='tab:orange')

        plt.title(f"{variableName} (Scaling vs. Group Avg)")
        plt.xlabel("BFS Quarter Index (Scaled Timeline)")
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

    print("=== BFS aggregator + side-by-side scaled charts done. ===")

if __name__=="__main__":
    main()
