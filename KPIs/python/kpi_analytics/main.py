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
from config_reader import load_config
from baseline import find_oldest_date_for_repo
from splitted_metrics import gather_data_for_window
from aggregator import compute_velocity, compute_uig, compute_mac, compute_sei
from scale_factors import ratio_vs_group_average

def main():
    # Load config
    conf= load_config("config.ini")
    general_conf= conf["general"]
    aggregator_conf= conf["aggregator"]

    ENABLE_PNG_CHARTS= general_conf.get("ENABLE_PNG_CHARTS", True)

    # aggregator weights possibly from aggregator_conf
    # or environment overrides:
    # e.g. aggregator_conf["velocity_merges"] => float

    NUM_FISCAL_QUARTERS= int(os.environ.get('NUM_FISCAL_QUARTERS','8'))
    GLOBAL_OFFSET= int(os.environ.get('GLOBAL_OFFSET','0'))
    scaling_repo= os.environ.get("SCALING_REPO","ni/labview-icon-editor")

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

    # 1) find oldest date + global offset
    oldest_dates={}
    for r in all_repos:
        od= find_oldest_date_for_repo(r)
        if not od:
            od= datetime(2100,1,1)
        # apply offset
        od= od+ timedelta(days=GLOBAL_OFFSET)
        oldest_dates[r]= od

    # BFS_data => BFS_data[repo][quarter_idx]
    BFS_data= {}
    for r in all_repos:
        BFS_data[r]= {}
        startdt= oldest_dates[r]
        for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
            q_start= startdt
            q_end= q_start+ timedelta(days=90)
            BFS_data[r][q_idx]= {
              'start': q_start,
              'end': q_end,
              'partial': False,
              'raw':{},
              'agg':{},
              'ratio':{}
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
    def aggregator_compute(r, splitted, config):
        merges= splitted["mergesRaw"]
        cIss=  splitted["closedIssRaw"]
        cPR=   splitted["closedPRRaw"]
        forks= splitted["forksRaw"]
        stars= splitted["starsRaw"]
        nIss=  splitted["newIssRaw"]
        cIssComm= splitted["commentsIssRaw"]
        cPRComm=  splitted["commentsPRRaw"]
        rIss= splitted["reactIssRaw"]
        rPR=  splitted["reactPRRaw"]
        pull= splitted["pullRaw"]

        velocityVal= compute_velocity(merges, cIss, cPR, config)
        uigVal= compute_uig(forks, stars, config)
        macVal= compute_mac(nIss, cIssComm, cPRComm, rIss, rPR, pull, config)
        seiVal= compute_sei(velocityVal, uigVal, macVal, config)

        return {
          'mergesScaled': merges,
          'closedIssScaled': cIss,
          'closedPRScaled': cPR,
          'forksScaled': forks,
          'starsScaled': stars,
          'newIssScaled': nIss,
          'commentsIssScaled': cIssComm,
          'commentsPRScaled': cPRComm,
          'reactIssScaled': rIss,
          'reactPRScaled': rPR,
          'pullScaled': pull,
          'velocity': velocityVal,
          'uig': uigVal,
          'mac': macVal,
          'sei': seiVal
        }

    for r in all_repos:
        for q_idx in BFS_data[r]:
            splitted= BFS_data[r][q_idx]['raw']
            BFS_data[r][q_idx]['agg']= aggregator_compute(r, splitted, aggregator_conf)

    splitted_keys= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_keys= ["velocity","uig","mac","sei"]

    # compute group average per quarter per var => ratio
    def compute_avg_var(q_idx, var, BFS_data, skip_repo=None):
        s= 0.0
        c= 0
        for rp in BFS_data:
            if rp== skip_repo:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_keys:
                val= BFS_data[rp][q_idx]['raw'][var]
            else:
                val= BFS_data[rp][q_idx]['agg'][var]
            s+= val
            c+=1
        return (s/c) if c>0 else 0.0

    for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
        for var in splitted_keys+ aggregator_keys:
            group_avg= compute_avg_var(q_idx, var, BFS_data, skip_repo=None)
            # ratio => BFS_data[r][q_idx]['ratio'][var] = BFS_val / group_avg
            for r in BFS_data:
                if q_idx not in BFS_data[r]:
                    continue
                if var in splitted_keys:
                    myVal= BFS_data[r][q_idx]['raw'][var]
                else:
                    myVal= BFS_data[r][q_idx]['agg'][var]
                BFS_data[r][q_idx]['ratio'][var] = myVal/group_avg if group_avg>0 else 0.0

    def monospaced_table(rows):
        if not rows:
            return ""
        col_count= len(rows[0])
        widths= [0]* col_count
        for row in rows:
            for i,cel in enumerate(row):
                clen= len(str(cel))
                if clen> widths[i]:
                    widths[i]= clen
        lines= []
        for idx,row in enumerate(rows):
            parts= []
            for i,cel in enumerate(row):
                parts.append(str(cel).ljust(widths[i]))
            line= " | ".join(parts)
            lines.append(line)
            if idx==0:
                # insert dash row
                dash_parts= []
                for w in widths:
                    dash_parts.append("-"* w)
                dash_line= " | ".join(dash_parts)
                lines.append(dash_line)
        return "\n".join(lines)

    # BFS console prints
    for r in all_repos:
        print(f"=== BFS for Repo: {r} ===")
        print(f"Existing Quarter Data for {r} | (mergesFactor=1.0000, closedIssFactor=1.0000, closedPRFactor=1.0000, forksFactor=1.0000, starsFactor=1.0000, newIssFactor=1.0000, commentsIssFactor=1.0000, commentsPRFactor=1.0000, reactIssFactor=1.0000, reactPRFactor=1.0000, pullFactor=1.0000)")

        # build columns
        header= [
          "Q-Range",
          "mRaw","mRat",
          "cIss","cIssR",
          "cPR","cPRR",
          "fork","fRat",
          "star","sRat",
          "nIss","nIRt",
          "comI","cIRt",
          "comP","cPRt",
          "reaI","rIRt",
          "reaP","rPRt",
          "pull","pRat",
          "vel","vRat",
          "uig","uRat",
          "mac","mRat",
          "sei","sRat"
        ]
        rows= [header]
        sorted_q= sorted(BFS_data[r].keys())
        for q_idx in sorted_q:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            label= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            rawd= BFS_data[r][q_idx]['raw']
            aggd= BFS_data[r][q_idx]['agg']
            ratd= BFS_data[r][q_idx]['ratio']

            merges= rawd["mergesRaw"]
            mergesRat= ratd["mergesRaw"]
            cIss= rawd["closedIssRaw"]
            cIssRat= ratd["closedIssRaw"]
            cPR= rawd["closedPRRaw"]
            cPRRat= ratd["closedPRRaw"]
            frk= rawd["forksRaw"]
            frkRat= ratd["forksRaw"]
            stx= rawd["starsRaw"]
            stxRat= ratd["starsRaw"]
            ni= rawd["newIssRaw"]
            niRat= ratd["newIssRaw"]
            ci= rawd["commentsIssRaw"]
            ciRat= ratd["commentsIssRaw"]
            cp= rawd["commentsPRRaw"]
            cpRat= ratd["commentsPRRaw"]
            ri= rawd["reactIssRaw"]
            riRat= ratd["reactIssRaw"]
            rp= rawd["reactPRRaw"]
            rpRat= ratd["reactPRRaw"]
            pl= rawd["pullRaw"]
            plRat= ratd["pullRaw"]

            vel= aggd["velocity"]
            velRat= ratd["velocity"]
            u= aggd["uig"]
            uRat= ratd["uig"]
            mac= aggd["mac"]
            macRat= ratd["mac"]
            sei= aggd["sei"]
            seiRat= ratd["sei"]

            row= [
              label,
              str(merges), f"{mergesRat:.3f}",
              str(cIss),   f"{cIssRat:.3f}",
              str(cPR),    f"{cPRRat:.3f}",
              str(frk),    f"{frkRat:.3f}",
              str(stx),    f"{stxRat:.3f}",
              str(ni),     f"{niRat:.3f}",
              str(ci),     f"{ciRat:.3f}",
              str(cp),     f"{cpRat:.3f}",
              str(ri),     f"{riRat:.3f}",
              str(rp),     f"{rpRat:.3f}",
              str(pl),     f"{plRat:.3f}",
              f"{vel:.3f}", f"{velRat:.3f}",
              f"{u:.3f}",   f"{uRat:.3f}",
              f"{mac:.3f}", f"{macRat:.3f}",
              f"{sei:.3f}", f"{seiRat:.3f}"
            ]
            rows.append(row)
        print(monospaced_table(rows))
        print()

        # aggregator expansions
        print(f"--- Additional Calculation Details for {r} (Velocity, UIG, MAC, SEI) ---\n")

        # velocity
        h2= ["Q-Range","mergesScaled","closedIssScaled","closedPRScaled","Velocity"]
        r2= [h2]
        for q_idx in sorted_q:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lbl= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            row= [
              lbl,
              f"{a['mergesScaled']:.1f}",
              f"{a['closedIssScaled']:.1f}",
              f"{a['closedPRScaled']:.1f}",
              f"{a['velocity']:.3f}"
            ]
            r2.append(row)
        print(monospaced_table(r2))
        print()

        # uig
        h3= ["Q-Range","forksScaled","starsScaled","UIG"]
        r3= [h3]
        for q_idx in sorted_q:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lbl= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            row= [
              lbl,
              f"{a['forksScaled']:.1f}",
              f"{a['starsScaled']:.1f}",
              f"{a['uig']:.3f}"
            ]
            r3.append(row)
        print(monospaced_table(r3))
        print()

        # mac
        h4= ["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC"]
        r4= [h4]
        for q_idx in sorted_q:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lbl= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            sumAll= (a["newIssScaled"]+ a["commentsIssScaled"]+ a["commentsPRScaled"]+ a["reactIssScaled"]+ a["reactPRScaled"])
            row= [
              lbl,
              f"{sumAll:.1f}",
              f"{a['pullScaled']:.1f}",
              f"{a['mac']:.3f}"
            ]
            r4.append(row)
        print(monospaced_table(r4))
        print()

        # sei
        h5= ["Q-Range","Velocity","UIG","MAC","SEI"]
        r5= [h5]
        for q_idx in sorted_q:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lbl= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            a= BFS_data[r][q_idx]['agg']
            row= [
              lbl,
              f"{a['velocity']:.3f}",
              f"{a['uig']:.3f}",
              f"{a['mac']:.3f}",
              f"{a['sei']:.3f}"
            ]
            r5.append(row)
        print(monospaced_table(r5))
        print()

    # produce side-by-side scaled charts only if ENABLE_PNG_CHARTS=True
    splitted_vars= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_vars= ["velocity","uig","mac","sei"]

    def compute_avg_for_var(q_idx, var, BFS_data, skip_repo=None):
        sumv=0.0
        cnt=0
        for rp in BFS_data:
            if rp== skip_repo:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_vars:
                val= BFS_data[rp][q_idx]['raw'][var]
            else:
                val= BFS_data[rp][q_idx]['agg'][var]
            sumv+= val
            cnt+=1
        return (sumv/cnt) if cnt>0 else 0.0

    def produce_side_by_side_chart(variableName, BFS_data, all_repos, scaling_repo):
        # We'll incorporate the scaling_repo name in the chart title
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

            if variableName in splitted_vars:
                s_val= BFS_data[scaling_repo][q_idx]['raw'][variableName]
            else:
                s_val= BFS_data[scaling_repo][q_idx]['agg'][variableName]

            g_val= compute_avg_for_var(q_idx, variableName, BFS_data, skip_repo=scaling_repo)
            scale_vals.append(s_val)
            group_vals.append(g_val)
            x_vals.append(i)

        bar_width= 0.4
        x_arr= np.arange(len(x_vals))

        plt.figure(figsize=(10,6))
        plt.bar(x_arr- bar_width/2, scale_vals, bar_width, label=scaling_repo, color='tab:blue')
        plt.bar(x_arr+ bar_width/2, group_vals, bar_width, label='NonScalingAvg', color='tab:orange')

        plt.title(f"{variableName} ({scaling_repo} vs. group avg)")
        plt.xlabel("BFS Quarter Index (Scaled Timeline)")
        plt.ylabel(variableName)
        plt.xticks(x_arr, q_labels, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()

        out_name= f"{variableName}_scaled.png"
        plt.savefig(out_name)
        plt.close()
        print(f"[INFO] Created {out_name}")

    if general_conf["ENABLE_PNG_CHARTS"]:
        print("\n=== Producing side-by-side scaled charts... ===")
        for var in splitted_vars+ aggregator_keys:
            produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)
    else:
        print("\n[INFO] PNG chart creation disabled by config.\n")

    print("=== BFS aggregator done. ===")

if __name__=="__main__":
    main()
