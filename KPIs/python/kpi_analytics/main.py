############################################
# main.py
############################################

import os
import sys
import io
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
    # 1) Capture console in memory so we can write it to debug_log at the end
    old_stdout= sys.stdout
    console_buffer= io.StringIO()
    sys.stdout= console_buffer

    # 2) Load aggregator config
    conf= load_config("config.ini")
    aggregator_conf= conf["aggregator"]

    # environment
    NUM_FISCAL_QUARTERS= int(os.environ.get("NUM_FISCAL_QUARTERS","8"))
    GLOBAL_OFFSET= int(os.environ.get("GLOBAL_OFFSET","0"))
    scaling_repo= os.environ.get("SCALING_REPO","ni/labview-icon-editor")

    # List of repos
    all_repos= [
       "ni/labview-icon-editor",
       "facebook/react",
       "tensorflow/tensorflow",
       "dotnet/core"
    ]

    print("=== BFS Aggregator (Refined) ===\n")
    print(f"NUM_FISCAL_QUARTERS={NUM_FISCAL_QUARTERS}, GLOBAL_OFFSET={GLOBAL_OFFSET}")
    print(f"SCALING_REPO={scaling_repo}\n")

    # 3) baseline oldest date + offset
    oldest_dates={}
    for r in all_repos:
        od= find_oldest_date_for_repo(r)
        if od is None:
            # if no data at all
            od= datetime(2100,1,1)
        od= od+ timedelta(days=GLOBAL_OFFSET)
        oldest_dates[r]= od

    splitted_vars= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_vars= ["velocity","uig","mac","sei"]

    # BFS data structure
    BFS_data={}
    for r in all_repos:
        BFS_data[r]= {}
        sdt= oldest_dates[r]
        for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
            q_start= sdt
            q_end= q_start+ timedelta(days=90) # naive BFS 3mo
            BFS_data[r][q_idx]= {
              'start': q_start,
              'end': q_end,
              'raw': {},
              'agg': {},
              'ratio': {},
              'queriesUsed': {}
            }
            sdt= q_end

    # 4) gather splitted
    for r in all_repos:
        for q_idx in BFS_data[r]:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            splitted= gather_data_for_window(r, st, ed)
            BFS_data[r][q_idx]['raw']= splitted
            BFS_data[r][q_idx]['queriesUsed']= splitted["queriesUsed"]

    # aggregator compute
    def aggregator_compute(splitted, conf):
        merges= splitted["mergesRaw"]
        cIss=  splitted["closedIssRaw"]
        cPR=   splitted["closedPRRaw"]
        frk=   splitted["forksRaw"]
        stx=   splitted["starsRaw"]
        nIss=  splitted["newIssRaw"]
        cIssX= splitted["commentsIssRaw"]
        cPRX=  splitted["commentsPRRaw"]
        rIss=  splitted["reactIssRaw"]
        rPR=   splitted["reactPRRaw"]
        pl=    splitted["pullRaw"]

        velocityVal= compute_velocity(merges, cIss, cPR, conf)
        uigVal=      compute_uig(frk, stx, conf)
        macVal=      compute_mac(nIss, cIssX, cPRX, rIss, rPR, pl, conf)
        seiVal=      compute_sei(velocityVal, uigVal, macVal, conf)
        return {
          "mergesScaled": merges,
          "closedIssScaled": cIss,
          "closedPRScaled": cPR,
          "forksScaled": frk,
          "starsScaled": stx,
          "newIssScaled": nIss,
          "commentsIssScaled": cIssX,
          "commentsPRScaled": cPRX,
          "reactIssScaled": rIss,
          "reactPRScaled": rPR,
          "pullScaled": pl,
          "velocity": velocityVal,
          "uig": uigVal,
          "mac": macVal,
          "sei": seiVal
        }

    # 5) aggregator fill
    for r in all_repos:
        for q_idx in BFS_data[r]:
            splitted= BFS_data[r][q_idx]['raw']
            BFS_data[r][q_idx]['agg']= aggregator_compute(splitted, aggregator_conf)

    # group avg function
    def group_avg_var(q_idx, var, BFS_data, skip=None):
        s= 0.0
        c= 0
        for rp in BFS_data:
            if rp== skip:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_vars:
                val= BFS_data[rp][q_idx]['raw'][var]
            else:
                val= BFS_data[rp][q_idx]['agg'][var]
            s+= val
            c+=1
        if c>0:
            return s/c
        return 0.0

    # ratio vs group
    for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
        for var in splitted_vars+ aggregator_vars:
            avg_val= group_avg_var(q_idx, var, BFS_data, skip=None)
            for r in BFS_data:
                if q_idx not in BFS_data[r]:
                    continue
                if var in splitted_vars:
                    my_val= BFS_data[r][q_idx]['raw'][var]
                else:
                    my_val= BFS_data[r][q_idx]['agg'][var]
                BFS_data[r][q_idx]['ratio'][var]= ratio_vs_group_average(my_val, avg_val)

    # 6) BFS console prints
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
        for idx, row in enumerate(rows):
            parts= []
            for i,cel in enumerate(row):
                parts.append(str(cel).ljust(widths[i]))
            line= " | ".join(parts)
            lines.append(line)
            if idx==0:
                dash_parts= []
                for w in widths:
                    dash_parts.append("-"*w)
                dash_line= "-+-".join(dash_parts)
                lines.append(dash_line)
        return "\n".join(lines)

    splitted_vars_label= splitted_vars  # same

    for r in all_repos:
        print(f"=== BFS for Repo: {r} ===")

        # Summaries
        print(f"Existing Quarter Data for {r} | (mergesFactor=1.0000, closedIssFactor=1.0000, closedPRFactor=1.0000, forksFactor=1.0000, starsFactor=1.0000, newIssFactor=1.0000, commentsIssFactor=1.0000, commentsPRFactor=1.0000, reactIssFactor=1.0000, reactPRFactor=1.0000, pullFactor=1.0000)")

        header= [
          "Q-Range","mergesRaw","mRat",
          "closedIssRaw","cIssR","closedPRRaw","cPrR",
          "forksRaw","fRat","starsRaw","sRat",
          "newIss","nIssR","comIss","cIssR2","comPR","cPrR2",
          "reaIss","rIssR","reaPR","rPrR",
          "pull","pRat",
          "velocity","vRat",
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
            splitted= BFS_data[r][q_idx]['raw']
            agg= BFS_data[r][q_idx]['agg']
            rat= BFS_data[r][q_idx]['ratio']

            row= [
                label,
                str(splitted["mergesRaw"]),
                f"{rat['mergesRaw']:.3f}",
                str(splitted["closedIssRaw"]),
                f"{rat['closedIssRaw']:.3f}",
                str(splitted["closedPRRaw"]),
                f"{rat['closedPRRaw']:.3f}",
                str(splitted["forksRaw"]),
                f"{rat['forksRaw']:.3f}",
                str(splitted["starsRaw"]),
                f"{rat['starsRaw']:.3f}",
                str(splitted["newIssRaw"]),
                f"{rat['newIssRaw']:.3f}",
                str(splitted["commentsIssRaw"]),
                f"{rat['commentsIssRaw']:.3f}",
                str(splitted["commentsPRRaw"]),
                f"{rat['commentsPRRaw']:.3f}",
                str(splitted["reactIssRaw"]),
                f"{rat['reactIssRaw']:.3f}",
                str(splitted["reactPRRaw"]),
                f"{rat['reactPRRaw']:.3f}",
                str(splitted["pullRaw"]),
                f"{rat['pullRaw']:.3f}",
                f"{agg['velocity']:.3f}",
                f"{rat['velocity']:.3f}",
                f"{agg['uig']:.3f}",
                f"{rat['uig']:.3f}",
                f"{agg['mac']:.3f}",
                f"{rat['mac']:.3f}",
                f"{agg['sei']:.3f}",
                f"{rat['sei']:.3f}"
            ]
            rows.append(row)

        print(monospaced_table(rows))
        print()

        # aggregator expansions
        print(f"--- Additional Calculation Details for {r} (Velocity, UIG, MAC, SEI) ---\n")

        # mergesScaled, closedIssScaled, closedPRScaled, velocity
        h2= ["Q-Range","mergesScaled","closedIssScaled","closedPRScaled","Velocity"]
        r2= [h2]
        for q_idx in sorted_q:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            lbl= f"Q{q_idx}({st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')})"
            agg= BFS_data[r][q_idx]['agg']
            row= [
              lbl,
              f"{agg['mergesScaled']:.1f}",
              f"{agg['closedIssScaled']:.1f}",
              f"{agg['closedPRScaled']:.1f}",
              f"{agg['velocity']:.3f}"
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
            agg= BFS_data[r][q_idx]['agg']
            row= [
              lbl,
              f"{agg['forksScaled']:.1f}",
              f"{agg['starsScaled']:.1f}",
              f"{agg['uig']:.3f}"
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
            agg= BFS_data[r][q_idx]['agg']
            sumAll= (agg["newIssScaled"]
                     + agg["commentsIssScaled"]+ agg["commentsPRScaled"]
                     + agg["reactIssScaled"]+ agg["reactPRScaled"])
            row= [
              lbl,
              f"{sumAll:.1f}",
              f"{agg['pullScaled']:.1f}",
              f"{agg['mac']:.3f}"
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
            agg= BFS_data[r][q_idx]['agg']
            row= [
              lbl,
              f"{agg['velocity']:.3f}",
              f"{agg['uig']:.3f}",
              f"{agg['mac']:.3f}",
              f"{agg['sei']:.3f}"
            ]
            r5.append(row)
        print(monospaced_table(r5))
        print()

    print("=== Now produce side-by-side scaled charts for splitted + aggregator. ===")

    def compute_avg_for_var(q_idx, var, BFS_data, skip_repo=None):
        s=0.0
        c=0
        for rp in BFS_data:
            if rp== skip_repo:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_vars:
                vv= BFS_data[rp][q_idx]['raw'][var]
            else:
                vv= BFS_data[rp][q_idx]['agg'][var]
            s+= vv
            c+=1
        if c>0: return s/c
        return 0.0

    def produce_side_by_side_chart(variableName, BFS_data, all_repos, scaling_repo):
        if scaling_repo not in BFS_data:
            return
        sorted_quarters= sorted(BFS_data[scaling_repo].keys())
        x_vals= []
        scale_vals=[]
        group_vals=[]
        labels=[]
        for i,q_idx in enumerate(sorted_quarters):
            st= BFS_data[scaling_repo][q_idx]['start'].strftime("%Y-%m-%d")
            ed= BFS_data[scaling_repo][q_idx]['end'].strftime("%Y-%m-%d")
            qlbl= f"Q{q_idx}\n({st}..{ed})"
            labels.append(qlbl)
            if variableName in splitted_vars:
                s_val= BFS_data[scaling_repo][q_idx]['raw'][variableName]
            else:
                s_val= BFS_data[scaling_repo][q_idx]['agg'][variableName]

            g_val= compute_avg_for_var(q_idx, variableName, BFS_data, skip_repo=scaling_repo)
            x_vals.append(i)
            scale_vals.append(s_val)
            group_vals.append(g_val)

        bar_width= 0.4
        x_arr= np.arange(len(x_vals))

        plt.figure(figsize=(10,6))
        plt.bar(x_arr- bar_width/2, scale_vals, bar_width, label=scaling_repo, color='tab:blue')
        plt.bar(x_arr+ bar_width/2, group_vals, bar_width, label="NonScalingAvg", color='tab:orange')

        plt.title(f"{variableName} for {scaling_repo} vs Group Average")
        plt.xlabel("Index-Based BFS Quarters")
        plt.ylabel(variableName)
        plt.xticks(x_arr, labels, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()

        out_name= f"{variableName}_scaled.png"
        plt.savefig(out_name)
        plt.close()
        print(f"[INFO] Created {out_name}")

    all_vars= splitted_vars + aggregator_vars
    for var in all_vars:
        produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)

    print("\n=== BFS aggregator done. Now capturing debug log... ===")

    # 9) Print the queries used at the end, grouped by repo => q_idx => splitted variable
    print("\n=== QUERIES USED (By Repo, Quarter, Variable) ===")
    for r in all_repos:
        if r not in BFS_data: 
            continue
        print(f"\n--- Repo: {r} ---")
        sorted_q= sorted(BFS_data[r].keys())
        for q_idx in sorted_q:
            q_start= BFS_data[r][q_idx]['start'].strftime("%Y-%m-%d %H:%M:%S")
            q_end= BFS_data[r][q_idx]['end'].strftime("%Y-%m-%d %H:%M:%S")
            print(f"  Quarter {q_idx} => {q_start} .. {q_end}")
            Qdic= BFS_data[r][q_idx]['queriesUsed']
            for keyVar, (theSQL, theParams) in Qdic.items():
                print(f"    [{keyVar}] Query:")
                # Indent the SQL
                lines= theSQL.strip().split("\n")
                for ln in lines:
                    print(f"      {ln}")
                print(f"    Params: {theParams}")
                print()

    # flush console
    sys.stdout= old_stdout
    console_output= console_buffer.getvalue()

    # 10) Write console_output to debug_log
    with open("debug_log.txt","a", encoding="utf-8") as dbg:
        dbg.write(console_output)

    print("[INFO] Appended BFS aggregator logs + queries to debug_log.txt.\n")
    print("=== Done. ===")

if __name__=="__main__":
    main()
