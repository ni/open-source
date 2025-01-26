############################################
# main.py
############################################

import os
import sys
import io
from datetime import datetime, timedelta

import mysql.connector
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
    # 1) Prepare to capture console for debug_log
    old_stdout= sys.stdout
    buffer= io.StringIO()
    sys.stdout= buffer

    # 2) Load aggregator config (no chart toggle)
    conf= load_config("config.ini")
    aggregator_conf= conf["aggregator"]

    # environment vars
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
    print(f"=== BFS Aggregator (Refined) ===\n")
    print(f"NUM_FISCAL_QUARTERS={NUM_FISCAL_QUARTERS}, GLOBAL_OFFSET={GLOBAL_OFFSET}")
    print(f"SCALING_REPO={scaling_repo}")
    print()

    # 3) Find oldest date + apply offset
    oldest_dates={}
    for r in all_repos:
        od= find_oldest_date_for_repo(r)
        if od is None:
            od= datetime(2100,1,1)
        od= od+ timedelta(days=GLOBAL_OFFSET)
        oldest_dates[r]= od

    # splitted + aggregator var names
    splitted_vars= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]
    aggregator_vars= ["velocity","uig","mac","sei"]

    # BFS_data structure
    BFS_data={}
    for r in all_repos:
        BFS_data[r]= {}
        sdt= oldest_dates[r]
        # build N quarters in 3-month increments
        for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
            q_start= sdt
            q_end= q_start+ timedelta(days=90)
            BFS_data[r][q_idx]= {
              'start': q_start,
              'end': q_end,
              'raw': {},
              'agg': {},
              'ratio': {}
            }
            sdt= q_end

    # 4) gather splitted data
    for r in all_repos:
        for q_idx in BFS_data[r]:
            st= BFS_data[r][q_idx]['start']
            ed= BFS_data[r][q_idx]['end']
            splitted= gather_data_for_window(r, st, ed)
            BFS_data[r][q_idx]['raw']= splitted

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

    # 5) aggregator for each quarter
    for r in all_repos:
        for q_idx in BFS_data[r]:
            splitted= BFS_data[r][q_idx]['raw']
            BFS_data[r][q_idx]['agg']= aggregator_compute(splitted, aggregator_conf)

    # group average
    def group_avg_var(q_idx, var, BFS_data, skip=None):
        s= 0.0
        c= 0
        for rp in BFS_data:
            if rp== skip:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_vars:
                v= BFS_data[rp][q_idx]['raw'][var]
            else:
                v= BFS_data[rp][q_idx]['agg'][var]
            s+= v
            c+=1
        return (s/c) if c>0 else 0.0

    # 6) ratio vs group
    for q_idx in range(1, NUM_FISCAL_QUARTERS+1):
        for var in splitted_vars+ aggregator_vars:
            avg_val= group_avg_var(q_idx, var, BFS_data, skip=None)
            for r in BFS_data:
                if q_idx not in BFS_data[r]:
                    continue
                if var in splitted_vars:
                    mine= BFS_data[r][q_idx]['raw'][var]
                else:
                    mine= BFS_data[r][q_idx]['agg'][var]
                BFS_data[r][q_idx]['ratio'][var]= ratio_vs_group_average(mine, avg_val)

    # 7) BFS console prints
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

    splitted_vars_label= [
      "mergesRaw","closedIssRaw","closedPRRaw","forksRaw","starsRaw",
      "newIssRaw","commentsIssRaw","commentsPRRaw","reactIssRaw","reactPRRaw","pullRaw"
    ]

    for r in all_repos:
        print(f"=== BFS for Repo: {r} ===")
        # Print a summary line of splitted factor=1
        print(f"Existing Quarter Data for {r} | (mergesFactor=1.0000, closedIssFactor=1.0000, closedPRFactor=1.0000, forksFactor=1.0000, starsFactor=1.0000, newIssFactor=1.0000, commentsIssFactor=1.0000, commentsPRFactor=1.0000, reactIssFactor=1.0000, reactPRFactor=1.0000, pullFactor=1.0000)")
        # Build BFS table
        header= [
          "Q-Range",
          "mergesRaw","mRat",
          "cIssRaw","cIssR",
          "cPRRaw","cPRR",
          "forks","fRat",
          "stars","sRat",
          "newIss","nIRt",
          "comIss","cIssRt",
          "comPR","cPRRt",
          "reaIss","rIssRt",
          "reaPR","rPRRt",
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
            # optional partial label if we want to detect any reason
            # (omitted for brevity here, or you can add logic if needed)

            rawd= BFS_data[r][q_idx]['raw']
            aggd= BFS_data[r][q_idx]['agg']
            ratd= BFS_data[r][q_idx]['ratio']

            merges= rawd["mergesRaw"]
            mergesRat= ratd["mergesRaw"]
            ciIss= rawd["closedIssRaw"]
            ciIssRat= ratd["closedIssRaw"]
            ciPR= rawd["closedPRRaw"]
            ciPRRat= ratd["closedPRRaw"]
            frk= rawd["forksRaw"]
            frkRat= ratd["forksRaw"]
            stx= rawd["starsRaw"]
            stxRat= ratd["starsRaw"]
            ni= rawd["newIssRaw"]
            niRat= ratd["newIssRaw"]
            cIss= rawd["commentsIssRaw"]
            cIssRat= ratd["commentsIssRaw"]
            cPR= rawd["commentsPRRaw"]
            cPRRat= ratd["commentsPRRaw"]
            rIss= rawd["reactIssRaw"]
            rIssRat= ratd["reactIssRaw"]
            rPR= rawd["reactPRRaw"]
            rPRRat= ratd["reactPRRaw"]
            pl= rawd["pullRaw"]
            plRat= ratd["pullRaw"]

            vel= aggd["velocity"]
            velRat= ratd["velocity"]
            uig= aggd["uig"]
            uigRat= ratd["uig"]
            mac= aggd["mac"]
            macRat= ratd["mac"]
            sei= aggd["sei"]
            seiRat= ratd["sei"]

            row= [
              label,
              str(merges),   f"{mergesRat:.3f}",
              str(ciIss),    f"{ciIssRat:.3f}",
              str(ciPR),     f"{ciPRRat:.3f}",
              str(frk),      f"{frkRat:.3f}",
              str(stx),      f"{stxRat:.3f}",
              str(ni),       f"{niRat:.3f}",
              str(cIss),     f"{cIssRat:.3f}",
              str(cPR),      f"{cPRRat:.3f}",
              str(rIss),     f"{rIssRat:.3f}",
              str(rPR),      f"{rPRRat:.3f}",
              str(pl),       f"{plRat:.3f}",
              f"{vel:.3f}",  f"{velRat:.3f}",
              f"{uig:.3f}",  f"{uigRat:.3f}",
              f"{mac:.3f}",  f"{macRat:.3f}",
              f"{sei:.3f}",  f"{seiRat:.3f}"
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

    # 8) produce side-by-side scaled charts for splitted + aggregator
    # lumps them all in one loop
    def compute_avg_for_var(q_idx, var, BFS_data, skip_repo=None):
        s=0.0
        c=0
        for rp in BFS_data:
            if rp== skip_repo:
                continue
            if q_idx not in BFS_data[rp]:
                continue
            if var in splitted_vars:
                val= BFS_data[rp][q_idx]['raw'][var]
            else:
                val= BFS_data[rp][q_idx]['agg'][var]
            s+= val
            c+=1
        return (s/c) if c>0 else 0.0

    def produce_side_by_side_chart(variableName, BFS_data, all_repos, scaling_repo):
        # pick sorted BFS indices from scaling repo
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

            # group
            g_val= compute_avg_for_var(q_idx, variableName, BFS_data, skip_repo=scaling_repo)

            scale_vals.append(s_val)
            group_vals.append(g_val)
            x_vals.append(i)

        bar_width=0.4
        x_arr= np.arange(len(x_vals))

        plt.figure(figsize=(10,6))
        plt.bar(x_arr- bar_width/2, scale_vals, bar_width, label=scaling_repo, color='tab:blue')
        plt.bar(x_arr+ bar_width/2, group_vals, bar_width, label="NonScalingAvg", color='tab:orange')

        plt.title(f"{variableName} ({scaling_repo} vs. group avg)")
        plt.xlabel("3-month BFS Quarters")
        plt.ylabel(variableName)
        plt.xticks(x_arr, q_labels, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()

        out_name= f"{variableName}_scaled.png"
        plt.savefig(out_name)
        plt.close()
        print(f"[INFO] Created {out_name}")

    print("=== Now produce side-by-side scaled charts for splitted + aggregator. ===")
    for var in splitted_vars + aggregator_vars:
        produce_side_by_side_chart(var, BFS_data, all_repos, scaling_repo)

    print("\n=== BFS aggregator done. Now capturing debug log... ===")

    # 9) Save console buffer to debug_log.txt
    sys.stdout= old_stdout
    console_output= buffer.getvalue()
    with open("debug_log.txt","w", encoding="utf-8") as dbg:
        dbg.write(console_output)
    print("[INFO] Overwrote debug_log.txt with BFS aggregator logs.\n")

    print("=== Done. ===")

if __name__=="__main__":
    main()
