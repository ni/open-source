#!/usr/bin/env python3
"""
main.py - BFS aggregator with splitted metrics, aggregator expansions,
          side-by-side scaled charts, partial coverage labeling, openIssRatio=1.0 etc.

Mirrors the sample console output you provided. No placeholders remain
except we set openIssRatio=open_pr_ratio=1.0 stub. 
No lines omitted.
"""

import sys
import os
import io
from datetime import datetime
import mysql.connector

import matplotlib
matplotlib.use("Agg")  # so it doesn't pop up an interactive window

from config import get_scaling_repo, get_num_groups
from baseline import find_oldest_date_for_repo
from quarters import generate_time_groups, label_group
from scale_factors import compute_scale_factors
from splitted_metrics import gather_data_for_window
from aggregator import velocity, user_interest_growth, monthly_active_contributors, compute_sei
from aggregator import open_iss_ratio_stub, open_pr_ratio_stub
import charts

original_stdout= sys.stdout
log_capture= io.StringIO()

class DualOutput:
    def write(self, txt):
        original_stdout.write(txt)
        log_capture.write(txt)
    def flush(self):
        original_stdout.flush()
        log_capture.flush()

sys.stdout= DualOutput()

def print_table(table_data):
    if not table_data: return
    widths= [0]* len(table_data[0])
    for row in table_data:
        for i, cell in enumerate(row):
            sc= str(cell)
            if len(sc)> widths[i]:
                widths[i]= len(sc)
    def left(s,w):
        return s.ljust(w)
    def right(s,w):
        return s.rjust(w)
    # header
    hdr= " | ".join(left(str(table_data[0][i]), widths[i]) for i in range(len(table_data[0])))
    print(hdr)
    sep= "-+-".join("-"*widths[i] for i in range(len(table_data[0])))
    print(sep)
    # rows
    for row in table_data[1:]:
        line= " | ".join(right(str(row[i]), widths[i]) for i in range(len(row)))
        print(line)

def find_repo_last_date(repo):
    """
    Finds the maximum date among various tables for partial coverage labeling.
    """
    from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
    cnx= mysql.connector.connect(host=DB_HOST,user=DB_USER,password=DB_PASSWORD,database=DB_DATABASE)
    cursor= cnx.cursor()
    query= """
        SELECT MAX(all_max)
        FROM (
            SELECT MAX(created_at) AS all_max FROM issues WHERE repo_name=%s
            UNION
            SELECT MAX(created_at) AS all_max FROM pulls WHERE repo_name=%s
            UNION
            SELECT MAX(created_at) AS all_max FROM forks WHERE repo_name=%s
            UNION
            SELECT MAX(starred_at) AS all_max FROM stars WHERE repo_name=%s
            UNION
            SELECT MAX(created_at) AS all_max FROM issue_comments WHERE repo_name=%s
            UNION
            SELECT MAX(created_at) AS all_max FROM comment_reactions WHERE repo_name=%s
            UNION
            SELECT MAX(created_at) AS all_max FROM issue_events WHERE repo_name=%s
            UNION
            SELECT MAX(created_at) AS all_max FROM pull_events WHERE repo_name=%s
        ) subq
    """
    cursor.execute(query,(repo,repo,repo,repo,repo,repo,repo,repo))
    row= cursor.fetchone()
    cursor.close()
    cnx.close()
    if row and row[0]:
        return row[0]
    return None

def main():
    print("=== ENVIRONMENT VARIABLES ===")
    print(f"SCALING_REPO={os.environ.get('SCALING_REPO','ni/labview-icon-editor')}")
    print(f"NUM_FISCAL_QUARTERS={os.environ.get('NUM_FISCAL_QUARTERS','8')}")

    print("\n=== CAPTURED CONSOLE OUTPUT ===\n")

    repos= ["ni/labview-icon-editor","facebook/react","tensorflow/tensorflow","dotnet/core"]
    scaling_repo= get_scaling_repo()
    if scaling_repo not in repos:
        repos.append(scaling_repo)
    num_g= get_num_groups()

    BFS_data= {}
    mergesF, closedF, forksF, starsF, newIssF, commF, reacF, pullsF= compute_scale_factors(scaling_repo, repos)

    for r in repos:
        BFS_data[r]= {}
        oldest= find_oldest_date_for_repo(r)
        if not oldest:
            continue
        last_dt= find_repo_last_date(r)
        if not last_dt:
            continue
        groups= generate_time_groups(oldest, num_g)
        idx=1
        for (st,ed) in groups:
            partial= False
            real_end= ed
            if ed> last_dt:
                partial= True
                real_end= last_dt
            if st>= real_end:
                break
            splitted= gather_data_for_window(r, st, real_end)
            BFS_data[r][idx]= (st, real_end, partial, splitted)
            idx+=1

    # Print BFS aggregator 
    for r in repos:
        if not BFS_data[r]:
            continue
        print(f"=== BFS for Repo: {r} ===")

        # Summarize
        print(f"Existing Quarter Data for {r} | (mergesFactor={mergesF[r]:.4f}, closedFactor={closedF[r]:.4f}, forksFactor={forksF[r]:.4f}, starsFactor={starsF[r]:.4f}, newIssuesFactor={newIssF[r]:.4f}, commentsFactor={commF[r]:.4f}, reactionsFactor={reacF[r]:.4f}, pullsFactor={pullsF[r]:.4f})")
        # BFS table => mergesRaw, closedRaw, forksRaw, starsRaw, newIssRaw, commentsRaw, reactionsRaw, pullRaw
        # plus mergesScaled, closedScaled, forksScaled, etc.
        head= ["Q-Range","mergesRaw","closedRaw","forksRaw","starsRaw","newIssRaw","commentsRaw","reactRaw","pullRaw",
               "mergesScaled","closedScaled","forksScaled","starsScaled","newIssScaled","commentsScaled","reactScaled","pullScaled",
               "Velocity","UIG","MAC"]
        rows= [head]
        
        for idx in sorted(BFS_data[r].keys()):
            (st,ed,partialFlag, splitted)= BFS_data[r][idx]
            qlbl= f"Q{idx}({st:%Y-%m-%d}..{ed:%Y-%m-%d})"
            if partialFlag:
                qlbl+= " (partial)"
            # scaled
            merges_s= splitted["mergesRaw"]* mergesF[r]
            closed_s= splitted["closedRaw"]* closedF[r]
            forks_s= splitted["forksRaw"]* forksF[r]
            stars_s= splitted["starsRaw"]* starsF[r]
            newIss_s= splitted["newIssRaw"]* newIssF[r]
            comm_s= splitted["commentsRaw"]* commF[r]
            reac_s= splitted["reactionsRaw"]* reacF[r]
            pull_s= splitted["pullRaw"]* pullsF[r]

            # aggregator expansions
            vel= velocity(merges_s,closed_s)
            uig= user_interest_growth(forks_s, stars_s)
            # sum of (newIss_s + comm_s + reac_s) => X, then monthly_active_contributors(X, pull_s)
            sum_isscomm= (newIss_s + comm_s + reac_s)
            mac_val= monthly_active_contributors(sum_isscomm, pull_s)
            # we can compute SEI => but user wants it in a separate table or same. We'll do separate table below, or keep in "Velocity" line?
            # We'll do partial coverage approach => it's fine
            # For now, store them
            # We'll do openIssRatio=1.0, openPRRatio=1.0 from aggregator
            openIssR= open_iss_ratio_stub()
            openPRR= open_pr_ratio_stub()

            rows.append([
                qlbl,
                splitted["mergesRaw"],
                splitted["closedRaw"],
                splitted["forksRaw"],
                splitted["starsRaw"],
                splitted["newIssRaw"],
                splitted["commentsRaw"],
                splitted["reactionsRaw"],
                splitted["pullRaw"],
                f"{merges_s:.4f}",
                f"{closed_s:.4f}",
                f"{forks_s:.4f}",
                f"{stars_s:.4f}",
                f"{newIss_s:.4f}",
                f"{comm_s:.4f}",
                f"{reac_s:.4f}",
                f"{pull_s:.4f}",
                f"{vel:.4f}",
                f"{uig:.4f}",
                f"{mac_val:.4f}"
            ])
        # print BFS aggregator table
        print_table(rows)
        print()

        # We'll produce second table for openIssRatio, openPRRatio, Velocity, UIG, MAC, SEI
        head2= ["Q-Range","openIssRatio","openPRRatio","Velocity","UIG","MAC","SEI"]
        rows2= [head2]
        for idx in sorted(BFS_data[r].keys()):
            (st,ed,pf, splitted)= BFS_data[r][idx]
            qlbl= f"Q{idx}({st:%Y-%m-%d}..{ed:%Y-%m-%d})"
            if pf: qlbl+=" (partial)"
            merges_s= splitted["mergesRaw"]* mergesF[r]
            closed_s= splitted["closedRaw"]* closedF[r]
            forks_s= splitted["forksRaw"]* forksF[r]
            stars_s= splitted["starsRaw"]* starsF[r]
            newIss_s= splitted["newIssRaw"]* newIssF[r]
            comm_s= splitted["commentsRaw"]* commF[r]
            reac_s= splitted["reactionsRaw"]* reacF[r]
            pull_s= splitted["pullRaw"]* pullsF[r]
            vel= velocity(merges_s,closed_s)
            uig= user_interest_growth(forks_s,stars_s)
            mac_val= monthly_active_contributors(newIss_s+comm_s+reac_s,pull_s)
            sei_val= compute_sei(vel,uig,mac_val)
            # placeholders for openIssRatio, openPRRatio => 1.0
            oir= open_iss_ratio_stub()
            opr= open_pr_ratio_stub()
            rows2.append([
              qlbl,
              f"{oir:.3f}",
              f"{opr:.3f}",
              f"{vel:.4f}",
              f"{uig:.4f}",
              f"{mac_val:.4f}",
              f"{sei_val:.4f}"
            ])
        print_table(rows2)
        print("------------------------------------------------------\n")

        # "Additional Calculation Details for r (Velocity, UIG, MAC)"
        print(f"--- Additional Calculation Details for {r} (Velocity, UIG, MAC) ---\n")
        # velocity details
        headV= ["Q-Range","mergesScaled","closedScaled","Velocity=0.4*M+0.6*C"]
        rowsV= [headV]
        for idx in sorted(BFS_data[r].keys()):
            (st,ed,pf, splitted)= BFS_data[r][idx]
            qlbl= f"Q{idx}({st:%Y-%m-%d}..{ed:%Y-%m-%d})"
            if pf: qlbl+=" (partial)"
            merges_s= splitted["mergesRaw"]* mergesF[r]
            closed_s= splitted["closedRaw"]* closedF[r]
            vel= velocity(merges_s,closed_s)
            rowsV.append([
              qlbl,
              f"{merges_s:.4f}",
              f"{closed_s:.4f}",
              f"{vel:.4f}"
            ])
        print_table(rowsV)
        print()

        # uig details
        headU= ["Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"]
        rowsU= [headU]
        for idx in sorted(BFS_data[r].keys()):
            (st,ed,pf, splitted)= BFS_data[r][idx]
            qlbl= f"Q{idx}({st:%Y-%m-%d}..{ed:%Y-%m-%d})"
            if pf: qlbl+=" (partial)"
            forks_s= splitted["forksRaw"]* forksF[r]
            stars_s= splitted["starsRaw"]* starsF[r]
            uig= user_interest_growth(forks_s, stars_s)
            rowsU.append([
              qlbl,
              f"{forks_s:.4f}",
              f"{stars_s:.4f}",
              f"{uig:.4f}"
            ])
        print_table(rowsU)
        print()

        # mac details
        headM= ["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]
        rowsM= [headM]
        for idx in sorted(BFS_data[r].keys()):
            (st,ed,pf, splitted)= BFS_data[r][idx]
            qlbl= f"Q{idx}({st:%Y-%m-%d}..{ed:%Y-%m-%d})"
            if pf: qlbl+=" (partial)"
            newIss_s= splitted["newIssRaw"]* newIssF[r]
            comm_s= splitted["commentsRaw"]* commF[r]
            reac_s= splitted["reactionsRaw"]* reacF[r]
            sum_isscomm= newIss_s+ comm_s+ reac_s
            pull_s= splitted["pullRaw"]* pullsF[r]
            mac_val= monthly_active_contributors(sum_isscomm, pull_s)
            rowsM.append([
              qlbl,
              f"{sum_isscomm:.4f}",
              f"{pull_s:.4f}",
              f"{mac_val:.4f}"
            ])
        print_table(rowsM)
        print()

    print("=== BFS aggregator done. Now produce side-by-side scaled charts. ===\n")

    # produce splitted metrics charts
    splitted_metrics= ["merges","closed","forks","stars","newIss","comments","reactions","pull"]
    for met in splitted_metrics:
        charts.produce_side_by_side_chart(met, scaling_repo, BFS_data, mergesF, closedF, forksF, starsF, newIssF, commF, reacF, pullsF)

    # aggregator expansions charts => velocity, uig, mac, sei
    def aggregator_velocity(repo, splitted):
        merges_s= splitted["mergesRaw"]* mergesF[repo]
        closed_s= splitted["closedRaw"]* closedF[repo]
        return velocity(merges_s, closed_s)

    def aggregator_uig(repo, splitted):
        forks_s= splitted["forksRaw"]* forksF[repo]
        stars_s= splitted["starsRaw"]* starsF[repo]
        return user_interest_growth(forks_s, stars_s)

    def aggregator_mac(repo, splitted):
        newIss_s= splitted["newIssRaw"]* newIssF[repo]
        comm_s= splitted["commentsRaw"]* commF[repo]
        reac_s= splitted["reactionsRaw"]* reacF[repo]
        sum_isscomm= newIss_s+ comm_s+ reac_s
        pulls_s= splitted["pullRaw"]* pullsF[repo]
        return monthly_active_contributors(sum_isscomm,pulls_s)

    def aggregator_sei(repo, splitted):
        v= aggregator_velocity(repo,splitted)
        u= aggregator_uig(repo,splitted)
        m= aggregator_mac(repo,splitted)
        return compute_sei(v,u,m)

    charts.produce_aggregator_chart("velocity", scaling_repo, BFS_data, aggregator_velocity)
    charts.produce_aggregator_chart("uig", scaling_repo, BFS_data, aggregator_uig)
    charts.produce_aggregator_chart("mac", scaling_repo, BFS_data, aggregator_mac)
    charts.produce_aggregator_chart("sei", scaling_repo, BFS_data, aggregator_sei)

    print("\n=== Done. BFS aggregator + side-by-side scaled charts. ===")

    sys.stdout.flush()
    captured= log_capture.getvalue()
    sys.stdout= original_stdout
    with open("debug_log.txt","w",encoding="utf-8") as f:
        f.write(captured)
    print("[INFO] Overwrote debug_log => debug_log.txt")

if __name__=="__main__":
    main()
