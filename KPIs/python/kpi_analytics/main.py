"""
main.py (Diagnostic Edition)
Orchestrator for BFS aggregator with index-based intervals.
Prints queries, BFS aggregator tables, aggregator details to console & debug_log,
produces side-by-side bar charts for scaling_repo vs watchers-based group average.
Overwrites debug_log.txt each run.

Usage:
  python main.py
"""

import os
import sys
from datetime import datetime, timedelta

import mysql.connector
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# local imports
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
from splitted_metrics import gather_bfs_data
from aggregator import monthly_bfs_aggregator, watchers_weighted_group_avg

def connect_db(debug_lines):
    debug_lines.append("[INFO] Attempting DB connect...")
    print("[INFO] Attempting DB connect...")
    cnx = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )
    debug_lines.append("[INFO] DB connected successfully.")
    print("[INFO] DB connected successfully.")
    return cnx

def build_month_intervals(oldest_date, n_months=3):
    """
    Build intervals of 30 days each, for n_months total.
    If you want 3 intervals, you get intervals of 30 days each. That is a small coverage.
    Adapt or pass a bigger n_months if you want more intervals.
    """
    intervals=[]
    cur= oldest_date
    for i in range(n_months):
        nxt= cur + timedelta(days=30)
        intervals.append((cur,nxt))
        cur= nxt
    return intervals

def produce_side_by_side_chart(
    BFSkey,
    scaling_repo,
    BFSmap,
    groupArr,
    out_png,
    debug_lines
):
    """
    Compare scaling_repo BFSkey vs watchers-based group average on a bar chart.
    """
    debug_lines.append(f"[INFO] produce_side_by_side_chart => BFSkey={BFSkey}, out_png={out_png}")
    print(f"[INFO] produce_side_by_side_chart => BFSkey={BFSkey}, out_png={out_png}")

    if scaling_repo not in BFSmap or not BFSmap[scaling_repo]:
        debug_lines.append(f"[WARN] scaling_repo={scaling_repo} has no BFS data => skip chart {out_png}")
        print(f"[WARN] skip chart => {BFSkey} (no BFS data in BFSmap)")
        return

    mainList = BFSmap[scaling_repo]
    n = len(mainList)
    x_idx = range(n)
    scaling_vals = [ row.get(BFSkey, 0.0) for row in mainList ]
    group_vals = groupArr[:n]

    bar_width = 0.4
    x_scale = [x - bar_width/2 for x in x_idx]
    x_group = [x + bar_width/2 for x in x_idx]

    plt.figure(figsize=(10,6))
    plt.bar(x_scale, scaling_vals, bar_width, label=f"{scaling_repo} ({BFSkey})", color='tab:blue')
    plt.bar(x_group, group_vals, bar_width, label="GroupAvg(watchers)", color='tab:orange')

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    plt.title(f"{BFSkey} BFS - {scaling_repo}\nIndex-Based months\n{now_ts}")
    plt.xlabel("MonthIndex")
    plt.ylabel(BFSkey)
    plt.xticks(list(x_idx), [f"M{i+1}" for i in x_idx])
    plt.legend()
    plt.tight_layout()

    out_dir = os.path.dirname(out_png)
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    plt.savefig(out_png)
    plt.close()
    debug_lines.append(f"[INFO] saved chart => {out_png}")
    print(f"[INFO] saved chart => {out_png}")

def main():
    debug_lines = []
    print("=== Starting BFS aggregator (Diagnostic) ===")
    debug_lines.append("=== Starting BFS aggregator (Diagnostic) ===")

    # Prepare output folder
    out_dir = "kpi_outputs"
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    debug_file = os.path.join(out_dir, "debug_log.txt")

    # If a leftover file exists, we remove it to ensure fresh logs
    if os.path.isfile(debug_file):
        os.remove(debug_file)

    # Attempt DB connection
    try:
        cnx = connect_db(debug_lines)
    except Exception as ex:
        msg = f"[ERROR] DB connection failed => {ex}"
        print(msg)
        debug_lines.append(msg)
        with open(debug_file,"w",encoding="utf-8") as f:
            for ln in debug_lines:
                print(ln,file=sys.stderr)
                f.write(ln+"\n")
        sys.exit(1)

    cursor = cnx.cursor()

    # List repos (example)
    repos = [
        "ni/labview-icon-editor",
        "facebook/react",
        "tensorflow/tensorflow",
        "dotnet/core"
    ]
    scaling_repo = "ni/labview-icon-editor"

    debug_lines.append(f"[INFO] Repos => {repos}")
    debug_lines.append(f"[INFO] scaling_repo => {scaling_repo}")
    print(f"[INFO] Repos => {repos}")
    print(f"[INFO] scaling_repo => {scaling_repo}")

    # Oldest date or from environment?
    # We'll just define a sample:
    oldest_date = datetime(2024,1,1)
    debug_lines.append(f"[INFO] oldest_date => {oldest_date}")
    print(f"[INFO] oldest_date => {oldest_date}")

    # Build intervals => by default let's do 3 intervals
    intervals = build_month_intervals(oldest_date, n_months=3)
    debug_lines.append(f"[INFO] intervals => {intervals}")
    print(f"[INFO] intervals => {intervals}")

    BFSmap = {}
    # Gather BFS splitted data
    for repo in repos:
        debug_lines.append(f"\n=== BFS for Repo: {repo} ===")
        print(f"\n=== BFS for Repo: {repo} ===")

        splitted_rows = []
        try:
            splitted_rows = gather_bfs_data(repo, intervals, cursor, debug_lines)
        except Exception as ex:
            dbg = f"[ERROR] gather_bfs_data => {repo}, ex={ex}"
            debug_lines.append(dbg)
            print(dbg)
            splitted_rows = []

        # aggregator
        from aggregator import monthly_bfs_aggregator
        final_data = monthly_bfs_aggregator(repo, splitted_rows, debug_lines, weightingApproach="watchers")
        BFSmap[repo] = final_data

        # Print BFS table
        # sample columns
        head = ("Interval                         | mergesRaw | closedIssRaw | closedPRRaw | forksRaw | starsRaw | watchersRaw |"
                " commentsIssueRaw | commentsPRRaw | reactIssueRaw | reactPRRaw | distinctPartRaw | mergesScaled | watchersScaled | velocity | uig | mac | sei")
        sep = "-"*(len(head)+40)
        debug_lines.append(head)
        debug_lines.append(sep)
        print(head)
        print(sep)

        for i, row in enumerate(final_data):
            # build line
            st= row.get('start_dt')
            ed= row.get('end_dt')
            rng= ""
            if st and ed:
                rng = f"{st.strftime('%Y-%m-%d')}..{ed.strftime('%Y-%m-%d')}"
                if row.get('partialCoverage'):
                    rng += " (partial)"
            elif row.get('forecastRow'):
                rng = "(Forecast)"

            mRaw= row['mergesRaw']
            cIssRaw= row['closedIssRaw']
            cPRRaw= row['closedPRRaw']
            fRaw= row['forksRaw']
            sRaw= row['starsRaw']
            wRaw= row['watchersRaw']
            ciRaw= row['commentsIssueRaw']
            cpRaw= row['commentsPRRaw']
            riRaw= row['reactIssueRaw']
            rpRaw= row['reactPRRaw']
            dpRaw= row['distinctPartRaw']

            mS= row.get('mergesScaled',0.0)
            wS= row.get('watchersScaled',0.0)
            vel= row.get('velocity',0.0)
            uig= row.get('uig',0.0)
            mac= row.get('mac',0.0)
            sei= row.get('sei',0.0)

            line= (f"{rng:<32} | {mRaw:>9} | {cIssRaw:>12} | {cPRRaw:>11} | {fRaw:>8} | {sRaw:>8} | {wRaw:>11} |"
                   f" {ciRaw:>16} | {cpRaw:>13} | {riRaw:>14} | {rpRaw:>10} | {dpRaw:>16} | "
                   f"{mS:>12.2f} | {wS:>14.2f} | {vel:>8.2f} | {uig:>4.2f} | {mac:>4.2f} | {sei:>4.2f}")
            debug_lines.append(line)
            print(line)

    # watchers-based group average => mergesScaled, velocity, mac, uig, sei
    from aggregator import watchers_weighted_group_avg
    mergesGA= watchers_weighted_group_avg(BFSmap,"mergesScaled",debug_lines,"watchers")
    velocityGA= watchers_weighted_group_avg(BFSmap,"velocity",debug_lines,"watchers")
    macGA= watchers_weighted_group_avg(BFSmap,"mac",debug_lines,"watchers")
    uigGA= watchers_weighted_group_avg(BFSmap,"uig",debug_lines,"watchers")
    seiGA= watchers_weighted_group_avg(BFSmap,"sei",debug_lines,"watchers")

    debug_lines.append("[INFO] Now produce side-by-side scaled charts")
    print("[INFO] Now produce side-by-side scaled charts")

    def do_side(BFSkey, groupArr, fname):
        produce_side_by_side_chart(
            BFSkey,
            scaling_repo,
            BFSmap,
            groupArr,
            os.path.join(out_dir,fname),
            debug_lines
        )

    do_side("mergesScaled", mergesGA, "merges_scaled.png")
    do_side("velocity", velocityGA, "velocity_compare.png")
    do_side("mac", macGA, "mac_compare.png")
    do_side("uig", uigGA, "uig_compare.png")
    do_side("sei", seiGA, "sei_compare.png")

    debug_lines.append("[INFO] aggregator done, saving debug_log now")
    print("[INFO] aggregator done, saving debug_log now")

    debug_path = os.path.join(out_dir,"debug_log.txt")
    with open(debug_path,"w",encoding="utf-8") as f:
        for ln in debug_lines:
            # also print to console so you see everything
            print(ln)
            f.write(ln+"\n")

    cursor.close()
    cnx.close()
    print(f"=== BFS aggregator complete. Overwrote debug_log => {debug_path} ===")

if __name__=="__main__":
    main()
