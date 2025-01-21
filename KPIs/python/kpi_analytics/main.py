# main.py

import sys
import os
from datetime import datetime
import math
import matplotlib.pyplot as plt

from baseline import find_oldest_date_for_repo
from quarters import generate_quarter_windows
from merges_issues import (
    count_merged_pulls, count_closed_issues,
    count_new_pulls, count_new_issues
)
from forks_stars import count_forks, count_stars
from comments_reactions import (
    count_issue_comments,
    count_all_reactions
)
from aggregator import (
    velocity, user_interest_growth, monthly_active_contributors
)
from scale_factors import compute_scale_factors
from config import (
    get_scaling_repo,
    get_num_fiscal_quarters
)

def main():
    repos = [
        "ni/labview-icon-editor",
        "facebook/react",
        "tensorflow/tensorflow",
        "dotnet/core"
    ]
    scaling_repo = get_scaling_repo()
    if not scaling_repo:
        print("[ERROR] No scaling_repo provided, aborting.")
        sys.exit(1)
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    # retrieve 5 scale factor dicts
    sfM, sfI, sfF, sfS, sfP = compute_scale_factors(scaling_repo, repos)

    num_quarters = get_num_fiscal_quarters()
    now = datetime.utcnow()

    for repo in repos:
        oldest_dt = find_oldest_date_for_repo(repo)
        if oldest_dt is None:
            print(f"\n[INFO] No data for {repo}, skipping.")
            continue

        # generate windows
        raw_quarters = generate_quarter_windows(oldest_dt, num_quarters)
        quarter_ranges = []
        for (qs, qe) in raw_quarters:
            if qs > now:
                break
            if qe > now:
                qe = now
            quarter_ranges.append((qs, qe))

        if not quarter_ranges:
            print(f"[INFO] No valid windows for {repo}.")
            continue

        print(f"\n================= REPO: {repo} =================")
        print(f"Scale Factors => merges={sfM[repo]:.3f}, issues={sfI[repo]:.3f}, forks={sfF[repo]:.3f}, stars={sfS[repo]:.3f}, pulls={sfP[repo]:.3f}")
        print(f"Oldest date: {oldest_dt} => Quarters= {num_quarters} (partial if beyond now={now})\n")

        col_header = [
            "           Quarter       ", "mergesRaw", "issuesRaw", "forksRaw", "starsRaw",
            "newIssRaw", "newCommRaw", "newReactRaw", "newPullRaw",
            "Velocity", "UIG", "MAC"
        ]
        widths = [17, 10, 10, 9, 9, 10, 11, 12, 11, 9, 6, 6]

        header_str = " | ".join(h.ljust(widths[i]) for i,h in enumerate(col_header))
        line_len = sum(widths) + 3*(len(widths)-1)
        print(header_str)
        print("-"*line_len)

        quarter_labels = []
        velocity_vals = []
        uig_vals = []
        mac_vals = []

        idx=1
        for (q_start, q_end) in quarter_ranges:
            mergesRaw   = count_merged_pulls(repo, q_start, q_end)
            issuesRaw   = count_closed_issues(repo, q_start, q_end)
            forksRaw    = count_forks(repo, q_start, q_end)
            starsRaw    = count_stars(repo, q_start, q_end)
            newIssRaw   = count_new_issues(repo, q_start, q_end)
            newCommRaw  = count_issue_comments(repo, q_start, q_end)
            newReactRaw = count_all_reactions(repo, q_start, q_end)
            newPullRaw  = count_new_pulls(repo, q_start, q_end)

            # scale them
            merges_s  = mergesRaw  * sfM[repo]
            issues_s  = issuesRaw  * sfI[repo]
            forks_s   = forksRaw   * sfF[repo]
            stars_s   = starsRaw   * sfS[repo]
            newIss_s  = newIssRaw  * sfI[repo]
            newComm_s = newCommRaw * sfI[repo]
            newReact_s= newReactRaw* sfI[repo]
            # new pulls => scaleFactorP
            newPull_s = newPullRaw * sfP[repo]

            vel = velocity(merges_s, issues_s)
            uig = user_interest_growth(forks_s, stars_s)
            mac = monthly_active_contributors(
                newIss_s, newComm_s, newReact_s, newPull_s
            )

            label_str = f"Q{idx}({q_start:%Y-%m-%d}-{q_end:%Y-%m-%d})"
            quarter_labels.append(label_str)
            velocity_vals.append(vel)
            uig_vals.append(uig)
            mac_vals.append(mac)

            row_data = [
                label_str,
                str(mergesRaw),
                str(issuesRaw),
                str(forksRaw),
                str(starsRaw),
                str(newIssRaw),
                str(newCommRaw),
                str(newReactRaw),
                str(newPullRaw),
                f"{vel:.2f}",
                f"{uig:.2f}",
                f"{mac:.2f}"
            ]
            row_str = " | ".join(row_data[i].ljust(widths[i]) for i in range(len(widths)))
            print(row_str)
            idx+=1

        # produce bar charts
        plt.figure(figsize=(8,4))
        plt.bar(range(len(velocity_vals)), velocity_vals, color='teal')
        plt.title(f"Velocity(Scaled)-{repo}")
        plt.xticks(range(len(quarter_labels)), quarter_labels, rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"velocity_{repo.replace('/','_')}.png")
        plt.close()

        plt.figure(figsize=(8,4))
        plt.bar(range(len(uig_vals)), uig_vals, color='blue')
        plt.title(f"UIG(Scaled)-{repo}")
        plt.xticks(range(len(quarter_labels)), quarter_labels, rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"uig_{repo.replace('/','_')}.png")
        plt.close()

        plt.figure(figsize=(8,4))
        plt.bar(range(len(mac_vals)), mac_vals, color='green')
        plt.title(f"MAC(Scaled)-{repo}")
        plt.xticks(range(len(quarter_labels)), quarter_labels, rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"mac_{repo.replace('/','_')}.png")
        plt.close()

        print(f"\n[INFO] velocity/uig/mac bar charts saved for {repo}.\n")


if __name__=="__main__":
    main()
