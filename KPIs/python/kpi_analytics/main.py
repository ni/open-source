#!/usr/bin/env python3

import sys
import os
import argparse
from datetime import datetime, timedelta
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

def print_aligned_table(table_data, alignments=None):
    """
    Print a 2D list (table_data) with alignment for each column.
    table_data[0] = header row, subsequent = data rows.
    alignments is a list specifying 'left', 'center', or 'right' for each column.
      e.g. ['left', 'center', 'center', ...]
    If alignments is None, default to 'left' for all.
    """
    if not table_data:
        return

    num_cols = len(table_data[0])
    if alignments is None:
        alignments = ['left']*num_cols
    if len(alignments) < num_cols:
        alignments += ['left']*(num_cols - len(alignments))

    # Compute column widths
    col_widths = [0]*num_cols
    for row in table_data:
        for c_idx, cell in enumerate(row):
            cell_str = str(cell)
            if len(cell_str) > col_widths[c_idx]:
                col_widths[c_idx] = len(cell_str)

    # helper to format a cell given alignment
    def format_cell(cell_str, width, alignment):
        if alignment == 'left':
            return cell_str.ljust(width)
        elif alignment == 'center':
            # center
            pad = width - len(cell_str)
            left_pad = pad//2
            right_pad = pad - left_pad
            return ' '*left_pad + cell_str + ' '*right_pad
        else:
            # right
            return cell_str.rjust(width)

    # Print header row
    header = table_data[0]
    header_line = " | ".join(format_cell(str(header[i]), col_widths[i], alignments[i])
                             for i in range(num_cols))
    print(header_line)
    # separator
    sep_line = "-+-".join('-'*col_widths[i] for i in range(num_cols))
    print(sep_line)

    # Print data rows
    for row in table_data[1:]:
        row_line = " | ".join(format_cell(str(row[i]), col_widths[i], alignments[i])
                              for i in range(num_cols))
        print(row_line)


def main():
    parser = argparse.ArgumentParser(
        description="KPI Analytics with scaling repo, partial quarters, bar charts, and global date offset."
    )
    parser.add_argument("--global-offset", type=int, default=0,
                        help="Shift each repo's oldest date by this many days (can be negative).")
    args = parser.parse_args()
    global_offset_days = args.global_offset

    repos = [
        "ni/labview-icon-editor",
        "facebook/react", 
        "dotnet/core",
        "tensorflow/tensorflow"
    ]

    scaling_repo = get_scaling_repo()
    if not scaling_repo:
        print("[ERROR] No scaling_repo provided, aborting.")
        sys.exit(1)
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    # compute scale factors
    sfM, sfI, sfF, sfS, sfP = compute_scale_factors(scaling_repo, repos)

    num_quarters = get_num_fiscal_quarters()
    now = datetime.utcnow()

    for repo in repos:
        oldest_dt = find_oldest_date_for_repo(repo)
        if oldest_dt is None:
            print(f"\n[INFO] No data for {repo}, skipping.")
            continue

        # apply global offset
        oldest_dt = oldest_dt + timedelta(days=global_offset_days)

        # generate windows => partial if beyond now
        raw_quarters = generate_quarter_windows(oldest_dt, num_quarters)
        quarter_ranges = []
        for (qs, qe) in raw_quarters:
            if qs > now:
                break
            if qe > now:
                qe = now
            if qs < qe:
                quarter_ranges.append((qs, qe))

        if not quarter_ranges:
            print(f"[INFO] No valid windows for {repo}.")
            continue

        print(f"\n================= REPO: {repo} =================")
        print(f"Global offset = {global_offset_days} days => earliest date now = {oldest_dt}")
        print(f"Scale Factors => merges={sfM[repo]:.3f}, issues={sfI[repo]:.3f}, forks={sfF[repo]:.3f}, stars={sfS[repo]:.3f}, pulls={sfP[repo]:.3f}")
        print(f"Analyzing {num_quarters} quarters (partial if beyond now={now})\n")

        # build table data
        header = [
          "Quarter", "mergesRaw", "issuesRaw", "forksRaw", "starsRaw",
          "newIssRaw", "newCommRaw", "newReactRaw", "newPullRaw",
          "Velocity", "UIG", "MAC"
        ]
        table_data = [header]

        # alignment: first col left, rest center
        col_align = ["left"] + ["center"]*(len(header)-1)

        quarter_labels = []
        velocity_vals = []
        uig_vals = []
        mac_vals = []

        idx=1
        from comments_reactions import count_all_reactions
        from merges_issues import (
            count_merged_pulls, count_closed_issues,
            count_new_pulls, count_new_issues
        )
        from forks_stars import count_forks, count_stars
        from aggregator import velocity, user_interest_growth, monthly_active_contributors

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
            newPull_s = newPullRaw * sfP[repo]

            vel = velocity(merges_s, issues_s)
            uig = user_interest_growth(forks_s, stars_s)
            mac = monthly_active_contributors(newIss_s, newComm_s, newReact_s, newPull_s)

            label_str = f"Q{idx}({q_start:%Y-%m-%d}-{q_end:%Y-%m-%d})"
            quarter_labels.append(label_str)
            velocity_vals.append(vel)
            uig_vals.append(uig)
            mac_vals.append(mac)

            row = [
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
            table_data.append(row)
            idx+=1

        # print table with center alignment for numeric columns
        print_aligned_table(table_data, alignments=col_align)

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
