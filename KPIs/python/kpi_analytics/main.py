#!/usr/bin/env python3
"""
main.py - Final integrated solution

- BFS debug prints for raw & aggregator data
- Creates separate PNGs for each raw variable + aggregator metrics
- Single PNG: bar chart on left, table on right
- Removes text_props=..., instead sets _text properties after creation
  to fix older Matplotlib TypeError.
"""

import sys
import os
import io
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

from config import get_scaling_repo, get_num_fiscal_quarters
from aggregator import (
    load_aggregator_weights,
    velocity,
    user_interest_growth,
    monthly_active_contributors
)
from scale_factors import (
    compute_scale_factors,
    compute_target_reached_data,
    compute_sei_data
)
from baseline import find_oldest_date_for_repo
from quarters import generate_quarter_windows
from merges_issues import (
    count_merged_pulls,
    count_closed_issues,
    count_new_pulls,
    count_new_issues,
    count_open_issues_at_date,
    count_open_prs_at_date
)
from forks_stars import count_forks, count_stars
from comments_reactions import count_issue_comments, count_all_reactions

original_stdout= sys.stdout
log_capture= io.StringIO()

class DualOutput:
    def write(self, text):
        original_stdout.write(text)
        log_capture.write(text)
    def flush(self):
        original_stdout.flush()
        log_capture.flush()

sys.stdout= DualOutput()


def print_aligned_table(table_data, alignments=None):
    if not table_data:
        return
    num_cols= len(table_data[0])
    if alignments is None:
        alignments= ['left']* num_cols
    if len(alignments)< num_cols:
        alignments+= ['left']* (num_cols- len(alignments))

    col_widths= [0]* num_cols
    for row in table_data:
        for c_idx, cell in enumerate(row):
            c_str= str(cell)
            if len(c_str)> col_widths[c_idx]:
                col_widths[c_idx]= len(c_str)

    def format_cell(cell_str, width, alignment):
        if alignment=='left':
            return cell_str.ljust(width)
        elif alignment=='center':
            pad= width- len(cell_str)
            left_pad= pad//2
            right_pad= pad- left_pad
            return ' '*left_pad+ cell_str+ ' '*right_pad
        else:
            return cell_str.rjust(width)

    header_line= " | ".join(
        format_cell(str(table_data[0][i]), col_widths[i], alignments[i])
        for i in range(num_cols)
    )
    print(header_line)
    sep_line= "-+-".join("-"*col_widths[i] for i in range(num_cols))
    print(sep_line)
    for row in table_data[1:]:
        row_line= " | ".join(
            format_cell(str(row[i]), col_widths[i], alignments[i])
            for i in range(num_cols)
        )
        print(row_line)


def BFS_print_repo_table(
    repo, mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    velocity_data, uig_data, mac_data,
    quarter_dates
    ):

    fac_str= (f"(mergesFactor={mergesFactor[repo]:.4f}, closedFactor={closedFactor[repo]:.4f}, "
              f"forksFactor={forksFactor[repo]:.4f}, starsFactor={starsFactor[repo]:.4f}, "
              f"newIssuesFactor={newIssuesFactor[repo]:.4f}, commentsFactor={commentsFactor[repo]:.4f}, "
              f"reactionsFactor={reactionsFactor[repo]:.4f}, pullsFactor={pullsFactor[repo]:.4f})")
    print(f"Existing Quarter Data for {repo} | {fac_str}")

    BFS_data= [[
        "Q-Range","mergesRaw","closedRaw","forksRaw","starsRaw",
        "newIssRaw","newCommRaw","newReactRaw","newPullRaw",
        "mergesScaled","closedScaled","forksScaled","starsScaled",
        "newIssScaled","newCommScaled","newReactScaled","newPullScaled",
        "Velocity","UIG","MAC"
    ]]
    BFS_align= ["left"]+ ["center"]*18

    sorted_quarters= sorted(quarter_dates[repo].keys())
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"

        mergesRaw= merges_data[repo].get(q_idx,0.0)
        closedRaw= closed_data[repo].get(q_idx,0.0)
        forksRaw= forks_data[repo].get(q_idx,0.0)
        starsRaw= stars_data[repo].get(q_idx,0.0)
        newIssRaw= newIss_data[repo].get(q_idx,0.0)
        commRaw= comm_data[repo].get(q_idx,0.0)
        reacRaw= reac_data[repo].get(q_idx,0.0)
        pullRaw= pull_data[repo].get(q_idx,0.0)

        vel= velocity_data[repo].get(q_idx,0.0)
        uigv= uig_data[repo].get(q_idx,0.0)
        macv= mac_data[repo].get(q_idx,0.0)

        mergesScale= mergesRaw* mergesFactor[repo]
        closedScale= closedRaw* closedFactor[repo]
        forksScale= forksRaw* forksFactor[repo]
        starsScale= starsRaw* starsFactor[repo]
        newIssScale= newIssRaw* newIssuesFactor[repo]
        commScale= commRaw* commentsFactor[repo]
        reacScale= reacRaw* reactionsFactor[repo]
        pullScale= pullRaw* pullsFactor[repo]

        BFS_data.append([
            label_str,
            f"{mergesRaw}", f"{closedRaw}", f"{forksRaw}", f"{starsRaw}",
            f"{newIssRaw}", f"{commRaw}", f"{reacRaw}", f"{pullRaw}",

            f"{mergesScale:.4f}", f"{closedScale:.4f}", f"{forksScale:.4f}", f"{starsScale:.4f}",
            f"{newIssScale:.4f}", f"{commScale:.4f}", f"{reacScale:.4f}", f"{pullScale:.4f}",

            f"{vel:.4f}", f"{uigv:.4f}", f"{macv:.4f}"
        ])

    print_aligned_table(BFS_data, BFS_align)
    print()

    print(f"--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")
    # Velocity
    print(f"=== Detailed Calculations for {repo}: Velocity ===")
    vtab= [["Q-Range","mergesScaled","closedScaled","Velocity=0.4*M+0.6*C"]]
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"
        mergesScale= merges_data[repo].get(q_idx,0.0)* mergesFactor[repo]
        closedScale= closed_data[repo].get(q_idx,0.0)* closedFactor[repo]
        vel= velocity_data[repo].get(q_idx,0.0)
        vtab.append([
            label_str, f"{mergesScale:.4f}", f"{closedScale:.4f}", f"{vel:.4f}"
        ])
    print_aligned_table(vtab, ["left","center","center","center"])
    print()

    # UIG
    print(f"=== Detailed Calculations for {repo}: UIG ===")
    uitab= [["Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"]]
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"
        forksScale= forks_data[repo].get(q_idx,0.0)* forksFactor[repo]
        starsScale= stars_data[repo].get(q_idx,0.0)* starsFactor[repo]
        uigv= uig_data[repo].get(q_idx,0.0)
        uitab.append([label_str, f"{forksScale:.4f}", f"{starsScale:.4f}", f"{uigv:.4f}"])
    print_aligned_table(uitab, ["left","center","center","center"])
    print()

    # MAC
    print(f"=== Detailed Calculations for {repo}: MAC ===")
    mctab= [["Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"]]
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        label_str= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            label_str+= " (partial)"
        issScale= (newIss_data[repo].get(q_idx,0.0)* newIssuesFactor[repo])
        comScale= (comm_data[repo].get(q_idx,0.0)* commentsFactor[repo])
        reaScale= (reac_data[repo].get(q_idx,0.0)* reactionsFactor[repo])
        summ= issScale+ comScale+ reaScale
        pullScale= (pull_data[repo].get(q_idx,0.0)* pullsFactor[repo])
        macv= mac_data[repo].get(q_idx,0.0)
        mctab.append([label_str, f"{summ:.4f}", f"{pullScale:.4f}", f"{macv:.4f}"])
    print_aligned_table(mctab, ["left","center","center","center"])
    print()

def quarter_fy_ranges(fy):
    import datetime
    return {
      "Q1": (datetime.datetime(fy-1,10,1), datetime.datetime(fy-1,12,31,23,59,59)),
      "Q2": (datetime.datetime(fy,1,1), datetime.datetime(fy,3,31,23,59,59)),
      "Q3": (datetime.datetime(fy,4,1), datetime.datetime(fy,6,30,23,59,59)),
      "Q4": (datetime.datetime(fy,7,1), datetime.datetime(fy,9,30,23,59,59)),
    }

def find_fy(d):
    if d.month>=10:
        return d.year+1
    return d.year

def largest_overlap_quarter(dt_start, dt_end):
    import datetime
    fy= find_fy(dt_start)
    Q= quarter_fy_ranges(fy)
    best_label="Q?"
    best_ov= 0
    for qlbl,(qs,qe) in Q.items():
        overlap_start= max(dt_start, qs)
        overlap_end= min(dt_end, qe)
        overlap= (overlap_end - overlap_start).total_seconds()
        if overlap> best_ov:
            best_ov= overlap
            best_label= qlbl
    return best_label

def produce_chart_with_table(
    quarter_ranges,   # list of (start_dt, end_dt, partial_flag)
    bar_values,       # numeric
    scaling_repo,
    all_repos,
    oldest_map,       # {repo: (oldest_date, final_end, partialAny)}
    chart_title,
    filename
):
    # auto font scaling
    base_font= 10
    if len(all_repos)> 10:
        base_font= 8
    if len(all_repos)> 20:
        base_font= 6

    import matplotlib
    matplotlib.rcParams.update({'font.size': base_font})

    import matplotlib.pyplot as plt
    from matplotlib.table import Table
    fig= plt.figure(figsize=(14,8))
    ax_chart= fig.add_axes([0.05,0.1,0.55,0.8])
    ax_table= fig.add_axes([0.65,0.1,0.3,0.8])
    ax_table.set_axis_off()

    x_labels=[]
    for (st,ed,part_f) in quarter_ranges:
        qlbl= largest_overlap_quarter(st,ed)
        x_labels.append(qlbl)

    import numpy as np
    x= np.arange(len(quarter_ranges))
    ax_chart.bar(x, bar_values, 0.6, color='steelblue')
    ax_chart.set_title(chart_title)
    ax_chart.set_xticks(x)
    ax_chart.set_xticklabels(x_labels, rotation=0)

    tbl= Table(ax_table, bbox=[0,0,1,1])
    col_labels= ["Repo","OldestDate","WindowEnd"]
    table_data= [col_labels]

    # top row => scaling
    if scaling_repo in oldest_map:
        (odt, wend, pf)= oldest_map[scaling_repo]
        odt_str= odt.strftime("%Y-%m-%d %H:%M:%S")
        wend_str= wend.strftime("%Y-%m-%d %H:%M:%S")
        if pf:
            wend_str+= " (partial)"
        table_data.append([scaling_repo+" (scaling)", odt_str, wend_str])

    # then => non-scaling
    for rp in all_repos:
        if rp== scaling_repo:
            continue
        if rp not in oldest_map:
            continue
        (odt, wend, pf)= oldest_map[rp]
        odt_str= odt.strftime("%Y-%m-%d %H:%M:%S")
        wend_str= wend.strftime("%Y-%m-%d %H:%M:%S")
        if pf:
            wend_str+= " (partial)"
        table_data.append([rp, odt_str, wend_str])

    nrows= len(table_data)
    ncols= len(table_data[0])
    row_h= 1.0/ nrows
    col_w= 1.0/ ncols

    for irow in range(nrows):
        for icol in range(ncols):
            cell_txt= table_data[irow][icol]
            cell= tbl.add_cell(row=irow, col=icol,
                width=col_w, height=row_h,
                text= cell_txt,
                loc='center',
                facecolor= 'white'
            )
            if irow==0:
                cell.set_facecolor('lightgray')
            # We handle fonts *after* creation
            # to avoid text_props error
            # e.g.:
            if irow==0:
                cell._text.set_weight('bold')
            cell._text.set_fontsize(base_font)

    ax_table.add_table(tbl)
    ax_table.set_xlim(0,1)
    ax_table.set_ylim(0,1)

    fig.savefig(filename)
    plt.close(fig)
    print(f"[INFO] Created {filename} with chart + table: {chart_title}")

def build_ranges_for_repo(r, data_dict, quarter_dates):
    if r not in quarter_dates:
        return [], []
    qkeys= sorted(quarter_dates[r].keys())
    qr=[]
    vals=[]
    for q_idx in qkeys:
        (qs,qe,pf)= quarter_dates[r][q_idx]
        qr.append((qs,qe,pf))
        vals.append(data_dict[r].get(q_idx,0.0))
    return qr, vals

def main():
    env_scaling= os.environ.get("SCALING_REPO","<not set>")
    env_quarters= os.environ.get("NUM_FISCAL_QUARTERS","<not set>")
    print("=== ENVIRONMENT VARIABLES ===")
    print(f"SCALING_REPO={env_scaling}")
    print(f"NUM_FISCAL_QUARTERS={env_quarters}\n")

    print("=== CAPTURED CONSOLE OUTPUT ===\n")

    repos= ["ni/labview-icon-editor","facebook/react","tensorflow/tensorflow","dotnet/core"]
    scaling_repo= get_scaling_repo() or "ni/labview-icon-editor"
    if scaling_repo not in repos:
        repos.append(scaling_repo)

    q_count= get_num_fiscal_quarters() or 4
    aggregator_weights= load_aggregator_weights()
    from scale_factors import compute_scale_factors
    (sfM, sfCl, sfF, sfS, sfNi, sfCo, sfRe, sfP)= compute_scale_factors(scaling_repo, repos)

    now= datetime.utcnow()

    velocity_data={} ; uig_data={} ; mac_data={}
    merges_data={} ; closed_data={} ; forks_data={} ; stars_data={}
    newIss_data={} ; comm_data={} ; reac_data={} ; pull_data={}
    quarter_dates={}
    oldest_map={}

    for r in repos:
        velocity_data[r]= {}
        uig_data[r]= {}
        mac_data[r]= {}
        merges_data[r]= {}
        closed_data[r]= {}
        forks_data[r]= {}
        stars_data[r]= {}
        newIss_data[r]= {}
        comm_data[r]= {}
        reac_data[r]= {}
        pull_data[r]= {}

        oldest= find_oldest_date_for_repo(r)
        if not oldest:
            print(f"[WARN] No data for {r}, skipping BFS aggregator.\n")
            continue

        raw_quarters= generate_quarter_windows(oldest, q_count)
        quarter_dates[r]={}
        idx=1
        final_end= oldest
        any_partial= False

        for (qs,qe) in raw_quarters:
            if qs> now:
                break
            p_flag= False
            if qe> now:
                p_flag= True
                qe= now
            if qs>=qe:
                continue

            mergesRaw= count_merged_pulls(r, qs, qe)
            closedRaw= count_closed_issues(r, qs, qe)
            forksRaw= count_forks(r, qs, qe)
            starsRaw= count_stars(r, qs, qe)
            newIssRaw= count_new_issues(r, qs, qe)
            commRaw= count_issue_comments(r, qs, qe)
            reacRaw= count_all_reactions(r, qs, qe)
            pullRaw= count_new_pulls(r, qs, qe)

            merges_s= mergesRaw* sfM[r]
            closed_s= closedRaw* sfCl[r]
            forks_s= forksRaw* sfF[r]
            stars_s= starsRaw* sfS[r]
            newIss_s= newIssRaw* sfNi[r]
            comm_s= commRaw* sfCo[r]
            reac_s= reacRaw* sfRe[r]
            pull_s= pullRaw* sfP[r]

            # aggregator
            oIss_start= count_open_issues_at_date(r, qs)
            oIss_end= count_open_issues_at_date(r, qe)
            oIss_avg= (oIss_start+ oIss_end)/2
            denom_iss= oIss_avg+ closedRaw
            if denom_iss<1e-9:
                openIssRatio=1.0
            else:
                openIssRatio= oIss_avg/ denom_iss

            oPR_start= count_open_prs_at_date(r, qs)
            oPR_end= count_open_prs_at_date(r, qe)
            oPR_avg= (oPR_start+ oPR_end)/2
            denom_pr= oPR_avg+ mergesRaw
            if denom_pr<1e-9:
                openPRRatio=1.0
            else:
                openPRRatio= oPR_avg/ denom_pr

            vel= velocity(merges_s, closed_s, openIssRatio, openPRRatio, aggregator_weights)
            uigv= user_interest_growth(forks_s, stars_s)
            macv= monthly_active_contributors(newIss_s, comm_s, reac_s, pull_s, aggregator_weights)

            velocity_data[r][idx]= vel
            uig_data[r][idx]= uigv
            mac_data[r][idx]= macv

            merges_data[r][idx]= mergesRaw
            closed_data[r][idx]= closedRaw
            forks_data[r][idx]= forksRaw
            stars_data[r][idx]= starsRaw
            newIss_data[r][idx]= newIssRaw
            comm_data[r][idx]= commRaw
            reac_data[r][idx]= reacRaw
            pull_data[r][idx]= pullRaw

            quarter_dates[r][idx]= (qs,qe,p_flag)
            final_end= qe
            if p_flag:
                any_partial= True

            idx+=1

        oldest_map[r]= (oldest, final_end, any_partial)

    # BFS debug
    for r in repos:
        if r not in quarter_dates:
            continue
        if not quarter_dates[r]:
            continue
        BFS_print_repo_table(
            repo=r,
            mergesFactor= sfM, closedFactor= sfCl, forksFactor= sfF, starsFactor= sfS,
            newIssuesFactor= sfNi, commentsFactor= sfCo, reactionsFactor= sfRe, pullsFactor= sfP,
            merges_data= merges_data, closed_data= closed_data, forks_data= forks_data, stars_data= stars_data,
            newIss_data= newIss_data, comm_data= comm_data, reac_data= reac_data, pull_data= pull_data,
            velocity_data= velocity_data, uig_data= uig_data, mac_data= mac_data,
            quarter_dates= quarter_dates
        )

    print("\n=== Generating separate PNGs for each raw variable. ===\n")

    raw_vars= {
      "Merges": merges_data,
      "Closed": closed_data,
      "Forks": forks_data,
      "Stars": stars_data,
      "NewIssues": newIss_data,
      "Comments": comm_data,
      "Reactions": reac_data,
      "Pulls": pull_data
    }

    def build_ranges_for_r(r, datadict):
        if r not in quarter_dates:
            return [],[]
        qk= sorted(quarter_dates[r].keys())
        quarts=[]
        vals=[]
        for q_idx in qk:
            (qs,qe,pf)= quarter_dates[r][q_idx]
            quarts.append((qs,qe,pf))
            vals.append(datadict[r].get(q_idx,0.0))
        return quarts,vals

    for var_label, var_dict in raw_vars.items():
        qrng, barv= build_ranges_for_r(scaling_repo, var_dict)
        produce_chart_with_table(
          quarter_ranges= qrng,
          bar_values= barv,
          scaling_repo= scaling_repo,
          all_repos= repos,
          oldest_map= oldest_map,
          chart_title= f"{var_label} ({scaling_repo}) - Fiscal Quarters",
          filename= f"{var_label.lower()}_fiscal.png"
        )

    print("\n=== Now generating aggregator (Velocity, MAC, UIG, SEI). ===\n")

    def aggregator_ranges(r, dct):
        if r not in quarter_dates:
            return [],[]
        qk= sorted(quarter_dates[r].keys())
        Qs=[]
        Vs=[]
        for qi in qk:
            (qs,qe,pf)= quarter_dates[r][qi]
            Qs.append((qs,qe,pf))
            Vs.append(dct[r].get(qi,0.0))
        return Qs,Vs

    # velocity
    v_q, v_val= aggregator_ranges(scaling_repo, velocity_data)
    produce_chart_with_table(
      quarter_ranges= v_q,
      bar_values= v_val,
      scaling_repo= scaling_repo,
      all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"Velocity ({scaling_repo}) - Fiscal",
      filename= "velocity_fiscal.png"
    )

    # mac
    m_q, m_val= aggregator_ranges(scaling_repo, mac_data)
    produce_chart_with_table(
      quarter_ranges= m_q,
      bar_values= m_val,
      scaling_repo= scaling_repo,
      all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"MAC ({scaling_repo}) - Fiscal",
      filename= "mac_fiscal.png"
    )

    # uig
    u_q, u_val= aggregator_ranges(scaling_repo, uig_data)
    produce_chart_with_table(
      quarter_ranges= u_q,
      bar_values= u_val,
      scaling_repo= scaling_repo,
      all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"UIG ({scaling_repo}) - Fiscal",
      filename= "uig_fiscal.png"
    )

    # sei
    sei_data={}
    if scaling_repo in velocity_data:
        for q_idx in quarter_dates[scaling_repo]:
            vv= velocity_data[scaling_repo].get(q_idx,0.0)
            uu= uig_data[scaling_repo].get(q_idx,0.0)
            mm= mac_data[scaling_repo].get(q_idx,0.0)
            val= 0.5*mm + 0.3*vv + 0.2*uu
            if scaling_repo not in sei_data:
                sei_data[scaling_repo]= {}
            sei_data[scaling_repo][q_idx]= val

    s_q, s_val= aggregator_ranges(scaling_repo, sei_data)
    produce_chart_with_table(
      quarter_ranges= s_q,
      bar_values= s_val,
      scaling_repo= scaling_repo,
      all_repos= repos,
      oldest_map= oldest_map,
      chart_title= f"SEI ({scaling_repo}) - Fiscal",
      filename= "sei_fiscal.png"
    )

    print("\n=== Done. BFS debug plus raw variable PNGs plus aggregator PNGs. ===")

    sys.stdout.flush()
    console_text= log_capture.getvalue()
    sys.stdout= original_stdout

    debug_file= "debug_log.txt"
    if os.path.exists(debug_file):
        os.remove(debug_file)
    with open(debug_file,"w",encoding="utf-8") as f:
        f.write("=== ENVIRONMENT VARIABLES ===\n")
        f.write(f"SCALING_REPO={env_scaling}\n")
        f.write(f"NUM_FISCAL_QUARTERS={env_quarters}\n\n")
        f.write("=== CAPTURED CONSOLE OUTPUT ===\n")
        f.write(console_text)

    print(f"[INFO] Overwrote debug_log => {debug_file}")

if __name__=="__main__":
    main()
