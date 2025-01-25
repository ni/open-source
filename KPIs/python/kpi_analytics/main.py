#!/usr/bin/env python3
"""
main.py - Master BFS aggregator script with scaled side-by-side charts,
plus aggregator (velocity, mac, uig, sei).
No lines omitted. Uses real queries from analytics/ merges_issues, forks_stars, etc.
Overwrites debug_log.txt with console output each run.
"""

import sys
import os
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# local imports
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
from config import get_scaling_repo, get_num_fiscal_quarters
from aggregator import load_aggregator_weights, velocity, user_interest_growth, monthly_active_contributors, compute_sei
from scale_factors import compute_scale_factors
from baseline import find_oldest_date_for_repo
from quarters import generate_quarter_windows, find_fy, quarter_fy_ranges, largest_overlap_quarter
from merges_issues import (
    count_merged_pulls, count_closed_issues, count_new_pulls, count_new_issues,
    count_open_issues_at_date, count_open_prs_at_date
)
from forks_stars import (
    count_forks, count_stars
)
from comments_reactions import (
    count_issue_comments, count_all_reactions
)

###################### CAPTURE CONSOLE OUTPUT ######################
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
            left= pad//2
            right= pad- left
            return ' '*left+ cell_str+ ' '*right
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

def BFS_print_detailed_calculations(
    repo, quarter_dates,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor
):
    if repo not in quarter_dates:
        return
    sorted_q= sorted(quarter_dates[repo].keys())
    if not sorted_q:
        return

    print(f"\n--- Additional Calculation Details for {repo} (Velocity, UIG, MAC) ---\n")

    # Detailed Velocity
    v_tab= [[
      "Q-Range","mergesScaled","closedScaled","Velocity=0.4*M+0.6*C"
    ]]
    for q_idx in sorted_q:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        lbl= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            lbl+=" (partial)"
        mg= merges_data[repo].get(q_idx,0.0)
        cl= closed_data[repo].get(q_idx,0.0)
        mg_s= mg* mergesFactor[repo]
        cl_s= cl* closedFactor[repo]
        vel= 0.4* mg_s+ 0.6* cl_s
        v_tab.append([lbl, f"{mg_s:.4f}", f"{cl_s:.4f}", f"{vel:.4f}"])
    print_aligned_table(v_tab, ["left","right","right","right"])
    print()

    # Detailed UIG
    uig_tab= [[
      "Q-Range","forksScaled","starsScaled","UIG=0.4*F+0.6*S"
    ]]
    for q_idx in sorted_q:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        lbl= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            lbl+=" (partial)"
        fo= forks_data[repo].get(q_idx,0.0)
        st= stars_data[repo].get(q_idx,0.0)
        fo_s= fo* forksFactor[repo]
        st_s= st* starsFactor[repo]
        ui= 0.4* fo_s + 0.6* st_s
        uig_tab.append([lbl, f"{fo_s:.4f}", f"{st_s:.4f}", f"{ui:.4f}"])
    print_aligned_table(uig_tab, ["left","right","right","right"])
    print()

    # Detailed MAC
    mac_tab= [[
      "Q-Range","(Iss+Comm+React)Scaled","pullScaled","MAC=0.8*(sum)+0.2*pull"
    ]]
    for q_idx in sorted_q:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        lbl= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            lbl+=" (partial)"
        ni= newIss_data[repo].get(q_idx,0.0)
        co= comm_data[repo].get(q_idx,0.0)
        re= reac_data[repo].get(q_idx,0.0)
        pu= pull_data[repo].get(q_idx,0.0)

        ni_s= ni* newIssuesFactor[repo]
        co_s= co* commentsFactor[repo]
        re_s= re* reactionsFactor[repo]
        pu_s= pu* pullsFactor[repo]

        sum_= ni_s+ co_s+ re_s
        ma= 0.8* sum_ + 0.2* pu_s

        mac_tab.append([
          lbl, f"{sum_:.4f}", f"{pu_s:.4f}", f"{ma:.4f}"
        ])
    print_aligned_table(mac_tab, ["left","right","right","right"])
    print()


def BFS_print_repo(
    repo, mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates
):
    print(f"=== BFS for Repo: {repo} ===")
    if repo in mergesFactor:
        print(f"Existing Quarter Data for {repo} | (mergesFactor={mergesFactor[repo]:.4f}, closedFactor={closedFactor[repo]:.4f}, forksFactor={forksFactor[repo]:.4f}, starsFactor={starsFactor[repo]:.4f}, newIssuesFactor={newIssuesFactor[repo]:.4f}, commentsFactor={commentsFactor[repo]:.4f}, reactionsFactor={reactionsFactor[repo]:.4f}, pullsFactor={pullsFactor[repo]:.4f})")
    else:
        print(f"Existing Quarter Data for {repo} | (pseudo-repo, no scale factors)")

    BFS_data= [[
      "Q-Range","mergesRaw","closedRaw","forksRaw","starsRaw",
      "newIssRaw","commentsRaw","reactRaw","pullRaw",
      "mergesScaled","closedScaled","forksScaled","starsScaled",
      "newIssScaled","commentsScaled","reactScaled","pullScaled",
      "Velocity","UIG","MAC"
    ]]
    BFS_align= ["left"]+ ["right"]*19

    if repo not in quarter_dates:
        print(f"[WARN] BFS_print_repo: No quarter_dates for {repo}\n")
        return

    sorted_quarters= sorted(quarter_dates[repo].keys())
    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        lbl= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            lbl+=" (partial)"

        mg= merges_data[repo].get(q_idx,0.0)
        cl= closed_data[repo].get(q_idx,0.0)
        fo= forks_data[repo].get(q_idx,0.0)
        st= stars_data[repo].get(q_idx,0.0)
        ni= newIss_data[repo].get(q_idx,0.0)
        co= comm_data[repo].get(q_idx,0.0)
        re= reac_data[repo].get(q_idx,0.0)
        pu= pull_data[repo].get(q_idx,0.0)

        mg_s= mg* mergesFactor.get(repo,0.0)
        cl_s= cl* closedFactor.get(repo,0.0)
        fo_s= fo* forksFactor.get(repo,0.0)
        st_s= st* starsFactor.get(repo,0.0)
        ni_s= ni* newIssuesFactor.get(repo,0.0)
        co_s= co* commentsFactor.get(repo,0.0)
        re_s= re* reactionsFactor.get(repo,0.0)
        pu_s= pu* pullsFactor.get(repo,0.0)

        vel= 0.4* mg_s + 0.6* cl_s
        ui= 0.4* fo_s + 0.6* st_s
        sum_= ni_s + co_s + re_s
        ma= 0.8* sum_ + 0.2* pu_s

        BFS_data.append([
          lbl,
          f"{mg}", f"{cl}", f"{fo}", f"{st}",
          f"{ni}", f"{co}", f"{re}", f"{pu}",
          f"{mg_s:.4f}", f"{cl_s:.4f}", f"{fo_s:.4f}", f"{st_s:.4f}",
          f"{ni_s:.4f}", f"{co_s:.4f}", f"{re_s:.4f}", f"{pu_s:.4f}",
          f"{vel:.4f}", f"{ui:.4f}", f"{ma:.4f}"
        ])

    print_aligned_table(BFS_data, BFS_align)
    print()

    # aggregator BFS
    aggregator_tab= [[
       "Q-Range","openIssRatio","openPRRatio","Velocity","UIG","MAC","SEI"
    ]]
    aggregator_align= ["left"]+ ["right"]*6

    for q_idx in sorted_quarters:
        (qs,qe,part_flag)= quarter_dates[repo][q_idx]
        lbl= f"Q{q_idx}({qs:%Y-%m-%d}..{qe:%Y-%m-%d})"
        if part_flag:
            lbl+=" (partial)"

        issR= issueRatio_data[repo].get(q_idx,1.0)
        prR= prRatio_data[repo].get(q_idx,1.0)
        ve= velocity_data[repo].get(q_idx,0.0)
        uu= uig_data[repo].get(q_idx,0.0)
        mm= mac_data[repo].get(q_idx,0.0)
        se= sei_data[repo].get(q_idx,0.0)

        aggregator_tab.append([
          lbl, f"{issR:.3f}", f"{prR:.3f}",
          f"{ve:.4f}", f"{uu:.4f}", f"{mm:.4f}", f"{se:.4f}"
        ])

    print_aligned_table(aggregator_tab, aggregator_align)
    print("------------------------------------------------------")

    # BFS_print_detailed_calculations
    BFS_print_detailed_calculations(
      repo=repo,
      quarter_dates= quarter_dates,
      merges_data= merges_data,
      closed_data= closed_data,
      forks_data= forks_data,
      stars_data= stars_data,
      newIss_data= newIss_data,
      comm_data= comm_data,
      reac_data= reac_data,
      pull_data= pull_data,
      mergesFactor= mergesFactor,
      closedFactor= closedFactor,
      forksFactor= forksFactor,
      starsFactor= starsFactor,
      newIssuesFactor= newIssuesFactor,
      commentsFactor= commentsFactor,
      reactionsFactor= reactionsFactor,
      pullsFactor= pullsFactor
    )
    print()

def compute_non_scaling_aggregator_target(
    scaling_repo, all_repos,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates
):
    tname= "TARGET(avgNonScaling)"
    velocity_data[tname]= {}
    uig_data[tname]= {}
    mac_data[tname]= {}
    sei_data[tname]= {}
    issueRatio_data[tname]= {}
    prRatio_data[tname]= {}

    union_q= set()
    non_scalers= [r for r in all_repos if r!= scaling_repo]
    for nr in non_scalers:
        if nr in quarter_dates:
            union_q.update(quarter_dates[nr].keys())
    union_q= sorted(union_q)

    quarter_dates[tname]= {}

    for q_idx in union_q:
        sum_vel=0.0; sum_uig=0.0; sum_mac=0.0; sum_sei=0.0
        sum_oi=0.0; sum_pr=0.0
        ccount=0
        partial_any=False
        st_list=[]; ed_list=[]
        for nr in non_scalers:
            if nr not in quarter_dates:
                continue
            if q_idx in quarter_dates[nr]:
                (qs,qe,pf)= quarter_dates[nr][q_idx]
                st_list.append(qs)
                ed_list.append(qe)
                if pf: partial_any= True

                val_v= velocity_data[nr].get(q_idx,0.0)
                val_u= uig_data[nr].get(q_idx,0.0)
                val_m= mac_data[nr].get(q_idx,0.0)
                val_s= sei_data[nr].get(q_idx,0.0)
                val_oi= issueRatio_data[nr].get(q_idx,1.0)
                val_pr= prRatio_data[nr].get(q_idx,1.0)

                sum_vel+= val_v
                sum_uig+= val_u
                sum_mac+= val_m
                sum_sei+= val_s
                sum_oi+= val_oi
                sum_pr+= val_pr
                ccount+=1
        if ccount>0 and st_list and ed_list:
            avg_vel= sum_vel/ ccount
            avg_uig= sum_uig/ ccount
            avg_mac= sum_mac/ ccount
            avg_sei= sum_sei/ ccount
            avg_oi= sum_oi/ ccount
            avg_pr_= sum_pr/ ccount

            sdt= min(st_list)
            edt= max(ed_list)
            quarter_dates[tname][q_idx]= (sdt,edt,partial_any)
            velocity_data[tname][q_idx]= avg_vel
            uig_data[tname][q_idx]= avg_uig
            mac_data[tname][q_idx]= avg_mac
            sei_data[tname][q_idx]= avg_sei
            issueRatio_data[tname][q_idx]= avg_oi
            prRatio_data[tname][q_idx]= avg_pr_
    return tname

def compute_non_scaling_scaled_target(
    scaling_repo, all_repos,
    merges_data, closed_data, forks_data, stars_data,
    newIss_data, comm_data, reac_data, pull_data,
    mergesFactor, closedFactor, forksFactor, starsFactor,
    newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
    quarter_dates
):
    tname= "TARGETscaled(avgNonScaling)"
    merges_data[tname]= {}
    closed_data[tname]= {}
    forks_data[tname]= {}
    stars_data[tname]= {}
    newIss_data[tname]= {}
    comm_data[tname]= {}
    reac_data[tname]= {}
    pull_data[tname]= {}

    quarter_dates[tname]= {}

    non_scalers= [r for r in all_repos if r!= scaling_repo]
    union_q= set()
    for nr in non_scalers:
        if nr in quarter_dates:
            union_q.update(quarter_dates[nr].keys())
    union_q= sorted(union_q)

    for q_idx in union_q:
        sum_m=0.0; sum_c=0.0; sum_f=0.0; sum_s=0.0
        sum_ni=0.0; sum_co=0.0; sum_re=0.0; sum_pu=0.0
        ccount=0
        partial_any=False
        st_list=[]; ed_list=[]
        for nr in non_scalers:
            if nr not in quarter_dates:
                continue
            if q_idx in quarter_dates[nr]:
                (qs,qe,pf)= quarter_dates[nr][q_idx]
                st_list.append(qs)
                ed_list.append(qe)
                if pf: partial_any= True

                mg= merges_data[nr].get(q_idx,0.0)
                cl= closed_data[nr].get(q_idx,0.0)
                fo= forks_data[nr].get(q_idx,0.0)
                stv= stars_data[nr].get(q_idx,0.0)
                ni= newIss_data[nr].get(q_idx,0.0)
                co= comm_data[nr].get(q_idx,0.0)
                re= reac_data[nr].get(q_idx,0.0)
                pu= pull_data[nr].get(q_idx,0.0)

                mg_s= mg* mergesFactor[nr]
                cl_s= cl* closedFactor[nr]
                fo_s= fo* forksFactor[nr]
                st_s= stv* starsFactor[nr]
                ni_s= ni* newIssuesFactor[nr]
                co_s= co* commentsFactor[nr]
                re_s= re* reactionsFactor[nr]
                pu_s= pu* pullsFactor[nr]

                sum_m+= mg_s
                sum_c+= cl_s
                sum_f+= fo_s
                sum_s+= st_s
                sum_ni+= ni_s
                sum_co+= co_s
                sum_re+= re_s
                sum_pu+= pu_s
                ccount+=1
        if ccount>0 and st_list and ed_list:
            avg_m= sum_m/ ccount
            avg_c= sum_c/ ccount
            avg_f= sum_f/ ccount
            avg_s= sum_s/ ccount
            avg_ni= sum_ni/ ccount
            avg_co= sum_co/ ccount
            avg_re= sum_re/ ccount
            avg_pu= sum_pu/ ccount

            sdt= min(st_list)
            edt= max(ed_list)
            quarter_dates[tname][q_idx]= (sdt,edt,partial_any)

            merges_data[tname][q_idx]= avg_m
            closed_data[tname][q_idx]= avg_c
            forks_data[tname][q_idx]= avg_f
            stars_data[tname][q_idx]= avg_s
            newIss_data[tname][q_idx]= avg_ni
            comm_data[tname][q_idx]= avg_co
            reac_data[tname][q_idx]= avg_re
            pull_data[tname][q_idx]= avg_pu

    return tname

def produce_side_by_side_chart(
    metric_label, scaling_repo, target_repo,
    data_dict,
    quarter_dates, all_repos, oldest_map,
    filename
):
    import matplotlib.pyplot as plt

    qset= set()
    if scaling_repo in quarter_dates:
        qset.update(quarter_dates[scaling_repo].keys())
    if target_repo in quarter_dates:
        qset.update(quarter_dates[target_repo].keys())
    sorted_q= sorted(qset)

    scaling_vals=[]
    target_vals=[]
    labels=[]
    from datetime import datetime

    for q_idx in sorted_q:
        sc= data_dict[scaling_repo].get(q_idx,0.0) if scaling_repo in data_dict else 0.0
        tg= data_dict[target_repo].get(q_idx,0.0) if target_repo in data_dict else 0.0
        sp=False
        if scaling_repo in quarter_dates and q_idx in quarter_dates[scaling_repo]:
            (_,_,pf)= quarter_dates[scaling_repo][q_idx]
            if pf: sp= True
        if target_repo in quarter_dates and q_idx in quarter_dates[target_repo]:
            (_,_,pf2)= quarter_dates[target_repo][q_idx]
            if pf2: sp= True

        st_list=[]; ed_list=[]
        if scaling_repo in quarter_dates and q_idx in quarter_dates[scaling_repo]:
            (sqs,sqe,_)= quarter_dates[scaling_repo][q_idx]
            st_list.append(sqs)
            ed_list.append(sqe)
        if target_repo in quarter_dates and q_idx in quarter_dates[target_repo]:
            (tqs,tqe,_)= quarter_dates[target_repo][q_idx]
            st_list.append(tqs)
            ed_list.append(tqe)
        if st_list and ed_list:
            st= min(st_list)
            ed= max(ed_list)
        else:
            st= datetime(2000,1,1)
            ed= datetime(2000,1,1)

        from quarters import largest_overlap_quarter
        lbl= largest_overlap_quarter(st,ed)
        if sp:
            lbl+="(partial)"

        scaling_vals.append(sc)
        target_vals.append(tg)
        labels.append(lbl)

    import numpy as np
    x= np.arange(len(sorted_q))
    width= 0.35

    fig= plt.figure(figsize=(12,8))
    ax= fig.add_axes([0.1,0.3,0.8,0.65])
    ax.set_title(f"{metric_label}: {scaling_repo} vs. {target_repo}")
    bar_s= ax.bar(x - width/2, scaling_vals, width, label=scaling_repo, color='steelblue')
    bar_t= ax.bar(x + width/2, target_vals, width, label=target_repo, color='orange')

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    for i, rect in enumerate(bar_s):
        scv= scaling_vals[i]
        tgv= target_vals[i]
        if abs(tgv)<1e-9:
            ratio_str="N/A"
        else:
            ratio= (scv/tgv)*100.0
            ratio_str= f"{ratio:.2f}%"
        ht= rect.get_height()
        pad= max(0.05*ht, 0.1)
        ax.text(rect.get_x()+ rect.get_width()/2, ht+ pad,
                ratio_str, ha='center', va='bottom', fontsize=9)

    ax_table= fig.add_axes([0.1,0.05,0.8,0.2])
    ax_table.set_axis_off()
    from matplotlib.table import Table
    tbl= Table(ax_table, bbox=[0,0,1,1])
    col_labels= ["Repo","OldestDate","WindowEnd"]
    table_data= [col_labels]

    # scaling
    if scaling_repo in oldest_map:
        (odt,wend,pf)= oldest_map[scaling_repo]
        od_str= odt.strftime("%Y-%m-%d %H:%M")
        we_str= wend.strftime("%Y-%m-%d %H:%M")
        if pf: we_str+="(partial)"
        table_data.append([scaling_repo, od_str, we_str])
    for rp in all_repos:
        if rp==scaling_repo:
            continue
        if rp not in oldest_map:
            continue
        (odt,wend,pp)= oldest_map[rp]
        od_str= odt.strftime("%Y-%m-%d %H:%M")
        we_str= wend.strftime("%Y-%m-%d %H:%M")
        if pp: we_str+="(partial)"
        table_data.append([rp, od_str, we_str])

    nrows= len(table_data)
    ncols= len(table_data[0])
    row_h= 1.0/nrows
    col_w= 1.0/ncols
    for irow in range(nrows):
        for icol in range(ncols):
            cval= table_data[irow][icol]
            cell= tbl.add_cell(irow,icol,
                width= col_w, height= row_h,
                text=cval, loc='center', facecolor='white'
            )
            if irow==0:
                cell.set_facecolor('lightgray')
                cell._text.set_weight('bold')
            cell._text.set_fontsize(9)
    ax_table.add_table(tbl)
    ax_table.set_xlim(0,1)
    ax_table.set_ylim(0,1)

    fig.savefig(filename)
    plt.close(fig)
    print(f"[INFO] Created {filename}")

def main():
    print("=== ENVIRONMENT VARIABLES ===")
    print(f"SCALING_REPO={os.environ.get('SCALING_REPO','<not set>')}")
    print(f"NUM_FISCAL_QUARTERS={os.environ.get('NUM_FISCAL_QUARTERS','<not set>')}\n")
    print("=== CAPTURED CONSOLE OUTPUT ===\n")

    repos= ["ni/labview-icon-editor","facebook/react","tensorflow/tensorflow","dotnet/core"]
    scaling_repo= get_scaling_repo()
    if scaling_repo not in repos:
        repos.append(scaling_repo)
    q_count= get_num_fiscal_quarters()

    # aggregator config
    aggregator_cfg= load_aggregator_weights()

    # scale_factors
    (sfM,sfCl,sfF,sfS,sfNi,sfCo,sfRe,sfP)= compute_scale_factors(scaling_repo, repos)

    # aggregator storages
    velocity_data={} ; uig_data={} ; mac_data={} ; sei_data={}
    issueRatio_data={} ; prRatio_data={}

    merges_data={} ; closed_data={} ; forks_data={} ; stars_data={}
    newIss_data={} ; comm_data={} ; reac_data={} ; pull_data={}

    quarter_dates={}
    oldest_map={}

    now= datetime.utcnow()

    # BFS aggregator fill
    for r in repos:
        velocity_data[r]= {}
        uig_data[r]= {}
        mac_data[r]= {}
        sei_data[r]= {}
        issueRatio_data[r]= {}
        prRatio_data[r]= {}

        merges_data[r]= {}
        closed_data[r]= {}
        forks_data[r]= {}
        stars_data[r]= {}
        newIss_data[r]= {}
        comm_data[r]= {}
        reac_data[r]= {}
        pull_data[r]= {}

        odt= find_oldest_date_for_repo(r)
        if not odt:
            print(f"[WARN] No data => {r}")
            continue
        quarter_dates[r]={}
        final_end= odt
        partial_any=False
        # generate q_count windows
        local_q=[]
        curr= odt
        for _ in range(q_count):
            ed= curr+ relativedelta(months=3)
            local_q.append((curr,ed))
            curr= ed

        idx=1
        for (qs,qe) in local_q:
            if qs> now:
                break
            pf= False
            if qe> now:
                pf= True
                qe= now
            if qs>=qe:
                continue

            mg= count_merged_pulls(r, qs, qe)
            cl= count_closed_issues(r, qs, qe)
            fo= count_forks(r, qs, qe)
            st= count_stars(r, qs, qe)
            ni= count_new_issues(r, qs, qe)
            co= count_issue_comments(r, qs, qe)
            re= count_all_reactions(r, qs, qe)
            pu= count_new_pulls(r, qs, qe)

            merges_data[r][idx]= mg
            closed_data[r][idx]= cl
            forks_data[r][idx]= fo
            stars_data[r][idx]= st
            newIss_data[r][idx]= ni
            comm_data[r][idx]= co
            reac_data[r][idx]= re
            pull_data[r][idx]= pu

            mg_s= mg* sfM[r]
            cl_s= cl* sfCl[r]
            fo_s= fo* sfF[r]
            st_s= st* sfS[r]
            ni_s= ni* sfNi[r]
            co_s= co* sfCo[r]
            re_s= re* sfRe[r]
            pu_s= pu* sfP[r]

            # open ratio
            oiss_st= count_open_issues_at_date(r, qs)
            oiss_en= count_open_issues_at_date(r, qe)
            oiss_avg= (oiss_st+ oiss_en)/2
            denom_iss= oiss_avg+ cl
            if denom_iss<1e-9:
                issRatio= 1.0
            else:
                issRatio= oiss_avg/ denom_iss

            opr_st= count_open_prs_at_date(r, qs)
            opr_en= count_open_prs_at_date(r, qe)
            opr_avg= (opr_st+ opr_en)/2
            denom_pr= opr_avg+ mg
            if denom_pr<1e-9:
                prRat=1.0
            else:
                prRat= opr_avg/ mg if mg else 1.0
                # or prRat= opr_avg/ denom_pr if you prefer

            vel= 0.4* mg_s + 0.6* cl_s
            ui= 0.4* fo_s + 0.6* st_s
            sum_= ni_s+ co_s+ re_s
            ma= 0.8* sum_ + 0.2* pu_s
            se= compute_sei(vel, ui, ma)

            velocity_data[r][idx]= vel
            uig_data[r][idx]= ui
            mac_data[r][idx]= ma
            sei_data[r][idx]= se
            issueRatio_data[r][idx]= issRatio
            prRatio_data[r][idx]= prRat

            quarter_dates[r][idx]= (qs,qe,pf)
            final_end= qe
            if pf: partial_any= True

            idx+=1
        oldest_map[r]= (odt, final_end, partial_any)

    # BFS aggregator prints
    for r in repos:
        if r not in quarter_dates or not quarter_dates[r]:
            continue
        BFS_print_repo(
          repo=r,
          mergesFactor= sfM, closedFactor= sfCl, forksFactor= sfF, starsFactor= sfS,
          newIssuesFactor= sfNi, commentsFactor= sfCo, reactionsFactor= sfRe, pullsFactor= sfP,
          merges_data= merges_data, closed_data= closed_data, forks_data= forks_data, stars_data= stars_data,
          newIss_data= newIss_data, comm_data= comm_data, reac_data= reac_data, pull_data= pull_data,
          velocity_data= velocity_data, uig_data= uig_data, mac_data= mac_data, sei_data= sei_data,
          issueRatio_data= issueRatio_data, prRatio_data= prRatio_data,
          quarter_dates= quarter_dates
        )

    # aggregator target => velocity, uig, mac, sei => "TARGET(avgNonScaling)"
    def compute_non_scaling_aggregator_target(
        scaling_repo, all_repos,
        velocity_data, uig_data, mac_data, sei_data,
        issueRatio_data, prRatio_data,
        quarter_dates
    ):
        tname= "TARGET(avgNonScaling)"
        velocity_data[tname]= {}
        uig_data[tname]= {}
        mac_data[tname]= {}
        sei_data[tname]= {}
        issueRatio_data[tname]= {}
        prRatio_data[tname]= {}

        non_scalers= [r for r in all_repos if r!= scaling_repo]
        union_q= set()
        for nr in non_scalers:
            if nr in quarter_dates:
                union_q.update(quarter_dates[nr].keys())
        union_q= sorted(union_q)
        quarter_dates[tname]= {}

        for q_idx in union_q:
            sum_vel=0.0; sum_uig=0.0; sum_mac=0.0; sum_sei=0.0
            sum_oi=0.0; sum_pr=0.0
            ccount=0
            partial_any=False
            st_list=[]; ed_list=[]
            for nr in non_scalers:
                if nr not in quarter_dates:
                    continue
                if q_idx in quarter_dates[nr]:
                    (qs,qe,pf)= quarter_dates[nr][q_idx]
                    st_list.append(qs)
                    ed_list.append(qe)
                    if pf: partial_any= True
                    v_= velocity_data[nr].get(q_idx,0.0)
                    u_= uig_data[nr].get(q_idx,0.0)
                    m_= mac_data[nr].get(q_idx,0.0)
                    s_= sei_data[nr].get(q_idx,0.0)
                    oi= issueRatio_data[nr].get(q_idx,1.0)
                    pr= prRatio_data[nr].get(q_idx,1.0)

                    sum_vel+= v_
                    sum_uig+= u_
                    sum_mac+= m_
                    sum_sei+= s_
                    sum_oi+= oi
                    sum_pr+= pr
                    ccount+=1
            if ccount>0 and st_list and ed_list:
                avg_vel= sum_vel/ ccount
                avg_uig= sum_uig/ ccount
                avg_mac= sum_mac/ ccount
                avg_sei= sum_sei/ ccount
                avg_oi= sum_oi/ ccount
                avg_pr_= sum_pr/ ccount

                sdt= min(st_list)
                edt= max(ed_list)
                quarter_dates[tname][q_idx]= (sdt,edt,partial_any)

                velocity_data[tname][q_idx]= avg_vel
                uig_data[tname][q_idx]= avg_uig
                mac_data[tname][q_idx]= avg_mac
                sei_data[tname][q_idx]= avg_sei
                issueRatio_data[tname][q_idx]= avg_oi
                prRatio_data[tname][q_idx]= avg_pr_
        return tname

    aggregator_t= compute_non_scaling_aggregator_target(
        scaling_repo, repos,
        velocity_data, uig_data, mac_data, sei_data,
        issueRatio_data, prRatio_data, quarter_dates
    )

    def compute_non_scaling_scaled_target(
        scaling_repo, all_repos,
        merges_data, closed_data, forks_data, stars_data,
        newIss_data, comm_data, reac_data, pull_data,
        mergesFactor, closedFactor, forksFactor, starsFactor,
        newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor,
        quarter_dates
    ):
        tname= "TARGETscaled(avgNonScaling)"
        merges_data[tname]= {}
        closed_data[tname]= {}
        forks_data[tname]= {}
        stars_data[tname]= {}
        newIss_data[tname]= {}
        comm_data[tname]= {}
        reac_data[tname]= {}
        pull_data[tname]= {}

        quarter_dates[tname]= {}

        non_scalers= [r for r in all_repos if r!= scaling_repo]
        union_q= set()
        for nr in non_scalers:
            if nr in quarter_dates:
                union_q.update(quarter_dates[nr].keys())
        union_q= sorted(union_q)

        for q_idx in union_q:
            sum_m=0; sum_c=0; sum_f=0; sum_s=0
            sum_ni=0; sum_co=0; sum_re=0; sum_pu=0
            ccount=0
            partial_any=False
            st_list=[]; ed_list=[]
            for nr in non_scalers:
                if nr not in quarter_dates:
                    continue
                if q_idx in quarter_dates[nr]:
                    (qs,qe,pf)= quarter_dates[nr][q_idx]
                    st_list.append(qs)
                    ed_list.append(qe)
                    if pf: partial_any= True
                    mg= merges_data[nr].get(q_idx,0.0)
                    cl= closed_data[nr].get(q_idx,0.0)
                    fo= forks_data[nr].get(q_idx,0.0)
                    stv= stars_data[nr].get(q_idx,0.0)
                    ni= newIss_data[nr].get(q_idx,0.0)
                    co= comm_data[nr].get(q_idx,0.0)
                    re= reac_data[nr].get(q_idx,0.0)
                    pu= pull_data[nr].get(q_idx,0.0)

                    mg_s= mg* mergesFactor[nr]
                    cl_s= cl* closedFactor[nr]
                    fo_s= fo* forksFactor[nr]
                    st_s= stv* starsFactor[nr]
                    ni_s= ni* newIssuesFactor[nr]
                    co_s= co* commentsFactor[nr]
                    re_s= re* reactionsFactor[nr]
                    pu_s= pu* pullsFactor[nr]

                    sum_m+= mg_s
                    sum_c+= cl_s
                    sum_f+= fo_s
                    sum_s+= st_s
                    sum_ni+= ni_s
                    sum_co+= co_s
                    sum_re+= re_s
                    sum_pu+= pu_s
                    ccount+=1

            if ccount>0 and st_list and ed_list:
                avg_m= sum_m/ ccount
                avg_c= sum_c/ ccount
                avg_f= sum_f/ ccount
                avg_s= sum_s/ ccount
                avg_ni= sum_ni/ ccount
                avg_co= sum_co/ ccount
                avg_re= sum_re/ ccount
                avg_pu= sum_pu/ ccount

                sdt= min(st_list)
                edt= max(ed_list)
                quarter_dates[tname][q_idx]= (sdt,edt,partial_any)

                merges_data[tname][q_idx]= avg_m
                closed_data[tname][q_idx]= avg_c
                forks_data[tname][q_idx]= avg_f
                stars_data[tname][q_idx]= avg_s
                newIss_data[tname][q_idx]= avg_ni
                comm_data[tname][q_idx]= avg_co
                reac_data[tname][q_idx]= avg_re
                pull_data[tname][q_idx]= avg_pu

        return tname

    scaled_t= compute_non_scaling_scaled_target(
        scaling_repo, repos,
        merges_data, closed_data, forks_data, stars_data,
        newIss_data, comm_data, reac_data, pull_data,
        sfM, sfCl, sfF, sfS, sfNi, sfCo, sfRe, sfP,
        quarter_dates
    )

    print("\n=== BFS aggregator done. Now produce side-by-side scaled charts. ===\n")

    # side-by-side scaled merges,closed,forks,stars,newIssues,comments,reactions,pulls
    scaled_vars= {
      "Merges": merges_data,
      "Closed": closed_data,
      "Forks": forks_data,
      "Stars": stars_data,
      "NewIssues": newIss_data,
      "Comments": comm_data,
      "Reactions": reac_data,
      "Pulls": pull_data
    }
    for lbl, ddict in scaled_vars.items():
        fname= f"{lbl.lower()}_scaled.png"
        produce_side_by_side_chart(
          metric_label= f"{lbl} (Scaled)",
          scaling_repo= scaling_repo,
          target_repo= scaled_t,  # "TARGETscaled(avgNonScaling)"
          data_dict= ddict,
          quarter_dates= quarter_dates,
          all_repos= repos,
          oldest_map= oldest_map,
          filename= fname
        )

    aggregator_metrics= {
       "Velocity": velocity_data,
       "MAC": mac_data,
       "UIG": uig_data,
       "SEI": sei_data
    }
    for lbl, dd in aggregator_metrics.items():
        fname= f"{lbl.lower()}_compare.png"
        produce_side_by_side_chart(
          metric_label= lbl,
          scaling_repo= scaling_repo,
          target_repo= aggregator_t,  # "TARGET(avgNonScaling)"
          data_dict= dd,
          quarter_dates= quarter_dates,
          all_repos= repos,
          oldest_map= oldest_map,
          filename= fname
        )

    print("\n=== Done. BFS aggregator + side-by-side scaled charts. ===")

    sys.stdout.flush()
    console_out= log_capture.getvalue()
    sys.stdout= original_stdout

    debug_file= "debug_log.txt"
    if os.path.exists(debug_file):
        os.remove(debug_file)
    with open(debug_file,"w",encoding="utf-8") as f:
        f.write(console_out)

    print(f"[INFO] Overwrote debug_log => {debug_file}")

#####################################
# aggregator target
#####################################
def compute_non_scaling_aggregator_target(
    scaling_repo, all_repos,
    velocity_data, uig_data, mac_data, sei_data,
    issueRatio_data, prRatio_data,
    quarter_dates
):
    """
    Summation of aggregator among non-scaling => average => store in 
    'TARGET(avgNonScaling)' for velocity, uig, mac, sei, openIssRatio, openPRRatio.
    """
    tname= "TARGET(avgNonScaling)"
    velocity_data[tname]= {}
    uig_data[tname]= {}
    mac_data[tname]= {}
    sei_data[tname]= {}
    issueRatio_data[tname]= {}
    prRatio_data[tname]= {}

    quarter_dates[tname]= {}

    union_q= set()
    non_scalers= [r for r in all_repos if r!= scaling_repo]
    for nr in non_scalers:
        if nr in quarter_dates:
            union_q.update(quarter_dates[nr].keys())
    union_q= sorted(union_q)

    for q_idx in union_q:
        sum_vel=0.0; sum_uig=0.0; sum_mac=0.0; sum_sei=0.0
        sum_oi=0.0; sum_pr=0.0
        ccount=0
        partial_any=False
        st_list=[]; ed_list=[]
        for nr in non_scalers:
            if nr not in quarter_dates:
                continue
            if q_idx in quarter_dates[nr]:
                (qs,qe,pf)= quarter_dates[nr][q_idx]
                st_list.append(qs)
                ed_list.append(qe)
                if pf: partial_any= True
                val_v= velocity_data[nr].get(q_idx,0.0)
                val_u= uig_data[nr].get(q_idx,0.0)
                val_m= mac_data[nr].get(q_idx,0.0)
                val_s= sei_data[nr].get(q_idx,0.0)
                val_oi= issueRatio_data[nr].get(q_idx,1.0)
                val_pr= prRatio_data[nr].get(q_idx,1.0)
                sum_vel+= val_v
                sum_uig+= val_u
                sum_mac+= val_m
                sum_sei+= val_s
                sum_oi+= val_oi
                sum_pr+= val_pr
                ccount+=1
        if ccount>0 and st_list and ed_list:
            avg_vel= sum_vel/ccount
            avg_uig= sum_uig/ccount
            avg_mac= sum_mac/ccount
            avg_sei= sum_sei/ccount
            avg_oi= sum_oi/ccount
            avg_pr_= sum_pr/ccount

            sdt= min(st_list)
            edt= max(ed_list)
            quarter_dates[tname][q_idx]= (sdt, edt, partial_any)
            velocity_data[tname][q_idx]= avg_vel
            uig_data[tname][q_idx]= avg_uig
            mac_data[tname][q_idx]= avg_mac
            sei_data[tname][q_idx]= avg_sei
            issueRatio_data[tname][q_idx]= avg_oi
            prRatio_data[tname][q_idx]= avg_pr_

    return tname

if __name__=="__main__":
    main()
