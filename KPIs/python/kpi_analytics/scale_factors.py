#!/usr/bin/env python3
"""
scale_factors.py

Computes the 8 scale factors by comparing sums in [oldest_date..(oldest_date+windowDays)]
for the scaling repo vs. other repos. Then merges them into mergesFactor, closedFactor, etc.

We also define:
  - compute_target_reached_data(...) => used by produce_raw_comparison_chart
  - compute_sei_data(...) => used by produce_sei_comparison_chart

Additionally prints out each repo's oldest_date + that window so you see
the exact time range for scale factor summation.
"""

import yaml
import os
from datetime import datetime, timedelta

from baseline import find_oldest_date_for_repo
from merges_issues import (
    count_merged_pulls, count_closed_issues,
    count_new_pulls, count_new_issues
)
from forks_stars import count_forks, count_stars
from comments_reactions import count_issue_comments, count_all_reactions

FALLBACK_SCALE_WINDOW= 290

def read_scale_window_days():
    config_file= "config.yaml"
    if not os.path.exists(config_file):
        print(f"[WARN] scale_factors => {config_file} missing => fallback={FALLBACK_SCALE_WINDOW}")
        return FALLBACK_SCALE_WINDOW
    with open(config_file,"r",encoding="utf-8") as f:
        data= yaml.safe_load(f) or {}
    sc_data= data.get("scaleFactors", {})
    return sc_data.get("windowDays", FALLBACK_SCALE_WINDOW)

def compute_scale_factors(scaling_repo, all_repos):
    """
    Summation => factor= scaleSum / repoSum
    merges, closed, forks, stars, newIss, comments, reac, pulls => 8 dicts

    Prints out:
     - scaling_oldest_date
     - (scaling_oldest_date + scale_factor_window)
     and similarly for each other repo.
    """
    window_days= read_scale_window_days()

    mergesFactor={}
    closedFactor={}
    forksFactor={}
    starsFactor={}
    newIssuesFactor={}
    commentsFactor={}
    reactionsFactor={}
    pullsFactor={}

    def sum_all(r, st, ed):
        m= count_merged_pulls(r, st, ed)
        c= count_closed_issues(r, st, ed)
        f= count_forks(r, st, ed)
        s= count_stars(r, st, ed)
        ni= count_new_issues(r, st, ed)
        co= count_issue_comments(r, st, ed)
        re= count_all_reactions(r, st, ed)
        pu= count_new_pulls(r, st, ed)
        return (m,c,f,s,ni,co,re,pu)

    def fallback_ones(rrs):
        for rr in rrs:
            mergesFactor[rr]=1.0
            closedFactor[rr]=1.0
            forksFactor[rr]=1.0
            starsFactor[rr]=1.0
            newIssuesFactor[rr]=1.0
            commentsFactor[rr]=1.0
            reactionsFactor[rr]=1.0
            pullsFactor[rr]=1.0

    scaling_old= find_oldest_date_for_repo(scaling_repo)
    if not scaling_old:
        print(f"[INFO] scaling repo '{scaling_repo}': no data => set scale factors=1.0 for all.")
        fallback_ones(all_repos)
        return (mergesFactor, closedFactor, forksFactor, starsFactor,
                newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor)

    scaling_end= scaling_old+ timedelta(days=window_days)
    print(f"[INFO] scale_factors: (scaling repo) {scaling_repo}"
          f" => oldest_date={scaling_old}, window_end={scaling_end}"
          f" (window={window_days} days)")

    (m_s, c_s, f_s, st_s, ni_s, co_s, re_s, pu_s)= sum_all(scaling_repo, scaling_old, scaling_end)

    def ratio_func(sc_val, rp_val):
        if sc_val>0 and rp_val==0:
            return 0.0
        elif sc_val==0 and rp_val>0:
            return 0.0
        elif sc_val==0 and rp_val==0:
            return 1.0
        else:
            return float(sc_val)/ float(rp_val)

    for rr in all_repos:
        if rr==scaling_repo:
            mergesFactor[rr]=1.0
            closedFactor[rr]=1.0
            forksFactor[rr]=1.0
            starsFactor[rr]=1.0
            newIssuesFactor[rr]=1.0
            commentsFactor[rr]=1.0
            reactionsFactor[rr]=1.0
            pullsFactor[rr]=1.0
            continue

        rold= find_oldest_date_for_repo(rr)
        if not rold:
            print(f"[INFO] other repo '{rr}': no data => set scale factors=1.0")
            mergesFactor[rr]=1.0
            closedFactor[rr]=1.0
            forksFactor[rr]=1.0
            starsFactor[rr]=1.0
            newIssuesFactor[rr]=1.0
            commentsFactor[rr]=1.0
            reactionsFactor[rr]=1.0
            pullsFactor[rr]=1.0
            continue

        rend= rold+ timedelta(days=window_days)
        print(f"[INFO] scale_factors: (other repo) {rr}"
              f" => oldest_date={rold}, window_end={rend}"
              f" (window={window_days} days)")

        (mr,cr,fr,st_r,ni_r,co_r,re_r,pu_r)= sum_all(rr, rold, rend)

        mergesFactor[rr]= ratio_func(m_s,mr)
        closedFactor[rr]= ratio_func(c_s,cr)
        forksFactor[rr]= ratio_func(f_s,fr)
        starsFactor[rr]= ratio_func(st_s,st_r)
        newIssuesFactor[rr]= ratio_func(ni_s,ni_r)
        commentsFactor[rr]= ratio_func(co_s,co_r)
        reactionsFactor[rr]= ratio_func(re_s,re_r)
        pullsFactor[rr]= ratio_func(pu_s,pu_r)

    return (mergesFactor, closedFactor, forksFactor, starsFactor,
            newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor)

def compute_target_reached_data(repos, scaling_repo, quarter_data_dict):
    """
    Summaries for raw comparison charts:
      - we gather average among non-scaling repos => "target"
      - scaling repo's value => sc_val
      - ratio => (sc_val / target)*100 if target>0

    Returns a dict: { q_idx: (targetVal, scalingVal, ratioPercent) }
    """
    target_data={}
    union_q= set()
    for r in repos:
        union_q |= set(quarter_data_dict[r].keys())
    union_q= sorted(union_q)
    non_scaling= [rr for rr in repos if rr!= scaling_repo]

    for q_idx in union_q:
        sum_v=0.0
        ccount=0
        for nr in non_scaling:
            if q_idx in quarter_data_dict[nr]:
                sum_v+= quarter_data_dict[nr][q_idx]
                ccount+=1
        avg_v=0.0
        if ccount>0:
            avg_v= sum_v/ ccount
        sc_val= quarter_data_dict[scaling_repo].get(q_idx,0.0)
        ratio_val=0.0
        if abs(avg_v)>1e-9:
            ratio_val= (sc_val/ avg_v)*100.0
        target_data[q_idx]= (avg_v, sc_val, ratio_val)

    return target_data

def compute_sei_data(velocity_tr, uig_tr, mac_tr):
    """
    used by produce_sei_comparison_chart in main.py
    velocity_tr => {q_idx: (vTarget, vScaling, vRatio)}
    uig_tr      => likewise
    mac_tr      => likewise

    We unify them => SEI ratio= 0.3*(vRatio) + 0.2*(uigRatio) + 0.5*(macRatio)
    or fallback if missing.

    Return {q_idx: (100.0, scaledSei, ratioVal)}
    we let produce_sei_comparison_chart handle final bar chart, etc.
    """
    sei_data={}
    all_q= set(velocity_tr.keys())| set(uig_tr.keys())| set(mac_tr.keys())
    all_q= sorted(all_q)
    for q_idx in all_q:
        (vT,vS,vR)= velocity_tr.get(q_idx,(0,0,0))
        (uT,uS,uR)= uig_tr.get(q_idx,(0,0,0))
        (mT,mS,mR)= mac_tr.get(q_idx,(0,0,0))

        ratio_weights=[]
        ratio_values=[]
        # if vT>0 => vR is valid
        if abs(vT)>1e-9:
            ratio_weights.append(0.3)
            ratio_values.append(vR)
        if abs(uT)>1e-9:
            ratio_weights.append(0.2)
            ratio_values.append(uR)
        if abs(mT)>1e-9:
            ratio_weights.append(0.5)
            ratio_values.append(mR)

        if not ratio_weights:
            # fallback => just 0.5*mS +0.3*vS +0.2*uS
            scaled_sei= 0.5*mS+ 0.3*vS+ 0.2*uS
            sei_data[q_idx]= (100.0, scaled_sei, 0.0)
            continue

        wsum= sum(ratio_weights)
        partial_sum=0.0
        for i in range(len(ratio_weights)):
            partial_sum+= ratio_weights[i]* ratio_values[i]
        sei_ratio= partial_sum/wsum

        scaled_sei= 0.5*mS+ 0.3*vS+0.2*uS
        sei_data[q_idx]= (100.0, scaled_sei, sei_ratio)
    return sei_data
