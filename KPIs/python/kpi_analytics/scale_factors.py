#!/usr/bin/env python3
"""
scale_factors.py

This module defines:
1) compute_scale_factors(...):
   - Creates 8 separate dictionaries for scaling merges, closed issues,
     forks, stars, new issues, comments, reactions, new pulls.

2) compute_target_reached_data(...):
   - Sums up each metric across non-scaling repos to produce an average,
     then compares the scaling repo's value => ratio.

3) compute_sei_data(...):
   - Weighted SEI ratio approach if you want a final "SEI" aggregator
     from velocity, UIG, and MAC target dicts.
"""

from datetime import datetime, timedelta

# Suppose these are your aggregator references if you need them:
# from merges_issues import count_merged_pulls, count_closed_issues, ...
# from forks_stars import count_forks, count_stars
# from comments_reactions import count_issue_comments, count_all_reactions


def compute_scale_factors(scaling_repo, all_repos):
    """
    For each repository, define a separate scale factor for each raw variable:
      mergesFactor, closedFactor, forksFactor, starsFactor,
      newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor.

    Steps:
      1) Sum merges, closed, forks, stars, new issues, comments, reactions, new pulls
         in [scalingOldestDate, scalingOldestDate + 120 days] for the scaling repo.
      2) For each other repo, do the same in [repoOldestDate, repoOldestDate + 120 days].
      3) factor = scalingSum / repoSum (with fallback logic).
    Returns:
      (mergesFactor, closedFactor, forksFactor, starsFactor,
       newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor)

    Each factor dict: mergesFactor[repo] = float scaling factor, etc.
    """

    # We'll define window=120 days. Adjust if needed or read from config.
    window_days = 10

    # Prepare result dictionaries
    mergesFactor = {}
    closedFactor = {}
    forksFactor = {}
    starsFactor = {}
    newIssuesFactor = {}
    commentsFactor = {}
    reactionsFactor = {}
    pullsFactor = {}

    # Helper function to sum the eight raw variables for a given repo in [start,end].
    def sum_all_counts(r, start_dt, end_dt):
        """
        Summation stub to handle merges, closed, forks, stars,
        new issues, comments, reactions, new pulls in [start_dt, end_dt].
        Replace with your real merges_issues, forks_stars, etc.
        """
        from merges_issues import (
            count_merged_pulls, count_closed_issues,
            count_new_pulls, count_new_issues
        )
        from forks_stars import (
            count_forks, count_stars
        )
        from comments_reactions import (
            count_issue_comments, count_all_reactions
        )

        merges_count = count_merged_pulls(r, start_dt, end_dt)
        closed_count = count_closed_issues(r, start_dt, end_dt)
        forks_count  = count_forks(r, start_dt, end_dt)
        stars_count  = count_stars(r, start_dt, end_dt)
        newIss_count = count_new_issues(r, start_dt, end_dt)
        comm_count   = count_issue_comments(r, start_dt, end_dt)
        reac_count   = count_all_reactions(r, start_dt, end_dt)
        pulls_count  = count_new_pulls(r, start_dt, end_dt)

        return (
            merges_count, closed_count, forks_count, stars_count,
            newIss_count, comm_count, reac_count, pulls_count
        )

    # Attempt to find oldest date for scaling repo
    from baseline import find_oldest_date_for_repo
    scaling_oldest = find_oldest_date_for_repo(scaling_repo)

    def fallback_factor_initialize():
        """If scaling repo has no data => set all=1.0 for each repo."""
        for rr in all_repos:
            mergesFactor[rr]=1.0
            closedFactor[rr]=1.0
            forksFactor[rr]=1.0
            starsFactor[rr]=1.0
            newIssuesFactor[rr]=1.0
            commentsFactor[rr]=1.0
            reactionsFactor[rr]=1.0
            pullsFactor[rr]=1.0

    if not scaling_oldest:
        # no data => fallback
        fallback_factor_initialize()
        return (mergesFactor, closedFactor, forksFactor, starsFactor,
                newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor)

    scaling_end= scaling_oldest + timedelta(days=window_days)
    (m_scl, c_scl, f_scl, st_scl,
     ni_scl, co_scl, re_scl, pu_scl)= sum_all_counts(scaling_repo, scaling_oldest, scaling_end)

    def ratio_func(scale_val, repo_val):
        """
        Basic approach:
          if scale_val>0 && repo_val=0 => 0.0
          elif both=0 => 1.0
          else => scale_val / repo_val
        """
        if scale_val>0 and repo_val==0:
            return 0.0
        elif scale_val==0 and repo_val>0:
            return 0.0
        elif scale_val==0 and repo_val==0:
            return 1.0
        else:
            return float(scale_val)/ float(repo_val)

    for rr in all_repos:
        if rr==scaling_repo:
            # scaling => factor=1.0
            mergesFactor[rr] = 1.0
            closedFactor[rr] = 1.0
            forksFactor[rr]  = 1.0
            starsFactor[rr]  = 1.0
            newIssuesFactor[rr] = 1.0
            commentsFactor[rr]  = 1.0
            reactionsFactor[rr] = 1.0
            pullsFactor[rr]     = 1.0
            continue

        repoOldest= find_oldest_date_for_repo(rr)
        if not repoOldest:
            # no data => fallback
            mergesFactor[rr]=1.0
            closedFactor[rr]=1.0
            forksFactor[rr]=1.0
            starsFactor[rr]=1.0
            newIssuesFactor[rr]=1.0
            commentsFactor[rr]=1.0
            reactionsFactor[rr]=1.0
            pullsFactor[rr]=1.0
            continue

        repo_end= repoOldest + timedelta(days=window_days)
        (m_repo, c_repo, f_repo, st_repo,
         ni_repo, co_repo, re_repo, pu_repo)= sum_all_counts(rr, repoOldest, repo_end)

        mergesFactor[rr]    = ratio_func(m_scl, m_repo)
        closedFactor[rr]    = ratio_func(c_scl, c_repo)
        forksFactor[rr]     = ratio_func(f_scl, f_repo)
        starsFactor[rr]     = ratio_func(st_scl, st_repo)
        newIssuesFactor[rr] = ratio_func(ni_scl, ni_repo)
        commentsFactor[rr]  = ratio_func(co_scl, co_repo)
        reactionsFactor[rr] = ratio_func(re_scl, re_repo)
        pullsFactor[rr]     = ratio_func(pu_scl, pu_repo)

    return (
        mergesFactor,
        closedFactor,
        forksFactor,
        starsFactor,
        newIssuesFactor,
        commentsFactor,
        reactionsFactor,
        pullsFactor
    )


def compute_target_reached_data(repos, scaling_repo, quarter_data_dict):
    """
    Summation among non-scaling => average => compare to scaling => ratio.
    Returns a dict: {q_idx: (averageVal, scalingVal, ratioVal)}

    averageVal = average of non-scaling repos' metric
    scalingVal = scaling repo's metric
    ratioVal   = (scalingVal / averageVal)*100 if averageVal>0 else 0
    """
    target_data = {}
    union_q_idx= set()
    for r in repos:
        union_q_idx |= set(quarter_data_dict[r].keys())
    union_q_idx= sorted(union_q_idx)

    non_scaling= [rr for rr in repos if rr!=scaling_repo]

    for q_idx in union_q_idx:
        sum_val= 0.0
        count_val=0
        for r in non_scaling:
            if q_idx in quarter_data_dict[r]:
                sum_val+= quarter_data_dict[r][q_idx]
                count_val+=1
        avg_val=0.0
        if count_val>0:
            avg_val= sum_val/count_val

        scaling_val= quarter_data_dict[scaling_repo].get(q_idx,0.0)
        ratio_val=0.0
        if abs(avg_val)>1e-9:
            ratio_val= (scaling_val/ avg_val)*100.0

        target_data[q_idx]= (avg_val, scaling_val, ratio_val)
    return target_data


def compute_sei_data(velocity_dict, uig_dict, mac_dict):
    """
    Weighted ratio approach => 0.3 velocity, 0.2 uig, 0.5 mac if present
    returns {q_idx: (100.0, scaledSei, ratioVal)}.

    scaledSei is the scaling repo's actual SEI if needed,
    ratioVal is partialSum / weightSum from velocityRatio, uigRatio, macRatio.
    """
    sei_data={}
    all_q= set(velocity_dict.keys())| set(uig_dict.keys())| set(mac_dict.keys())
    all_q= sorted(all_q)
    for q_idx in all_q:
        (vT,vS,vR)= velocity_dict.get(q_idx,(0,0,0))
        (uT,uS,uR)= uig_dict.get(q_idx,(0,0,0))
        (mT,mS,mR)= mac_dict.get(q_idx,(0,0,0))

        ratio_weights=[]
        ratio_values=[]
        # if velocity target is nonzero => we weigh vR with 0.3
        if abs(vT)>1e-9:
            ratio_weights.append(0.3)
            ratio_values.append(vR)
        # if uig target is nonzero => weigh uR with 0.2
        if abs(uT)>1e-9:
            ratio_weights.append(0.2)
            ratio_values.append(uR)
        # if mac target is nonzero => weigh mR with 0.5
        if abs(mT)>1e-9:
            ratio_weights.append(0.5)
            ratio_values.append(mR)

        if len(ratio_weights)==0:
            # fallback => no scaling ratio => call it 0.0, scaled=some aggregator
            scaled_sei= 0.5*mS + 0.3*vS + 0.2*uS
            sei_data[q_idx]= (100.0, scaled_sei, 0.0)
            continue

        wsum= sum(ratio_weights)
        partial_sum=0.0
        for i in range(len(ratio_weights)):
            partial_sum+= ratio_weights[i]* ratio_values[i]
        sei_ratio= partial_sum/wsum
        scaled_sei= 0.5*mS + 0.3*vS + 0.2*uS
        sei_data[q_idx]= (100.0, scaled_sei, sei_ratio)
    return sei_data
