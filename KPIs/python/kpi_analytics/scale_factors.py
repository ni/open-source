# analytics/scale_factors.py

from datetime import timedelta
from db import get_connection
from baseline import find_oldest_date_for_repo
from merges_issues import (
    count_merged_pulls, count_closed_issues,
    count_new_pulls  # we need this for pull creation
)
from forks_stars import count_forks, count_stars
from config import get_scale_factor_window

def compute_scaling_sums(repo_name, start_dt, window_days):
    """
    Summation of merges, issues, forks, stars, plus new_pulls 
    in [start_dt..(start_dt+window_days)].
    watchers ignored
    """
    end_dt = start_dt + timedelta(days=window_days)
    merges = count_merged_pulls(repo_name, start_dt, end_dt)
    issues = count_closed_issues(repo_name, start_dt, end_dt)
    frks   = count_forks(repo_name, start_dt, end_dt)
    strs   = count_stars(repo_name, start_dt, end_dt)
    newpls = count_new_pulls(repo_name, start_dt, end_dt)
    return (merges, issues, frks, strs, newpls)

def compute_scale_factors(scaling_repo, all_repos):
    """
    Return 5 dicts => merges(M), issues(I), forks(F), stars(S), pulls(P).
    Each non-scaling repo compares sums in the same 'window_days' 
    from their oldest date.
    """
    window_days = get_scale_factor_window()
    sfM = {}
    sfI = {}
    sfF = {}
    sfS = {}
    sfP = {}  # separate factor for pull creation

    s_old = find_oldest_date_for_repo(scaling_repo)
    if s_old is None:
        # fallback => everything=1
        for r in all_repos:
            sfM[r] = 1.0
            sfI[r] = 1.0
            sfF[r] = 1.0
            sfS[r] = 1.0
            sfP[r] = 1.0
        return sfM, sfI, sfF, sfS, sfP

    (scM, scI, scF, scS, scPls) = compute_scaling_sums(scaling_repo, s_old, window_days)
    # scaling repo => factor=1
    sfM[scaling_repo] = 1.0
    sfI[scaling_repo] = 1.0
    sfF[scaling_repo] = 1.0
    sfS[scaling_repo] = 1.0
    sfP[scaling_repo] = 1.0

    for r in all_repos:
        if r == scaling_repo:
            continue
        r_old = find_oldest_date_for_repo(r)
        if r_old is None:
            sfM[r] = 1.0
            sfI[r] = 1.0
            sfF[r] = 1.0
            sfS[r] = 1.0
            sfP[r] = 1.0
            continue
        (rm, ri, rf, rs, rpls) = compute_scaling_sums(r, r_old, window_days)

        # merges
        if scM>0 and rm==0:
            sfM[r] = 1.0
        elif scM==0 and rm>0:
            sfM[r] = 0.0
        else:
            sfM[r] = scM/rm if rm else 1.0

        # issues
        if scI>0 and ri==0:
            sfI[r] = 1.0
        elif scI==0 and ri>0:
            sfI[r] = 0.0
        else:
            sfI[r] = scI/ri if ri else 1.0

        # forks
        if scF>0 and rf==0:
            sfF[r] = 1.0
        elif scF==0 and rf>0:
            sfF[r] = 0.0
        else:
            sfF[r] = scF/rf if rf else 1.0

        # stars
        if scS>0 and rs==0:
            sfS[r] = 1.0
        elif scS==0 and rs>0:
            sfS[r] = 0.0
        else:
            sfS[r] = scS/rs if rs else 1.0

        # pull creation
        if scPls>0 and rpls==0:
            sfP[r] = 1.0
        elif scPls==0 and rpls>0:
            sfP[r] = 0.0
        else:
            sfP[r] = scPls/rpls if rpls else 1.0

    return sfM, sfI, sfF, sfS, sfP
