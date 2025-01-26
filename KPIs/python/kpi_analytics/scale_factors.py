# scale_factors.py
"""
Defines mergesFactor, closedFactor, etc. if ratio-based scaling is desired.
No lines omitted.
"""

def compute_scale_factors(scaling_repo, all_repos):
    mergesFactor={}
    closedFactor={}
    forksFactor={}
    starsFactor={}
    newIssuesFactor={}
    commentsFactor={}
    reactionsFactor={}
    pullsFactor={}

    for r in all_repos:
        if r==scaling_repo:
            mergesFactor[r]=1.0
            closedFactor[r]=1.0
            forksFactor[r]=1.0
            starsFactor[r]=1.0
            newIssuesFactor[r]=1.0
            commentsFactor[r]=1.0
            reactionsFactor[r]=1.0
            pullsFactor[r]=1.0
        else:
            mergesFactor[r]=0.5
            closedFactor[r]=0.4
            forksFactor[r]=0.05
            starsFactor[r]=0.02
            newIssuesFactor[r]=0.8
            commentsFactor[r]=0.3
            reactionsFactor[r]=0.1
            pullsFactor[r]=0.6
    return (mergesFactor, closedFactor, forksFactor, starsFactor,
            newIssuesFactor, commentsFactor, reactionsFactor, pullsFactor)
