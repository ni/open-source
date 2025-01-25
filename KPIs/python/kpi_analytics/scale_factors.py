# scale_factors.py
"""
Computes mergesFactor, closedFactor, forksFactor, etc. per repo,
plus optional function compute_sei_data if needed.
"""
import os
import datetime
import mysql.connector

from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def compute_scale_factors(scaling_repo, all_repos):
    """
    For each repo r:
     - mergesFactor[r], closedFactor[r], forksFactor[r], ...
     - Possibly using a 120-day window from oldest to oldest+120
       do sums for scaling repo vs. sums for each repo => ratio
    For brevity, we just do a naive approach:
      - if r == scaling_repo => 1.0
      - else => 0.5, 0.4, etc.
    But you can do a real approach if desired.
    """
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

def compute_sei_data(velocity_val, uig_val, mac_val):
    """
    If you want a direct function to compute SEI from aggregator approach
    """
    return 0.5* mac_val + 0.3* velocity_val + 0.2* uig_val
