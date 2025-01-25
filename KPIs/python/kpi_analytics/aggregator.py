#!/usr/bin/env python3
"""
aggregator.py

Implements aggregator logic for velocity, MAC, user_interest_growth, SEI,
plus reading aggregator weights from config.yaml.
"""

import os
import yaml

DEFAULT_VELOCITY_MERGES_WEIGHT= 0.6
DEFAULT_VELOCITY_CLOSED_WEIGHT= 0.4

DEFAULT_MAC_NEWISS_WEIGHT= 0.8
DEFAULT_MAC_PULLS_WEIGHT= 0.2

def load_aggregator_weights():
    config_file= "config.yaml"
    if not os.path.exists(config_file):
        print(f"[WARN] aggregator => {config_file} missing => fallback.")
        return {
          "velocity_merges_weight": DEFAULT_VELOCITY_MERGES_WEIGHT,
          "velocity_closed_weight": DEFAULT_VELOCITY_CLOSED_WEIGHT,
          "mac_newIssues_weight":   DEFAULT_MAC_NEWISS_WEIGHT,
          "mac_pulls_weight":       DEFAULT_MAC_PULLS_WEIGHT
        }
    with open(config_file,"r",encoding="utf-8") as f:
        data= yaml.safe_load(f) or {}
    agg_data= data.get("aggregatorWeights",{})

    vmw= agg_data.get("velocity_merges_weight", DEFAULT_VELOCITY_MERGES_WEIGHT)
    vcw= agg_data.get("velocity_closed_weight", DEFAULT_VELOCITY_CLOSED_WEIGHT)
    m_ni= agg_data.get("mac_newIssues_weight",  DEFAULT_MAC_NEWISS_WEIGHT)
    m_pl= agg_data.get("mac_pulls_weight",      DEFAULT_MAC_PULLS_WEIGHT)

    return {
      "velocity_merges_weight": vmw,
      "velocity_closed_weight": vcw,
      "mac_newIssues_weight":   m_ni,
      "mac_pulls_weight":       m_pl
    }

def velocity(merges_s, closed_s, openIssueRatio, openPRRatio, weights):
    """
    velocity = merges_s* openPRRatio* mergesWeight 
             + closed_s* openIssueRatio* closedWeight
    """
    w_m= weights.get("velocity_merges_weight",0.6)
    w_c= weights.get("velocity_closed_weight",0.4)
    val= (merges_s + closed_s) * openPRRatio * openIssueRatio
    return val

def user_interest_growth(forks_s, stars_s):
    """
    Hard-coded => 0.4*forks + 0.6*stars
    """
    return 0.4*forks_s + 0.6*stars_s

def monthly_active_contributors(newIss_s, comm_s, reac_s, pull_s, weights):
    """
    mac= w_ni*(newIss_s + comm_s + reac_s) + w_pl*(pull_s)
    """
    w_ni= weights.get("mac_newIssues_weight",0.8)
    w_pl= weights.get("mac_pulls_weight",0.2)
    sumICR= newIss_s+ comm_s+ reac_s
    val= w_ni* sumICR + w_pl* pull_s
    return val
