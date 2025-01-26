# aggregator.py
"""
Optional aggregator formulas (Velocity, UIG, MAC, SEI).
We also define placeholders for openIssRatio/openPRRatio => 1.0.
No lines omitted.
"""

def velocity(merges_s, closed_s):
    return 0.4* merges_s + 0.6* closed_s

def user_interest_growth(forks_s, stars_s):
    return 0.4* forks_s + 0.6* stars_s

def monthly_active_contributors(issCommReac_s, pulls_s):
    return 0.8* issCommReac_s + 0.2* pulls_s

def compute_sei(velocity_val, uig_val, mac_val):
    return 0.5* mac_val + 0.3* velocity_val + 0.2* uig_val

def open_iss_ratio_stub():
    """
    Hard-coded to 1.0 for demonstration, since we have no real logic for # open issues vs. closed.
    """
    return 1.0

def open_pr_ratio_stub():
    """
    Hard-coded to 1.0, same reason as above.
    """
    return 1.0
