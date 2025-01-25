# aggregator.py
"""
Defines aggregator functions: velocity, uig, mac, sei,
plus a placeholder load_aggregator_weights() if you store them in a config.
"""

def load_aggregator_weights():
    """
    If you have aggregator.ini or environment, parse them.
    For now, we do a direct fallback demonstration.
    """
    return {
      "velocity_mergesWeight": 0.4,
      "velocity_closedWeight": 0.6,
      # ...
    }

def velocity(merges_s, closed_s):
    """
    Basic aggregator formula for velocity:
    velocity = 0.4 * mergesScaled + 0.6 * closedScaled
    """
    return 0.4* merges_s + 0.6* closed_s

def user_interest_growth(forks_s, stars_s):
    """
    Basic aggregator formula for UIG:
    uig = 0.4 * forksScaled + 0.6 * starsScaled
    """
    return 0.4* forks_s + 0.6* stars_s

def monthly_active_contributors(issues_comm_reac_s, pulls_s):
    """
    MAC = 0.8 * (issues+comments+reactions scaled) + 0.2 * pullsScaled
    """
    return 0.8* issues_comm_reac_s + 0.2* pulls_s

def compute_sei(velocity_val, uig_val, mac_val):
    """
    SEI = 0.5*MAC + 0.3*Velocity + 0.2*UIG
    """
    return 0.5* mac_val + 0.3* velocity_val + 0.2* uig_val
