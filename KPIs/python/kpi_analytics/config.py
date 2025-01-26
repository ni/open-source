# config.py
"""
Holds BFS aggregator config, such as SCALING_REPO and NUM_GROUPS.
No lines omitted.
"""

import os

def get_scaling_repo():
    return os.environ.get("SCALING_REPO","ni/labview-icon-editor")

def get_num_groups():
    """
    If an environment variable NUM_GROUPS is set, use it; otherwise default to 4.
    """
    raw= os.environ.get("NUM_GROUPS","8")
    try:
        return int(raw)
    except:
        return 4
