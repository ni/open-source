############################################
# scale_factors.py
############################################

def ratio_vs_group_average(my_value, group_avg):
    """
    ratio = my_value / group_avg if group_avg > 0 else 0
    Used for BFS aggregator debug prints and side-by-side chart references.
    """
    if group_avg>0:
        return my_value / group_avg
    return 0.0
