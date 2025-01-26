############################################
# scale_factors.py
############################################

def ratio_vs_group_average(value, group_avg):
    """
    If group_avg>0 => ratio= value/group_avg
    else => 0 or 'N/A'
    """
    if group_avg> 0:
        return value/ group_avg
    else:
        return 0.0
