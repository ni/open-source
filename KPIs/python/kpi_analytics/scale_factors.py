############################################################
# scale_factors.py
# ratio vs group average or any additional scaling approach
############################################################

def ratio_vs_group_average(my_value, group_avg):
    """
    ratio = my_value / group_avg if group_avg>0 else 0
    """
    if group_avg> 0:
        return my_value / group_avg
    return 0.0
