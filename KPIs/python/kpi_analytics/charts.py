# charts.py
"""
Produces side-by-side scaled bar charts for each splitted metric
plus aggregator expansions (velocity, uig, mac, sei).
No lines omitted. 
"""

import matplotlib.pyplot as plt
import os

def produce_side_by_side_chart(metric_name, scaling_repo, BFS_data, mergesF, closedF, forksF, starsF, newIssF, commentsF, reacF, pullsF):
    """
    Creates e.g. merges_scaled.png, where each quarter on X-axis
    has 2 bars: one for scaling repo's mergesScaled, one for average mergesScaled among non-scalers.
    We'll do partial logic => if no data, 0 bar.
    """
    # Gather quarter indices from scaling repo
    if scaling_repo not in BFS_data:
        print(f"[WARN] scaling repo {scaling_repo} not in BFS_data => cannot produce {metric_name} chart.")
        return
    scaling_data= BFS_data[scaling_repo]
    if not scaling_data:
        print(f"[WARN] no BFS data for scaling repo => skip {metric_name} chart.")
        return

    # We'll define a function that maps splitted to scaled
    def computeScaled(repo, splitted):
        # mergesFactor etc. approach
        raw= splitted[metric_name+"Raw"]
        fac= 1.0
        if metric_name=="merges": fac= mergesF.get(repo,1.0)
        elif metric_name=="closed": fac= closedF.get(repo,1.0)
        elif metric_name=="forks": fac= forksF.get(repo,1.0)
        elif metric_name=="stars": fac= starsF.get(repo,1.0)
        elif metric_name=="newIss": fac= newIssF.get(repo,1.0)
        elif metric_name=="comments": fac= commentsF.get(repo,1.0)
        elif metric_name=="reactions": fac= reacF.get(repo,1.0)
        elif metric_name=="pull": fac= pullsF.get(repo,1.0)
        return raw* fac

    # Construct x-locations from the scaling repo's quarter indexes
    quarter_ids= sorted(scaling_data.keys())
    x_vals= range(len(quarter_ids))

    scaling_vals=[]
    for q_i in quarter_ids:
        splitted= scaling_data[q_i][3]
        val= computeScaled(scaling_repo, splitted)
        scaling_vals.append(val)

    # for average among non-scalers
    # BFS_data => BFS_data[repo][idx]= (st,ed, partial, splitted)
    other_repos= [r for r in BFS_data.keys() if r!= scaling_repo]
    avg_vals=[]
    for q_i in quarter_ids:
        sum_v= 0.0
        count= 0
        for orp in other_repos:
            if q_i in BFS_data[orp]:
                splitted= BFS_data[orp][q_i][3]
                val= computeScaled(orp, splitted)
                sum_v+= val
                count+=1
        if count>0:
            avg_vals.append(sum_v/count)
        else:
            avg_vals.append(0.0)

    bar_width= 0.3
    plt.figure(figsize=(9,6))
    plt.title(f"{metric_name.capitalize()} scaled: {scaling_repo} vs Non-Scaling Average")
    plt.xlabel("Quarter Index")
    plt.ylabel(f"{metric_name.capitalize()} (Scaled)")

    # shift left vs. right
    import numpy as np
    x_arr= np.arange(len(x_vals))
    x_scaling= x_arr- bar_width/2
    x_avg= x_arr+ bar_width/2

    plt.bar(x_scaling, scaling_vals, width=bar_width, color='blue', label=f"{scaling_repo}")
    plt.bar(x_avg, avg_vals, width=bar_width, color='orange', label="Non-Scaling Avg")

    plt.xticks(x_arr,[f"Q{q}" for q in quarter_ids], rotation=0)
    plt.legend()
    fname= f"{metric_name}_scaled.png"
    plt.tight_layout()
    plt.savefig(fname)
    plt.close()
    print(f"[INFO] Created {fname}")


def produce_aggregator_chart(agg_name, scaling_repo, BFS_data, aggregator_func):
    """
    aggregator_func(BFS_data[repo][q_i][3]) => a single aggregator value.
    Then produce a side-by-side chart for scaling vs. average
    e.g. velocity_compare.png or mac_compare.png
    """
    import matplotlib.pyplot as plt
    import numpy as np

    if scaling_repo not in BFS_data:
        print(f"[WARN] no BFS data for {scaling_repo} => skip aggregator chart {agg_name}")
        return
    quarter_ids= sorted(BFS_data[scaling_repo].keys())
    x_arr= np.arange(len(quarter_ids))

    def aggregator_for_repo(repo, q_i):
        splitted= BFS_data[repo][q_i][3]
        return aggregator_func(repo, splitted)

    # build scaling
    scaling_vals=[]
    for q_i in quarter_ids:
        if q_i not in BFS_data[scaling_repo]:
            scaling_vals.append(0.0)
        else:
            v= aggregator_for_repo(scaling_repo, q_i)
            scaling_vals.append(v)

    # build average
    other_repos= [r for r in BFS_data.keys() if r!=scaling_repo]
    avg_vals=[]
    for q_i in quarter_ids:
        sum_v= 0.0
        c= 0
        for orp in other_repos:
            if q_i in BFS_data[orp]:
                vv= aggregator_for_repo(orp, q_i)
                sum_v+= vv
                c+=1
        if c>0:
            avg_vals.append(sum_v/c)
        else:
            avg_vals.append(0.0)

    bar_width= 0.3
    plt.figure(figsize=(9,6))
    plt.title(f"{agg_name} Compare: {scaling_repo} vs. Non-Scaling")
    plt.xlabel("Quarter Index")
    plt.ylabel(f"{agg_name} (Scaled)")

    x_scaling= x_arr- bar_width/2
    x_avg= x_arr+ bar_width/2

    plt.bar(x_scaling, scaling_vals, width=bar_width, color='blue', label=scaling_repo)
    plt.bar(x_avg, avg_vals, width=bar_width, color='orange', label="Non-Scaling Avg")

    plt.xticks(x_arr,[f"Q{q}" for q in quarter_ids], rotation=0)
    plt.legend()
    fname= f"{agg_name.lower()}_compare.png"
    plt.tight_layout()
    plt.savefig(fname)
    plt.close()
    print(f"[INFO] Created {fname}")
