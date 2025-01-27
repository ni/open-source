"""
aggregator.py
Scales BFS splitted columns, aggregator formulas, plus 2-step forecast for merges, watchers, forks, stars, distinct participants.

Variables:
  mergesScaled, closedIssScaled, closedPRScaled, forksScaled, starsScaled, watchersScaled,
  commentsIssueScaled, commentsPRScaled, reactIssueScaled, reactPRScaled, distinctPartScaled
Aggregator:
  velocity = 0.4*mergesScaled + 0.6*closedIssScaled (focusing on issues closed)
  uig      = 0.4*forksScaled + 0.6*starsScaled
  mac      = 0.8*( (commentsIssueScaled+reactIssueScaled) + (commentsPRScaled+reactPRScaled) ) +0.2* mergesScaled
  sei      = 0.3*velocity +0.2*uig +0.3*mac +0.1*watchersScaled +0.1*distinctPartScaled

Weighted Group "target" uses watchers or stars as weighting factor to compute average.
"""

from forecasting import produce_forecast_values

def compute_scaled_columns(row):
    # Basic 1:1 scale factor => mergesScaled= mergesRaw, etc.
    row['mergesScaled']= row['mergesRaw']
    row['closedIssScaled']= row['closedIssRaw']
    row['closedPRScaled']= row['closedPRRaw']
    row['forksScaled']= row['forksRaw']
    row['starsScaled']= row['starsRaw']
    row['watchersScaled']= row['watchersRaw']
    row['commentsIssueScaled']= row['commentsIssueRaw']
    row['commentsPRScaled']= row['commentsPRRaw']
    row['reactIssueScaled']= row['reactIssueRaw']
    row['reactPRScaled']= row['reactPRRaw']
    row['distinctPartScaled']= row['distinctPartRaw']

def compute_aggregators(row, weightingApproach="watchers"):
    mg= row['mergesScaled']
    cIss= row['closedIssScaled']
    velocity= 0.4* mg + 0.6* cIss

    f= row['forksScaled']
    s= row['starsScaled']
    uig= 0.4*f + 0.6*s

    cI= row['commentsIssueScaled']+ row['reactIssueScaled']
    cP= row['commentsPRScaled']+ row['reactPRScaled']
    sumComm= cI + cP
    mac= 0.8*sumComm + 0.2*mg

    watchersVal= row['watchersScaled']
    if weightingApproach=="stars":
        watchersVal= row['starsScaled']
    part= row['distinctPartScaled']
    sei= 0.3*velocity + 0.2*uig + 0.3*mac + 0.1*watchersVal + 0.1* part

    row['velocity']= velocity
    row['uig']= uig
    row['mac']= mac
    row['sei']= sei

def monthly_bfs_aggregator(repo, monthly_data, debug_lines, weightingApproach="watchers"):
    # Scale splitted BFS => aggregator
    for row in monthly_data:
        compute_scaled_columns(row)
        compute_aggregators(row, weightingApproach)

    # forecast mergesScaled, watchersScaled, forksScaled, starsScaled, distinctPartScaled
    merges_series= [r['mergesScaled'] for r in monthly_data]
    watchers_series= [r['watchersScaled'] for r in monthly_data]
    forks_series= [r['forksScaled'] for r in monthly_data]
    stars_series= [r['starsScaled'] for r in monthly_data]
    part_series= [r['distinctPartScaled'] for r in monthly_data]

    merges_fc= produce_forecast_values(merges_series)
    watchers_fc= produce_forecast_values(watchers_series)
    forks_fc= produce_forecast_values(forks_series)
    stars_fc= produce_forecast_values(stars_series)
    part_fc= produce_forecast_values(part_series)

    nfc= min(len(merges_fc),len(watchers_fc),len(forks_fc),len(stars_fc),len(part_fc))
    for i in range(nfc):
        frow= {
          'forecastRow': True,
          'mergesRaw': merges_fc[i],
          'closedIssRaw': 0.0,
          'closedPRRaw': 0.0,
          'forksRaw': forks_fc[i],
          'starsRaw': stars_fc[i],
          'watchersRaw': watchers_fc[i],
          'commentsIssueRaw':0.0,
          'commentsPRRaw':0.0,
          'reactIssueRaw':0.0,
          'reactPRRaw':0.0,
          'distinctPartRaw': part_fc[i],
          'start_dt': None,
          'end_dt': None,
          'partialCoverage':False
        }
        compute_scaled_columns(frow)
        compute_aggregators(frow, weightingApproach)
        monthly_data.append(frow)

    return monthly_data

def watchers_weighted_group_avg(repoMonthMap, BFSkey, debug_lines, weightingApproach="watchers"):
    """
    Aggregates BFS data across multiple repos, returning a side-by-side "average" array.
    For each index i in 0..max_len-1, it sums BFSkey across all repos that have i'th row,
    weighting them by watchers or some other approach, then divides by total watchers, etc.
    """
    debug_lines.append(f"[INFO] watchers_weighted_group_avg => BFSkey={BFSkey}, weighting={weightingApproach}")

    # Filter out repos that have BFS data
    valid_data = { r: repoMonthMap[r] for r in repoMonthMap if repoMonthMap[r] }
    if not valid_data:
        debug_lines.append("[WARN] watchers_weighted_group_avg => no BFS data => returning empty aggregator")
        return []

    # Ensure we have at least one non-empty list
    length_candidates = [len(valid_data[r]) for r in valid_data]
    if not length_candidates:
        debug_lines.append("[WARN] watchers_weighted_group_avg => length_candidates empty => return []")
        return []

    max_len = max(length_candidates)  # guaranteed not empty now

    # Build an array of size max_len
    final_array = [0.0]*max_len

    for i in range(max_len):
        # Weighted sum approach
        sum_val = 0.0
        sum_w = 0.0
        for repo in valid_data:
            arr = valid_data[repo]
            if i < len(arr):
                row = arr[i]
                watchers = row.get('watchersRaw',0.0)
                val = row.get(BFSkey,0.0)
                sum_val += (val * watchers)
                sum_w += watchers

        if sum_w> 0:
            final_array[i] = sum_val/sum_w
        else:
            final_array[i] = 0.0

    debug_lines.append(f"[INFO] watchers_weighted_group_avg => done BFSkey={BFSkey}, length={max_len}")
    return final_array