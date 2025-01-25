# quarters.py
"""
Simple library to generate quarter windows from a starting date
for BFS aggregator usage.
"""

from dateutil.relativedelta import relativedelta

def generate_quarter_windows(oldest_date, q_count):
    """
    Each quarter is 3 months. We produce q_count windows.
    """
    out=[]
    current= oldest_date
    for _ in range(q_count):
        end= current+ relativedelta(months=3)
        out.append((current,end))
        current= end
    return out

def find_fy(d):
    """
    If month >=10 => fiscal year= d.year+1, else d.year
    """
    if d.month>=10:
        return d.year+1
    return d.year

def quarter_fy_ranges(fy):
    """
    Return Q1..Q4 for that FY
    """
    import datetime
    return {
      "Q1": (datetime.datetime(fy-1,10,1), datetime.datetime(fy-1,12,31,23,59,59)),
      "Q2": (datetime.datetime(fy,1,1), datetime.datetime(fy,3,31,23,59,59)),
      "Q3": (datetime.datetime(fy,4,1), datetime.datetime(fy,6,30,23,59,59)),
      "Q4": (datetime.datetime(fy,7,1), datetime.datetime(fy,9,30,23,59,59))
    }

def largest_overlap_quarter(st, ed):
    fy= find_fy(st)
    Q= quarter_fy_ranges(fy)
    best_lbl= "Q?"
    best_ov= 0
    for qlbl,(qs,qe) in Q.items():
        overlap_s= max(st, qs)
        overlap_e= min(ed, qe)
        ov_sec= (overlap_e- overlap_s).total_seconds()
        if ov_sec> best_ov:
            best_ov= ov_sec
            best_lbl= qlbl
    return best_lbl
