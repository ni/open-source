# quarters.py
"""
Generates n consecutive 3-month 'GROUP' windows from a given oldest date,
and a helper to label them as partial if needed.
No lines omitted.
"""

from dateutil.relativedelta import relativedelta

def generate_time_groups(oldest_dt, n):
    out=[]
    cur= oldest_dt
    for i in range(n):
        ed= cur+ relativedelta(months=3)
        out.append((cur, ed))
        cur= ed
    return out

def label_group(index, st, ed, partial_flag):
    lbl= f"GROUP {index} ({st:%Y-%m-%d}..{ed:%Y-%m-%d})"
    if partial_flag:
        lbl+= " (partial)"
    return lbl
