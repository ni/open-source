#!/usr/bin/env python3
"""
quarters.py

Generates n consecutive 3-month windows from start_dt. 
(If you want a real "fiscal approach," just adapt the logic here.)
"""

from datetime import datetime
import calendar

def generate_quarter_windows(start_dt, n):
    """
    Return list of (q_start, q_end) for n consecutive 3-month increments
    from start_dt.
    """
    windows = []
    cur = start_dt
    for _ in range(n):
        end = add_months(cur, 3)
        windows.append((cur, end))
        cur = end
    return windows

def add_months(dt, months):
    """
    Simple function that increments dt.month by `months`. 
    Clamps day if it exceeds last day of the new month.
    """
    year = dt.year
    month = dt.month + months
    day = dt.day
    while month > 12:
        month -= 12
        year += 1
    last_day = calendar.monthrange(year, month)[1]
    if day > last_day:
        day = last_day
    return dt.replace(year=year, month=month, day=day)
