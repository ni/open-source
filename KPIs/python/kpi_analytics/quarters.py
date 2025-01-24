#!/usr/bin/env python3
"""
quarters.py

Generates n consecutive 3-month windows from a start_dt.
If you want a real "fiscal" approach, adapt add_months or define exact Q1..Q4 bounds.
"""

from datetime import datetime
import calendar

def generate_quarter_windows(start_dt, n):
    windows=[]
    cur= start_dt
    for _ in range(n):
        end= add_months(cur, 3)
        windows.append((cur, end))
        cur= end
    return windows

def add_months(dt, months):
    year= dt.year
    month= dt.month+ months
    day= dt.day
    while month>12:
        month-=12
        year+=1
    last_day= calendar.monthrange(year, month)[1]
    if day> last_day:
        day= last_day
    return dt.replace(year=year, month=month, day=day)
