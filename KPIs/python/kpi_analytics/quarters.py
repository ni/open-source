# quarters.py

from dateutil.relativedelta import relativedelta
from datetime import datetime

def generate_quarter_windows(start_date, num_quarters):
    """
    Generate exactly num_quarters windows, each 3 months, 
    from start_date, but do not skip partial quarters 
    that extend beyond now -- we clamp them at now if needed.
    """
    windows = []
    current = start_date
    for _ in range(num_quarters):
        nxt = current + relativedelta(months=3)
        windows.append((current, nxt))
        current = nxt
    return windows
