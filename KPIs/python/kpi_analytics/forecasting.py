"""
forecasting.py
Provides ARIMA-based forecasting with fallback Weighted Rolling.
Index-based alignment is used, so each BFS row is monthly (or quarterly) in an array.
We do 2-step forecast on merges, watchers, forks, etc.
"""

import numpy as np

# statsmodels for ARIMA
import statsmodels.api as sm

def can_forecast_arima(series):
    """
    Minimum of 6 data points to try ARIMA(1,1,1). Otherwise fallback to Weighted Rolling.
    """
    return len(series) >= 6

def forecast_arima(series, steps=2):
    """
    Tries ARIMA(1,1,1). Returns an np.array of length 'steps'.
    If it fails, returns empty => fallback Weighted Rolling used.
    """
    try:
        model = sm.tsa.ARIMA(series, order=(1,1,1))
        fitted = model.fit()
        fc = fitted.forecast(steps=steps)
        return fc
    except:
        return np.array([])

def fallback_weighted_rolling(series, steps=2):
    """
    Weighted rolling average fallback:
      if >=3 data => 0.5*x[-1] + 0.3*x[-2] + 0.2*x[-3]
      if 2 => 0.6*x[-1] + 0.4*x[-2]
      if 1 => x[-1]
      else => 0
    We produce multi-step forecast by repeatedly appending the last forecast.
    """
    arr = list(series)
    results=[]
    for _ in range(steps):
        n = len(arr)
        if n>=3:
            val= arr[-1]*0.5 + arr[-2]*0.3 + arr[-3]*0.2
        elif n==2:
            val= arr[-1]*0.6 + arr[-2]*0.4
        elif n==1:
            val= arr[-1]
        else:
            val= 0.0
        results.append(val)
        arr.append(val)
    return np.array(results)

def produce_forecast_values(bfs_values):
    """
    2-step forecast for BFS series.
    ARIMA if can_forecast_arima -> if fails => fallback Weighted Rolling
    """
    if len(bfs_values)<1:
        return []

    if can_forecast_arima(bfs_values):
        fc= forecast_arima(bfs_values, steps=2)
        if len(fc)==0:
            fc= fallback_weighted_rolling(bfs_values, steps=2)
        return fc.tolist()
    else:
        fc= fallback_weighted_rolling(bfs_values, steps=2)
        return fc.tolist()
