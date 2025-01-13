#!/usr/bin/env python
# monthly_quarters_forks_stars.py

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# CONFIGURE HOW MANY "QUARTERS" (LUMPS) YOU WANT
############################
QUARTERS = 4  # e.g., 4 => Q01..Q04, 5 => Q01..Q05, etc.

def connect_db():
    """
    Connect to the MySQL DB
    """
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def get_monthly_data(table_name, date_col):
    """
    Generic function that queries 'table_name' (e.g. 'forks' or 'stars'),
    grouping by YEAR(date_col), MONTH(date_col), thus avoiding only_full_group_by errors.
    
    Returns a DF with columns:
      repo_name, y, m, monthly_count
    """
    conn = connect_db()
    query = f"""
    SELECT
      repo_name,
      YEAR({date_col}) AS y,
      MONTH({date_col}) AS m,
      COUNT(*) AS monthly_count
    FROM {table_name}
    GROUP BY repo_name, y, m
    ORDER BY repo_name, y, m
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df  # columns => [repo_name, y, m, monthly_count]

def build_date_for_row(row):
    """
    Convert (y, m) => a real date (YYYY-MM-01).
    If invalid or missing, returns None.
    """
    try:
        yyyy = int(row["y"])
        mm = int(row["m"])
        return datetime(yyyy, mm, 1)
    except:
        return None

def group_into_configurable_quarters(df, quarters=4):
    """
    Takes columns: repo_name, y, m, monthly_count.
    1) Convert (y,m) => dt
    2) Find global min_dt, max_dt across all rows
    3) We divide the entire range [min_dt, max_dt] into 'quarters' lumps.
       => each lump is a contiguous date interval
    4) Each monthly row is assigned to exactly one of those lumps
    5) We label lumps => Q01, Q02, ...
    6) Return DF with columns [repo_name, q_label, q_value].
    """
    df = df.copy()
    df["dt"] = df.apply(build_date_for_row, axis=1)
    df = df.dropna(subset=["dt"])  # in case any row is invalid
    df = df.sort_values(["repo_name", "dt"]).reset_index(drop=True)

    if df.empty:
        return pd.DataFrame(columns=["repo_name","q_label","q_value"])

    # 2) find global min_dt, max_dt
    min_dt = df["dt"].min()
    max_dt = df["dt"].max()
    if min_dt == max_dt:
        # all data in one date => just put it in Q01
        df["q_label"] = "Q01"
        df["q_value"] = df["monthly_count"]
        return df[["repo_name","q_label","q_value"]]

    # define the lumps
    # we create 'quarters' lumps from min_dt..max_dt
    total_days = (max_dt - min_dt).days
    if total_days < 0:
        total_days = 0
    if quarters < 1:
        quarters = 1

    # step in days
    step = total_days / quarters  # float
    boundaries = [min_dt]
    for i in range(1, quarters):
        # boundary i => min_dt + i*step
        next_boundary = min_dt + timedelta(days=i*step)
        boundaries.append(next_boundary)
    boundaries.append(max_dt + timedelta(seconds=1))  # ensure we include max_dt in last bucket

    # label lumps => Q01..Qxx
    # we define intervals: [boundaries[i], boundaries[i+1])
    # each row dt belongs to the first interval that dt < boundary[i+1]
    # if dt >= boundary[i] and dt < boundary[i+1]
    label_map = {}
    for i in range(quarters):
        label_map[i] = f"Q{(i+1):02d}"

    def find_q_label(row_dt):
        # find i such that row_dt in [boundaries[i], boundaries[i+1])
        # we can do a simple loop
        for i in range(quarters):
            if boundaries[i] <= row_dt < boundaries[i+1]:
                return label_map[i]
        return label_map[quarters-1]  # fallback

    df["q_label"] = df["dt"].apply(find_q_label)

    # 5) sum monthly_count for each repo, q_label
    g = df.groupby(["repo_name","q_label"])["monthly_count"].sum().reset_index()
    g = g.rename(columns={"monthly_count":"q_value"})

    # sort q_label => Q01..Q02..Q10 => normal string sort is fine
    g = g.sort_values(["repo_name","q_label"]).reset_index(drop=True)

    return g

def create_quarterly_figure(df, title_str):
    """
    Creates a figure for either forks or stars from df:
    columns => [repo_name, q_label, q_value].
    We pivot => row=q_label, columns=repo_name, val=q_value
    Then bar chart side-by-side for each repo, table below, returning the figure.
    """
    pivot_df = df.pivot(index="q_label", columns="repo_name", values="q_value").fillna(0)

    fig, ax = plt.subplots(figsize=(10, 6))
    pivot_df.plot(kind="bar", ax=ax)
    ax.set_title(title_str)
    ax.set_ylabel("Count")

    # add table
    cell_text = pivot_df.values.tolist()
    col_labels = pivot_df.columns.tolist()
    row_labels = pivot_df.index.tolist()

    # put the table at the bottom
    t0 = ax.table(cellText=cell_text,
                  rowLabels=row_labels,
                  colLabels=col_labels,
                  loc='bottom',
                  cellLoc='center')
    ax.set_ylim(top=ax.get_ylim()[1]*1.2)

    plt.tight_layout()
    return fig

def main():
    global QUARTERS

    # 1) monthly forks => group => figure
    forks_m = get_monthly_data("forks","forked_at")  # repo_name,y,m,monthly_count
    forks_q = group_into_configurable_quarters(forks_m, quarters=QUARTERS)
    fig_forks = create_quarterly_figure(forks_q, f"Forks: {QUARTERS} lumps => Qxx")

    # 2) monthly stars => group => figure
    stars_m = get_monthly_data("stars","starred_at")
    stars_q = group_into_configurable_quarters(stars_m, quarters=QUARTERS)
    fig_stars = create_quarterly_figure(stars_q, f"Stars: {QUARTERS} lumps => Qxx")

    # 3) show
    plt.show()

if __name__ == "__main__":
    main()
