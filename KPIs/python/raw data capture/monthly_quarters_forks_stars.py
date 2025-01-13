#!/usr/bin/env python
# monthly_quarters_forks_stars.py

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

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

def get_monthly_forks():
    """
    Returns a DataFrame with columns:
      repo_name, y, m, monthly_count
    by grouping the 'forks' table on (repo_name, YEAR(forked_at), MONTH(forked_at)).
    This avoids only_full_group_by issues.
    """
    conn = connect_db()
    query = """
    SELECT
      repo_name,
      YEAR(forked_at) AS y,
      MONTH(forked_at) AS m,
      COUNT(*) AS monthly_count
    FROM forks
    GROUP BY repo_name, y, m
    ORDER BY repo_name, y, m;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df  # columns: repo_name, y, m, monthly_count

def get_monthly_stars():
    """
    Returns a DataFrame with columns:
      repo_name, y, m, monthly_count
    by grouping the 'stars' table on (repo_name, YEAR(starred_at), MONTH(starred_at)).
    This avoids only_full_group_by issues.
    """
    conn = connect_db()
    query = """
    SELECT
      repo_name,
      YEAR(starred_at) AS y,
      MONTH(starred_at) AS m,
      COUNT(*) AS monthly_count
    FROM stars
    GROUP BY repo_name, y, m
    ORDER BY repo_name, y, m;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df  # columns: repo_name, y, m, monthly_count

def group_into_consecutive_quarters(df):
    """
    Takes columns: repo_name, y, m, monthly_count
    1) Convert y,m => a date for sorting
    2) Sort by that date
    3) For each repo, lumps every 3 monthly rows into Q01, Q02, etc.
    4) Sort the resulting 'Qxx' labels in numeric order (Q01 < Q02 < Q10).
    
    Returns columns: repo_name, q_label, q_value
    """
    # 1) Construct a dt
    df = df.copy()
    df["month_date"] = pd.to_datetime(df.apply(lambda row: f"{int(row.y)}-{int(row.m):02d}-01", axis=1))
    # 2) sort
    df = df.sort_values(["repo_name", "month_date"]).reset_index(drop=True)

    # 3) for each repo, consecutive lumps of 3
    grouped_list = []
    for repo_name, subdf in df.groupby("repo_name"):
        subdf = subdf.sort_values("month_date").reset_index(drop=True)
        subdf["q_index"] = subdf.index // 3  # each group of 3 months => Q
        sums = subdf.groupby("q_index")["monthly_count"].sum().reset_index()
        # create "Q01," "Q02," ...
        sums["q_label"] = sums["q_index"].apply(lambda x: f"Q{(x+1):02d}")
        sums["repo_name"] = repo_name
        sums = sums.rename(columns={"monthly_count": "q_value"})
        grouped_list.append(sums[["repo_name", "q_label", "q_value"]])

    result = pd.concat(grouped_list, ignore_index=True)
    # 4) sort q_label => Q01 < Q02 < Q10 by normal string sorting
    result = result.sort_values(["repo_name", "q_label"]).reset_index(drop=True)
    return result

def create_quarterly_charts(df_forks, df_stars):
    """
    We produce 2 subplots (forks, stars), each with side-by-side bars for each repo in each Qxx,
    plus a table below. The lumps are labeled Q01, Q02, etc.
    """
    pivot_forks = df_forks.pivot(index="q_label", columns="repo_name", values="q_value").fillna(0)
    pivot_stars = df_stars.pivot(index="q_label", columns="repo_name", values="q_value").fillna(0)

    fig, axes = plt.subplots(2, 1, figsize=(10, 8))

    # 1) forks
    pivot_forks.plot(kind="bar", ax=axes[0])
    axes[0].set_title("Forks: Consecutive 3-month lumps => Q01, Q02, etc.")
    axes[0].set_ylabel("Fork Count")

    # table below
    from matplotlib.table import Table
    cell_text = pivot_forks.values.tolist()
    col_labels = pivot_forks.columns.tolist()
    row_labels = pivot_forks.index.tolist()

    t0 = axes[0].table(cellText=cell_text,
                       rowLabels=row_labels,
                       colLabels=col_labels,
                       loc='bottom',
                       cellLoc='center')
    axes[0].set_ylim(top=axes[0].get_ylim()[1]*1.2)

    # 2) stars
    pivot_stars.plot(kind="bar", ax=axes[1])
    axes[1].set_title("Stars: Consecutive 3-month lumps => Q01, Q02, etc.")
    axes[1].set_ylabel("Star Count")

    cell_text2 = pivot_stars.values.tolist()
    col_labels2 = pivot_stars.columns.tolist()
    row_labels2 = pivot_stars.index.tolist()

    t1 = axes[1].table(cellText=cell_text2,
                       rowLabels=row_labels2,
                       colLabels=col_labels2,
                       loc='bottom',
                       cellLoc='center')
    axes[1].set_ylim(top=axes[1].get_ylim()[1]*1.2)

    plt.tight_layout()
    plt.show()

def main():
    # monthly forks
    df_forks_m = get_monthly_forks()
    forks_groups = group_into_consecutive_quarters(df_forks_m)  # => q_label, q_value

    # monthly stars
    df_stars_m = get_monthly_stars()
    stars_groups = group_into_consecutive_quarters(df_stars_m)

    # create chart
    create_quarterly_charts(forks_groups, stars_groups)

if __name__ == "__main__":
    main()
