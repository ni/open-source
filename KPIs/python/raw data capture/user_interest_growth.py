#!/usr/bin/env python
# monthly_quarters_forks_stars.py

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

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


def get_monthly_data(table_name, date_col):
    """
    Generic function to get monthly counts from a given table (forks or stars).
    table_name: 'forks' or 'stars'
    date_col: 'forked_at' or 'starred_at'
    
    Returns a DataFrame:
      repo_name, yearmonth, monthly_count
    """
    conn = connect_db()
    query = f"""
    SELECT
      repo_name,
      DATE_FORMAT({date_col}, '%%Y-%%m') AS yearmonth,
      COUNT(*) AS monthly_count
    FROM {table_name}
    GROUP BY repo_name, YEAR({date_col}), MONTH({date_col})
    ORDER BY repo_name, YEAR({date_col}), MONTH({date_col});
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df["table_name"] = table_name  # so we know if it's forks or stars
    return df


def group_into_quarters_consecutive(df):
    """
    We have columns: repo_name, yearmonth, monthly_count, table_name
    We'll sort by (repo_name, actual date).
    Then for each repo_name, we group every 3 consecutive rows => 'quarter_index'
       quarter_label = Quarter1, Quarter2...
       quarter_value = sum of monthly_count in those 3 rows
    Return a DF with columns:
      repo_name, quarter_label, quarter_value, table_name
    """
    # parse yearmonth -> dt
    df = df.copy()
    df["dt"] = pd.to_datetime(df["yearmonth"] + "-01")
    df = df.sort_values(["repo_name", "dt"]).reset_index(drop=True)

    group_list = []
    for repo, subdf in df.groupby("repo_name"):
        subdf = subdf.sort_values("dt").reset_index(drop=True)
        subdf["quarter_index"] = subdf.index // 3  # consecutive lumps of 3
        grouped = subdf.groupby("quarter_index")["monthly_count"].sum().reset_index()
        grouped["quarter_label"] = "Quarter" + (grouped["quarter_index"] + 1).astype(str)
        grouped["repo_name"] = repo
        group_list.append(grouped[["repo_name", "quarter_label", "monthly_count"]])

    out = pd.concat(group_list, ignore_index=True)
    # keep table_name from original df
    if "table_name" in df.columns:
        out["table_name"] = df["table_name"].iloc[0]
    else:
        out["table_name"] = "unknown"
    out = out.rename(columns={"monthly_count": "quarter_value"})
    return out


def scale_quarter_values(df):
    """
    Implement user request #10:
    - find the smallest positive quarter_value among all repos
    - for each repo, find that repo's smallest positive quarter_value
    - scale_factor = (global smallest) / (that repo's smallest)
    - multiply that entire repo's quarter values by scale_factor
    - store that scale_factor so we can print it next to the label in the legend

    We'll add a new column 'scaled_value' = quarter_value * scale_factor
    We'll also create a dict: repo->scale_factor so we can rename the columns in the legend.
    """
    # find global smallest positive
    mask_pos = df["quarter_value"] > 0
    if not mask_pos.any():
        # no positive values => no scaling
        df["scaled_value"] = df["quarter_value"]
        return df, {}
    global_smallest = df.loc[mask_pos, "quarter_value"].min()

    scale_dict = {}
    # for each repo, find its smallest positive
    for repo_name in df["repo_name"].unique():
        sub = df[(df["repo_name"] == repo_name) & (df["quarter_value"] > 0)]
        if len(sub) == 0:
            # all zero => scale=1
            scale_dict[repo_name] = 1.0
        else:
            repo_smallest = sub["quarter_value"].min()
            scale_factor = global_smallest / repo_smallest
            scale_dict[repo_name] = scale_factor

    # now multiply
    def compute_scaled(row):
        r = row["repo_name"]
        val = row["quarter_value"]
        return val * scale_dict[r]

    df["scaled_value"] = df.apply(compute_scaled, axis=1)
    return df, scale_dict


def create_quarterly_chart(df_forks, df_stars):
    """
    We'll produce 2 subplots: one for forks, one for stars.
    We'll pivot so each 'quarter_label' is a row, each 'repo_name' is a column, values are scaled_value.
    Then we plot bar charts side by side, with table below each chart.
    We'll rename each repo's column in the pivot as "repo_name(scale_factor=xx.x)" from the scale dictionary.
    """
    import matplotlib.pyplot as plt

    # separate data
    # df_forks, df_stars each has columns: repo_name, quarter_label, quarter_value, scaled_value, table_name
    # pivot => rows=quarter_label, columns=repo_name, values=scaled_value

    # first do the pivot for forks
    pivot_forks = df_forks.pivot(index="quarter_label", columns="repo_name", values="scaled_value").fillna(0)
    # then for stars
    pivot_stars = df_stars.pivot(index="quarter_label", columns="repo_name", values="scaled_value").fillna(0)

    # We'll do 2 subplots, one for forks, one for stars
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))

    # 1) Forks
    pivot_forks.plot(kind="bar", ax=axes[0])
    axes[0].set_title("Quarterly Forks (Scaled to global smallest quarter)")
    axes[0].set_ylabel("Scaled Forks")

    # let's put a table below the chart
    from matplotlib.table import Table
    cell_text = pivot_forks.values.tolist()
    col_labels = pivot_forks.columns.tolist()
    row_labels = pivot_forks.index.tolist()

    table_obj = axes[0].table(cellText=cell_text,
                              rowLabels=row_labels,
                              colLabels=col_labels,
                              loc='bottom',
                              cellLoc='center')
    axes[0].set_ylim(top=axes[0].get_ylim()[1]*1.2)

    # 2) Stars
    pivot_stars.plot(kind="bar", ax=axes[1])
    axes[1].set_title("Quarterly Stars (Scaled to global smallest quarter)")
    axes[1].set_ylabel("Scaled Stars")

    cell_text2 = pivot_stars.values.tolist()
    col_labels2 = pivot_stars.columns.tolist()
    row_labels2 = pivot_stars.index.tolist()

    table_obj2 = axes[1].table(cellText=cell_text2,
                               rowLabels=row_labels2,
                               colLabels=col_labels2,
                               loc='bottom',
                               cellLoc='center')
    axes[1].set_ylim(top=axes[1].get_ylim()[1]*1.2)

    # tight layout
    plt.tight_layout()
    plt.show()


def main():
    # 1) Query monthly data for forks => group => scale
    # 2) Query monthly data for stars => group => scale
    conn = connect_db()
    conn.close()

    # fetch monthly
    df_forks_m = get_monthly_data("forks", "forked_at")  # columns: repo_name, yearmonth, monthly_count
    df_stars_m = get_monthly_data("stars", "starred_at") # same

    # group into quarters
    q_forks = group_into_quarters_consecutive(df_forks_m)  # columns: repo_name, quarter_label, quarter_value, table_name
    q_stars = group_into_quarters_consecutive(df_stars_m)

    # scale them
    q_forks, scale_dict_forks = scale_quarter_values(q_forks)
    q_stars, scale_dict_stars = scale_quarter_values(q_stars)

    # rename columns in pivot so that we show scale_factor next to the label
    # e.g. if scale_dict_forks["some/repo"] = 0.25 => we rename that pivot column to "some/repo(sf=0.25)"
    def rename_repo_cols(df, scale_dict):
        # We'll create a dict: old_name->new_name
        rename_map = {}
        for r in df["repo_name"].unique():
            sf = scale_dict[r]
            rename_map[r] = f"{r}(sf={sf:.2f})"
        # We'll create a new column "repo_alias" in the DF so pivot can use that
        # Alternatively, we do it post-pivot
        df["repo_alias"] = df["repo_name"].apply(lambda x: rename_map[x])
        return df

    q_forks = rename_repo_cols(q_forks, scale_dict_forks)
    q_stars = rename_repo_cols(q_stars, scale_dict_stars)

    # now we pivot using "repo_alias" instead of "repo_name"
    # => group them into sub-DataFrames and plot
    def create_pivoted_df(df):
        # columns: repo_alias, quarter_label, scaled_value
        pivot = df.pivot(index="quarter_label", columns="repo_alias", values="scaled_value").fillna(0)
        return pivot

    pivot_forks = create_pivoted_df(q_forks)
    pivot_stars = create_pivoted_df(q_stars)

    # 2 subplots
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, figsize=(10, 8))

    # Forks chart
    pivot_forks.plot(kind="bar", ax=axes[0])
    axes[0].set_title("Quarterly Forks (Scaled to global smallest quarter)")
    axes[0].set_ylabel("Scaled Forks")

    # add table
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

    # Stars chart
    pivot_stars.plot(kind="bar", ax=axes[1])
    axes[1].set_title("Quarterly Stars (Scaled to global smallest quarter)")
    axes[1].set_ylabel("Scaled Stars")

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


############################
# Utility Functions
############################

def get_monthly_data(table_name, date_col):
    """
    Returns monthly aggregates from the specified table+date_col => monthly_count
    """
    conn = connect_db()
    query = f"""
    SELECT
      repo_name,
      DATE_FORMAT({date_col}, '%%Y-%%m') AS yearmonth,
      COUNT(*) AS monthly_count
    FROM {table_name}
    GROUP BY repo_name, YEAR({date_col}), MONTH({date_col})
    ORDER BY repo_name, YEAR({date_col}), MONTH({date_col});
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df["table_name"] = table_name
    return df


def group_into_quarters_consecutive(df):
    """
    Each repo => consecutive lumps of 3 monthly rows => Quarter1, Quarter2,...
    """
    import pandas as pd
    df = df.copy()
    df["dt"] = pd.to_datetime(df["yearmonth"] + "-01")
    df = df.sort_values(["repo_name", "dt"]).reset_index(drop=True)

    out_list = []
    for repo, sub in df.groupby("repo_name"):
        sub = sub.sort_values("dt").reset_index(drop=True)
        sub["quarter_index"] = sub.index // 3
        grouped = sub.groupby("quarter_index")["monthly_count"].sum().reset_index()
        grouped["quarter_label"] = "Quarter" + (grouped["quarter_index"] + 1).astype(str)
        grouped["repo_name"] = repo
        out_list.append(grouped[["repo_name", "quarter_label", "monthly_count"]])
    result = pd.concat(out_list, ignore_index=True)
    return result.rename(columns={"monthly_count": "quarter_value"})


def scale_quarter_values(df):
    """
    Find the global smallest positive quarter_value => for each repo, find that repo's smallest positive => scale factor => multiply
    Return df with new col 'scaled_value', and also a dict {repo_name: scale_factor}
    """
    import numpy as np
    mask_pos = df["quarter_value"] > 0
    if not mask_pos.any():
        # no positive => scaled_value = quarter_value
        df["scaled_value"] = df["quarter_value"]
        return df, {r: 1.0 for r in df["repo_name"].unique()}

    global_smallest = df.loc[mask_pos, "quarter_value"].min()

    scale_dict = {}
    for r in df["repo_name"].unique():
        sub = df[(df["repo_name"] == r) & (df["quarter_value"] > 0)]
        if len(sub) == 0:
            scale_dict[r] = 1.0
        else:
            repo_smallest = sub["quarter_value"].min()
            scale_factor = global_smallest / repo_smallest
            scale_dict[r] = scale_factor

    def calc_scaled(row):
        return row["quarter_value"] * scale_dict[row["repo_name"]]

    df["scaled_value"] = df.apply(calc_scaled, axis=1)
    return df, scale_dict


def main():
    # main logic
    conn = connect_db()
    conn.close()

    # monthly forks
    forks_m = get_monthly_data("forks", "forked_at")
    # monthly stars
    stars_m = get_monthly_data("stars", "starred_at")

    # group each => consecutive lumps of 3 => quarter
    q_forks = group_into_quarters_consecutive(forks_m)
    q_stars = group_into_quarters_consecutive(stars_m)

    # scale them
    q_forks, scale_forks = scale_quarter_values(q_forks)
    q_stars, scale_stars = scale_quarter_values(q_stars)

    # rename columns => "repo_name(sf=xx.xx)"
    def rename_repo_cols(df, scale_dict):
        rename_map = {}
        for r in df["repo_name"].unique():
            sf = scale_dict[r]
            rename_map[r] = f"{r}(sf={sf:.2f})"
        df["repo_alias"] = df["repo_name"].map(rename_map)
        return df

    q_forks = rename_repo_cols(q_forks, scale_forks)
    q_stars = rename_repo_cols(q_stars, scale_stars)

    # pivot each => row=quarter_label, col=repo_alias, val=scaled_value
    def pivot_for_plot(df):
        p = df.pivot(index="quarter_label", columns="repo_alias", values="scaled_value").fillna(0)
        return p

    pf = pivot_for_plot(q_forks)
    ps = pivot_for_plot(q_stars)

    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))

    # 1) forks
    pf.plot(kind="bar", ax=axes[0])
    axes[0].set_title("Quarterly Forks (Scaled to global smallest quarter)")
    axes[0].set_ylabel("Scaled Forks")

    # table below
    from matplotlib.table import Table
    cell_text = pf.values.tolist()
    col_labels = pf.columns.tolist()
    row_labels = pf.index.tolist()

    t0 = axes[0].table(cellText=cell_text,
                       rowLabels=row_labels,
                       colLabels=col_labels,
                       loc='bottom',
                       cellLoc='center')
    axes[0].set_ylim(top=axes[0].get_ylim()[1]*1.2)

    # 2) stars
    ps.plot(kind="bar", ax=axes[1])
    axes[1].set_title("Quarterly Stars (Scaled to global smallest quarter)")
    axes[1].set_ylabel("Scaled Stars")

    cell_text2 = ps.values.tolist()
    col_labels2 = ps.columns.tolist()
    row_labels2 = ps.index.tolist()

    t1 = axes[1].table(cellText=cell_text2,
                       rowLabels=row_labels2,
                       colLabels=col_labels2,
                       loc='bottom',
                       cellLoc='center')
    axes[1].set_ylim(top=axes[1].get_ylim()[1]*1.2)

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
