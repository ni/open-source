#!/usr/bin/env python
# forks_quarters.py

import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# Number of lumps (like "quarters")
############################
QUARTERS = 4  # user sets how many lumps => Q01..Qxx

############################
# The single text file that tracks repos
############################
REPOS_TXT = "repos.txt"

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def read_repos_txt():
    """
    Parse repos.txt lines of form:
      repo_name=owner/repo,enabled=1
    Return a dict: { "owner/repo": True/False } meaning enabled or disabled
    or a DataFrame, but let's do a dict for convenience
    """
    if not os.path.isfile(REPOS_TXT):
        return {}
    result = {}
    with open(REPOS_TXT, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#"):
                continue
            # example: repo_name=owner/repo,enabled=1
            parts = line.split(",")
            # parts[0] => repo_name=owner/repo
            # parts[1] => enabled=1
            if len(parts) < 2:
                continue
            repo_part = parts[0].split("=")
            en_part   = parts[1].split("=")
            if len(repo_part)<2 or len(en_part)<2:
                continue
            repo_str = repo_part[1].strip()
            enabled_str = en_part[1].strip()
            enabled_bool = (enabled_str=="1")
            result[repo_str] = enabled_bool
    return result

def write_repos_txt(repo_dict):
    """
    Overwrite repos.txt with lines from repo_dict => {repo_name: bool}
    Format: repo_name=...,enabled=1 or 0
    """
    with open(REPOS_TXT, "w", encoding="utf-8") as f:
        for repo_name, en_bool in repo_dict.items():
            en_val = "1" if en_bool else "0"
            line = f"repo_name={repo_name},enabled={en_val}\n"
            f.write(line)

def update_repos_txt_with_new(db_repos):
    """
    db_repos => list of repos from the DB
    We'll read repos.txt => existing dict
    For each db repo not in the dict, add it with enabled=1 (or 0 if you prefer)
    Then rewrite repos.txt
    """
    repo_map = read_repos_txt()
    changed=False
    for r in db_repos:
        if r not in repo_map:
            # add it => default enabled=1
            repo_map[r] = True
            changed=True
    if changed:
        write_repos_txt(repo_map)

def get_enabled_repos_from_txt():
    """
    Return a list of repos that have enabled=1 in repos.txt
    """
    repo_map = read_repos_txt()
    # filter for only enabled
    return [r for r, en in repo_map.items() if en]

def get_monthly_forks():
    """
    Query monthly fork data, grouping by year,month => columns [repo_name, y,m, monthly_count]
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
    return df

def build_date_for_row(row):
    """ Convert (y,m) => datetime """
    try:
        return datetime(int(row["y"]), int(row["m"]), 1)
    except:
        return None

def group_into_configurable_quarters(df, quarters=4):
    """
    1) find global min_dt,max_dt
    2) chunk into quarters lumps
    3) label lumps => Q01..Qxx
    4) sum monthly_count => q_value
    """
    df = df.copy()
    df["dt"] = df.apply(build_date_for_row, axis=1)
    df = df.dropna(subset=["dt"])
    if df.empty:
        return pd.DataFrame(columns=["repo_name","q_label","q_value"])
    df = df.sort_values(["repo_name","dt"]).reset_index(drop=True)
    min_dt = df["dt"].min()
    max_dt = df["dt"].max()
    if min_dt == max_dt:
        df["q_label"] = "Q01"
        df["q_value"] = df["monthly_count"]
        return df[["repo_name","q_label","q_value"]]

    total_days = (max_dt - min_dt).days
    if total_days<1:
        total_days=1
    step = total_days/quarters
    boundaries=[min_dt]
    for i in range(1, quarters):
        boundaries.append(min_dt + timedelta(days=i*step))
    boundaries.append(max_dt + timedelta(seconds=1))

    def label_dt(row_dt):
        for i in range(quarters):
            if boundaries[i] <= row_dt < boundaries[i+1]:
                return f"Q{(i+1):02d}"
        return f"Q{quarters:02d}"  # fallback

    df["q_label"] = df["dt"].apply(label_dt)
    g = df.groupby(["repo_name","q_label"])["monthly_count"].sum().reset_index()
    g = g.rename(columns={"monthly_count":"q_value"})
    g = g.sort_values(["repo_name","q_label"]).reset_index(drop=True)
    return g

def create_quarterly_figure(df, title_str):
    """ pivot => row=q_label, col=repo_name => side-by-side bars, table below """
    pivot_df = df.pivot(index="q_label", columns="repo_name", values="q_value").fillna(0)
    fig, ax = plt.subplots(figsize=(10, 6))
    pivot_df.plot(kind="bar", ax=ax)
    ax.set_title(title_str)
    ax.set_ylabel("Count")
    # table
    cell_text = pivot_df.values.tolist()
    col_labels = pivot_df.columns.tolist()
    row_labels = pivot_df.index.tolist()
    t0=ax.table(cellText=cell_text,
                rowLabels=row_labels,
                colLabels=col_labels,
                loc='bottom',
                cellLoc='center')
    ax.set_ylim(top=ax.get_ylim()[1]*1.2)
    plt.tight_layout()
    return fig

def main():
    # 1) get distinct repos from DB => add them to repos.txt if missing
    df_forks = get_monthly_forks()
    # also read from stars if we want, but let's assume forks is enough or we do union
    all_repos_forks = df_forks["repo_name"].unique().tolist()

    # if you want stars:
    conn = connect_db()
    st_query = """SELECT repo_name FROM stars"""
    df_st = pd.read_sql(st_query, conn)
    conn.close()
    all_repos_stars = df_st["repo_name"].unique().tolist()

    # union
    all_repos = sorted(list(set(all_repos_forks + all_repos_stars)))
    update_repos_txt_with_new(all_repos)

    # 2) read which repos are enabled
    enabled_repos = get_enabled_repo
