#!/usr/bin/env python
# pr_merge_time_quarters.py

import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from sqlalchemy import create_engine
from calendar import monthrange
import matplotlib.gridspec as gridspec

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# Maximum lumps => 4
############################
MAX_LUMPS = 4

############################
# Single text file => track enabled repos
############################
REPOS_TXT = "repos.txt"

############################
# SELECT WHICH REPO IS THE "SCALING" REPO
############################
SCALING_REPO = "ni/actor-framework"

############################
# Minimally 5 minutes => 5/(24*60)= 0.00347 days
MIN_TIME_DAYS = 5.0/(24.0*60.0)

def get_engine():
    """
    Create a SQLAlchemy engine to connect MySQL => no DBAPI2 warnings.
    Example: mysql+mysqlconnector://root:root@localhost/my_kpis_db
    """
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    from sqlalchemy import create_engine
    return create_engine(conn_str)

def read_repos_txt():
    """
    Parse repos.txt lines of the form:
      repo_name=owner/repo,enabled=1
    Return a dict => { 'owner/repo': True/False }
    """
    if not os.path.isfile(REPOS_TXT):
        return {}
    d = {}
    with open(REPOS_TXT, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#"):
                continue
            parts=line.split(",")
            if len(parts)<2:
                continue
            repo_part=parts[0].split("=")
            en_part=parts[1].split("=")
            if len(repo_part)<2 or len(en_part)<2:
                continue
            repo_str=repo_part[1].strip()
            en_str=en_part[1].strip()
            en_bool = (en_str=="1")
            d[repo_str] = en_bool
    return d

def write_repos_txt(repo_dict):
    """ Overwrite repos.txt """
    with open(REPOS_TXT,"w",encoding="utf-8") as f:
        for repo,en_bool in repo_dict.items():
            en_val="1" if en_bool else "0"
            f.write(f"repo_name={repo},enabled={en_val}\n")

def update_repos_txt_with_new(db_repos):
    """ If new repos appear in DB, add them with enabled=1 """
    old_map= read_repos_txt()
    changed=False
    for r in db_repos:
        if r not in old_map:
            old_map[r]=True
            changed=True
    if changed:
        write_repos_txt(old_map)

def parse_datetime(s):
    """ Convert string => datetime or None """
    if not s:
        return None
    # attempt multiple parse formats if needed
    fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except:
            pass
    return None

def add_months(dt, months):
    """
    dt + 'months' => naive => clamp day if needed
    """
    y=dt.year
    m=dt.month
    d=dt.day
    total_m = (m - 1)+months
    new_y = y + total_m//12
    new_m = (total_m%12)+1
    max_day= monthrange(new_y, new_m)[1]
    new_d= min(d, max_day)
    return datetime(new_y, new_m, new_d)

def lumps_for_repo(repo_df):
    """
    For each PR => we have created_dt, merged_dt.
    'time_to_merge_days' => skip if < MIN_TIME_DAYS => remove those rows.
    Then group the remaining => Q01..Q04 => each lumps=3 months from earliest created_dt.
    lumps_info => { 'Q01': (start_dt, average_merge_days or NaN), ...}
    """
    if repo_df.empty:
        return {}

    earliest= repo_df["created_dt"].min()
    if pd.isna(earliest):
        return {}

    # discard merges that are below 5 minutes => < MIN_TIME_DAYS
    valid_sub= repo_df[repo_df["time_to_merge_days"] >= MIN_TIME_DAYS]
    if valid_sub.empty:
        return {}

    lumps_info={}
    for i in range(1,MAX_LUMPS+1):
        label=f"Q{i:02d}"
        lumps_start= add_months(earliest, 3*(i-1))
        lumps_end  = add_months(earliest, 3*i)
        sub= valid_sub[(valid_sub["created_dt"]>=lumps_start)&(valid_sub["created_dt"]<lumps_end)]
        if sub.empty:
            lumps_info[label]=(lumps_start,float('nan'))
        else:
            lumps_info[label]=(lumps_start, sub["time_to_merge_days"].mean())

    return lumps_info

def main():
    engine= get_engine()

    # read from pulls => must have created_at, merged_at
    # we do NOT skip if there's no first_review_at => ignoring that
    df_pulls= pd.read_sql("""
      SELECT
        repo_name,
        created_at,
        merged_at
      FROM pulls
    """, engine)

    # discover repos => update repos.txt
    all_repos= sorted(df_pulls["repo_name"].unique().tolist())
    old_map= read_repos_txt()
    changed=False
    for r in all_repos:
        if r not in old_map:
            old_map[r]=True
            changed=True
    if changed:
        write_repos_txt(old_map)

    final_map= read_repos_txt()
    enabled_repos= [r for r,en in final_map.items() if en]

    # filter => only enabled
    df_pulls= df_pulls[df_pulls["repo_name"].isin(enabled_repos)].copy()

    # parse to datetime
    df_pulls["created_dt"]= pd.to_datetime(df_pulls["created_at"], errors="coerce")
    df_pulls["merged_dt"] = pd.to_datetime(df_pulls["merged_at"],  errors="coerce")

    # only keep rows that have created_dt, merged_dt => non-null
    df_pulls= df_pulls.dropna(subset=["created_dt","merged_dt"])
    if df_pulls.empty:
        print("No data remains after requiring created_dt, merged_dt.")
        return

    # time_to_merge => days
    df_pulls["time_to_merge_days"]= (df_pulls["merged_dt"] - df_pulls["created_dt"]).dt.total_seconds()/86400.0

    lumps_dict_list=[]
    lumps_start_dict_list=[]
    earliest_list=[]

    for repo, subdf in df_pulls.groupby("repo_name"):
        if subdf.empty:
            continue
        earliest_c= subdf["created_dt"].min()
        if pd.isna(earliest_c):
            continue

        lumps_info= lumps_for_repo(subdf)
        if not lumps_info:
            # skip
            continue

        earliest_list.append({"repo_name": repo, "EarliestDate": earliest_c})

        for qlab,(qstart,qval) in lumps_info.items():
            lumps_dict_list.append({
                "repo_name": repo,
                "q_label": qlab,
                "q_value": qval
            })

        row_dict={"repo_name":repo}
        for i in range(1,MAX_LUMPS+1):
            lb=f"Q{i:02d}"
            if lb in lumps_info:
                sdt, val= lumps_info[lb]
                if pd.isna(val):
                    row_dict[f"{lb}_start"]=""
                else:
                    row_dict[f"{lb}_start"]= sdt.strftime("%Y-%m-%d")
            else:
                row_dict[f"{lb}_start"]=""
        lumps_start_dict_list.append(row_dict)

    lumps_df= pd.DataFrame(lumps_dict_list)
    if lumps_df.empty:
        print("No lumps data available after processing. Possibly merges <5m or no created_dt found.")
        return

    lumps_pivot= lumps_df.pivot(index="q_label", columns="repo_name", values="q_value")
    lumps_start_df= pd.DataFrame(lumps_start_dict_list)

    earliest_df= pd.DataFrame(earliest_list).drop_duplicates(subset=["repo_name"])
    earliest_df["EarliestDate"]= earliest_df["EarliestDate"].dt.strftime("%Y-%m-%d")
    earliest_df= earliest_df.sort_values("repo_name").reset_index(drop=True)

    # SCALING => forcibly uses SCALING_REPO's Q01 => skip if missing or <=0
    lumps_pivot_scaled= lumps_pivot.copy()
    scale_map={}
    new_cols_table=[]
    new_cols_bar=[]

    if (SCALING_REPO in lumps_pivot_scaled.columns) and ("Q01" in lumps_pivot_scaled.index):
        scale_ref_val= lumps_pivot_scaled.loc["Q01", SCALING_REPO]
        if pd.isna(scale_ref_val) or scale_ref_val<=0:
            # skip => scale=1
            for repo in lumps_pivot_scaled.columns:
                scale_map[repo]=1.0
                new_cols_table.append(f"{repo}\n(sf=1.00)")
                new_cols_bar.append(f"{repo}(sf=1.00)")
        else:
            for repo in lumps_pivot_scaled.columns:
                val_q01= lumps_pivot_scaled.loc["Q01", repo]
                if pd.isna(val_q01) or val_q01<=0:
                    scale_map[repo]=1.0
                else:
                    sf= scale_ref_val/val_q01
                    scale_map[repo]= sf
            for repo in lumps_pivot_scaled.columns:
                sf= scale_map[repo]
                lumps_pivot_scaled[repo]*= sf
                new_cols_table.append(f"{repo}\n(sf={sf:.2f})")
                new_cols_bar.append(f"{repo}(sf={sf:.2f})")
        lumps_pivot_scaled.columns= new_cols_table
    else:
        # no scale
        for repo in lumps_pivot_scaled.columns:
            scale_map[repo]=1.0
            new_cols_table.append(f"{repo}\n(sf=1.00)")
            new_cols_bar.append(f"{repo}(sf=1.00)")
        lumps_pivot_scaled.columns= new_cols_table

    lumps_bar= lumps_pivot_scaled.copy()
    lumps_bar.columns= new_cols_bar

    ############################
    # build main figure => lumps table top-left, lumps start bottom-left, bar chart center, earliest date right
    ############################
    fig= plt.figure(figsize=(15,6))
    gs= gridspec.GridSpec(nrows=2, ncols=3, figure=fig,
                          width_ratios=[1.8,2,1],
                          height_ratios=[3,1])

    ax_lumps_top= fig.add_subplot(gs[0,0])
    ax_lumps_bot= fig.add_subplot(gs[1,0])
    ax_chart=     fig.add_subplot(gs[:,1])
    ax_earliest=  fig.add_subplot(gs[:,2])

    # lumps table
    ax_lumps_top.axis("off")
    # fillna("") => 2 decimals
    lumps_table_data= lumps_pivot_scaled.fillna(0).round(2).astype(str).replace("0.0","").values.tolist()
    lumps_table_rows= lumps_pivot_scaled.index.tolist()   # Q01..Q04
    lumps_table_cols= lumps_pivot_scaled.columns.tolist()
    lumps_table= ax_lumps_top.table(cellText=lumps_table_data,
                                    rowLabels=lumps_table_rows,
                                    colLabels=lumps_table_cols,
                                    loc="center",
                                    cellLoc="center")
    lumps_table.auto_set_font_size(False)
    lumps_table.set_fontsize(12)
    lumps_table.scale(1.2,1.2)
    for _, cell in lumps_table.get_celld().items():
        cell.set_facecolor("white")

    # lumps start => bottom-left
    ax_lumps_bot.axis("off")
    lumps_start_cols= ["repo_name"]+[f"Q{i:02d}_start" for i in range(1,MAX_LUMPS+1)]
    lumps_start_disp= lumps_start_df[lumps_start_cols].copy()
    # no rounding needed for dates
    lumps_start_data= lumps_start_disp.values.tolist()
    lumps_start_headers= lumps_start_disp.columns.tolist()
    lumps_start_tab= ax_lumps_bot.table(cellText=lumps_start_data,
                                        colLabels=lumps_start_headers,
                                        loc="center",
                                        cellLoc="center")
    lumps_start_tab.auto_set_font_size(False)
    lumps_start_tab.set_fontsize(12)
    lumps_start_tab.scale(1.2,1.2)
    for _, cell in lumps_start_tab.get_celld().items():
        cell.set_facecolor("white")

    # bar chart => lumps => row=Qxx => x-axis => side by side
    lumps_bar2= lumps_bar.fillna(0)
    lumps_bar2.plot(kind="bar", ax=ax_chart)
    ax_chart.set_title("Merge time over time (days)")
    ax_chart.set_ylabel("Scaled Merge Time (days)")
    ax_chart.set_xlabel("")
    ax_chart.set_xticklabels(lumps_bar2.index, rotation=0, ha="center")

    # earliest date => right
    ax_earliest.axis("off")
    earliest_df= earliest_df.rename(columns={"repo_name":"Repo"})
    e_data= earliest_df.values.tolist()
    e_cols= earliest_df.columns.tolist()
    e_tab= ax_earliest.table(cellText=e_data,
                             colLabels=e_cols,
                             loc="center",
                             cellLoc="center")
    e_tab.auto_set_font_size(False)
    e_tab.set_fontsize(12)
    e_tab.scale(1.2,1.2)
    for _, cell in e_tab.get_celld().items():
        cell.set_facecolor("white")

    plt.tight_layout()
    plt.show()

    ############################
    # figure 2 => lumps average ignoring scaling col + bar for scaling col
    ############################
    scaling_col= None
    for c in lumps_bar2.columns:
        if c.startswith(SCALING_REPO+"(") or c.startswith(SCALING_REPO+"\n("):
            scaling_col= c
            break

    lumps_avg= lumps_bar2.drop(columns=[scaling_col], errors="ignore").mean(axis=1)

    lumps_plot_df= pd.DataFrame(index=lumps_bar2.index)
    lumps_plot_df["Average (excl. "+SCALING_REPO+")"] = lumps_avg
    if scaling_col in lumps_bar2.columns:
        lumps_plot_df[scaling_col]= lumps_bar2[scaling_col]
    else:
        lumps_plot_df[scaling_col or "ScalingRepoMissing"]= 0.0

    fig2, ax2= plt.subplots(figsize=(7,4))
    lumps_plot_df.plot(kind="bar", ax=ax2)
    ax2.set_title("Avg lumps (excl. scaling repo) vs. scaling repo lumps")
    ax2.set_ylabel("Scaled Merge Time (days)")
    ax2.set_xlabel("")
    ax2.set_xticklabels(lumps_plot_df.index, rotation=0, ha="center")
    plt.tight_layout()
    plt.show()


if __name__=="__main__":
    main()
