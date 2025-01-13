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
import importlib.util

############################
# MySQL Credentials
############################
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "my_kpis_db"

############################
# 4 lumps => Q01..Q04
############################
MAX_LUMPS = 4

############################
# Single text file => track enabled repos (still used for toggling on/off).
# We'll rely on repo_list.py for start_date + enabling as well, but we can keep repos.txt if needed.
############################
REPOS_TXT = "repos.txt"

############################
# We have a separate `repo_list.py` that we import
############################
REPO_LIST_PY = "repo_list.py"

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

def import_repo_list(repo_list_py_path):
    """
    Dynamically import repo_list.py which must define a variable: repo_list = [ {...}, ... ]
    Return the list of repos.
    """
    if not os.path.isfile(repo_list_py_path):
        print(f"Could not find {repo_list_py_path} => returning empty list.")
        return []
    import importlib.util
    spec = importlib.util.spec_from_file_location("repo_list_module", repo_list_py_path)
    repo_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(repo_module)
    # we expect repo_module.repo_list
    return getattr(repo_module, "repo_list", [])

def parse_date_str(s):
    """
    Parse 'YYYY-MM-DD' => Python datetime.date or None
    We'll store as a datetime to be consistent with lumps logic but with time=00:00
    """
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except:
        return None

def add_months(dt, months):
    """
    dt + 'months' => naive approach => clamp day if needed
    """
    y=dt.year
    m=dt.month
    d=dt.day
    total_m = (m -1)+months
    new_y = y + total_m//12
    new_m = (total_m%12)+1
    max_day= monthrange(new_y, new_m)[1]
    new_d= min(d, max_day)
    return datetime(new_y, new_m, new_d)

def lumps_for_repo(subdf, lumps_base_date):
    """
    subdf => all PRs for that repo that remain after skipping merges <5m
    lumps_base_date => datetime from repo_list (the 'start_date').
    We'll create Q01..Q04 => 3 month intervals from lumps_base_date.
    For each lumps => average subdf["time_to_merge_days"] in [start, end).
    Return lumps_info => { 'Q01': (start_dt, average_merge_days or NaN), ... }
    """
    if subdf.empty or lumps_base_date is None:
        return {}

    lumps_info={}
    for i in range(1, MAX_LUMPS+1):
        label=f"Q{i:02d}"
        lumps_start= add_months(lumps_base_date, 3*(i-1))
        lumps_end  = add_months(lumps_base_date, 3*i)
        chunk= subdf[(subdf["created_dt"]>= lumps_start)&(subdf["created_dt"]< lumps_end)]
        if chunk.empty:
            lumps_info[label]=(lumps_start, float('nan'))
        else:
            lumps_info[label]=(lumps_start, chunk["time_to_merge_days"].mean())

    return lumps_info

def main():
    ############################
    # 1) Import repo_list.py
    ############################
    my_repo_list = import_repo_list(REPO_LIST_PY)
    # build a map => { "repo_name": {"start_date": datetime, "enabled": bool} }
    repo_info_map = {}
    for r in my_repo_list:
        rname = r.get("repo_name", "")
        sdate_str = r.get("start_date", "")
        en       = r.get("enabled", False)
        rdate = parse_date_str(sdate_str)
        repo_info_map[rname] = {
            "start_date": rdate,
            "enabled": en
        }

    # connect to DB
    engine = get_engine()
    # read from pulls => must have created_at, merged_at
    df_pulls= pd.read_sql("""
      SELECT
        repo_name,
        created_at,
        merged_at
      FROM pulls
    """, engine)

    # discover repos => update repos.txt (still used for toggling on/off if needed)
    all_repos= sorted(df_pulls["repo_name"].unique().tolist())

    # ============ If you still want to keep repos.txt approach =============
    old_map = {}
    if os.path.isfile(REPOS_TXT):
        with open(REPOS_TXT, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#"):
                    continue
                parts=line.split(",")
                if len(parts)<2:
                    continue
                rp=parts[0].split("=")[1].strip()
                en=parts[1].split("=")[1].strip()
                old_map[rp]= (en=="1")

    changed=False
    for r in all_repos:
        if r not in old_map:
            old_map[r] = True
            changed=True

    if changed:
        # rewrite repos.txt
        with open(REPOS_TXT,"w",encoding="utf-8") as f:
            for rr, enb in old_map.items():
                en_val="1" if enb else "0"
                f.write(f"repo_name={rr},enabled={en_val}\n")
    # ======================================================================

    # combine the enabling from repo_info_map AND repos.txt if desired:
    # We'll do final_enabled if both are True (or either, depending on your preference).
    # For simplicity => final_enabled = repo_info_map[r]["enabled"]
    # or if r not in repo_info_map => skip
    final_enabled_repos = []
    for r in all_repos:
        if r in repo_info_map and repo_info_map[r]["enabled"]:
            final_enabled_repos.append(r)

    # filter => only final_enabled_repos
    df_pulls= df_pulls[df_pulls["repo_name"].isin(final_enabled_repos)].copy()
    if df_pulls.empty:
        print("No data remains after checking final_enabled_repos.")
        return

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

    # skip merges < 5 minutes => time_to_merge_days < MIN_TIME_DAYS
    df_pulls= df_pulls[df_pulls["time_to_merge_days"] >= MIN_TIME_DAYS]
    if df_pulls.empty:
        print("No data remains after removing merges <5 minutes.")
        return

    lumps_dict_list=[]
    lumps_start_dict_list=[]
    earliest_list=[]

    for repo_name, subdf in df_pulls.groupby("repo_name"):
        # get lumps_base_date from repo_list
        lumps_base= None
        if repo_name in repo_info_map:
            lumps_base= repo_info_map[repo_name]["start_date"]
        if lumps_base is None:
            # no valid start_date => skip lumps
            continue

        # lumps_for_repo => group from lumps_base
        lumps_info= lumps_for_repo(subdf, lumps_base)
        if not lumps_info:
            # skip
            continue

        # earliest date => for reference only => min created_dt
        e_c= subdf["created_dt"].min()
        earliest_list.append({"repo_name": repo_name, "EarliestDate": e_c})

        # store lumps info => lumps_dict_list
        for qlab, (qstart, qval) in lumps_info.items():
            lumps_dict_list.append({
                "repo_name": repo_name,
                "q_label": qlab,
                "q_value": qval
            })

        # lumps start => row= repo => col= Q01_start..Q04_start
        row_dict={"repo_name": repo_name}
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
        print("No lumps data available after processing. Possibly merges <5m or no lumps_base_date in repo_list.")
        return

    lumps_pivot= lumps_df.pivot(index="q_label", columns="repo_name", values="q_value")
    lumps_start_df= pd.DataFrame(lumps_start_dict_list)

    # earliest => for reference
    earliest_df= pd.DataFrame(earliest_list).drop_duplicates(subset=["repo_name"])
    earliest_df= earliest_df.sort_values("repo_name").reset_index(drop=True)
    earliest_df["EarliestDate"]= earliest_df["EarliestDate"].dt.strftime("%Y-%m-%d")

    ########## SCALING => forcibly uses SCALING_REPO's Q01 => skip if missing or <=0
    lumps_pivot_scaled= lumps_pivot.copy()
    scale_map={}
    new_cols_table=[]
    new_cols_bar=[]

    if (SCALING_REPO in lumps_pivot_scaled.columns) and ("Q01" in lumps_pivot_scaled.index):
        scale_ref_val= lumps_pivot_scaled.loc["Q01", SCALING_REPO]
        if pd.isna(scale_ref_val) or scale_ref_val<=0:
            # skip => scale=1
            for r in lumps_pivot_scaled.columns:
                scale_map[r]=1.0
                new_cols_table.append(f"{r}\n(sf=1.00)")
                new_cols_bar.append(f"{r}(sf=1.00)")
        else:
            for r in lumps_pivot_scaled.columns:
                val_q01= lumps_pivot_scaled.loc["Q01", r]
                if pd.isna(val_q01) or val_q01<=0:
                    scale_map[r]=1.0
                else:
                    sf= scale_ref_val/ val_q01
                    scale_map[r]= sf
            for r in lumps_pivot_scaled.columns:
                sf= scale_map[r]
                lumps_pivot_scaled[r]*= sf
                new_cols_table.append(f"{r}\n(sf={sf:.2f})")
                new_cols_bar.append(f"{r}(sf={sf:.2f})")
        lumps_pivot_scaled.columns= new_cols_table
    else:
        # skip scaling => scale=1
        for r in lumps_pivot_scaled.columns:
            scale_map[r]=1.0
            new_cols_table.append(f"{r}\n(sf=1.00)")
            new_cols_bar.append(f"{r}(sf=1.00)")
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

    # lumps table => 2 decimals
    ax_lumps_top.axis("off")
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

    # lumps start => bottom-left (just dates, no rounding needed)
    ax_lumps_bot.axis("off")
    lumps_start_cols= ["repo_name"]+[f"Q{i:02d}_start" for i in range(1,MAX_LUMPS+1)]
    lumps_start_disp= lumps_start_df[lumps_start_cols].copy()
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
