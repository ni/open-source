#!/usr/bin/env python
# multi_quarters.py

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
# Lumps => Q01..Q04
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
# MODE => "stars" or "forks"
############################
MODE = "forks"   # can set "forks" to measure forks

def get_engine():
    """
    Create a SQLAlchemy engine to connect to MySQL => avoids DBAPI2 warnings
    Example: mysql+mysqlconnector://root:root@localhost/my_kpis_db
    """
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    from sqlalchemy import create_engine
    return create_engine(conn_str)

def read_repos_txt():
    """
    Parse repos.txt lines: repo_name=<repo>,enabled=1
    Return dict { 'owner/repo': bool }
    """
    if not os.path.isfile(REPOS_TXT):
        return {}
    d = {}
    with open(REPOS_TXT,"r",encoding="utf-8") as f:
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
            d[repo_str]=en_bool
    return d

def write_repos_txt(repo_dict):
    """ Overwrite repos.txt with lines: repo_name=<repo>,enabled=1/0 """
    with open(REPOS_TXT,"w",encoding="utf-8") as f:
        for repo,en_bool in repo_dict.items():
            en_val="1" if en_bool else "0"
            f.write(f"repo_name={repo},enabled={en_val}\n")

def update_repos_txt_with_new(db_repos):
    """
    If new repos discovered not in repos.txt => add them with enabled=1
    """
    old_map= read_repos_txt()
    changed=False
    for r in db_repos:
        if r not in old_map:
            old_map[r]=True
            changed=True
    if changed:
        write_repos_txt(old_map)

def build_date_for_row(row):
    """
    Convert (y,m)=> datetime(YYYY,MM,1).
    """
    try:
        yy=int(row["y"])
        mm=int(row["m"])
        return datetime(yy, mm, 1)
    except:
        return None

def add_months(dt, months):
    """
    dt + 'months' => naive approach => clamp day if needed
    """
    y= dt.year
    m= dt.month
    d= dt.day
    total_m = (m-1)+months
    new_y = y + total_m//12
    new_m = (total_m%12)+1
    max_day= monthrange(new_y, new_m)[1]
    new_d= min(d, max_day)
    return datetime(new_y, new_m, new_d)

def lumps_for_repo(subdf):
    """
    For a single repo => Q01..Q04 => each lumps is 3 months from that repo's earliest dt.
    lumps_info => { 'Q01': (start_dt, sumVal or NaN), ...}
    If MODE="stars", that sumVal is sum of monthly_count. If "forks", likewise.
    """
    if subdf.empty:
        return {}

    subdf["dt"] = subdf.apply(build_date_for_row, axis=1)
    subdf= subdf.dropna(subset=["dt"]).sort_values("dt")
    if subdf.empty:
        return {}

    earliest= subdf["dt"].min()
    lumps_info={}
    for i in range(1, MAX_LUMPS+1):
        label=f"Q{i:02d}"
        lumps_start= add_months(earliest, 3*(i-1))
        lumps_end  = add_months(earliest, 3*i)
        chunk= subdf[(subdf["dt"]>= lumps_start)&(subdf["dt"]< lumps_end)]
        if chunk.empty:
            sum_val= float('nan')
        else:
            sum_val= chunk["monthly_count"].sum()
        lumps_info[label]=(lumps_start, sum_val)
    return lumps_info

def main():
    engine= get_engine()

    ############################
    # discover repos => update repos.txt
    ############################
    if MODE=="stars":
        # read from 'stars' => columns => [repo_name, starred_at]
        df_mode= pd.read_sql("""
          SELECT repo_name FROM stars
        """, engine)
    elif MODE=="forks":
        df_mode= pd.read_sql("""
          SELECT repo_name FROM forks
        """, engine)
    else:
        print(f"Invalid MODE={MODE}, must be 'stars' or 'forks'. Exiting.")
        return

    all_mode= df_mode["repo_name"].unique().tolist()

    # also read from "other" table if you want to unify logic => we skip that for brevity
    # we do an intersection if needed. For now we just have all_mode
    old_map= read_repos_txt()
    changed=False
    for r in all_mode:
        if r not in old_map:
            old_map[r]=True
            changed=True
    if changed:
        write_repos_txt(old_map)

    final_map= read_repos_txt()
    enabled_repos=[r for r,en in final_map.items() if en]
    if MODE=="stars":
        df_main= pd.read_sql("""
          SELECT
            repo_name,
            YEAR(starred_at) as y,
            MONTH(starred_at) as m,
            COUNT(*) as monthly_count
          FROM stars
          GROUP BY repo_name,y,m
          ORDER BY repo_name,y,m
        """, engine)
    else:
        # forks
        df_main= pd.read_sql("""
          SELECT
            repo_name,
            YEAR(forked_at) as y,
            MONTH(forked_at) as m,
            COUNT(*) as monthly_count
          FROM forks
          GROUP BY repo_name,y,m
          ORDER BY repo_name,y,m
        """, engine)

    df_main= df_main[df_main["repo_name"].isin(enabled_repos)].copy()

    lumps_dict_list=[]
    lumps_start_dict_list=[]
    earliest_list=[]

    # group by repo => lumps_for_repo
    for repo, subdf in df_main.groupby("repo_name"):
        subdf= subdf.dropna(subset=["y","m"]).sort_values(["y","m"])
        if subdf.empty:
            continue
        lumps_info= lumps_for_repo(subdf)
        if not lumps_info:
            continue
        # earliest => min dt
        subdf["dt"]= subdf.apply(build_date_for_row, axis=1)
        e_dt= subdf["dt"].min()
        earliest_list.append({"repo_name": repo, "EarliestDate": e_dt})

        for qlab,(qstart,qval) in lumps_info.items():
            lumps_dict_list.append({
                "repo_name": repo,
                "q_label": qlab,
                "q_value": qval
            })
        # lumps start => row= repo => col= Q01_start.. Q04_start
        row_dict={"repo_name": repo}
        for i in range(1, MAX_LUMPS+1):
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
        print("No lumps data => possibly empty or no monthly_count found.")
        return

    # pivot => lumps => row=Qxx, col=repo => sumVal
    lumps_pivot= lumps_df.pivot(index="q_label", columns="repo_name", values="q_value")

    lumps_start_df= pd.DataFrame(lumps_start_dict_list)
    earliest_df= pd.DataFrame(earliest_list)
    earliest_df= earliest_df.drop_duplicates(subset=["repo_name"])
    earliest_df["EarliestDate"]= earliest_df["EarliestDate"].dt.strftime("%Y-%m-%d")
    earliest_df= earliest_df.sort_values("repo_name").reset_index(drop=True)

    ############### SCALING => forcibly uses SCALING_REPO's Q01 => skip if missing or <=0
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
                new_cols_table.append(f"{r}\n(sf=1.00000000)")
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
                new_cols_table.append(f"{r}\n(sf={sf:.8f})")
                new_cols_bar.append(f"{r}(sf={sf:.2f})")
        lumps_pivot_scaled.columns= new_cols_table
    else:
        # skip scaling => scale=1
        for r in lumps_pivot_scaled.columns:
            scale_map[r]=1.0
            new_cols_table.append(f"{r}\n(sf=1.00000000)")
            new_cols_bar.append(f"{r}(sf=1.00)")
        lumps_pivot_scaled.columns= new_cols_table

    lumps_bar= lumps_pivot_scaled.copy()
    lumps_bar.columns= new_cols_bar

    ############################
    # Build main figure => lumps table top-left, lumps start bottom-left, bar chart center, earliest date right
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

    # lumps start => bottom-left (just dates)
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
    ax_chart.set_title(f"{MODE.capitalize()} growth over time")
    ax_chart.set_ylabel("Scaled Summation")
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

    ############################################################
    # Figure 2 => average lumps ignoring the scaling repo, plus a bar for SCALING_REPO
    # Then a table => for each lumps => % from 0..100 => (scaling lumps / average lumps)*100
    ############################################################
    scaling_col= None
    for c in lumps_bar2.columns:
        if c.startswith(SCALING_REPO+"(") or c.startswith(SCALING_REPO+"\n("):
            scaling_col= c
            break

    lumps_avg= lumps_bar2.drop(columns=[scaling_col], errors="ignore").mean(axis=1)

    lumps_plot_df= pd.DataFrame(index=lumps_bar2.index)
    lumps_plot_df["Avg (excl. SCALING)"] = lumps_avg
    if scaling_col in lumps_bar2.columns:
        lumps_plot_df[scaling_col]= lumps_bar2[scaling_col]
    else:
        lumps_plot_df[scaling_col or "ScalingRepoMissing"]= 0.0

    # compute closeness => (scaling lumps / average lumps) *100
    # if lumps_avg=0 => define closeness=0 or np.nan
    closeness_list=[]
    for lumps_label in lumps_plot_df.index:
        avg_val= lumps_plot_df.loc[lumps_label,"Avg (excl. SCALING)"]
        scale_val= lumps_plot_df.loc[lumps_label, scaling_col] if scaling_col in lumps_plot_df.columns else 0.0
        if pd.isna(avg_val) or (avg_val<=0):
            closeness= 0.0
        else:
            closeness= (scale_val/ avg_val)*100.0
        closeness_list.append(closeness)

    lumps_closeness_df= pd.DataFrame({
        "Lumps": lumps_plot_df.index,
        "Closeness%": [f"{v:.2f}%" for v in closeness_list]
    })

    fig2= plt.figure(figsize=(7,5))
    gs2= gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[3,1], figure=fig2)

    ax_bar= fig2.add_subplot(gs2[0,0])
    lumps_plot_df.plot(kind="bar", ax=ax_bar)
    ax_bar.set_title("Avg lumps vs. scaling repo lumps")
    ax_bar.set_ylabel("Scaled Summation")
    ax_bar.set_xlabel("")
    ax_bar.set_xticklabels(lumps_plot_df.index, rotation=0, ha="center")

    # second subplot => table => closeness
    ax_tab= fig2.add_subplot(gs2[1,0])
    ax_tab.axis("off")
    # closeness table => lumps label as rowLabels, col => "Closeness%"
    closeness_data= lumps_closeness_df[["Closeness%"]].values.tolist()
    lumps_labels= lumps_closeness_df["Lumps"].tolist()
    col_labels= ["Closeness%"]
    closeness_table= ax_tab.table(cellText=closeness_data,
                                  rowLabels=lumps_labels,
                                  colLabels=col_labels,
                                  loc="center",
                                  cellLoc="center")
    closeness_table.auto_set_font_size(False)
    closeness_table.set_fontsize(12)
    closeness_table.scale(1.2,1.2)
    for _, cell in closeness_table.get_celld().items():
        cell.set_facecolor("white")

    plt.tight_layout()
    plt.show()

if __name__=="__main__":
    main()
