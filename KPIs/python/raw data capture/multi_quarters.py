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
# 4 lumps => Q01..Q04
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
# We'll produce 3 Figures
############################

def get_engine():
    """
    Create a SQLAlchemy engine to connect MySQL => avoids DBAPI2 warnings
    Example: mysql+mysqlconnector://root:root@localhost/my_kpis_db
    """
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    from sqlalchemy import create_engine
    return create_engine(conn_str)

def read_repos_txt():
    """
    Parse repos.txt lines => { 'owner/repo': bool }
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
    """ Overwrite repos.txt => lines => repo_name=<repo>,enabled=1/0 """
    with open(REPOS_TXT,"w",encoding="utf-8") as f:
        for repo,en_bool in repo_dict.items():
            en_val="1" if en_bool else "0"
            f.write(f"repo_name={repo},enabled={en_val}\n")

def update_repos_txt_with_new(db_repos):
    """
    If new repos appear not in repos.txt => add with enabled=1
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
    dt + months => naive => clamp day if needed
    """
    y= dt.year
    m= dt.month
    d= dt.day
    total_m = (m -1)+ months
    new_y = y + total_m//12
    new_m = (total_m%12)+1
    max_day= monthrange(new_y, new_m)[1]
    new_d= min(d, max_day)
    return datetime(new_y, new_m, new_d)

def lumps_for_repo(subdf):
    """
    For a single repo => lumps Q01..Q04 => from earliest dt => sum monthly_count
    lumps_info => { 'Q01': (start_dt, sumVal or NaN), 'Q02':..., etc.}
    """
    if subdf.empty:
        return {}

    subdf["dt"]= subdf.apply(build_date_for_row, axis=1)
    subdf= subdf.dropna(subset=["dt"]).sort_values("dt")
    if subdf.empty:
        return {}

    earliest= subdf["dt"].min()
    lumps_info={}
    for i in range(1,MAX_LUMPS+1):
        label= f"Q{i:02d}"
        lumps_start= add_months(earliest, 3*(i-1))
        lumps_end  = add_months(earliest, 3*i)
        chunk= subdf[(subdf["dt"]>= lumps_start)&(subdf["dt"]< lumps_end)]
        if chunk.empty:
            sum_val= float('nan')
        else:
            sum_val= chunk["monthly_count"].sum()
        lumps_info[label]=(lumps_start, sum_val)
    return lumps_info

def scale_lumps(lumps_pivot, scaling_repo):
    """
    If lumps_pivot has a nonzero Q01 for scaling_repo => apply scale => rename columns.
    Otherwise => scale=1
    Returns lumps_pivot_scaled, new_col_names_bar, new_col_names_table
    """
    out = lumps_pivot.copy()
    scale_map={}
    col_table=[]
    col_bar=[]
    if (scaling_repo in out.columns) and ("Q01" in out.index):
        scale_val= out.loc["Q01", scaling_repo]
        if pd.isna(scale_val) or scale_val<=0:
            # skip => scale=1
            for c in out.columns:
                scale_map[c]= 1.0
                col_table.append(f"{c}\n(sf=1.00000000)")
                col_bar.append(f"{c}(sf=1.00)")
        else:
            for c in out.columns:
                val_q01= out.loc["Q01", c]
                if pd.isna(val_q01) or val_q01<=0:
                    scale_map[c]=1.0
                else:
                    scale_map[c]= scale_val/ val_q01
            for c in out.columns:
                sf= scale_map[c]
                out[c]*= sf
                col_table.append(f"{c}\n(sf={sf:.8f})")
                col_bar.append(f"{c}(sf={sf:.2f})")
        out.columns= col_table
    else:
        # skip => scale=1
        for c in out.columns:
            scale_map[c]=1.0
            col_table.append(f"{c}\n(sf=1.00000000)")
            col_bar.append(f"{c}(sf=1.00)")
        out.columns= col_table

    return out, col_bar

def lumps_figure(lumps_pivot_scaled, lumps_start_df, earliest_df, bar_cols, title):
    """
    Build a figure => lumps table top-left, lumps start bottom-left,
    bar chart center, earliest date table right
    We label the bar chart with 'title'
    Return lumps_bar2 => the df used for the bar chart
    """
    fig= plt.figure(figsize=(15,6))
    gs= gridspec.GridSpec(nrows=2, ncols=3, figure=fig,
                          width_ratios=[1.8,2,1],
                          height_ratios=[3,1])

    ax_lumps_top= fig.add_subplot(gs[0,0])
    ax_lumps_bot= fig.add_subplot(gs[1,0])
    ax_chart=     fig.add_subplot(gs[:,1])
    ax_earliest=  fig.add_subplot(gs[:,2])

    ax_lumps_top.axis("off")
    lumps_table_data= lumps_pivot_scaled.fillna(0).round(2).astype(str).replace("0.0","").values.tolist()
    lumps_table_rows= lumps_pivot_scaled.index.tolist()
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

    ax_chart.axis("on")
    lumps_bar2= lumps_pivot_scaled.fillna(0)
    lumps_bar2.columns= bar_cols
    lumps_bar2.plot(kind="bar", ax=ax_chart)
    ax_chart.set_title(title)
    ax_chart.set_ylabel("Scaled Summation")
    ax_chart.set_xlabel("")
    ax_chart.set_xticklabels(lumps_bar2.index, rotation=0, ha="center")

    ax_earliest.axis("off")
    earliest_disp= earliest_df.rename(columns={"repo_name":"Repo"})
    e_data= earliest_disp.values.tolist()
    e_cols= earliest_disp.columns.tolist()
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

    return lumps_bar2

def compute_closeness(lumps_bar2, scaling_repo):
    """
    lumps_bar2 => scaled lumps => row= Qxx => lumps, columns => e.g. 'repo(sf=xx.xx)'
    We find the 'scaling_col' => star or fork => we compute average lumps excluding that col => closeness= (scaling lumps/ avg lumps)*100 capped at 100
    Return => lumps_closeness => row= Qxx => closeness
    Also return lumps_avg => row= Qxx => average lumps
    If scaling not found => all closeness=0
    """
    scaling_col=None
    for c in lumps_bar2.columns:
        if c.startswith(scaling_repo+"(") or c.startswith(scaling_repo+"\n("):
            scaling_col= c
            break

    lumps_avg= lumps_bar2.drop(columns=[scaling_col], errors="ignore").mean(axis=1)
    closeness_series=[]
    for lumps_label in lumps_bar2.index:
        scale_val= lumps_bar2.loc[lumps_label, scaling_col] if scaling_col in lumps_bar2.columns else 0.0
        avg_val  = lumps_avg.loc[lumps_label]
        if pd.isna(avg_val) or avg_val<=0:
            closeness=0.0
        else:
            closeness= (scale_val/ avg_val)*100
            if closeness>100:
                closeness=100
        closeness_series.append(closeness)
    lumps_closeness= pd.Series(closeness_series, index=lumps_bar2.index)
    return lumps_closeness, lumps_avg

def main():
    engine= get_engine()

    ############################
    # STEP A: Lumps for stars
    ############################
    # discover repos => update repos.txt
    df_s= pd.read_sql("SELECT repo_name FROM stars", engine)
    all_st= df_s["repo_name"].unique().tolist()
    old_map= read_repos_txt()
    changed=False
    for r in all_st:
        if r not in old_map:
            old_map[r]=True
            changed=True
    if changed:
        write_repos_txt(old_map)
    final_map= read_repos_txt()
    enabled_repos= [r for r,en in final_map.items() if en]

    df_stars= pd.read_sql("""
      SELECT
        repo_name,
        YEAR(starred_at) as y,
        MONTH(starred_at) as m,
        COUNT(*) as monthly_count
      FROM stars
      GROUP BY repo_name,y,m
      ORDER BY repo_name,y,m
    """, engine)
    df_stars= df_stars[ df_stars["repo_name"].isin(enabled_repos) ].copy()

    # lumps => pivot => scale => figure
    lumps_dict_list=[]
    lumps_start_dict_list=[]
    earliest_list=[]
    for repo, subdf in df_stars.groupby("repo_name"):
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
        row_dict={"repo_name":repo}
        for i in range(1,MAX_LUMPS+1):
            lb= f"Q{i:02d}"
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
    lumps_pivot_stars= lumps_df.pivot(index="q_label", columns="repo_name", values="q_value") if not lumps_df.empty else pd.DataFrame()

    lumps_start_stars= pd.DataFrame(lumps_start_dict_list)
    earliest_df_stars= pd.DataFrame(earliest_list)
    earliest_df_stars= earliest_df_stars.drop_duplicates(subset=["repo_name"])
    earliest_df_stars["EarliestDate"]= earliest_df_stars["EarliestDate"].dt.strftime("%Y-%m-%d")
    earliest_df_stars= earliest_df_stars.sort_values("repo_name").reset_index(drop=True)

    # scale lumps => produce figure => lumps_bar2 => for closeness
    lumps_pivot_stars_scaled, bar_cols_stars= scale_lumps(lumps_pivot_stars, SCALING_REPO) if not lumps_pivot_stars.empty else (pd.DataFrame(), [])
    lumps_bar2_stars= pd.DataFrame()
    if not lumps_pivot_stars_scaled.empty:
        lumps_bar2_stars= lumps_figure(lumps_pivot_stars_scaled, lumps_start_stars, earliest_df_stars,
                                       bar_cols_stars, title="Stars Growth Over Time")

    # closeness for stars => lumps_bar2_stars => scaled lumps => compute closeness
    stars_closeness= pd.Series(dtype=float)
    if not lumps_bar2_stars.empty:
        sc, _= compute_closeness(lumps_bar2_stars, SCALING_REPO)
        stars_closeness= sc

    ############################
    # STEP B: Lumps for forks
    ############################
    df_f= pd.read_sql("SELECT repo_name FROM forks", engine)
    all_fk= df_f["repo_name"].unique().tolist()
    old_map= read_repos_txt()
    changed=False
    for r in all_fk:
        if r not in old_map:
            old_map[r]=True
            changed=True
    if changed:
        write_repos_txt(old_map)
    final_map= read_repos_txt()
    enabled_repos= [r for r,en in final_map.items() if en]

    df_forks= pd.read_sql("""
      SELECT
        repo_name,
        YEAR(forked_at) as y,
        MONTH(forked_at) as m,
        COUNT(*) as monthly_count
      FROM forks
      GROUP BY repo_name,y,m
      ORDER BY repo_name,y,m
    """, engine)
    df_forks= df_forks[ df_forks["repo_name"].isin(enabled_repos) ].copy()

    lumps_dict_list_fk=[]
    lumps_start_dict_list_fk=[]
    earliest_list_fk=[]
    for repo, subdf in df_forks.groupby("repo_name"):
        subdf= subdf.dropna(subset=["y","m"]).sort_values(["y","m"])
        if subdf.empty:
            continue
        lumps_info= lumps_for_repo(subdf)
        if not lumps_info:
            continue

        subdf["dt"]= subdf.apply(build_date_for_row, axis=1)
        e_dt= subdf["dt"].min()
        earliest_list_fk.append({"repo_name": repo, "EarliestDate": e_dt})

        for qlab,(qstart,qval) in lumps_info.items():
            lumps_dict_list_fk.append({
                "repo_name": repo,
                "q_label": qlab,
                "q_value": qval
            })
        row_dict={"repo_name":repo}
        for i in range(1,MAX_LUMPS+1):
            lb= f"Q{i:02d}"
            if lb in lumps_info:
                sdt, val= lumps_info[lb]
                if pd.isna(val):
                    row_dict[f"{lb}_start"]=""
                else:
                    row_dict[f"{lb}_start"]= sdt.strftime("%Y-%m-%d")
            else:
                row_dict[f"{lb}_start"]=""
        lumps_start_dict_list_fk.append(row_dict)

    lumps_df_fk= pd.DataFrame(lumps_dict_list_fk)
    lumps_pivot_forks= lumps_df_fk.pivot(index="q_label", columns="repo_name", values="q_value") if not lumps_df_fk.empty else pd.DataFrame()

    lumps_start_forks= pd.DataFrame(lumps_start_dict_list_fk)
    earliest_df_forks= pd.DataFrame(earliest_list_fk)
    earliest_df_forks= earliest_df_forks.drop_duplicates(subset=["repo_name"])
    earliest_df_forks["EarliestDate"]= earliest_df_forks["EarliestDate"].dt.strftime("%Y-%m-%d")
    earliest_df_forks= earliest_df_forks.sort_values("repo_name").reset_index(drop=True)

    lumps_pivot_forks_scaled, bar_cols_forks= scale_lumps(lumps_pivot_forks, SCALING_REPO) if not lumps_pivot_forks.empty else (pd.DataFrame(), [])
    lumps_bar2_forks= pd.DataFrame()
    if not lumps_pivot_forks_scaled.empty:
        lumps_bar2_forks= lumps_figure(lumps_pivot_forks_scaled, lumps_start_forks, earliest_df_forks,
                                       bar_cols_forks, title="Forks Growth Over Time")

    # closeness for forks
    forks_closeness= pd.Series(dtype=float)
    if not lumps_bar2_forks.empty:
        fc, _= compute_closeness(lumps_bar2_forks, SCALING_REPO)
        forks_closeness= fc

    ############################
    # STEP C: Combined user interest figure => lumps Q01..Q04 => single bar per lumps => from 0..100
    # closeness_stars => lumps index => Qxx
    # closeness_forks => lumps index => Qxx
    # combined => 0.6*stars + 0.4*forks (each individually capped at 100) => up to 100
    # We produce a lumps-level table => stars closeness%, forks closeness%, final => "Target reached"
    ############################
    lumps_index= sorted(list(set(stars_closeness.index).union(forks_closeness.index)))  # typically ["Q01","Q02","Q03","Q04"]
    combined_list=[]
    table_rows=[]
    for lumps_label in lumps_index:
        sc_star= 0.0 if lumps_label not in stars_closeness.index else stars_closeness[lumps_label]
        sc_star_clamped= min(sc_star, 100)
        sc_fork= 0.0 if lumps_label not in forks_closeness.index else forks_closeness[lumps_label]
        sc_fork_clamped= min(sc_fork, 100)
        final_val= sc_star_clamped*0.6 + sc_fork_clamped*0.4
        if final_val>100:
            final_val=100  # in theory it won't exceed 100

        combined_list.append(final_val)
        table_rows.append([f"{sc_star:.2f}%", f"{sc_fork:.2f}%", f"{final_val:.2f}%"])

    lumps_combined_df= pd.DataFrame({
        "Combined": combined_list
    }, index=lumps_index)

    ############################
    # Build figure 3 => single bar chart => lumps_index => final combined => "Target"
    # plus a lumps-level table => columns => [stars closeness%, forks closeness%, "Target reached"]
    ############################
    fig3= plt.figure(figsize=(8,6))
    gs3= gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[3,1], figure=fig3)

    # top => bar chart
    ax_bar= fig3.add_subplot(gs3[0,0])
    lumps_combined_df.plot(kind="bar", ax=ax_bar, legend=False)
    ax_bar.set_title("User Interest Growth")
    ax_bar.set_ylabel("Target (0..100)")
    ax_bar.set_xlabel("")
    ax_bar.set_xticklabels(lumps_index, rotation=0, ha="center")

    # rename the bar => "Target" in the legend
    # lumps_combined_df has col "Combined", let's rename => "Target"
    # but we set legend=False. If you want a legend => True, then rename
    lumps_combined_df.columns= ["Target"]

    # bottom => table => lumps => [stars closeness%, forks closeness%, final combined% => "Target reached"]
    ax_table= fig3.add_subplot(gs3[1,0])
    ax_table.axis("off")
    # build table
    col_labels=["Stars closeness%", "Forks closeness%", "Target reached"]
    lumps_table= ax_table.table(cellText=table_rows,
                                rowLabels=lumps_index,
                                colLabels=col_labels,
                                loc="center",
                                cellLoc="center")
    lumps_table.auto_set_font_size(False)
    lumps_table.set_fontsize(12)
    lumps_table.scale(1.2,1.2)
    for _, cell in lumps_table.get_celld().items():
        cell.set_facecolor("white")

    plt.tight_layout()
    plt.show()

    print("Done. We produced 3 figures:\n"
          "1) Stars lumps figure\n"
          "2) Forks lumps figure\n"
          "3) Combined user interest figure (0..100) with a lumps-level table showing 'stars closeness%', 'forks closeness%', 'Target reached'.\n")

if __name__=="__main__":
    main()
