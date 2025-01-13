#!/usr/bin/env python
# multi_data_quarters.py

import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
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
# Path to repos.txt
############################
REPOS_TXT = "repos.txt"

############################
# Path to repo_list.py
############################
REPO_LIST_PATH = "repo_list.py"

############################
# SCALING REPO => from env or default
############################
DEFAULT_SCALING_REPO = "ni/actor-framework"
SCALING_REPO = os.getenv("SCALING_REPO", DEFAULT_SCALING_REPO)

############################
# GLOBAL => number of YEARS to analyze
############################
GLOBAL_YEARS_TO_ANALYZE = 2

################################################################
# 1) read repos.txt => skip repos with enabled=0
################################################################
def read_repos_txt(path):
    """
    repos.txt lines look like:
      repo_name=dotnet/core,enabled=1
      repo_name=ni/actor-framework,enabled=1
      repo_name=ni/grpc-labview,enabled=0
      ...
    Return a dict => { 'dotnet/core': True, 'ni/actor-framework': True, 'ni/grpc-labview': False, ... }
    """
    if not os.path.isfile(path):
        print(f"Could not find {path}, returning empty dict.")
        return {}
    d= {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#"):
                continue
            parts=line.split(",")
            if len(parts)<2:
                continue
            # e.g. "repo_name=dotnet/core", "enabled=1"
            repo_part= parts[0].split("=")
            en_part=   parts[1].split("=")
            if len(repo_part)<2 or len(en_part)<2:
                continue
            rn_str= repo_part[1].strip()
            en_str= en_part[1].strip()
            en_bool= (en_str=="1")
            d[rn_str]= en_bool
    return d

################################################################
# 2) read repo_list.py => ignoring end_date
################################################################
def import_repo_list(path):
    if not os.path.isfile(path):
        print(f"Could not find {path}, returning empty list.")
        return []
    spec = importlib.util.spec_from_file_location("repo_list_module", path)
    repo_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(repo_module)
    return getattr(repo_module, "repo_list", [])

def parse_date_str(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except:
        return None

def build_repo_info_map(repo_list_data):
    """
    each entry => { 'enabled': bool, 'owner': str, 'repo': str,
                    'start_date': 'YYYY-MM-DD', 'end_date': '...' => ignored }
    unify => 'owner/repo' => store { 'enabled':..., 'start_dt':... }
    """
    info_map= {}
    for entry in repo_list_data:
        en   = entry.get("enabled", False)
        ow   = entry.get("owner", "")
        rp   = entry.get("repo", "")
        sdat = parse_date_str(entry.get("start_date",""))
        name = f"{ow}/{rp}"
        info_map[name]= {
            "enabled": en,
            "start_dt": sdat
        }
    return info_map

################################################################
# 3) DB engine creation
################################################################
def get_engine():
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    return create_engine(conn_str)

################################################################
# 4) lumps_for_repo => ignoring end_date, lumps_end = forced_start_dt + GLOBAL_YEARS_TO_ANALYZE
################################################################
def build_date_for_row(row):
    try:
        yy= int(row["y"])
        mm= int(row["m"])
        return datetime(yy, mm, 1)
    except:
        return None

def add_months(dt, months):
    y= dt.year
    m= dt.month
    d= dt.day
    total_m= (m-1)+ months
    new_y= y + total_m//12
    new_m= (total_m%12)+1
    last_day= monthrange(new_y,new_m)[1]
    new_d= min(d,last_day)
    return datetime(new_y,new_m,new_d)

def lumps_for_repo(subdf, forced_start_dt=None):
    """
    lumps_end => forced_start_dt + GLOBAL_YEARS_TO_ANALYZE
    Q01..Q04 => each covers 3 months from forced_start_dt
    If forced_start_dt is None => fallback to earliest subdf dt
    """
    if subdf.empty:
        return {}

    subdf["dt"]= subdf.apply(build_date_for_row, axis=1)
    subdf= subdf.dropna(subset=["dt"]).sort_values("dt")
    if subdf.empty:
        return {}

    if not forced_start_dt:
        forced_start_dt= subdf["dt"].min()

    lumps_end= forced_start_dt + relativedelta(years=GLOBAL_YEARS_TO_ANALYZE)
    # filter => [forced_start_dt, lumps_end)
    subdf= subdf[(subdf["dt"]>= forced_start_dt)&(subdf["dt"]< lumps_end)]
    if subdf.empty:
        return {}

    lumps_info= {}
    for i in range(1, MAX_LUMPS+1):
        label= f"Q{i:02d}"
        sdt= add_months(forced_start_dt, 3*(i-1))
        edt= add_months(forced_start_dt, 3*i)
        chunk= subdf[(subdf["dt"]>= sdt)&(subdf["dt"]< edt)]
        if chunk.empty:
            sum_val= float('nan')
        else:
            sum_val= chunk["monthly_count"].sum()
        lumps_info[label]= (sdt, sum_val)

    return lumps_info

################################################################
# 5) lumps_figure => bigger figure => 50% more area
################################################################
def lumps_figure(lumps_pivot_scaled, lumps_start_df, earliest_df, bar_cols, title):
    """
    'EarliestDate shall be the same as Q01_start.'
    => We'll forcibly override earliest_df's 'EarliestDate' with the 'Q01_start' from lumps_start_df.
    Now figure size => ~1.5 times bigger area than original (15,6).
    We'll do (18.37,7.35) => ~1.2247 factor => ~50% more area.
    """
    # override earliest with Q01_start
    if not lumps_start_df.empty:
        q01_map= {}
        for idx, row in lumps_start_df.iterrows():
            rn= row["repo_name"]
            q01_val= row.get("Q01_start", None) or ""
            q01_map[rn]= q01_val
        if not earliest_df.empty:
            for idx, row in earliest_df.iterrows():
                rn= row["repo_name"]
                if rn in q01_map:
                    earliest_df.loc[idx, "EarliestDate"]= q01_map[rn]

    fig= plt.figure(figsize=(18.37,7.35))  # ~50% bigger area than (15,6)
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

    # lumps start => if empty => minimal row
    ax_lumps_bot.axis("off")
    if lumps_start_df.empty:
        lumps_start_df= pd.DataFrame([
            {
                "repo_name": "N/A",
                "Q01_start": "N/A",
                "Q02_start": "N/A",
                "Q03_start": "N/A",
                "Q04_start": "N/A"
            }
        ])

    lumps_start_cols= ["repo_name"]+[f"Q{i:02d}_start" for i in range(1,MAX_LUMPS+1)]
    for col in lumps_start_cols:
        if col not in lumps_start_df.columns:
            lumps_start_df[col]= "N/A"

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

    # bar chart
    lumps_bar2= lumps_pivot_scaled.fillna(0).copy()
    lumps_bar2.columns= bar_cols
    lumps_bar2.plot(kind="bar", ax=ax_chart)
    ax_chart.set_title(title)
    ax_chart.set_ylabel("Scaled Summation")
    ax_chart.set_xlabel("")
    ax_chart.set_xticklabels(lumps_bar2.index, rotation=0, ha="center")

    # earliest => if empty => minimal row
    ax_earliest.axis("off")
    if earliest_df.empty:
        earliest_df= pd.DataFrame([{"repo_name":"N/A", "EarliestDate":"N/A"}])
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
    return lumps_bar2

################################################################
# lumps_closeness_figure => bigger figure => ~1.5 times area
################################################################
def lumps_closeness_figure(lumps_bar2, scaling_repo, dataset_name="Stars"):
    """
    Original was (8,5) => let's do ~ (9.8,6.1) => ~ sqrt(1.5)* each dimension => ~1.5 area
    """
    scaling_col= None
    for c in lumps_bar2.columns:
        if c.startswith(scaling_repo+"(") or c.startswith(scaling_repo+"\n("):
            scaling_col= c
            break

    lumps_avg= lumps_bar2.drop(columns=[scaling_col], errors="ignore").mean(axis=1)
    chart_df= pd.DataFrame(index= lumps_bar2.index)
    chart_df["Target"] = lumps_avg
    if scaling_col in lumps_bar2.columns:
        chart_df[scaling_col] = lumps_bar2[scaling_col]
    else:
        chart_df["ScalingRepoMissing"]= 0.0

    table_rows=[]
    for lumps_lbl in chart_df.index:
        targ_val= chart_df.loc[lumps_lbl, "Target"]
        if scaling_col in chart_df.columns:
            scale_val= chart_df.loc[lumps_lbl, scaling_col]
        else:
            scale_val= 0.0
        if pd.isna(targ_val) or targ_val<=0:
            closeness=0.0
        else:
            closeness= (scale_val/ targ_val)*100
            if closeness>100:
                closeness=100.0
        table_rows.append([f"{closeness:.2f}%"])

    fig= plt.figure(figsize=(9.8,6.1))  # ~50% bigger area than (8,5)
    gs= gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[3,1], figure=fig)

    ax_top= fig.add_subplot(gs[0,0])
    chart_df.plot(kind="bar", ax=ax_top)
    ax_top.set_title(f"{dataset_name} vs. {scaling_repo} (Closeness)")
    ax_top.set_ylabel("Scaled Summation")
    ax_top.set_xlabel("")
    ax_top.set_xticklabels(chart_df.index, rotation=0, ha="center")

    ax_bot= fig.add_subplot(gs[1,0])
    ax_bot.axis("off")
    closeness_data= [[row[0]] for row in table_rows]
    closeness_tab= ax_bot.table(cellText=closeness_data,
                                rowLabels= chart_df.index,
                                colLabels=["Target reached"],
                                loc="center",
                                cellLoc="center")
    closeness_tab.auto_set_font_size(False)
    closeness_tab.set_fontsize(12)
    closeness_tab.scale(1.2,1.2)
    for _, cell in closeness_tab.get_celld().items():
        cell.set_facecolor("white")

    plt.tight_layout()
    plt.show()

################################################################
# scale_lumps => same logic
################################################################
def scale_lumps(lumps_pivot, scaling_repo):
    out= lumps_pivot.copy()
    col_table=[]
    col_bar=[]
    if (scaling_repo in out.columns) and ("Q01" in out.index):
        scale_val= out.loc["Q01", scaling_repo]
        if pd.isna(scale_val) or scale_val<=0:
            for c in out.columns:
                col_table.append(f"{c}\n(sf=1.00000000)")
                col_bar.append(f"{c}(sf=1.00)")
        else:
            scale_map={}
            for c in out.columns:
                v= out.loc["Q01", c]
                if pd.isna(v) or v<=0:
                    scale_map[c]=1.0
                else:
                    scale_map[c]= scale_val / v
            for c in out.columns:
                sf= scale_map[c]
                out[c]*= sf
                col_table.append(f"{c}\n(sf={sf:.8f})")
                col_bar.append(f"{c}(sf={sf:.2f})")
        out.columns= col_table
    else:
        # skip => scale=1
        for c in out.columns:
            col_table.append(f"{c}\n(sf=1.00000000)")
            col_bar.append(f"{c}(sf=1.00)")
        out.columns= col_table

    return out, col_bar


############################################
# MAIN
############################################
def main():
    # 1) read repos.txt => skip repos with enabled=0
    repos_txt_map= read_repos_txt(REPOS_TXT)

    # 2) read repo_list.py => parse start_date => ignore end_date
    repo_list_data= import_repo_list(REPO_LIST_PATH)
    repo_info_map= build_repo_info_map(repo_list_data)

    engine= get_engine()

    print(f"Analyzing data => SCALING_REPO={SCALING_REPO}, 50% bigger figure area, lumps end = start_date + {GLOBAL_YEARS_TO_ANALYZE} year(s).")
    print("EarliestDate is forcibly set to Q01_start.\n")

    #####################################################
    # A) STARS lumps => monthly_count
    #####################################################
    df_stars= pd.read_sql("""
      SELECT
        repo_name         AS full_repo,
        YEAR(starred_at)  AS y,
        MONTH(starred_at) AS m,
        COUNT(*)          AS monthly_count
      FROM stars
      GROUP BY repo_name, y, m
      ORDER BY repo_name, y, m
    """, engine)

    lumps_dict_st= []
    lumps_start_rows_st= []
    earliest_rows_st= []
    if not df_stars.empty:
        for (repo_name), subdf in df_stars.groupby("full_repo"):
            if repo_name not in repos_txt_map or not repos_txt_map[repo_name]:
                continue
            if repo_name not in repo_info_map or not repo_info_map[repo_name]["enabled"]:
                continue
            forced_start_dt= repo_info_map[repo_name]["start_dt"]

            lumps_info= lumps_for_repo(subdf, forced_start_dt=forced_start_dt)
            if lumps_info:
                row_dict= {"repo_name": repo_name}
                q01_dt= lumps_info["Q01"][0] if "Q01" in lumps_info else None
                if q01_dt:
                    # We forcibly set earliest= Q01_start
                    earliest_rows_st.append({"repo_name": repo_name, "EarliestDate": q01_dt})

                for lbl,(startdt, val) in lumps_info.items():
                    lumps_dict_st.append({
                        "repo_name": repo_name,
                        "q_label": lbl,
                        "q_value": val
                    })
                    if pd.isna(val):
                        row_dict[f"{lbl}_start"]= ""
                    else:
                        row_dict[f"{lbl}_start"]= startdt.strftime("%Y-%m-%d")
                lumps_start_rows_st.append(row_dict)

    lumps_df_stars= pd.DataFrame(lumps_dict_st)
    lumps_pivot_stars= pd.DataFrame()
    lumps_start_df_stars= pd.DataFrame(lumps_start_rows_st)
    earliest_df_stars= pd.DataFrame(earliest_rows_st)
    if not lumps_df_stars.empty:
        lumps_pivot_stars= lumps_df_stars.pivot(index="q_label", columns="repo_name", values="q_value")

    lumps_pivot_stars_scaled, col_bar_stars= scale_lumps(lumps_pivot_stars, SCALING_REPO) if not lumps_pivot_stars.empty else (pd.DataFrame(),[])
    lumps_bar2_stars= pd.DataFrame()
    if not lumps_pivot_stars_scaled.empty:
        lumps_bar2_stars= lumps_figure(lumps_pivot_stars_scaled, lumps_start_df_stars, earliest_df_stars,
                                       col_bar_stars, "Stars Growth Over Time")
        lumps_closeness_figure(lumps_bar2_stars, SCALING_REPO, dataset_name="Stars")

    #####################################################
    # B) FORKS lumps => monthly_count
    #####################################################
    df_forks= pd.read_sql("""
      SELECT
        repo_name        AS full_repo,
        YEAR(forked_at)  AS y,
        MONTH(forked_at) AS m,
        COUNT(*)         AS monthly_count
      FROM forks
      GROUP BY repo_name, y, m
      ORDER BY repo_name, y, m
    """, engine)

    lumps_dict_fk= []
    lumps_start_rows_fk= []
    earliest_rows_fk= []
    if not df_forks.empty:
        for (repo_name), subdf in df_forks.groupby("full_repo"):
            if repo_name not in repos_txt_map or not repos_txt_map[repo_name]:
                continue
            if repo_name not in repo_info_map or not repo_info_map[repo_name]["enabled"]:
                continue
            forced_start_dt= repo_info_map[repo_name]["start_dt"]

            lumps_info= lumps_for_repo(subdf, forced_start_dt=forced_start_dt)
            if lumps_info:
                row_dict= {"repo_name": repo_name}
                q01_dt= lumps_info["Q01"][0] if "Q01" in lumps_info else None
                if q01_dt:
                    earliest_rows_fk.append({"repo_name": repo_name, "EarliestDate": q01_dt})

                for lbl,(startdt,val) in lumps_info.items():
                    lumps_dict_fk.append({
                        "repo_name": repo_name,
                        "q_label": lbl,
                        "q_value": val
                    })
                    if pd.isna(val):
                        row_dict[f"{lbl}_start"]= ""
                    else:
                        row_dict[f"{lbl}_start"]= startdt.strftime("%Y-%m-%d")
                lumps_start_rows_fk.append(row_dict)

    lumps_df_forks= pd.DataFrame(lumps_dict_fk)
    lumps_pivot_forks= pd.DataFrame()
    lumps_start_df_forks= pd.DataFrame(lumps_start_rows_fk)
    earliest_df_forks= pd.DataFrame(earliest_rows_fk)
    if not lumps_df_forks.empty:
        lumps_pivot_forks= lumps_df_forks.pivot(index="q_label", columns="repo_name", values="q_value")

    lumps_pivot_forks_scaled, col_bar_forks= scale_lumps(lumps_pivot_forks, SCALING_REPO) if not lumps_pivot_forks.empty else (pd.DataFrame(),[])
    lumps_bar2_forks= pd.DataFrame()
    if not lumps_pivot_forks_scaled.empty:
        lumps_bar2_forks= lumps_figure(lumps_pivot_forks_scaled, lumps_start_df_forks, earliest_df_forks,
                                       col_bar_forks, "Forks Growth Over Time")
        lumps_closeness_figure(lumps_bar2_forks, SCALING_REPO, dataset_name="Forks")

    #####################################################
    # C) PULL REQUEST lumps => monthly_count
    #####################################################
    df_pulls= pd.read_sql("""
      SELECT
        repo_name            AS full_repo,
        YEAR(created_at)     AS y,
        MONTH(created_at)    AS m,
        COUNT(*)             AS monthly_count
      FROM pulls
      GROUP BY repo_name, y, m
      ORDER BY repo_name, y, m
    """, engine)

    lumps_dict_pr= []
    lumps_start_rows_pr= []
    earliest_rows_pr= []
    if not df_pulls.empty:
        for (repo_name), subdf in df_pulls.groupby("full_repo"):
            if repo_name not in repos_txt_map or not repos_txt_map[repo_name]:
                continue
            if repo_name not in repo_info_map or not repo_info_map[repo_name]["enabled"]:
                continue
            forced_start_dt= repo_info_map[repo_name]["start_dt"]

            lumps_info= lumps_for_repo(subdf, forced_start_dt=forced_start_dt)
            if lumps_info:
                row_dict= {"repo_name": repo_name}
                q01_dt= lumps_info["Q01"][0] if "Q01" in lumps_info else None
                if q01_dt:
                    earliest_rows_pr.append({"repo_name": repo_name, "EarliestDate": q01_dt})

                for lbl,(startdt,val) in lumps_info.items():
                    lumps_dict_pr.append({
                        "repo_name": repo_name,
                        "q_label": lbl,
                        "q_value": val
                    })
                    if pd.isna(val):
                        row_dict[f"{lbl}_start"]= ""
                    else:
                        row_dict[f"{lbl}_start"]= startdt.strftime("%Y-%m-%d")
                lumps_start_rows_pr.append(row_dict)

    lumps_df_pr= pd.DataFrame(lumps_dict_pr)
    lumps_pivot_pr= pd.DataFrame()
    lumps_start_df_pr= pd.DataFrame(lumps_start_rows_pr)
    earliest_df_pr= pd.DataFrame(earliest_rows_pr)
    if not lumps_df_pr.empty:
        lumps_pivot_pr= lumps_df_pr.pivot(index="q_label", columns="repo_name", values="q_value")

    lumps_pivot_pr_scaled, col_bar_pr= scale_lumps(lumps_pivot_pr, SCALING_REPO) if not lumps_pivot_pr.empty else (pd.DataFrame(),[])
    lumps_bar2_pr= pd.DataFrame()
    if not lumps_pivot_pr_scaled.empty:
        lumps_bar2_pr= lumps_figure(lumps_pivot_pr_scaled, lumps_start_df_pr, earliest_df_pr,
                                   col_bar_pr, "Pull Requests Over Time")
        lumps_closeness_figure(lumps_bar2_pr, SCALING_REPO, dataset_name="PullRequests")

    #####################################################
    # D) ISSUES => sum(comments) => monthly_count
    #####################################################
    df_issues= pd.read_sql("""
      SELECT
        repo_name           AS full_repo,
        YEAR(created_at)    AS y,
        MONTH(created_at)   AS m,
        SUM(comments)       AS monthly_count
      FROM issues
      GROUP BY repo_name, y, m
      ORDER BY repo_name, y, m
    """, engine)
    df_issues["monthly_count"]= df_issues["monthly_count"].fillna(0)

    lumps_dict_iss= []
    lumps_start_rows_iss= []
    earliest_rows_iss= []
    if not df_issues.empty:
        for (repo_name), subdf in df_issues.groupby("full_repo"):
            if repo_name not in repos_txt_map or not repos_txt_map[repo_name]:
                continue
            if repo_name not in repo_info_map or not repo_info_map[repo_name]["enabled"]:
                continue
            forced_start_dt= repo_info_map[repo_name]["start_dt"]

            lumps_info= lumps_for_repo(subdf, forced_start_dt=forced_start_dt)
            if lumps_info:
                row_dict= {"repo_name": repo_name}
                q01_dt= lumps_info["Q01"][0] if "Q01" in lumps_info else None
                if q01_dt:
                    earliest_rows_iss.append({"repo_name": repo_name, "EarliestDate": q01_dt})

                for lbl,(startdt,val) in lumps_info.items():
                    lumps_dict_iss.append({
                        "repo_name": repo_name,
                        "q_label": lbl,
                        "q_value": val
                    })
                    if pd.isna(val):
                        row_dict[f"{lbl}_start"]= ""
                    else:
                        row_dict[f"{lbl}_start"]= startdt.strftime("%Y-%m-%d")
                lumps_start_rows_iss.append(row_dict)

    lumps_df_iss= pd.DataFrame(lumps_dict_iss)
    lumps_pivot_iss= pd.DataFrame()
    lumps_start_df_iss= pd.DataFrame(lumps_start_rows_iss)
    earliest_df_iss= pd.DataFrame(earliest_rows_iss)
    if not lumps_df_iss.empty:
        lumps_pivot_iss= lumps_df_iss.pivot(index="q_label", columns="repo_name", values="q_value")

    lumps_pivot_iss_scaled, col_bar_iss= scale_lumps(lumps_pivot_iss, SCALING_REPO) if not lumps_pivot_iss.empty else (pd.DataFrame(),[])
    lumps_bar2_iss= pd.DataFrame()
    if not lumps_pivot_iss_scaled.empty:
        lumps_bar2_iss= lumps_figure(lumps_pivot_iss_scaled, lumps_start_df_iss, earliest_df_iss,
                                     col_bar_iss, "Issue Engagement Over Time")
        lumps_closeness_figure(lumps_bar2_iss, SCALING_REPO, dataset_name="Issues")

    ############################################################
    # E) MONTHLY ACTIVE CONTRIBUTORS => 0.2 * Pull + 0.8 * Issue
    ############################################################
    lumps_all_labels= sorted(list(set(lumps_pivot_pr.index).union(lumps_pivot_iss.index))) if not lumps_pivot_pr.empty or not lumps_pivot_iss.empty else []
    lumps_all_repos= sorted(list(set(lumps_pivot_pr.columns).union(lumps_pivot_iss.columns))) if not lumps_pivot_pr.empty or not lumps_pivot_iss.empty else []

    mac_data= []
    for lbl in lumps_all_labels:
        row_vals=[]
        for rn in lumps_all_repos:
            pr_val=0.0
            iss_val=0.0
            if (lbl in lumps_pivot_pr.index) and (rn in lumps_pivot_pr.columns):
                tmp= lumps_pivot_pr.loc[lbl, rn]
                pr_val= 0.0 if pd.isna(tmp) else tmp
            if (lbl in lumps_pivot_iss.index) and (rn in lumps_pivot_iss.columns):
                tmp= lumps_pivot_iss.loc[lbl, rn]
                iss_val= 0.0 if pd.isna(tmp) else tmp
            mac_val= 0.2*pr_val + 0.8*iss_val
            row_vals.append(mac_val)
        mac_data.append(row_vals)

    lumps_mac= pd.DataFrame(mac_data, index=lumps_all_labels, columns=lumps_all_repos)
    lumps_mac_scaled, col_bar_mac= scale_lumps(lumps_mac, SCALING_REPO) if not lumps_mac.empty else (pd.DataFrame(),[])
    lumps_bar2_mac= pd.DataFrame()
    if not lumps_mac_scaled.empty:
        lumps_bar2_mac= lumps_figure(lumps_mac_scaled, pd.DataFrame([]), pd.DataFrame([]),
                                     col_bar_mac, "Monthly Active Contributors (0.2 Pull + 0.8 Issue)")
        lumps_closeness_figure(lumps_bar2_mac, SCALING_REPO, dataset_name="MonthlyActiveContrib")

    print("\nDone! Repos are skip/enabled via repos.txt, ignoring end_date from repo_list.py,")
    print("EarliestDate forced to Q01_start, 50% bigger figure area, and SCALING_REPO configurable via env var.")
    print(f"Currently SCALING_REPO={SCALING_REPO}. We produced lumps+closeness for Stars/Forks/PullRequests/Issues + MAC => 10 figures.")


if __name__=="__main__":
    main()
