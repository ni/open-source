# config.py

import os

def get_env_or_prompt(env_key, prompt_text, default=None):
    val = os.environ.get(env_key)
    if not val:
        d_str = f" (Default: {default})" if default else ""
        user_in = input(f"{prompt_text}{d_str}: ")
        if user_in.strip():
            val = user_in.strip()
        elif default is not None:
            val = str(default)
        else:
            val = ""
        os.environ[env_key] = val
    return val

def load_db_config():
    host = get_env_or_prompt("DB_HOST", "Enter DB Host", "localhost")
    port = get_env_or_prompt("DB_PORT", "Enter DB Port", "3306")
    user = get_env_or_prompt("DB_USER", "Enter DB User (Read-Only)", "readonly_user")
    password = get_env_or_prompt("DB_PASS", "Enter DB Password", "root")
    dbname = get_env_or_prompt("DB_NAME", "Enter DB Name", "my_kpis_analytics_db")
    return {
        "host": host,
        "port": int(port),
        "user": user,
        "password": password,
        "database": dbname
    }

def get_scaling_repo():
    return get_env_or_prompt("SCALING_REPO", "Enter scaling repo (e.g. 'facebook/react')", "")

def get_num_fiscal_quarters():
    val = get_env_or_prompt("NUM_FISCAL_QUARTERS", "Number of Fiscal Quarters to Analyze", "4")
    try:
        return int(val)
    except:
        return 4

def get_scale_factor_window():
    """
    Configurable scale factor window (days).
    Default = 180
    """
    val = get_env_or_prompt("SCALE_FACTOR_WINDOW", "Days for Scale Factor Window", "180")
    try:
        return int(val)
    except:
        return 180
