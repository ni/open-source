# repo_list.py
"""
Each repo dict has:
  - owner (str)
  - repo  (str)
  - start_date (str 'YYYY-MM-DD')
  - end_date   (str 'YYYY-MM-DD' or "")
  - enabled    (bool)
"""

repo_list = [
    {
        "enabled": False,
        "owner": "ni",
        "repo": "grpc-labview",
        "start_date": "2020-11-02",
        "end_date": "2021-11-02"
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "labview-memory-management-tools",
        "start_date": "2018-06-30",
        "end_date": "2019-06-30"
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "niveristand-custom-device-development-tools",
        "start_date": "2016-07-13",
        "end_date": "2017-07-13"
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "niveristand-custom-device-build-tools",
        "start_date": "2017-06-14",
        "end_date": "2018-06-14"
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "labview-icon-editor",
        "start_date": "2024-01-01",
        "end_date": "2025-01-01"
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "actor-framework",
        "start_date": "2024-08-15",
        "end_date": "2025-08-15"
    },
    {
        "enabled": False,
        "owner": "facebook",
        "repo": "react",
        "start_date": "2013-06-03",
        "end_date": "2014-06-03"
    },
    {
        "enabled": True,
        "owner": "tensorflow",
        "repo": "tensorflow",
        "start_date": "2015-11-09",
        "end_date": "2016-11-09"
    },
    {
        "enabled": True,
        "owner": "dotnet",
        "repo": "core",
        "start_date": "2014-11-12",
        "end_date": "2015-11-12"
    }
]
