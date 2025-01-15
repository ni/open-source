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
        "start_date": ""  # let metadata DB handle it
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "labview-memory-management-tools",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "niveristand-custom-device-development-tools",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "niveristand-custom-device-build-tools",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "ni",
        "repo": "labview-icon-editor",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": True,
        "owner": "ni",
        "repo": "actor-framework",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "facebook",
        "repo": "react",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "tensorflow",
        "repo": "tensorflow",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "dotnet",
        "repo": "core",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "keysight",
        "repo": "Rhme-2016",
        "start_date": ""  # let metadata DB handle it,
    },
    {
        "enabled": False,
        "owner": "keysight",
        "repo": "Jlsca",
        "start_date": ""  # let metadata DB handle it
    },
    {
        "enabled": False,
        "owner": "keysight",
        "repo": "optee_fuzzer",
        "start_date": ""  # let metadata DB handle it
    }
]