############################################################
# config_reader.py
# Reads aggregator weighting from config.ini (if present),
# else uses defaults
############################################################

import configparser
import os

def load_config(file_path="config.ini"):
    """
    Reads aggregator weighting from config.ini, if present.
    Fallback for any missing aggregator keys.
    Returns a dict with "aggregator" subkey.
    """
    parser = configparser.ConfigParser()
    if os.path.exists(file_path):
        parser.read(file_path)
    config_data = {}

    aggregator = {}
    if "aggregator" in parser.sections():
        sec = parser["aggregator"]
        aggregator["velocity_merges"]    = sec.getfloat("velocity_merges",    fallback=0.4)
        aggregator["velocity_closedIss"] = sec.getfloat("velocity_closedIss", fallback=0.2)
        aggregator["velocity_closedPR"]  = sec.getfloat("velocity_closedPR",  fallback=0.4)

        aggregator["uig_forks"] = sec.getfloat("uig_forks", fallback=0.4)
        aggregator["uig_stars"] = sec.getfloat("uig_stars", fallback=0.6)

        aggregator["mac_mainWeight"] = sec.getfloat("mac_mainWeight", fallback=0.8)
        aggregator["mac_subWeight"]  = sec.getfloat("mac_subWeight",  fallback=0.2)

        aggregator["sei_velocity"] = sec.getfloat("sei_velocity", fallback=0.3)
        aggregator["sei_uig"]      = sec.getfloat("sei_uig",      fallback=0.2)
        aggregator["sei_mac"]      = sec.getfloat("sei_mac",      fallback=0.5)
    else:
        # Defaults if aggregator not found
        aggregator["velocity_merges"]    = 0.4
        aggregator["velocity_closedIss"] = 0.2
        aggregator["velocity_closedPR"]  = 0.4

        aggregator["uig_forks"] = 0.4
        aggregator["uig_stars"] = 0.6

        aggregator["mac_mainWeight"] = 0.8
        aggregator["mac_subWeight"]  = 0.2

        aggregator["sei_velocity"] = 0.3
        aggregator["sei_uig"]      = 0.2
        aggregator["sei_mac"]      = 0.5

    config_data["aggregator"] = aggregator
    return config_data
