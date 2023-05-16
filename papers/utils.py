""" Utils functions for the project
"""
# System packages
from pathlib import Path

# Third-party packages
import pandas as pd
import yaml
from joblib import Parallel, delayed
import paramiko 

# Constant definitions
with open("./config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

tech = {key: val["map"] for key, val in config["technologies"].items()}
tech_colors = {key: val["color"] for key, val in config["technologies"].items()}
tech_order = config["tech_order"]

tech_dict = {
    new_tech: tech for tech, new_techs in tech.items() for new_tech in new_techs
}

data_path = "/data/switch/wave_cases_v2/"
data_path_remote="/data/switch/wave_cases_v2/"

def get_single_df(
    scenario: str, fname: str, load_zone: str = None, fpath="outputs", *args, **kwargs
):
    fname = data_path + scenario+ '/'+ fpath +'/'+ fname
    return (
        pd.read_csv(fname, *args, **kwargs)
        .pipe(tech_map)
        .pipe(timepoint_map)
        .assign(scenario=scenario)
    )

def get_single_df_sftp(
    hostname: str, username: str, scenario: str, fname: str, load_zone: str = None, fpath="outputs", *args, **kwargs
):
    fname = data_path_remote + scenario + "/" + fpath + "/"+  fname
    # open an SSH connection
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username)
    # read the file using SFTP
    sftp = client.open_sftp()
    remote_file = sftp.open(fname)
    dataframe = pd.read_csv(remote_file, *args, **kwargs)
    remote_file.close()
    # close the connections
    sftp.close()
    client.close()
    return (
        dataframe
        .pipe(tech_map)
        .pipe(timepoint_map)
        .assign(scenario=scenario)
    )

def get_data(scenario: str, fname: str, fpath="outputs", *args, **kwargs):
    """Small wrapper of get same file for multiple scenarios

    It uses joblib to read a df in each thread which by default (-1) uses
    all threads available in the computer.
    """
    if isinstance(scenario, list):
        fname_dfs = Parallel(n_jobs=-1)(
            delayed(get_single_df)(sce, fname, fpath=fpath,*args, **kwargs)
            for sce in scenario
        )

        return pd.concat(fname_dfs)
    else:
        return get_single_df(scenario, fname, fpath=fpath, *args, **kwargs)

def get_data_sftp(hostname: str, username: str, scenario: str, fname: str, fpath="outputs", *args, **kwargs):
    """Small wrapper of get same file for multiple scenarios

    It uses joblib to read a df in each thread which by default (-1) uses
    all threads available in the computer.
    """
    if isinstance(scenario, list):
        fname_dfs = Parallel(n_jobs=-1)(
            delayed(get_single_df_sftp)(hostname, username, sce, fname, fpath=fpath,*args, **kwargs)
            for sce in scenario
        )

        return pd.concat(fname_dfs)
    else:
        return get_single_df_sftp(hostname, username, scenario, fname, fpath=fpath, *args, **kwargs)



def tech_map(df):
    """ Apply custom technology map"""
    # Create new column if data contains technology
    df = df.copy()
    if "gen_tech" in df.columns:
        df["tech_map"] = df["gen_tech"].map(tech_dict).astype("category")
        assert df["tech_map"].isnull().values.any() == False
    return df


def timepoint_map(df):
    """ Convert timepoint to datetime object"""
    df = df.copy()
    columns = ["timestamp", "timepoint"]
    if any(val in df.columns for val in columns):
        try:
            df["datetime"] = pd.to_datetime(df["timepoint"], format="%Y%m%d%H")
        except:
            print("exception")
            if "timestamp" in columns: 
                print("timestamp in column")
#                 df["datetime"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H")
        return df
    return df


PLOT_PARAMS = {
    "font.size": 7,
    "font.family": "Source Sans Pro",
    "legend.fontsize": 6,
    "legend.handlelength": 2,
    "figure.dpi": 120,
    "lines.markersize": 4,
    "lines.markeredgewidth": 0.5,
    "lines.linewidth": 1.5,
    "axes.titlesize": 8,
    "axes.labelsize": 8,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "xtick.major.width": 0.6,
    "xtick.minor.width": 0.4,
    "ytick.major.width": 0.6,
    "ytick.minor.width": 0.4,
    "ytick.minor.size": 2.5,
    "ytick.major.size": 5,
    "xtick.direction": "in",
    "ytick.direction": "in",
    "grid.linewidth": 0.1,
    "savefig.dpi":300,
    "legend.frameon": False,
    "legend.framealpha": 0.8,
    #"legend.edgecolor": 0.9,
    "legend.borderpad": 0.2,
    "legend.columnspacing": 1.5,
    "legend.labelspacing":  0.4,
}
