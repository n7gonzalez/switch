""" This post-process selects which technologies can provide reserves"""
# Standard packages
import os
import shutil

# Third-party packages
import pandas as pd

from switch_model.wecc.get_inputs.register_post_process import post_process_step


@post_process_step(
    msg="Adding a minimum state of charge requirement"
)
def post_process(func_config):
    """This function sets to zero the column that allows each candidate technology to
    provide"""

    breakpoint()
    min_soc_value = func_config["value"]

    fname = "generation_projects_info.csv"
    df = pd.read_csv(fname)

    # Energy sources to exclude from reserves
    storage_techs = ["Battery_Storage"]

    # Create min_soc column
    df.loc[:, "gen_min_soc"] = "."

    # Set to zero column that allows technology to provide reserves
    df.loc[
        df["gen_tech"].isin(storage_techs), "gen_min_soc"
    ] = min_soc_value

    # Save file again
    df.to_csv(fname, index=False)

