""" Aggregate candidate generators into a single entry

This is a second version of the aggregate candidate script with a single case for
aggregating the capacity factor profiles.

Implementation details:

1. We first aggregate the plants in generation_projects_info.csv.
    - We average the connection costs (weighted by capacity limit)
    - We sum the capacity limit

2. We verify that the build costs are the same for all the aggregated projects and update
build_costs.csv

3. We aggregate the variable_capacity_factors.csv depending on the method specified in the parameter 'cf_method'.
"""
# System packages
import warnings

# Third-party packages
import numpy as np
import pandas as pd

# Local packages
from switch_model.wecc.get_inputs.register_post_process import post_process_step

@post_process_step(
    msg="Aggregating candidate projects by load zone for specified technologies"
)
def post_process(config):

    # Read agg_technologies from configuration file
    agg_techs = config["agg_techs"]

    # Read capacity factor method from configuration file
    cf_method = config["cf_method"]

    # Don't allow hydro to be aggregated since we haven't implemented how to handle
    # hydro_timeseries.csv
    assert type(agg_techs) == list
    assert "Hydro_NonPumped" not in agg_techs
    assert "Hydro_Pumped" not in agg_techs

    print(
        f"\t\tAggregating on projects where gen_tech in {agg_techs} with capacity factor"
        f"method {cf_method}"
    )
    key = "GENERATION_PROJECT"

    # NOTE: Keep only the projects we want to aggregate (must be candidate and in agg_techs)
    # save the projects we're not aggregating into projects_no_agg for later

    # Read generator projects
    fname = "generation_projects_info.csv"
    all_projects = pd.read_csv(fname, index_col=False, dtype={key: str})
    legacy_projects = pd.read_csv("gen_build_predetermined.csv", dtype={key: str})[
        key
    ].values

    # Check for non-legacy plants and that match the aggregate technology
    agg_mask = all_projects["gen_tech"].isin(agg_techs) & (
        ~all_projects[key].isin(legacy_projects)
    )
    legacy_projects = all_projects.loc[~agg_mask].copy()
    candidate_projects = all_projects.loc[agg_mask].copy()
    candidate_projects_ids = all_projects.loc[agg_mask, key].copy()
    candidate_projects.loc[:, "GENERATION_PROJECT"] = candidate_projects.apply(
        lambda row: f"{row['gen_load_zone']}_{row['gen_tech']}_agg", axis=1
    )

    columns = ["gen_capacity_limit_mw", "gen_connect_cost_per_mw"]
    candidate_projects = candidate_projects.astype({"gen_capacity_limit_mw": float})
    candidate_projects.loc[:, ["gen_capacity_limit_mw"]] = candidate_projects.groupby(
        ["gen_load_zone", "gen_tech"]
    )["gen_capacity_limit_mw"].transform(sum)
    candidate_projects = candidate_projects.astype({"gen_connect_cost_per_mw": float})
    candidate_projects.loc[:, ["gen_connect_cost_per_mw"]] = candidate_projects.groupby(
        ["gen_load_zone", "gen_tech"]
    )["gen_connect_cost_per_mw"].transform(np.mean)

    candidate_projects = candidate_projects.drop_duplicates(
        subset=["GENERATION_PROJECT"]
    ).sort_values("gen_load_zone")
    # Add back the non aggregate projects and write to csv
    all_projects_agg = pd.concat([legacy_projects, candidate_projects])
    new_name = "generation_projects_info.csv"
    all_projects_agg.to_csv(new_name, index=False)

    # Fetch the capacity factors for the projects of interest
    fname = "variable_capacity_factors.csv"
    cf_all = pd.read_csv(fname, index_col=False, dtype={key: str})
    cf_candidate = cf_all.loc[cf_all[key].isin(candidate_projects_ids)]
    cf_legacy = cf_all.loc[~cf_all[key].isin(candidate_projects_ids)]

    cf_profiles = cf_candidate.merge(all_projects, on="GENERATION_PROJECT")

    cf_profiles_by_lz = cf_profiles.groupby(
        ["gen_load_zone", "gen_tech", "timepoint"], as_index=False
    )["gen_max_capacity_factor"].quantile(0.95)

    cf_profiles_by_lz.loc[:, "GENERATION_PROJECT"] = cf_profiles_by_lz.apply(
        lambda row: f"{row['gen_load_zone']}_{row['gen_tech']}_agg", axis=1
    )
    new_name = "variable_capacity_factors.csv"
    cf_profiles_all = pd.concat([cf_legacy, cf_profiles_by_lz])
    cf_profiles_all.to_csv(new_name, index=False)

    # Gen build cost
    fname = "gen_build_costs.csv"
    build_cost_all = pd.read_csv(fname, index_col=False, dtype={key: str})
    build_cost_candidate = build_cost_all.loc[
        build_cost_all[key].isin(candidate_projects_ids)
    ]
    build_cost_legacy = build_cost_all.loc[
        ~build_cost_all[key].isin(candidate_projects_ids)
    ]

    build_cost_candidate = build_cost_candidate.merge(
        all_projects, on="GENERATION_PROJECT"
    )
    build_cost_candidate_agg = build_cost_candidate.groupby(
        ["gen_load_zone", "gen_tech", "build_year"], as_index=False
    )[["gen_overnight_cost", "gen_fixed_om"]].mean(0.95)
    build_cost_candidate_agg.loc[
        :, "GENERATION_PROJECT"
    ] = build_cost_candidate_agg.apply(
        lambda row: f"{row['gen_load_zone']}_{row['gen_tech']}_agg", axis=1
    )
    new_name = "gen_build_costs_2.csv"
    build_cost_candidate_agg.loc[:, "gen_storage_energy_overnight_cost"] = "."
    build_cost_all = pd.concat(
        [build_cost_legacy, build_cost_candidate_agg[build_cost_legacy.columns]]
    )
    build_cost_all.to_csv(new_name, index=False)
    breakpoint()

    cf_profiles_by_lz.loc[:, "GENERATION_PROJECT"] = cf_profiles_by_lz.apply(
        lambda row: f"{row['gen_load_zone']}_{row['gen_tech']}_agg", axis=1
    )
    new_name = "gen_build_costs.csv"
    cf_profiles_all = pd.concat([cf_legacy, cf_profiles_by_lz])
    cf_profiles_all.to_csv(new_name, index=False)
