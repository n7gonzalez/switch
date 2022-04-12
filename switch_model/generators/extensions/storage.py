# Copyright (c) 2016-2017 The Switch Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0, which is in the LICENSE file.

"""
This module defines storage technologies. It builds on top of generic
generators, adding components for deciding how much energy to build into
storage, when to charge, energy accounting, etc.

INPUT FILE FORMAT
    Import storage parameters. Optional columns are noted with a *.

    generation_projects_info.csv
        GENERATION_PROJECT, ...
        gen_storage_efficiency, gen_discharge_efficiency*, gen_store_to_release_ratio*,
        gen_storage_energy_to_power_ratio*, gen_storage_max_cycles_per_year*
        gen_self_discharge_rate*, gen_land_use_rate*

    # TODO: maybe move the columns above to a storage_gen_info file to avoid the weird index
    # reading and avoid having to create these extra columns for all projects;
    # Alternatively, say that these values are specified for _all_ projects (maybe with None
    # as default) and then define STORAGE_GENS as the subset of projects for which
    # gen_storage_efficiency has been specified, then require valid settings for all
    # STORAGE_GENS.

    gen_build_costs.csv
        GENERATION_PROJECT, build_year, ...
        gen_storage_energy_overnight_cost

    gen_build_predetermined.csv
        GENERATION_PROJECT, build_year, ...,
        gen_predetermined_storage_energy_mwh*

"""
import math

import pandas as pd
from scipy import fft

from pyomo.environ import *
import os, collections
from switch_model.financials import capital_recovery_factor as crf
from switch_model.tools.graph import graph

dependencies = (
    "switch_model.timescales",
    "switch_model.balancing.load_zones",
    "switch_model.financials",
    "switch_model.energy_sources.properties",
    "switch_model.generators.core.build",
    "switch_model.generators.core.dispatch",
)


def define_components(mod):
    """

    STORAGE_GENS is the subset of projects that can provide energy storage.

    STORAGE_GEN_BLD_YRS is the subset of GEN_BLD_YRS, restricted
    to storage projects.

    gen_storage_efficiency[STORAGE_GENS] describes the round trip
    efficiency of a storage technology. A storage technology that is 75
    percent efficient would have a storage_efficiency of .75. If 1 MWh
    was stored in such a storage project, 750 kWh would be available for
    extraction later. Internal leakage or energy dissipation of storage
    technologies is assumed to be neglible, which is consistent with
    short-duration storage technologies currently on the market which
    tend to consume stored power within 1 day. If a given storage
    technology has significant internal discharge when it stores power
    for extended time perios, then those behaviors will need to be
    modeled in more detail.

    gen_discharge_efficiency[STORAGE_GENS] describes the efficiency during
    discharging. A discharge efficiency of 0.90 means that 90% of the energy
    stored reaches the grid during discharging. Note that gen_storage_efficiency
    is the efficiency while charging. To only specify the round trip efficiency
    set gen_storage_efficiency to the round trip efficiency and leave this
    parameter at its default of 1.

    gen_store_to_release_ratio[STORAGE_GENS] describes the maximum rate
    that energy can be stored, expressed as a ratio of discharge power
    capacity. This is an optional parameter and will default to 1. If a
    storage project has 1 MW of dischage capacity and a gen_store_to_release_ratio
    of 1.2, then it can consume up to 1.2 MW of power while charging.

    gen_storage_energy_to_power_ratio[STORAGE_GENS], if specified, restricts
    the storage capacity (in MWh) to be a fixed multiple of the output
    power (in MW), i.e., specifies a particular number of hours of
    storage capacity. Omit this column or specify "." to allow Switch
    to choose the energy/power ratio. (Note: gen_storage_energy_overnight_cost
    or gen_overnight_cost should often be set to 0 when using this.)

    gen_storage_max_cycles_per_year[STORAGE_GENS], if specified, restricts
    the number of charge/discharge cycles each storage project can perform
    per year; one cycle is defined as discharging an amount of energy
    equal to the storage capacity of the project.

    gen_self_discharge_rate[STORAGE_GENS] is the fraction of the charge that is lost
    over a day. This is used for certain types of storage such as thermal energy
    storage that slowly loses its charge over time. Default is 0 (no self discharge).

    gen_land_use_rate[STORAGE_GENS] is the amount of land used in square meters per MWh
    of storage for the given storage technology. Defaults to 0.

    gen_storage_energy_overnight_cost[(g, bld_yr) in
    STORAGE_GEN_BLD_YRS] is the overnight capital cost per MWh of
    energy capacity for building the given storage technology installed in the
    given investment period. This is only defined for storage technologies.
    Note that this describes the energy component and the overnight_cost
    describes the power component.

    gen_predetermined_storage_energy_mwh[(g, bld_yr) in
    PREDETERMINED_GEN_BLD_YRS] is the amount of storage that has either been
    installed previously, or is slated for installation and is not a free
    decision variable. This is analogous to gen_predetermined_cap, but in
    units of energy of storage capacity (MWh) rather than power (MW).

    BuildStorageEnergy[(g, bld_yr) in STORAGE_GEN_BLD_YRS]
    is a decision of how much energy capacity to build onto a storage
    project. This is analogous to BuildGen, but for energy rather than power.

    StorageEnergyInstallCosts[PERIODS] is an expression of the
    annual costs incurred by the BuildStorageEnergy decision.

    StorageEnergyCapacity[g, period] is an expression describing the
    cumulative available energy capacity of BuildStorageEnergy. This is
    analogous to GenCapacity.

    STORAGE_GEN_TPS is the subset of GEN_TPS,
    restricted to storage projects.

    ChargeStorage[(g, t) in STORAGE_GEN_TPS] is a dispatch
    decision of how much to charge a storage project in each timepoint.

    StorageNetCharge[LOAD_ZONE, TIMEPOINT] is an expression describing the
    aggregate impact of ChargeStorage in each load zone and timepoint.

    Charge_Storage_Upper_Limit[(g, t) in STORAGE_GEN_TPS]
    constrains ChargeStorage to available power capacity (accounting for
    gen_store_to_release_ratio)

    StateOfCharge[(g, t) in STORAGE_GEN_TPS] is a variable
    for tracking state of charge. This value stores the state of charge at
    the end of each timepoint for each storage project.

    Track_State_Of_Charge[(g, t) in STORAGE_GEN_TPS] constrains
    StateOfCharge based on the StateOfCharge in the previous timepoint,
    ChargeStorage and DispatchGen.

    State_Of_Charge_Upper_Limit[(g, t) in STORAGE_GEN_TPS]
    constrains StateOfCharge based on installed energy capacity.

    LandUseRate[g, period] is an expression for the amount of land used
    in meters squared for a given storage project during a given period.
    """

    mod.STORAGE_GENS = Set(within=mod.GENERATION_PROJECTS, dimen=1)
    mod.STORAGE_GEN_PERIODS = Set(
        within=mod.GEN_PERIODS,
        initialize=lambda m: [
            (g, p) for g in m.STORAGE_GENS for p in m.PERIODS_FOR_GEN[g]
        ],
    )
    mod.gen_storage_efficiency = Param(
        mod.STORAGE_GENS,
        input_file="generation_projects_info.csv",
        within=PercentFraction,
    )
    mod.gen_discharge_efficiency = Param(
        mod.STORAGE_GENS,
        within=PercentFraction,
        default=1,
        input_file="generation_projects_info.csv",
        doc="The percent of stored energy that reaches the grid during discharging",
    )
    # TODO: rename to gen_charge_to_discharge_ratio?
    mod.gen_store_to_release_ratio = Param(
        mod.STORAGE_GENS,
        within=NonNegativeReals,
        input_file="generation_projects_info.csv",
        default=1.0,
    )
    mod.gen_storage_energy_to_power_ratio = Param(
        mod.STORAGE_GENS,
        input_file="generation_projects_info.csv",
        within=NonNegativeReals,
        default=float("inf"),
    )  # inf is a flag that no value is specified (nan and None don't work)
    mod.gen_storage_max_cycles_per_year = Param(
        mod.STORAGE_GENS,
        within=NonNegativeReals,
        input_file="generation_projects_info.csv",
        default=float("inf"),
    )
    mod.gen_self_discharge_rate = Param(
        mod.STORAGE_GENS,
        within=PercentFraction,
        default=0,
        input_file="generation_projects_info.csv",
        doc="Percent of stored energy lost per day.",
    )
    mod.gen_land_use_rate = Param(
        mod.STORAGE_GENS,
        within=NonNegativeReals,
        default=0,
        input_file="generation_projects_info.csv",
        doc="Meters squared of land used per MWh of storage",
    )
    mod.gen_min_soc = Param(
        mod.STORAGE_GENS,
        within=PercentFraction,
        default=0,
        input_file="generation_projects_info.csv",
        input_optional=True,
        doc="Enforce minimum state of charge for battery storage assets",
    )

    mod.STORAGE_GEN_BLD_YRS = Set(
        dimen=2,
        initialize=lambda m: [
            (g, bld_yr) for g in m.STORAGE_GENS for bld_yr in m.BLD_YRS_FOR_GEN[g]
        ],
    )
    mod.gen_storage_energy_overnight_cost = Param(
        mod.STORAGE_GEN_BLD_YRS,
        input_file="gen_build_costs.csv",
        within=NonNegativeReals,
    )
    mod.min_data_check("gen_storage_energy_overnight_cost")
    mod.gen_predetermined_storage_energy_mwh = Param(
        mod.PREDETERMINED_GEN_BLD_YRS,
        input_file="gen_build_predetermined.csv",
        within=NonNegativeReals,
    )
    mod.PREDETERMINED_STORAGE_GEN_BLD_YRS = Set(
        initialize=mod.PREDETERMINED_GEN_BLD_YRS,
        filter=lambda m, g, bld_yr: (g, bld_yr)
        in m.gen_predetermined_storage_energy_mwh,
    )

    def bounds_BuildStorageEnergy(m, g, bld_yr):
        if (g, bld_yr) in m.PREDETERMINED_STORAGE_GEN_BLD_YRS:
            return (
                m.gen_predetermined_storage_energy_mwh[g, bld_yr],
                m.gen_predetermined_storage_energy_mwh[g, bld_yr],
            )
        else:
            return (0, None)

    mod.BuildStorageEnergy = Var(
        mod.STORAGE_GEN_BLD_YRS,
        within=NonNegativeReals,
        bounds=bounds_BuildStorageEnergy,
    )

    # Some projects are retired before the first study period, so they
    # don't appear in the objective function or any constraints.
    # In this case, pyomo may leave the variable value undefined even
    # after a solve, instead of assigning a value within the allowed
    # range. This causes errors in the Progressive Hedging code, which
    # expects every variable to have a value after the solve. So as a
    # starting point we assign an appropriate value to all the existing
    # projects here.
    # TODO Don't include projects that are retired in the first study period in
    #   the model in the first place. Same thing in build.py with BuildGen.
    def BuildStorageEnergy_assign_default_value(m, g, bld_yr):
        m.BuildStorageEnergy[g, bld_yr] = m.gen_predetermined_storage_energy_mwh[
            g, bld_yr
        ]

    mod.BuildStorageEnergy_assign_default_value = BuildAction(
        mod.PREDETERMINED_STORAGE_GEN_BLD_YRS,
        rule=BuildStorageEnergy_assign_default_value,
    )

    # Summarize capital costs of energy storage for the objective function
    # Note: A bug in to 2.0.0b3 - 2.0.5, assigned costs that were several times
    # too high
    mod.StorageEnergyFixedCost = Expression(
        mod.PERIODS,
        rule=lambda m, p: sum(
            sum(
                m.BuildStorageEnergy[g, bld_yr]
                * m.gen_storage_energy_overnight_cost[g, bld_yr]
                * crf(m.interest_rate, m.gen_max_age[g])
                for bld_yr in m.BLD_YRS_FOR_GEN_PERIOD[g, p]
            )
            for g in m.STORAGE_GENS
        ),
    )
    mod.Cost_Components_Per_Period.append("StorageEnergyFixedCost")

    # 2.0.0b3 code:
    # mod.StorageEnergyInstallCosts = Expression(
    # mod.PERIODS,
    # rule=lambda m, p: sum(m.BuildStorageEnergy[g, bld_yr] *
    #            m.gen_storage_energy_overnight_cost[g, bld_yr] *
    #            crf(m.interest_rate, m.gen_max_age[g])
    #            for (g, bld_yr) in m.STORAGE_GEN_BLD_YRS))

    mod.StorageEnergyCapacity = Expression(
        mod.STORAGE_GENS,
        mod.PERIODS,
        rule=lambda m, g, period: sum(
            m.BuildStorageEnergy[g, bld_yr]
            for bld_yr in m.BLD_YRS_FOR_GEN_PERIOD[g, period]
        ),
    )

    mod.LandUse = Expression(
        mod.STORAGE_GENS,
        mod.PERIODS,
        rule=lambda m, g, p: m.gen_land_use_rate[g] * m.StorageEnergyCapacity[g, p],
    )

    mod.STORAGE_GEN_TPS = Set(
        dimen=2,
        initialize=lambda m: (
            (g, tp) for g in m.STORAGE_GENS for tp in m.TPS_FOR_GEN[g]
        ),
    )

    mod.ChargeStorage = Var(mod.STORAGE_GEN_TPS, within=NonNegativeReals)

    # Summarize storage charging for the energy balance equations
    # TODO: rename this StorageTotalCharging or similar (to indicate it's a
    # sum for a zone, not a net quantity for a project)
    def rule(m, z, t):
        # Construct and cache a set for summation as needed
        if not hasattr(m, "Storage_Charge_Summation_dict"):
            m.Storage_Charge_Summation_dict = collections.defaultdict(set)
            for g, t2 in m.STORAGE_GEN_TPS:
                z2 = m.gen_load_zone[g]
                m.Storage_Charge_Summation_dict[z2, t2].add(g)
        # Use pop to free memory
        relevant_projects = m.Storage_Charge_Summation_dict.pop((z, t), {})
        return sum(m.ChargeStorage[g, t] for g in relevant_projects)

    mod.StorageNetCharge = Expression(mod.LOAD_ZONES, mod.TIMEPOINTS, rule=rule)
    # Register net charging with zonal energy balance. Discharging is already
    # covered by DispatchGen.
    mod.Zone_Power_Withdrawals.append("StorageNetCharge")

    # use fixed energy/power ratio (# hours of capacity) when specified
    mod.Enforce_Fixed_Energy_Storage_Ratio = Constraint(
        mod.STORAGE_GEN_BLD_YRS,
        rule=lambda m, g, y: Constraint.Skip
        if m.gen_storage_energy_to_power_ratio[g] == float("inf")  # no value specified
        else (
            m.BuildStorageEnergy[g, y]
            == m.gen_storage_energy_to_power_ratio[g] * m.BuildGen[g, y]
        ),
    )

    def Charge_Storage_Upper_Limit_rule(m, g, t):
        return (
            m.ChargeStorage[g, t]
            <= m.DispatchUpperLimit[g, t] * m.gen_store_to_release_ratio[g]
        )

    mod.Charge_Storage_Upper_Limit = Constraint(
        mod.STORAGE_GEN_TPS, rule=Charge_Storage_Upper_Limit_rule
    )

    mod.StateOfCharge = Var(mod.STORAGE_GEN_TPS, within=NonNegativeReals)

    mod.StorageFlow = Expression(
        mod.STORAGE_GEN_TPS,
        rule=lambda m, g, t: m.ChargeStorage[g, t] * m.gen_storage_efficiency[g]
        - m.DispatchGen[g, t] / m.gen_discharge_efficiency[g],
    )

    def Track_State_Of_Charge_rule(m, g, t):
        storage_efficiency = 1 - m.gen_self_discharge_rate[g]
        tp_duration_days = m.tp_duration_hrs[t] / 24
        # Energy in storage that remains from the energy in storage at the previous timepoint
        carry_over_energy = (
            m.StateOfCharge[g, m.tp_previous[t]]
            * storage_efficiency**tp_duration_days
        )
        # Energy change due to flow in or out of the battery (StorageFlow).
        flow_energy = m.StorageFlow[g, t] * (
            # If there's no decay, it's simply StorageFlow * tp_duration_hrs
            m.tp_duration_hrs[t]
            if storage_efficiency == 1
            else
            # If there is decay, we need to account for energy decay during the timepoint duration.
            # To derive the following expression, simply solve the differential equation:
            # dZ/dt = -rZ + StorageFlow
            # where r is the instantaneous decay rate, Z is the state of charge and t is time.
            # Note that exp(-24r) = (1 - daily_decay_rate).
            24
            * (storage_efficiency**tp_duration_days - 1)
            / math.log(storage_efficiency)
        )

        return m.StateOfCharge[g, t] == carry_over_energy + flow_energy

    mod.Track_State_Of_Charge = Constraint(
        mod.STORAGE_GEN_TPS, rule=Track_State_Of_Charge_rule
    )

    def State_Of_Charge_Upper_Limit_rule(m, g, t):
        return m.StateOfCharge[g, t] <= m.StorageEnergyCapacity[g, m.tp_period[t]]

    mod.State_Of_Charge_Upper_Limit = Constraint(
        mod.STORAGE_GEN_TPS, rule=State_Of_Charge_Upper_Limit_rule
    )

    # NOTE: This piece of code adds a minimum state of charge requirement for the purpuse
    # of the PRM paper. It might not make it to the stable version
    def State_Of_Charge_Lower_Limit_rule(m, g, t):
        return (
            m.StateOfCharge[g, t]
            >= m.StorageEnergyCapacity[g, m.tp_period[t]] * m.gen_min_soc[g]
        )

    mod.State_Of_Charge_Lower_Limit = Constraint(
        mod.STORAGE_GEN_TPS, rule=State_Of_Charge_Lower_Limit_rule
    )

    # batteries can only complete the specified number of cycles per year, averaged over each period
    mod.Battery_Cycle_Limit = Constraint(
        mod.STORAGE_GEN_PERIODS,
        rule=lambda m, g, p:
        # solvers sometimes perform badly with infinite constraint
        Constraint.Skip
        if m.gen_storage_max_cycles_per_year[g] == float("inf")
        else (
            sum(
                m.DispatchGen[g, tp] * m.tp_duration_hrs[tp]
                for tp in m.TPS_IN_PERIOD[p]
            )
            <= m.gen_storage_max_cycles_per_year[g]
            * m.StorageEnergyCapacity[g, p]
            * m.period_length_years[p]
        ),
    )


def load_inputs(mod, switch_data, inputs_dir):
    # Base the set of storage projects on storage efficiency being specified.
    # TODO: define this in a more normal way
    switch_data.data()["STORAGE_GENS"] = {
        None: list(switch_data.data(name="gen_storage_efficiency").keys())
    }


def post_solve(instance, outdir):
    """
    Export storage build information to storage_builds.csv,
    storage capacity to storage_capacity.csv, and storage
    dispatch info to storage_dispatch.csv
    """
    import switch_model.reporting as reporting

    # Write how much is built each build year for each project to storage_builds.csv
    reporting.write_table(
        instance,
        instance.STORAGE_GEN_BLD_YRS,
        output_file=os.path.join(outdir, "storage_builds.csv"),
        headings=(
            "generation_project",
            "build_year",
            "load_zone",
            "IncrementalPowerCapacityMW",
            "IncrementalEnergyCapacityMWh",
        ),
        values=lambda m, g, bld_yr: (
            g,
            bld_yr,
            m.gen_load_zone[g],
            m.BuildGen[g, bld_yr],
            m.BuildStorageEnergy[g, bld_yr],
        ),
    )
    # Write the total capacity for each project at each period to storage_capacity.csv
    reporting.write_table(
        instance,
        instance.STORAGE_GEN_PERIODS,
        output_file=os.path.join(outdir, "storage_capacity.csv"),
        headings=(
            "generation_project",
            "period",
            "load_zone",
            "OnlinePowerCapacityMW",
            "OnlineEnergyCapacityMWh",
        ),
        values=lambda m, g, p: (
            g,
            p,
            m.gen_load_zone[g],
            m.GenCapacity[g, p],
            m.StorageEnergyCapacity[g, p],
        ),
    )
    # Write how much is dispatched by each project at each time point to storage_dispatch.csv
    reporting.write_table(
        instance,
        instance.STORAGE_GEN_TPS,
        output_file=os.path.join(outdir, "storage_dispatch.csv"),
        # TODO renaming heading to timestamp (and update graphing accordingly)
        headings=(
            "generation_project",
            "timepoint",
            "load_zone",
            "ChargeMW",
            "DischargeMW",
            "StateOfCharge",
        ),
        values=lambda m, g, t: (
            g,
            m.tp_timestamp[t],
            m.gen_load_zone[g],
            m.ChargeStorage[g, t],
            m.DispatchGen[g, t],
            m.StateOfCharge[g, t],
        ),
    )


@graph(
    "state_of_charge",
    title="State of Charge Throughout the Year",
    supports_multi_scenario=True,
    note="The daily charge/discharge amount is calculated as"
    " the difference between the maximum and minimum"
    " state of charge in a 1-day rolling window.\n"
    "The black line is the 14-day rolling mean of the state of charge.",
)
def graph_state_of_charge(tools):
    # Each panel is a period and scenario
    panel_group = ["period", "scenario_name"]
    rolling_mean_window_size = "14D"

    # Get the total state of charge per timepoint and scenario
    soc = tools.get_dataframe("storage_dispatch.csv").rename(
        {"StateOfCharge": "value"}, axis=1
    )
    soc = soc.groupby(["timepoint", "scenario_name"], as_index=False).value.sum()
    # Convert values to TWh
    soc.value /= 1e6
    # Add datetime information
    soc = tools.transform.timestamp(soc, key_col="timepoint")[
        panel_group + ["datetime", "value"]
    ]
    # Count num rows
    num_periods = len(soc["period"].unique())

    # Used later
    grouped_soc = soc.set_index("datetime").groupby(panel_group, as_index=False)

    # Calculate the weekly SOC
    weekly_soc = (
        grouped_soc.rolling(rolling_mean_window_size, center=True)
        .value.mean()
        .reset_index()
    )

    # Get the total capacity per period and scenario
    capacity = tools.get_dataframe("storage_capacity.csv")
    capacity = (
        capacity.groupby(panel_group, as_index=False)["OnlineEnergyCapacityMWh"]
        .sum()
        .rename({"OnlineEnergyCapacityMWh": "value"}, axis=1)
    )
    capacity.value /= 1e6
    capacity["type"] = "Total Energy Capacity"

    # Add information regarding the diurnal cycle to the dataframe
    # Find the difference between the min and max for every day of the year
    group = grouped_soc.rolling("D", center=True).value
    daily_size = (
        (group.max() - group.min()).reset_index().groupby(panel_group, as_index=False)
    )
    # Find the mean between the difference of the min and max
    avg_daily_size = daily_size.mean()[panel_group + ["value"]]
    avg_daily_size["type"] = "Mean Daily Charge/Discharge"
    max_daily_size = daily_size.max()[panel_group + ["value"]]
    max_daily_size["type"] = "Maximum Daily Charge/Discharge"

    # Determine information for the labels
    y_axis_max = capacity.value.max()
    label_x_pos = soc["datetime"].median()

    hlines = pd.concat([capacity, avg_daily_size, max_daily_size])

    # For the max label
    hlines["label_pos"] = hlines.value + y_axis_max * 0.05
    hlines["label"] = hlines.value.round(decimals=2)

    # Plot with plotnine
    pn = tools.pn
    plot = (
        pn.ggplot(soc, pn.aes(x="datetime", y="value"))
        + pn.geom_line(color="gray")
        + pn.geom_line(data=weekly_soc, color="black")
        + pn.labs(y="State of Charge (TWh)", x="Time of Year")
        + pn.geom_hline(
            pn.aes(yintercept="value", label="label", color="type"),
            data=hlines,
            linetype="dashed",
        )
        + pn.geom_text(
            pn.aes(label="label", x=label_x_pos, y="label_pos"),
            data=hlines,
            fontweight="light",
            size="10",
        )
    )
    tools.save_figure(by_scenario_and_period(tools, plot, num_periods).draw())


@graph(
    "state_of_charge_per_duration",
    title="State of Charge Throughout the Year by Duration",
    supports_multi_scenario=True,
)
def graph_state_of_charge_per_duration(tools):
    # Read the capacity of each project and label they by duration
    capacity = tools.get_dataframe("storage_capacity.csv")
    capacity["duration"] = (
        capacity["OnlineEnergyCapacityMWh"] / capacity["OnlinePowerCapacityMW"]
    )
    capacity["duration"] = tools.pd.cut(
        capacity["duration"],
        bins=(0, 10, 25, 300, 365),
        precision=0,
    )

    # Get the total state of charge at each timepoint for each project
    df = tools.get_dataframe("storage_dispatch")[
        ["generation_project", "timepoint", "StateOfCharge", "scenario_name"]
    ]
    df = tools.transform.timestamp(df, key_col="timepoint")

    # Add the capacity information to the state of charge information
    df = df.merge(
        capacity,
        on=["generation_project", "period", "scenario_name"],
        validate="many_to_one",
    )
    # Aggregate projects in the same duration group
    df = df.groupby(
        ["duration", "scenario_name", "datetime", "period"], as_index=False
    )[["StateOfCharge", "OnlineEnergyCapacityMWh"]].sum()
    # Convert to GWh
    # df["StateOfCharge"] /= 1e3
    # Convert to percent
    df["StateOfCharge"] /= df["OnlineEnergyCapacityMWh"]

    # Plot with plotnine
    pn = tools.pn
    plot = (
        pn.ggplot(df, pn.aes(x="datetime", y="StateOfCharge", color="duration"))
        + pn.geom_line(alpha=0.5)
        + pn.labs(
            y="State of Charge (GWh)", x="Time of Year", color="Storage Duration (h)"
        )
    )

    tools.save_figure(
        by_scenario_and_period(tools, plot, len(df["period"].unique())).draw()
    )


@graph(
    "storage_dispatch_frequency",
)
def graph_dispatch_cycles(tools):
    df = tools.get_dataframe("storage_dispatch")
    # Aggregate by timepoint
    df = df.groupby("timepoint", as_index=False).sum()
    # Add datetime column
    df = tools.transform.timestamp(df, key_col="timepoint")
    # Find charge in GWh
    df["StateOfCharge"] /= 1e3

    # Storage Frequency graph
    df = df.set_index("datetime")
    df = df.sort_index()
    charge = df["StateOfCharge"].values
    # TODO don't hardcode
    timestep = (df.index[1] - df.index[0]).seconds / 3600
    N = len(charge)
    yfreq = tools.np.abs(fft.fft(charge, norm="forward"))
    xfreq = fft.fftfreq(N, timestep)

    # Drop negative frequencies and first value (0)
    yfreq = yfreq[1 : N // 2] * 2
    xfreq = xfreq[1 : N // 2]

    # Plot
    ax = tools.get_axes(
        "storage_dispatch_frequency", title="Fourier transform of State of Charge"
    )
    ax.plot(xfreq, yfreq)
    ax.set_xlabel("Cycles per hour")

    # Plot
    ax = tools.get_axes(
        "storage_dispatch_cycle_duration",
        title="Storage cycle duration based on fourier transform" " of state of charge",
    )
    ax.semilogx(1 / xfreq, yfreq)
    # Plot some key cycle lengths
    ax.axvline(24, linestyle="dotted", label="24 hours", color="red")  # A day
    ax.axvline(24 * 21, linestyle="dotted", label="3 weeks", color="green")  # 3 weeks
    ax.axvline(24 * 182.5, linestyle="dotted", label="1/2 Year", color="purple")
    ax.set_xlabel("Hours per cycle")
    ax.legend()
    ax.grid(True, which="both", axis="x")


@graph("graph_buildout", supports_multi_scenario=True)
def graph_buildout(tools):
    """
    Create graphs relating to the storage that has been built
    """
    df = tools.get_dataframe("storage_builds.csv")
    df = tools.transform.load_zone(df)
    # Filter out rows where there's no power built
    df = df[df["IncrementalPowerCapacityMW"] != 0]
    df["duration"] = (
        df["IncrementalEnergyCapacityMWh"] / df["IncrementalPowerCapacityMW"]
    )
    df["power"] = df["IncrementalPowerCapacityMW"] / 1e3
    df["energy"] = df["IncrementalEnergyCapacityMWh"] / 1e3
    df = tools.transform.build_year(df)
    pn = tools.pn
    num_regions = len(df["region"].unique())
    plot = (
        pn.ggplot(df, pn.aes(x="duration", y="power", color="build_year"))
        + pn.geom_point()
        + pn.labs(
            title="Storage Buildout",
            color="Build Year",
            x="Duration (h)",
            y="Power Capacity (GW)",
        )
    )

    tools.save_figure(by_scenario(tools, plot).draw(), "storage_duration")
    tools.save_figure(
        by_scenario_and_region(tools, plot, num_regions).draw(),
        "storage_duration_by_region",
    )

    plot = (
        pn.ggplot(df, pn.aes(x="duration"))
        + pn.geom_histogram(pn.aes(weight="power"), binwidth=5)
        + pn.labs(
            title="Storage Duration Histogram",
            x="Duration (h)",
            y="Power Capacity (GW)",
        )
    )

    tools.save_figure(by_scenario(tools, plot).draw(), "storage_duration_histogram")
    tools.save_figure(
        by_scenario_and_region(tools, plot, num_regions).draw(),
        "storage_duration_histogram_by_region",
    )

    plot = (
        pn.ggplot(df, pn.aes(x="duration"))
        + pn.geom_histogram(pn.aes(weight="energy"), binwidth=5)
        + pn.labs(
            title="Storage Duration Histogram",
            x="Duration (h)",
            y="Energy Capacity (GWh)",
        )
    )

    tools.save_figure(
        by_scenario(tools, plot).draw(), "storage_duration_histogram_by_energy"
    )
    tools.save_figure(
        by_scenario_and_region(tools, plot, num_regions).draw(),
        "storage_duration_histogram_by_region_and_energy",
    )


def by_scenario(tools, plot):
    pn = tools.pn
    return (
        plot
        + pn.facet_grid(". ~ scenario_name")
        + pn.theme(
            figure_size=(
                pn.options.figure_size[0] * tools.num_scenarios,
                pn.options.figure_size[1],
            )
        )
    )


def by_scenario_and_period(tools, plot, num_periods):
    pn = tools.pn
    num_periods = min(num_periods, 3)
    return (
        plot
        + pn.facet_grid("period ~ scenario_name")
        + pn.theme(
            figure_size=(
                pn.options.figure_size[0] * tools.num_scenarios,
                pn.options.figure_size[1] * num_periods,
            )
        )
    )


def by_scenario_and_region(tools, plot, num_regions):
    pn = tools.pn
    num_regions = min(num_regions, 5)
    return (
        plot
        + pn.facet_grid("scenario_name ~ region")
        + pn.theme(
            figure_size=(
                pn.options.figure_size[0] * num_regions,
                pn.options.figure_size[1] * tools.num_scenarios,
            )
        )
    )