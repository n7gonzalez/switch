# Copyright (c) 2015-2019 The Switch Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0, which is in the LICENSE file.

"""
Defines load zone parameters for the Switch model.

INPUT FILE INFORMATION
    Import load zone data. The following tab-separated files are
    expected in the input directory. Their index columns need to be on
    the left, but the data columns can be in any order. Extra columns
    will be ignored during import, and optional columns can be dropped.
    Other modules (such as local_td) may look for additional columns in
    some of these files. If you don't want to specify data for any
    optional parameter, use a dot . for its value. Optional columns and
    files are noted with a *.

    load_zones.csv
        LOAD_ZONE, zone_ccs_distance_km*, zone_dbid*

    loads.csv
        LOAD_ZONE, TIMEPOINT, zone_demand_mw

    zone_coincident_peak_demand.csv*
        LOAD_ZONE, PERIOD, zone_expected_coincident_peak_demand
"""
import os

from pyomo.environ import *
from switch_model.reporting import write_table
from switch_model.tools.graph import graph

dependencies = 'switch_model.timescales'
optional_dependencies = 'switch_model.transmission.local_td'

def define_dynamic_lists(mod):
    """
    Zone_Power_Injections and Zone_Power_Withdrawals are lists of
    components that contribute to load-zone level power balance equations.
    sum(Zone_Power_Injections[z,t]) == sum(Zone_Power_Withdrawals[z,t])
        for all z,t
    Other modules may append to either list, as long as the components they
    add are indexed by [zone, timepoint] and have units of MW. Other modules
    often include Expressions to summarize decision variables on a zonal basis.
    """
    mod.Zone_Power_Injections = []
    mod.Zone_Power_Withdrawals = []


def define_components(mod):
    """
    Augments a Pyomo abstract model object with sets and parameters that
    describe load zones and associated power balance equations. Unless
    otherwise stated, each set and parameter is mandatory.

    LOAD_ZONES is the set of load zones. Each zone is effectively modeled as a
    single bus connected to the inter-zonal transmission network (assuming
    transmission is enabled). If local_td is included, the central zonal bus,
    is connected to a "distributed bus" via local transmission and
    distribution that incurs efficiency losses and must be upgraded over time
    to always meet peak demand. Load zones are abbreviated as zone in
    parameter names and as z for indexes.

    zone_demand_mw[z,t] describes the power demand from the high voltage
    transmission grid each load zone z and timepoint t. This will either go
    into the Zone_Power_Withdrawals or the Distributed_Power_Withdrawals power
    balance equations, depending on whether the local_td module is included
    and has defined a distributed node for power balancing. If the local_td
    module is excluded, this value should be the total withdrawals from the
    central grid and should include any distribution losses. If the local_td
    module is included, this should be set to total end-use demand (aka sales)
    and should not include distribution losses. zone_demand_mw must be
    non-negative.

    zone_dbid[z] stores an external database id for each load zone. This
    is optional and defaults to the name of the load zone. It will be
    printed out when results are exported.

    zone_ccs_distance_km[z] describes the length of a pipeline in
    kilometers that would need to be built to transport CO2 from a load
    zones central bus to the nearest viable CCS reservoir. This
    parameter is optional and defaults to 0.

    EXTERNAL_COINCIDENT_PEAK_DEMAND_ZONE_PERIODS is a set of load zones and
    periods (z,p) that have zone_expected_coincident_peak_demand specified.

    zone_expected_coincident_peak_demand[z,p] is an optional parameter than can
    be used to externally specify peak load planning requirements in MW.
    Currently local_td and planning_reserves determine capacity requirements
    use zone_expected_coincident_peak_demand as well as load timeseries. Do not
    specify this parameter if you wish for the model to endogenously determine
    capacity requirements after accounting for both load and Distributed
    Energy Resources (DER).

    Derived parameters:

    zone_total_demand_in_period_mwh[z,p] describes the total energy demand
    of each load zone in each period in Megawatt hours.

    """

    mod.LOAD_ZONES = Set(dimen=1, input_file='load_zones.csv')
    mod.ZONE_TIMEPOINTS = Set(dimen=2,
        initialize=lambda m: m.LOAD_ZONES * m.TIMEPOINTS,
        doc="The cross product of load zones and timepoints, used for indexing.")
    mod.zone_demand_mw = Param(
        mod.ZONE_TIMEPOINTS,
        input_file="loads.csv",
        within=NonNegativeReals)
    mod.zone_ccs_distance_km = Param(
        mod.LOAD_ZONES,
        within=NonNegativeReals,
        input_file="load_zones.csv",
        default=0.0)
    mod.zone_dbid = Param(
        mod.LOAD_ZONES,
        input_file="load_zones.csv",
        default=lambda m, z: z)
    mod.min_data_check('LOAD_ZONES', 'zone_demand_mw')
    try:
        mod.Distributed_Power_Withdrawals.append('zone_demand_mw')
    except AttributeError:
        mod.Zone_Power_Withdrawals.append('zone_demand_mw')

    mod.EXTERNAL_COINCIDENT_PEAK_DEMAND_ZONE_PERIODS = Set(
        dimen=2, within=mod.LOAD_ZONES * mod.PERIODS,
        input_file="zone_coincident_peak_demand.csv",
        input_optional=True,
        doc="Zone-Period combinations with zone_expected_coincident_peak_demand data.")
    mod.zone_expected_coincident_peak_demand = Param(
        mod.EXTERNAL_COINCIDENT_PEAK_DEMAND_ZONE_PERIODS,
        input_file="zone_coincident_peak_demand.csv",
        within=NonNegativeReals)
    mod.zone_total_demand_in_period_mwh = Param(
        mod.LOAD_ZONES, mod.PERIODS,
        within=NonNegativeReals,
        initialize=lambda m, z, p: (
            sum(m.zone_demand_mw[z, t] * m.tp_weight[t]
                for t in m.TPS_IN_PERIOD[p])))

    # Make sure the model has duals enabled since we use the duals in post_solve()
    mod.enable_duals()


def define_dynamic_components(mod):
    """
    Adds components to a Pyomo abstract model object to enforce the
    first law of thermodynamics at the level of load zone buses. Unless
    otherwise stated, all terms describing power are in units of MW and
    all terms describing energy are in units of MWh.

    Zone_Energy_Balance[load_zone, timepoint] is a constraint that mandates
    conservation of energy in every load zone and timepoint. This constraint
    sums the model components in the lists Zone_Power_Injections and
    Zone_Power_Withdrawals - each of which is indexed by (z, t) and
    has units of MW - and ensures they are equal. The term tp_duration_hrs
    is factored out of the equation for brevity.
    """

    mod.Zone_Energy_Balance = Constraint(
        mod.ZONE_TIMEPOINTS,
        rule=lambda m, z, t: (
            sum(
                getattr(m, component)[z, t]
                for component in m.Zone_Power_Injections
            ) == sum(
                getattr(m, component)[z, t]
                for component in m.Zone_Power_Withdrawals)))


def post_solve(instance, outdir):
    """
    Exports load_balance.csv, load_balance_annual_zonal.csv, and load_balance_annual.csv.
    Each component registered with Zone_Power_Injections and Zone_Power_Withdrawals will
    become a column in these .csv files. As such, each column represents a power injection
    or withdrawal and the sum of across all columns should be zero. Note that positive
    terms are net injections (e.g. generation) while negative terms are net withdrawals
    (e.g. load).

    load_balance.csv contains the energy balance terms for for every zone and timepoint.
    We also include a column called normalized_energy_balance_duals_dollar_per_mwh
    that is a proxy for the locational marginal pricing (LMP). This value represents
    the incremental cost per hour to increase the demand by 1 MW (or equivalently
    the incremental cost of providing one more MWh of energy). This is not a perfect
    proxy for LMP since it factors in build costs etc.

    load_balance_annual_zonal.csv contains the energy injections and withdrawals
    throughout a year for a given load zone.

    load_balance_annual.csv contains the energy injections and withdrawals
    throughout a year across all zones.
    """
    write_table(
        instance, instance.LOAD_ZONES, instance.TIMEPOINTS,
        output_file=os.path.join(outdir, "load_balance.csv"),
        headings=("load_zone", "timestamp", "normalized_energy_balance_duals_dollar_per_mwh",) + tuple(
            instance.Zone_Power_Injections +
            instance.Zone_Power_Withdrawals),
        values=lambda m, z, t:
        (
            z,
            m.tp_timestamp[t],
            m.get_dual(
                "Zone_Energy_Balance",
                z, t,
                divider=m.bring_timepoint_costs_to_base_year[t]
            )
        )
        + tuple(getattr(m, component)[z, t] for component in m.Zone_Power_Injections)
        + tuple(-getattr(m, component)[z, t] for component in m.Zone_Power_Withdrawals)
    )

    def get_component_per_year(m, z, p, component):
        """
        Returns the weighted sum of component across all timepoints in the given period.
        The components must be indexed by zone and timepoint.
        """
        return sum(getattr(m, component)[z, t] * m.tp_weight_in_year[t] for t in m.TPS_IN_PERIOD[p])

    write_table(
        instance, instance.LOAD_ZONES, instance.PERIODS,
        output_file=os.path.join(outdir, "load_balance_annual_zonal.csv"),
        headings=("load_zone", "period",) + tuple(instance.Zone_Power_Injections + instance.Zone_Power_Withdrawals),
        values=lambda m, z, p:
        (z, p)
        + tuple(get_component_per_year(m, z, p, component) for component in m.Zone_Power_Injections)
        + tuple(-get_component_per_year(m, z, p, component) for component in m.Zone_Power_Withdrawals)
    )

    write_table(
        instance, instance.PERIODS,
        output_file=os.path.join(outdir, "load_balance_annual.csv"),
        headings=("period",) + tuple(instance.Zone_Power_Injections + instance.Zone_Power_Withdrawals),
        values=lambda m, p:
        (p,)
        + tuple(sum(get_component_per_year(m, z, p, component) for z in m.LOAD_ZONES)
                for component in m.Zone_Power_Injections)
        + tuple(-sum(get_component_per_year(m, z, p, component) for z in m.LOAD_ZONES)
                for component in m.Zone_Power_Withdrawals)
    )


@graph(
    "energy_balance_duals",
    title="Energy balance duals per period",
    note="Note: Outliers and zero-valued duals are ignored."
)
def graph_energy_balance(tools):
    load_balance = tools.get_dataframe('load_balance.csv')
    load_balance = tools.transform.timestamp(load_balance)
    load_balance["energy_balance_duals"] = tools.pd.to_numeric(
        load_balance["normalized_energy_balance_duals_dollar_per_mwh"], errors="coerce") / 10
    load_balance = load_balance[["energy_balance_duals", "time_row"]]
    load_balance = load_balance.pivot(columns="time_row", values="energy_balance_duals")
    percent_of_zeroes = sum(load_balance == 0) / len(load_balance) * 100
    # Don't include the zero-valued duals
    load_balance = load_balance.replace(0, tools.np.nan)
    if load_balance.count().sum() != 0:
        load_balance.plot.box(
            ax=tools.get_axes(note=f"{percent_of_zeroes:.1f}% of duals are zero"),
            xlabel='Period',
            ylabel='Energy balance duals (cents/kWh)',
            showfliers=False
        )


@graph(
    "daily_demand",
    title="Total daily demand",
    supports_multi_scenario=True
)
def demand(tools):
    df = tools.get_dataframe("loads.csv", from_inputs=True, drop_scenario_info=False)
    df = df.groupby(["TIMEPOINT", "scenario_name"], as_index=False).sum()
    df = tools.transform.timestamp(df, key_col="TIMEPOINT", use_timepoint=True)
    df = df.groupby(["season", "hour", "scenario_name", "time_row"], as_index=False).mean()
    df["zone_demand_mw"] /= 1e3
    pn = tools.pn

    plot = pn.ggplot(df) + \
           pn.geom_line(pn.aes(x="hour", y="zone_demand_mw", color="scenario_name")) + \
           pn.facet_grid("time_row ~ season") + \
           pn.labs(x="Hour (PST)", y="Demand (GW)", color="Scenario")
    tools.save_figure(plot.draw())


@graph(
    "demand",
    title="Total demand",
    supports_multi_scenario=True
)
def yearly_demand(tools):
    df = tools.get_dataframe("loads.csv", from_inputs=True, drop_scenario_info=False)
    df = df.groupby(["TIMEPOINT", "scenario_name"], as_index=False).sum()
    df = tools.transform.timestamp(df, key_col="TIMEPOINT", use_timepoint=True)
    df["zone_demand_mw"] *= df["tp_duration"] / 1e3
    df["day"] = df["datetime"].dt.day_of_year
    df = df.groupby(["day", "scenario_name", "time_row"], as_index=False)["zone_demand_mw"].sum()
    pn = tools.pn

    plot = pn.ggplot(df) + \
           pn.geom_line(pn.aes(x="day", y="zone_demand_mw", color="scenario_name")) + \
           pn.facet_grid("time_row ~ .") + \
           pn.labs(x="Day of Year", y="Demand (GW)", color="Scenario")
    tools.save_figure(plot.draw())
