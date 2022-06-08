# Copyright (c) 2015-2019 The Switch Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0, which is in the LICENSE file.
"""
Defines generation projects build-outs.

INPUT FILE FORMAT
    Import data describing project builds. The following files are
    expected in the input directory.

    generation_projects_info.csv has mandatory and optional columns. The
    operations.gen_dispatch module will also look for additional columns in
    this file. You may drop optional columns entirely or mark blank
    values with a dot '.' for select rows for which the column does not
    apply. Mandatory columns are:
        GENERATION_PROJECT, gen_tech, gen_energy_source, gen_load_zone,
        gen_max_age, gen_is_variable, gen_is_baseload,
        gen_full_load_heat_rate, gen_variable_om, gen_connect_cost_per_mw
    Optional columns are:
        gen_dbid, gen_scheduled_outage_rate, gen_forced_outage_rate,
        gen_capacity_limit_mw, gen_unit_size, gen_ccs_energy_load,
        gen_ccs_capture_efficiency, gen_min_build_capacity, gen_is_cogen,
        gen_is_distributed

    The following file lists existing builds of projects, and is
    optional for simulations where there is no existing capacity:

    gen_build_predetermined.csv
        GENERATION_PROJECT, build_year, gen_predetermined_cap

    The following file is mandatory, because it sets cost parameters for
    both existing and new project buildouts:

    gen_build_costs.csv
        GENERATION_PROJECT, build_year, gen_overnight_cost, gen_fixed_om
"""

import os
from pyomo.environ import *
from switch_model.financials import capital_recovery_factor as crf
from switch_model.reporting import write_table
from switch_model.tools.graph import graph
from switch_model.utilities.scaling import get_assign_default_value_rule

dependencies = (
    "switch_model.timescales",
    "switch_model.balancing.load_zones",
    "switch_model.financials",
    "switch_model.energy_sources.properties.properties",
)


def define_components(mod):
    """

    Adds components to a Pyomo abstract model object to describe
    generation and storage projects. Unless otherwise stated, all power
    capacity is specified in units of MW and all sets and parameters
    are mandatory.

    GENERATION_PROJECTS is the set of generation and storage projects that
    have been built or could potentially be built. A project is a combination
    of generation technology, load zone and location. A particular build-out
    of a project should also include the year in which construction was
    complete and additional capacity came online. Members of this set are
    abbreviated as gen in parameter names and g in indexes. Use of p instead
    of g is discouraged because p is reserved for period.

    gen_dbid[g] is an external database id for each generation project. This is
    an optional parameter than defaults to the project index.

    gen_tech[g] describes what kind of technology a generation project is
    using.

    gen_load_zone[g] is the load zone this generation project is built in.

    VARIABLE_GENS is a subset of GENERATION_PROJECTS that only includes
    variable generators such as wind or solar that have exogenous
    constraints on their energy production.

    BASELOAD_GENS is a subset of GENERATION_PROJECTS that only includes
    baseload generators such as coal or geothermal.

    GENS_IN_ZONE[z in LOAD_ZONES] is an indexed set that lists all
    generation projects within each load zone.

    CAPACITY_LIMITED_GENS is the subset of GENERATION_PROJECTS that are
    capacity limited. Most of these will be generator types that are resource
    limited like wind, solar or geothermal, but this can be specified for any
    generation project. Some existing or proposed generation projects may have
    upper bounds on increasing capacity or replacing capacity as it is retired
    based on permits or local air quality regulations.

    gen_capacity_limit_mw[g] is defined for generation technologies that are
    resource limited and do not compete for land area. This describes the
    maximum possible capacity of a generation project in units of megawatts.

    -- CONSTRUCTION --

    GEN_BLD_YRS is a two-dimensional set of generation projects and the
    years in which construction or expansion occured or can occur. You
    can think of a project as a physical site that can be built out over
    time. BuildYear is the year in which construction is completed and
    new capacity comes online, not the year when constrution begins.
    BuildYear will be in the past for existing projects and will be the
    first year of an investment period for new projects. Investment
    decisions are made for each project/invest period combination. This
    set is derived from other parameters for all new construction. This
    set also includes entries for existing projects that have already
    been built and planned projects whose capacity buildouts have already been
    decided; information for legacy projects come from other files
    and their build years will usually not correspond to the set of
    investment periods. There are two recommended options for
    abbreviating this set for denoting indexes: typically this should be
    written out as (g, build_year) for clarity, but when brevity is
    more important (g, b) is acceptable.

    NEW_GEN_BLD_YRS is a subset of GEN_BLD_YRS that only
    includes projects that have not yet been constructed. This is
    derived by joining the set of GENERATION_PROJECTS with the set of
    NEW_GENERATION_BUILDYEARS using generation technology.

    PREDETERMINED_GEN_BLD_YRS is a subset of GEN_BLD_YRS that
    only includes existing or planned projects that are not subject to
    optimization.

    gen_predetermined_cap[(g, build_year) in PREDETERMINED_GEN_BLD_YRS] is
    a parameter that describes how much capacity was built in the past
    for existing projects, or is planned to be built for future projects.

    BuildGen[g, build_year] is a decision variable that describes
    how much capacity of a project to install in a given period. This also
    stores the amount of capacity that was installed in existing projects
    that are still online.

    GenCapacity[g, period] is an expression that returns the total
    capacity online in a given period. This is the sum of installed capacity
    minus all retirements.

    Max_Build_Potential[g] is a constraint defined for each project
    that enforces maximum capacity limits for resource-limited projects.

        GenCapacity <= gen_capacity_limit_mw

    NEW_GEN_WITH_MIN_BUILD_YEARS is the subset of NEW_GEN_BLD_YRS for
    which minimum capacity build-out constraints will be enforced.

    BuildMinGenCap[g, build_year] is a binary variable that indicates
    whether a project will build capacity in a period or not. If the model is
    committing to building capacity, then the minimum must be enforced.

    Enforce_Min_Build_Lower[g, build_year]  and
    Enforce_Min_Build_Upper[g, build_year] are a pair of constraints that
    force project build-outs to meet the minimum build requirements for
    generation technologies that have those requirements. They force BuildGen
    to be 0 when BuildMinGenCap is 0, and to be greater than
    g_min_build_capacity when BuildMinGenCap is 1. In the latter case,
    the upper constraint should be non-binding; the upper limit is set to 10
    times the peak non-conincident demand of the entire system.

    --- OPERATIONS ---

    PERIODS_FOR_GEN_BLD_YR[g, build_year] is an indexed
    set that describes which periods a given project build will be
    operational.

    BLD_YRS_FOR_GEN_PERIOD[g, period] is a complementary
    indexed set that identify which build years will still be online
    for the given project in the given period. For some project-period
    combinations, this will be an empty set.

    GEN_PERIODS describes periods in which generation projects
    could be operational. Unlike the related sets above, it is not
    indexed. Instead it is specified as a set of (g, period)
    combinations useful for indexing other model components.


    --- COSTS ---

    gen_connect_cost_per_mw[g] is the cost of grid upgrades to support a
    new project, in dollars per peak MW. These costs include new
    transmission lines to a substation, substation upgrades and any
    other grid upgrades that are needed to deliver power from the
    interconnect point to the load center or from the load center to the
    broader transmission network.

    The following cost components are defined for each project and build
    year. These parameters will always be available, but will typically
    be populated by the generic costs specified in generator costs
    inputs file and the load zone cost adjustment multipliers from
    load_zones inputs file.

    gen_overnight_cost[g, build_year] is the overnight capital cost per
    MW of capacity for building a project in the given period. By
    "installed in the given period", I mean that it comes online at the
    beginning of the given period and construction starts before that.

    gen_fixed_om[g, build_year] is the annual fixed Operations and
    Maintenance costs (O&M) per MW of capacity for given project that
    was installed in the given period.

    -- Derived cost parameters --

    gen_capital_cost_annual[g, build_year] is the annualized loan
    payments for a project's capital and connection costs in units of
    $/MW per year. This is specified in non-discounted real dollars in a
    future period, not real dollars in net present value.

    Proj_Fixed_Costs_Annual[g, period] is the total annual fixed
    costs (capital as well as fixed operations & maintenance) incurred
    by a project in a period. This reflects all of the builds are
    operational in the given period. This is an expression that reflect
    decision variables.

    ProjFixedCosts[period] is the sum of
    Proj_Fixed_Costs_Annual[g, period] for all projects that could be
    online in the target period. This aggregation is performed for the
    benefit of the objective function.

    TODO:
    - Allow early capacity retirements with savings on fixed O&M

    """
    # This set is defined by generation_projects_info.csv
    mod.GENERATION_PROJECTS = Set(dimen=1, input_file="generation_projects_info.csv")
    mod.gen_dbid = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        default=lambda m, g: g,
        within=Any,
    )
    mod.gen_tech = Param(
        mod.GENERATION_PROJECTS, input_file="generation_projects_info.csv", within=Any
    )
    mod.GENERATION_TECHNOLOGIES = Set(
        ordered=False,
        initialize=lambda m: {m.gen_tech[g] for g in m.GENERATION_PROJECTS},
    )
    mod.gen_energy_source = Param(
        mod.GENERATION_PROJECTS,
        within=Any,
        input_file="generation_projects_info.csv",
        validate=lambda m, val, g: val in m.ENERGY_SOURCES or val == "multiple",
    )
    mod.gen_load_zone = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=mod.LOAD_ZONES,
    )
    mod.gen_max_age = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=PositiveIntegers,
    )
    mod.gen_is_variable = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=Boolean,
    )
    mod.gen_is_baseload = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=Boolean,
        default=False,
    )
    mod.gen_is_cogen = Param(
        mod.GENERATION_PROJECTS,
        within=Boolean,
        default=False,
        input_file="generation_projects_info.csv",
    )
    mod.gen_is_distributed = Param(
        mod.GENERATION_PROJECTS,
        within=Boolean,
        default=False,
        input_file="generation_projects_info.csv",
    )
    mod.gen_scheduled_outage_rate = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=PercentFraction,
        default=0,
    )
    mod.gen_forced_outage_rate = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=PercentFraction,
        default=0,
    )
    mod.min_data_check(
        "GENERATION_PROJECTS",
        "gen_tech",
        "gen_energy_source",
        "gen_load_zone",
        "gen_max_age",
        "gen_is_variable",
    )

    """Construct GENS_* indexed sets efficiently with a
    'construction dictionary' pattern: on the first call, make a single
    traversal through all generation projects to generate a complete index,
    use that for subsequent lookups, and clean up at the last call."""

    def GENS_IN_ZONE_init(m, z):
        if not hasattr(m, "GENS_IN_ZONE_dict"):
            m.GENS_IN_ZONE_dict = {_z: [] for _z in m.LOAD_ZONES}
            for g in m.GENERATION_PROJECTS:
                m.GENS_IN_ZONE_dict[m.gen_load_zone[g]].append(g)
        result = m.GENS_IN_ZONE_dict.pop(z)
        if not m.GENS_IN_ZONE_dict:
            del m.GENS_IN_ZONE_dict
        return result

    mod.GENS_IN_ZONE = Set(mod.LOAD_ZONES, initialize=GENS_IN_ZONE_init)
    mod.VARIABLE_GENS = Set(
        initialize=mod.GENERATION_PROJECTS, filter=lambda m, g: m.gen_is_variable[g]
    )
    mod.VARIABLE_GENS_IN_ZONE = Set(
        mod.LOAD_ZONES,
        initialize=lambda m, z: [g for g in m.GENS_IN_ZONE[z] if m.gen_is_variable[g]],
    )
    mod.BASELOAD_GENS = Set(
        initialize=mod.GENERATION_PROJECTS, filter=lambda m, g: m.gen_is_baseload[g]
    )

    def GENS_BY_TECHNOLOGY_init(m, t):
        if not hasattr(m, "GENS_BY_TECH_dict"):
            m.GENS_BY_TECH_dict = {_t: [] for _t in m.GENERATION_TECHNOLOGIES}
            for g in m.GENERATION_PROJECTS:
                m.GENS_BY_TECH_dict[m.gen_tech[g]].append(g)
        result = m.GENS_BY_TECH_dict.pop(t)
        if not m.GENS_BY_TECH_dict:
            del m.GENS_BY_TECH_dict
        return result

    mod.GENS_BY_TECHNOLOGY = Set(
        mod.GENERATION_TECHNOLOGIES, initialize=GENS_BY_TECHNOLOGY_init
    )

    mod.CAPACITY_LIMITED_GENS = Set(within=mod.GENERATION_PROJECTS)
    mod.gen_capacity_limit_mw = Param(
        mod.CAPACITY_LIMITED_GENS,
        input_file="generation_projects_info.csv",
        input_optional=True,
        within=NonNegativeReals,
    )
    mod.DISCRETELY_SIZED_GENS = Set(within=mod.GENERATION_PROJECTS)
    mod.gen_unit_size = Param(
        mod.DISCRETELY_SIZED_GENS,
        input_file="generation_projects_info.csv",
        input_optional=True,
        within=PositiveReals,
    )
    mod.CCS_EQUIPPED_GENS = Set(within=mod.GENERATION_PROJECTS)
    mod.gen_ccs_capture_efficiency = Param(
        mod.CCS_EQUIPPED_GENS,
        input_file="generation_projects_info.csv",
        input_optional=True,
        within=PercentFraction,
    )
    mod.gen_ccs_energy_load = Param(
        mod.CCS_EQUIPPED_GENS,
        input_file="generation_projects_info.csv",
        input_optional=True,
        within=PercentFraction,
    )

    mod.gen_uses_fuel = Param(
        mod.GENERATION_PROJECTS,
        initialize=lambda m, g: (
            m.gen_energy_source[g] in m.FUELS or m.gen_energy_source[g] == "multiple"
        ),
    )
    mod.NON_FUEL_BASED_GENS = Set(
        initialize=mod.GENERATION_PROJECTS, filter=lambda m, g: not m.gen_uses_fuel[g]
    )
    mod.FUEL_BASED_GENS = Set(
        initialize=mod.GENERATION_PROJECTS, filter=lambda m, g: m.gen_uses_fuel[g]
    )

    mod.gen_full_load_heat_rate = Param(
        mod.FUEL_BASED_GENS,
        input_file="generation_projects_info.csv",
        within=NonNegativeReals,
    )
    mod.MULTIFUEL_GENS = Set(
        initialize=mod.GENERATION_PROJECTS,
        filter=lambda m, g: m.gen_energy_source[g] == "multiple",
    )
    mod.FUELS_FOR_MULTIFUEL_GEN = Set(mod.MULTIFUEL_GENS, within=mod.FUELS)
    mod.FUELS_FOR_GEN = Set(
        mod.FUEL_BASED_GENS,
        initialize=lambda m, g: (
            m.FUELS_FOR_MULTIFUEL_GEN[g]
            if g in m.MULTIFUEL_GENS
            else [m.gen_energy_source[g]]
        ),
    )

    def GENS_BY_ENERGY_SOURCE_init(m, e):
        if not hasattr(m, "GENS_BY_ENERGY_dict"):
            m.GENS_BY_ENERGY_dict = {_e: [] for _e in m.ENERGY_SOURCES}
            for g in m.GENERATION_PROJECTS:
                if g in m.FUEL_BASED_GENS:
                    for f in m.FUELS_FOR_GEN[g]:
                        m.GENS_BY_ENERGY_dict[f].append(g)
                else:
                    m.GENS_BY_ENERGY_dict[m.gen_energy_source[g]].append(g)
        result = m.GENS_BY_ENERGY_dict.pop(e)
        if not m.GENS_BY_ENERGY_dict:
            del m.GENS_BY_ENERGY_dict
        return result

    mod.GENS_BY_ENERGY_SOURCE = Set(
        mod.ENERGY_SOURCES, initialize=GENS_BY_ENERGY_SOURCE_init
    )
    mod.GENS_BY_NON_FUEL_ENERGY_SOURCE = Set(
        mod.NON_FUEL_ENERGY_SOURCES, initialize=lambda m, s: m.GENS_BY_ENERGY_SOURCE[s]
    )
    mod.GENS_BY_FUEL = Set(
        mod.FUELS, initialize=lambda m, f: m.GENS_BY_ENERGY_SOURCE[f]
    )

    # This set is defined by gen_build_predetermined.csv
    mod.PREDETERMINED_GEN_BLD_YRS = Set(
        input_file="gen_build_predetermined.csv", input_optional=True, dimen=2
    )
    mod.PREDETERMINED_BLD_YRS = Set(
        dimen=1,
        ordered=False,
        initialize=lambda m: set(bld_yr for (g, bld_yr) in m.PREDETERMINED_GEN_BLD_YRS),
        doc="Set of all the years where pre-determined builds occurs.",
    )

    # This set is defined by gen_build_costs.csv
    mod.GEN_BLD_YRS = Set(
        dimen=2,
        input_file="gen_build_costs.csv",
        validate=lambda m, g, bld_yr: (
            (g, bld_yr) in m.PREDETERMINED_GEN_BLD_YRS
            or (g, bld_yr) in m.GENERATION_PROJECTS * m.PERIODS
        ),
    )
    mod.NEW_GEN_BLD_YRS = Set(
        dimen=2, initialize=lambda m: m.GEN_BLD_YRS - m.PREDETERMINED_GEN_BLD_YRS
    )
    mod.gen_predetermined_cap = Param(
        mod.PREDETERMINED_GEN_BLD_YRS,
        input_file="gen_build_predetermined.csv",
        within=NonNegativeReals,
    )
    mod.min_data_check("gen_predetermined_cap")

    mod.gen_retirement_year = Param(
        mod.GENERATION_PROJECTS,
        default=0,
        input_file="generation_projects_info.csv",
        input_optional=True,
    )

    def gen_build_can_operate_in_period(m, g, build_year, period):
        # If a period has the same name as a predetermined build year then we have a problem.
        # For example, consider what happens if we have both a period named 2020
        # and a predetermined build in 2020. In this case, "build_year in m.PERIODS"
        # will be True even if the project is a 2020 predetermined build.
        # This will result in the "online" variable being the start of the period rather
        # than the prebuild year which can cause issues such as the project retiring too soon.
        # To prevent this we've added the no_predetermined_bld_yr_vs_period_conflict BuildCheck below.
        if build_year in m.PERIODS:
            online = m.period_start[build_year]
        else:
            online = build_year

        # Add retirement year
        if m.gen_retirement_year[g] == 0:
            retirement = online + m.gen_max_age[g]
        else:
            retirement = m.gen_retirement_year[g]
        # Previously the code read return online <= m.period_start[period] < retirement
        # However using the midpoint of the period as the "cutoff" seems more correct so
        # we've made the switch.
        return (
            online
            <= m.period_start[period] + 0.5 * m.period_length_years[period]
            < retirement
        )

    # This verifies that a predetermined build year doesn't conflict with a period since if that's the case
    # gen_build_can_operate_in_period will mistaken the prebuild for an investment build
    # (see note in gen_build_can_operate_in_period)
    mod.no_predetermined_bld_yr_vs_period_conflict = BuildCheck(
        mod.PREDETERMINED_BLD_YRS, mod.PERIODS, rule=lambda m, bld_yr, p: bld_yr != p
    )

    # The set of periods when a project built in a certain year will be online
    mod.PERIODS_FOR_GEN_BLD_YR = Set(
        mod.GEN_BLD_YRS,
        within=mod.PERIODS,
        ordered=True,
        initialize=lambda m, g, bld_yr: [
            period
            for period in m.PERIODS
            if gen_build_can_operate_in_period(m, g, bld_yr, period)
        ],
    )

    mod.BLD_YRS_FOR_GEN = Set(
        mod.GENERATION_PROJECTS,
        ordered=False,
        initialize=lambda m, g: set(
            bld_yr for (gen, bld_yr) in m.GEN_BLD_YRS if gen == g
        ),
    )

    # The set of build years that could be online in the given period
    # for the given project.
    mod.BLD_YRS_FOR_GEN_PERIOD = Set(
        mod.GENERATION_PROJECTS,
        mod.PERIODS,
        ordered=False,
        initialize=lambda m, g, period: set(
            bld_yr
            for bld_yr in m.BLD_YRS_FOR_GEN[g]
            if gen_build_can_operate_in_period(m, g, bld_yr, period)
        ),
    )
    # The set of periods when a generator is available to run
    mod.PERIODS_FOR_GEN = Set(
        mod.GENERATION_PROJECTS,
        initialize=lambda m, g: [
            p for p in m.PERIODS if len(m.BLD_YRS_FOR_GEN_PERIOD[g, p]) > 0
        ],
    )

    def bounds_BuildGen(model, g, bld_yr):
        if (g, bld_yr) in model.PREDETERMINED_GEN_BLD_YRS:
            return (
                model.gen_predetermined_cap[g, bld_yr],
                model.gen_predetermined_cap[g, bld_yr],
            )
        elif g in model.CAPACITY_LIMITED_GENS:
            # This does not replace Max_Build_Potential because
            # Max_Build_Potential applies across all build years.
            return (0, model.gen_capacity_limit_mw[g])
        else:
            return (0, None)

    mod.BuildGen = Var(mod.GEN_BLD_YRS, within=NonNegativeReals, bounds=bounds_BuildGen)
    # Some projects are retired before the first study period, so they
    # don't appear in the objective function or any constraints.
    # In this case, pyomo may leave the variable value undefined even
    # after a solve, instead of assigning a value within the allowed
    # range. This causes errors in the Progressive Hedging code, which
    # expects every variable to have a value after the solve. So as a
    # starting point we assign an appropriate value to all the existing
    # projects here.
    mod.BuildGen_assign_default_value = BuildAction(
        mod.PREDETERMINED_GEN_BLD_YRS,
        rule=get_assign_default_value_rule("BuildGen", "gen_predetermined_cap"),
    )

    # note: in pull request 78, commit e7f870d..., GEN_PERIODS
    # was mistakenly redefined as GENERATION_PROJECTS * PERIODS.
    # That didn't directly affect the objective function in the tests
    # because most code uses GEN_TPS, which was defined correctly.
    # But it did have some subtle effects on the main Hawaii model.
    # It would be good to have a test that this set is correct,
    # e.g., assertions that in the 3zone_toy model,
    # ('C-Coal_ST', 2020) in m.GEN_PERIODS and ('C-Coal_ST', 2030) not in m.GEN_PERIODS
    # and 'C-Coal_ST' in m.GENS_IN_PERIOD[2020] and 'C-Coal_ST' not in m.GENS_IN_PERIOD[2030]
    mod.GEN_PERIODS = Set(
        dimen=2,
        initialize=lambda m: [
            (g, p) for g in m.GENERATION_PROJECTS for p in m.PERIODS_FOR_GEN[g]
        ],
    )

    mod.GenCapacity = Expression(
        mod.GENERATION_PROJECTS,
        mod.PERIODS,
        rule=lambda m, g, period: sum(
            m.BuildGen[g, bld_yr] for bld_yr in m.BLD_YRS_FOR_GEN_PERIOD[g, period]
        ),
    )

    # We use a scaling factor to improve the numerical properties
    # of the model. The scaling factor was determined using trial
    # and error and this tool https://github.com/staadecker/lp-analyzer.
    # Learn more by reading the documentation on Numerical Issues.
    max_build_potential_scaling_factor = 1e-1
    mod.Max_Build_Potential = Constraint(
        mod.CAPACITY_LIMITED_GENS,
        mod.PERIODS,
        rule=lambda m, g, p: (
            m.gen_capacity_limit_mw[g] * max_build_potential_scaling_factor
            >= m.GenCapacity[g, p] * max_build_potential_scaling_factor
        ),
    )

    # The following components enforce minimum capacity build-outs.
    # Note that this adds binary variables to the model.
    mod.gen_min_build_capacity = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=NonNegativeReals,
        default=0,
    )
    mod.NEW_GEN_WITH_MIN_BUILD_YEARS = Set(
        dimen=2,
        initialize=mod.NEW_GEN_BLD_YRS,
        filter=lambda m, g, p: (m.gen_min_build_capacity[g] > 0),
    )
    mod.BuildMinGenCap = Var(mod.NEW_GEN_WITH_MIN_BUILD_YEARS, within=Binary)
    mod.Enforce_Min_Build_Lower = Constraint(
        mod.NEW_GEN_WITH_MIN_BUILD_YEARS,
        rule=lambda m, g, p: (
            m.BuildMinGenCap[g, p] * m.gen_min_build_capacity[g] <= m.BuildGen[g, p]
        ),
    )

    # Define a constant for enforcing binary constraints on project capacity
    # The value of 100 GW should be larger than any expected build size. For
    # perspective, the world's largest electric power plant (Three Gorges Dam)
    # is 22.5 GW. I tried using 1 TW, but CBC had numerical stability problems
    # with that value and chose a suboptimal solution for the
    # discrete_and_min_build example which is installing capacity of 3-5 MW.
    mod._gen_max_cap_for_binary_constraints = 10**5
    mod.Enforce_Min_Build_Upper = Constraint(
        mod.NEW_GEN_WITH_MIN_BUILD_YEARS,
        rule=lambda m, g, p: (
            m.BuildGen[g, p]
            <= m.BuildMinGenCap[g, p] * mod._gen_max_cap_for_binary_constraints
        ),
    )

    # Costs
    mod.gen_variable_om = Param(
        mod.GENERATION_PROJECTS,
        input_file="generation_projects_info.csv",
        within=NonNegativeReals,
    )
    mod.gen_connect_cost_per_mw = Param(
        mod.GENERATION_PROJECTS,
        within=NonNegativeReals,
        input_file="generation_projects_info.csv",
    )
    mod.min_data_check("gen_variable_om", "gen_connect_cost_per_mw")

    mod.gen_overnight_cost = Param(
        mod.GEN_BLD_YRS, input_file="gen_build_costs.csv", within=NonNegativeReals
    )
    mod.gen_fixed_om = Param(
        mod.GEN_BLD_YRS, input_file="gen_build_costs.csv", within=NonNegativeReals
    )
    mod.min_data_check("gen_overnight_cost", "gen_fixed_om")

    # Derived annual costs
    mod.gen_capital_cost_annual = Param(
        mod.GEN_BLD_YRS,
        initialize=lambda m, g, bld_yr: (
            (m.gen_overnight_cost[g, bld_yr] + m.gen_connect_cost_per_mw[g])
            * crf(m.interest_rate, m.gen_max_age[g])
        ),
    )

    mod.GenCapitalCosts = Expression(
        mod.GENERATION_PROJECTS,
        mod.PERIODS,
        rule=lambda m, g, p: sum(
            m.BuildGen[g, bld_yr] * m.gen_capital_cost_annual[g, bld_yr]
            for bld_yr in m.BLD_YRS_FOR_GEN_PERIOD[g, p]
        ),
    )
    mod.GenFixedOMCosts = Expression(
        mod.GENERATION_PROJECTS,
        mod.PERIODS,
        rule=lambda m, g, p: sum(
            m.BuildGen[g, bld_yr] * m.gen_fixed_om[g, bld_yr]
            for bld_yr in m.BLD_YRS_FOR_GEN_PERIOD[g, p]
        ),
    )
    # Summarize costs for the objective function. Units should be total
    # annual future costs in $base_year real dollars. The objective
    # function will convert these to base_year Net Present Value in
    # $base_year real dollars.
    mod.TotalGenFixedCosts = Expression(
        mod.PERIODS,
        rule=lambda m, p: sum(
            m.GenCapitalCosts[g, p] + m.GenFixedOMCosts[g, p]
            for g in m.GENERATION_PROJECTS
        ),
    )
    mod.Cost_Components_Per_Period.append("TotalGenFixedCosts")


def load_inputs(mod, switch_data, inputs_dir):
    # Construct sets of capacity-limited, ccs-capable and unit-size-specified
    # projects. These sets include projects for which these parameters have
    # a value
    if "gen_capacity_limit_mw" in switch_data.data():
        switch_data.data()["CAPACITY_LIMITED_GENS"] = {
            None: list(switch_data.data(name="gen_capacity_limit_mw").keys())
        }
    if "gen_unit_size" in switch_data.data():
        switch_data.data()["DISCRETELY_SIZED_GENS"] = {
            None: list(switch_data.data(name="gen_unit_size").keys())
        }
    if "gen_ccs_capture_efficiency" in switch_data.data():
        switch_data.data()["CCS_EQUIPPED_GENS"] = {
            None: list(switch_data.data(name="gen_ccs_capture_efficiency").keys())
        }
    # read FUELS_FOR_MULTIFUEL_GEN from gen_multiple_fuels.dat if available
    if os.path.isfile(os.path.join(inputs_dir, "gen_multiple_fuels.dat")):
        if "switch_model.generators.core.commit.fuel_use" in mod.module_list:
            raise NotImplementedError(
                "Multi-fuel generation is being used with generators.core.commit.fuel_use despite not being fully "
                "supported.\n"
                "Specifically, DispatchGenByFuel has not been constrained to match the true fuel use (GenFuelUseRate)."
                "Therefore, DispatchGenByFuel may result in incorrect values. DispatchGenByFuel is used when calculating"
                "non-CO2 emissions resulting in incorrect non-CO2 emission values. If there exists carbon_policies for"
                "non-CO2 emissions, the model may return an incorrect solution."
            )

        # TODO handle multi fuel input file
        raise NotImplementedError(
            "This code has not been updated to the latest version. We no longer handle .dat files."
        )


def post_solve(m, outdir):
    write_table(
        m,
        m.GEN_PERIODS,
        output_file=os.path.join(outdir, "gen_cap.csv"),
        headings=(
            "GENERATION_PROJECT",
            "PERIOD",
            "gen_tech",
            "gen_load_zone",
            "gen_energy_source",
            "GenCapacity",
            "GenCapitalCosts",
            "GenFixedOMCosts",
        ),
        # Indexes are provided as a tuple, so put (g,p) in parentheses to
        # access the two components of the index individually.
        values=lambda m, g, p: (
            g,
            p,
            m.gen_tech[g],
            m.gen_load_zone[g],
            m.gen_energy_source[g],
            m.GenCapacity[g, p],
            m.GenCapitalCosts[g, p],
            m.GenFixedOMCosts[g, p],
        ),
    )


@graph("generation_capacity_per_period", title="Online Generation Capacity Per Period")
def graph_capacity(tools):
    # Load gen_cap.csv
    gen_cap = tools.get_dataframe("gen_cap.csv")
    # Map energy sources to technology type
    gen_cap = tools.transform.gen_type(gen_cap)
    # Aggregate by gen_tech_type and PERIOD by summing the generation capacity
    capacity_df = gen_cap.pivot_table(
        index="PERIOD",
        columns="gen_type",
        values="GenCapacity",
        aggfunc=tools.np.sum,
        fill_value=0,
    )
    capacity_df = capacity_df * 1e-3  # Convert values to GW

    # For generation types that make less than 0.5% in every period, group them under "Other"
    # ---------
    # sum the generation across the energy_sources for each period, 0.5% of that is the cutoff for that period
    cutoff_value = 0.005
    cutoff_per_period = capacity_df.sum(axis=1) * cutoff_value
    # Check for each technology if it's below the cutoff for every period
    is_below_cutoff = capacity_df.lt(cutoff_per_period, axis=0).all()
    # groupby if the technology is below the cutoff
    capacity_df = capacity_df.groupby(
        axis=1, by=lambda c: "Other" if is_below_cutoff[c] else c
    ).sum()

    # Sort columns by the last period
    capacity_df = capacity_df.sort_values(by=capacity_df.index[-1], axis=1)

    # Plot
    # Get a new set of axis to create a breakdown of the generation capacity
    capacity_df.plot(
        kind="bar",
        ax=tools.get_axes(),
        stacked=True,
        ylabel="Capacity Online (GW)",
        xlabel="Period",
        color=tools.get_colors(len(capacity_df.index)),
    )

    tools.bar_label()


@graph(
    "buildout_gen_per_period",
    title="Built Capacity per Period",
    supports_multi_scenario=True,
)
def graph_buildout(tools):
    build_gen = tools.get_dataframe("BuildGen.csv", dtype={"GEN_BLD_YRS_1": str})
    build_gen = build_gen.rename(
        {
            "GEN_BLD_YRS_1": "GENERATION_PROJECT",
            "GEN_BLD_YRS_2": "build_year",
            "BuildGen": "Amount",
        },
        axis=1,
    )
    build_gen = tools.transform.build_year(build_gen)
    gen = tools.get_dataframe("generation_projects_info", from_inputs=True)
    gen = tools.transform.gen_type(gen)
    gen = gen[["GENERATION_PROJECT", "gen_type", "scenario_name"]]
    build_gen = build_gen.merge(
        gen,
        on=["GENERATION_PROJECT", "scenario_name"],
        how="left",
        validate="many_to_one",
    )
    groupby = (
        "build_year" if tools.num_scenarios == 1 else ["build_year", "scenario_name"]
    )
    build_gen = build_gen.pivot_table(
        index=groupby, columns="gen_type", values="Amount", aggfunc=tools.np.sum
    )
    build_gen = build_gen * 1e-3  # Convert values to GW
    build_gen = build_gen.sort_index(ascending=False, key=tools.sort_build_years)

    # For generation types that make less than 0.5% in every period, group them under "Other"
    # ---------
    # sum the generation across the energy_sources for each period, 0.5% of that is the cutoff for that period
    cutoff_value = 0.005
    cutoff_per_period = build_gen.sum(axis=1) * cutoff_value
    # Check for each technology if it's below the cutoff for every period
    is_below_cutoff = build_gen.lt(cutoff_per_period, axis=0).all()
    # groupby if the technology is below the cutoff
    build_gen = build_gen.groupby(
        axis=1, by=lambda c: "Other" if is_below_cutoff[c] else c
    ).sum()

    # Sort columns by the last period
    build_gen = build_gen.sort_values(by=build_gen.index[-1], axis=1)

    # Plot
    # Get a new set of axis to create a breakdown of the generation capacity
    build_gen.plot(
        kind="bar",
        ax=tools.get_axes(),
        stacked=True,
        ylabel="Capacity Online (GW)",
        xlabel="Period",
        color=tools.get_colors(len(build_gen.index)),
    )


@graph(
    "gen_buildout_per_tech_period",
    title="Buildout relative to max allowed for period",
    note="\nNote 1: This graph excludes predetermined buildout and projects that have no capacity limit."
    "\nTechnologies that contain projects with no capacity limit are marked by a * and their graphs may"
    "be misleading.",
)
def graph_buildout_per_tech(tools):
    # Load gen_cap.csv
    gen_cap = tools.get_dataframe("gen_cap.csv")
    # Map energy sources to technology type
    gen_cap = tools.transform.gen_type(gen_cap)
    # Load generation_projects_info.csv
    gen_info = tools.get_dataframe("generation_projects_info.csv", from_inputs=True)
    # Filter out projects with unlimited capacity since we can't consider those (coerce converts '.' to NaN)
    gen_info["gen_capacity_limit_mw"] = tools.pd.to_numeric(
        gen_info["gen_capacity_limit_mw"], errors="coerce"
    )
    # Set the type to be the same to ensure merge works
    gen_cap["GENERATION_PROJECT"] = gen_cap["GENERATION_PROJECT"].astype(object)
    gen_info["GENERATION_PROJECT"] = gen_info["GENERATION_PROJECT"].astype(object)
    # Add the capacity_limit to the gen_cap dataframe which has the total capacity at each period
    df = gen_cap.merge(
        gen_info[["GENERATION_PROJECT", "gen_capacity_limit_mw"]],
        on="GENERATION_PROJECT",
        validate="many_to_one",
    )
    # Get the predetermined generation
    predetermined = tools.get_dataframe("gen_build_predetermined.csv", from_inputs=True)
    # Filter out projects that are predetermined
    df = df[~df["GENERATION_PROJECT"].isin(predetermined["GENERATION_PROJECT"])]
    # Make PERIOD a category to ensure x-axis labels don't fill in years between period
    # TODO we should order this by period here to ensure they're in increasing order
    df["PERIOD"] = df["PERIOD"].astype("category")
    # Get gen_types that have projects with unlimited buildout
    unlimited_gen_types = df[df["gen_capacity_limit_mw"].isna()][
        "gen_type"
    ].drop_duplicates()
    # Filter out unlimited generation
    df = df[~df["gen_capacity_limit_mw"].isna()]
    if (
        df.size == 0
    ):  # in this case there are no projects that have a limit on build capacity
        return
        # Sum the GenCapacity and gen_capacity_limit_mw for all projects in the same period and type
    df = df.groupby(["PERIOD", "gen_type"]).sum()
    # Create a dataframe that's the division of the Capacity and the capacity limit
    df = (df["GenCapacity"] / df["gen_capacity_limit_mw"]).unstack()
    # Filter out generation types that don't make up a large percent of the energy mix to decultter graph
    # df = df.loc[:, ~is_below_cutoff]

    # Set the name of the legend.
    df = df.rename_axis("Type", axis="columns")
    # Add a * to tech
    df = df.rename(
        lambda c: f"{c}*" if c in unlimited_gen_types.values else c, axis="columns"
    )
    # Plot
    colors = tools.get_colors()
    if colors is not None:
        # Add the same colors but with a * to support our legend.
        colors.update({f"{k}*": v for k, v in colors.items()})
    ax = tools.get_axes()
    df.plot(ax=ax, kind="line", color=colors, xlabel="Period", marker="x")
    # Set the y-axis to use percent
    ax.yaxis.set_major_formatter(tools.plt.ticker.PercentFormatter(1.0))
    # Horizontal line at 100%
    ax.axhline(y=1, linestyle="--", color="b")


@graph("online_capacity_map", title="Map of online capacity per load zone.")
def buildout_map(tools):
    if not tools.maps.can_make_maps():
        return
    buildout = tools.get_dataframe("gen_cap.csv").rename(
        {"GenCapacity": "value"}, axis=1
    )
    buildout = tools.transform.gen_type(buildout)
    buildout = buildout.groupby(["gen_type", "gen_load_zone"], as_index=False)[
        "value"
    ].sum()
    buildout["value"] *= 1e-3  # Convert to GW
    ax = tools.maps.graph_pie_chart(buildout)
    transmission = tools.get_dataframe(
        "transmission.csv", convert_dot_to_na=True
    ).fillna(0)
    transmission = transmission.rename(
        {"trans_lz1": "from", "trans_lz2": "to", "TxCapacityNameplate": "value"}, axis=1
    )
    transmission = transmission[["from", "to", "value", "PERIOD"]]
    transmission = (
        transmission.groupby(["from", "to", "PERIOD"], as_index=False)
        .sum()
        .drop("PERIOD", axis=1)
    )
    # Rename the columns appropriately
    transmission.value *= 1e-3
    tools.maps.graph_transmission_capacity(transmission, ax=ax, legend=True)
