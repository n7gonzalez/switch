"""

Defines simple limitations on project dispatch without considering unit
commitment. This module is mutually exclusive with the project.commit
module which constrains dispatch to unit committment decisions.

SYNOPSIS
>>> import switch_mod.utilities as utilities
>>> switch_modules = ('timescales', 'financials', 'load_zones', 'fuels',\
    'gen_tech', 'project.build', 'project.dispatch', 'project.no_commit')
>>> utilities.load_modules(switch_modules)
>>> switch_model = utilities.define_AbstractModel(switch_modules)
>>> inputs_dir = 'test_dat'
>>> switch_data = utilities.load_data(switch_model, inputs_dir, switch_modules)
>>> switch_instance = switch_model.create(switch_data)

Note, this can be tested with `python -m doctest project/no_commit.py`
within the switch_mod source directory.

Switch-pyomo is licensed under Apache License 2.0 More info at switch-model.org
"""

from pyomo.environ import *


def define_components(mod):
    """

    Adds components to a Pyomo abstract model object to constrain
    dispatch decisions subject to available capacity, renewable resource
    availability, and baseload restrictions. Unless otherwise stated,
    all power capacity is specified in units of MW and all sets and
    parameters are mandatory. This module estimates project dispatch
    limits and fuel consumption without consideration of unit
    commitment. This can be a useful approximation if fuel startup
    requirements are a small portion of overall fuel consumption, so
    that the aggregate fuel consumption with respect to energy
    production can be approximated as a line with a 0 intercept. This
    estimation method has been known to result in excessive cycling of
    Combined Cycle Gas Turbines in the SWITCH-WECC model.

    DispatchUpperLimit[(proj, t) in PROJ_DISPATCH_POINTS] is an
    expression that defines the upper bounds of dispatch subject to
    installed capacity, average expected outage rates, and renewable
    resource availability.

    DispatchLowerLimit[(proj, t) in PROJ_DISPATCH_POINTS] in an
    expression that defines the lower bounds of dispatch, which is 0
    except for baseload plants where is it the upper limit.

    Enforce_Dispatch_Lower_Limit[(proj, t) in PROJ_DISPATCH_POINTS] and
    Enforce_Dispatch_Upper_Limit[(proj, t) in PROJ_DISPATCH_POINTS] are
    constraints that limit DispatchProj to the upper and lower bounds
    defined above.

        DispatchLowerLimit <= DispatchProj <= DispatchUpperLimit

    ConsumeFuelProj_Calculate[(proj, t) in PROJ_DISPATCH_POINTS]
    calculates fuel consumption for the variable ConsumeFuelProj as
    DispatchProj * proj_full_load_heat_rate.


    """

    def DispatchUpperLimit_expr(m, proj, t):
        if proj in m.VARIABLE_PROJECTS:
            return (m.ProjCapacityTP[proj, t] * m.proj_availability[proj] *
                    m.prj_max_capacity_factor[proj, t])
        else:
            return m.ProjCapacityTP[proj, t] * m.proj_availability[proj]
    mod.DispatchUpperLimit = Expression(
        mod.PROJ_DISPATCH_POINTS,
        initialize=DispatchUpperLimit_expr)

    def DispatchLowerLimit_expr(m, proj, t):
        if proj in m.BASELOAD_PROJECTS:
            return DispatchUpperLimit_expr(m, proj, t)
        else:
            return 0
    mod.DispatchLowerLimit = Expression(
        mod.PROJ_DISPATCH_POINTS,
        initialize=DispatchLowerLimit_expr)

    mod.Enforce_Dispatch_Lower_Limit = Constraint(
        mod.PROJ_DISPATCH_POINTS,
        rule=lambda m, proj, t: (
            m.DispatchLowerLimit[proj, t] <= m.DispatchProj[proj, t]))
    mod.Enforce_Dispatch_Upper_Limit = Constraint(
        mod.PROJ_DISPATCH_POINTS,
        rule=lambda m, proj, t: (
            m.DispatchProj[proj, t] <= m.DispatchUpperLimit[proj, t]))

    mod.ConsumeFuelProj_Calculate = Constraint(
        mod.PROJ_FUEL_DISPATCH_POINTS,
        rule=lambda m, proj, t: (
            m.ConsumeFuelProj[proj, t] ==
            m.DispatchProj[proj, t] * m.proj_full_load_heat_rate[proj]))
