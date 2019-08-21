# Copyright (c) 2015-2017 The Switch Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0, which is in the LICENSE file.

"""
Defines simple limitations on project dispatch without considering unit
commitment. This module is mutually exclusive with the operations.unitcommit
module which constrains dispatch to unit commitment decisions.
TODO:
Allow the usage of the commit module.
"""
import os
from pyomo.environ import *
from switch_model.reporting import write_table


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

    DispatchUpperLimit[(g, t) in GEN_TPS] is an
    expression that defines the upper bounds of dispatch subject to
    installed capacity, average expected outage rates, and renewable
    resource availability.

    DispatchLowerLimit[(g, t) in GEN_TPS] in an
    expression that defines the lower bounds of dispatch, which is 0
    except for baseload plants where is it the upper limit.

    Enforce_Dispatch_Lower_Limit[(g, t) in GEN_TPS] and
    Enforce_Dispatch_Upper_Limit[(g, t) in GEN_TPS] are
    constraints that limit DispatchGen to the upper and lower bounds
    defined above.

        DispatchLowerLimit <= DispatchGen <= DispatchUpperLimit

    GenFuelUseRate_Calculate[(g, t) in GEN_TPS]
    calculates fuel consumption for the variable GenFuelUseRate as
    DispatchGen * gen_full_load_heat_rate. The units become:
    MW * (MMBtu / MWh) = MMBTU / h

    DispatchGenByFuel[(g, t, f) in GEN_TP_FUELS]
    calculates power production by each project from each fuel during
    each timepoint.

    """

    # NOTE: DispatchBaseloadByPeriod should eventually be replaced by 
    # an "ActiveCapacityDuringPeriod" decision variable that applies to all
    # projects. This should be constrained
    # based on the amount of installed capacity each period, and then 
    # DispatchUpperLimit and DispatchLowerLimit should be calculated
    # relative to ActiveCapacityDuringPeriod. Fixed O&M (but not capital 
    # costs) should be calculated based on ActiveCapacityDuringPeriod.
    # This would allow mothballing (and possibly restarting) projects.

    # Choose flat operating level for baseload plants during each period
    # (not necessarily running all available capacity)
    # Note: this is unconstrained, because other constraints limit project 
    # dispatch during each timepoint and therefore the level of this variable.
    #mod.DispatchBaseloadByPeriod = Var(mod.BASELOAD_GENS, mod.PERIODS)

    mod.GEN_PERIODS_cur = Set(
        dimen=2,
        initialize=mod.GENERATION_PROJECTS * mod.PERIODS)   

   # mod.curtailment_source=Set()
   # mod.curtailment_source_par = Param(mod.curtailment_source)
    mod.PERIOD_ENERGY_MAX_CUR = Set(
    	dimen=2,
    	validate=lambda m, p, e: (
    		p in m.PERIODS and
    		e in m.ENERGY_SOURCES))
   
    #mod.gen_energy_source_cur = Param(mod.GENERATION_PROJECTS, 
    #    validate=lambda m,val,g: val in m.curtailment_source)

    mod.maximum_curtailment_CUR = Param(
    	mod.PERIOD_ENERGY_MAX_CUR,
    	within=NonNegativeReals,
        default=1)
    mod.ENERGY_SOURCES_CUR = Param(mod.PERIODS)

    mod.gen_energy_actual = Expression(
        #mod.PERIOD_ENERGY_MAX_CUR,
        mod.GEN_PERIODS_cur,
        rule=lambda m, p, e: sum(m.DispatchGen[g, t] * m.tp_weight[t]            
            for g in m.GENERATION_PROJECTS if m.gen_energy_source[g] == e
                for t in m.TPS_FOR_GEN_IN_PERIOD[g, p]))
    
    mod.gen_energy_ideal = Expression(
       # mod.PERIOD_ENERGY_MAX_CUR,
        mod.GEN_PERIODS_cur,
        rule=lambda m, p, e: sum(m.DispatchUpperLimit[g, t] * m.tp_weight[t]
            for g in m.GENERATION_PROJECTS if m.gen_energy_source[g] == e
                for t in m.TPS_FOR_GEN_IN_PERIOD[g, p]))    
    

    mod.gen_curtailment_ratio= Expression(
        mod.GEN_PERIODS_cur,
        rule=lambda m, p, e: (
            (m.gen_energy_ideal[p, e] - m.gen_energy_actual[p, e])/m.gen_energy_ideal[p, e] ))
   
    mod.gen_curtailment_ratio_cos= Constraint(
        mod.PERIOD_ENERGY_MAX_CUR,
        rule=lambda m, p, e: (
            sum(m.DispatchUpperLimit[g, t] * m.tp_weight[t]  for g in m.GENERATION_PROJECTS if m.gen_energy_source[g] == e
                    for t in m.TPS_FOR_GEN_IN_PERIOD[g, p]) - sum(m.DispatchGen[g, t] * m.tp_weight[t]  for g in m.GENERATION_PROJECTS if m.gen_energy_source[g] == e
                    for t in m.TPS_FOR_GEN_IN_PERIOD[g, p])
            <=sum(m.DispatchUpperLimit[g, t] * m.tp_weight[t]  for g in m.GENERATION_PROJECTS if m.gen_energy_source[g] == e
                    for t in m.TPS_FOR_GEN_IN_PERIOD[g, p] )*m.maximum_curtailment_CUR[p, e]
                ))

    #mod.Maximum_curtailment = Constraint(
    #	mod.PERIOD_ENERGY_MAX_CUR,
    #	rule=lambda m, p, e: (
    #		 m.gen_curtailment_ratio[p, e]<=m.maximum_curtailment_CUR[p, e]))



def load_inputs(mod, switch_data, inputs_dir):
    
    """
    The RPS target goals input file is mandatory, to discourage people from
    loading the module if it is not going to be used. It is not necessary to
    specify targets for all periods.
    
    Mandatory input files:
        rps_targets.tab
            PERIOD rps_target
    
    The optional parameter to define fuels as RPS eligible can be inputted
    in the following file:
        fuels.tab
            fuel  f_rps_eligible   
    """

    switch_data.load_aug(
    	filename=os.path.join(inputs_dir, 'curtailment_target.tab'),
    	select=('Period', 'Energy_Source', 'Maximum_curtailment'),
    	index=(mod.PERIOD_ENERGY_MAX_CUR),
    	param=[mod.maximum_curtailment_CUR])
    
    #switch_data.load_aug(
     #   filename=os.path.join(inputs_dir, 'curtailment_source.tab'),
     #   set=('curtailment_source'))
        #select=('fuel'),
        #index=(mod.curtailment_source),
        #param=[mod.curtailment_source_par])

def post_solve(instance, outdir):
    import switch_model.reporting as reporting
    #def get_row(mod, period):
        #row = [period,mod.PERIOD_ENERGY_MAX_CUR[period]]
    write_table(
        instance, instance.PERIOD_ENERGY_MAX_CUR,
        output_file=os.path.join(outdir, "curtailment.txt"),
        headings=( "PERIOD", "energy","gen_energy"),
        # Indexes are provided as a tuple, so put (g,p) in parentheses to
        # access the two components of the index individually.
        values=lambda m,(p,e): (p, e))

