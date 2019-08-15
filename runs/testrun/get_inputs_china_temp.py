'''
get_inputs_china.py

A version of get_inputs for SWITCH-China which connects to the database, using more explicit and
less hardcoded authentication, and imports the data needed by scenarios for the SWITCH-China model.

Adapted form the SWITCH-WECC model's script get_inputs.py.

Copyright 2019 The Switch Authors. All rights reserved.
Licensed under the Apache License, Version 2, which is in the LICENSE file.

'''

import psycopg2
import paramiko
import getpass
import os
import argparse
import sys
import time

# scenario_temp details dictionary. In the original script, this information was stored immediately after
# being found in the scenario_temp table. In this version of the script, all queries are handled in
# functions, so a variable initialized in get_scenario would not have the proper scope to be used in
# later queries.
scenario_details = dict()

parser = argparse.ArgumentParser(usage='get_switch_pyomo_input_tables.py [--help] [options]',
	description='Write SWITCH input files from database tables. Default \
	options asume an SSH tunnel has been opened between the local port 5432\
	and the Postgres port at the remote host where the database is stored.',
	formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-H', '--hostname', dest="host", type=str,
	default='switch-db2.erg.berkeley.edu', metavar='hostname',
	help='Database host address')
parser.add_argument('-P', '--port', dest="port", type=int, default=5432, metavar='port',
	help='Database host port')
parser.add_argument('-U', '--user', dest='user', type=str, default=getpass.getuser(), metavar='username',
	help='Server username')
parser.add_argument('-D', '--database', dest='database', type=str, default='switch_china',
	metavar='dbname', help='Database name')
parser.add_argument('-s', type=int, default=1, metavar='scenario_id',
	help='scenario_temp ID for the simulation')
parser.add_argument('-i', type=str, default='inputs', metavar='inputsdir',
	help='Directory where the inputs will be built')
parser.add_argument('-k', type=str, dest='key', required=True, metavar='key_path',
	help='Path to private key for authentication')
parser.add_argument('-c', type=str, dest='schema', default="test", metavar='SWITCH Schema',
	help='Database schema to access')
args = parser.parse_args()

def write_tab(fname, headers, cursor):
    with open(fname + '.tab', 'w') as f:
        f.write('\t'.join(headers) + '\n')
        for row in cursor:
            # Replace None values with dots for Pyomo. Also turn all datatypes into strings
            row_as_clean_strings = ['.' if element is None else str(element) for element in row]
            f.write('\t'.join(row_as_clean_strings) + "\n") # concatenates "line" separated by tabs, and appends the real \n.
            # Alex: the above line used to concatenate an os.linesep character.
    print "\tData written out to %s/%s.tab." % (args.i, fname)


## DATABASE QUERY FUNCTIONS ##

def get_scenario(cursor):
	cursor.execute(("""SELECT name, description, study_timeframe_id, time_sample_id, demand_scenario_id,
		fuel_simple_price_scenario_id, generation_plant_scenario_id, generation_plant_cost_scenario_id,
		generation_plant_existing_and_planned_scenario_id, hydro_simple_scenario_id, carbon_cap_scenario_id,
		supply_curves_scenario_id, regional_fuel_market_scenario_id, zone_to_regional_fuel_market_scenario_id,
		rps_scenario_id, enable_dr, enable_ev
		FROM {schema}.scenario_temp WHERE scenario_id = {id};""").format(schema=args.schema, id=args.s))
	s_details = cursor.fetchone()
	# Big update block. I can't think of a better way to do this off the top of my head.
	scenario_details.update({'name': s_details[0]})
	scenario_details.update({'description': s_details[1]})
	scenario_details.update({'study_timeframe_id': s_details[2]})
	scenario_details.update({'time_sample_id': s_details[3]})
	scenario_details.update({'demand_scenario_id': s_details[4]})
	scenario_details.update({'fuel_simple_price_scenario_id': s_details[5]})
	scenario_details.update({'generation_plant_scenario_id': s_details[6]})
	scenario_details.update({'generation_plant_cost_scenario_id': s_details[7]})
	scenario_details.update({'generation_plant_existing_and_planned_scenario_id': s_details[8]})
	scenario_details.update({'hydro_simple_scenario_id': s_details[9]})
	scenario_details.update({'carbon_cap_scenario_id': s_details[10]})
	scenario_details.update({'supply_curves_scenario_id': s_details[11]})
	scenario_details.update({'regional_fuel_market_scenario_id': s_details[12]})
	scenario_details.update({'zone_to_regional_fuel_market_scenario_id': s_details[13]})
	scenario_details.update({'rps_scenario_id': s_details[14]})
	scenario_details.update({'enable_dr': s_details[15]})
	scenario_details.update({'enable_ev': s_details[16]})
	scenario_details.update({'schema': args.schema})
	# In the original program, the timeseries_id_select string was initialized with very little fanfare in one line hidden amongst the timescales queries.
	scenario_details.update({'timeseries_id_select': "date_part('year', first_timepoint_utc)|| '_' || replace(sampled_timeseries_temp.name, ' ', '_') as timeseries"})
	# Write general scenario_temp parameters into a documentation file
	print 'Writing scenario_temp documentation into scenario_params.txt.'
	colnames = [desc[0] for desc in cursor.description]
	with open('scenario_params.txt', 'w') as f:
		f.write('scenario_temp id: %s\n' % args.s)
		f.write('scenario_temp name: %s\n' % scenario_details['name'])
		f.write('scenario_temp notes: %s\n' % scenario_details['description'])
		for colname in colnames:
			f.write('{}: {}\n'.format(colname, scenario_details[colname]))

	# Which input specification are we writing against?
	with open('switch_inputs_version.txt', 'w') as f: 
		f.write('2.0.0b2' + os.linesep)

def get_schema(cursor, schema_string):
	cursor.execute(("""
		SET search_path to {schema};""").format(schema=schema_string))

def get_timescales(cursor):

	cursor.execute(("""
		SELECT label, start_year as period_start, end_year as period_end
		FROM period_temp where study_timeframe_id={id}
		ORDER BY 1;""").format(id=scenario_details['study_timeframe_id']))
	write_tab('periods', ['INVESTMENT_PERIOD', 'period_start', 'period_end'], cursor)

	cursor.execute(("""
		SELECT {timeseries_id_select}, t.label as ts_period, hours_per_tp as ts_duration_of_tp,
			num_timepoints as ts_num_tps, scaling_to_period as ts_scale_to_period
		FROM sampled_timeseries_temp
		JOIN period_temp as t using(period_id)
		WHERE sampled_timeseries_temp.time_sample_id={id}
		ORDER BY label;""").format(timeseries_id_select=scenario_details['timeseries_id_select'], id=scenario_details['time_sample_id']))
	write_tab('timeseries', ['TIMESERIES', 'ts_period', 'ts_duration_of_tp', 'ts_num_tps', 'ts_scale_to_period'], cursor)

	cursor.execute(("""
		SELECT raw_timepoint_id as timepoint_id, to_char(timestamp_utc, 'YYYYMMDDHH24') as timestamp, {timeseries_id_select}
		FROM sampled_timepoint_temp as t
		JOIN sampled_timeseries_temp using(sampled_timeseries_id)
		WHERE t.time_sample_id={id}
		ORDER BY 1;""").format(timeseries_id_select=scenario_details['timeseries_id_select'], id=scenario_details['time_sample_id']))
	write_tab('timepoints', ['timepoint_id','timestamp','timeseries'], cursor)

def get_load_zones(cursor):
	
	cursor.execute("""SELECT name, ccs_distance_km as zone_ccs_distance_km, load_zone_id as zone_dbid
		FROM load_zone
		ORDER BY 1;""")
 	write_tab('load_zones', ['LOAD_ZONE', 'zone_ccs_distance_km', 'zone_dbid'], cursor)
 
	cursor.execute(("""
		SELECT load_zone_name, t.raw_timepoint_id as timepoint, 
			CASE WHEN demand_mw < 0 THEN 0 ELSE demand_mw END as zone_demand_mw
		FROM sampled_timepoint_temp as t
			JOIN demand_timeseries as d using(raw_timepoint_id)
		WHERE t.time_sample_id={id}
			AND demand_scenario_id={id2}
		ORDER BY 1,2;
		""").format(id=scenario_details['time_sample_id'], id2=scenario_details['demand_scenario_id']))
	write_tab('loads', ['LOAD_ZONE', 'TIMEPOINT', 'zone_demand_mw'], cursor)

def get_balancing_areas(cursor):

	cursor.execute("""SELECT
		balancing_area, quickstart_res_load_frac, quickstart_res_wind_frac, quickstart_res_solar_frac,spinning_res_load_frac, spinning_res_wind_frac,
		spinning_res_solar_frac FROM balancing_areas;""")
	write_tab('balancing_areas',['BALANCING_AREAS','quickstart_res_load_frac','quickstart_res_wind_frac','quickstart_res_solar_frac','spinning_res_load_frac','spinning_res_wind_frac','spinning_res_solar_frac'], cursor)

	cursor.execute("""SELECT name, reserves_area as balancing_area
		FROM load_zone; """)
	write_tab('zone_balancing_areas',['LOAD_ZONE','balancing_area'], cursor)

def get_transmission_lines(cursor):

	cursor.execute("""SELECT start_load_zone_id || '-' || end_load_zone_id, t1.name, t2.name,
		trans_length_km, trans_efficiency, existing_trans_cap_mw
		FROM transmission_lines
		join load_zone as t1 on(t1.load_zone_id=start_load_zone_id)
		join load_zone as t2 on(t2.load_zone_id=end_load_zone_id)
		ORDER BY 2,3;""")
	write_tab('transmission_lines',['TRANSMISSION_LINE','trans_lz1','trans_lz2','trans_length_km','trans_efficiency','existing_trans_cap'], cursor)

	cursor.execute("""SELECT start_load_zone_id || '-' || end_load_zone_id, transmission_line_id, derating_factor, terrain_multiplier,
		new_build_allowed FROM transmission_lines ORDER BY 1;""")
	write_tab('trans_optional_params', ['TRANSMISSION_LINE','trans_dbid','trans_derating_factor','trans_terrain_multiplier','trans_new_build_allowed'], cursor)

def get_fuels(cursor):

	cursor.execute("""SELECT name, co2_intensity, upstream_co2_intensity
		FROM energy_source WHERE is_fuel IS TRUE;""")
	write_tab('fuels',['fuel','co2_intensity','upstream_co2_intensity'], cursor)

	cursor.execute("""SELECT name FROM energy_source
		WHERE is_fuel IS FALSE;""")
	write_tab('non_fuel_energy_sources', ['energy_source'], cursor)

	cursor.execute(("""SELECT load_zone_name as load_zone, fuel, period_temp, AVG(fuel_price) as fuel_cost
		FROM
			(select load_zone_name, fuel, fuel_price, projection_year, 
				(case when 
					projection_year >= period_temp.start_year 
					AND projection_year <= period_temp.start_year + length_yrs -1 then label else 0 end) as period_temp
					FROM fuel_simple_price_yearly
					JOIN period_temp on(projection_year>=start_year)
				WHERE study_timeframe_id = {timeframe} and fuel_simple_scenario_id = {fuel}) as w
			WHERE period_temp!=0
			GROUP BY load_zone_name, fuel, period_temp
			ORDER BY 1,2,3;""").format(timeframe=scenario_details['study_timeframe_id'], fuel=scenario_details['fuel_simple_price_scenario_id']))
	write_tab('fuel_cost', ['load_zone', 'fuel', 'period_temp', 'fuel_cost'], cursor)

def get_generation(cursor):

	cursor.execute(("""SELECT generation_plant_id, gen_tech, energy_source as gen_energy_source, t2.name as gen_load_zone, max_age as gen_max_age,
			is_variable as gen_is_variable, is_baseload as gen_is_baseload, full_load_heat_rate as gen_full_load_heat_rate, variable_o_m as gen_variable_om,
			connect_cost_per_mw as gen_connect_cost_per_mw, generation_plant_id as gen_dbid, scheduled_outage_rate as gen_scheduled_outage_rate,
			forced_outage_rate as gen_forced_outage_rate, capacity_limit_mw as gen_capacity_limit_mw, min_build_capacity as gen_min_build_capacity,
			is_cogen as gen_is_cogen, storage_efficiency as gen_storage_efficiency, store_to_release_ratio as gen_store_to_release_ratio
		FROM generation_plant as t
		JOIN load_zone as t2 using(load_zone_id)
		JOIN generation_plant_scenario_member using(generation_plant_id)
		WHERE generation_plant_scenario_id={gen}
		ORDER BY gen_dbid;""").format(gen=scenario_details['generation_plant_scenario_id']))
	write_tab('generation_projects_info', ['GENERATION_PROJECT','gen_tech','gen_energy_source','gen_load_zone','gen_max_age','gen_is_variable','gen_is_baseload','gen_full_load_heat_rate','gen_variable_om','gen_connect_cost_per_mw','gen_dbid','gen_scheduled_outage_rate','gen_forced_outage_rate','gen_capacity_limit_mw', 'gen_min_build_capacity', 'gen_is_cogen', 'gen_storage_efficiency','gen_store_to_release_ratio'], cursor)

	cursor.execute(("""SELECT generation_plant_id, build_year, capacity as gen_predetermined_cap
		FROM generation_plant_existing_and_planned
		JOIN generation_plant as t using(generation_plant_id)
		JOIN generation_plant_scenario_member using(generation_plant_id)
		WHERE generation_plant_scenario_id={gen}
		AND generation_plant_existing_and_planned_scenario_id={ep};""").format(gen=scenario_details['generation_plant_scenario_id'], ep=scenario_details['generation_plant_existing_and_planned_scenario_id']))
	write_tab('gen_build_predetermined', ['GENERATION_PROJECT','build_year','gen_predetermined_cap'], cursor)

	cursor.execute("""SELECT generation_plant_id, generation_plant_cost_temp.build_year, 
			overnight_cost as gen_overnight_cost, fixed_o_m as gen_fixed_om,
			storage_energy_capacity_cost_per_mwh as gen_storage_energy_overnight_cost 
		FROM generation_plant_cost_temp
			JOIN generation_plant_existing_and_planned USING (generation_plant_id)
			JOIN generation_plant_scenario_member using(generation_plant_id)
			JOIN generation_plant as t1 using(generation_plant_id)
		WHERE generation_plant_scenario_id=%(gen_plant_scenario)s 
			AND generation_plant_cost_temp.generation_plant_cost_scenario_id=%(cost_scenario)s
			AND generation_plant_existing_and_planned_scenario_id=%(ep_id)s
		UNION
		SELECT generation_plant_id, period_temp.label, 
			avg(overnight_cost) as gen_overnight_cost, avg(fixed_o_m) as gen_fixed_om,
			avg(storage_energy_capacity_cost_per_mwh) as gen_storage_energy_overnight_cost
		FROM generation_plant_cost 
			JOIN generation_plant using(generation_plant_id) 
			JOIN period_temp on(build_year>=start_year and build_year<=end_year)
			JOIN generation_plant_scenario_member using(generation_plant_id)
        WHERE generation_plant_scenario_id=%(gen_plant_scenario)s 
			AND period_temp.study_timeframe_id=%(timeframe)s 
		  	AND generation_plant_cost.generation_plant_cost_scenario_id=%(cost_scenario)s
		  	AND generation_plant_id NOT IN
		  		(SELECT generation_plant_id FROM generation_plant_existing_and_planned)
		GROUP BY 1,2
		ORDER BY 1,2;""", 
		{'timeframe': scenario_details['study_timeframe_id'], 
    	 'cost_scenario': scenario_details['generation_plant_cost_scenario_id'],
		 'gen_plant_scenario': scenario_details['generation_plant_scenario_id'], 
		 'ep_id': scenario_details['generation_plant_existing_and_planned_scenario_id']
		})
	write_tab('gen_build_costs', ['GENERATION_PROJECT','build_year','gen_overnight_cost','gen_fixed_om', 'gen_storage_energy_overnight_cost'], cursor)

def get_variable_capacity(cursor):
	cursor.execute("""
		SELECT generation_plant_id, t.raw_timepoint_id, capacity_factor  
		FROM variable_capacity_factors_historical v
			JOIN projection_to_future_timepoint ON(v.raw_timepoint_id = historical_timepoint_id)
			JOIN generation_plant_scenario_member USING(generation_plant_id)
			JOIN sampled_timepoint_temp as t ON(t.raw_timepoint_id = future_timepoint_id)
		WHERE generation_plant_scenario_id = %(generation_plant_scenario)s
			AND t.time_sample_id=%(id)s""", {
			'id': scenario_details['time_sample_id'],
			'generation_plant_scenario': scenario_details['generation_plant_scenario_id']})
	write_tab('variable_capacity_factors', ['GENERATION_PROJECT','timepoint','gen_max_capacity_factor'], cursor)

def get_hydro_timeseries(cursor):
	cursor.execute(("""
		SELECT generation_plant_id as hydro_project, 
			{ts_id}, 
			CASE WHEN hydro_min_flow_mw <= 0 THEN 0.01 
			WHEN hydro_min_flow_mw > capacity_limit_mw*(1-forced_outage_rate) THEN capacity_limit_mw*(1-forced_outage_rate)
			ELSE hydro_min_flow_mw END, 
			CASE WHEN hydro_avg_flow_mw <= 0 THEN 0.01 ELSE
			least(hydro_avg_flow_mw, (capacity_limit_mw) * (1-forced_outage_rate)) END as hydro_avg_flow_mw
		from hydro_historical_monthly_capacity_factors
			join sampled_timeseries_temp on(month = date_part('month', first_timepoint_utc) and year = date_part('year', first_timepoint_utc))
			join generation_plant using (generation_plant_id)
			join generation_plant_scenario_member using(generation_plant_id)
		where generation_plant_scenario_id = {gen}
		and hydro_simple_scenario_id={hydro}
			and time_sample_id = {sample};
		""").format(ts_id=scenario_details['timeseries_id_select'], gen=scenario_details['generation_plant_scenario_id'], hydro=scenario_details['hydro_simple_scenario_id'], sample=scenario_details['time_sample_id']))
	write_tab('hydro_timeseries', ['hydro_project','timeseries','hydro_min_flow_mw', 'hydro_avg_flow_mw'], cursor)

def get_carbon_policies(cursor):
	cursor.execute(("""
		SELECT period_temp, AVG(carbon_cap_tco2_per_yr) as carbon_cap_tco2_per_yr,
			'.' as  carbon_cost_dollar_per_tco2
		FROM 
			(select carbon_cap_tco2_per_yr, year, 
				(case when 
					year >= period_temp.start_year 
					AND year <= period_temp.start_year + length_yrs -1 then label else 0 end) as period_temp
				FROM carbon_cap
				JOIN period_temp on(year>=start_year)
			WHERE study_timeframe_id = {id1} and carbon_cap_scenario_id = {id2}) as w
		WHERE period_temp!=0
		GROUP BY period_temp
		ORDER BY 1;""").format(id1=scenario_details['study_timeframe_id'], id2=scenario_details['carbon_cap_scenario_id']))
	write_tab('carbon_policies', ['period_temp', 'carbon_cap_tco2_per_yr', 'carbon_cap_tco2_per_yr_CA', 'carbon_cost_dollar_per_tco2'], cursor)

def get_rps_targets(cursor):
	cursor.execute(("""
		SELECT load_zone, w.period_temp as period_temp, avg(rps_target) as rps_target
		FROM
			(SELECT load_zone, rps_target,
				(CASE WHEN 
					year >= period_temp.start_year 
					AND year <= period_temp.start_year + length_yrs -1 then label else 0 end) as period_temp
				FROM rps_target
			JOIN period_temp on(year>=start_year)
			WHERE study_timeframe_id = {id1} and rps_scenario_id = {id2}) as w
		WHERE period_temp!=0
		GROUP BY load_zone, period_temp
		ORDER BY 1, 2;""").format(id1=scenario_details['study_timeframe_id'], id2=scenario_details['rps_scenario_id']))
	write_tab('rps_targets', ['load_zone','period_temp','rps_target'], cursor)

def get_regional_supply_curves(cursor):
	cursor.execute(("""
		SELECT regional_fuel_market, label as period_temp, tier, unit_cost, 
			(CASE WHEN max_avail_at_cost is null then 'inf' 
				ELSE max_avail_at_cost::varchar end) as max_avail_at_cost
			FROM fuel_supply_curves
			JOIN period_temp on(year>=start_year)
		WHERE year=FLOOR(period_temp.start_year + length_yrs/2-1)
		AND study_timeframe_id = {id1} 
		AND supply_curves_scenario_id = {id2};""").format(id1=scenario_details['study_timeframe_id'], id2=scenario_details['supply_curves_scenario_id']))
	write_tab('fuel_supply_curves', ['regional_fuel_market', 'period_temp', 'tier', 'unit_cost', 'max_avail_at_cost'], cursor)

	cursor.execute(("""
		SELECT regional_fuel_market, fuel 
		FROM regional_fuel_market
		WHERE regional_fuel_market_scenario_id={id};""").format(id=scenario_details['regional_fuel_market_scenario_id']))
	write_tab('regional_fuel_markets', ['regional_fuel_market', 'fuel'], cursor)

	cursor.execute(("""
		SELECT load_zone, regional_fuel_market
		FROM zone_to_regional_fuel_market
		WHERE zone_to_regional_fuel_market_scenario_id={id};""").format(id=scenario_details['zone_to_regional_fuel_market_scenario_id']))
	write_tab('zone_to_regional_fuel_market', ['load_zone', 'regional_fuel_market'], cursor)

def get_demand_response(cursor):
	cursor.execute(("""
		SELECT load_zone_name as load_zone, sampled_timepoint_temp.raw_timepoint_id AS timepoint, 
		CASE 
			WHEN load_zone_id>=10 and load_zone_id<=21 and extract(year from sampled_timepoint_temp.timestamp_utc)=2020 then 2/3.0*0.00754508*demand_mw
    		WHEN load_zone_id>=10 and load_zone_id<=21 and extract(year from sampled_timepoint_temp.timestamp_utc)=2030 then 2/3.0*0.045379091*demand_mw
    		WHEN load_zone_id>=10 and load_zone_id<=21 and extract(year from sampled_timepoint_temp.timestamp_utc)=2040 then 2/3.0*0.13360012*demand_mw
    		WHEN load_zone_id>=10 and load_zone_id<=21 and extract(year from sampled_timepoint_temp.timestamp_utc)=2050 then 2/3.0*0.206586443*demand_mw
    		WHEN (load_zone_id<10 or load_zone_id>21) and extract(year from sampled_timepoint_temp.timestamp_utc)=2020 then 2/3.0*0.001596991*demand_mw
    		WHEN (load_zone_id<10 or load_zone_id>21) and extract(year from sampled_timepoint_temp.timestamp_utc)=2030 then 2/3.0*0.013019135*demand_mw
    		WHEN (load_zone_id<10 or load_zone_id>21) and extract(year from sampled_timepoint_temp.timestamp_utc)=2040 then 2/3.0*0.048725149*demand_mw
    		WHEN (load_zone_id<10 or load_zone_id>21) and extract(year from sampled_timepoint_temp.timestamp_utc)=2050 then 2/3.0*0.12583359*demand_mw
    	END as dr_shift_down_limit,
    		NULL as dr_shift_up_limit
		FROM sampled_timepoint_temp
		LEFT JOIN demand_timeseries on sampled_timepoint_temp.raw_timepoint_id=demand_timeseries.raw_timepoint_id
		WHERE demand_scenario_id = {id1} 
			AND study_timeframe_id = {id2}
		ORDER BY demand_scenario_id, load_zone_id, sampled_timepoint_temp.raw_timepoint_id;""").format(id1=scenario_details['demand_scenario_id'], id2=scenario_details['study_timeframe_id']))
	write_tab('dr_data', ['LOAD_ZONE', 'timepoint', 'dr_shift_down_limit', 'dr_shift_up_limit'], cursor)

def get_ev(cursor):
	cursor.execute(("""
		SELECT load_zone_name as load_zone, raw_timepoint_id as timepoint,
		(CASE 
			WHEN raw_timepoint_id=max_raw_timepoint_id THEN ev_cumulative_charge_upper_mwh
			ELSE ev_cumulative_charge_lower_mwh
		END) AS ev_cumulative_charge_lower_mwh,
		ev_cumulative_charge_upper_mwh,
		ev_charge_limit as ev_charge_limit_mw
		FROM(
		--Table sample_points: with the sample points
			SELECT 
				load_zone_id, 
				ev_profiles_per_timepoint_v3.raw_timepoint_id, 
				sampled_timeseries_id, 
				sampled_timepoint_temp.timestamp_utc, 
				load_zone_name, 
				ev_cumulative_charge_lower_mwh, 
				ev_cumulative_charge_upper_mwh, 
				ev_charge_limit  FROM ev_profiles_per_timepoint_v3
			LEFT JOIN {schema}.sampled_timepoint_temp
			ON ev_profiles_per_timepoint_v3.raw_timepoint_id = sampled_timepoint_temp.raw_timepoint_id 
			WHERE study_timeframe_id = {id}
			--END sample_points
		) AS sample_points
		LEFT JOIN(
		--Table max_raw: with max raw_timepoint_id per _sample_timesseries_id
		SELECT 
			sampled_timeseries_id,
			MAX(raw_timepoint_id) AS max_raw_timepoint_id
		FROM sampled_timepoint_temp 
		WHERE study_timeframe_id = {id}
		GROUP BY sampled_timeseries_id
		--END max_raw
		) AS max_raw
		ON max_raw.sampled_timeseries_id=sample_points.sampled_timeseries_id
		ORDER BY load_zone_id, raw_timepoint_id ;
					""").format(id=scenario_details['study_timeframe_id']))
	write_tab('ev_limits', ['LOAD_ZONE', 'timepoint', 'ev_cumulative_charge_lower_mwh', 'ev_cumulative_charge_upper_mwh', 'ev_charge_limit_mw'], cursor)


##############################

## .DAT WRITE FUNCTIONS ##

def write_financials():
	with open('financials.dat','w') as f:
		f.write("param base_financial_year := 2016;\n")
		f.write("param interest_rate := .07;\n")
		f.write("param discount_rate := .07;\n")
	print "\tParameters written out to %s/financials.dat." % args.i

def write_trans_params():
	with open('trans_params.dat','w') as f:
		f.write("param trans_capital_cost_per_mw_km:=1150;\n") # $1150 opposed to $1000 to reflect change to US$2016
		f.write("param trans_lifetime_yrs:=20;\n") # Paty: check what lifetime has been used for the wecc
		f.write("param trans_fixed_om_fraction:=0.03;\n")
		#f.write("param distribution_loss_rate:=0.0652;\n")
	print "\tParameters written out to %s/trans_params.dat." % args.i

##########################

# Function to close the database and server connections.
def close_connections(cursor, conn, client):
	cursor.close()
	conn.close()
	client.close()

def main():

	'''
	# Parameters for server and database connection
	db = 'switch_china'
	hostname = "switch-db2.erg.berkeley.edu"
	username = "alathem"
	key = "../../../../erg_rsa_open.ppk"
	port = 22
	'''
	start_time = time.time()

	client = paramiko.SSHClient()
	client.load_system_host_keys()
	client.set_missing_host_key_policy(paramiko.WarningPolicy)
    
	client.connect(args.host, port=22, username=args.user, key_filename=args.key)

	# Database authentication information
	another_username = ""
	while (another_username.lower() != 'y') and (another_username.lower() != 'n'):
		another_username = raw_input("Is your database username %s? (y/n) > " % args.user)
	if another_username.lower() == "n":
		db_user = raw_input('Enter database username: ')
	else:
		print "Using %s as database user." % args.user
		db_user = args.user
	passw = getpass.getpass('Enter database password for user %s: ' % db_user)

	try:
		db_connection = psycopg2.connect(database=args.database, user=args.user, host=args.host, port=args.port, password=passw)
	except:
		print "\nDatabase connection failed. Make sure your password and username are correct and registered on the database."
		client.close()
		sys.exit(1)

	try:

		print "\nDatabase connection estabished.\n"

		if not os.path.exists(args.i):
			os.makedirs(args.i)
			print 'Inputs directory created...\n'
		else:
			print 'Inputs directory exists, so contents will be overwritten.\n'

		db_cursor = db_connection.cursor()
		# print "Cursor made.\n" # This is a debug line to ensure the database cursor was created.

		os.chdir(args.i) # change directory to the inputs folder

		## Table access function calls and DAT file construction
		get_scenario(db_cursor)
		print ""
		get_schema(db_cursor, scenario_details['schema'])
		get_timescales(db_cursor)
		get_load_zones(db_cursor)
		get_balancing_areas(db_cursor)
		get_transmission_lines(db_cursor)
		get_fuels(db_cursor)
		get_generation(db_cursor)
		get_variable_capacity(db_cursor) # Currently outputs nothing. Cause: projection_to_future_timepoints is empty
		get_hydro_timeseries(db_cursor) # Currently outputs nothing. Cause: sampled_timeseries_temp and
			# hydro_historical_monthly_capacity_factors do not overlap on their scenario_temp id.
		if scenario_details['carbon_cap_scenario_id'] is not None:
			get_carbon_policies(db_cursor)
		if scenario_details['rps_scenario_id'] is not None:
			get_rps_targets(db_cursor)
		if scenario_details['supply_curves_scenario_id'] is not None:
			get_regional_supply_curves(db_cursor)
		if scenario_details['enable_dr'] is not None:
			get_demand_response(db_cursor)
		if scenario_details['enable_ev'] is not None:
			get_ev(db_cursor)
		write_financials()
		write_trans_params()
		#####

		end_time = time.time()
		print '\nScript took %s seconds building input tables.' % (end_time-start_time)

	finally:
		# Shut down
		close_connections(db_cursor, db_connection, client)

if __name__ == '__main__':
	main()
