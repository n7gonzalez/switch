# Third-party packages
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from matplotlib.ticker import FormatStrFormatter
from calendar import monthrange
import re
import matplotlib.colors as colors
import matplotlib.cm as cmx

import geopandas as gpd
from shapely.geometry import Point, Polygon

from scipy.spatial import distance

pd.options.display.float_format = '{:,.2f}'.format

print("All the packages were uploaded")

def closest_node(x, y, nodes):
    node=[x,y]
    closest_index = distance.cdist([node], nodes).argmin()
    return nodes[closest_index]

def dms_to_dd(lat):
    deg, minutes, seconds, direction =  re.split('[°\'"]', lat)
    return (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in [' W', ' S'] else 1)

print("closest_node and dms_to_dd functions were uploaded.")

##### Substations
substation=pd.read_csv("electric_substations.csv")
substation.drop(columns=['type', 'properties/Interval', 'properties/Price Level', 'properties/LMP', 'properties/Congestion', 'properties/Energy','properties/Losses', 'properties/node_ba', 'geometry/type', 'geometry/coordinates/0', 'geometry/coordinates/1'], inplace=True)
substation.loc[:,'type']='electrical substation'
nsubs=len(substation['node'].unique())
substation.loc[:,'es_id']=range(0,nsubs)
substation.rename(columns={'latitude':'lat', 'longitude':'lon'}, inplace=True)
es=substation['node'].unique()
print("Electric substations were uploaded.")
print("There are " + str(len(es)) + " substations based on electric_substations.csv.")


##### Prices
price_data = pd.read_csv("data/LMP_20200101_20201231.csv", usecols=['time', 'node', 'lmp'])
price_data["time"]=pd.to_datetime(price_data["time"], format='%Y-%m-%dT%H:%M:%S')
price_data= price_data[price_data['node'].str[-4:]!='APND']
price_data.sort_values(by='time', inplace=True)
price_data.reset_index(drop=True, inplace=True)
print('CAISO prices were uploaded.')

#Filtered prices
filtered_price_data=pd.merge(price_data,substation[['node','es_id']], on='node', how='right')
print("The new price table was created based on electric substation.csv")

#US states maps
usa = gpd.read_file('states_21basic/states.shp')
print('The shape files of the US states were uploaded')

def localize(x):
     if (x.within(usa[usa.STATE_ABBR=='CA']['geometry'].reset_index()['geometry'][0])==1):
        return 'CA'
     if (x.within(usa[usa.STATE_ABBR=='WA']['geometry'].reset_index()['geometry'][0])==1):
        return 'WA'
     if (x.within(usa[usa.STATE_ABBR=='OR']['geometry'].reset_index()['geometry'][0])==1):
        return 'OR'

### Industry sites of interest
sites_of_interest=pd.read_csv("sites_of_interest.csv", usecols=['Name', 'Latitude', 'Longitude', 'State'])
sites_of_interest.loc[:,'lat']=sites_of_interest.apply(lambda x: dms_to_dd(x['Latitude']), axis=1)
sites_of_interest.loc[:, 'lon']=sites_of_interest.apply(lambda x: dms_to_dd(x['Longitude']), axis=1)
sites_of_interest.loc[:,'type']="industry interest"
sites_of_interest.drop(columns=['Latitude', 'Longitude'], inplace=True)
sites_of_interest.columns= sites_of_interest.columns.str.lower()
print('The industry sites of interest were uploaded.')

sites_of_interest.loc[:,'es_coordinate']=sites_of_interest.apply(lambda x: closest_node(x['lon'],x['lat'],substation[["lon", "lat"]].values.tolist()), axis=1)
sites_of_interest.loc[:,'es_lon']=sites_of_interest.loc[:,'es_coordinate'].str[0]
sites_of_interest.loc[:,'es_lat']=sites_of_interest.loc[:,'es_coordinate'].str[1]
sites_of_interest.loc[:,'es_id']=sites_of_interest.apply(lambda x: (substation.loc[(substation['lon']==x['es_lon']) & (substation['lat']==x['es_lat'])]['es_id'].reset_index())['es_id'][0], axis=1)
sites_of_interest.drop(columns='es_coordinate', inplace=True)


nodes_to_analyze=list(substation[substation.es_id.isin(sites_of_interest['es_id'])]['node'])

price_selected_nodes=filtered_price_data[filtered_price_data.node.isin(nodes_to_analyze)]

price_selected_nodes=(price_selected_nodes.groupby([price_selected_nodes['time'], price_selected_nodes['node']])['lmp']
            .sum()
            .unstack(fill_value=0))

geo_substation= gpd.GeoDataFrame(substation, geometry=gpd.points_from_xy(substation.lon, substation.lat))

geo_substation.loc[:,'state']=geo_substation.apply(lambda x: localize(x['geometry']), axis=1)

price_selected_nodes.columns = [c+'('+str(substation.loc[substation.node==c,'state'].reset_index()['state'][0])+')' for c in price_selected_nodes.columns]

price_selected_nodes

# Plot
fig = plt.figure(figsize=(11,5), dpi=150)

fig.patch.set_facecolor('white')

ax=fig.add_subplot(111)

ax=price_selected_nodes.resample(rule='1D').mean()[['BIGRIVR_6_N001 (CA)', 'MYRTLEPO_LNODE86 (OR)', 'GRI_LNODEXF1 (WA)']].plot.line(ax=ax)

# Label configuration
ax.set_ylabel("Price (USD/MWh)")
ax.set_xlabel("Time")

ax.grid(axis='y', linestyle='dashed')
ax.grid(axis='x', linestyle='dashed')
 
#handles, labels = ax.get_legend_handles_labels()
ax.legend(
   # handles[::-1],
   # labels[::-1],
   # bbox_to_anchor=(1, 1),
    loc="upper left",
    fontsize=9,
    handlelength=1,
    frameon=True,
    edgecolor="black",
    facecolor="white",
)

plt.savefig("figures/random_WECC_prices.png", transparent=False, bbox_inches='tight')