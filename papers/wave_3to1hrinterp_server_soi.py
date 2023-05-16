import numpy as np
import pandas as pd
from pandas import datetime
import time
from scipy.interpolate import interp1d

wave_data_2006 = pd.read_csv('wave_data_syn_west_2006.csv', index_col='time', parse_dates=True)

sites_of_interest = pd.read_csv('sites_of_interest.csv', index_col='index')

ids = sites_of_interest['wc_id']

for s in ids:
    upsample = wave_data_2006[wave_data_2006['loc_id']== s].resample('h').interpolate()
    new = upsample.append(upsample.iloc[[8757]])
    new = new.append(upsample.iloc[[8757]])
    new = new.reset_index(drop=False)
    new['time'].iloc[[8758]] = upsample.iloc[[8757]].index + pd.DateOffset(hours=1)
    new['time'].iloc[[8759]] = upsample.iloc[[8757]].index + pd.DateOffset(hours=2)
    new = new.set_index('time', drop=True)
    if (s == ids[0]):
        wave_data = new.copy()
    else:
        wave_data = pd.concat([wave_data, new])

wave_data.to_csv('wave_data_syn_west_2006_hourly_soi.csv',index=True)