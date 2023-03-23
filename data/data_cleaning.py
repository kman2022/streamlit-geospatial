#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 15:40:27 2023
The purpose of this code is to clean up some of the data and to save the output to csv files
This will allow the maps to run faster and reduce some of the obvious errors in the underlying data

@author: kristian.lande@leveltenenergy.com
"""

import numpy as np
import pandas as pd

LOC_PATH = '/Users/kristian.lande@leveltenenergy.com/Documents/GitHub/streamlit-geospatial/data/'
IQ = 'queues_2021_clean_data.xlsx'

dt_cols = ['q_date','wd_date','ia_date','on_date','prop_date']
df_iq = pd.read_excel(LOC_PATH+IQ,sheet_name='data',parse_dates=dt_cols)

# q_date = date project entered queue; 24,303
df_iq['q_date'] = pd.to_datetime(df_iq['q_date'],errors ='coerce') #23,694 or 97.5%
# ia_date = date of signed interconnection agreement (if available); 2,526
df_iq['ia_date'] = pd.to_datetime(df_iq['ia_date'],errors ='coerce') # 2,524
df_iq['ia_year'] = df_iq['ia_date'].dt.year
# wd_date = date project withdrawn from queue (if applicable); 6,323
df_iq['wd_date'] = pd.to_datetime(df_iq['wd_date'],errors ='coerce') # 6,322
# on_date = date project became operational (if applicable); 1,571
df_iq['on_date'] = pd.to_datetime(df_iq['on_date'],errors ='coerce') # 1,571
# prop_date = proposed online date; 22,020
df_iq['prop_date'] = pd.to_datetime(df_iq['prop_date'],errors ='coerce') # 21,294 or 96.7% manually cleaned some dates in xls
df_iq['prop_year'] = np.where(pd.notnull(df_iq['prop_year']),df_iq['prop_date'].dt.year,0) # increased count by 2k



# any easy cleanings - no done manually in Excel
# original non_null
# notnull = df_iq['prop_date'].notnull()
# where to_datetime fails
# not_datetime = pd.to_datetime(df_iq['prop_date'],errors='coerce').isna()
# test = not_datetime * df_iq['prop_date']
# test = test.sort_values(ascending=False)

# cleaned up online and prop date (assumption is prop date was an estimate) made by developer
# cod = commercial operation date; combo on date and proposed on date
df_iq['cod']=np.where(~df_iq['on_date'].isnull(),df_iq['on_date'],df_iq['prop_date'])

# create the difference in months col IQ Request to COD
df_iq['diff_months_cod']=(df_iq['cod']-df_iq['q_date'])/np.timedelta64(1,'M')

# add cod year
df_iq['cod_year'] = df_iq['cod'].dt.year

# clean up records where the queue joined and cod is less than 3 months
# test1 = df_iq[df_iq.diff_months_cod < 3]
# test1.groupby('q_status').count() # 525 records, 279 withdrawn, 106 active, 106 operational
# for 246 records where the cod is less than 3 months, set that to 3 months
df_iq['diff_months_cod'] = np.where((df_iq['diff_months_cod']<3)&(df_iq['q_status']!='withdrawn'),3,df_iq['diff_months_cod'])

# create the difference in months col IQ Request to IA
df_iq['diff_months_ia']=(df_iq['ia_date']-df_iq['q_date'])/np.timedelta64(1,'M')

# add voltage lvl - cleaned up as number, these values could be off as the naming is not consistent across ISOs
df_iq['ix_voltage'] = df_iq["poi_name"].str.extract(r'(\d+)', expand=False)

df_iq.info()