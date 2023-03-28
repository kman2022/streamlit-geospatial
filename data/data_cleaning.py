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
import geopandas as gpd

LOC_PATH = '/Users/kristian.lande@leveltenenergy.com/Documents/GitHub/streamlit-geospatial/data/'
IQ = 'queues_2021_clean_data.xlsx'

dt_cols = ['q_date','wd_date','ia_date','on_date','prop_date']
df_iq = pd.read_excel(LOC_PATH+IQ,sheet_name='data',parse_dates=dt_cols)

# q_date = date project entered queue; 24,380
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
volts = df_iq["poi_name"].str.extract(r'(\d+)\s[kK][vV]|(\d+)[kK][vV]', expand=False).fillna(0).astype(int)
volts['sum'] = volts[0]+volts[1]
df_iq['ix_voltage'] = volts['sum']

# create the difference in months col WD Request to withdraw date∫
df_iq['diff_months_wd']=(df_iq['wd_date']-df_iq['q_date'])/np.timedelta64(1,'M')

# df_iq.to_csv('df_iq_clean.csv')

# what is the diff between region(24,381) and entity(24,381)
df_iq['region'].unique()# 9 
df_iq['entity'].nunique() # 42
df_iq['utility'].nunique() # 228

# Probability of successful completion based on stage of study - what is the haircut and where is it trending?

# TREND USING SAME PARAMS AS REPORT
# tie their numbers
# Only 27%** of all projects requesting interconnection from 2000 to 2016 achieved commercial operation by year-end 2021
df_trend = df_iq[['q_year','q_status','cod_year','type_clean','mw1','region']]
df_trend = df_trend[(df_trend['q_year']>=2000)&(df_trend['q_year']<=2016)&(df_trend['cod_year']<=2021)]
# df_trend.groupby(['q_status']).count()
# x = 464+2682+35+6709
# 2682/x # ties out
# df_trend.to_csv('df_trend.csv')
## bin by online year
## toggle by technology
## toggle by location

# TIME to IA/COD/WD
# WD: All mkts
# IA: ERCOT, MISO, NYISO
# ON: CAISO, ERCOT, NYISO, PJM, WEST
df_trend_dur = df_iq[['q_year','q_status','cod_year','type_clean','mw1','region','ix_voltage','diff_months_ia','diff_months_cod','diff_months_wd']]
# df_trend_dur = df_trend_dur[(df_trend_dur['q_year']>=2000)&(df_trend_dur['q_year']<=2016)&(df_trend_dur['cod_year']<=2021)]
# df_trend_dur.to_csv('df_trend_dur.csv')
# seems some projects get withdrawn then placed back in queue
df_trend_sum = df_trend_dur[['q_status','diff_months_ia','diff_months_cod','diff_months_wd']]
df_trend_sum = df_trend_sum.groupby(['q_status']).mean(['diff_months_ia','diff_months_cod','diff_months_wd']) # 24.5 months to ia and 39 months to cod 
df_trend_sum







# import FIPS to match the geolocs
df_fips = pd.read_csv('fips.csv',dtype=str)
df_fips = df_fips[['Abbr.','FIPS']]
df_iq = pd.merge(df_iq,df_fips,how='left',left_on='state',right_on='Abbr.')
df_iq.info()
df_iq.drop(labels=['Abbr.'],axis=1, inplace=True)

# 24,380 records, 23,973 with state, 23,963 with county, 272 mnissing both state and county
# df_iq[df_iq.loc[ :,['state','county_1'] ].isnull().sum(axis=1) == 2]

# MATCH GEOLOC
geo_county=gpd.read_file(LOC_PATH+'us_counties.geojson')
geo_county['NAME'] = geo_county['NAME'].str.lower()
geo_c = geo_county[['NAME','STATEFP','geometry']]
geo_c.info()

# merge the geo to iq
# 19,653/23,973 or 18% did not join
df_iq_geo = df_iq.merge(geo_c,how='left',right_on=['NAME','STATEFP'],left_on=['county_1','FIPS'],indicator=True).reset_index()
df_iq_geo.drop(labels='_merge',axis=1,inplace=True)

## MAPPING FILE FOR QUEUE - cleaned active MISO, PJM and NYISO
df_iq_geo.to_csv('df_iq_geo.csv')
# active 6629/8151 or 18% did not join
# manually mapped around 200 in the maerkets for which we have cost data 3970/4100 so 96.8% active mapped in those markets
# df_iq_geo[(df_iq_geo['q_status']=='active')&(df_iq_geo['region'].isin(['PJM','MISO','NYISO']))].info()
df_iq_geo[(df_iq_geo['q_status']=='operational')&(df_iq_geo['region'])].info()

df_iq_geo_unmatch = df_iq_geo[['q_id','q_year','state','county_1','region','type_clean','q_status','geometry']]
df_iq_geo_unmatch = df_iq_geo_unmatch[(df_iq_geo_unmatch['q_year']>2016)&(df_iq_geo_unmatch['geometry'].isnull())&(df_iq_geo_unmatch['q_status']=='operational')]
df_iq_geo_unmatch[df_iq_geo_unmatch['region']=='MISO']
df_iq_geo_unmatch.groupby(['region','q_status']).count()








