#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 15:40:27 2023
The purpose of this code is to clean up some of the data and to save the output to csv files
This will allow the maps to run faster and reduce some of the obvious errors in the underlying data
https://emojipedia.org/fire/
@author: kristian.lande@leveltenenergy.com
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas.tools import overlay

LOC_PATH = '/Users/kristian.lande@leveltenenergy.com/Documents/GitHub/streamlit-geospatial/data/'
IQ = 'queues_2021_clean_data.xlsx'
PJM_COST = 'pjm_costs_2022_clean_data.xlsx'
MISO_COST = 'miso_costs_2021_clean_data.xlsx'
NYISO_COST = 'nyiso_2022_final_data_cleaned_publication_vfinal.xlsx'

#   unrealistic dates
############################################
dt_cols = ['Queue Date', # date project entered queue
           'Study Date'] # date of most recently available interconnection study
df_cost = pd.read_excel(LOC_PATH+NYISO_COST,sheet_name='data',parse_dates=dt_cols)
df_cost.columns= df_cost.columns.str.lower()
df_cost.columns = df_cost.columns.str.replace(' ','_')
df_cost.rename({'queue_id':'q_id'},inplace=True,axis=1)
df_cost.rename({'queue_id_2':'q_id_2'},inplace=True,axis=1)

df_cost['q_id']= df_cost['q_id'].astype(str)
df_cost['q_id_2']= df_cost['q_id_2'].astype(str)
df_cost['q_year'] = df_cost['queue_date'].dt.year
df_cost['study_year'] = df_cost['study_date'].dt.year

df_cost.info()
# df_cost['cod_year'] = df_cost['done_date'].dt.year
# df_cost['wd_year'] = df_cost['withdrawn_date'].dt.year
# df_cost['diff_months_cod']=(df_cost['done_date']-df_cost['queue_date'])/np.timedelta64(1,'M')
df_cost.info()

df_cost['state'].unique() # 3 states in NYISO data
df_cost['resource_type'].unique()
df_cost.q_id
#################
# Need to extract the counties
df_nyiso_iq = df_iq[df_iq['region']=='NYISO']
df_nyiso_iq.info()
df_nyiso_iq.q_id
df_nyiso_iq['q_id'] = df_nyiso_iq.q_id.str.lstrip("0")



df_nyiso_iq.info() #1,123; 1,036 with county, 1,123 with state, 68 with online date
df_nyiso_iq.developer.unique()
df_nyiso_iq.q_id

df_nyiso_iq.info()

df_nyiso_iq_county = df_nyiso_iq[['q_id','county_1','state','developer','wd_date','diff_months_wd','cod','diff_months_cod','cod_year','ix_voltage']]
df_nyiso_iq_county.info()
df_cost_geo = pd.merge(df_cost,df_nyiso_iq_county,how='left',left_on=['q_id'],right_on=['q_id'])
#########
df_cost_geo.info()

# df_cost_nyiso_geo = df_cost_geo.to_csv('df_cost_nyiso_geo.csv')



df_cost_geo_f = pd.merge(df_cost_geo,df_fips,how='left',left_on='state_x',right_on='Abbr.')
df_cost_geo_f.info()
df_cost_geo_f.county
# df_cost_geo.to_csv('df_miso_cost_county.csv') # fixed 28 missing counties
# df_cost_geo_clean = pd.read_csv('df_miso_cost_county.csv')
df_cost_geo_f['county'] = df_cost_geo_f['county'].str.lower()

geo_c.NAME

df_nyiso_cost_geo = df_cost_geo_f.merge(geo_c,how='left',right_on=['NAME','STATEFP'],left_on=['county','FIPS'],indicator=True).reset_index()
df_nyiso_cost_geo.info() # all records joined

test = df_nyiso_cost_geo[df_nyiso_cost_geo['_merge']=='left_only']


test[test.loc[ :,['county_1','NAME'] ].isnull().sum(axis=1) == 1]
test.county
df_cost_geo_cln[df_cost_geo_cln.loc[ :,['NAME','county_1'] ].isnull().sum(axis=1) == 1]

df_iq_geo.drop(labels='_merge',axis=1,inplace=True)

ny_geo = geo_c[geo_c['STATEFP']=='36']
ny_geo.info()

df_nyiso_cost_geo.FIPS.unique()
nj_geo = geo_c[geo_c['STATEFP']=='34']
nj_geo.info()


ny_geo[ny_geo['NAME'].str.startswith("c")]
df_nyiso_cost_geo.drop(['Abbr.','FIPS','NAME','STATEFP','_merge'],inplace=True,axis=1)
df_nyiso_cost_geo.info()
gdf_nyiso_cost_geo = gpd.GeoDataFrame(df_nyiso_cost_geo,geometry='geometry',crs='epsg:4326')
gdf_nyiso_cost_geo.info()
gdf_nyiso_cost_geo.drop(['index'],inplace=True,axis=1)


gdf_nyiso_cost_geo.to_file('gdp_cost_nyiso_qeo.geojson',driver="GeoJSON")

# merge fips # 921 clean records
# df_cost_geo_clean_fip = pd.merge(df_cost_geo_clean,df_fips,how='left',left_on='state',right_on='Abbr.')
# df_cost_geo_clean_fip.to_csv('df_miso_cost_county.csv')

gdf_nyiso_cost_geo['$2022_network_cost/kw'] = gdf_nyiso_cost_geo['$2022_network_cost/kw'].astype(int)
gdf_nyiso_cost_geo.info()
len(gdf_nyiso_cost_geo[gdf_nyiso_cost_geo['$2022_network_cost/kw']>0])

gdf_nyiso_cost_geo['$2022_network_cost/kw'].unique()
274/294

len(df_cost_geo_clean[df_cost_geo_clean['real_poi/kw'].notna()])

df_cost_geo_clean.dropna(subset=['FIPS'],inplace=True)
df_cost_geo_clean['FIPS']=df_cost_geo_clean['FIPS'].astype(int)
df_cost_geo_clean['FIPS']=df_cost_geo_clean['FIPS'].astype(str)
df_cost_geo_clean[(df_cost_geo_clean['county_1'].str.startswith('st.'))]

df_cost_geo_clean['FIPS'].unique()
geo_c['STATEFP']=geo_c['STATEFP'].astype(int)
geo_c['STATEFP']=geo_c['STATEFP'].astype(str)
geo_c['STATEFP'].unique()


geo_c.info()

df_cost_geo_cln = df_cost_geo_clean_fip.merge(geo_c,how='left',right_on=['NAME','STATEFP'],left_on=['county_1','FIPS'],indicator=True).reset_index()
df_cost_geo_cln.info() #missing 25 records

25/923
test = df_cost_geo_cln[['county_1','NAME','FIPS']]
test[test.loc[ :,['county_1','NAME'] ].isnull().sum(axis=1) == 1]
df_cost_geo_cln[df_cost_geo_cln.loc[ :,['NAME','county_1'] ].isnull().sum(axis=1) == 1]

df_cost_geo_cln.drop(labels=['_merge','county_1','Abbr.','FIPS','STATEFP'],axis=1,inplace=True)
df_cost_geo_cln.dropna(subset='geometry',inplace=True)
df_cost_geo_cln.info()

gdf_cost_geo_cln = gpd.GeoDataFrame(df_cost_geo_cln,geometry='geometry',crs='epsg:4326')
gdf_cost_geo_cln.info()

gdf_cost_geo_cln.to_file('gdp_cost_miso_qeo.geojson',driver="GeoJSON")

geo_c[(geo_c.NAME.str.startswith('c'))&(geo_c.STATEFP.isin(['55']))]




# df_cost_geo_cln.drop(labels=['_merge','STATEFP'],axis=1,inplace=True)



df_pjm_cost_geo_merge = df_pjm_cost_geo.merge(geo_c,how='left',right_on=['NAME','STATEFP'],left_on=['county_1','FIPS'],indicator=True).reset_index()
df_pjm_cost_geo_merge.info()

gdf_pjm_map = gpd.GeoDataFrame(df_pjm_cost_geo_merge,geometry='geometry',crs='epsg:4326')
gdf_pjm_map.drop(['index'],axis=1,inplace=True)


gdf_pjm_map = gdf_pjm_map[gdf_pjm_map['geometry'].notna()]
gdf_pjm_map.info()

gdf_pjm_map_clean = gpd.GeoDataFrame(gdf_pjm_map,geometry='geometry',crs='epsg:4326')
gdf_pjm_map_clean.info()

gdf_pjm_map_clean = gdf_pjm_map_clean[['q_year','request_status','fuel','NAME','state','$2022_poi_cost/kw','$2022_network_cost/kw','$2022_total_cost/kw','nameplate_mw']]
gdf_pjm_map_clean.info()
gdf_pjm_map_agg = gdf_pjm_map.dissolve(by=['q_year','request_status','fuel','NAME','state'], aggfunc = {'$2022_poi_cost/kw':'mean','$2022_network_cost/kw':'mean','$2022_total_cost/kw':'mean','nameplate_mw':'sum'},as_index=False) 

gdf_pjm_map_agg['lat'] = gdf_pjm_map_agg.centroid.y
gdf_pjm_map_agg['lon']  = gdf_pjm_map_agg.centroid.x
# gdf_pjm_map_agg.drop('geometry',axis=1,inplace=True)
gdf_pjm_map_agg.info()
gdf_pjm_map_agg.drop('geometry',axis=1,inplace=True)




test_county = df_pjm_cost_geo_merge[df_pjm_cost_geo_merge.loc[ :,['nameplate_mw','geometry'] ].isnull().sum(axis=1) == 1]
test_county
############################################
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
# df_trend = df_trend[(df_trend['q_year']>=2000)&(df_trend['q_year']<=2016)&(df_trend['cod_year']<=2021)]
# df_trend.groupby(['q_status']).count()
# x = 464+2682+35+6709
# 2682/x # ties out
df_trend.info()
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
df_fips = pd.read_csv(LOC_PATH+'fips.csv',dtype=str)
df_fips = df_fips[['Abbr.','FIPS']]
df_fips[df_fips['Abbr.']=='DC']


df_fips.info()
df_fips['Abbr.'].nunique()
df_iq = pd.merge(df_iq,df_fips,how='left',left_on='state',right_on='Abbr.')
df_iq.info()
df_iq.drop(labels=['Abbr.'],axis=1, inplace=True)

# 24,380 records, 24,154 with state, 23,969 with county, 94 missing both state and county
df_iq[df_iq.loc[ :,['state','county_1'] ].isnull().sum(axis=1) == 2]

# MATCH GEOLOC
geo_county=gpd.read_file(LOC_PATH+'us_counties.geojson')
geo_county['NAME'] = geo_county['NAME'].str.lower()
geo_c = geo_county[['NAME','STATEFP','geometry']]
geo_c.STATEFP.nunique()


geo_c[geo_c.NAME.str.startswith('aroo')]
geo_c[geo_c['STATEFP']==11]

geo_state=gpd.read_file(LOC_PATH+'us_states.geojson')
geo_state[geo_state['NAME']=='District of Columbia']
geo_state.to_csv(LOC_PATH+'geo_state.csv')


# merge the geo to iq
# 19,653/23,969 or 18% did not join
df_iq_geo = df_iq.merge(geo_c,how='left',right_on=['NAME','STATEFP'],left_on=['county_1','FIPS'],indicator=True).reset_index()
df_iq_geo.drop(labels='_merge',axis=1,inplace=True)
df_iq_geo.info()

test_state = df_iq_geo[df_iq_geo.loc[ :,['state','county_1','geometry'] ].isnull().sum(axis=1) == 2]
test_state = test_state[test_state['q_status']=='operational']

gdp_iq_qeo = gpd.GeoDataFrame(df_iq_geo,geometry='geometry',crs='EPSG:4326')
gdp_iq_qeo.columns
geo_cols = ['q_id','q_status','q_year','cod_year','project_name','type_clean','utility','NAME','state','region','mw1','diff_months_cod','ix_voltage','geometry']
gdp_iq_qeo_short = gpd.GeoDataFrame(df_iq_geo,geometry='geometry',crs='EPSG:4326',columns=geo_cols)
gdp_iq_qeo_short.set_index('q_id',inplace=True)
gdp_iq_qeo_short.dropna(subset=['geometry'],inplace=True)
gdp_iq_qeo_short.info()

gdp_iq_qeo_short.to_file('gdp_iq_qeo.geojson',driver="GeoJSON")

## MAPPING FILE FOR QUEUE - cleaned active MISO, PJM and NYISO
# df_iq_geo.to_csv('df_iq_geo.csv')
# active 6629/8151 or 18% did not join
# manually mapped around 200 in the maerkets for which we have cost data 3970/4100 so 96.8% active mapped in those markets
# df_iq_geo[(df_iq_geo['q_status']=='active')&(df_iq_geo['region'].isin(['PJM','MISO','NYISO']))].info()
df_iq_geo[(df_iq_geo['q_status']=='operational')&(df_iq_geo['region'])].info()

df_iq_geo_unmatch = df_iq_geo[['q_id','q_year','state','county_1','region','type_clean','q_status','geometry']]
df_iq_geo_unmatch = df_iq_geo_unmatch[(df_iq_geo_unmatch['q_year']>2016)&(df_iq_geo_unmatch['geometry'].isnull())&(df_iq_geo_unmatch['q_status']=='operational')]
df_iq_geo_unmatch[df_iq_geo_unmatch['region']=='MISO']
df_iq_geo_unmatch.groupby(['region','q_status']).count()


gdf_ISO = gpd.read_file(LOC_PATH+'Independent_System_Operators.shp')
gdf_ISO.NAME
gdf_ISO.NAME[3]

gdf_NYISO = gdf_ISO[gdf_ISO['NAME']=='NEW YORK INDEPENDENT SYSTEM OPERATOR']
gdf_NYISO.info()

gdf_NYISO.to_file('nyiso.geojson',driver="GeoJSON")

gdf_MISO = gpd.read_file('miso.geojson',driver="GeoJSON")
gdf_MISO.info()
gdf_MISO.iloc[0]
gdf_MISO.crs

DSK_LOC = '/Users/kristian.lande@leveltenenergy.com/Desktop/'
trany = gpd.read_file(DSK_LOC+'U.S._Electric_Power_Transmission_PJM.geojson',driver="GeoJSON")
trany.iloc[0]
trany.crs



trany= trany.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,41]]
trany.info()

miso_join = gpd.sjoin(gdf_MISO,trany,op="intersects")
miso_join.info()

miso_join.to_file('miso_transmission_short.geojson',driver="GeoJSON")
miso_join.plot()



