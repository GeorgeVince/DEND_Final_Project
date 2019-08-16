import pandas as pd
import logging
import json

from utils import extract_names

FINAL_METER_NAMES = ['site_id','region','meter_name', 'date_time','consumption']

def transform_s1_df(df):
	"""Transforms SOURCE ONE files, into files ready for staging"""

	df = df[['meter_name','TIMESTAMP','VALUE']]
	
	#Create extracted name
	#SITE_ID, REGION_ID, METER_NAME, TIMESTAMP, CONSUMPTION
	extracted_name = pd.DataFrame(df['meter_name'].map(extract_names).to_list(), index=df.index)
	df_out = pd.merge(extracted_name, df[['TIMESTAMP','VALUE']], left_index=True, right_index=True)
	df_out.rename(columns={0:'site_id', 1:'region', 2:'meter_name'}, inplace=True)
	
	#Compute delta between meter reads
	#Sort values before computing delta
	df_out = df_out.sort_values('TIMESTAMP')
	df_out['consumption'] = df_out['VALUE'] - df['VALUE'].shift(1)
	#Remove first HH period (since it has no consumption)
	df_out = df_out[1:]

	#Convert TIMESTAMP to date
	df_out['date_time'] = pd.to_datetime(df_out['TIMESTAMP'], unit='ms')
	df_out = df_out[FINAL_METER_NAMES]

	return(df_out)

def transform_s2_df(df):
	"""Transforms SOURCE TWO files, into files ready for staging"""

	#Create extracted name
	#SITE_ID, REGION_ID, METER_NAME, TIMESTAMP, CONSUMPTION
	extracted_name = pd.DataFrame(df['meter_name'].map(extract_names).to_list(), index=df.index)
	df_out = pd.merge(extracted_name, df[['TIMESTAMP','VALUE']], left_index=True, right_index=True)
	df_out.rename(columns={0:'site_id', 1:'region', 2:'meter_name', 'VALUE':'consumption'}, inplace=True)

	#Convert TIMESTAMP to date
	df_out['date_time'] = pd.to_datetime(df_out['TIMESTAMP'], unit='ms')
	df_out = df_out[FINAL_METER_NAMES]

	return df_out

def transform_s3_df(df):
	"""Transforms SOURCE THREE files, into files ready for staging"""

	#Remove "unnamed" column
	df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
	#Melt DF
	df_out = pd.melt(df, id_vars=['Company Name', 'Date'], var_name='time', value_name='consumption')
	
	#Convert date and time columns to date_time
	df_out['date_time'] = pd.to_datetime(df_out['Date']+" "+df_out['time'],format='%d/%m/%Y %H:%M')

	df_out['site_id'] = df_out['Company Name']
	df_out['region'] = df_out['Company Name'].str[:3]
	df_out['meter_name'] = "Main_Meter"

	df_out = df_out[FINAL_METER_NAMES]
	return df_out

