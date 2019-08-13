import pandas as pd
import logging
import json

from utils import extract_names

def transform_s1_df(df):
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
	df_out['date'] = pd.to_datetime(df_out['TIMESTAMP'], unit='ms')
	df_out = df_out[['site_id','region','meter_name', 'date', 'VALUE','consumption']]

	return(df_out)

def transform_s2_df(df):
	#Create extracted name
	#SITE_ID, REGION_ID, METER_NAME, TIMESTAMP, CONSUMPTION
	extracted_name = pd.DataFrame(df['meter_name'].map(extract_names).to_list(), index=df.index)
	df_out = pd.merge(extracted_name, df[['TIMESTAMP','VALUE']], left_index=True, right_index=True)
	df_out.rename(columns={0:'site_id', 1:'region', 2:'meter_name', 'VALUE':'consumption'}, inplace=True)

	#Convert TIMESTAMP to date
	df_out['date'] = pd.to_datetime(df_out['TIMESTAMP'], unit='ms')
	df_out = df_out[['site_id','region','meter_name', 'date', 'consumption']]

	return df_out
	
if __name__ == "__main__":
	source_file = "Pre-Processing/data_out/SOURCE_2/AAA013_AAA013_dba_kwh.json"

	data = json.load(open(source_file))
	df = pd.DataFrame(data["data"])
	transform_s2_df(df)
