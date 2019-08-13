import pandas as pd
import logging

from utils import find_nth

def transform_source_one(df):
	df = df[['meter_name','TIMESTAMP','VALUE']]
	
	#SITE_ID, REGION_ID, METER_NAME, TIMESTAMP, CONSUMPTION

	extracted_name = pd.DataFrame(df['meter_name'].map(extract_names).to_list(), index=df.index)
	df_out = pd.merge(extracted_name, df[['TIMESTAMP','VALUE']], left_index=True, right_index=True)
	
	df_out.rename(columns={0:'site_id', 1:'region', 2:'meter_name'}, inplace=True)
	df_out['consumption'] = df['VALUE'] - df['VALUE'].shift(1)
	df_out['date'] = pd.to_datetime(df['TIMESTAMP'], unit='ms')
	df_out = df_out[['site_id','region','meter_name', 'date', 'VALUE','consumption']]

	print(df_out)

def extract_names(meter_in):
	site_id = ""
	region_id = ""
	formatted_meter = ""

	try:
		site_id = meter_in[0:6]
	except IndexError:
		logging.info("Could not parse site_id from metername: {}".format(meter_in))
	try:
		region_id = meter_in[0:3]
	except IndexError:
		logging.info("Could not parse region_id from metername: {}".format(meter_in))
	try:
		formatted_meter = meter_in[(find_nth(meter_in,"_",2)+1):-4]
	except IndexError:
		logging.info("Could not find formatted meter from metername: {}".format(meter_in))

	return (site_id, region_id, formatted_meter)

if __name__ == "__main__":

	file_name = "Pre-Processing/data_out/SOURCE_1/AAA069_AAA069_dba_kwh.csv"
	df = pd.read_csv(file_name)
	transform_source_one(df)