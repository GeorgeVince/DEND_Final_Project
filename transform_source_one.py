import pandas as pd

def transform_source_one(df):
	df = df[['meter_name','TIMESTAMP','VALUE']]
	
	#SITE_ID, REGION_ID, METER_NAME, TIMESTAMP, CONSUMPTION
	

if __name__ == "__main__":
	file_name = "Pre-Processing/data_out/SOURCE_1/AAA069_AAA069_dba_kwh.csv"
	df = pd.read_csv(file_name)
	transform_source_one(df)