import pandas as pd
import random
import json

DATA_OUT="./data_out/"


def reformat_csv_v2(file_name):
    df = pd.read_csv(file_name)
    mappings = create_random_mappings(df['Company Name'])
    df['Company Name'] = df['Company Name'].apply(lambda x: mappings[x])
    df.to_csv('data_out/SOURCE_3/2019_05.csv')

## No long used
def reformat_csv_v1(file_name):
    """Given a source file, anonymise it and convert to JSON.
    
    Args:
        sql (conn): pymsql connection object
    """  
    
    print("reformating file...".format(file_name))

    df = pd.read_csv(file_name)
    df_melted = pd.melt(df, id_vars=['Site Name', 'Date'], var_name='time', value_name='consumption')

    mappings = create_random_mappings(df['Site Name'])

    #Anonymise the data
    df_melted['Site Name'] = df_melted['Site Name'].apply(lambda x: mappings[x])

    df = df_melted[['Site Name', 'Date', 'time','consumption']]

    df = df.sort_values(['Site Name'])

    #Parition these out into region files
    unique_regions = sorted(set([region[0:3] for region in df['Site Name']]))

    for region in unique_regions:
        region_partition = df[df['Site Name'].str[0:3] == region]
        data = json.loads(region_partition.to_json(orient='table'))

        json_file_name = DATA_OUT + "SOURCE_3/" + file_name[8:-4] + "_" + region + ".json" 

        with open(json_file_name, 'w') as json_file:
            json.dump(data, json_file)


def create_random_mappings(all_meters):
    """Given a list of table names, this function will return
    an anonymised mapping of store_id:new_id
    
    Args:
        sql (conn): pymsql connection object
    """    
    #Unique site_ids, without MID region.
    unique_site_id = set([meter[0:6] for meter in all_meters if meter[0:6] not in ['mid']])
    
    anonymised_store_ids = dict([swap_id(site) for site in unique_site_id])

    return(anonymised_store_ids)

def swap_id(store_id):
    anonymised_site_ids = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    return store_id, store_id.replace(store_id[0:3], random.choice(anonymised_site_ids))




if __name__ == "__main__":
    
    #reformat_csv_v1('data_in/2018_05_mm.csv')
    reformat_csv_v2('data_in/2019_07_mm.csv')