import pymysql
import configparser
import pandas as pd
import json
import random

config = configparser.ConfigParser()
config.read("config.ini")

DATA_OUT="./data_out/"

def db_conn(db_name):
    """return pymysql connections object
    """
    return pymysql.connect(host=config['DB']['host'],
                     user=config['DB']['user'],
                     password=config['DB']['password'],
                     db=db_name,
                     charset='utf8mb4',
                     cursorclass=pymysql.cursors.DictCursor)

def process_sql_to_csv():
    conn = db_conn(config['DB']['db_one'])

    table_names = get_tablenames(conn, config['DB']['db_one'])
    table_names = [x for x in table_names if 'kwh' in x]

    site_mappings = create_random_mappings(table_names)


    #Loop through a subset of the table data
    #Only export a subset of the data
    random.shuffle(table_names)
    for table in table_names[:100]:
            export_to_csv(conn, table, site_mappings)
            
    conn.close()

def export_to_csv(conn, table, site_mappings):
    """Prepares an indiviudal meter table for export
    
    Args:
        conn (TYPE): pymysql connection object
        table (TYPE): individual meter table
        site_mappings (TYPE): key:val mapping of old to new site ID
    """
    print ("Reading... {}".format(table))
    sql = "SELECT * from {}".format(table)

    df = pd.read_sql(sql, conn).set_index('ID')

    #Create a new name based on the sitemapping dict
    new_name = table.replace(table[0:6], site_mappings[table[0:6]])
    
    df['meter_name'] = new_name
    df = df.set_index('meter_name')

    file_name = DATA_OUT + "SOURCE_1/" + new_name + ".csv"
    
    df.to_csv(file_name)

def process_sql_to_json():
    """Get table names, loop through them and process each site.
    """
    conn = db_conn(config['DB']['db_two'])

    table_names = get_tablenames(conn, config['DB']['db_two'])
    table_names = [x for x in table_names if 'kwh' in x]
    site_mappings = create_random_mappings(table_names)


    #Loop through a subset of the table data
    #Only export a subset of the data

    #Shuffle table names and only get a subset of these
    random.shuffle(table_names)
    for table in table_names:
        export_to_json(conn, table, site_mappings)

    conn.close()


def export_to_json(conn, table, site_mappings):
    """Exports a table to JSON
    
    Args:
        conn (TYPE): pymysql connection object
        table (TYPE): individual meter table
        site_mappings (TYPE): key:val mapping of old to new site ID
    """
    print ("Reading... {}".format(table))
    sql = "SELECT * from {}".format(table)

    df = pd.read_sql(sql, conn).set_index('ID')

    #Create a new name based on the sitemapping dict
    new_name = table.replace(table[0:6], site_mappings[table[0:6]])
    
    df['meter_name'] = new_name
    df = df.set_index('meter_name')

    data = json.loads(df.to_json(orient='table'))
    file_name = DATA_OUT + "SOURCE_2/" + new_name + ".json"

    with open(file_name, 'w') as json_file:
        json.dump(data, json_file)

def get_tablenames(conn, table_to_search):
        """Summary
        
        Args:
            conn (TYPE): pymysql connection object
            table_to_search (TYPE): table to obtain table names from
        
        Returns:
            TYPE: All table names from specified table
        """
        sql = "SELECT table_name FROM information_schema.tables where table_schema='{}'".format(table_to_search)
        data = execute_sql(sql, conn)
        table_names = [item['table_name'] for item in data]
        
        return table_names

def execute_sql(sql, conn):
    """Execute SQL and return data"""
    try:
    
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            cursor.close()
        return data
    except Exception as e:
        print(e)

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
    
    process_sql_to_csv()
    process_sql_to_json()


