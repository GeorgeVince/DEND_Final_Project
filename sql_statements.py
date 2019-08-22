s3_copy = ("""
    copy staging_collated_energy 
    from {}
    credentials 'aws_iam_role={}'
    TIMEFORMAT 'auto'
    compupdate off region 'us-west-1'
    IGNOREHEADER 1
    CSV;
    """)

create_sql_table = ("""
				CREATE TABLE IF NOT EXISTS {} (
                energy_id    INT     IDENTITY(0,1)  NOT NULL,
                store_id     varchar(10)            NOT NULL,
                region       varchar(10)            NOT NULL,
                meter_name   varchar(20)            NOT NULL,
                date_time    TIMESTAMP              NOT NULL,
                consumption  numeric(18,5)          NOT NULL
            )""")

create_time_table = ("""
                CREATE TABLE IF NOT EXISTS dim_time (
                start_time      timestamp NOT NULL,
                "hour"          int4,
                "minute"        int4,
                "day"           int4,
                week            int4,
                "month"         varchar(256),
                "year"          int4,
                weekday         varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            );
    		""")

#Take data from the staging table and place it into the collated energy table
#Only take valid regions
#Only take valid store ids
stage_energy_sql =("""
            	INSERT INTO collated_energy(store_id, region, meter_name, date_time, consumption)
	            (
	            SELECT  es.store_id, 
	                    es.region, 
	                    es.meter_name, 
	                    date_trunc('minute', date_time::TIMESTAMP) as date_time, 
	                    es.consumption
	                        
	            FROM staging_collated_energy as es
	            
	            WHERE 
	                es.region in ('AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF')
	            AND
	                es.store_id ~ '^[a-zA-Z]{3}([0-9]{3}|[R][D][C])$'
	            )
            """)


stage_time_sql = ("""
	            INSERT INTO dim_time(start_time, hour, minute, day, week, month, year, weekday)
	            (
	            SELECT  distinct(date_time) as date_time, 
	                    extract(hour from date_time) as hour,
	                    extract(minute from date_time) as minute,
	                    extract(day from date_time) as day, 
	                    extract(week from date_time) as week, 
	                    extract(month from date_time) as month, 
	                    extract(year from date_time) as year, 
	                    extract(dayofweek from date_time) as weekday
	            FROM collated_energy
	            )
        	""")