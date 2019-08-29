# DEND - Capstone Project

## Description
This is my final capstone project for the Data Engineer Nanodegree provided by Udacity.
For more information please refer to the notebook.

## Files
- `PreProcessing Folder` - Used to take data from source, anonymize and stage in S3 (OUT OF PROJECT SCOPE)
- `sql_statements.py` - Contains the nessessary statements to create the database, as well as copy and stage data in Redshift.
- `transformations.py` - Contains the Pandas code required for transforming each of the data sources
- `Capstone Project - ETL Pipeline.ipynb` - Project description, write up and ETL Pipeline documentation

## Project Set Up

3 example source files are provided in `Pre-Processing\data_out` and a further example of zipped files are provided in `Pre-Processing\s1_examples.rar` and `Pre-Processing\s2_examples.rar`

The `requirements.txt` contains the packages required to run the project.


A description of the config file is below:
```
[AWS]
ACCESS_KEY=KEY_TO_AWS
SECRET_KEY=SECRET_KEY

[IAM]
ARN=AWS S3 ROLE WITH REDSHIFT / S3 ACCESS

[CLUSTER]
HOST=DB END POINT
DB_NAME= DATABASE NAME
DB_USER= DATABASE USER
DB_PASSWORD= DATABASE PASSWORD
DB_PORT= DATABASE PORT
```
