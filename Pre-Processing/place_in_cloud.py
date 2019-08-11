import boto3
import os
import configparser
from botocore.exceptions import NoCredentialsError
config = configparser.ConfigParser()
config.read("config.ini")

ACCESS_KEY = config['AWS']['ACCESS_KEY']
SECRET_KEY = config['AWS']['SECRET_KEY']


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

source_folders = ['SOURCE_1', 'SOURCE_2', 'SOURCE_3']

bucket_name = "dend-capstone-sample"
for folder in source_folders:
	print("Uploading files from... {}".format(folder))
	for file in os.listdir("data_out/" + folder):
		print("Uploading {} from {}".format(folder, file))
		local_file_path = "data_out/" + folder + "/" + file
		s3_file_path = folder + "/" + file
		upload_to_aws(local_file_path, bucket_name, s3_file_path)
