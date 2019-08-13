import boto3

#CREDIT FOR THIS METHOD: https://alexwlchan.net/2017/07/listing-s3-keys/
def get_matching_s3_keys(s3, bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

#https://stackoverflow.com/questions/1883980/find-the-nth-occurrence-of-substring-in-a-string
def find_nth(string, substring, n):
   if (n == 1):
       return string.find(substring)
   else:
       return string.find(substring, find_nth(string, substring, n - 1) + 1)

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
