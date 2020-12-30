import logging
import boto3
from botocore.exceptions import ClientError


def create_bucket(bucket_name, region="us-west-2"):
	"""Create an S3 bucket in a specified region

	:param bucket_name: Bucket to create
	:param region: String region to create bucket in, e.g., 'us-west-2'
	:return: True if bucket created, else False
	"""
	# Create bucket
	try:
		# get list of existing buckets
		s3_client = boto3.client('s3', region_name=region)
		list_buckets = s3_client.list_buckets()
		for bucket in list_buckets['Buckets']:
			if bucket["Name"] == bucket_name:
				print("------- Bucket already exists")
				return s3_client
		location = {'LocationConstraint': region}
		s3_client.create_bucket(Bucket=bucket_name,
                          CreateBucketConfiguration=location)
		return s3_client
	except ClientError as e:
		logging.error(e)
		return


if __name__ == "__main__":
	bucket_name = "sparkify-dl-sp"
	region = "us-west-2"
	etl_script = "etl.py" # etl script to copy to s3 and that will be used to submit spark job at cluster creation

	s3_client = create_bucket(bucket_name, region)
	if s3_client:
		print("-"*80,
				f"\tS3 Bucket `{bucket_name}` created in region `{region}`", "-"*80, sep="\n")
		print("-"*80,
		f"\tUploading pyspark ETL script to s3", "-"*80, sep="\n")
		try:
			response = s3_client.upload_file(etl_script, bucket_name, "sparkify-scripts/spark-etl.py")
		except ClientError as e:
			logging.error(e)
	else:
		print("-"*80,
				f"\tUnable to create S3 Bucket `{bucket_name}` in region `{region}`", "-"*80, sep="\n")
