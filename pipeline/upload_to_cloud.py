import boto3
from botocore.client import Config

# Connect to our LOCAL MINIO Docker container instead of real AWS
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',     # <--- MAGIC: Pointing to Local MinIO natively
    aws_access_key_id='YOUR_KEY',
    aws_secret_access_key='YOUR_SECRET',
    config=Config(signature_version='s3v4'),
    region_name='ap-south-1'    # Mumbai
)

# First, create the bucket (since the UI hasn't been used)
bucket_name = 'emergency-response-data-yourname'
try:
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
except Exception as e:
    pass

# Upload raw data
s3.upload_file(
    'data/processed/transformed.parquet',
    bucket_name,
    'processed/incidents_clean.parquet'
)
print("Uploaded to S3 (Local MinIO)!")

# Download it back
s3.download_file(
    bucket_name,
    'processed/incidents_clean.parquet',
    'data/downloaded_from_s3.parquet'
)
print("Downloaded back from S3 (Local MinIO) successfully!")
