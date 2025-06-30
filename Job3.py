import sys
import re
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions

# Get parameters from Glue job arguments
args = getResolvedOptions(
    sys.argv,
    ['BUCKET_NAME', 'BASE_PREFIX', 'CRAWLER_NAME', 'AWS_REGION']
)

bucket_name = args['BUCKET_NAME']
base_prefix = args['BASE_PREFIX'].rstrip('/') + '/'  # Ensure it ends with /
crawler_name = args['CRAWLER_NAME']
region = args['AWS_REGION']

# Initialize boto3 clients
s3 = boto3.client('s3', region_name=region)
glue = boto3.client('glue', region_name=region)

# Function to get the latest timestamped folder
def get_latest_timestamp_folder(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    timestamp_folders = []

    pattern = re.compile(r'\d{8}_\d{6}/$')  # e.g., 20250620_154210/

    for page in pages:
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix']
            if pattern.search(folder):
                timestamp_folders.append(folder)

    if not timestamp_folders:
        raise Exception("❌ No timestamp folders found under prefix:", prefix)

    return max(timestamp_folders)

# Step 1: Find latest timestamp folder
latest_folder = get_latest_timestamp_folder(bucket_name, base_prefix)
latest_s3_path = f"s3://{bucket_name}/{latest_folder}"
print("📁 Latest folder for crawler:", latest_s3_path)

# Step 2: Update crawler source
response = glue.update_crawler(
    Name=crawler_name,
    Targets={
        'S3Targets': [
            {
                'Path': latest_s3_path
            }
        ]
    }
)
print("✅ Crawler updated to latest path.")

# Step 3: Start the crawler
glue.start_crawler(Name=crawler_name)
print("🚀 Crawler started.")
