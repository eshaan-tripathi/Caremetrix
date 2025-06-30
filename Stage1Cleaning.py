import sys
import boto3
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

from pyspark.context import SparkContext
from pyspark.sql.functions import col, regexp_replace
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Get parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_S3_PATH', 'TARGET_S3_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_s3_path = args['SOURCE_S3_PATH']
target_s3_path = args['TARGET_S3_PATH']

# Generate IST timestamped folder
IST = timezone(timedelta(hours=5, minutes=30))
timestamp_str = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
timestamped_target_path = f"{target_s3_path.rstrip('/')}/{timestamp_str}"

def parse_s3_path(s3_path):
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip('/')

bucket, prefix = parse_s3_path(source_s3_path)
s3 = boto3.client('s3')

# Get all CSV file keys
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

file_keys = [obj['Key'] for page in pages for obj in page.get('Contents', []) if obj['Key'].endswith('.csv')]

print(f"Found {len(file_keys)} CSV files.")

for file_key in file_keys:
    input_file_s3 = f"s3://{bucket}/{file_key}"
    print(f"Processing file: {input_file_s3}")

    try:
        # Read CSV directly as DataFrame
        df = spark.read.option("header", "true").option("mode", "PERMISSIVE").csv(input_file_s3)

        # Remove commas from string fields
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, regexp_replace(col(col_name), ",", ""))

        file_base_name = file_key.split('/')[-1].rsplit('.', 1)[0]
        output_path = f"{timestamped_target_path}/{file_base_name}"

        # Write to temp location
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"✅ Written to temporary folder: {output_path}")

        # Rename part file to desired file name
        output_bucket, output_prefix = parse_s3_path(output_path)
        response = s3.list_objects_v2(Bucket=output_bucket, Prefix=output_prefix + "/")

        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') and 'part-' in key:
                new_key = f"{output_prefix}/{file_base_name}.csv"
                s3.copy_object(
                    Bucket=output_bucket,
                    CopySource={'Bucket': output_bucket, 'Key': key},
                    Key=new_key
                )
                s3.delete_object(Bucket=output_bucket, Key=key)
                print(f"✅ Renamed {key} to {new_key}")
                break

    except Exception as e:
        print(f"❌ Failed to process {input_file_s3}: {e}")

print("✅ All files processed.")
job.commit()
