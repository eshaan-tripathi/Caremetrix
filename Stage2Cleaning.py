import sys
import logging
import boto3
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp,
    split, to_timestamp, monotonically_increasing_id, date_format, expr, coalesce
)
from pyspark.sql.types import TimestampType
import re
from functools import reduce

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get input arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'DEST_PATH', 'AWS_REGION'])
source_root_path = args['SOURCE_PATH']
dest_path = args['DEST_PATH']
region = args.get('AWS_REGION', 'us-east-1')

# Generate IST timestamp folder
ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
timestamp_str = ist_time.strftime("%Y%m%d_%H%M%S")
timestamped_dest_path = f"{dest_path.rstrip('/')}/{timestamp_str}"  # ✅ Fixed: Timestamped destination

# Setup Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize S3 client
s3 = boto3.client('s3', region_name=region)

# Helper: Get latest timestamped folder
def get_latest_timestamp_folder(s3_path):
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    timestamp_folders = []
    pattern = re.compile(r'^\d{8}_\d{6}/$')
    for page in pages:
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].split('/')[-2] + '/' if cp['Prefix'].endswith('/') else cp['Prefix']
            if pattern.match(folder):
                timestamp_folders.append(folder)
    if not timestamp_folders:
        raise Exception("❌ No timestamp folders found.")
    latest = sorted(timestamp_folders)[-1]
    logger.info(f"📁 Latest timestamp folder: {latest}")
    return f"s3://{bucket}/{prefix.rstrip('/')}/{latest.rstrip('/')}/"

# Helper: List CSV Files
def list_all_csv_files(bucket, prefix):
    csv_keys, token = [], None
    while True:
        if token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                csv_keys.append(obj["Key"])
        if response.get("IsTruncated"):
            token = response["NextContinuationToken"]
        else:
            break
    return csv_keys

# Helper: Add Surrogate Key
def add_surrogate_key(df, key_name):
    df = df.withColumn(key_name, monotonically_increasing_id())
    cols = [key_name] + [c for c in df.columns if c != key_name]
    return df.select(*cols)

# Begin processing
source_path = get_latest_timestamp_folder(source_root_path)
bucket, prefix = source_path.replace("s3://", "").split("/", 1)
csv_files = list_all_csv_files(bucket, prefix)
logger.info(f"📄 CSV files to process: {csv_files}")

for key in csv_files:
    table_name = key.split("/")[-1].replace(".csv", "").lower()
    input_s3_uri = f"s3://{bucket}/{key}"
    logger.info(f"🔄 Processing: {input_s3_uri}")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_s3_uri)
    df = df.toDF(*[c.lower() for c in df.columns])

    # Cleaning
    for column in df.columns:
        dtype = dict(df.dtypes)[column]
        if dtype == 'string':
            df = df.withColumn(column, when(col(column).isNull(), "null").otherwise(trim(col(column))))
            if df.filter(col(column).rlike(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")).limit(1).count() > 0:
                df = df.withColumn(column, to_timestamp(col(column), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            elif df.filter(col(column).rlike(r"^\d{4}-\d{2}-\d{2}$")).limit(1).count() > 0:
                df = df.withColumn(column, col(column).cast("timestamp"))
            if df.filter(col(column).rlike(r"\s+[oOóÓ][rR]\s+")).limit(1).count() > 0:
                df = df.withColumn(f"{column}_part1", trim(split(col(column), r"\s+[oOóÓ][rR]\s+").getItem(0)))
                df = df.withColumn(f"{column}_part2", trim(split(col(column), r"\s+[oOóÓ][rR]\s+").getItem(1)))
                df = df.drop(column)
        elif dtype.startswith("timestamp") or dtype.startswith("date"):
            df = df.withColumn(column, col(column).cast("timestamp"))
        elif dtype in ('int', 'bigint', 'double', 'float', 'long', 'decimal'):
            df = df.withColumn(column, when(col(column).isNull(), lit(None)).otherwise(col(column).cast(dtype)))

    # Add surrogate key if needed
    if table_name == 'observations':
        df = add_surrogate_key(df, 'observation_sk')
    elif table_name == 'procedures':
        df = add_surrogate_key(df, 'procedure_sk')

    # Add Metadata Columns
    df = df.withColumn("source_file", lit(input_s3_uri))
    df = df.withColumn("ingestion_time", current_timestamp())
    df = df.withColumn("effective_date", current_timestamp())
    df = df.withColumn("end_date", expr("timestamp('2300-01-01')"))
    df = df.withColumn("is_current", lit(True))

    business_key = f"{table_name}_id"
    output_path = f"{timestamped_dest_path}/{table_name}"  # ✅ Fixed: Write to timestamped folder

    try:
        historical_df = spark.read.parquet(output_path)
        current_df = historical_df.filter(col("is_current") == True)

        join_cond = [df[business_key] == current_df[business_key]]
        diff_cond = [coalesce(df[c], lit("")).cast("string") != coalesce(current_df[c], lit("")).cast("string")
                     for c in df.columns if c in current_df.columns and c not in ['effective_date', 'end_date', 'is_current', 'ingestion_time']]

        changed_df = df.alias("new").join(current_df.alias("old"), join_cond, "left") \
            .filter(reduce(lambda a, b: a | b, diff_cond)) \
            .select("new.*")

        expired_df = current_df.join(changed_df.select(business_key), business_key, "inner") \
            .withColumn("is_current", lit(False)) \
            .withColumn("end_date", current_timestamp().cast(TimestampType()))

        unchanged_df = current_df.join(changed_df.select(business_key), business_key, "left_anti")
        final_df = unchanged_df.unionByName(expired_df).unionByName(changed_df)
        final_df.select("ingestion_time", "effective_date", "end_date").show(truncate=False)


    except Exception as e:
        logger.warning(f"No historical data found or error occurred: {e}")
        final_df = df

    final_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"✅ SCD-2 written to: {output_path}")

job.commit()
logger.info("✅ Glue SCD-2 job completed successfully.")
