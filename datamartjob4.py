import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, row_number
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# ========== Glue Job Setup ==========
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_DATABASE',
    'OUTPUT_BASE_PATH'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ========== Config ==========
input_database = args['INPUT_DATABASE']
output_base_path = args['OUTPUT_BASE_PATH']
database_name = input_database  # You can change this if output DB is different

# ========== Helper: Read + Add SCD Columns ==========
def read_and_prepare(table, cols):
    return spark.read.table(f"{input_database}.{table}") \
        .selectExpr(*cols) \
        .dropDuplicates() \
        .withColumn("effective_date", current_timestamp()) \
        .withColumn("end_date", to_timestamp(lit("2300-01-01"))) \
        .withColumn("is_current", lit(True))

def write_dim(df, path, table_name):
    df.write.mode("overwrite").format("parquet").option("path", path).saveAsTable(f"{database_name}.{table_name}")

# ========== DIM TABLES ==========
write_dim(read_and_prepare("patients", [
    "id AS patient_id", "birthdate", "deathdate", "ssn", "drivers", "passport", "prefix",
    "first", "middle", "last", "suffix", "maiden", "marital", "race", "ethnicity", "gender",
    "birthplace", "address AS patient_address", "city AS patient_city", "state AS patient_state",
    "county AS patient_country", "fips", "zip AS patientaddress_zip", "lat AS patientaddress_latitude",
    "lon AS patientaddress_longitude"
]), f"{output_base_path}dim_patient/", "dim_patient")

write_dim(read_and_prepare("encounters", [
    "id AS encounters_id",
    "patient AS patient_name",
    "start AS encounters_start",
    "stop AS encounters_stop",
    "provider AS provider_id", 
    "code AS encounters_code",
    "description_part1 AS encounters_description1",
    "description_part2 AS encounters_description2"
]), f"{output_base_path}dim_encounters/", "dim_encounters")

write_dim(read_and_prepare("observations", [
    "observation_sk", "date", "category", "patient AS patient_name", "description_part1 AS observation_desc1",
    "description_part2 AS observation_desc2", "units", "value_part1 AS observation_value1", "value_part2 AS observation_value2"
]), f"{output_base_path}dim_observations/", "dim_observations")

write_dim(read_and_prepare("procedures", [
    "procedure_sk", "start AS procedure_start", "stop AS procedure_stop", "code AS procedure_code",
    "description_part1 AS procedure_description1", "description_part2 AS procedure_description2"
]), f"{output_base_path}dim_procedures/", "dim_procedures")

# ========== DIM_PROVIDERS ==========
providers_df = spark.read.table(f"{input_database}.providers")
orgs_df = spark.read.table(f"{input_database}.organizations")

dim_providers = providers_df.join(orgs_df, providers_df["organization"] == orgs_df["id"], "inner") \
    .select(
        col("providers.id").alias("provider_id"),
        col("providers.name").alias("provider_name"),
        col("organizations.id").alias("org_id")
    ) \
    .dropDuplicates() \
    .withColumn("effective_date", current_timestamp()) \
    .withColumn("end_date", to_timestamp(lit("2300-01-01"))) \
    .withColumn("is_current", lit(True))

write_dim(dim_providers, f"{output_base_path}dim_providers/", "dim_providers")

write_dim(read_and_prepare("organizations", [
    "id AS organization_id", "name AS organization_name", "address AS organizations_address",
    "city AS organization_city", "zip AS organization_zip", "lat AS organization_latitude",
    "lon AS organization_longitude", "phone_part1 AS org_phonenumber", "phone_part2 AS org_altnumber"
]), f"{output_base_path}dim_organizations/", "dim_organizations")

# ========== FACT TABLE ==========
obs_df = spark.read.table(f"{database_name}.dim_observations").filter("is_current = true").alias("obs")
patients_df = spark.read.table(f"{database_name}.dim_patient").filter("is_current = true").alias("p")
encounters_df = spark.read.table(f"{database_name}.dim_encounters").filter("is_current = true").alias("e")
providers_df = spark.read.table(f"{database_name}.dim_providers").filter("is_current = true").alias("pr")
orgs_df = spark.read.table(f"{database_name}.dim_organizations").filter("is_current = true").alias("o")

fact_df = obs_df \
    .join(patients_df, col("obs.patient_name") == col("p.patient_id")) \
    .join(encounters_df, col("e.patient_name") == col("obs.patient_name"), "left") \
    .join(providers_df, col("pr.provider_id") == col("e.provider_id"), "left") \
    .join(orgs_df, col("o.organization_id") == col("pr.org_id"), "left")

window_spec = Window.orderBy("obs.observation_sk")

fact_df = fact_df \
    .withColumn("fact_patient_id", row_number().over(window_spec)) \
    .withColumn("fact_effective_date", current_timestamp()) \
    .withColumn("fact_end_date", to_timestamp(lit("2300-01-01"))) \
    .withColumn("fact_is_current", lit(True)) \
    .select(
        col("fact_patient_id"),
        col("obs.observation_sk").alias("observation_id"),
        col("p.patient_id"),
        col("o.organization_id"),
        col("e.encounters_id").alias("encounter_id"),
        col("pr.provider_id"),
        col("fact_effective_date").alias("effective_date"),
        col("fact_end_date").alias("end_date"),
        col("fact_is_current").alias("is_current")
    )

fact_df.write.mode("overwrite").format("parquet") \
    .option("path", f"{output_base_path}fact_patient_e/") \
    .saveAsTable(f"{database_name}.fact_patient_e")

# ========== Commit ==========
job.commit()
