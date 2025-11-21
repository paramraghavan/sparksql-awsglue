# Architecture Overview

**Vehicle Emissions Testing System:**

- **Source**: Vehicle test stations submit test results (new tests + corrections to previous tests)
- **Frequency**: Hourly incremental loads
- **Pattern**: Upsert-based (merge) to handle corrections/updates
- **Storage**: S3 (Iceberg format)
- **Catalog**: Snowflake manages Iceberg metadata
- **Query**: Business users via Snowflake SQL

---

## 1. Table Schema Design

```python
# emissions_tests (Bronze/Raw Layer)
"""
test_id: string (PK)
vehicle_id: string
test_date: timestamp
station_id: string
odometer_reading: long
test_result: string (PASS/FAIL)
co_emissions: double
hc_emissions: double
nox_emissions: double
particulate_matter: double
inspector_id: string
created_timestamp: timestamp
updated_timestamp: timestamp
source_file: string
ingestion_timestamp: timestamp (for tracking incremental loads)
"""

# emissions_summary (Silver/Transformed Layer)
"""
vehicle_id: string (PK)
latest_test_date: timestamp
total_tests: int
pass_count: int
fail_count: int
avg_co_emissions: double
avg_nox_emissions: double
last_pass_date: timestamp
last_fail_date: timestamp
risk_score: double
updated_date: date (partition key)
"""
```

---

## 2. Initial Full Load - PySpark Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

# Initialize Spark with Iceberg support
spark = SparkSession.builder
.appName("VehicleEmissions-FullLoad")
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.snowflake_catalog", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.snowflake_catalog.type", "snowflake")
.config("spark.sql.catalog.snowflake_catalog.snowflake.url", "your-account.snowflakecomputing.com")
.config("spark.sql.catalog.snowflake_catalog.snowflake.user", "your-user")
.config("spark.sql.catalog.snowflake_catalog.snowflake.password", "your-password")
.config("spark.sql.catalog.snowflake_catalog.snowflake.database", "EMISSIONS_DB")
.config("spark.sql.catalog.snowflake_catalog.snowflake.schema", "RAW")
.config("spark.sql.catalog.snowflake_catalog.warehouse", "s3://your-bucket/iceberg-warehouse")
.getOrCreate()

# Create Iceberg table (one-time setup)
spark.sql("""
    CREATE TABLE IF NOT EXISTS snowflake_catalog.emissions_db.raw.emissions_tests (
        test_id STRING,
        vehicle_id STRING,
        test_date TIMESTAMP,
        station_id STRING,
        odometer_reading LONG,
        test_result STRING,
        co_emissions DOUBLE,
        hc_emissions DOUBLE,
        nox_emissions DOUBLE,
        particulate_matter DOUBLE,
        inspector_id STRING,
        created_timestamp TIMESTAMP,
        updated_timestamp TIMESTAMP,
        source_file STRING,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(test_date))
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='snappy',
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='10'
    )
""")


# Full load from source
def full_load_emissions():
    # Read source data (CSV, Parquet, JSON, or from source DB)
    source_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3://your-landing-bucket/emissions/full_load/*.csv")


# Add metadata columns
ingestion_time = datetime.now()
enriched_df = source_df
.withColumn("ingestion_timestamp", lit(ingestion_time))
.withColumn("created_timestamp", col("test_date"))
.withColumn("updated_timestamp", lit(ingestion_time))

# Write to Iceberg table
enriched_df.writeTo("snowflake_catalog.emissions_db.raw.emissions_tests")
.option("write-audit-publish", "true")
.append()

print(f"Full load completed: {enriched_df.count()} records")

if __name__ == "__main__":
    full_load_emissions()
```

---

## 3. Incremental Load (Hourly) - Merge Pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import sys


def incremental_load_emissions(checkpoint_table="emissions_checkpoints"):
    """
    Performs incremental load using merge (upsert) pattern
    Handles: New tests + Updates to existing tests (corrections)
    """

    spark = SparkSession.builder
    .appName("VehicleEmissions-IncrementalLoad")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.snowflake_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.snowflake_catalog.type", "snowflake")
    .config("spark.sql.catalog.snowflake_catalog.snowflake.url", "your-account.snowflakecomputing.com")
    .config("spark.sql.catalog.snowflake_catalog.warehouse", "s3://your-bucket/iceberg-warehouse")
    .getOrCreate()


# Get last successful checkpoint
try:
    checkpoint_df = spark.table(f"snowflake_catalog.emissions_db.metadata.{checkpoint_table}")
    last_checkpoint = checkpoint_df
    .filter(col("job_name") == "emissions_incremental")
    .agg(max("checkpoint_timestamp").alias("last_run"))
    .collect()[0]["last_run"]
except:
# First run - use 24 hours ago as default
last_checkpoint = datetime.now() - timedelta(hours=24)

print(f"Loading data since: {last_checkpoint}")

# Read incremental data from landing zone
# Assumes files are timestamped or organized by hour
current_time = datetime.now()
incremental_path = f"s3://your-landing-bucket/emissions/incremental/{current_time.strftime('%Y/%m/%d/%H')}/*.csv"

try:
    incremental_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(incremental_path)
except:
print("No new files to process")
return

# Add metadata
enriched_df = incremental_df
.withColumn("ingestion_timestamp", lit(current_time))
.withColumn("updated_timestamp", lit(current_time))
.withColumn("created_timestamp",
            when(col("created_timestamp").isNull(), col("test_date"))
            .otherwise(col("created_timestamp")))

# Create temp view for merge
enriched_df.createOrReplaceTempView("emissions_updates")

# MERGE Operation (Upsert)
merge_query = """
        MERGE INTO snowflake_catalog.emissions_db.raw.emissions_tests AS target
        USING emissions_updates AS source
        ON target.test_id = source.test_id
        
        WHEN MATCHED THEN
            UPDATE SET
                vehicle_id = source.vehicle_id,
                test_date = source.test_date,
                station_id = source.station_id,
                odometer_reading = source.odometer_reading,
                test_result = source.test_result,
                co_emissions = source.co_emissions,
                hc_emissions = source.hc_emissions,
                nox_emissions = source.nox_emissions,
                particulate_matter = source.particulate_matter,
                inspector_id = source.inspector_id,
                updated_timestamp = source.updated_timestamp,
                source_file = source.source_file,
                ingestion_timestamp = source.ingestion_timestamp
        
        WHEN NOT MATCHED THEN
            INSERT (
                test_id, vehicle_id, test_date, station_id, odometer_reading,
                test_result, co_emissions, hc_emissions, nox_emissions,
                particulate_matter, inspector_id, created_timestamp,
                updated_timestamp, source_file, ingestion_timestamp
            )
            VALUES (
                source.test_id, source.vehicle_id, source.test_date, 
                source.station_id, source.odometer_reading, source.test_result,
                source.co_emissions, source.hc_emissions, source.nox_emissions,
                source.particulate_matter, source.inspector_id,
                source.created_timestamp, source.updated_timestamp,
                source.source_file, source.ingestion_timestamp
            )
    """

spark.sql(merge_query)

record_count = enriched_df.count()
print(f"Incremental load completed: {record_count} records processed")

# Update checkpoint
checkpoint_data = [(
    "emissions_incremental",
    current_time,
    record_count,
    "SUCCESS"
)]

checkpoint_schema = "job_name STRING, checkpoint_timestamp TIMESTAMP, records_processed LONG, status STRING"
spark.createDataFrame(checkpoint_data, schema=checkpoint_schema)
.writeTo(f"snowflake_catalog.emissions_db.metadata.{checkpoint_table}")
.append()

return record_count

if __name__ == "__main__":
    incremental_load_emissions()
```

---

## 4. Transformation Job - Aggregated Summary (Silver Layer)

```python
def transform_emissions_summary():
    """
    Creates aggregated vehicle emissions summary
    Runs after incremental load
    """

    spark = SparkSession.builder
    .appName("VehicleEmissions-Transform")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.snowflake_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .getOrCreate()


# Read from raw Iceberg table
raw_tests = spark.table("snowflake_catalog.emissions_db.raw.emissions_tests")

# Calculate risk score and aggregations
summary_df = raw_tests.groupBy("vehicle_id").agg(
    max("test_date").alias("latest_test_date"),
    count("*").alias("total_tests"),
    sum(when(col("test_result") == "PASS", 1).otherwise(0)).alias("pass_count"),
    sum(when(col("test_result") == "FAIL", 1).otherwise(0)).alias("fail_count"),
    avg("co_emissions").alias("avg_co_emissions"),
    avg("nox_emissions").alias("avg_nox_emissions"),
    max(when(col("test_result") == "PASS", col("test_date"))).alias("last_pass_date"),
    max(when(col("test_result") == "FAIL", col("test_date"))).alias("last_fail_date")
).withColumn("risk_score",
             when(col("fail_count") > 2, 100.0)
             .when(col("fail_count") > 0, 50.0 + (col("avg_nox_emissions") * 10))
             .otherwise(col("avg_co_emissions") * 5)
             ).withColumn("updated_date", current_date())

# Write to summary table (create if not exists)
spark.sql("""
        CREATE TABLE IF NOT EXISTS snowflake_catalog.emissions_db.transformed.emissions_summary (
            vehicle_id STRING,
            latest_test_date TIMESTAMP,
            total_tests INT,
            pass_count INT,
            fail_count INT,
            avg_co_emissions DOUBLE,
            avg_nox_emissions DOUBLE,
            last_pass_date TIMESTAMP,
            last_fail_date TIMESTAMP,
            risk_score DOUBLE,
            updated_date DATE
        )
        USING iceberg
        PARTITIONED BY (updated_date)
    """)

# Merge into summary table
summary_df.createOrReplaceTempView("summary_updates")

spark.sql("""
        MERGE INTO snowflake_catalog.emissions_db.transformed.emissions_summary AS target
        USING summary_updates AS source
        ON target.vehicle_id = source.vehicle_id
        
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Summary transformation completed: {summary_df.count()} vehicles")

if __name__ == "__main__":
    transform_emissions_summary()
```

---

## 5. Orchestration with Airflow DAG

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'vehicle_emissions_incremental_load',
    default_args=default_args,
    description='Hourly incremental load of vehicle emissions data',
    schedule_interval='0 * * * *',  # Hourly
    catchup=False,
    tags=['emissions', 'iceberg', 'incremental'],
)

# EMR Spark job configuration
SPARK_STEPS = [
    {
        'Name': 'Emissions Incremental Load',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                '--jars', 's3://your-bucket/jars/iceberg-spark-runtime.jar,s3://your-bucket/jars/snowflake-jdbc.jar',
                's3://your-bucket/scripts/incremental_load.py'
            ]
        }
    },
    {
        'Name': 'Emissions Transform',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://your-bucket/scripts/transform_summary.py'
            ]
        }
    }
]

# Add steps to EMR cluster
add_steps = EmrAddStepsOperator(
    task_id='add_incremental_steps',
    job_flow_id='{{ var.value.emr_cluster_id }}',
    steps=SPARK_STEPS,
    aws_conn_id='aws_default',
    dag=dag,
)

# Wait for completion
wait_for_load = EmrStepSensor(
    task_id='wait_for_incremental_load',
    job_flow_id='{{ var.value.emr_cluster_id }}',
    step_id='{{ task_instance.xcom_pull(task_ids="add_incremental_steps")[0] }}',
    aws_conn_id='aws_default',
    dag=dag,
)

wait_for_transform = EmrStepSensor(
    task_id='wait_for_transform',
    job_flow_id='{{ var.value.emr_cluster_id }}',
    step_id='{{ task_instance.xcom_pull(task_ids="add_incremental_steps")[1] }}',
    aws_conn_id='aws_default',
    dag=dag,
)

# Define dependencies
add_steps >> wait_for_load >> wait_for_transform
```

---

## 6. Snowflake Query Examples for Business Users

```sql
-- Query 1: Failed tests in last 24 hours
SELECT 
    vehicle_id,
    test_date,
    station_id,
    test_result,
    co_emissions,
    nox_emissions
FROM snowflake_catalog.emissions_db.raw.emissions_tests
WHERE test_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
  AND test_result = 'FAIL'
ORDER BY test_date DESC;

-- Query 2: High-risk vehicles summary
SELECT 
    vehicle_id,
    latest_test_date,
    total_tests,
    fail_count,
    risk_score,
    avg_nox_emissions
FROM snowflake_catalog.emissions_db.transformed.emissions_summary
WHERE risk_score > 70
ORDER BY risk_score DESC
LIMIT 100;

-- Query 3: Station performance
SELECT 
    station_id,
    COUNT(*) as total_tests,
    SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) as pass_count,
    ROUND(100.0 * SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM snowflake_catalog.emissions_db.raw.emissions_tests
WHERE test_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY station_id
ORDER BY pass_rate ASC;
```

---

## Key Benefits of This Approach

1. **Incremental Efficiency**: Only process new/changed records
2. **Upsert Capability**: Handle corrections to historical data
3. **Partition Pruning**: Query optimization via date partitioning
4. **Time Travel**: Iceberg's built-in versioning for auditing
5. **Snowflake Integration**: Business users query via familiar SQL interface
6. **S3 Cost Efficiency**: Parquet compression + Iceberg's metadata management

## Monitoring & Optimization

```python
# Check table history
spark.sql("SELECT * FROM snowflake_catalog.emissions_db.raw.emissions_tests.history")

# Check table snapshots
spark.sql("SELECT * FROM snowflake_catalog.emissions_db.raw.emissions_tests.snapshots")

# Expire old snapshots (data retention)
spark.sql("""
    CALL snowflake_catalog.system.expire_snapshots(
        table => 'emissions_db.raw.emissions_tests',
        older_than => TIMESTAMP '2024-01-01 00:00:00',
        retain_last => 10
    )
""")

# Compact small files
spark.sql("""
    CALL snowflake_catalog.system.rewrite_data_files(
        table => 'emissions_db.raw.emissions_tests'
    )
""")
```
