# 08 - S3 & Snowflake Integration

## Why S3 + Snowflake?

**S3:** Cheap storage, accessible from anywhere
**Snowflake:** Fast SQL queries, separate compute, easy sharing

**Together:** Decoupled storage and compute = cost-effective data warehouse

---

## Table of Contents
1. [S3 Best Practices](#s3-best-practices)
2. [Reading from S3](#reading-from-s3)
3. [Writing to S3](#writing-to-s3)
4. [Snowflake Integration](#snowflake-integration)
5. [Real Examples](#real-examples)

---

## S3 Best Practices

🔗 **Related:** Section 01 - Partitioning Basics | Section 05 - Shuffle Optimization

### Partitioning Strategy

**Good partitioning = Faster queries + Lower costs**

```python
# BAD: No partitioning
df.write.parquet("s3://bucket/data/all_sales/")
# Reading 100GB to query 2024 data = slow!

# GOOD: Partition by date
df.write\
    .partitionBy("year", "month", "day")\
    .parquet("s3://bucket/data/sales/")

# Structure:
# s3://bucket/data/sales/
#   year=2024/
#     month=01/
#       day=15/
#         part-0000.parquet
#       day=16/
#         part-0000.parquet
#     month=02/
#       ...

# Reading 2024-01-15 only = instant (partitions pruned!)

# Query in Spark
df = spark.read.parquet("s3://bucket/data/sales/")\
    .filter((col("year") == 2024) & (col("month") == 1) & (col("day") == 15))

# Spark automatically skips other partitions!
```

### Partition Sizing

```python
# Strategy: Partition by date + category

df.write\
    .partitionBy("load_date", "category")\
    .parquet("s3://bucket/data/sales/")

# Results in:
# load_date=2024-01-15/
#   category=Electronics/
#     part-0000.parquet (should be 50-200MB)
#   category=Clothing/
#     part-0000.parquet (should be 50-200MB)
# load_date=2024-01-16/
#   ...

# Ideal: 50-200MB per file
# Too small: Too many files, slow
# Too large: Memory issues when reading

# Adjust coalescing
df.coalesce(100)\  # 100 files per partition combo
    .write\
    .partitionBy("load_date", "category")\
    .parquet("s3://bucket/data/sales/")
```

### Directory Structure

```
Data Lake Structure (Best Practice)

s3://my-company-data-lake/
│
├─ raw/
│   ├─ sales/          (Original data, never modified)
│   │   ├─ 2024/
│   │   │   ├─ 01/
│   │   │   └─ 02/
│   │   └─ 2025/
│   ├─ customers/
│   └─ products/
│
├─ processed/
│   ├─ sales_clean/    (Cleaned, validated)
│   ├─ sales_summary/  (Aggregated)
│   └─ customer_dims/  (Dimension tables)
│
├─ warehouse/
│   ├─ fact_sales/     (Fact tables for warehouse)
│   ├─ fact_orders/
│   └─ dim_*/          (Dimension tables)
│
└─ temp/
    └─ scratch work... (Temporary, can be deleted)
```

### Naming Conventions

```python
# Good naming helps with automation and understanding

# Raw data: Original format
s3://bucket/raw/source_system/table_name/year=YYYY/month=MM/day=DD/

# Processed: Cleaned, validated
s3://bucket/processed/table_name_clean/

# Aggregated: Pre-computed summaries
s3://bucket/processed/table_name_daily_summary/

# Warehouse: Final facts and dimensions
s3://bucket/warehouse/fact_sales_daily/
s3://bucket/warehouse/dim_customers/
```

---

## Reading from S3

### Basic Read

```python
# Parquet (recommended)
df = spark.read.parquet("s3://bucket/data/sales/")

# CSV
df = spark.read.csv("s3://bucket/data/sales.csv", header=True)

# JSON
df = spark.read.json("s3://bucket/data/sales/")

# ORC
df = spark.read.orc("s3://bucket/data/sales/")
```

### Efficient Reading (Partitions)

```python
# Read only specific partitions (fast!)
df = spark.read.parquet("s3://bucket/data/sales/")\
    .filter((col("year") == 2024) & (col("month") == 1))

# Spark prunes partitions automatically
# Only reads: s3://bucket/data/sales/year=2024/month=01/
# Skips: year=2023/, year=2025/, etc.

# Actual data size: 5GB → Read size: 5GB ✓ Fast!

# BAD: Read entire dataset, then filter
df = spark.read.parquet("s3://bucket/data/sales/")  # 500GB!
df = df.filter((col("year") == 2024) & (col("month") == 1))

# Actual data size: 5GB → Read size: 500GB ✗ Slow!
```

### Handling Corrupted Files

```python
# Option 1: Skip corrupted records
df = spark.read\
    .option("mode", "PERMISSIVE")\
    .option("columnNameOfCorruptRecord", "_corrupt")\
    .parquet("s3://bucket/data/")

# Check for corruption
corrupt = df.filter(col("_corrupt").isNotNull())
print(f"Corrupted records: {corrupt.count()}")

# Remove and continue
df = df.filter(col("_corrupt").isNull()).drop("_corrupt")

# Option 2: Fail on corruption (strict)
df = spark.read\
    .option("mode", "FAILFAST")\
    .parquet("s3://bucket/data/")
```

### S3 Performance Tuning

```python
# Parallel reads
spark.conf.set("spark.hadoop.fs.s3a.threads.max", 100)

# Connection timeout (for slow networks)
spark.conf.set("spark.hadoop.fs.s3a.connection.timeout", 120000)

# Socket timeout
spark.conf.set("spark.hadoop.fs.s3a.socket.timeout", 120000)

# Multipart upload size
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", 33554432)  # 32MB

# Connection pooling
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", 100)
```

---

## Writing to S3

### Write Modes

```python
# OVERWRITE: Replace entire output
df.write\
    .mode("overwrite")\
    .parquet("s3://bucket/data/output/")

# APPEND: Add to existing (for incremental loads)
df.write\
    .mode("append")\
    .parquet("s3://bucket/data/output/")

# IGNORE: Don't write if path exists
df.write\
    .mode("ignore")\
    .parquet("s3://bucket/data/output/")

# ERROR (default): Fail if path exists
df.write\
    .mode("error")\
    .parquet("s3://bucket/data/output/")
```

### Optimized Writing

```python
# Coalesce for fewer files
df.coalesce(50)\
    .write\
    .mode("overwrite")\
    .parquet("s3://bucket/output/")

# Partitioning
df.write\
    .mode("overwrite")\
    .partitionBy("load_date")\
    .parquet("s3://bucket/output/")

# Compression
df.write\
    .option("compression", "snappy")\
    .mode("overwrite")\
    .parquet("s3://bucket/output/")

# Bucketing (pre-sorting for joins)
df.write\
    .mode("overwrite")\
    .bucketBy(100, "customer_id")\
    .sortBy("customer_id")\
    .parquet("s3://bucket/output/")
```

### Cost Optimization

```python
# S3 is cheap (~$0.023/GB/month)
# But network transfers cost money!

# Optimization 1: Write to local cluster storage first
df.write.mode("overwrite").parquet("/tmp/local_output/")
# Then move to S3 once (cheaper than multiple S3 writes)

# Optimization 2: Compress aggressively
df.write\
    .option("compression", "gzip")\  # Better compression
    .parquet("s3://bucket/output/")
# Tradeoff: Slower write, smaller files, lower S3 storage cost

# Optimization 3: Delete old data
# S3 has no "delete old partitions" function
# Must do manually
import subprocess
subprocess.run([
    "aws", "s3", "rm",
    "s3://bucket/old_data/year=2020/",
    "--recursive"
])
```

---

## Snowflake Integration

### Prerequisites

```bash
# 1. Install Snowflake connector
pip install snowflake-spark-connector

# 2. Snowflake account setup
# Account: xy12345.us-east-1
# Database: analytics_db
# Schema: raw
# Warehouse: compute_wh

# 3. AWS credentials configured
# Either in ~/.aws/credentials or environment variables
```

### Reading from Snowflake

```python
# Configuration
sfOptions = {
    "sfURL": "xy12345.us-east-1.snowflakecomputing.com",
    "sfUser": "etl_user",
    "sfPassword": "your_password_or_use_env_var",
    "sfDatabase": "analytics_db",
    "sfSchema": "raw",
    "sfWarehouse": "compute_wh"
}

# Read entire table
df = spark.read\
    .format("snowflake")\
    .options(**sfOptions)\
    .option("dbtable", "customers")\
    .load()

# Read query result
df = spark.read\
    .format("snowflake")\
    .options(**sfOptions)\
    .option("query", "SELECT * FROM customers WHERE active = 1")\
    .load()

# Read with partition
df = spark.read\
    .format("snowflake")\
    .options(**sfOptions)\
    .option("dbtable", "sales")\
    .option("sfWarehouse", "large_wh")  # Use bigger warehouse for large reads
    .load()
```

### Writing to Snowflake

```python
# Write new data
df.write\
    .format("snowflake")\
    .mode("overwrite")\
    .options(**sfOptions)\
    .option("dbtable", "sales_processed")\
    .save()

# Append data
df.write\
    .format("snowflake")\
    .mode("append")\
    .options(**sfOptions)\
    .option("dbtable", "sales_log")\
    .save()

# Optimize for writes
df.write\
    .format("snowflake")\
    .mode("overwrite")\
    .options(**sfOptions)\
    .option("dbtable", "sales_daily")\
    .option("sfWarehouse", "large_wh")  # Use bigger warehouse
    .option("spark.databricks.sql.io.cache.enabled", "false")  # Don't cache
    .save()
```

### Snowflake-S3 Integration

```python
# Snowflake can read directly from S3!
# This is faster than going through Spark

# In Snowflake SQL:
# CREATE STAGE s3_stage
#   URL = 's3://my-bucket/data/'
#   CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...');
#
# COPY INTO raw.sales
# FROM @s3_stage/sales/
# FILE_FORMAT = (TYPE = PARQUET);

# This is faster than Spark ETL in many cases!
# Use Spark for complex transformations,
# Use Snowflake COPY for simple data ingestion
```

---

## Real Examples

### Example 1: S3 to Snowflake ETL

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
import logging

logger = logging.getLogger(__name__)

# Snowflake credentials
snowflake_options = {
    "sfURL": "xy12345.us-east-1.snowflakecomputing.com",
    "sfUser": "etl_user",
    "sfPassword": "password",
    "sfDatabase": "analytics",
    "sfSchema": "raw",
    "sfWarehouse": "compute"
}

spark = SparkSession.builder.appName("S3ToSnowflakeETL").getOrCreate()

# Step 1: Read from S3
logger.info("Reading from S3...")
sales = spark.read.parquet("s3://data-lake/sales/year=2024/month=01/")

# Step 2: Transform
logger.info("Transforming...")
sales_clean = sales\
    .filter(col("amount") > 0)\
    .withColumn("sale_date", from_unixtime(col("timestamp"), "yyyy-MM-dd"))

# Step 3: Write to Snowflake
logger.info("Writing to Snowflake...")
sales_clean.write\
    .format("snowflake")\
    .mode("overwrite")\
    .options(**snowflake_options)\
    .option("dbtable", "SALES_PROCESSED")\
    .save()

logger.info("ETL complete!")
```

### Example 2: Incremental Load from S3 to Snowflake

```python
def incremental_s3_to_snowflake(
    spark,
    s3_path,
    snowflake_options,
    snowflake_table,
    last_modified_column
):
    """Load only new/modified records from S3 to Snowflake"""

    # Read from S3
    df = spark.read.parquet(s3_path)

    # Get max date from Snowflake
    try:
        snowflake_df = spark.read\
            .format("snowflake")\
            .options(**snowflake_options)\
            .option("query", f"SELECT MAX({last_modified_column}) as max_date FROM {snowflake_table}")\
            .load()

        max_date = snowflake_df.collect()[0]['max_date']
        logger.info(f"Last load: {max_date}")
    except:
        max_date = None
        logger.info("First load detected")

    # Filter new records
    if max_date:
        df = df.filter(col(last_modified_column) > max_date)

    if df.count() == 0:
        logger.info("No new records to load")
        return

    logger.info(f"Loading {df.count()} new records...")

    # Append to Snowflake
    df.write\
        .format("snowflake")\
        .mode("append")\
        .options(**snowflake_options)\
        .option("dbtable", snowflake_table)\
        .save()

    logger.info("Load complete!")

# Usage
incremental_s3_to_snowflake(
    spark,
    "s3://data-lake/events/",
    snowflake_options,
    "RAW_EVENTS",
    "LOADED_AT"
)
```

### Example 3: Snowflake to S3 (Unload)

```python
def unload_snowflake_to_s3(
    spark,
    query,
    snowflake_options,
    s3_path
):
    """Read from Snowflake, write to S3 (unload)"""

    # Read query results
    df = spark.read\
        .format("snowflake")\
        .options(**snowflake_options)\
        .option("query", query)\
        .load()

    logger.info(f"Loaded {df.count()} records from Snowflake")

    # Optimize for S3 write
    df.coalesce(100)\
        .write\
        .mode("overwrite")\
        .option("compression", "snappy")\
        .parquet(s3_path)

    logger.info(f"Unloaded to {s3_path}")

# Usage
unload_snowflake_to_s3(
    spark,
    "SELECT * FROM ANALYTICS.PUBLIC.SALES WHERE YEAR = 2024",
    snowflake_options,
    "s3://archive/sales_2024/"
)
```

### Example 4: S3 Data Lake to Snowflake Data Warehouse

```python
# Complete architecture for syncing data lake to warehouse

def sync_datalake_to_warehouse(spark, date_str):
    """
    Sync processed data from S3 data lake to Snowflake warehouse

    Architecture:
    S3 Raw → S3 Processed → Snowflake Analytics
    """

    snowflake_options = {
        "sfURL": "xy12345.us-east-1.snowflakecomputing.com",
        "sfUser": "warehouse_etl",
        "sfPassword": "password",
        "sfDatabase": "analytics",
        "sfSchema": "warehouse",
        "sfWarehouse": "load_wh"
    }

    # 1. Sales data
    logger.info("Processing sales...")
    sales = spark.read.parquet(f"s3://lake/processed/sales/{date_str}/")
    sales.write.format("snowflake").mode("append")\
        .options(**snowflake_options)\
        .option("dbtable", "FACT_SALES")\
        .save()

    # 2. Customer data
    logger.info("Processing customers...")
    customers = spark.read.parquet(f"s3://lake/processed/customers/{date_str}/")
    customers.write.format("snowflake").mode("overwrite")\
        .options(**snowflake_options)\
        .option("dbtable", "DIM_CUSTOMERS")\
        .save()

    # 3. Products data
    logger.info("Processing products...")
    products = spark.read.parquet(f"s3://lake/processed/products/{date_str}/")
    products.write.format("snowflake").mode("overwrite")\
        .options(**snowflake_options)\
        .option("dbtable", "DIM_PRODUCTS")\
        .save()

    logger.info(f"Sync complete for {date_str}")

# Usage
sync_datalake_to_warehouse(spark, "2024-01-15")
```

---

## Cost Optimization Tips

### S3 Costs

```
Storage: $0.023/GB/month
Requests: $0.0004 per PUT request
Transfer: $0.02/GB to internet, free within AWS region

Optimization:
- Delete old partitions (use lifecycle policies)
- Use compression (gzip, snappy)
- Limit requests (batch operations)
- Keep data in us-east-1 (cheaper region)
```

### Snowflake Costs

```
Compute: $4/credit/hour
Storage: $23/TB/month

Optimization:
- Use smaller warehouses for reads (xs, small)
- Use larger warehouses only for writes (large, xl)
- Enable automatic suspension (10 min default)
- Archive to S3 for long-term storage
- Use time-travel sparingly
```

### Combined Strategy

```
Frequent access (hot data):
├─ Keep in Snowflake (fast queries)
└─ Cost: ~$4/credit + $23/TB

Infrequent access (warm data):
├─ Keep in S3, query via Spark
└─ Cost: ~$0.023/GB + compute

Archive (cold data):
├─ Keep in S3 Glacier ($0.005/GB)
└─ Cost: ~$0.005/GB + restore fees
```

---

## Key Takeaways

✅ **Partition S3 data by date** - Faster queries, lower costs
✅ **Use Snowflake for SQL analytics** - Separate compute from storage
✅ **Broadcast small Snowflake tables** - Join with Spark DataFrames
✅ **Compress when writing to S3** - 50% smaller files
✅ **Incremental loads** - Load only new/changed data
✅ **Archive old data** - Move to Glacier for cost savings

---

## Next Steps

1. **Set up Snowflake connector** - Test reading/writing
2. **Partition existing S3 data** - By date for queries
3. **Build incremental load job** - Only new records
4. **Move to Section 09** - Interview questions and review

---

**Remember:** S3 + Snowflake is the modern data stack. Master this combination and you can build enterprise-scale data platforms!
