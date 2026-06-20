# 07 - Real-World ETL Pipelines

## What is an ETL Pipeline?

**ETL = Extract → Transform → Load**

```
Raw Data (Messy) → Clean & Enrich → Output (Ready)
      ↓                  ↓              ↓
   Extract          Transform        Load
   Read files       Apply rules      Write results
```

**Reality:** Most production jobs are ETL pipelines. Learning to build robust ones is crucial.

---

## Table of Contents
1. [ETL Architecture](#etl-architecture)
2. [Complete Example: Daily Sales ETL](#complete-example-daily-sales-etl)
3. [Error Handling & Retry Logic](#error-handling--retry-logic)
4. [Monitoring & Alerting](#monitoring--alerting)
5. [Common Patterns](#common-patterns)
6. [Optimization Tips](#optimization-tips)

---

## ETL Architecture

### Simple Architecture

```
Data Source
    ↓
  Extract (Read)
    ├─ Validate schema
    └─ Handle corrupted records
    ↓
  Transform (Process)
    ├─ Clean data
    ├─ Enrich with dimensions
    ├─ Aggregate
    └─ Validate business rules
    ↓
  Load (Write)
    ├─ Write to target
    ├─ Update metadata
    └─ Mark as complete
    ↓
Data Warehouse / Lake
```

### Structure Pattern

```python
# ETL pipelines follow this structure:
class ETLPipeline:
    def extract(self): pass      # Read source data
    def transform(self, df): pass # Clean, validate, enrich
    def load(self, df): pass      # Write to target

    def run(self):
        try:
            data = self.extract()
            transformed = self.transform(data)
            self.load(transformed)
        except Exception as e:
            log_error(f"Pipeline failed: {e}")
            raise
```

---

## Complete Example: Daily Sales ETL

### Scenario

You work for an e-commerce company. Every day:
1. Extract raw sales data from S3
2. Clean and validate data
3. Join with customer and product dimensions
4. Aggregate by department
5. Load to data warehouse

### Full Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, broadcast
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SalesETL:
    def __init__(self):
        self.spark = SparkSession.builder.appName("SalesETL").getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", 200)

    def extract(self, date_str):
        """Read raw sales data, handle corrupted records"""
        try:
            df = self.spark.read\
                .option("columnNameOfCorruptRecord", "_corrupt_record")\
                .parquet(f"s3://raw-data/sales/{date_str}/")

            # Remove corrupted records
            df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            logger.info(f"Extracted {df.count()} records from {date_str}")
            return df
        except Exception as e:
            logger.error(f"Extract failed: {e}")
            raise

    def transform(self, df):
        """Clean, enrich, aggregate"""
        # Validate: check required fields and remove nulls
        required = ["transaction_id", "customer_id", "amount", "timestamp"]
        for field in required:
            if field not in df.columns:
                raise ValueError(f"Missing required field: {field}")
        df = df.dropna(subset=required)

        # Parse date, apply business rules
        df = df\
            .withColumn("sale_date", col("timestamp").cast("date"))\
            .filter(col("amount") > 0)\
            .filter(col("amount") < 1000000)

        # Enrich with dimensions (broadcast small tables)
        customers = self.spark.read.parquet("s3://dimensions/customers/")
        products = self.spark.read.parquet("s3://dimensions/products/")

        df = df.join(broadcast(customers), "customer_id", "left")\
               .join(broadcast(products), "product_id", "left")

        # Add derived fields
        df = df\
            .withColumn("tax", when(col("product_category") == "Food", col("amount") * 0.05)\
                              .otherwise(col("amount") * 0.10))\
            .withColumn("total", col("amount") + col("tax"))

        # Aggregate by date and category
        result = df.groupBy("sale_date", "product_category").agg(
            spark_sum("amount").alias("revenue"),
            count("*").alias("count"),
            avg("amount").alias("avg_amount")
        )

        logger.info(f"Transform complete: {result.count()} aggregated rows")
        return result

    def load(self, df, output_path):
        """Write to target with partitioning"""
        try:
            df.write\
                .mode("overwrite")\
                .partitionBy("sale_date")\
                .parquet(output_path)
            logger.info(f"Loaded to {output_path}")
        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise

    def run(self, date_str):
        """Execute full pipeline"""
        try:
            data = self.extract(date_str)
            transformed = self.transform(data)
            self.load(transformed, "s3://processed-data/sales/")
            logger.info("Pipeline completed")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    import sys
    etl = SalesETL()
    date = sys.argv[1] if len(sys.argv) > 1 else \
        (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    etl.run(date)
```

### Running the Job

```bash
# Local test
spark-submit daily_sales_etl.py 2024-01-15

# On EMR cluster
aws emr add-steps \
  --cluster-id j-xxx \
  --steps Type=spark,Name="DailySalesETL",\
ActionOnFailure=CONTINUE,\
Args=[--num-executors,10,--executor-cores,4,\
--executor-memory,16G,s3://scripts/daily_sales_etl.py,2024-01-15]
```

---

## Error Handling & Retry Logic

### Basic Try-Catch

```python
def load_with_error_handling(df, path):
    """Load with basic error handling"""
    try:
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Successfully wrote to {path}")
    except Exception as e:
        if "Permission denied" in str(e):
            logger.error("S3 permission issue - check AWS credentials")
        elif "Connection timeout" in str(e):
            logger.error("Network timeout - S3 may be slow")
        else:
            logger.error(f"Unknown error: {e}")
        raise
```

### Retry Logic

```python
import time
from functools import wraps

def retry_on_error(max_retries=3, backoff_factor=2):
    """Decorator for retrying failed operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            retry_count = 0
            last_exception = None

            while retry_count < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = backoff_factor ** (retry_count - 1)
                        logger.warning(
                            f"Attempt {retry_count} failed: {e}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)

            logger.error(f"Failed after {max_retries} attempts")
            raise last_exception

        return wrapper
    return decorator

# Usage
@retry_on_error(max_retries=3, backoff_factor=2)
def read_from_s3(path):
    return spark.read.parquet(path)

# Will retry up to 3 times with exponential backoff
data = read_from_s3("s3://bucket/data/")
```

### Fault-Tolerant Extract

```python
def extract_with_fallback(spark, primary_path, backup_path):
    """Extract from primary, fallback to backup if fails"""
    try:
        logger.info(f"Attempting to read from primary: {primary_path}")
        return spark.read.parquet(primary_path)
    except Exception as e:
        logger.warning(f"Primary read failed: {e}. Falling back to backup...")
        try:
            return spark.read.parquet(backup_path)
        except Exception as e2:
            logger.error(f"Both primary and backup failed: {e2}")
            raise

# Usage
data = extract_with_fallback(
    spark,
    "s3://current-day/sales/",
    "s3://backup/sales/"
)
```

---

## Monitoring & Alerting

### Logging Best Practices

```python
import logging

# Structured logging for parsing
logger = logging.getLogger(__name__)

def log_metrics(stage, rows, duration_sec, bytes_processed):
    """Log metrics in structured format"""
    logger.info(
        f"STAGE={stage} ROWS={rows} DURATION={duration_sec}s "
        f"BYTES={bytes_processed} THROUGHPUT={bytes_processed/duration_sec:.0f}B/s"
    )

# Usage
start = time.time()
df_processed = transform(df)
count = df_processed.count()
duration = time.time() - start
log_metrics("transform", count, duration, count * 100)  # Rough bytes estimate
```

### Health Checks

```python
def validate_output(df, expected_row_count):
    """Validate output meets expectations"""
    actual_count = df.count()
    percent_of_expected = (actual_count / expected_row_count) * 100

    if percent_of_expected < 50:
        raise ValueError(
            f"Output has only {percent_of_expected:.1f}% of expected rows "
            f"({actual_count} vs {expected_row_count})"
        )

    if percent_of_expected > 150:
        logger.warning(
            f"Output has {percent_of_expected:.1f}% of expected rows "
            f"(possible duplicate processing)"
        )

    logger.info(f"Output validation passed: {actual_count} rows")

# Usage
expected = 1000000
validate_output(result_df, expected)
```

---

## Common Patterns

### Pattern 1: Incremental Loading

```python
def incremental_load(spark, source_path, target_path, check_column):
    """Load only new/changed records (incremental)"""
    from pyspark.sql.functions import max as spark_max

    # Read current state
    try:
        current = spark.read.parquet(target_path)
        max_check_value = current.agg(spark_max(check_column)).collect()[0][0]
        logger.info(f"Current max {check_column}: {max_check_value}")
    except:
        max_check_value = 0  # First run
        logger.info("No existing data, doing full load")

    # Read new data
    source = spark.read.parquet(source_path)
    new_data = source.filter(col(check_column) > max_check_value)

    logger.info(f"Found {new_data.count()} new records to process")

    # Append to existing
    if max_check_value == 0:
        new_data.write.mode("overwrite").parquet(target_path)
    else:
        new_data.write.mode("append").parquet(target_path)

    logger.info("Incremental load complete")

# Usage
incremental_load(
    spark,
    "s3://raw-data/sales/",
    "s3://processed-data/sales/",
    "last_update_timestamp"
)
```

### Pattern 2: Slowly Changing Dimensions (SCD)

```python
def update_scd_type2(spark, new_data, target_path):
    """SCD Type 2: Keep history with effective dates"""

    # Read current dimension
    try:
        current = spark.read.parquet(target_path)
    except:
        # First run
        return new_data.withColumn(
            "effective_date",
            from_unixtime(col("updated_at"), "yyyy-MM-dd")
        ).withColumn(
            "end_date",
            lit("9999-12-31")
        ).write.mode("overwrite").parquet(target_path)

    # Find changed records
    changed = new_data.join(
        current.select("id", "version"),
        "id",
        "left_anti"  # Records in new_data but not in current
    )

    if changed.count() == 0:
        logger.info("No changes detected")
        return

    # Mark old records as expired
    expired = current.filter(col("id").isin(changed.select("id").rdd.map(lambda r: r[0]).collect()))\
        .withColumn("end_date", from_unixtime(col("updated_at"), "yyyy-MM-dd"))

    # Prepare new records
    new_versions = changed.withColumn(
        "effective_date",
        from_unixtime(col("updated_at"), "yyyy-MM-dd")
    ).withColumn(
        "end_date",
        lit("9999-12-31")
    )

    # Combine and write
    result = expired.union(new_versions)
    result.write.mode("overwrite").parquet(target_path)

    logger.info(f"SCD Type 2 update: {changed.count()} records changed")
```

### Pattern 3: Deduplication

```python
def deduplicate(df, partition_cols, order_cols):
    """Keep latest record per partition, remove duplicates"""
    from pyspark.sql.window import Window

    # Window to get rank of records
    window = Window.partitionBy(*partition_cols).orderBy(
        *[col(c).desc() for c in order_cols]
    )

    # Keep only first (latest) record
    deduped = df.withColumn("row_num", row_number().over(window))\
        .filter(col("row_num") == 1)\
        .drop("row_num")

    logger.info(f"Deduplicated: {df.count()} → {deduped.count()} records")
    return deduped

# Usage
deduped_sales = deduplicate(
    sales_df,
    partition_cols=["transaction_id"],
    order_cols=["timestamp"]
)
```

---

## Optimization Tips

### 1. Partition by Load Date

```python
# Write with date partition for efficient historical queries
df.write\
    .mode("overwrite")\
    .partitionBy("load_date")\
    .parquet("s3://data/warehouse/fact_sales/")

# Reading only specific dates is now fast:
# SELECT * FROM fact_sales WHERE load_date = '2024-01-15'
# (Only reads 1 partition, not entire table!)
```

### 2. Coalesce Before Writing

```python
# Writing 1000 small partitions → 1000 files (slow!)
df.write.parquet("output/")  # 1000 files

# Writing 10 coalesced partitions → 10 files (fast!)
df.coalesce(10).write.parquet("output/")  # 10 files

# Tradeoff: Fewer files = faster write, but larger per file
```

### 3. Use Vectorized Operations

```python
from pyspark.sql.functions import array_contains, from_json

# Avoid UDFs when possible
# BAD: Row-at-a-time UDF
@udf(StringType())
def slow_parse(json_str):
    import json
    return json.loads(json_str)['key']

# GOOD: Built-in vectorized function
df = df.withColumn("key", get_json_object(col("json_col"), "$.key"))
# 10-100x faster!
```

---

## Key Takeaways

✅ **ETL = Extract → Transform → Load** - Simple architecture, big impact
✅ **Validate early** - Check data quality at each stage
✅ **Use broadcast for small tables** - Speeds up joins
✅ **Log metrics** - Track rows, duration, bytes
✅ **Handle errors gracefully** - Retry logic and fallbacks
✅ **Partition by date** - Efficient historical queries
✅ **Coalesce before writing** - Fewer, larger files

---

## Next Steps

1. **Build a simple ETL** - Follow the daily sales example
2. **Add error handling** - Implement retry logic
3. **Monitor production job** - Watch for issues
4. **Move to Section 08** - S3 and Snowflake integration

---

**Remember:** Production ETL jobs fail. The ones that survive are the ones with good error handling, logging, and monitoring!
