# 03 - Real-World PySpark Examples: ETL & Industry Use Cases

## Overview

Production-ready PySpark code for common industry ETL pipelines. Each example is concise, commented, and ready to adapt for your use case.

**Audience:** Beginners building first pipelines + Advanced users optimizing existing systems.

---

## Table of Contents

1. [Basic ETL Pipeline](#basic-etl-pipeline)
2. [E-Commerce Sales ETL](#e-commerce-sales-etl)
3. [Data Cleaning & Validation](#data-cleaning--validation)
4. [Aggregations & GroupBy](#aggregations--groupby)
5. [Joins (Broadcast & Regular)](#joins-broadcast--regular)
6. [Window Functions (Ranking, Running Totals)](#window-functions)
7. [Deduplication](#deduplication)
8. [Incremental Loading (Upsert Pattern)](#incremental-loading-upsert-pattern)
9. [JSON & Complex Data](#json--complex-data)
10. [S3 Operations with Error Handling](#s3-operations-with-error-handling)
11. [Performance Optimization Patterns](#performance-optimization-patterns)
12. [Advanced: Streaming Basics](#advanced-streaming-basics)

---

## Basic ETL Pipeline

### Scenario
Read CSV → Clean → Filter → Aggregate → Write Parquet

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum, year, month
import logging

# Setup
spark = SparkSession.builder.appName("BasicETL").getOrCreate()
logger = logging.getLogger(__name__)

def basic_etl():
    """Simple ETL: Read CSV, transform, write parquet"""

    # EXTRACT: Read with error handling
    try:
        df = spark.read.csv(
            "s3://raw-data/customers.csv",
            header=True,
            inferSchema=True,
            mode="DROPMALFORMED"  # Skip corrupted rows
        )
        logger.info(f"Extracted {df.count()} records")
    except Exception as e:
        logger.error(f"Extract failed: {e}")
        raise

    # TRANSFORM: Clean and validate
    df_clean = df \
        .dropna(subset=["customer_id", "email"]) \
        .filter(col("age") > 0) \
        .filter(col("age") < 150) \
        .withColumn("signup_year", year(col("signup_date"))) \
        .withColumn("is_vip", when(col("lifetime_value") > 10000, True).otherwise(False))

    # LOAD: Write to S3 with partitioning
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("signup_year") \
        .parquet("s3://processed-data/customers/")

    logger.info("ETL complete")

if __name__ == "__main__":
    basic_etl()
```

---

## E-Commerce Sales ETL

### Scenario
Daily sales pipeline: Extract orders → Enrich with customer/product → Aggregate → Load to DW

```python
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max, broadcast,
    when, coalesce, datediff, current_date
)
from datetime import datetime, timedelta

class SalesETL:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SalesETL") \
            .config("spark.sql.shuffle.partitions", "100") \
            .getOrCreate()

    def extract(self, date_str):
        """Read raw sales data for specific date"""
        return self.spark.read \
            .parquet(f"s3://raw-data/sales/date={date_str}/")

    def enrich_with_dimensions(self, sales_df):
        """Join with customer and product dimensions"""
        # Read small tables (will be broadcast automatically if < 10MB)
        customers = self.spark.read.parquet("s3://dimensions/customers/")
        products = self.spark.read.parquet("s3://dimensions/products/")

        # Enrich with customer info
        sales = sales_df.join(
            broadcast(customers.select("customer_id", "segment", "country")),
            "customer_id",
            "left"
        )

        # Enrich with product info
        sales = sales.join(
            broadcast(products.select("product_id", "category", "margin")),
            "product_id",
            "left"
        )

        return sales

    def transform(self, sales_df):
        """Clean, validate, add business logic"""
        # Remove invalid records
        df = sales_df \
            .dropna(subset=["order_id", "amount"]) \
            .filter(col("amount") > 0) \
            .filter(col("amount") < 1000000)

        # Add calculated fields
        df = df \
            .withColumn("profit", col("amount") * col("margin")) \
            .withColumn("tax",
                when(col("category") == "Food", col("amount") * 0.05)
                .otherwise(col("amount") * 0.10)
            ) \
            .withColumn("total", col("amount") + col("tax"))

        return df

    def aggregate(self, df):
        """Daily summary by category and segment"""
        return df.groupBy("sale_date", "category", "segment").agg(
            spark_sum("amount").alias("total_sales"),
            spark_sum("profit").alias("total_profit"),
            count("order_id").alias("order_count"),
            avg("amount").alias("avg_order_value"),
            max("amount").alias("max_order_value")
        ).orderBy(col("total_sales").desc())

    def load(self, df, output_path):
        """Write with partitioning for efficient queries"""
        df.write \
            .mode("overwrite") \
            .partitionBy("sale_date") \
            .parquet(output_path)

    def run(self, date_str):
        """Execute full pipeline"""
        sales = self.extract(date_str)
        sales = self.enrich_with_dimensions(sales)
        sales = self.transform(sales)
        summary = self.aggregate(sales)
        self.load(summary, "s3://warehouse/sales_summary/")
        return summary

# Usage
if __name__ == "__main__":
    etl = SalesETL()
    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    etl.run(date)
```

---

## Data Cleaning & Validation

### Common Patterns for Raw Data

```python
def clean_and_validate(df):
    """Production-ready data cleaning"""

    # 1. Remove duplicates (keep latest by timestamp)
    from pyspark.sql.window import Window
    window = Window.partitionBy("id").orderBy(col("updated_at").desc())
    df = df.withColumn("rn", F.row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # 2. Handle null values
    df = df \
        .fillna({"age": 0, "salary": 0}) \
        .fillna({"country": "Unknown"})

    # 3. Trim whitespace from string columns
    string_cols = [col_name for col_name, dtype in df.dtypes if dtype == "string"]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(col(col_name)))

    # 4. Validate data quality
    df = df \
        .filter(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) \
        .filter(col("age") >= 0) \
        .filter(col("age") <= 120) \
        .filter(col("salary") >= 0) \
        .filter(col("salary") <= 10000000)

    # 5. Standardize formats
    df = df \
        .withColumn("email", F.lower(col("email"))) \
        .withColumn("phone", F.regexp_replace(col("phone"), "[^0-9]", "")) \
        .withColumn("signup_date", F.to_date(col("signup_date"), "yyyy-MM-dd"))

    # 6. Flag suspicious records (for audit)
    df = df.withColumn("quality_flag",
        when((col("salary") > 500000) | (col("age") > 100), "REVIEW")
        .otherwise("OK")
    )

    return df

# Usage
df_clean = clean_and_validate(df)
```

---

## Aggregations & GroupBy

### Common Industry Patterns

```python
def aggregation_patterns(df):
    """Real-world groupBy and aggregation examples"""
    from pyspark.sql.functions import (
        col, sum as spark_sum, count, avg, stddev,
        percentile_approx, collect_list, concat_ws
    )

    spark = SparkSession.getActiveSession()

    # 1. Simple aggregation
    daily_summary = df.groupBy("date", "category").agg(
        spark_sum("amount").alias("revenue"),
        count("*").alias("transactions"),
        avg("amount").alias("avg_transaction")
    )

    # 2. Multiple aggregations with different conditions
    agg_df = df.groupBy("customer_id").agg(
        spark_sum("amount").alias("total_spent"),
        count("order_id").alias("order_count"),
        count(when(col("status") == "returned", 1)).alias("return_count"),
        avg("amount").alias("avg_order"),
        percentile_approx("amount", 0.5).alias("median_order"),
        min("order_date").alias("first_purchase"),
        max("order_date").alias("last_purchase")
    )

    # 3. Pivot: Convert categories to columns
    # Before: (customer, month, sales) → many rows
    # After: (customer, jan_sales, feb_sales, mar_sales) → fewer rows
    pivoted = df.groupBy("customer_id").pivot("month").sum("sales")

    # 4. Window functions for ranking and running totals
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")

    ranked_orders = df.withColumn(
        "order_number",
        F.row_number().over(window_spec)
    ).withColumn(
        "cumulative_spent",
        F.sum("amount").over(window_spec)
    )

    # 5. Group by with having clause (filter groups)
    large_customers = df.groupBy("customer_id").agg(
        spark_sum("amount").alias("total")
    ).filter(col("total") > 10000)  # Only customers > $10k

    # 6. Collect arrays within groups
    customer_orders = df.groupBy("customer_id").agg(
        collect_list("order_id").alias("orders"),
        collect_list("amount").alias("amounts")
    )

    return agg_df

# Usage
agg_results = aggregation_patterns(df)
```

---

## Joins (Broadcast & Regular)

### When to Use Broadcast vs Regular Joins

```python
def join_patterns(spark, df_large, df_medium, df_small):
    """Best practices for joins in Spark"""
    from pyspark.sql.functions import broadcast

    # 1. BROADCAST JOIN (< 10MB table)
    # Effect: Small table cached on all executors, no shuffle
    # Speed: 10-100x faster than regular join
    result = df_large.join(
        broadcast(df_small),  # Explicitly broadcast
        on="id",
        how="inner"
    )

    # 2. REGULAR JOIN (both tables > 10MB)
    # Effect: Both tables shuffled by join key
    # Use only when necessary
    result = df_large.join(
        df_medium,
        on=["customer_id"],
        how="left"
    )

    # 3. MULTI-TABLE JOIN with optimization
    # Order matters! Catalyst reorders, but being explicit helps
    result = df_large \
        .join(broadcast(df_small), "small_id") \
        .join(broadcast(df_medium), "medium_id")

    # 4. JOIN on multiple columns
    result = df_large.join(
        df_small,
        on=(df_large.customer_id == df_small.id) &
           (df_large.date == df_small.date),
        how="inner"
    )

    # 5. ANTI JOIN (find non-matching records)
    # Use case: Find customers with NO orders
    customers_no_orders = df_customers.join(
        df_orders.select("customer_id").distinct(),
        on="customer_id",
        how="left_anti"
    )

    # 6. SEMI JOIN (filter by matching)
    # Use case: Keep only top 100 customers' orders
    top_customers = df_customers \
        .filter(col("revenue") > 100000) \
        .select("customer_id")

    orders_top_customers = df_orders.join(
        top_customers,
        on="customer_id",
        how="left_semi"  # Keeps left columns only
    )

    # PERFORMANCE TIP: Check if broadcast will help
    # df_small.count() → How many rows?
    # df_small.rdd.map(lambda x: len(str(x))).sum() / 1024 / 1024 → MB?
    # If < 100MB, consider broadcasting

    return result
```

---

## Window Functions

### Ranking, Running Totals, Comparisons

```python
def window_function_patterns(df):
    """Advanced window functions for analytics"""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import (
        col, row_number, rank, dense_rank, lag, lead,
        sum as spark_sum, avg, max, min
    )

    # 1. RANKING within groups
    window_rank = Window.partitionBy("department").orderBy(col("salary").desc())

    ranked_employees = df.withColumn("rank", rank().over(window_rank)) \
        .withColumn("dense_rank", dense_rank().over(window_rank)) \
        .withColumn("row_number", row_number().over(window_rank))
    # rank: 1, 1, 3, 4 (skips after ties)
    # dense_rank: 1, 1, 2, 3 (no gaps)
    # row_number: 1, 2, 3, 4 (all unique)

    # 2. RUNNING TOTALS
    window_running = Window.partitionBy("customer_id").orderBy("order_date")

    with_cumulative = df.withColumn(
        "cumulative_sales",
        spark_sum("amount").over(window_running)
    ).withColumn(
        "avg_order_value",
        avg("amount").over(window_running)
    )

    # 3. LAG / LEAD (previous/next rows)
    window_order = Window.partitionBy("customer_id").orderBy("order_date")

    with_previous = df.withColumn(
        "previous_amount", lag("amount").over(window_order)
    ).withColumn(
        "next_amount", lead("amount").over(window_order)
    ).withColumn(
        "days_since_last",
        datediff(col("order_date"), lag("order_date").over(window_order))
    )

    # 4. YEAR-OVER-YEAR comparison
    window_yoy = Window.partitionBy("customer_id", month("order_date")).orderBy("order_date")

    yoy_sales = df.withColumn(
        "prev_year_amount",
        lag("amount", 12).over(window_yoy)  # 12 months back
    ).withColumn(
        "yoy_growth",
        (col("amount") - col("prev_year_amount")) / col("prev_year_amount")
    )

    # 5. TOP N per group
    window_top = Window.partitionBy("department").orderBy(col("salary").desc())

    top_3_per_dept = df.withColumn(
        "rank", row_number().over(window_top)
    ).filter(col("rank") <= 3)

    # 6. MOVING AVERAGE (last 7 days)
    window_moving = Window.partitionBy("store_id").orderBy("date") \
        .rangeBetween(-6, 0)  # Last 7 days

    moving_avg = df.withColumn(
        "moving_avg_sales", avg("amount").over(window_moving)
    )

    return ranked_employees
```

---

## Deduplication

### Remove Duplicates, Keep Latest

```python
def deduplication_patterns(df):
    """Industry-standard deduplication techniques"""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    # 1. SIMPLE DISTINCT (if duplicates are exact copies)
    df_unique = df.distinct()

    # 2. KEEP LATEST by timestamp (common pattern)
    window = Window.partitionBy("id").orderBy(col("updated_at").desc())
    df_latest = df.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # 3. KEEP LATEST with multiple ID columns
    window_multi = Window.partitionBy("customer_id", "product_id") \
        .orderBy(col("transaction_date").desc())
    df_dedup = df.withColumn("rn", row_number().over(window_multi)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # 4. KEEP BASED ON PRIORITY
    # Example: Keep "CONFIRMED" over "PENDING"
    window_priority = Window.partitionBy("order_id").orderBy(
        when(col("status") == "CONFIRMED", 0).otherwise(1)
    )
    df_best = df.withColumn("rn", row_number().over(window_priority)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # 5. REMOVE DUPLICATES with data quality check
    # Keep: not null values, valid emails, positive amounts
    window_quality = Window.partitionBy("email").orderBy(
        col("email").isNull(),  # nulls last
        col("updated_at").desc()
    )
    df_clean = df.withColumn("rn", row_number().over(window_quality)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    return df_latest

# Usage: Common in CDC (Change Data Capture) and data lake scenarios
```

---

## Incremental Loading (Upsert Pattern)

### Load Only New/Changed Data

```python
def incremental_load(spark, source_path, target_path):
    """Upsert pattern: Insert new, update changed, keep old"""
    from pyspark.sql.functions import col, max as spark_max

    # 1. Get current state
    try:
        current = spark.read.parquet(target_path)
        max_updated = current.agg(
            spark_max("updated_at")
        ).collect()[0][0]
    except:
        max_updated = None  # First run

    # 2. Load only new/changed data
    source = spark.read.parquet(source_path)
    if max_updated:
        new_data = source.filter(col("updated_at") > max_updated)
    else:
        new_data = source

    # 3. For true upsert: Remove old versions, keep new
    if max_updated:
        # Get list of updated IDs
        updated_ids = new_data.select("id").distinct()
        # Remove old versions from current
        current_other = current.join(updated_ids, "id", "left_anti")
        # Combine
        result = current_other.union(new_data)
    else:
        result = new_data

    # 4. Write back
    result.write.mode("overwrite").parquet(target_path)
    print(f"Loaded {new_data.count()} new records")

# ALTERNATIVE: Use Delta Lake for true ACID transactions
def incremental_load_delta(spark, source_path, target_path):
    """More production-ready with Delta Lake"""
    from delta.tables import DeltaTable

    source = spark.read.parquet(source_path)

    try:
        delta_table = DeltaTable.forPath(spark, target_path)

        # Merge: Update if exists, insert if new
        delta_table.alias("t").merge(
            source.alias("s"),
            "t.id = s.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    except:
        # First run: just write
        source.write.format("delta").mode("overwrite").save(target_path)
```

---

## JSON & Complex Data

### Parse, Explode, Handle Nested Structures

```python
def json_patterns(df):
    """Working with JSON and nested data"""
    from pyspark.sql.functions import (
        col, from_json, to_json, get_json_object,
        explode, explode_outer, arrays_zip
    )
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # 1. PARSE JSON string to columns
    json_schema = StructType([
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("email", StringType())
    ])

    df_parsed = df.withColumn(
        "parsed",
        from_json(col("json_string"), json_schema)
    ).select(
        col("parsed.name"),
        col("parsed.age"),
        col("parsed.email")
    )

    # 2. EXTRACT from JSON without full parse
    df_extracted = df.select(
        get_json_object(col("json_string"), "$.name").alias("name"),
        get_json_object(col("json_string"), "$.contact.email").alias("email"),
        get_json_object(col("json_string"), "$.tags[0]").alias("first_tag")
    )

    # 3. EXPLODE arrays (one row per element)
    # Before: (customer, [product1, product2, product3])
    # After: (customer, product1), (customer, product2), (customer, product3)
    df_exploded = df.select(
        col("customer_id"),
        explode(col("products")).alias("product")
    )

    # 4. EXPLODE with null handling
    df_safe = df.select(
        col("customer_id"),
        explode_outer(col("products")).alias("product")  # Keeps nulls
    )

    # 5. CONVERT back to JSON
    df_to_json = df.select(
        col("id"),
        to_json(struct("name", "age", "email")).alias("customer_json")
    )

    # 6. NESTED structure (struct within struct)
    df_nested = df.select(
        col("customer_id"),
        col("address.street"),
        col("address.city"),
        col("address.zip")
    )

    return df_parsed
```

---

## S3 Operations with Error Handling

### Production-Ready S3 ETL

```python
import time
from functools import wraps

def retry_on_s3_error(max_retries=3, backoff_factor=2):
    """Decorator for S3 operations with exponential backoff"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor ** attempt
                        print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"Failed after {max_retries} attempts")
                        raise
        return wrapper
    return decorator

class S3ETL:
    def __init__(self, spark):
        self.spark = spark
        # Configure S3 for optimal performance
        self.spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
        self.spark.conf.set("spark.sql.shuffle.partitions", "100")

    @retry_on_s3_error(max_retries=3)
    def read_with_retry(self, path, format="parquet"):
        """Read from S3 with retry logic"""
        if format == "parquet":
            return self.spark.read.parquet(path)
        elif format == "csv":
            return self.spark.read.csv(
                path,
                header=True,
                inferSchema=True,
                mode="DROPMALFORMED"
            )
        elif format == "json":
            return self.spark.read.json(path)

    @retry_on_s3_error(max_retries=3)
    def write_with_retry(self, df, path, format="parquet", mode="overwrite"):
        """Write to S3 with retry logic"""
        if format == "parquet":
            df.write.mode(mode).parquet(path)
        elif format == "csv":
            df.coalesce(1).write.mode(mode).csv(path, header=True)

    def etl_with_fallback(self, primary_path, backup_path):
        """Try primary path, fallback to backup if fails"""
        try:
            print(f"Reading from primary: {primary_path}")
            return self.read_with_retry(primary_path)
        except Exception as e:
            print(f"Primary failed: {e}. Using backup: {backup_path}")
            return self.read_with_retry(backup_path)

    def write_with_coalesce(self, df, path, target_files=10):
        """Write with optimized file count"""
        # Too many partitions = many small files (slow reads)
        # Too few = large files (slow writes, memory issues)
        df.coalesce(target_files).write.mode("overwrite").parquet(path)

    def write_partitioned(self, df, path, partition_cols):
        """Write with partitioning for efficient queries"""
        # Example: partition by year, month
        # Queries like WHERE year=2024 AND month=3 only read those partitions
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .parquet(path)

# Usage
s3_etl = S3ETL(spark)
df = s3_etl.read_with_retry("s3://my-bucket/data.parquet")
s3_etl.write_partitioned(df, "s3://warehouse/data/", ["year", "month"])
```

---

## Performance Optimization Patterns

### Real-World Tuning

```python
def optimize_for_performance(spark, df):
    """Production optimization patterns"""
    from pyspark.sql.functions import col

    # 1. SHUFFLE PARTITION TUNING
    # Default 200 is for large clusters. Adjust based on data size
    spark.conf.set("spark.sql.shuffle.partitions", "100")

    # 2. COALESCE before WRITE to reduce output files
    # Writing 500 partitions = 500 files (slow!)
    # Writing 10 partitions = 10 files (fast!)
    df_optimized = df.coalesce(10).write.parquet("s3://output/")

    # 3. PARTITION by load date for efficient historical queries
    df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .parquet("s3://warehouse/")

    # 4. SELECT only needed columns early (column pruning)
    # BAD: Read 100GB, then select 2 columns
    df_bad = spark.read.parquet("huge_file").select("name", "age")

    # GOOD: Let Catalyst push down, but be explicit
    df_good = spark.read.parquet("huge_file").select("name", "age")
    # Catalyst reads ONLY name + age columns

    # 5. FILTER early (predicate pushdown)
    # BAD: Group all, then filter
    df_bad = df.groupBy("category").count().filter(col("count") > 100)

    # GOOD: Filter before groupBy
    df_good = df.filter(col("amount") > 0).groupBy("category").count()

    # 6. USE BROADCAST for small tables
    from pyspark.sql.functions import broadcast
    df_large = spark.read.parquet("large.parquet")  # 100GB
    df_small = spark.read.parquet("small.parquet")  # 500MB
    result = df_large.join(broadcast(df_small), "id")

    # 7. CACHE intermediate results if reused
    df_intermediate = df \
        .filter(col("year") == 2024) \
        .select("customer_id", "amount")

    df_intermediate.cache()  # Keep in memory

    revenue = df_intermediate.groupBy("customer_id").sum()
    top_customers = df_intermediate.filter(col("amount") > 1000)

    df_intermediate.unpersist()  # Release memory

    # 8. CHECK EXECUTION PLAN
    df.explain(extended=False)  # Shows optimizations Catalyst applied

    return df_optimized

# Real scenario: 5GB file, slow groupBy
# Solution: Filter (2GB) → GroupBy (fast)
df_large = spark.read.parquet("s3://data/5gb_file/")
df_filtered = df_large.filter(col("status") == "ACTIVE")  # 2GB
df_summary = df_filtered.groupBy("category").sum()  # Fast!
```

---

## Advanced: Streaming Basics

### Real-Time Data Pipeline

```python
def streaming_example(spark):
    """Basic streaming for real-time data"""

    # Source: Kafka, S3, or other streaming source
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "events") \
        .load()

    # Parse JSON from Kafka
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    schema = StructType([
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("amount", IntegerType()),
        StructField("timestamp", StringType())
    ])

    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Transform
    df_transformed = df_parsed \
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .filter(col("amount") > 0)

    # Aggregate (tumbling window, 5 minutes)
    from pyspark.sql.window import Window

    df_windowed = df_transformed.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ).agg(
        sum("amount").alias("total"),
        count("*").alias("count")
    )

    # Write to console (for testing)
    query = df_windowed.writeStream \
        .format("console") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .outputMode("append") \
        .start()

    # Write to S3 (for production)
    query_s3 = df_windowed.writeStream \
        .format("parquet") \
        .option("path", "s3://warehouse/events/") \
        .option("checkpointLocation", "s3://checkpoints/events/") \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .partitionBy("event_type") \
        .start()

    query.awaitTermination()

# Usage: Start streaming job in background
# Keep running, processes new data as it arrives
```

---

## When NOT to Use UDFs (User Defined Functions)

### Avoid Performance Bottlenecks

```python
# BAD: Row-at-a-time Python UDF (1000x slower!)
from pyspark.sql.types import DoubleType

@udf(DoubleType())
def slow_multiply(x):
    return x * 1.1

df_slow = df.withColumn("result", slow_multiply(col("salary")))
# For 1M rows, Python function called 1M times
# Crosses Python/JVM boundary 1M times = SLOW!

# GOOD: Use built-in Spark functions
df_fast = df.withColumn("result", col("salary") * 1.1)
# Uses Catalyst-optimized code, vectorized execution

# Example: Parse date string
# BAD:
@udf(DateType())
def parse_date(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%Y-%m-%d").date()

df_bad = df.withColumn("date", parse_date(col("date_string")))

# GOOD:
df_good = df.withColumn("date", to_date(col("date_string"), "yyyy-MM-dd"))

# EXCEPTION: Use UDF only for complex logic Spark can't express
@udf(DoubleType())
def apply_business_logic(sales, cost, category):
    """Custom logic that can't be expressed in SQL"""
    base_profit = sales - cost
    if category == "ELECTRONICS":
        return base_profit * 0.15  # 15% markup
    elif category == "CLOTHING":
        return base_profit * 0.25  # 25% markup
    else:
        return base_profit * 0.10  # 10% markup

df = df.withColumn("profit", apply_business_logic(
    col("sales"), col("cost"), col("category")
))
```

---

## Complete Production Example: Multi-Table ETL

### Real scenario: Customer 360 Platform

```python
class Customer360ETL:
    """Build customer 360 view from multiple sources"""

    def __init__(self, spark):
        self.spark = spark

    def run(self):
        # 1. Read customer master
        customers = self.spark.read.parquet("s3://source/customers/")

        # 2. Read transactions (large table)
        transactions = self.spark.read.parquet("s3://source/transactions/")

        # 3. Read marketing (small table - broadcast it)
        from pyspark.sql.functions import broadcast
        marketing = self.spark.read.parquet("s3://source/marketing_segments/")

        # 4. Aggregate transaction data
        tx_summary = transactions.groupBy("customer_id").agg(
            F.sum("amount").alias("lifetime_value"),
            F.count("*").alias("transaction_count"),
            F.min("transaction_date").alias("first_purchase"),
            F.max("transaction_date").alias("last_purchase"),
            F.avg("amount").alias("avg_transaction")
        )

        # 5. Join all together
        customer_360 = customers \
            .join(tx_summary, "customer_id", "left") \
            .join(broadcast(marketing), "customer_id", "left") \
            .fillna({
                "lifetime_value": 0,
                "transaction_count": 0
            })

        # 6. Add derived metrics
        customer_360 = customer_360 \
            .withColumn(
                "days_since_purchase",
                F.datediff(F.current_date(), F.col("last_purchase"))
            ) \
            .withColumn(
                "is_active",
                F.when(F.col("days_since_purchase") <= 90, True).otherwise(False)
            ) \
            .withColumn(
                "customer_value_segment",
                F.when(F.col("lifetime_value") > 50000, "VIP")
                .when(F.col("lifetime_value") > 10000, "HIGH")
                .when(F.col("lifetime_value") > 1000, "MEDIUM")
                .otherwise("LOW")
            )

        # 7. Quality checks
        total_before = customers.count()
        total_after = customer_360.count()
        assert total_before == total_after, "Row count mismatch!"

        # 8. Write to warehouse
        customer_360.write \
            .mode("overwrite") \
            .partitionBy("customer_value_segment") \
            .parquet("s3://warehouse/customer_360/")

        print(f"Processed {total_after} customers")
        return customer_360

# Run
etl = Customer360ETL(spark)
df_360 = etl.run()
```

---

## Key Takeaways

✅ **Always coalesce before writing** - Reduces output files, faster reads
✅ **Partition by date/key** - Enables partition pruning, 10x faster queries
✅ **Use broadcast for small tables** - No shuffle, massive speed boost
✅ **Filter early** - Predicate pushdown reduces data
✅ **Check explain()** - See what Catalyst is doing
✅ **Avoid UDFs** - 100-1000x slower than native functions
✅ **Handle errors** - Retry logic for S3, validation checks
✅ **Test incrementally** - Build ETL in steps, validate each stage

---

## Quick Reference: Common ETL Operations

```python
# Read various formats
df_parquet = spark.read.parquet("s3://bucket/data/")
df_csv = spark.read.csv("s3://bucket/data.csv", header=True, inferSchema=True)
df_json = spark.read.json("s3://bucket/data.json")

# Basic transforms
df = df.filter(col("amount") > 0)
df = df.select("name", "age", "salary")
df = df.withColumn("new_col", col("salary") * 1.1)
df = df.withColumnRenamed("old_name", "new_name")
df = df.drop("unwanted_column")

# Aggregations
df.groupBy("category").sum()
df.groupBy("category").agg(F.sum("sales"), F.avg("price"))

# Joins
df1.join(df2, "id", "inner")
df1.join(broadcast(df2), "id", "left")

# Write
df.write.mode("overwrite").parquet("s3://output/")
df.coalesce(10).write.parquet("s3://output/")  # Optimized
df.write.partitionBy("year").parquet("s3://output/")  # Partitioned

# Check your work
df.show()
df.printSchema()
df.explain()
```

---

**Remember:** Start simple, validate data at each stage, and optimize for readability first. Performance tuning comes after you have working code!
