# If `df.na.fill()` didn't help, the issue is likely **data skew, memory, or corrupt data**. 

Here are other fixes to try:

## 1. **Repartition Before Writing** (Most Common Fix)

```python
# Try different partition counts
df.repartition(200).write.mode("overwrite").parquet("s3://bucket/path/")

# Or coalesce if you have too many partitions
df.coalesce(50).write.mode("overwrite").parquet("s3://bucket/path/")

# Check current partitions first
print(f"Current partitions: {df.rdd.getNumPartitions()}")
print(f"Rows per partition: {df.count() / df.rdd.getNumPartitions()}")
```

## 2. **Limit Records Per File**

```python
# Prevent any single file from getting too large
df.write \
  .option("maxRecordsPerFile", 50000) \
  .mode("overwrite") \
  .parquet("s3://bucket/path/")
```

## 3. **Cache/Persist Before Writing**

```python
# If dataframe has complex transformations, cache it first
df.cache()
df.count()  # Force cache to materialize

df.write.mode("overwrite").parquet("s3://bucket/path/")

df.unpersist()  # Clean up after
```

## 4. **Change Compression Codec**

```python
# Try different compression (default is snappy)
df.write \
  .option("compression", "none") \
  .mode("overwrite") \
  .parquet("s3://bucket/path/")

# Or try gzip (slower but more compatible)
df.write \
  .option("compression", "gzip") \
  .mode("overwrite") \
  .parquet("s3://bucket/path/")
```

## 5. **Find and Remove Corrupt Rows**

```python
# Sample to find problematic data
try:
    # Write in small batches to isolate bad data
    total_rows = df.count()
    batch_size = 10000
    
    for i in range(0, total_rows, batch_size):
        print(f"Writing rows {i} to {i+batch_size}...")
        df.limit(i+batch_size).subtract(df.limit(i)) \
          .write.mode("append").parquet(f"s3://bucket/path/batch_{i}/")
except Exception as e:
    print(f"Failed at row {i}: {e}")
```

## 6. **Check for Special Characters in String Columns**

```python
from pyspark.sql.functions import regexp_replace, col

# Clean all string columns
string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

for col_name in string_cols:
    df = df.withColumn(
        col_name,
        regexp_replace(col(col_name), "[\x00-\x1F\x7F]", "")  # Remove control chars
    )

df.write.mode("overwrite").parquet("s3://bucket/path/")
```

## 7. **Write with Explicit Schema (Avoid Schema Inference)**

```python
# Sometimes mixed types cause issues
# Cast columns explicitly
df_fixed = df.select(
    col("col1").cast("string"),
    col("col2").cast("int"),
    col("col3").cast("double"),
    # ... all columns with explicit types
)

df_fixed.write.mode("overwrite").parquet("s3://bucket/path/")
```

## 8. **Increase Executor Resources**

```python
# If it's a memory issue, increase executor memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Or reduce memory pressure
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## 9. **Write to Local/HDFS First (Isolate S3 Issues)**

```python
# Test if it's S3-specific
df.write.mode("overwrite").parquet("/tmp/test_parquet/")

# If this works, might be S3 permissions or config
# Check S3 settings:
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
```

## 10. **Break Into Smaller Writes**

```python
# Write one partition at a time (slower but reliable)
from pyspark.sql.functions import spark_partition_id

num_partitions = df.rdd.getNumPartitions()

for partition_id in range(num_partitions):
    print(f"Writing partition {partition_id}...")
    try:
        df.filter(spark_partition_id() == partition_id) \
          .write.mode("append").parquet(f"s3://bucket/path/part_{partition_id}/")
    except Exception as e:
        print(f"Failed on partition {partition_id}: {e}")
        # Investigate this partition specifically
        bad_data = df.filter(spark_partition_id() == partition_id)
        bad_data.show(5, truncate=False)
```

## 11. **Complete Diagnostic & Fix Script**

```python
def safe_write_parquet(df, s3_path):
    """
    Try multiple strategies to write parquet safely
    """
    
    print("=== Starting safe write process ===")
    
    # Strategy 1: Repartition + maxRecordsPerFile
    try:
        print("Strategy 1: Repartition with record limit...")
        df.repartition(200) \
          .write \
          .option("maxRecordsPerFile", 50000) \
          .option("compression", "snappy") \
          .mode("overwrite") \
          .parquet(s3_path)
        print("✓ Success!")
        return
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # Strategy 2: Cache + different compression
    try:
        print("\nStrategy 2: Cache + no compression...")
        df.cache()
        df.count()
        df.repartition(100) \
          .write \
          .option("compression", "none") \
          .mode("overwrite") \
          .parquet(s3_path)
        df.unpersist()
        print("✓ Success!")
        return
    except Exception as e:
        print(f"✗ Failed: {e}")
        df.unpersist()
    
    # Strategy 3: Write to temp local, then copy
    try:
        print("\nStrategy 3: Write local first...")
        temp_path = "/tmp/temp_parquet/"
        df.write.mode("overwrite").parquet(temp_path)
        # Then copy to S3 (using hadoop fs commands or boto3)
        print("✓ Success writing locally! Now copy to S3 manually")
        return
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # Strategy 4: Find bad partition
    print("\nStrategy 4: Isolating bad partitions...")
    num_parts = df.rdd.getNumPartitions()
    for i in range(num_parts):
        try:
            df.filter(spark_partition_id() == i) \
              .write.mode("append").parquet(f"{s3_path}_part_{i}/")
            print(f"✓ Partition {i} OK")
        except Exception as e:
            print(f"✗ Partition {i} FAILED: {e}")
            print("Showing sample of bad partition:")
            df.filter(spark_partition_id() == i).show(10, truncate=False)

# Usage
safe_write_parquet(df, "s3://bucket/path/")
```

## Quick Checklist to Try in Order:

1. ✅ `df.repartition(200).write.option("maxRecordsPerFile", 50000).parquet(...)`
2. ✅ `df.cache()` then `df.count()` then write
3. ✅ Change compression to `"none"` or `"gzip"`
4. ✅ Write to local `/tmp/` first to isolate S3 issues
5. ✅ Check if specific partitions fail using the partition-by-partition approach

**Which one should you try first?** Start with #1 (repartition + maxRecordsPerFile) - this fixes 80% of complex dataframe write issues.