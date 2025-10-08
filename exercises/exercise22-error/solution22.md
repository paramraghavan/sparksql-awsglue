`TASK_WRITE_FAILED` error when writing a complex dataframe to S3 is typically caused by data or partitioning issues
rather than S3 connectivity (since simple writes work). Here are the most likely causes and solutions:

## Common Causes & Solutions

### 1. **Data Skew / Partition Size Issues**

After complex transformations, data may be unevenly distributed:
> Try the repartition approach first - it often resolves these issues with complex dataframes.

```python
# Check partition distribution
df.groupBy(spark.spark_partition_id()).count().show()

# Repartition before writing
df.repartition(200).write.mode("overwrite").parquet("s3://...")

# Or use coalesce if reducing partitions
df.coalesce(50).write.mode("overwrite").parquet("s3://...")
```

### 2. **Null Values in Non-Nullable Columns**

Complex operations can introduce nulls where parquet doesn't expect them:

```python
# Check for nulls
from pyspark.sql.functions import col, sum as _sum

df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# Fill nulls before writing
df.na.fill({"column_name": "default_value"}).write.parquet("s3://...")
```
[dataframe_fill_values.md](dataframe_fill_values.md)
ß
### 3. **Schema Issues (Nested/Complex Types)**

```python
# Print schema to check for complex nested structures
df.printSchema()

# Flatten or simplify if needed
# Or try different write options:
df.write
    .option("compression", "snappy")
    .option("maxRecordsPerFile", 100000)
    .mode("overwrite")
    .parquet("s3://...")
```

### 4. **Memory Issues**

```python
# Persist/cache before writing if dataframe is recomputed
df.persist()
df.write.parquet("s3://...")
df.unpersist()

# Or break into smaller chunks
df.write.partitionBy("date_column").parquet("s3://...")
```

### 5. **Enable Better Error Logging**

```python
# Add this to see the actual data causing issues
df.write
    .option("spark.sql.files.ignoreMissingFiles", "true")
    .option("spark.sql.adaptive.enabled", "false")
    .mode("overwrite")
    .parquet("s3://...")
```

## Quick Debugging Steps

1. **Sample the data first:**

```python
df.limit(1000).write.mode("overwrite").parquet("s3://test_path/")
```

2. **Check specific partition causing issues:**

```python
# Task 107 failed - try to isolate that partition
df.write.partitionBy("some_column").parquet("s3://...")
```

3. **Try writing to local/HDFS first** to rule out S3:

```python
df.write.parquet("/tmp/test_parquet/")
```

**What would help narrow this down:**

- Does `df.limit(1000).write.parquet(...)` work?
- What's the schema? (`df.printSchema()`)
- How many rows/partitions? (`df.count()`, `df.rdd.getNumPartitions()`)

