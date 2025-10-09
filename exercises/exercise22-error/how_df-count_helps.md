Why `df.count()` after `df.cache()` is important:

## Understanding Spark's Lazy Evaluation

Spark transformations are **lazy** - they don't execute immediately. They only execute when an **action** is triggered.

```python
# These DON'T actually execute yet (lazy transformations)
df = df.filter(col("amount") > 100)
df = df.join(other_df, "id")
df = df.groupBy("category").agg(sum("amount"))

# cache() just MARKS the dataframe to be cached, but doesn't compute it yet
df.cache()  # ← Nothing happens yet!

# count() is an ACTION - NOW everything executes and gets cached
df.count()  # ← This actually computes and caches the data
```

## Why This Helps with Parquet Writing

### Without Cache + Count:

```python
df.cache()
df.write.parquet("s3://bucket/path/")  # ← Computes transformations during write
```

**Problem:** Your complex transformations are computed **during the write operation**. If they fail or are unstable, the
write fails at Task 107 (or wherever).

### With Cache + Count:

```python
df.cache()
df.count()  # ← Compute and cache NOW, separately from write
df.write.parquet("s3://bucket/path/")  # ← Just reads from cache
```

**Benefit:**

1. Transformations are computed **upfront** and stored in memory/disk
2. If transformations fail, you see the error **before** attempting to write
3. Write operation just reads stable, pre-computed data from cache
4. Subsequent operations (like multiple writes) reuse the same cached data

## Real-World Example

```python
# Complex modeling operations
df = raw_df
    .join(features_df, "id")
    .join(scores_df, "id")
    .withColumn("complex_calc", some_udf(col("data")))
    .groupBy("segment").agg(...)  # Many transformations

# WITHOUT caching
df.write.parquet("s3://path1/")  # Computes everything
df.write.parquet("s3://path2/")  # Computes AGAIN (redundant!)
# If it fails at Task 107, you don't know if issue is in transformations or write

# WITH caching
df.cache()
df.count()  # Force computation NOW - if this fails, you know transformations are bad
# Success? Great! Now data is in cache

df.write.parquet("s3://path1/")  # Fast! Reads from cache
df.write.parquet("s3://path2/")  # Also fast! Reuses cache
# If write fails, you know it's a write-specific issue, not transformation issue
```

## What Actually Gets Cached

```python
df.cache()
print(df.rdd.getNumPartitions())  # This is an action, forces cache
# OR
df.count()  # This is an action, forces cache
# OR  
df.show(5)  # This is an action, forces cache

# Now the RESULTS of all transformations are stored in memory/disk
# Subsequent operations are much faster and more stable
```

## Memory Considerations

```python
# Check if data fits in memory
df.cache()
df.count()

# Check cache status
print(spark.sparkContext._jsc.sc().getRDDStorageInfo())

# If too large for memory, use disk
df.persist(StorageLevel.DISK_ONLY)
df.count()

# Or memory + disk
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()
```

## When This Helps Most

✅ **Helps when:**

- Complex transformations (joins, aggregations, UDFs)
- Multiple operations on same dataframe
- Unstable/flaky transformations
- Need to isolate transformation errors from write errors

❌ **Doesn't help when:**

- Simple dataframes (no transformations)
- Data is too large for memory/disk
- Issue is actually in the write operation itself (S3 permissions, etc.)

## Complete Pattern

```python
# 1. Build your complex dataframe
df = complex_transformations(raw_df)

# 2. Cache and materialize
df.cache()
row_count = df.count()  # Forces cache + gives you row count
print(f"Cached {row_count} rows in {df.rdd.getNumPartitions()} partitions")

# 3. Now write is more stable
df.write.mode("overwrite").parquet("s3://bucket/path/")

# 4. Clean up when done
df.unpersist()
```

**Bottom line:** `df.count()` forces Spark to compute your transformations **immediately** and store the results, making
subsequent operations (like writing) more stable and faster because they work with pre-computed, cached data rather than
recomputing on-the-fly.