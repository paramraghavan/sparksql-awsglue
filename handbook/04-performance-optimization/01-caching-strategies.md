# Caching Strategies for Performance Optimization

## Overview

Caching is one of the most effective ways to speed up Spark jobs, but misused caching can cause OOM errors and memory pressure. This guide covers when, how, and when NOT to cache.

---

## Quick Rule of Thumb

| Scenario | Cache? | Reason |
|----------|--------|--------|
| DataFrame used 2+ times | ✅ Yes | Avoid recomputation |
| DataFrame used once | ❌ No | Wasted memory |
| Small intermediate tables | ✅ Yes | Low memory cost, high reuse benefit |
| Large tables with single use | ❌ No | High memory cost, single compute |
| Before expensive ML operations | ✅ Yes | ML operations need stable data |
| Before I/O operations | ⚠️ Conditional | Only if reused multiple times |

---

## Caching for ML Workloads

### Problem: ML Operations without Caching

When running ML algorithms (e.g., Generalized Linear Regression via MLlib) on PySpark DataFrames:
- Without caching: each model fit triggers full DataFrame recomputation
- With large DataFrames (100M+ rows): recomputation is extremely slow
- Memory allocated inefficiently across iterations

### Solution: Cache Before ML Operations

```python
from pyspark.sql import SparkSession
from pyspark.ml.regression import GeneralizedLinearRegression
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("MLWorkload").getOrCreate()

# Read large DataFrame
df = spark.read.parquet("s3://bucket/large_data.parquet")

# CACHE BEFORE ML OPERATIONS
df.cache()

# Trigger cache materialization (forces computation)
df_count = df.count()
print(f"Cached DataFrame with {df_count} rows")

# Now run ML operations - these will be MUCH faster
glr = GeneralizedLinearRegression(labelCol="target", featuresCol="features")
model = glr.fit(df)  # Uses cached data, not recomputing from S3

# Get predictions - also fast because data is cached
predictions = model.transform(df)
predictions.show()

# When done, unpersist to free memory
df.unpersist()
```

### Why This Works

1. **First `.cache()` call**: Registers the DataFrame for caching
2. **`.count()` action**: Forces Spark to compute and store in memory
3. **Subsequent operations**: Use in-memory cached data instead of re-reading S3
4. **ML model fitting**: No longer bottlenecked by S3 I/O

### Performance Impact (Real Example)

**Without caching:**
- DataFrame count: 20 seconds (S3 read + parse)
- Model fit: 5 minutes (re-reads data from S3 multiple times)
- Transform: 3 minutes (re-reads data)
- **Total: ~8.5 minutes**

**With caching:**
- DataFrame count + cache: 25 seconds (S3 read + materialize to memory)
- Model fit: 30 seconds (uses cached data)
- Transform: 10 seconds (uses cached data)
- **Total: ~65 seconds (12.3x faster!)**

---

## Cache Storage Levels

### Default: MEMORY_ONLY
```python
df.cache()  # Equivalent to df.persist(StorageLevel.MEMORY_ONLY)
```
- Stores in memory
- Fast access
- **Risk**: If data doesn't fit, Spark drops partitions and recomputes on demand (slow)

### With Disk Fallback: MEMORY_AND_DISK
```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
```
- Stores in memory first
- Spills to disk if memory full
- **Trade-off**: Slower than pure memory, but more reliable than recomputation

### With Compression: MEMORY_ONLY_SER
```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY_SER)
```
- Compresses serialized data in memory
- Uses 2-3x less memory
- Slightly slower CPU due to deserialization
- **Best for**: Large DataFrames that must fit in memory

### For ML Workloads (Recommended)
```python
from pyspark.storagelevel import StorageLevel

# Compress to fit more data in memory
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.count()  # Materialize

# Your ML operations here...

df.unpersist()
```

---

## When NOT to Cache

### 1. One-Off Transformations
```python
# ❌ Bad: Cache single-use DataFrame
result = df.filter(df.age > 30).cache()
final = result.select("name", "age")
result.unpersist()

# ✅ Good: No cache needed
result = df.filter(df.age > 30).select("name", "age")
```

### 2. Memory is Tight
```python
# ❌ Bad: With 99% memory usage, caching causes OOM
if memory_usage > 90:
    df.cache()  # Will cause OOM

# ✅ Good: Check memory first
if memory_usage < 60:
    df.cache()
else:
    # Use intermediate checkpoints to S3 instead
    df.repartition(100).write.parquet("s3://temp/checkpoint/")
    df = spark.read.parquet("s3://temp/checkpoint/")
```

### 3. Large Tables with No Reuse
```python
# ❌ Bad: Wastes 50GB of RAM for single computation
large_df.cache()
result = large_df.groupBy("id").count()
large_df.unpersist()

# ✅ Good: Let Spark handle intermediate results
result = large_df.groupBy("id").count()
```

---

## Best Practices

### 1. Always Materialize After Caching
```python
# ❌ Incomplete: Cache registered but not materialized
df.cache()
# ... code continues, but cache might not be ready

# ✅ Complete: Explicitly materialize
df.cache()
_ = df.count()  # Force materialization
```

### 2. Unpersist When Done
```python
df.cache()
result = df.filter(df.age > 30)  # Reuses cached data
df.unpersist()  # Free memory for other operations
```

### 3. Monitor Cached Data
```python
# Check what's cached in the session
spark.catalog.listCachedTables().show(truncate=False)

# Estimate memory usage
df_rdd = df.rdd
print(f"DataFrame memory estimate: {df_rdd.getNumPartitions()} partitions")

# Clear all cache if needed
spark.catalog.clearCache()
```

### 4. For ML: Cache Raw Data, Not Transformed
```python
# ❌ Less efficient: Cache after transformations
df = spark.read.parquet("s3://data.parquet")
df_scaled = df.withColumn("age_scaled", df.age / 100)
df_scaled.cache()
model = glr.fit(df_scaled)

# ✅ More efficient: Cache raw data, transform reused
df = spark.read.parquet("s3://data.parquet")
df.cache()
df.count()

df_scaled = df.withColumn("age_scaled", df.age / 100)
model = glr.fit(df_scaled)  # Uses cached df, builds scaled version on-the-fly
df.unpersist()
```

---

## Case Study: ML Workload

### Before (Slow)
```python
# Each operation re-reads from S3
df = spark.read.parquet("s3://large_training_data.parquet")

# 1. Feature engineering (reads S3)
df_features = df.select("id", "target", "feature1", "feature2")
print(f"Records: {df_features.count()}")  # S3 read #1

# 2. Model training (reads S3)
glr = GeneralizedLinearRegression(labelCol="target", featuresCol="features")
model = glr.fit(df_features)  # S3 read #2 (maybe more internally)

# 3. Predictions (reads S3)
predictions = model.transform(df_features)  # S3 read #3
predictions.show(5)
```
**Problem**: Each action (count, fit, transform) re-reads 100GB from S3

### After (Fast)
```python
# Single read, reused many times
df = spark.read.parquet("s3://large_training_data.parquet")

# Cache and materialize
df.cache()
df.count()  # Reads S3 ONCE and keeps in memory

# Now all operations reuse cached data (no S3 reads)
df_features = df.select("id", "target", "feature1", "feature2")

glr = GeneralizedLinearRegression(labelCol="target", featuresCol="features")
model = glr.fit(df_features)  # Uses cached df

predictions = model.transform(df_features)  # Uses cached df
predictions.show(5)

df.unpersist()  # Free the 100GB when done
```
**Result**: 10-20x faster for multi-step ML workflows

---

## Troubleshooting Cache Issues

### Issue: OOM After Caching
```python
# Symptom: java.lang.OutOfMemoryError after df.cache()

# Solution 1: Use compressed serialization
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.count()

# Solution 2: Checkpoint to disk instead
df.repartition(100).write.parquet("s3://checkpoint/")
df = spark.read.parquet("s3://checkpoint/")

# Solution 3: Cache smaller subset
subset = df.limit(1000000)  # Sample instead of full
subset.cache()
subset.count()
```

### Issue: Cache Not Being Used (Still Reading S3)
```python
# Symptom: cache() called but operations still hitting S3

# Possible cause 1: Didn't materialize
# ❌ Wrong
df.cache()
# ... operations here might not have triggered materialization

# ✅ Right
df.cache()
_ = df.count()  # Explicit materialization

# Possible cause 2: Lost reference
# ❌ Wrong
def process_data():
    df = spark.read.parquet("s3://data.parquet")
    df.cache()
    return df.select("col1")

result = process_data()  # df goes out of scope after function

# ✅ Right: Materialize before returning
def process_data():
    df = spark.read.parquet("s3://data.parquet")
    df.cache()
    _ = df.count()  # Materialize while df is in scope
    return df.select("col1")
```

---

## Related Topics

- [Data Skew Problems](../07-troubleshooting/03-skew-problems.md) - Caching can help with skew
- [Performance Degradation](../07-troubleshooting/05-performance-degradation.md) - Monitor cache effectiveness
- [Memory Errors](../07-troubleshooting/02-memory-errors.md) - OOM prevention with caching

---

**Last Updated**: March 2026
