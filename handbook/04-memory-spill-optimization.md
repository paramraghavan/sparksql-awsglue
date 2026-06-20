# 04 - Memory Spill & Optimization

## The Memory Problem

**Scenario:** You have 4GB executor memory. Your transformation needs 10GB to hold all data.

**What happens?** Spark spills excess data to disk. Disk is 100-1000x slower than RAM.

**Real Impact:** Job that should finish in 5 minutes takes 45 minutes due to spill.

---

## Table of Contents
1. [Memory Architecture](#memory-architecture)
2. [Understanding Spill](#understanding-spill)
3. [Diagnosing Spill](#diagnosing-spill)
4. [Prevention Strategies](#prevention-strategies)
5. [Real Examples](#real-examples)

---

## Memory Architecture

**Executor Memory Breakdown (4GB typical):**
- **Execution Memory (1.5GB):** Used for shuffle, joins, aggregations. Spill triggered when exceeded.
- **Storage Memory (1.5GB):** Used for caching and broadcast variables.
- **Reserved (1GB):** Garbage collection and OS overhead.

**Visual Analogy:** Think of execution memory like a kitchen cutting board (1.5GB). You're prepping 150 dishes but the board fits 80. You prep 80, move them to storage (disk - slow!), prep 70 more, move those too. Constant movement = SPILL = 100-1000x slower.

**When it happens:** If operation (groupBy, join, etc.) tries to hold more data than execution memory, excess data spilled to disk.

---

## Understanding Spill

🔗 **Related:** Section 02 - Python Data Structures | Section 05 - Shuffle Optimization

### When Spill Happens

```python
# Scenario 1: GroupBy (high cardinality)
df.groupBy("user_id").sum()  # If 1M unique users, lots of memory needed

# Scenario 2: Join without broadcast
df_large.join(df_medium, "key")  # Both tables shuffle

# Scenario 3: OrderBy / Sort
df.orderBy("timestamp")  # Sorts entire partition

# Scenario 4: Window Functions
df.withColumn("rank", row_number().over(Window.orderBy("date")))
```

### Spill Types

```
EXECUTION SPILL (common)
├─ During: groupBy, join, orderBy
├─ Cause: Not enough execution memory
└─ Impact: 10-100x slower

STORAGE SPILL (less common)
├─ During: Caching with cache()
├─ Cause: Cache exceeds storage memory
└─ Impact: Cached data becomes disk-resident
```

---

## Diagnosing Spill

### Method 1: Look at Logs

```bash
# SSH into EMR master
aws emr ssh --cluster-id j-xxx -i key.pem

# View executor logs
tail -f /mnt/var/log/spark/apps/spark-*.log | grep -i spill

# Output:
# 2024-01-15 10:23:45 WARN ShuffleExchangeExec: Spilling
# 2024-01-15 10:23:45 INFO ShuffleBlockFetcherIterator: Spilling
```

### Method 2: Spark UI

```
1. Open browser: http://<master-ip>:8080/
2. Find your application
3. Click "Stages" tab
4. Look for stages with high "Shuffle Write" size
5. If "Shuffle Write" >> "Shuffle Read", likely spill
```

### Method 3: Metrics in Code

```python
# Get partition size statistics
def get_partition_stats(partition):
    count = 0
    size_estimate = 0
    for record in partition:
        count += 1
        # Rough estimate: Python object is ~50 bytes + data
        size_estimate += len(str(record))

    print(f"Partition: {count} records, ~{size_estimate/1024/1024}MB")
    return partition

df.rdd.mapPartitions(get_partition_stats).collect()
```

### Method 4: Check Task Metrics

```python
# After job completes
spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()

# Shows executor memory usage
# If "memory used" ~= "memory allocated", likely spill happening
```

---

## Prevention Strategies

### Strategy 1: Increase Executor Memory

**Simplest solution (if budget allows)**

```bash
# Before: 4GB executor memory → lots of spill
spark-submit \
  --executor-memory 4G \
  --num-executors 10 \
  script.py

# After: 16GB executor memory → no spill
spark-submit \
  --executor-memory 16G \
  --num-executors 10 \
  script.py

# Cost: Same 10 executors, just larger instances
# Trade-off: More expensive, but 10x faster
```

### Strategy 2: Reduce Partition Size (Pre-filter)

**Filter before expensive operations**

```python
# BAD: Process 100GB, then aggregate
df_100gb = spark.read.parquet("huge_file.parquet")
result = df_100gb.groupBy("category").sum()
# Each partition: ~100MB data → needs ~800MB memory
# With 10 partitions: ~8GB needed!

# GOOD: Filter first (50GB → much smaller partitions)
df_50gb = df_100gb.filter(df.year == 2024)
result = df_50gb.groupBy("category").sum()
# Each partition: ~50MB data → needs ~400MB memory
# Much safer!
```

### Strategy 3: Repartition for Even Distribution

```python
# BAD: Data skewed - some partitions huge
df = spark.read.parquet("data.parquet")
# Partition 1: 100MB
# Partition 2: 500MB  ← This executor needs 4GB memory for spill!
# Partition 3: 50MB

# GOOD: Repartition to spread evenly
df_repart = df.repartition(100)  # Redistribute to 100 partitions
# Each partition: ~10MB
# Much safer!
```

### Strategy 4: Use Broadcast for Small Tables

```python
# BAD: Normal join (both shuffle)
result = df_large.join(df_medium, "key")

# GOOD: Broadcast small table
from pyspark.sql.functions import broadcast

result = df_large.join(broadcast(df_medium), "key")
# df_medium cached on each executor (no shuffle!)
# Only df_large shuffles → 50% memory usage
```

### Strategy 5: Reduce Shuffle Partitions

```python
# Default: 200 shuffle partitions (for wide operations)
# For small jobs: Too many partitions, each gets tiny data

spark.conf.set("spark.sql.shuffle.partitions", 50)  # Reduce partitions

# Effect:
# - Fewer, larger partitions
# - Less overhead
# - Better executor memory usage
```

### Strategy 6: Use Columnar Storage Format

```python
# BAD: CSV → More memory overhead
df = spark.read.csv("data.csv")

# GOOD: Parquet → More memory efficient
df = spark.read.parquet("data.parquet")

# Better: Orc (even more compression)
df = spark.read.orc("data.orc")

# Result: 10-50% less memory usage for same data
```

---

## Real Examples

### Example 1: GroupBy + Pre-Filter + Partition Tuning

```python
df = spark.read.parquet("s3://data/transactions/")  # 500GB

# Problem: GroupBy on large partition needs 20GB executor memory
# Solution: Pre-filter + reduce partitions + tune shuffle

spark.conf.set("spark.sql.shuffle.partitions", 100)  # Reduce from default 200

result = df\
    .filter((col("date") >= "2024-01-01") & (col("amount") > 0))\
    .coalesce(200)\
    .groupBy("user_id", "date")\
    .agg(F.sum("amount"), F.count("*"))

# Memory: 20GB → 2GB (10x improvement!)
```

### Example 2: Broadcast Join

```python
from pyspark.sql.functions import broadcast

customers = spark.read.parquet("s3://data/customers/")  # 1GB
transactions = spark.read.parquet("s3://data/transactions/")  # 500GB

# Problem: Normal join shuffles both tables
# Solution: Broadcast small table to all executors

result = transactions.join(
    broadcast(customers),
    "customer_id"
)

# Memory: Customers cached on each executor (1GB)
#         Transactions NOT shuffled
# Savings: ~50% memory vs. normal join
```

---

## Monitoring & Tuning Checklist

```
□ Check executor logs for spill warnings
□ Monitor Spark UI for shuffle metrics
□ Measure partition sizes
□ Test with different executor memory sizes
□ Compare before/after execution times

□ Pre-filter data before expensive operations
□ Use broadcast for tables < 1GB
□ Repartition to fix data skew
□ Reduce shuffle partitions for small jobs
□ Increase executor memory if budget allows
```

---

## Key Takeaways

✅ **Spill happens when data > execution memory** - Slow execution
✅ **Diagnosis:** Check logs, monitor Spark UI, measure partitions
✅ **Prevention:** Pre-filter, broadcast, repartition, reduce partitions
✅ **Increase memory carefully** - More cost, but sometimes necessary
✅ **Monitor production** - Catch spill early before impacting SLAs

---

## Next Steps

1. **Check your current jobs** - Are they spilling?
2. **Apply prevention strategies** - Start with pre-filtering
3. **Measure impact** - Compare before/after execution times
4. **Move to Section 05** - Shuffle optimization in detail

---

**Remember:** A job that spills to disk is telling you something is wrong. Listen to it, diagnose, and fix the root cause!
