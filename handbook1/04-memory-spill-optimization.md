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

### Executor Memory Breakdown (4GB Example)

```
Total Executor Memory: 4GB
│
├─ Execution Memory (1.5GB): Shuffle, joins, aggregations
│  └─ Spill threshold: ~1.5GB
│
├─ Storage Memory (1.5GB): Caching, broadcast variables
│
└─ Reserved for Python/OS (1GB)
    └─ Garbage collection, OS overhead
```

### How It Works

```
DataFrame operation: df.groupBy("id").sum()
│
├─ Data flows into executor partition
│  └─ Using Execution Memory
│
├─ If data > 1.5GB
│  └─ Spill triggered: Excess data → disk
│
└─ Result: Partial in-memory, partial on-disk
   └─ Slower execution!
```

### Visual: The Cutting Board Analogy

```
Your kitchen cutting board = 4GB executor memory

Task: Prepare ingredients for 100 dishes
├─ Each dish needs 50MB of prepared ingredients
├─ Board can hold: 80 dishes worth (4GB ÷ 50MB)
│
└─ What if you need to prep 150 dishes?
   ├─ Prep 80 dishes on board
   ├─ Move 80 dishes to storage (slow!)
   ├─ Continue prepping remaining 70
   └─ Result: Constant movement to/from storage
      (This is SPILL - very slow!)
```

---

## Understanding Spill

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

### Example 1: GroupBy Spill Prevention

```python
# Scenario: Daily sales aggregation
# Data: 500GB raw transactions
# Problem: GroupBy user_id creates 10M partitions, needs 20GB memory

df_sales = spark.read.parquet("s3://data/transactions/")

# Solution: Pre-filter + repartition + reduce shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 100)

df_filtered = df_sales.filter(
    (df_sales.date >= "2024-01-01") &
    (df_sales.amount > 0)  # Pre-filter
).coalesce(200)  # Reduce partitions before groupBy

result = df_filtered.groupBy("user_id", "date").agg(
    F.sum("amount").alias("daily_total"),
    F.count("*").alias("transaction_count")
)

result.write.mode("overwrite").parquet(
    "s3://output/daily_summary"
)

# Memory impact:
# Before: 20GB needed
# After: 2GB needed (10x improvement!)
```

### Example 2: Join Optimization

```python
# Scenario: Join customer data with transactions
# Problem: Both tables shuffle, need 16GB memory per executor

from pyspark.sql.functions import broadcast

customers = spark.read.parquet("s3://data/customers/")  # 1GB
transactions = spark.read.parquet("s3://data/transactions/")  # 500GB

# Check sizes
print(f"Customers: {customers.count()} rows")  # 10M
print(f"Transactions: {transactions.count()} rows")  # 5B

# Solution: Broadcast customers (it's small)
result = transactions.join(
    broadcast(customers),  # Broadcast the 1GB table
    "customer_id"
)

# Memory impact:
# Before: Both shuffle → 500GB + 1GB in executors
# After: Only transactions shuffle → 500GB in executors
#        Customers cached on each executor (1GB)
# Savings: ~50% memory usage!
```

### Example 3: Handling Data Skew

```python
# Scenario: Some users have 100x more data than others
# Problem: Executor with skewed data needs 16GB, others only use 100MB

df = spark.read.parquet("s3://data/user_events/")

# Before: Uneven distribution
# User 1: 100GB
# User 2: 50GB
# User 3: 1GB
# User 4: 1GB
# (Heavy users cause spill, light users idle)

# Solution 1: Salt (add random suffix to heavy keys)
from pyspark.sql.functions import col, concat, lit, rand

df_salted = df.withColumn(
    "user_id_salted",
    when(
        col("user_id").isin(["heavy_user_1", "heavy_user_2"]),
        concat(col("user_id"), lit("_"), (rand() * 10).cast("int"))
    ).otherwise(col("user_id"))
)

result = df_salted.groupBy("user_id_salted").agg(
    F.sum("value").alias("total")
)

# Solution 2: Isolate heavy keys
df_heavy = df.filter(df.user_id.isin(["heavy_user_1", "heavy_user_2"]))
df_light = df.filter(~df.user_id.isin(["heavy_user_1", "heavy_user_2"]))

result_heavy = df_heavy.repartition(50, "user_id").groupBy("user_id").agg(F.sum("value"))
result_light = df_light.groupBy("user_id").agg(F.sum("value"))

result = result_heavy.union(result_light)

# Memory impact:
# Before: One executor struggles
# After: Work distributed evenly
# Result: No spill, 10x faster!
```

### Example 4: Window Function Optimization

```python
# Scenario: Calculate running sum per user
# Problem: Window function needs to sort entire partition

df = spark.read.parquet("s3://data/events/")

# Before: Window function causes spill
from pyspark.sql.window import Window

result = df.withColumn(
    "running_sum",
    F.sum("amount").over(
        Window.partitionBy("user_id").orderBy("timestamp")
    )
)

# After: Pre-sort, reduce shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 50)

df_presorted = df.repartition("user_id").sortWithinPartitions("timestamp")

result = df_presorted.withColumn(
    "running_sum",
    F.sum("amount").over(
        Window.partitionBy("user_id").orderBy("timestamp")
    )
)

# Memory impact:
# Before: Each window calculation needs sorting
# After: Already sorted within partitions
# Result: Less memory needed, faster execution!
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
