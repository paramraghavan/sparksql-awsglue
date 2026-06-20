# 05 - Shuffle Optimization

## What is Shuffle?

**Definition:** Moving data between partitions/executors.

**When it happens:**
```
df.groupBy("id").sum()      # GroupBy needs all records with same id together
df.join(df2, "key")          # Join needs matching records on same executor
df.repartition(50)           # Repartition redistributes all data
df.orderBy("timestamp")      # OrderBy needs all data sorted
df.distinct()                # Distinct needs to check for duplicates
```

---

## Table of Contents
1. [Shuffle Mechanics](#shuffle-mechanics)
2. [Partition Sizing](#partition-sizing)
3. [Shuffle Tuning Parameters](#shuffle-tuning-parameters)
4. [Real Examples](#real-examples)

---

## Shuffle Mechanics

🔗 **Related:** Section 01 - Partitioning | Section 04 - Memory Spill | Section 06 - Join Strategies

### Two-Phase Shuffle

```
PHASE 1: SHUFFLE WRITE (on write executors)
│
├─ Step 1: Map each record to shuffle bucket
│  └─ Bucket = destination partition for that record
│
├─ Step 2: Sort records by key (within bucket)
│  └─ Writing sorted data to disk
│
└─ Step 3: Write sorted buckets to disk
   └─ Each executor writes multiple files (one per destination partition)

PHASE 2: SHUFFLE READ (on read executors)
│
├─ Step 1: Read shuffle files from write executors
│  └─ Fetch files for your assigned partitions
│
├─ Step 2: Merge-sort all fetched data
│  └─ Combine files from multiple write executors
│
└─ Step 3: Group by key (for groupBy) or match keys (for join)
   └─ Now data is ready for next operation
```

### Memory Usage During Shuffle

```
Write Phase (on each executor)
├─ Buffer size: spark.shuffle.file.buffer (default 32KB per output)
├─ If 200 shuffle partitions: 200 × 32KB = 6.4MB buffer per record
└─ Impact: Can grow quickly with many output partitions

Read Phase (on each executor)
├─ Fetch buffer: spark.reducer.maxSizeInFlight (default 48MB)
├─ Merge buffer: spark.task.maxMemory for each partition
└─ Impact: Many small fetches = overhead, few large fetches = memory spill
```

### Visual Example: 100 records, 4 output partitions

```
Original: [1,5,3,2,8,4,7,6,9,10, ...]
           ↓
Write Phase:
├─ Partition 0 (key%4==0): [4, 8, 12, ...]  → Sort & Write
├─ Partition 1 (key%4==1): [1, 5, 9, ...]   → Sort & Write
├─ Partition 2 (key%4==2): [2, 6, 10, ...]  → Sort & Write
└─ Partition 3 (key%4==3): [3, 7, 11, ...]  → Sort & Write
           ↓
Read Phase:
├─ Executor 0 fetches partition 0 data from all write executors
├─ Executor 1 fetches partition 1 data from all write executors
├─ Executor 2 fetches partition 2 data from all write executors
└─ Executor 3 fetches partition 3 data from all write executors
           ↓
Result: Each executor has all records for its partition, sorted by key
```

---

## Partition Sizing

### Default Shuffle Partitions

```python
# Spark default: 200 partitions for all wide operations
spark.conf.get("spark.sql.shuffle.partitions")  # Returns: "200"

# This causes problems:
# - Small jobs: 200 partitions × 128MB = 25.6GB (don't need!)
# - Large jobs: 200 partitions × 512MB = 102.4GB (too much!)
```

### Choosing Optimal Partitions

```python
import math

total_data_size = 100 * 1024  # 100GB in MB
target_partition_size = 128    # 128MB per partition (ideal)

optimal_partitions = math.ceil(total_data_size / target_partition_size)
print(optimal_partitions)  # ~800 partitions

# But also consider:
# - Executor cores available (avoid >2-4 partitions per core)
# - Network bandwidth (too many = overhead)
# - Operation type (groupBy vs join have different needs)
```

### Formula for Sizing

```python
# General formula:
num_shuffle_partitions = min(
    ceil(total_data / target_partition_size),  # Data-based
    num_executors * cores_per_executor * 2     # Executor-based
)

# Example: 500GB data, 10 executors (8 cores each)
data_based = ceil(500 * 1024 / 128)     # 4096 partitions
executor_based = 10 * 8 * 2             # 160 partitions
optimal = min(4096, 160)                # 160 partitions

# Result: 160 partitions = ~3.1GB per partition (manageable)
```

### Setting Shuffle Partitions

```python
# Session-wide configuration
spark.conf.set("spark.sql.shuffle.partitions", 100)

# Job-specific override
spark.conf.set("spark.sql.shuffle.partitions", 200)

# DataFrame-specific (less common)
df.repartition(150)  # Explicit repartition to 150
```

---

## Shuffle Tuning Parameters

### Key Parameters

```
spark.sql.shuffle.partitions (default 200)
├─ Number of output partitions for wide ops
├─ Rule: Max(executor cores × 2, data size / 128MB)
└─ Too high: Overhead; Too low: Memory spill

spark.shuffle.file.buffer (default 32KB)
├─ Buffer size per output partition during write
├─ Too high: Memory usage increases; Too low: Disk thrashing
└─ Typical: 32KB-1MB

spark.shuffle.compress (default true)
├─ Compress shuffle data on disk
├─ Tradeoff: 50% smaller files vs CPU cost
└─ Recommended: true (disk I/O is bottleneck)

spark.shuffle.spill.compress (default true)
├─ Compress spilled data to disk
├─ Similar tradeoff as shuffle.compress
└─ Recommended: true

spark.reducer.maxSizeInFlight (default 48MB)
├─ Max data fetched from map tasks simultaneously
├─ Too high: Memory usage increases; Too low: Network overhead
└─ Typical: 48MB-100MB

spark.shuffle.sort.bypassMergeThreshold (default 200)
├─ If num partitions < threshold, skip merge-sort
├─ Faster for small jobs
└─ Typical: 200
```

### Recommended Configuration

```python
# For general workloads
spark.conf.set("spark.sql.shuffle.partitions", 150)
spark.conf.set("spark.shuffle.file.buffer", "64k")
spark.conf.set("spark.shuffle.compress", True)
spark.conf.set("spark.shuffle.spill.compress", True)
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")

# For small jobs (< 10GB)
spark.conf.set("spark.sql.shuffle.partitions", 30)

# For large jobs (> 100GB)
spark.conf.set("spark.sql.shuffle.partitions", 500)
spark.conf.set("spark.reducer.maxSizeInFlight", "128m")
```

---

## Real Examples

### Example 1: GroupBy Shuffle Optimization

```python
# Scenario: Aggregate 500GB sales data by store

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("GroupByOpt").getOrCreate()

# Configuration for this job
spark.conf.set("spark.sql.shuffle.partitions", 200)

# Read data
df = spark.read.parquet("s3://data-lake/sales/")  # 500GB
print(f"Partitions: {df.rdd.getNumPartitions()}")  # ~4000 (from file split)

# Problematic groupBy (4000 → 200 shuffle partitions)
result = df.groupBy("store_id").agg(
    F.sum("amount").alias("total_sales"),
    F.count("*").alias("transaction_count")
)
# Shuffle impact: 4000 partitions → 200 (aggressive reduction)
# Each partition: ~2.5GB (may cause spill!)

# Optimized: Pre-filter, then groupBy
spark.conf.set("spark.sql.shuffle.partitions", 150)

df_filtered = df.filter(
    (df.date >= "2024-01-01") & (df.amount > 0)
).coalesce(800)  # Reduce from 4000 to 800 partitions

result = df_filtered.groupBy("store_id").agg(
    F.sum("amount").alias("total_sales"),
    F.count("*").alias("transaction_count")
)
# Shuffle impact: 800 partitions → 150
# Each partition: ~300MB (safe!)

# Write result
result.write.mode("overwrite").parquet("s3://output/daily_summary/")
```

### Example 2: Join Shuffle Optimization

```python
# Scenario: Join large transaction log with product catalog

from pyspark.sql.functions import broadcast

# Read datasets
transactions = spark.read.parquet("s3://data/transactions/")  # 100GB
products = spark.read.parquet("s3://data/products/")          # 500MB

# Check sizes
print(f"Transactions: {transactions.count()} rows")  # 1B
print(f"Products: {products.count()} rows")         # 100K

# Approach 1: Without broadcast (both shuffle)
spark.conf.set("spark.sql.shuffle.partitions", 200)

result = transactions.join(products, "product_id")
# Shuffle: Both tables → 200 partitions
# Data moved: 100GB + 500MB = 100.5GB (expensive!)

# Approach 2: With broadcast (only one shuffles)
result = transactions.join(broadcast(products), "product_id")
# Shuffle: Only transactions → 200 partitions
# Data moved: 100GB (50% reduction!)
# Products cached on each executor (~500MB per executor)
# Benefit: Faster join, less memory spill

# Approach 3: If products were large (10GB)
spark.conf.set("spark.sql.shuffle.partitions", 100)

# Use repartition to match partition count before join
products_repart = products.repartition(100)
transactions_repart = transactions.repartition(100)

result = transactions_repart.join(products_repart, "product_id")
# Both have 100 partitions before join
# Join happens locally per partition (no shuffle!)
# Fast and memory-efficient
```

### Example 3: Handling Shuffle Skew

```python
# Scenario: Some keys have 100x more data than others
# E.g., "USA" state has 100B records, others have 1B

df = spark.read.parquet("s3://data/events/")

# Problem: Uneven distribution after shuffle
# Partition 1: "USA" data (100B records)
# Partition 2: "UK" data (1B records)
# Partition 3: "DE" data (1B records)
# → Partition 1 executor needs 100GB memory, others need 1GB (imbalance!)

# Solution 1: Separate hot keys
df_hot = df.filter(df.country == "USA").repartition(50)
df_cold = df.filter(df.country != "USA")

result_hot = df_hot.groupBy("state").agg(F.sum("value"))
result_cold = df_cold.groupBy("country").agg(F.sum("value"))

result = result_hot.union(result_cold)

# Solution 2: Salt hot keys (add random suffix)
from pyspark.sql.functions import when, col, rand, concat, lit

df_salted = df.withColumn(
    "key_salted",
    when(col("country") == "USA",
         concat(col("state"), lit("_"), (rand() * 50).cast("int")))
    .otherwise(col("country"))
)

result = df_salted.groupBy("key_salted").agg(F.sum("value"))
# Each "USA_0" ... "USA_49" gets ~2B records
# Evenly distributed across 50 partitions!

# Solution 3: Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 500)  # Instead of 200
result = df.groupBy("country").agg(F.sum("value"))
# More partitions = data spread thinner
# But more overhead (use sparingly)
```

### Example 4: Optimal Shuffle Partition Sizing

```python
# Scenario: Multiple jobs, different data sizes
# We want to find optimal shuffle partitions for each

def calculate_optimal_partitions(
    total_data_gb,
    num_executors,
    cores_per_executor,
    target_partition_mb=128
):
    """Calculate optimal shuffle partitions"""
    import math

    # Data-based: How many partitions to fit target size
    total_mb = total_data_gb * 1024
    data_based = math.ceil(total_mb / target_partition_mb)

    # Executor-based: Avoid too many per executor
    executor_based = num_executors * cores_per_executor * 2

    # Final: Use conservative estimate
    optimal = max(
        30,  # Minimum
        min(data_based, executor_based)
    )

    return optimal

# Example 1: 100GB data, 5 executors, 8 cores each
opt1 = calculate_optimal_partitions(100, 5, 8)
print(f"100GB data: {opt1} partitions")  # 800

# Example 2: 10GB data, 5 executors, 8 cores each
opt2 = calculate_optimal_partitions(10, 5, 8)
print(f"10GB data: {opt2} partitions")  # 80

# Example 3: 500GB data, 20 executors, 8 cores each
opt3 = calculate_optimal_partitions(500, 20, 8)
print(f"500GB data: {opt3} partitions")  # 320 (limited by executor-based)

# Use in Spark
spark.conf.set("spark.sql.shuffle.partitions", opt1)
```

---

## Shuffle Monitoring Checklist

```
□ Check shuffle write/read sizes in Spark UI
□ Compare to expected data size
□ Look for skewed partition sizes
□ Monitor disk I/O during shuffle
□ Check for memory spill messages
□ Measure job execution time

□ Adjust spark.sql.shuffle.partitions
□ Consider pre-filtering or pre-sorting
□ Use broadcast for small tables
□ Investigate skewed keys
□ Compress shuffle data
```

---

## Key Takeaways

✅ **Shuffle moves data between executors** - Expensive operation
✅ **Partition size matters** - Too many = overhead, too few = memory spill
✅ **Default 200 partitions** - Often wrong (too many for small jobs, too few for large)
✅ **Broadcast small tables** - Avoid shuffle entirely
✅ **Detect and handle skew** - Separate hot keys or salt them
✅ **Monitor shuffle metrics** - Watch Spark UI for signs of problems

---

## Next Steps

1. **Check your shuffle partitions** - Are they optimal?
2. **Adjust for your workload** - Small jobs need fewer, large jobs need more
3. **Monitor and measure** - Before/after execution times
4. **Move to Section 06** - Join strategies in detail

---

**Remember:** Shuffle is expensive. Every optimization here saves seconds/minutes per job × hundreds of jobs = huge production impact!
