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

### Clarification: What are "Write Executors" vs "Read Executors"?

**Key Point:** They're the SAME pool of executors - just different roles in the same shuffle operation!

```
BEFORE SHUFFLE (Map stage):
├─ Your 100 input partitions distributed across 10 executors
├─ Each executor processes assigned partitions (e.g., Executor A has 10 partitions)
└─ Executors doing this work = "WRITE EXECUTORS" (writing output to shuffle files)

AFTER SHUFFLE (Reduce stage):
├─ Your 200 output shuffle partitions distributed across same 10 executors
├─ Each executor processes assigned output partitions (e.g., Executor A now has 20 output partitions)
└─ Executors doing this work = "READ EXECUTORS" (reading from shuffle files)

SAME CLUSTER, SAME 10 EXECUTORS, DIFFERENT JOBS/ROLES:
```

So it's not 2 sets of executors - it's the same executors taking on different responsibilities!

### Two-Phase Shuffle (Distributed Across the Cluster)

**Important:** Shuffle is a DISTRIBUTED operation - happens on ALL executors simultaneously

```
PHASE 1: SHUFFLE WRITE (happens on ALL write executors in parallel)
│
├─ Each write executor processes its assigned input partitions
│  └─ (could be on core or task nodes)
│
├─ Step 1: Map each record to shuffle bucket
│  └─ Bucket = destination partition for that record (based on key hash)
│
├─ Step 2: Sort records by key within each bucket
│  └─ Writing sorted data to executor's local disk
│
└─ Step 3: Write sorted buckets to disk
   └─ Each executor writes multiple files
      └─ 1 file per destination partition
      └─ Example: Executor A writes 200 files (one for each of 200 shuffle partitions)
      └─ Files stored locally on executor's machine

PHASE 2: SHUFFLE READ (happens on ALL read executors in parallel)
│
├─ Each read executor assigned to certain output partitions
│  └─ Example: Executor 1 → output partitions 0-10, Executor 2 → partitions 11-20
│
├─ Step 1: Fetch shuffle files from ALL write executors
│  └─ Network transfer: Read executor reaches across network to write executor disks
│  └─ Example: Executor 1 fetches "partition 5" files from Executor A, B, C, D, etc.
│  └─ Memory buffered: Limited by spark.reducer.maxSizeInFlight (default 48MB)
│
├─ Step 2: Merge-sort all fetched data
│  └─ Combine files from multiple write executors into single sorted stream
│
└─ Step 3: Group by key (for groupBy) or match keys (for join)
   └─ Now data is ready for next operation
```

**Key Point:** Network traffic is the bottleneck!
- Write phase: LOCAL disk I/O (fast)
- Read phase: NETWORK transfer between executors (slow, expensive)


### Memory Usage During Shuffle: Write and Read Buffers

**See Section 04 - Memory Architecture** for complete executor memory breakdown (1.5GB execution, 1.5GB storage, 1GB reserved).

This section focuses on shuffle-specific buffers within execution memory:

#### Shuffle Write Buffer (Executor writes output files)

```
spark.shuffle.file.buffer (default 32KB per output partition):
├─ Example: 200 shuffle partitions × 32KB = 6.4MB total buffer per executor
├─ Purpose: Buffer records by destination partition before writing to disk
└─ Impact: More partitions = larger total buffer
```

#### Shuffle Read Buffer (Executor reads from remote executors)

```
spark.reducer.maxSizeInFlight (default 48MB):
├─ "In flight" = data being transferred over network but not yet processed
├─ Purpose: Prevent memory overflow while fetching from many write executors
└─ Back pressure: Pauses fetching when buffer full, resumes after processing
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

# This is a ONE-SIZE-FITS-ALL default and causes problems:

# Small job example (10GB data):
# - 200 partitions × 50MB = 10GB total
# - Problem: Too many partitions for small data
# - Impact: 200 tasks in scheduler, 200 network fetches, extra overhead
# - Better: 50 partitions × 200MB = 10GB (fewer, larger, less overhead)

# Large job example (500GB data):
# - 200 partitions × 2.5GB = 500GB total
# - Problem: Partitions too large
# - Impact: Each partition needs 2.5GB in execution memory, likely spill
# - Better: 500 partitions × 1GB = 500GB (smaller partitions, fit in memory)

# RULE OF THUMB:
# - Target partition size: 128-256MB (fits in executor memory comfortably)
# - Too many partitions: >10 per executor core = scheduling overhead
# - Too few partitions: <1 per executor core = underutilization
```

### Choosing Optimal Partitions

```python
import math

total_data_size = 100 * 1024  # 100GB in MB
target_partition_size = 128    # 128MB per partition (ideal)

optimal_partitions = math.ceil(total_data_size / target_partition_size)
print(optimal_partitions)  # ~800 partitions

# But also consider practical constraints:

# 1. Executor cores available (avoid >2-4 partitions per core)
#    If 10 executors × 8 cores = 80 cores total
#    Can handle 160-320 partitions efficiently
#
# 2. Network bandwidth impact
#    Too many partitions = more files = more network metadata overhead
#    Too few partitions = fewer parallel transfers, underutilize network
#
# 3. Memory per executor
#    Fewer partitions = each executor processes larger partition in shuffle read
#    More partitions = each executor processes smaller partition (fits in memory better)
#
# 4. Task scheduling overhead
#    Each partition = 1 task in scheduler queue
#    1000 tasks = more scheduling overhead than 100 tasks
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
├─ Max data BUFFERED IN MEMORY while fetching from remote executors
├─ "In flight" = data transferred over network but not yet processed
├─ How it works:
│  └─ Executor fetches partition data from 100 write executors
│  └─ Can buffer max 48MB of this incoming data
│  └─ Once buffer full, waits before fetching more (back pressure)
├─ Too high: Memory pressure, may cause spill
├─ Too low: Network underutilized, more round-trips, slower
└─ Typical: 48MB-100MB (10 executors) up to 256MB-512MB (50+ executors)

spark.shuffle.sort.bypassMergeThreshold (default 200)
├─ If num partitions < threshold, skip merge-sort
├─ Faster for small jobs
└─ Typical: 200
```

### Buffer Tuning Guide

#### When to Adjust Write Buffer (spark.shuffle.file.buffer)

```python
# DIAGNOSIS: Slow shuffle write or high disk I/O
# Symptom: Disk write latency, many small writes to disk

# Default: 32KB (good for most cases)
spark.conf.set("spark.shuffle.file.buffer", "32k")  # Default

# INCREASE to 64KB or 128KB if:
# ├─ You have many shuffle partitions (>1000)
# ├─ Disk I/O is the bottleneck (not memory)
# └─ You have plenty of memory available
spark.conf.set("spark.shuffle.file.buffer", "64k")

# DECREASE to 16KB if:
# ├─ Very tight memory constraints
# └─ Willing to trade disk I/O for memory savings
spark.conf.set("spark.shuffle.file.buffer", "16k")

# Memory cost calculation:
# buffer_cost = num_shuffle_partitions × buffer_size
# 200 partitions × 64KB = 12.8MB (acceptable)
# 5000 partitions × 64KB = 320MB (check if this causes memory issues!)
```

#### When to Adjust Read Buffer (spark.reducer.maxSizeInFlight)

```python
# DIAGNOSIS: Slow shuffle read or memory pressure during shuffle

# Rule of thumb:
# Small cluster (<=10 executors): 48MB-96MB
# Medium cluster (10-50 executors): 96MB-256MB
# Large cluster (>50 executors): 256MB-1GB

# Example 1: 5-executor cluster with 8 cores each
spark.conf.set("spark.reducer.maxSizeInFlight", "64m")

# Example 2: 20-executor cluster with 16 cores each
spark.conf.set("spark.reducer.maxSizeInFlight", "256m")

# Example 3: 100-executor cluster with 8 cores each
spark.conf.set("spark.reducer.maxSizeInFlight", "512m")

# INCREASE if:
# ├─ Network is the bottleneck (gaps in data transfer)
# ├─ You have plenty of memory (>8GB per executor)
# └─ Want to maximize network utilization
spark.conf.set("spark.reducer.maxSizeInFlight", "256m")  # From default 48m

# DECREASE if:
# ├─ Memory pressure (seeing spill messages)
# ├─ Executor OOM errors during shuffle
# └─ Running on memory-constrained cluster
spark.conf.set("spark.reducer.maxSizeInFlight", "24m")  # From default 48m
```

### Recommended Configuration

```python
# For general workloads (10 executors, 8 cores, 16GB RAM each)
spark.conf.set("spark.sql.shuffle.partitions", 150)
spark.conf.set("spark.shuffle.file.buffer", "64k")      # Balance write perf
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")  # Network efficient
spark.conf.set("spark.shuffle.compress", True)
spark.conf.set("spark.shuffle.spill.compress", True)

# For small jobs (< 10GB data)
spark.conf.set("spark.sql.shuffle.partitions", 30)
spark.conf.set("spark.shuffle.file.buffer", "32k")      # Less partitions, default OK
spark.conf.set("spark.reducer.maxSizeInFlight", "64m")  # Smaller footprint

# For large jobs (> 100GB data)
spark.conf.set("spark.sql.shuffle.partitions", 500)
spark.conf.set("spark.shuffle.file.buffer", "64k")      # More partitions, increase buffer
spark.conf.set("spark.reducer.maxSizeInFlight", "128m") # More data in flight OK

# For memory-constrained cluster (<4GB per executor)
spark.conf.set("spark.shuffle.file.buffer", "16k")      # Reduce write buffer
spark.conf.set("spark.reducer.maxSizeInFlight", "32m")  # Reduce network buffer
spark.conf.set("spark.sql.shuffle.partitions", 50)      # Fewer, safer partitions
```

### Concurrent Shuffle: Write and Read Happening Together

**Important Understanding:** Write and read phases don't happen strictly sequentially!

```
TIMELINE OF A LARGE SHUFFLE (simplified):

TIME 0-2s: WRITE PHASE (all write executors)
├─ Executor A: Processing partition 1 → buffering records → flush to disk
├─ Executor B: Processing partition 2 → buffering records → flush to disk
├─ Executor C: Processing partition 3 → buffering records → flush to disk
└─ Executor D: Processing partition 4 → buffering records → flush to disk

TIME 1-3s: EARLY READ PHASE (some read executors start fetching)
├─ Executor A (now as read executor): Starts fetching partition 1 data from A,B,C,D
│  └─ Reads into 48MB read buffer
├─ Executor B: Starts fetching partition 2 data from A,B,C,D
├─ Executor C: Starts fetching partition 3 data from A,B,C,D
└─ Executor D: Waits (still writing, hasn't started read phase yet)

TIME 2-4s: OVERLAP (write completing, read in progress)
├─ Executor A: Merging fetched partition 1 data
├─ Executor B: Merging fetched partition 2 data
├─ Executor C: Merging fetched partition 3 data
└─ Executor D: Finishing write phase, about to fetch partition 4

TIME 3-5s: READ PHASE (all read executors processing)
├─ All executors: Processing their assigned output partitions
├─ Memory: Read buffers now emptying as data processed
└─ Shuffle complete, ready for next stage

MEMORY PRESSURE TIMELINE:
├─ Time 0-2s: Write buffers use 6.4MB (manageable)
├─ Time 1-2s: Both write buffers + read buffers active!
│  └─ 6.4MB (write) + 48MB (read) = 54.4MB + execution memory
├─ Time 2-4s: Read buffers dominant
│  └─ 48MB read buffer + merge buffers + execution memory (peak pressure!)
└─ If total > 1.5GB execution memory: SPILL TO DISK
```

**Why This Matters:**
- Peak memory usage happens during overlap (write + read buffering simultaneously)
- Even if you have enough total memory, the TIMING of buffer fills can cause spill
- This is why tuning `spark.reducer.maxSizeInFlight` helps - prevents read buffer from growing too large during overlap period

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

# Repartition both to same count to enable co-partitioned join
products_repart = products.repartition(100)  # Shuffle 1: redistribute products
transactions_repart = transactions.repartition(100)  # Shuffle 2: redistribute transactions

result = transactions_repart.join(products_repart, "product_id")
# Both have 100 partitions with SAME key distribution
# Join happens locally per partition (no additional shuffle before join)
# Still has 2 shuffles (to repartition) + join execution
# Benefit: Joins data that's already co-located, avoiding 3rd shuffle
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

# Solution 2: Salt hot keys (add random suffix to split hot key across partitions)
from pyspark.sql.functions import when, col, rand, concat, lit

# Original: "USA" → all 100B records → 1 partition → 1 executor needs 100GB memory
# Problem: Single executor overloaded, others idle

# Salting: Add random suffix to "USA" records
df_salted = df.withColumn(
    "key_salted",
    when(col("country") == "USA",
         concat(col("state"), lit("_"), (rand() * 50).cast("int")))  # USA_0 ... USA_49
    .otherwise(col("country"))  # UK, DE, etc. stay same
)

result = df_salted.groupBy("key_salted").agg(F.sum("value"))

# Result after shuffle:
# - "USA_0" → partition 0 → 2B records → 1 executor (manageable)
# - "USA_1" → partition 1 → 2B records → 1 executor (manageable)
# - ... up to "USA_49"
# - "UK" → partition 50 → 1B records → 1 executor
# - "DE" → partition 51 → 1B records → 1 executor
#
# ADVANTAGE: Original 100B-to-1B ratio (100x imbalance) becomes 2B-to-1B (2x imbalance)
# Much more balanced load across executors!
#
# TRADEOFF: Need extra groupBy aggregation to combine back:
# result_final = result.groupBy("state").agg(F.sum("value"))
# This combines USA_0, USA_1, ..., USA_49 back into "USA" totals

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
