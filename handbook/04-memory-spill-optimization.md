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

### Executor Memory Breakdown (4GB typical)

```
┌─────────────────────────────────────────────────────┐
│           Executor Memory: 4GB Total               │
├─────────────────────────────────────────────────────┤
│ Execution Memory:    1.5GB  ← Used during transforms │
│ Storage Memory:      1.5GB  ← Used for caching       │
│ Reserved:            1.0GB  ← GC, OS, overhead       │
└─────────────────────────────────────────────────────┘

EXECUTION MEMORY (1.5GB) - Critical for Spill!
Used by operations that temporarily hold data:
├─ GroupBy aggregations (building hash tables)
├─ Joins (shuffling data between executors)
├─ OrderBy/Sort (sorting partitions)
└─ Window functions (buffering for ranking)

STORAGE MEMORY (1.5GB)
Used for data you explicitly cache:
├─ df.cache() or df.persist()
└─ Broadcast variables

RESERVED (1.0GB)
├─ Garbage collection
├─ OS memory
└─ Python/JVM overhead
```

### Real Example: GroupBy with DataFrame

**Scenario:** You have a 1GB Parquet file with sales data, and you want to aggregate by customer_id.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .appName("MemoryExample") \
    .getOrCreate()

# Step 1: Read 1GB file
df = spark.read.parquet("s3://data/sales/2024/")
# File: 1GB on disk
# In memory (Execution Memory): ~1GB (structured data, not loaded fully yet)

# Step 2: GroupBy aggregate (this is where spill happens!)
result = df.groupBy("customer_id").agg(
    spark_sum("amount").alias("total_sales"),
    count("*").alias("order_count")
)

# What happens in memory:
result.show()  # Action! Now execution starts
```

**What Happens Step-by-Step (The Accurate Timeline):**

**IMPORTANT:** Hash table building is **INCREMENTAL** - each partition is processed as it arrives, not waiting for all partitions.

```
TIME 0: Read file (Lazy evaluation)
├─ File: 1GB on S3, split into 10 partitions × 100MB
├─ Execution Memory: Empty (nothing loaded yet)
└─ Plan created, not executed

TIME 1: Partition 1 arrives & local aggregation starts
├─ Read partition 1 from S3: 100MB
├─ Create LOCAL hash table from partition 1 data:
│  └─ {customer_id → (total_sales, order_count)}
├─ Local hash table: ~50MB (aggregated/compressed)
├─ Execution Memory used: 100MB (partition) + 50MB (hash table) = 150MB ✓
└─ Keep hash table in memory (will merge with next partition)

TIME 2: Partition 2 arrives & merge
├─ Read partition 2: 100MB
├─ Merge partition 2 into existing hash table:
│  ├─ For each customer in partition 2:
│  │  └─ Look up in hash table, update total & count
│  └─ Hash table grows slightly (new customers from P2)
├─ Execution Memory: 100MB (partition) + 60MB (hash table) = 160MB ✓
└─ Discard partition 1 data, keep merged hash table

TIME 3-9: More partitions arrive & keep merging
├─ Partition 3: 100MB → merge → hash table 70MB
├─ Partition 4: 100MB → merge → hash table 80MB
├─ Partition 5: 100MB → merge → hash table 90MB
├─ Partition 6: 100MB → merge → hash table 100MB
├─ Partition 7: 100MB → merge → hash table 110MB
├─ Partition 8: 100MB → merge → hash table 120MB
├─ Partition 9: 100MB → merge → hash table 130MB
└─ Execution Memory: ~130MB + 100MB partition = 230MB ✓

TIME 10: Still within memory
├─ Partition 10: 100MB arrives
├─ Hash table now: 150MB (aggregated data accumulating)
├─ Execution Memory: 100MB + 150MB = 250MB ✓
├─ Still under 1.5GB limit!
└─ Continue...

TIME 15: Hash table growing too large!
├─ More partitions processed (P11-P15)
├─ Hash table accumulated: 800MB (many unique customers!)
├─ Partition 15: 100MB arrives
├─ Execution Memory: 100MB + 800MB = 900MB ✓
└─ Still safe...

TIME 18: Memory pressure increases
├─ Partitions P16-P18 being processed
├─ Hash table accumulated: 1.2GB (very large!)
├─ Partition 18: 100MB arrives
├─ Execution Memory: 100MB + 1.2GB = 1.3GB (approaching limit!)
└─ Getting close to 1.5GB!

TIME 19: MEMORY FULL! SPILL TRIGGERS!
├─ Partition 19: 100MB arrives
├─ Hash table size: 1.3GB
├─ New partition data: 100MB
├─ Total needed: 1.4GB + overhead
├─ Execution Memory available: 1.5GB (ALMOST FULL!)
├─ System triggers SPILL to make room
└─ ACTION: Write hash table to disk!

SPILL SEQUENCE:
├─ Write 1.3GB hash table to disk: 2 seconds (slow!)
├─ Free 1.3GB execution memory
├─ Continue processing remaining partitions (P19-P20)
├─ Merge partition data into new (smaller) hash table
├─ Keep repeating: when hash table gets big, spill to disk
└─ Multiple spills may occur for large datasets!

TIME 21: Spill complete, continue
├─ Continue merging remaining partitions (P20+)
├─ Partition data gets added to hash table
├─ Process continues until all partitions done
└─ All intermediate results on disk need to be merged

TIME 25: Final merge phase
├─ All partitions processed
├─ Intermediate results scattered on disk
├─ Read all spilled data from disk: 2-3 seconds
├─ Merge all intermediate results: 1 second
├─ Return final result
└─ Total time: 25 seconds (5x slower than without spill!)

COMPARISON:
├─ WITHOUT SPILL: 5 seconds (all in memory)
├─ WITH SPILL: 25 seconds (disk writes/reads)
└─ Slowdown: 5x (or worse for massive spills!)
```

**Key Insight:** The problem isn't loading all partitions at once - it's the **ACCUMULATED hash table growing too large** as you merge partition after partition. Each new partition adds new unique keys to the hash table, making it bigger and bigger!

### Critical Clarification: How Partitions, Cores, and Memory Share

**IMPORTANT:** The timeline above shows **sequential processing within ONE executor**. Here's how it actually works:

```
EXECUTOR with 8 cores, 1.5GB execution memory:

Scenario: GroupBy on 10 partitions (all belong to SAME executor)

Option A: Sequential (each partition processes fully before next)
├─ Task 1 (Core 1): Process P1 → create hash table → store in execution memory
├─ Task 2 (Core 2): Wait - execution memory occupied by Task 1's hash table!
└─ When Task 1 done: Task 2 processes P2, reuses/merges with same hash table

Option B: Parallel (multiple cores process different partitions at same time)
├─ Task 1 (Core 1): Process P1 → create hash table (50MB) → 150MB total execution memory
├─ Task 2 (Core 2): Process P2 → merge into SAME hash table (60MB) → 200MB total execution memory
├─ Task 3 (Core 3): Process P3 → merge into SAME hash table (70MB) → 270MB total execution memory
└─ All tasks share the SAME execution memory pool (1.5GB for entire executor)
   NOT divided per core!
```

**The key point: The hash table is NOT duplicated per core.**

```
WRONG understanding:
├─ Core 1 has its own hash table (50MB)
├─ Core 2 has its own hash table (50MB)
├─ Core 3 has its own hash table (50MB)
└─ Total: 150MB (WRONG - not how it works!)

CORRECT understanding:
├─ All cores on the executor
├─ Share ONE execution memory pool (1.5GB total)
├─ The hash table is stored ONCE in execution memory
├─ All tasks access/update the SAME hash table
├─ Synchronization ensures no conflicts
└─ Total memory: Just one hash table (50-150MB)
```

**Why this matters for spill:**
```
If executor has 8 cores all processing partitions from same groupBy:
├─ Each core is a separate task
├─ Each task reads a partition (100MB)
├─ Each task merges into the SHARED hash table
├─ Execution memory fills up: 100MB (partition) + hash table size
├─ If 8 tasks × 100MB = 800MB
│  Plus hash table = 1.2GB
│  Total: Within 1.5GB limit - no spill!

But if hash table grows to 1GB (many unique keys):
├─ 1 task reading partition: 100MB
├─ Hash table: 1GB
├─ Total: 1.1GB (fits)
├─ BUT: If another task starts simultaneously
│  Task 1: 100MB + 1GB hash table (reads/updates)
│  Task 2: 100MB + 1GB hash table (reads/updates)
│  Combined: Uses same 1.1GB pool (shared), not 2.2GB
└─ UNLESS synchronization requires temporary copies
   Then spill can happen!
```

**Bottom line:** All partitions processed by an executor for the same operation share ONE execution memory pool. The hash table is built incrementally and shared, not duplicated per core.

### Comparison: With vs Without Spill

**WITHOUT Spill (Enough Execution Memory):**
```
Execution Memory Available: 2GB (increased)
├─ Read all partitions into hash table: 1GB
├─ Hash table fits in memory completely
├─ No disk writes during aggregation
├─ Speed: 5 seconds ✓
```

**WITH Spill (Not Enough Memory):**
```
Execution Memory Available: 1.5GB (default)
├─ Read partitions, fill memory: 1.5GB
├─ Can't fit more! SPILL TO DISK
├─ Write 1GB to disk: 1 second
├─ Read more partitions: 2 seconds
├─ Write another 500MB: 0.5 seconds
├─ Read from disk for final merge: 3 seconds
├─ Speed: 20 seconds ✗ (4x slower!)
```

### Why This Matters: Real Numbers

```
Parquet file: 1GB
Unique customers: 100,000

Memory per customer (hash table entry):
├─ customer_id: 8 bytes
├─ total_sales: 8 bytes (double)
├─ order_count: 8 bytes (long)
└─ Overhead: ~16 bytes
   Total: ~40 bytes per customer

Total hash table size:
├─ 100,000 customers × 40 bytes = 4MB (metadata only)
├─ BUT: Spark buffers intermediate data
├─ Actual memory used: ~500MB-800MB for 1GB input
└─ Fits in 1.5GB execution memory? USUALLY YES

Problem case: 10M unique customers
├─ Hash table size: 10M × 40 bytes = 400MB
├─ Plus buffered data: 800MB
├─ Total: 1.2GB (still fits in 1.5GB)

Critical case: 50M unique customers
├─ Hash table size: 50M × 40 bytes = 2GB
├─ Plus buffered data: 3GB+
├─ Total: 5GB+ (EXCEEDS 1.5GB!)
├─ Result: MASSIVE SPILL (3.5GB written to disk!)
└─ Speed: 5 minutes → 45 minutes (9x slower!)
```

### Simple Rule of Thumb

```python
# Estimate memory needed for groupBy:
#
# Memory = (input_size / num_partitions) × buffering_factor
#          + (unique_keys × key_overhead)
#
# buffering_factor ≈ 2-3x (Spark buffers intermediate data)
# key_overhead ≈ 40-100 bytes per unique key

# Example:
# Input: 100GB file
# Partitions: 100
# Unique keys: 1M

memory_needed = (100 * 1024 / 100) * 2.5 + (1000000 * 50 / 1024 / 1024)
#             = (1024) * 2.5 + 48
#             = 2560 + 48
#             = 2.6GB

# If executor memory is 4GB:
# Execution memory available: 1.5GB
# Memory needed: 2.6GB
# SPILL EXPECTED! (2.6 - 1.5 = 1.1GB will spill)
```

---

## Understanding Spill

🔗 **Related:** Section 02 - Python Data Structures | Section 05 - Shuffle Optimization

### When Spill Happens

```python
# Scenario 1: GroupBy (high cardinality)
# Problem: Many unique keys = large hash table
df.groupBy("user_id").sum()  # If 1M unique users, lots of memory needed
# Memory needed: millions of hash entries × overhead
# Risk: HIGH if >1M unique keys

# Scenario 2: Join without broadcast
# Problem: Both tables shuffle to executors simultaneously
df_large.join(df_medium, "key")  # Both tables shuffle
# Memory needed: combined size of both tables in partition
# Risk: HIGH if both tables are large

# Scenario 3: OrderBy / Sort
# Problem: Must hold entire partition to sort
df.orderBy("timestamp")  # Sorts entire partition
# Memory needed: size of one full partition
# Risk: MEDIUM (depends on partition size)

# Scenario 4: Window Functions
# Problem: Must buffer all rows in partition for ranking
from pyspark.sql.window import Window
df.withColumn("rank", row_number().over(Window.orderBy("date")))
# Memory needed: full partition buffered in memory
# Risk: MEDIUM-HIGH (depends on partition size)
```

### Key Insights: When Spill is Most Likely

| Operation | Memory Pattern | Risk | Why |
|-----------|---|---|---|
| **GroupBy** | Linear with unique keys | 🔴 HIGH | Hash table grows with each unique key |
| **Join** | Linear with both table sizes | 🔴 HIGH | Both tables shuffled to same executor |
| **OrderBy** | Linear with partition size | 🟡 MEDIUM | Single partition must fit in memory |
| **Window** | Linear with partition size | 🟡 MEDIUM | Full partition buffered |
| **Broadcast** | Fixed (small table only) | 🟢 LOW | No shuffle needed |

---

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

**See Section 01** for detailed explanation of coalesce vs repartition (the key distinction and when to use each).

### Strategy 4: Use Broadcast for Small Tables

For joins with small tables (< 1GB), use broadcast to avoid shuffling the large table.

**See Section 06 - Broadcast Hash Join** for complete strategy, memory requirements, and when to use.

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
