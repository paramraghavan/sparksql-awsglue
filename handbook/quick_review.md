# QUICK REVIEW: Big Data Developer's Survival Guide

**Purpose:** On-site implementation, troubleshooting, and interview prep for PySpark, AWS, and big data engineering.

**Level:** Intermediate to Advanced | **Time to Read:** 30 min | **Time to Master:** Build 5 ETL pipelines using these patterns

---

## Table of Contents

1. [Decision Trees (5-Second Answers)](#decision-trees-5-second-answers)
2. [Core Concepts Deep Dive](#core-concepts-deep-dive)
3. [Join Strategies: Choose Wisely](#join-strategies-choose-wisely)
4. [Avoiding Shuffle Hell](#avoiding-shuffle-hell)
5. [Catalyst Optimizer Mastery](#catalyst-optimizer-mastery)
6. [Pandas UDF vs Python UDF](#pandas-udf-vs-python-udf)
7. [Snowflake + PySpark: Direct vs S3 Export](#snowflake--pyspark-direct-vs-s3-export)
8. [Production On-Site Patterns](#production-on-site-patterns)
9. [Troubleshooting Checklist](#troubleshooting-checklist)
10. [Interview Questions & Answers](#interview-questions--answers)
11. [Performance Tuning Decision Tree](#performance-tuning-decision-tree)

---

## Decision Trees (Clear 5-Second Answers)

### 1️⃣ "Should I use Broadcast Join?"

**Simple Answer:**
- **Table < 10MB?** → YES, always broadcast ✅
- **Table 10MB-500MB?** → Maybe (test if memory allows)
- **Table > 500MB?** → NO, use regular join ❌

**For Advanced Users (What to Check):**

| Table Size | Default? | Action | Memory Cost | Speed | Notes |
|-----------|----------|--------|------------|-------|-------|
| < 10MB | Auto-broadcast | Use it ✅ | Safe | 100x faster | No config needed |
| 10-100MB | No | Explicit broadcast() | Check executor memory | 50x faster | Increase threshold if memory allows |
| 100-500MB | No | Test first! | Risky (~500MB × 10 exec = 5GB) | 20x faster | Only if executor_memory > 8GB |
| > 500MB | Never | Regular join | No extra cost | 1x baseline | Use sort-merge (default) |

**Code Examples:**
```python
# Beginner: Simple broadcast
result = large_df.join(broadcast(small_df), "id")

# Advanced: Tune threshold for your cluster
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100mb")  # 4GB executor
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")  # 8GB executor
```

---

### 2️⃣ "Why is my job slow?"

**Quick Checklist (Check in this order):**

1. **Is data being SHUFFLED and dominating execution time?**
   - Check: Spark UI → Stages → Look at the shuffle stage
   - Metric 1: "Shuffle Write" size
   - Metric 2: Shuffle stage duration vs total job duration
   - **Shuffle is a problem IF:**
     - Shuffle stage takes > 50% of total job time, OR
     - Shuffle stage takes > 30 seconds, OR
     - "Shuffle Write" >> "Shuffle Read" (indicates spill/recomputation)
   - FIX: Pre-filter data or use broadcast join

2. **Is there DATA SKEW?**
   - Check: Spark UI → Stages → Task Duration (look at the numbers!)
   - If some tasks 10x slower than others → FIX: Pre-filter skewed keys
   - Example: Most tasks 10s, one task 300s = DATA SKEW

3. **Are ALL tasks slow (evenly)?**
   - Check: Task 1: 40s, Task 2: 42s, Task 3: 41s (all similar)
   - If all tasks slow → FIX: Memory spill or network issue
   - FIX: Pre-filter before operations or increase memory

4. **Are there TOO MANY output files?**
   - Check: S3 folder with 1000+ small files
   - If yes → FIX: `df.coalesce(10).write.parquet(...)`

5. **Is it a JOIN operation?**
   - Check: Execution time breakdown shows JOIN stage dominating
   - If yes → See [Join Strategies](#join-strategies-choose-wisely)

**Advanced Users - How to Detect Shuffle Problem (Context Matters!):**

```
The "Shuffle Problem" depends on YOUR cluster:

SMALL CLUSTER (2-5 executors):
├─ Shuffle > 5GB = Problem (network bottleneck)
├─ Shuffle 10GB = Definitely problem (10+ minutes)
└─ Example: 5GB shuffle × 1Gbps network = ~40 seconds = acceptable
             10GB shuffle × 1Gbps network = ~80 seconds = slow

MEDIUM CLUSTER (10-20 executors):
├─ Shuffle > 50GB = Problem (network bottleneck)
├─ Shuffle 10-50GB = Acceptable (parallel shuffle helps)
└─ Example: 50GB shuffle ÷ 20 executors = 2.5GB per executor = OK

LARGE CLUSTER (50+ executors):
├─ Shuffle > 200GB = Problem (disk I/O bottleneck)
├─ Shuffle 50-200GB = Acceptable (parallel network + disk)
└─ Example: 200GB shuffle ÷ 50 executors = 4GB per executor = OK

REAL INDICATOR (Ignore size, check these instead):
├─ Shuffle Write >> Shuffle Read: YES = Spill happening (PROBLEM!)
│  └─ Example: Write=100GB, Read=40GB = 60GB spilled to disk
├─ Shuffle stage duration: > 30 seconds = PROBLEM!
├─ Shuffle stage % of total time: > 50% = PROBLEM!
└─ FIX: Pre-filter, reduce partitions, or use broadcast
```

**How to Check Spill (Most Important!):**

```
Spark UI → Stages → Select shuffle stage:
├─ Look at: "Shuffle Write Size" and "Shuffle Read Size"
├─ If Write >> Read: SPILL HAPPENING!
│  └─ Example: Write=100GB but Read=40GB = 60GB went to disk
│  └─ FIX: Pre-filter data before shuffle or increase memory
├─ If Write ≈ Read: Normal (no spill)
└─ If Write < Read: Replication or multiple reads (check logs)
```

**Other Metrics:**

```
Data Skew Problem:
├─ Task Duration variance high (max/min > 10x): Yes = Skew
├─ Example: max_task_duration=300s, min=10s, variance=30x = SKEW
└─ FIX: Separate processing for skewed keys or repartition with salt

Uniform Slowness (All tasks slow, similar duration):
├─ All tasks 40-45 seconds (variance low): Yes = Uniform
├─ GC time > 10% in logs: Memory spill happening
├─ Task size varying but all small: Too many partitions
└─ FIX: Reduce shuffle partitions or pre-filter
```

---

### 3️⃣ "Should I use a UDF?"

**Quick Decision:**

| Question | Answer | Next Step |
|----------|--------|-----------|
| Does Spark have a built-in function? | YES ✅ | Use Spark's function (100-1000x faster!) |
| | NO | Continue... |
| Do you need complex custom logic? | YES | Use Pandas UDF (vectorized, 100x faster) |
| | NO | Just use Spark SQL expression |

**Examples:**
```python
# ❌ DON'T: Custom UDF for simple math
@udf(DoubleType())
def multiply_by_1_1(x):
    return x * 1.1

# ✅ DO: Use Spark function instead
df.withColumn("result", col("salary") * 1.1)  # 1000x faster!

# ✅ OK: Pandas UDF for complex logic (if needed)
@pandas_udf(DoubleType())
def complex_calc(batch):
    return batch.apply(lambda x: expensive_calculation(x))
```

**Performance Numbers:**
- Built-in function: 0.001ms per operation
- Pandas UDF: 0.1ms per operation (100x slower than built-in)
- Row UDF: 1ms per operation (1000x slower than built-in)

---

### 4️⃣ "Is my job memory-constrained?"

**Check Spark UI → Executors tab:**

| Metric | Check | Issue? | Fix |
|--------|-------|--------|-----|
| Execution Memory Used | > 1.5GB | YES = Spill happening ❌ | Pre-filter data or increase memory |
| | < 500MB | NO = Fine ✅ | You have extra capacity |
| | 500MB-1.5GB | MAYBE = Optimized ✓ | Monitor for spill warnings |
| GC Time | > 10% | YES = Memory pressure | Pre-filter or increase memory |

**Advanced Check - Spill Warnings in Logs:**
```
Look for: "Spilling" or "GC overhead limit exceeded"
If found: → Data spilling to disk (10-100x slower)
Fix: Pre-filter before groupBy/join operations
```

---

### 5️⃣ "Should I increase executor memory or cores?"

**Diagnose the bottleneck first:**

| What You See | Root Cause | Solution | Cost |
|--------------|-----------|----------|------|
| **All tasks > 30s, variance LOW** | Computation slow or memory spill | Increase MEMORY (4GB → 8GB or 16GB) | Higher AWS bill |
| **Some tasks 10x slower (HIGH variance)** | Data skew | Pre-filter data or repartition | No AWS cost |
| **Task takes 30s with only 1 core busy** | Not enough parallelism | Increase CORES (1 → 4 → 8) | Higher AWS bill |
| **GC warnings in logs** | Memory pressure | Increase MEMORY | Higher AWS bill |
| **Tasks fast but job takes forever** | Too few executors | Increase num_executors | Higher AWS bill |

**Example Config Tuning:**
```python
# Slow individual tasks → increase memory
spark-submit --executor-memory 8g --num-executors 10 ...

# Many tasks but job slow → increase cores
spark-submit --executor-cores 4 --num-executors 10 ...

# Both problems? Both slow AND some tasks slower
spark-submit --executor-memory 8g --executor-cores 4 --num-executors 10 ...

# Data skew (some tasks 10x slower)? Don't increase resources!
# Instead: Pre-filter data or repartition with salt
df.filter(col("category") != "UNKNOWN").groupBy(...).sum()
```

---

## **🎯 The Real Secret: Check These 3 Things FIRST (Before Tuning)**

1. **Are you reading too much data?** → Filter early
2. **Are you shuffling too much data?** → Use broadcast join
3. **Do you have data skew?** → Pre-filter or separate processing

Most slow jobs fix with these 3. Only add memory/cores if these don't work!

---

## Core Concepts Deep Dive

### Why Partitions Matter (And How They Work)

#### The Problem You're Solving

```
Raw 100GB file:
├─ Can't fit in executor memory (4GB typical)
├─ Can't process sequentially (too slow)
└─ Need to split work across executors

Solution: Spark auto-partitions based on file format
```

#### How Partitioning Works

```python
# Input: 100GB Parquet file
spark.read.parquet("s3://data/file/")

# Spark automatically creates ~780 partitions:
# - Parquet row groups: 128MB each
# - 100GB ÷ 128MB = 780 partitions
# - Each partition = one task
# - 10 executors × 8 cores = 80 parallel tasks
# - Process 780 partitions in waves: 80 at a time, then next 80, etc.

# Each executor processes 78 partitions (780 ÷ 10):
# - Partition 1 (128MB) → process → output to shuffle files → free memory
# - Partition 2 (128MB) → process → output to shuffle files → free memory
# - ... repeat 78 times
# - Total output: 78 × 128MB ≈ 10GB per executor
```

#### Key Insight

**Execution memory (1.5GB) processes data stream-by-stream, not all-at-once:**

```python
# NOT this (would need 100GB memory):
memory = load_entire_100gb_file()

# This (streaming, re-using memory):
for partition in partitions:
    memory = load_partition(128MB)  # Load one partition
    process(memory)                  # Process it
    output_to_disk()                 # Write results
    memory.clear()                   # Free memory
    # Loop back, load next partition
```

---

### Memory Architecture: The Complete Picture

```
Executor Memory: 4GB Total
│
├─ EXECUTION MEMORY: 1.5GB (used by operations)
│  ├─ GroupBy aggregations (building hash tables)
│  ├─ Joins (shuffling both tables)
│  ├─ OrderBy (sorting partitions)
│  └─ Window functions (buffering for ranking)
│
├─ STORAGE MEMORY: 1.5GB (used for caching)
│  ├─ df.cache() → stored here
│  └─ Broadcast variables → stored here
│
├─ RESERVED: 1GB (system overhead)
│  ├─ Garbage collection
│  ├─ OS memory
│  └─ Python/JVM overhead
│
└─ Per-task limit: execution_memory ÷ num_cores
   Example: 1.5GB ÷ 8 cores = ~187.5MB per task
   But: Execution memory is SHARED, not divided
   So: Multiple tasks compete for same 1.5GB pool
```

#### Critical Detail: Memory Sharing Across Cores

```
8 cores on executor, all processing same groupBy:

Task 1 (Core 1): Reading partition 1
└─ Uses execution memory: 100MB partition + hash table

Task 2 (Core 2): Reading partition 2
└─ Uses execution memory: 100MB partition + SHARED hash table

The hash table is NOT duplicated per core:
├─ All tasks update the SAME hash table
├─ Synchronization ensures consistency
├─ Memory pressure = combined from all tasks
└─ If 4 tasks × 100MB = 400MB + 1GB hash table = 1.4GB (close to limit!)
```

---

### Lazy Evaluation: Why It Matters

```python
# This does NOTHING (yet):
df = spark.read.parquet("100gb_file")  # Lazy
df2 = df.filter(...)                   # Lazy
df3 = df2.groupBy(...).sum()           # Lazy

# This TRIGGERS execution:
df3.show()   # Action! Now Spark builds execution plan
df3.count()  # Action!
df3.write... # Action!
```

#### Why This Matters

```
Lazy evaluation lets Spark OPTIMIZE before execution:

df.read.parquet("100gb_file")\
    .select("name", "age")\         # Don't read all 100 columns
    .filter(col("age") > 25)        # Don't process all rows
    .groupBy("name").sum()

Catalyst sees full pipeline:
├─ Column Pruning: "Read ONLY name + age columns" (saves 90% I/O!)
├─ Predicate Pushdown: "Filter before groupBy" (less data shuffled)
├─ Join Reordering: "Do cheaper operations first"
└─ Execution plan OPTIMIZED before first byte is read
```

---

## Join Strategies: Choose Wisely

### Decision Matrix

| Table Size | Default Behavior | Strategy | Speed | Memory Cost |
|-----------|-----------------|----------|-------|-------------|
| < 10MB | Auto-broadcast | Broadcast | ⚡⚡⚡ 10-100x faster | Safe (small table cached) |
| 10MB-100MB | No auto-broadcast | Explicit broadcast if memory allows | ⚡⚡ 5-50x faster | Check memory (100MB × num executors) |
| 100MB-500MB | No auto-broadcast | Consider increasing threshold, then broadcast | ⚡⚡ 5-50x faster | **IMPORTANT:** Test first! |
| 500MB-2GB | No auto-broadcast | Maybe (if executor memory > 8GB) | ⚡ 2-10x faster | Risky: can cause memory issues |
| 2GB-100GB | Never broadcast | Sort-Merge Join | ⚡ 1x baseline | Both tables shuffled (unavoidable) |
| > 100GB | Never broadcast | Sort-Merge Join | ⚠️ 0.1x | Massive shuffle, watch for spill |

**Tuning Tip:** If you have medium tables (100MB-500MB), increase the threshold:
```python
# Default: 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")  # More aggressive

# Then Catalyst auto-broadcasts tables < 500MB
```

### Implementation: Know This Code

```python
from pyspark.sql.functions import broadcast

# ✅ FAST: Broadcast small table
df_large = spark.read.parquet("large_100gb.parquet")
df_small = spark.read.parquet("small_500mb.parquet")

result = df_large.join(
    broadcast(df_small),  # Explicit broadcast
    on="customer_id",
    how="left"
)
# What happens:
# 1. Small table (500MB) → copied to all executors (distributed cache)
# 2. Large table → NOT shuffled (stays where it is)
# 3. Join happens locally on each executor
# 4. Result: Only large table shuffled, 50% data movement vs regular join

# ❌ SLOW: Regular join (both tables shuffled)
result = df_large.join(df_small, on="customer_id", how="left")
# What happens:
# 1. Both tables shuffled by join key
# 2. Large table: 100GB shuffled
# 3. Small table: 500MB shuffled
# 4. Total: 100.5GB data movement
# 5. Result: MASSIVE network I/O

# ✅ MODERATE: Sort-Merge (when both are large)
df_medium_1 = spark.read.parquet("medium_50gb.parquet")
df_medium_2 = spark.read.parquet("medium_50gb.parquet")

result = df_medium_1.join(df_medium_2, on="key")
# What happens:
# 1. Both tables shuffled and sorted by join key
# 2. Sorted tables merged (efficient)
# 3. Good: Sorted order helps cache locality
# 4. 100GB data movement (unavoidable with two large tables)
```

### How Spark Decides (Catalyst's Join Selection Algorithm)

```
Spark's decision tree (in order):

1. Can I broadcast one table?
   ├─ Check: spark.sql.autoBroadcastJoinThreshold
   ├─ DEFAULT: 10MB (can be increased!)
   ├─ If table_size < threshold → YES, broadcast!
   ├─ AND enough executor memory available
   └─ Result: Broadcast Hash Join (no shuffle!)

   💡 CONFIG TIP: Increase for medium tables
   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")

   ⚠️ WARNING: Don't go too high!
   ├─ Broadcast (500MB) × 10 executors = 5GB memory used
   ├─ If executor memory = 2GB, you'll get OOM errors
   └─ Test size first: df.rdd.map(len).sum() / (1024**3)

2. Should I use Sort-Merge?
   ├─ Both tables > spark.sql.autoBroadcastJoinThreshold
   ├─ AND spark.sql.join.preferSortMergeJoin = true (DEFAULT)
   └─ YES → Use Sort-Merge Join (both shuffled, but efficient)

3. Fallback to Shuffle Hash Join
   ├─ Both tables > threshold
   ├─ AND spark.sql.join.preferSortMergeJoin = false
   └─ Slowest (both shuffled, no sort)

CONFIG TUNING EXAMPLES:

# More aggressive broadcasting (medium tables)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")

# Force sort-merge join (handles skew better)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

# Disable broadcast completely (for debugging)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### Memory Cost of Joins

```
Broadcast Join (small table 100MB):
├─ DEFAULT threshold: 10MB
├─ Table 100MB > 10MB, so NOT auto-broadcast
├─ Must set: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200mb")
├─ Driver memory: 100MB (to collect small table)
├─ Executor memory per executor: 100MB (cached broadcast)
└─ Total: 100MB + (10 executors × 100MB) = 1.1GB (safe!)

Broadcast Join (medium table 500MB):
├─ If you increase threshold to 500MB
├─ Driver memory: 500MB (to collect table)
├─ Executor memory per executor: 500MB (cached broadcast)
└─ Total: 500MB + (10 executors × 500MB) = 5.5GB ⚠️ (watch memory!)

Sort-Merge Join (both tables 100GB):
├─ Shuffle write: 100GB + 100GB = 200GB to disk
├─ Shuffle read: 200GB from disk back to executors
├─ Execution memory per executor: ~2GB (for sorting)
└─ Total: 200GB data movement + memory for sorting

KEY INSIGHT:
Broadcast (100MB) = 1.1GB memory usage
Sort-Merge (100GB) = 200GB network I/O + 2GB memory
→ Broadcast is 180x faster (network bound)!

TUNING STRATEGY:
├─ If executor memory ≥ 8GB: Increase threshold to 500mb
├─ If executor memory = 4GB: Keep threshold at 10-100mb
├─ If executor memory = 2GB: Keep threshold at 10mb, use explicit broadcast()
```

---

## Avoiding Shuffle Hell

### When Shuffle Happens (Know Your Enemies)

```python
# 🔴 SHUFFLE: GroupBy
df.groupBy("category").sum()  # Data shuffled by category

# 🔴 SHUFFLE: Join (non-broadcast)
df1.join(df2, "key")  # Both tables shuffled by key

# 🔴 SHUFFLE: OrderBy
df.orderBy("timestamp")  # Shuffled for global sort

# 🔴 SHUFFLE: Repartition (wide transformation)
df.repartition(100)  # Shuffled to new partition count

# 🟢 NO SHUFFLE: Filter
df.filter(col("age") > 25)  # No shuffle

# 🟢 NO SHUFFLE: Select
df.select("name", "age")  # No shuffle

# 🟢 NO SHUFFLE: WithColumn (simple operations)
df.withColumn("age_plus_one", col("age") + 1)  # No shuffle

# 🟡 MAYBE SHUFFLE: Coalesce
df.coalesce(10)  # No shuffle (merges partitions)
df.repartition(10)  # YES shuffle (redistributes data)
```

### The Shuffle Process (What's Happening Under the Hood)

```
Step 1: SHUFFLE WRITE (on executors processing source data)
├─ Executor 1 reads 100MB partition
├─ Groups data by key: {A: 50MB, B: 30MB, C: 20MB}
├─ Writes to local disk: shuffle_files/
│  ├─ shuffle_1_0_0.data (data for partition 0)
│  ├─ shuffle_1_0_1.data (data for partition 1)
│  └─ shuffle_1_0_2.data (data for partition 2)
└─ Repeats for all partitions on all executors
   Total written: ~10GB shuffle files on each executor

Step 2: SHUFFLE READ (on executors doing the groupBy/join)
├─ Executor 1 needs all records with key A
├─ Reads shuffle_*_0.data from ALL executors
│  └─ Executor 1's shuffle_1_0_0.data
│  └─ Executor 2's shuffle_2_0_0.data
│  └─ ... (from all 10 executors)
├─ Merges/sorts/aggregates
└─ Repeats for all keys
   Total read: ~10GB shuffle files (compressed, network)

Step 3: AGGREGATE
├─ Executor 1: Groups all key A records → calculates sum
├─ Result: 1 output row for category A
└─ Much smaller output (few GB vs 100GB input!)
```

#### Why Shuffle is Slow

```
1. DISK WRITES (slow!)
   └─ 10GB written to disk per executor (2 seconds/executor)

2. NETWORK I/O (slow!)
   └─ 10GB × 10 executors = 100GB moved over network
   └─ Network bottleneck: 1Gbps ≈ 10 seconds per 1GB
   └─ 100GB shuffle = 1000+ seconds on slow network!

3. DISK READS (slow!)
   └─ 10GB read from disk per executor (2 seconds/executor)

4. GC PRESSURE (hidden slow!)
   └─ Large amounts of data = many garbage collections
   └─ Each GC pauses all executors for 100ms-1s
   └─ 1000 partitions × 1s GC = 16 minutes of pause time!

Total time: MUCH slower than in-memory operations
```

### Strategy 1: Pre-Filter (Reduce Data BEFORE Shuffle)

```python
# ❌ BAD: Shuffle 100GB, then filter
df = spark.read.parquet("100gb_file")
result = df.groupBy("category").sum()  # Shuffles 100GB
filtered = result.filter(col("sum") > 1000)
# Time: 5+ minutes (shuffle time dominates)

# ✅ GOOD: Filter first, shuffle less
df = spark.read.parquet("100gb_file")
result = df.filter(col("amount") > 0)\  # Reduce to 50GB
           .groupBy("category").sum()   # Shuffle only 50GB
filtered = result.filter(col("sum") > 1000)
# Time: 2-3 minutes (50% faster!)

# Why it works:
# - Filter is a narrow transformation (no shuffle needed)
# - Catalyst's predicate pushdown moves filter to source
# - Only matching rows shuffled
```

### Strategy 2: Use Broadcast Join (Avoid Shuffle Entirely)

```python
# ❌ BAD: Regular join (both tables shuffled)
customers = spark.read.parquet("customers_1gb.parquet")  # 1GB
transactions = spark.read.parquet("transactions_100gb.parquet")  # 100GB

result = transactions.join(customers, "customer_id")
# Shuffle: 100GB + 1GB = 101GB moved
# Time: 10+ minutes

# ✅ GOOD: Broadcast small table (1GB not shuffled)
result = transactions.join(broadcast(customers), "customer_id")
# Broadcast: 1GB copied to all executors (100MB per executor if 10 executors)
# Shuffle: 0GB (transactions NOT shuffled!)
# Time: 2-3 minutes (80% faster!)
```

### Strategy 3: Reduce Shuffle Partitions

```python
# Default: 200 shuffle partitions (for large clusters)
# Problem: Many small partitions → many network round-trips

spark.conf.set("spark.sql.shuffle.partitions", "50")

df.groupBy("category").sum()
# Before: 200 partitions × (network latency) = slow
# After: 50 partitions × (network latency) = 4x faster!

# How to choose partition count:
# - Small jobs: 10-50
# - Medium (100GB): 100-200
# - Large (1TB+): 200-500
# Formula: data_size_gb / 2 ≈ partition_count
```

### Strategy 4: Coalesce Output (Fewer Output Files)

```python
# ❌ BAD: Write 500 output files
df.write.mode("overwrite").parquet("s3://output/")
# 500 files × small filesize = slow reads + storage overhead
# Reading back: open/close 500 files = slow!

# ✅ GOOD: Coalesce to 10 files
df.coalesce(10).write.mode("overwrite").parquet("s3://output/")
# 10 files × large filesize = fast reads
# Trade-off: Coalesce costs 1 more shuffle stage (worth it!)
```

---

## How to Resolve Shuffle Spill (Memory)

### What is Shuffle Spill?

When Spark aggregates data in a shuffle operation, it builds a hash table in executor memory. If the hash table doesn't fit → it spills to disk (very slow).

**Check in Spark UI:**
- Go to **Stages tab** → find your shuffle stage
- Look for **"Shuffle Spill (Memory)"** metric
- If > 0 → you have a spill problem

### Simple Example: Why More Partitions Help

**Setup:**
- 100GB of data
- Executor memory: 2GB
- Operation: `groupBy("customer_id").sum()`

#### With Few Partitions (2 partitions):
```
100GB ÷ 2 = 50GB per partition

Executor tries to aggregate partition 1:
├─ Load 50GB into memory
├─ Build hash table
├─ Memory available: 2GB
├─ Needed: 50GB > 2GB
└─ Result: SPILL (write to disk, slow) ❌
```

#### With Many Partitions (100 partitions):
```
100GB ÷ 100 = 1GB per partition

Executor aggregates partition 1:
├─ Load 1GB into memory
├─ Build hash table
├─ Memory available: 2GB
├─ Needed: 1GB < 2GB
└─ Result: NO SPILL (stays in memory, fast) ✅

Then executor moves to partition 2:
├─ Load 1GB (discard previous hash table)
├─ Memory available: 2GB
├─ Needed: 1GB < 2GB
└─ Result: NO SPILL ✅
```

**The Rule: More partitions = smaller chunks = each chunk fits in memory**

### Quick Fixes (Ranked by Effort)

#### 1. Increase Executor Memory (Easiest)
```python
# Before (spill):
spark-submit --executor-memory 2g script.py

# After (no spill):
spark-submit --executor-memory 4g script.py
```
**Why it works:** Hash table now has more room. **Cost:** Fewer executors per node.

#### 2. Increase Number of Executors (Divides the Load)
```python
# Before: 10 executors, 100GB ÷ 10 = 10GB per executor (spill!)
spark-submit --num-executors 10 --executor-memory 4g script.py

# After: 25 executors, 100GB ÷ 25 = 4GB per executor (no spill!)
spark-submit --num-executors 25 --executor-memory 4g script.py
```
**Why it works:** Same total memory, spread across more executors. **Cost:** More executor overhead.

#### 3. Increase Shuffle Partitions (Splits Data Into Smaller Pieces)
```python
# Before: 200 partitions, 500MB per partition
spark.conf.set("spark.sql.shuffle.partitions", 200)
df.groupBy("customer_id").sum()

# After: 500 partitions, 200MB per partition
spark.conf.set("spark.sql.shuffle.partitions", 500)
df.groupBy("customer_id").sum()
```
**Why it works:** Each partition's hash table is smaller, less memory pressure. **Cost:** More network overhead.

#### 4. Pre-Aggregate Before Shuffle (Advanced)
```python
# Aggregate locally first, THEN shuffle (reduces shuffle data)
df.rdd.mapPartitions(local_aggregate).toDF().groupBy("key").sum()
# Reduces shuffle volume by 50-80%
```

### Decision Matrix

| Symptom | Quick Check | Best Fix |
|---------|------------|----------|
| Shuffle Spill > 5GB | Check executor memory | Increase memory or executors |
| Spill is 20% of shuffle write | Data is skewed | Add more shuffle partitions |
| One executor has huge spill | Data hotspot on specific key | Use salt + repartition |
| All executors have small spill | Balanced but tight | Increase executor memory |

---

## Catalyst Optimizer Mastery

### What Catalyst Does (Automatic Magic)

Catalyst runs these optimizations WITHOUT you asking:

#### 1. Column Pruning (Dead Code Elimination)

```python
# Your code (reads all 100 columns):
df = spark.read.parquet("huge_100_column_file")
result = df.select("name", "age")

# What Catalyst does:
# Reads ONLY "name" and "age" columns (99 columns ignored!)
# Result: Read 10GB instead of 100GB (10x faster!)

# Why: Column pruning happens BEFORE reading
# - Catalyst sees full pipeline
# - Knows you only need 2 columns
# - Tells Parquet to skip 98 columns
```

#### 2. Predicate Pushdown (Filter at Source)

```python
# Your code:
df = spark.read.parquet("sales_data")
result = df.filter(col("amount") > 100).groupBy("category").sum()

# What Catalyst does:
# 1. Sees filter condition
# 2. Pushes filter to Parquet read
# 3. Parquet skips rows where amount ≤ 100 (at file level!)
# 4. Only qualifying rows enter Spark
# Result: Process 10GB instead of 100GB (10x faster!)

# This works because:
# - Parquet has min/max stats per row group
# - Catalyst checks: "Is it possible this row group has amount > 100?"
# - If min > 100: read entire block
# - If max < 100: skip entire block
# - If min/max spans 100: read (might contain matches)
```

#### 3. Join Reordering

```python
# Your code (bad order):
large_100gb.join(medium_50gb).join(small_1gb)

# What Catalyst does:
# 1. Estimates selectivity of joins
# 2. Reorders to: large_100gb.join(small_1gb).join(medium_50gb)
# 3. First join shuffles 100GB (unavoidable)
# 4. Reduces intermediate result before second join
# Result: Less total data shuffled

# Even better: Catalyst broadcasts small tables automatically!
# Default threshold: spark.sql.autoBroadcastJoinThreshold = 10MB
# If table < 10MB: Catalyst broadcasts automatically (no shuffle!)
# If table 10MB-500MB: Increase threshold to broadcast:
#   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")
```

#### 4. Constant Folding

```python
# Your code:
df.filter((col("amount") * 1.10) > (100 * 12))

# What Catalyst does:
# 1. Sees: 100 * 12 = 1200 (computable at plan time)
# 2. Optimizes to: (col("amount") * 1.10) > 1200
# Result: 1200 computed once, not for every row!

# Impact:
# - Without folding: 100M rows × multiply + compare = 200M operations
# - With folding: 100M rows × multiply + compare vs constant = 100M operations
# - Saves: 100M unnecessary multiplications!
```

#### 5. IsNotNull Implicit Checks

```python
# Your code:
df.filter(col("age") > 25)

# What Catalyst does:
# Converts to: df.filter((col("age").isNotNull()) & (col("age") > 25))

# Why? Because null > 25 = null (not false!)
# Without isNotNull:
# ├─ age = 30 → 30 > 25 = true ✓
# ├─ age = 20 → 20 > 25 = false ✓
# └─ age = null → null > 25 = null → INCLUDED (wrong!)

# With isNotNull:
# ├─ age = 30 → isNotNull = true, 30 > 25 = true ✓
# ├─ age = 20 → isNotNull = true, 20 > 25 = false ✓
# └─ age = null → isNotNull = false, AND short-circuits = false ✓

# Result: Correct filtering + null handling automatic!
```

### How to Trust Catalyst (And Debug When It's Wrong)

```python
# Check what Catalyst is doing:
df.explain(extended=False)
# Output shows:
# - PushedFilters: What filters Catalyst pushed to source
# - SelectedFields: What columns Catalyst selected
# - Broadcast: What tables Catalyst broadcast

# Example output:
# == Physical Plan ==
# *(2) HashAggregate ...
# +- Exchange ...
# +- *(1) HashAggregate ...
#    +- *(1) Project [id#123, amount#124]  ← Column pruning!
#       +- *(1) Filter (amount#124 > 100)   ← Predicate pushdown!
#          +- FileScan parquet [id#123, amount#124, ...]

# What to look for:
# ✓ FileScan shows ONLY needed columns
# ✓ Filter appears before FileScan (pushed down)
# ✓ Broadcast shows for small tables
# ✗ FileScan reads all columns (column pruning not working)
# ✗ Filter after FileScan (pushed down failed)
```

### When Catalyst Falls Short

```python
# Catalyst CANNOT optimize across UDFs:
@udf(IntegerType())
def my_func(x):
    return x * 1.1

df.withColumn("result", my_func(col("salary")))
# Catalyst can't look inside my_func
# Must execute for every row (slow!)

# Solution: Use built-in functions when possible
df.withColumn("result", col("salary") * 1.1)
# Catalyst sees the operation, can optimize
```

---

## Pandas UDF vs Python UDF

### Speed Comparison (Know These Numbers!)

```
Scenario: Apply function to 1M rows

Row-by-row Python UDF:
├─ Row 1: Python function executes, returns value
├─ Row 2: Python function executes, returns value
├─ ...
└─ 1M times: Python/JVM boundary crossed
   └─ Each boundary crossing = 0.1-1ms overhead
   └─ Total: 1M × 1ms = 1000 seconds = 16+ minutes!

Vectorized Pandas UDF:
├─ Batch 1: 10,000 rows → Python function → 10,000 results
├─ Batch 2: 10,000 rows → Python function → 10,000 results
├─ ...
└─ 100 batches: Python/JVM boundary crossed only 100 times
   └─ Total: 100 × 1ms = 100 milliseconds!

Speed difference: 1000 seconds vs 0.1 seconds = 10,000x faster!
```

### Implementation: Know This Code

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# ❌ SLOW: Row-at-a-time Python UDF
@udf(DoubleType())
def slow_calculate(salary):
    """Called once per row"""
    return salary * 1.1 + bonus_calculation(salary)

# Time: 1000 seconds for 1M rows
df_slow = df.withColumn("result", slow_calculate(col("salary")))

# ✅ FAST: Vectorized Pandas UDF
@pandas_udf(DoubleType())
def fast_calculate(salaries: pd.Series) -> pd.Series:
    """Called with batches of rows"""
    return salaries * 1.1 + salaries.apply(bonus_calculation)

# Time: 0.1 seconds for 1M rows
df_fast = df.withColumn("result", fast_calculate(col("salary")))

# ✅ BEST: Use built-in Spark function
df_best = df.withColumn("result", col("salary") * 1.1)
# Time: 0.01 seconds (no Python overhead!)
```

### How Pandas UDF Works (Under the Hood)

```
Python UDF (Row-at-a-time):
┌─────────────────────────────────────────────┐
│ Executor JVM                                │
├─────────────────────────────────────────────┤
│ For each row:                               │
│  1. JVM → serialize row to Python bytes     │
│  2. Send to Python process                  │
│  3. Python deserialize → convert to object  │
│  4. Run Python function                     │
│  5. Serialize result back to bytes          │
│  6. JVM deserialize → use result            │
│                                              │
│ Overhead per row: 0.1-1ms (SLOW!)          │
└─────────────────────────────────────────────┘

Pandas UDF (Vectorized, Batched):
┌─────────────────────────────────────────────┐
│ Executor JVM                                │
├─────────────────────────────────────────────┤
│ For each batch of 10,000 rows:              │
│  1. JVM → serialize 10K rows to Arrow       │
│  2. Send to Python process                  │
│  3. Python deserialize → DataFrame          │
│  4. Run vectorized operation (NumPy ops)    │
│  5. Serialize results back to Arrow         │
│  6. JVM deserialize → use results           │
│                                              │
│ Overhead per batch: 1-10ms                  │
│ Per row: 1-10ms ÷ 10,000 = 0.0001-0.001ms  │
│ (1000x faster than row UDF!)                │
└─────────────────────────────────────────────┘
```

### When to Use Each

```
Use BUILT-IN SPARK FUNCTION:
├─ Available for your operation? (99% of cases)
├─ Speed: 0.001ms per operation
└─ Examples: col() * 1.1, when/otherwise, substr, etc.

Use PANDAS UDF (Vectorized):
├─ Built-in not available
├─ AND complex logic (ML, complex math)
├─ Speed: 0.01-0.1ms per operation
├─ Examples: apply ML model to batch, complex business logic
└─ Code:
   @pandas_udf(DoubleType())
   def my_func(batch):
       return complex_vectorized_operation(batch)

Use ROW UDF (Last Resort):
├─ Pandas UDF too complicated
├─ AND can't use built-in
├─ Speed: 0.1-1ms per operation (1000x slower!)
├─ Examples: legacy code, external API calls
└─ Code:
   @udf(DoubleType())
   def my_func(x):
       return external_api.call(x)
```

---

## Snowflake + PySpark: Direct vs S3 Export

### Quick Decision Tree

```
Do you need Snowflake data in PySpark?

├─ Table size < 100MB?
│  └─ YES → DIRECT READ from Snowflake ✅
│     └─ Fast, no extra storage, real-time data
│
├─ Table size 100MB-10GB?
│  ├─ Need real-time data (< 1 hour old)?
│  │  └─ YES → DIRECT READ
│  │     └─ Network overhead acceptable
│  └─ Can tolerate stale data (daily/weekly)?
│     └─ YES → COPY TO S3 ✅
│        └─ Faster processing, reusable, cheaper
│
├─ Table size > 10GB?
│  └─ ALWAYS COPY TO S3 ✅
│     └─ Direct read will be very slow
│     └─ Network bottleneck
│     └─ S3 allows partition pruning
│
└─ Using table multiple times in same job?
   └─ YES → COPY TO S3 ✅
      └─ Avoid repeated network reads
```

---

### Direct Read from Snowflake (Network-Based)

**When to use:**
- Small tables (< 100MB)
- One-time queries
- Real-time data required
- Simple joins

**Architecture:**
```
Snowflake Database
    ↓ (JDBC/Connector)
PySpark Executors
    ↓
Process in memory
```

**Implementation:**
```python
from snowflake.spark import functions as sf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SnowflakeRead").getOrCreate()

# Option 1: Direct SQL read
df = spark.read \
    .format("snowflake") \
    .options({
        "sfUrl": "xy12345.us-east-1.snowflakecomputing.com",
        "sfUser": "username",
        "sfPassword": "password",
        "sfDatabase": "ANALYTICS_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH"
    }) \
    .option("query", "SELECT * FROM dim_customers WHERE active = 1") \
    .load()

# Option 2: Read entire table
df = spark.read \
    .format("snowflake") \
    .options({...}) \
    .option("dbtable", "dim_customers") \
    .load()

# Process
result = df.filter(...).groupBy(...).sum()
```

**Pros:**
```
✅ No data duplication (single source of truth)
✅ Real-time data (not stale)
✅ Simple architecture (no intermediate storage)
✅ Cheaper (no S3 storage costs)
✅ No copy latency
```

**Cons:**
```
❌ Network latency (JDBC overhead)
❌ Snowflake compute costs (parallel extraction)
❌ Slow for large tables (> 10GB)
❌ Can't use partition pruning (unless filtered at source)
❌ Network bottleneck during read
❌ Can't reuse data (read each time)
```

**Performance Numbers:**
```
Table Size | Direct Read Time | Notes
-----------|------------------|-------
50MB       | 5-10 seconds      | Fast, acceptable
500MB      | 30-60 seconds     | Slower, becoming tedious
5GB        | 5-10 minutes      | Very slow, consider S3
50GB       | 30+ minutes       | Unacceptable
```

---

### Copy to S3 First (Storage-Based)

**When to use:**
- Large tables (> 100MB, especially > 1GB)
- Multiple uses in same job
- Frequent queries (daily/weekly)
- Can tolerate data latency
- Performance-critical jobs

**Architecture:**
```
Snowflake Database
    ↓ (UNLOAD to S3)
S3 (Parquet format)
    ↓ (Columnar, partitioned, optimized)
PySpark Executors
    ↓ (Fast parallel reads)
Process in memory
```

**Implementation:**
```python
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SnowflakeToS3:
    def __init__(self, spark, snowflake_config):
        self.spark = spark
        self.snowflake_config = snowflake_config

    def copy_to_s3(self, table_name, s3_path, run_date=None):
        """Copy Snowflake table to S3 in optimized Parquet format"""

        if run_date is None:
            run_date = datetime.now().strftime("%Y-%m-%d")

        try:
            # Step 1: Read from Snowflake (once!)
            logger.info(f"Reading {table_name} from Snowflake...")
            df = self.spark.read \
                .format("snowflake") \
                .options(self.snowflake_config) \
                .option("dbtable", table_name) \
                .load()

            logger.info(f"Loaded {df.count()} records from {table_name}")

            # Step 2: Optimize for PySpark (coalesce files)
            # Parquet optimized: 128MB per file
            partitions_needed = max(1, (df.count() * 128) // (1024**3))
            df_optimized = df.coalesce(partitions_needed)

            # Step 3: Write to S3 in Parquet (fast, columnar)
            logger.info(f"Writing to S3: {s3_path}")
            df_optimized.write \
                .mode("overwrite") \
                .partitionBy("date")  # If date column exists
                .parquet(f"{s3_path}/date={run_date}/")

            logger.info(f"Completed: {table_name} → S3")

            return df_optimized

        except Exception as e:
            logger.error(f"Failed to copy {table_name}: {e}")
            raise

    def read_from_s3(self, s3_path):
        """Read previously copied data from S3 (fast!)"""
        return self.spark.read.parquet(s3_path)

# Usage: Copy once per day
if __name__ == "__main__":
    spark = SparkSession.builder.appName("SF2S3").getOrCreate()

    config = {
        "sfUrl": "xy12345.us-east-1.snowflakecomputing.com",
        "sfUser": "username",
        "sfPassword": "password",
        "sfDatabase": "ANALYTICS_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH"
    }

    s3_copy = SnowflakeToS3(spark, config)

    # Copy once per day
    s3_copy.copy_to_s3(
        "dim_customers",
        "s3://data-lake/dimensions/customers/",
        run_date="2024-01-15"
    )

    # Later in job: read from S3 (fast, multiple times)
    df = s3_copy.read_from_s3("s3://data-lake/dimensions/customers/date=2024-01-15/")
    result = df.join(...).groupBy(...).sum()
```

**Pros:**
```
✅ Fast reads (Parquet optimized for Spark)
✅ Can reuse data (read multiple times, no network overhead)
✅ Supports partition pruning (WHERE date = 2024-01-15)
✅ Scalable to very large tables (100GB+)
✅ Column projection (only read needed columns)
✅ Better for complex transformations
✅ Cheaper Snowflake compute (one UNLOAD per day)
```

**Cons:**
```
❌ Storage cost (data duplication in S3)
❌ Copy latency (5-10 minutes for large tables)
❌ Data staleness (not real-time, daily/weekly copy)
❌ Extra ETL complexity (copy step required)
❌ Requires cleanup (old files in S3)
```

**Performance Numbers:**
```
Table Size | Direct Read | Copy to S3 | S3 Read (after) | Recommendation
-----------|-------------|------------|-----------------|---------------
50MB       | 5s          | 30s copy   | 1s              | Direct read
500MB      | 30s         | 2m copy    | 5s              | If used 3+ times
5GB        | 5m          | 5m copy    | 20s             | Copy to S3 ✅
50GB       | 30m+        | 10m copy   | 1m              | Copy to S3 ✅
```

---

### Cost Comparison

```
Scenario: 5GB Snowflake table, PySpark job runs daily

OPTION A: Direct Read (Network)
├─ Network transfer: 5GB → expensive per run
├─ Snowflake compute: ~5 minutes per run ($0.40/credit × credits)
├─ Cost per run: ~$2
├─ Cost per month: $2 × 30 = $60/month
├─ Data freshness: Real-time ✅
└─ Total: $60/month + network

OPTION B: Copy to S3 (Storage)
├─ S3 storage: 5GB × 30 days = 150GB = ~$3/month
├─ Copy overhead: Snowflake UNLOAD ~5 min = $0.40 per day = $12/month
├─ PySpark reads: Free (S3 to EC2 same region)
├─ Cost per month: $3 + $12 = $15/month
├─ Data freshness: Daily (acceptable) ✅
└─ Total: $15/month (4x cheaper!)

WINNER: Copy to S3 for large tables!
```

---

### Hybrid Approach (Best of Both)

```python
def smart_snowflake_read(table_name, size_mb, cache_s3_path=None):
    """Read from Snowflake smart: direct if small, S3 if large"""

    if size_mb < 100:
        # Small table: Direct read is fine
        logger.info(f"Direct read (size: {size_mb}MB)")
        return spark.read \
            .format("snowflake") \
            .options(snowflake_config) \
            .option("dbtable", table_name) \
            .load()

    elif cache_s3_path and s3_file_exists(cache_s3_path):
        # Large table AND cache exists: Read from S3 (fast!)
        logger.info(f"Cache hit! Reading from S3: {cache_s3_path}")
        return spark.read.parquet(cache_s3_path)

    else:
        # Large table, no cache: Copy to S3 first
        logger.info(f"No cache, copying {table_name} to S3...")

        df = spark.read \
            .format("snowflake") \
            .options(snowflake_config) \
            .option("dbtable", table_name) \
            .load()

        df.coalesce(10).write \
            .mode("overwrite") \
            .parquet(cache_s3_path)

        logger.info(f"Cached to: {cache_s3_path}")
        return df

# Usage
customers = smart_snowflake_read(
    "dim_customers",
    size_mb=5000,  # 5GB
    cache_s3_path="s3://cache/dim_customers/"
)
```

---

### Quick Configuration

```python
# Snowflake connector config
SNOWFLAKE_CONFIG = {
    "sfUrl": "xy12345.us-east-1.snowflakecomputing.com",
    "sfUser": "pyspark_user",
    "sfPassword": "secure_password",  # Use AWS Secrets Manager!
    "sfDatabase": "ANALYTICS_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "DATA_ENGINEER"
}

# Optimization configs
SPARK_CONFIGS = {
    # Parallel read from Snowflake (faster copy)
    "spark.sql.files.maxPartitionBytes": "128mb",

    # Snowflake specific tuning
    "snowflake.spark.snowflake_jdbc_use_aws_credentials": "true",  # Use IAM
}
```

---

## Production On-Site Patterns

### Pattern 1: Daily Incremental ETL

```python
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class DailyETL:
    def __init__(self, spark):
        self.spark = spark
        self.spark.conf.set("spark.sql.shuffle.partitions", "100")

    def run(self, run_date=None):
        """Run daily ETL: extract → transform → load"""
        if run_date is None:
            run_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # 1. Extract with validation
            logger.info(f"Extracting data for {run_date}")
            raw = self.spark.read\
                .option("columnNameOfCorruptRecord", "_corrupt_record")\
                .parquet(f"s3://raw/date={run_date}/")

            # Remove corrupted records
            raw = raw.filter(col("_corrupt_record").isNull())\
                     .drop("_corrupt_record")
            logger.info(f"Extracted {raw.count()} records")

            # 2. Transform with error handling
            logger.info("Transforming data")
            required_fields = ["id", "amount", "timestamp"]
            for field in required_fields:
                if field not in raw.columns:
                    raise ValueError(f"Missing required field: {field}")

            processed = raw\
                .dropna(subset=required_fields)\
                .filter(col("amount") > 0)\
                .filter(col("amount") < 1000000)\
                .withColumn("value", col("amount") * 1.10)

            # 3. Load with partitioning
            logger.info("Loading data")
            processed.write\
                .mode("overwrite")\
                .partitionBy("load_date")\
                .parquet(f"s3://processed/date={run_date}/")

            logger.info(f"ETL complete for {run_date}")
            return processed

        except Exception as e:
            logger.error(f"ETL failed for {run_date}: {e}")
            raise

# Usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("DailyETL").getOrCreate()
    etl = DailyETL(spark)
    etl.run("2024-01-15")
```

### Pattern 2: Fast Joins (Always Use Broadcast)

```python
from pyspark.sql.functions import broadcast

def smart_join(df_large, df_dimension, broadcast_threshold_mb=100):
    """Join large table with dimension, using broadcast when possible

    Args:
        df_large: Large table to join
        df_dimension: Small dimension table
        broadcast_threshold_mb: Size limit for broadcasting (default: 100MB)
                               Adjust based on executor memory:
                               - 2GB executor → 10MB
                               - 4GB executor → 100MB
                               - 8GB executor → 500MB
    """

    dim_size_mb = df_dimension.rdd\
        .map(lambda x: len(str(x)))\
        .sum() / (1024 * 1024)

    if dim_size_mb < broadcast_threshold_mb:
        logger.info(f"Broadcasting dimension table ({dim_size_mb:.1f}MB)")
        # For medium tables (100MB-500MB), you can increase threshold:
        # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")
        return df_large.join(broadcast(df_dimension), "id")
    else:
        logger.warning(f"Dimension too large ({dim_size_mb:.1f}MB), using sort-merge join")
        logger.warning(f"  Tip: Increase threshold to {dim_size_mb*1.2:.0f}MB if executor memory allows")
        return df_large.join(df_dimension, "id")

# Usage
orders = spark.read.parquet("orders_100gb")
customers = spark.read.parquet("customers_500mb")

result = smart_join(orders, customers)
```

### Pattern 3: Quick Debugging (Check Execution Plan)

```python
def debug_performance(df, description=""):
    """Print execution plan to understand bottlenecks"""
    print(f"\n{'='*80}")
    print(f"DEBUG: {description}")
    print('='*80)

    # Show logical plan
    df.explain(extended=False)

    # Show physical plan with costs
    df.explain(extended=True)

    # Show actual execution stats
    df.show(1)  # Triggers execution

    print('='*80)
```

---

## Troubleshooting Checklist

### Problem: "Job ran out of memory" (GC overhead limit exceeded)

**Diagnosis:**
```
Spark UI → Executor tab
├─ Memory usage: > 1.5GB?
├─ GC time > 20%?
└─ If yes → Memory spill happening
```

**Solutions (in order of preference):**

```python
# 1. Pre-filter data BEFORE expensive operations
df = spark.read.parquet("100gb_file")\
    .filter(col("date") == "2024-01-15")  # Reduce to 10GB
    .groupBy("category").sum()  # Shuffle 10GB, not 100GB

# 2. Reduce shuffle partitions (less memory per partition)
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Default 200

# 3. Use broadcast for small tables
df_large.join(broadcast(df_small), "id")

# 4. Increase executor memory
spark.conf.set("spark.executor.memory", "16g")  # Up from 4g

# 5. Increase number of partitions (spread load)
df = df.repartition(500)  # More partitions = less data per partition
```

---

### Problem: "Job is very slow" (10+ minutes for small job)

**Diagnosis:**
```
Spark UI → Stages tab
├─ Shuffle Write: > 10GB?
│  └─ YES → Shuffle is the problem
├─ Shuffle Read: Very different from Write?
│  └─ YES → Data skew or many failed tasks
├─ Task duration: > 30 seconds each?
│  └─ YES → GC, network, or computation issue
└─ Many tiny output files?
   └─ YES → Coalesce before write
```

**Solutions:**

```python
# If shuffle is problem:
df = df.filter(col("amount") > 0)  # Pre-filter
       .repartition(100)            # Even distribution
       .groupBy("category").sum()

# If computation is problem:
df.explain()  # Check for UDFs
# Replace with built-in functions or pandas UDF

# If output files problem:
df.coalesce(10).write.parquet("s3://output/")  # 10 files instead of 100

# If network problem (many tiny partitions):
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Default 200 is too much
```

---

### Problem: "Data skew" (VARIANCE in task duration = some tasks 10-30x slower)

**Diagnosis (Critical Difference):**
```
DATA SKEW (HIGH VARIANCE):
├─ Check Spark UI → Stages → Task Duration
├─ Most tasks: 10 seconds
├─ Some tasks: 200+ seconds (20x slower!)
├─ Median: 10s, Max: 200s, Variance: HIGH ← SKEW!
│
└─ Cause: Uneven key distribution
   ├─ Some keys have 90% of the data
   ├─ Other keys have 10% of the data
   └─ Some tasks process 1GB, others process 100MB

UNIFORM SLOWNESS (LOW VARIANCE):
├─ Check Spark UI → Stages → Task Duration
├─ All tasks: 40-45 seconds (similar)
├─ Median: 42s, Max: 45s, Variance: LOW ← NOT SKEW!
│
└─ Causes: Not data skew, but...
   ├─ Memory spill (all tasks affected equally)
   ├─ Network bottleneck
   ├─ GC pressure (all tasks pause together)
   └─ Slow computation (all tasks slow)
```

**To Confirm Skew:**
```python
# Look at Spark UI:
# 1. Stages tab → Select the slow stage
# 2. Click "Show Additional Metrics" → Task Size
# 3. If max task size >> min task size = SKEW!
#    Example: max 1000MB, min 10MB = 100x skew!

# Programmatic check:
df.groupBy("join_key").count() \
    .agg(max(col("count")), min(col("count")), avg(col("count"))) \
    .show()
# If max >> avg = skew!
# Example: max 100M, min 100K, avg 1M = SKEW!
```

**Solutions:**

```python
# 1. IDENTIFY the skewed key
df.groupBy("category").count().orderBy(col("count").desc()).show()
# If one category has >> 90% of data = FOUND IT!

# 2. PRE-FILTER to remove skewed data
df = df.filter(col("category") != "UNKNOWN")

# 3. OR SEPARATE PROCESSING for skewed group
normal = df.filter(col("category") != "UNKNOWN")\
    .groupBy("category").sum()

skewed = df.filter(col("category") == "UNKNOWN")\
    .groupBy("sub_category").sum()  # More granular grouping

result = normal.union(skewed)

# 4. Or salt the key (add random suffix to distribute)
from pyspark.sql.functions import rand, floor

df = df.withColumn(
    "salted_key",
    concat(col("category"), lit("_"), floor(rand() * 10))
)
result = df.groupBy("salted_key").sum()\
    .groupBy(substring("salted_key", 1, length("salted_key")-2)).sum()
```

---

### Problem: "Output files are slow to read" (thousands of tiny files)

**Diagnosis:**
```
S3 Console → Look at output folder
├─ 500+ files?
├─ Each file < 10MB?
└─ YES → Too many small files
```

**Solutions:**

```python
# ✗ BAD: Default write creates one file per partition
df.write.mode("overwrite").parquet("s3://output/")
# Result: 500 files × 5MB = many small files

# ✓ GOOD: Coalesce before write
df.coalesce(10).write.mode("overwrite").parquet("s3://output/")
# Result: 10 files × 250MB = fast reads

# ✓ BEST: Coalesce + Partition
df.coalesce(10).write\
    .mode("overwrite")\
    .partitionBy("date")  # Organize by date
    .parquet("s3://output/")
```

---

## Interview Questions & Answers

### Q1: "Explain how Spark distributes a 100GB file across 10 executors"

**Answer:**
```
1. Parquet file: 100GB with 128MB row groups
   → Auto-creates 780 partitions (100GB ÷ 128MB)

2. Spark has 10 executors with 8 cores each
   → 80 cores can process 80 partitions in parallel

3. Processing:
   - Partition 1-80: Processed by cores 1-80 in parallel (first wave)
   - Partition 81-160: Processed in second wave
   - ... until all 780 partitions processed
   - Total waves: 780 ÷ 80 = ~10 waves

4. Each executor processes 78 partitions (780 ÷ 10)
   - Partition loaded (128MB)
   - Processed using 1.5GB execution memory
   - Output written to disk
   - Memory freed, next partition loaded
   - Repeat 78 times

5. Execution memory shared across 8 cores on each executor
   - Not divided per core
   - All cores update same shared hash table
   - Memory pressure = combined from all tasks
```

---

### Q2: "Why would you use Broadcast Join instead of regular join?"

**Answer:**
```
Data movement:

Regular Join (100GB table + 500MB table):
├─ 100GB shuffled across network
├─ 500MB shuffled across network
└─ Total: 100.5GB data movement ❌

Broadcast Join (100GB table + 500MB table):
├─ 100GB NOT shuffled
├─ 500MB copied to all executors (10 executors = 5GB memory use)
├─ Join happens locally
└─ Total: 500MB data movement (200x less!) ✅

Speed comparison:
├─ Network I/O: 100.5GB ÷ 1Gbps = ~1600 seconds (regular join)
├─ Memory cost: 500MB × 10 = 5GB (broadcast join)
├─ Result: Broadcast is 200x faster! (network bottleneck)

IMPORTANT: Spark's Broadcast Threshold
├─ Default threshold: spark.sql.autoBroadcastJoinThreshold = 10MB
├─ Auto-broadcast if table < 10MB (automatic!)
├─ For 500MB table, must either:
│  1. Explicitly call broadcast(df_small)
│  2. OR increase threshold:
│     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")
│
├─ ⚠️ WARNING: Don't set threshold too high!
│  └─ If threshold = 1GB, broadcast uses 1GB × 10 executors = 10GB memory
│  └─ If executor memory = 2GB, you'll get OOM errors
│  └─ Safe rule: threshold should be < executor_memory / 2

When to use Broadcast:
├─ Small table < 10MB: Catalyst broadcasts automatically ✅
├─ Medium table 10MB-500MB: Increase threshold if memory allows
├─ Large table > 1GB: Never broadcast (use sort-merge join)
```

---

### Q3: "How does Catalyst optimize column selection?"

**Answer:**
```
Example: Read 100-column Parquet file, select only 2 columns

Without Catalyst:
├─ Read all 100 columns from S3 (entire file)
├─ Bring into memory
├─ Select only 2 columns
└─ Result: Read 100GB to get 2GB of data (99% wasted!)

With Catalyst (Column Pruning):
├─ Analyze query: only "name" and "age" needed
├─ BEFORE reading: tell Parquet which columns needed
├─ Parquet skips 98 columns at file level
├─ Read only 2 columns = 2GB
└─ Result: 50x less data read!

Why it's efficient:
- Parquet is columnar format (stores columns separately)
- Catalyst sees full query plan lazily
- Pushes column selection down to source
- Source (Parquet) can skip entire column blocks
```

---

### Q4: "Explain memory spill and how to prevent it"

**Answer:**
```
Memory Spill:

What happens:
├─ GroupBy on 1GB file with 100M unique keys
├─ Each key: 40 bytes (hash table entry)
├─ Total hash table: 100M × 40 bytes = 4GB
├─ Executor memory: 1.5GB execution memory
├─ 4GB > 1.5GB = SPILL!

When spill happens:
├─ Write 2.5GB to disk (2 seconds)
├─ Read back from disk (2 seconds)
├─ Total: 10 seconds → 40 seconds (4x slower!)

Prevention (in order):

1. Pre-filter data (reduce unique keys):
   df.filter(col("amount") > 0)  # 100M → 50M rows
   → 50M keys × 40 bytes = 2GB (fits in 1.5GB with buffers!)

2. Use broadcast for small dimensions:
   df.join(broadcast(dim), "key")  # No shuffle, no spill

3. Increase executor memory:
   spark.conf.set("spark.executor.memory", "16g")

4. Repartition to spread data:
   df.repartition(200)  # More partitions = less per partition
```

---

### Q5: "What's the difference between coalesce and repartition?"

**Answer:**
```
coalesce:
├─ Merges partitions WITHOUT shuffle
├─ Narrow transformation (no data movement)
├─ Usage: df.coalesce(10) - merge 100 partitions into 10
├─ Fast (no network)
├─ Can only REDUCE partition count
└─ Example: 100 partitions → 10 partitions

repartition:
├─ Redistributes data to new partition count
├─ Wide transformation (requires shuffle!)
├─ Usage: df.repartition(100) - shuffle into 100 partitions
├─ Slow (network I/O)
├─ Can increase OR decrease partition count
└─ Example: 100 partitions → 200 partitions

When to use:

coalesce:
├─ Merging small output files
├─ Reducing partition count before write
└─ Use this 95% of the time!

repartition:
├─ Fixing data skew
├─ Increasing parallelism
├─ Ensuring even distribution
└─ Use only when necessary
```

---

### Q6: "How would you optimize a job that's running out of shuffle memory?"

**Answer:**
```
Diagnosis:
├─ Check Spark UI: high shuffle write/read
├─ Check logs: spill warnings
└─ Check memory: execution memory > 1.5GB

Solutions (in order):

1. Pre-filter BEFORE shuffle:
   df = df.filter(col("date") == "2024-01-15")  # Reduce data
        .filter(col("amount") > 0)
        .groupBy("category").sum()

2. Reduce shuffle partitions:
   spark.conf.set("spark.sql.shuffle.partitions", "50")
   # Default 200 is for large clusters, overkill for small jobs

3. Use broadcast instead of shuffle:
   df_large.join(broadcast(df_small), "id")
   # No shuffle = no memory pressure

4. Increase executor memory:
   spark.conf.set("spark.executor.memory", "16g")

5. Cache intermediate results:
   df_filtered = df.filter(...).cache()
   # Forces evaluation, prevents re-computation
   df_result = df_filtered.groupBy(...).sum()
```

---

### Q7: "When would you use a UDF vs built-in function?"

**Answer:**
```
Decision tree:

Built-in Spark function available?
├─ YES → Use it! (100-1000x faster)
│  └─ Example: col("salary") * 1.1 instead of custom UDF
│
└─ NO → What's the complexity?
   ├─ Simple operations (math, string ops)
   │  └─ Use Spark SQL expressions or when/otherwise
   │
   └─ Complex business logic (ML, external APIs)
      ├─ Can vectorize? (process batches)
      │  └─ Use Pandas UDF (100x faster than row UDF!)
      │
      └─ Can't vectorize? (needs external API)
         └─ Use row UDF (slowest, but only option)

Examples:

Use built-in:
├─ col("salary") * 1.1
├─ when(col("status") == "active", col("revenue")).otherwise(0)
├─ substring(col("email"), 1, 10)
└─ year(col("date"))

Use Pandas UDF:
├─ Apply ML model to batch of customers
├─ Complex vector operations (NumPy)
├─ ML feature engineering
└─ @pandas_udf(DoubleType())
   def my_func(batch):
       return batch * 1.1  # Vectorized!

Use row UDF (last resort):
├─ Call external REST API
├─ Legacy Python code
├─ Can't express in SQL
└─ @udf(DoubleType())
   def my_func(x):
       return external_api.call(x)
```

---

## Performance Tuning Decision Tree

```
START: Job is slow

├─ Is it slow because of reading data?
│  ├─ YES → Check Spark UI Stages
│  │  ├─ Reading unnecessary columns?
│  │  │  └─ Use select() to prune → Catalyst column pruning
│  │  └─ Reading unnecessary rows?
│  │     └─ Use filter() early → Catalyst predicate pushdown
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of shuffle?
│  ├─ YES → Spark UI shows high "Shuffle Write"
│  │  ├─ Can you reduce partitions?
│  │  │  └─ spark.conf.set("spark.sql.shuffle.partitions", "50")
│  │  ├─ Can you broadcast small table?
│  │  │  └─ join(broadcast(df_small), "id")
│  │  └─ Can you pre-filter?
│  │     └─ df.filter(...).groupBy(...).sum()
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of joins?
│  ├─ YES → Which type of join?
│  │  ├─ Both tables large?
│  │  │  └─ Use Sort-Merge (default)
│  │  └─ One table < 10MB?
│  │     └─ Use broadcast(small_table)
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of too many output files?
│  ├─ YES → 1000+ small files?
│  │  └─ df.coalesce(10).write...
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of data skew?
│  ├─ YES → Some tasks 10x slower than others?
│  │  ├─ Pre-filter skewed data
│  │  ├─ Process skewed/normal separately
│  │  └─ Or salt the key
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of UDFs?
│  ├─ YES → Using Python row UDF?
│  │  ├─ Can you use built-in function?
│  │  │  └─ YES: Use built-in (1000x faster!)
│  │  └─ Otherwise use Pandas UDF (100x faster)
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of memory pressure?
│  ├─ YES → Executor memory > 1.5GB or GC warnings?
│  │  ├─ Pre-filter before operations
│  │  ├─ Increase executor memory
│  │  └─ Use broadcast for joins
│  │
│  └─ NO → Continue...
│
├─ Is it slow because of network?
│  ├─ YES → Check if data locality OK
│  │  ├─ Data in same region as cluster? (S3 in us-east-1)
│  │  └─ Otherwise: slow network transfer inevitable
│  │
│  └─ NO → Continue...
│
└─ Unknown → Profile with explain()
   df.explain(extended=True)
   → Check PushedFilters, SelectedFields, Broadcast, join types
```

---

## Quick Configuration Reference

```python
# BROADCAST TUNING (CRITICAL!)
# Default: 10MB (auto-broadcasts tables < 10MB)
# Problem: Medium tables (100MB-500MB) not auto-broadcast, require shuffle
# Solution: Increase threshold based on executor memory

# If executor memory = 2GB:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")  # Keep default

# If executor memory = 4GB (common):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100mb")  # Broadcast up to 100MB

# If executor memory = 8GB (large):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500mb")  # Broadcast up to 500MB

# If executor memory = 16GB (very large):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1gb")  # Broadcast up to 1GB

# ⚠️ Safety rule: threshold < executor_memory / 2
# Example: 4GB executor → max threshold = 2GB (but use 500mb for safety)

# JOIN STRATEGY TUNING
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")  # Default true
# true = prefer sort-merge (efficient, handles large tables)
# false = use shuffle hash join (less stable)

# OTHER PERFORMANCE TUNING
spark.conf.set("spark.sql.shuffle.partitions", "100")  # Default 200
spark.conf.set("spark.sql.files.maxPartitionBytes", "128mb")  # Parquet read size
spark.conf.set("spark.executor.memory", "16g")  # Default 4g
spark.conf.set("spark.executor.cores", "4")  # Default 1
spark.conf.set("spark.executor.instances", "10")  # Number of executors

# Memory settings
spark.conf.set("spark.memory.fraction", "0.6")  # Execution + Storage memory
spark.conf.set("spark.memory.storageFraction", "0.5")  # Storage as % of memory.fraction

# Shuffle optimization
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.shuffle.memoryFraction", "0.2")  # Memory for shuffle buffers

# S3 optimization
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB chunks
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
```

---

## Key Takeaways (Memorize These)

✅ **Partitions = parallel processing units. 100GB file = ~780 partitions = ~10 partitions per executor.**

✅ **Execution memory (1.5GB) processes data stream-by-stream, not all-at-once.**

✅ **Broadcast join when small table < autoBroadcastJoinThreshold (default: 10MB, can increase to 500MB).**
   - Default 10MB for safety
   - Increase to 100-500MB if executor memory allows
   - Memory cost: table_size × num_executors (e.g., 500MB × 10 = 5GB)
   - 10-100x faster than shuffle!

✅ **Pre-filter BEFORE expensive operations (reduce data early).**

✅ **Catalyst automatically optimizes: column pruning, predicate pushdown, join reordering.**

✅ **Shuffle is slow: avoid when possible with broadcast, pre-filter, or coalesce.**

✅ **Use built-in Spark functions instead of UDFs (1000x faster).**

✅ **If must use UDF: pandas_udf (100x faster than row UDF).**

✅ **Coalesce output files before write (10 large files > 1000 small files).**

✅ **Check explain() to see what Catalyst is doing.**

✅ **DATA SKEW = High variance in task duration (max task >> min task), not all tasks slow.**
   - Skew: Task 1 = 10s, Task 2 = 200s (20x variance!) = SKEW ❌
   - Normal: Task 1 = 42s, Task 2 = 44s (low variance) = Not skew ✅
   - Fix: Pre-filter skewed keys or process separately by key group

---

## The 5 Patterns That Get You Hired

1. **Smart Joins:** Always broadcast small tables
2. **Efficient Filtering:** Pre-filter before shuffle
3. **Optimized Output:** Coalesce before writing
4. **Error Handling:** Retry logic + validation
5. **Performance Debugging:** Check explain() + Spark UI

Master these 5 patterns, and you're a professional big data engineer.

---

**Created:** June 2026 | **For:** Certified PySpark/AWS/Python Developers | **Status:** Production-Ready Interview Prep
