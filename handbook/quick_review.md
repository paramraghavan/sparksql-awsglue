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
7. [Production On-Site Patterns](#production-on-site-patterns)
8. [Troubleshooting Checklist](#troubleshooting-checklist)
9. [Interview Questions & Answers](#interview-questions--answers)
10. [Performance Tuning Decision Tree](#performance-tuning-decision-tree)

---

## Decision Trees (5-Second Answers)

### "Should I use Broadcast Join?"

```
Is one table < 1GB?
├─ YES → BROADCAST JOIN (no shuffle, 10-100x faster) ✅
└─ NO → SORT-MERGE JOIN (default, both shuffled)
```

### "Why is my job slow?"

```
Check Spark UI Stages tab
├─ High "Shuffle Write" bytes?
│  └─ YES → Shuffle is the problem → [See "Avoiding Shuffle Hell"](#avoiding-shuffle-hell)
├─ Task taking >30sec each?
│  └─ YES → Data skew or memory spill → [See "Troubleshooting"](#troubleshooting-checklist)
├─ Lots of small output files?
│  └─ YES → Coalesce before write → `df.coalesce(10).write.parquet(...)`
└─ Join operation slow?
   └─ YES → [See "Join Strategies"](#join-strategies-choose-wisely)
```

### "Should I use a UDF?"

```
Do built-in Spark functions exist for this?
├─ YES → Use them! (100-1000x faster) ✅
└─ NO → Complex business logic only?
   ├─ YES → Use Pandas UDF (vectorized, 10-100x faster than row UDF)
   └─ NO → Row-at-a-time UDF (slowest, but simplest)
```

### "Is my job memory-constrained?"

```
Spark UI → Executor Memory
├─ Using > 1.5GB execution memory?
│  └─ YES → Spill happening → Pre-filter or increase executor memory
├─ Using < 500MB?
│  └─ YES → You have room → Can coalesce to fewer partitions
└─ Between 500MB-1.5GB?
   └─ YES → Optimized. Watch for spill warnings in logs.
```

### "Should I increase executor memory or cores?"

```
Current bottleneck: (check Spark UI)
├─ Task duration > 30 seconds
│  └─ → Increase CORES (more parallelism)
├─ GC warnings or spill messages
│  └─ → Increase MEMORY (more execution space)
└─ Both slow
   └─ → Increase both + pre-filter data
```

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

| Table Size | Join Type | Strategy | Speed | Why |
|-----------|-----------|----------|-------|-----|
| < 10MB | Any | **Broadcast** | ⚡⚡⚡ 10-100x faster | No shuffle, small table cached on all executors |
| 10MB-1GB | Any | Broadcast if memory allows | ⚡⚡ 5-50x faster | Still fits in memory, but slower broadcast |
| 1GB-100GB | Large ⊕ Small | **Broadcast small** | ⚡⚡ 10x faster | Broadcast small, shuffle large only |
| 1GB-100GB | Large ⊕ Large | **Sort-Merge** | ⚡ 1x baseline | Both tables shuffled, but efficient sort |
| > 100GB | Any | Careful! | ⚠️ 0.1x | Massive shuffle, potential spill |

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
   ├─ Table size < spark.sql.autoBroadcastJoinThreshold (10MB default)
   ├─ AND enough memory to broadcast
   └─ YES → Use Broadcast Hash Join

2. Should I use Sort-Merge?
   ├─ Both tables > 10MB
   ├─ AND spark.sql.join.preferSortMergeJoin = true (default)
   └─ YES → Use Sort-Merge Join

3. Fallback to Shuffle Hash Join
   ├─ Both tables shuffled by join key
   ├─ Hash tables built on each executor
   └─ Slowest but always works
```

### Memory Cost of Joins

```
Broadcast Join (small table 1GB):
├─ Driver memory: 1GB (to collect small table)
├─ Executor memory per executor: 1GB (cached broadcast)
└─ Total: 1GB + (10 executors × 1GB) = 11GB

Sort-Merge Join (both tables 100GB):
├─ Shuffle write: 100GB + 100GB = 200GB to disk
├─ Shuffle read: 200GB from disk back to executors
├─ Execution memory per executor: ~2GB (for sorting)
└─ Total: 200GB data movement + memory for sorting

Lesson: Broadcast when possible to save data movement!
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

# Even better: Catalyst broadcasts small_1gb automatically!
# If small_1gb < 10MB, uses broadcast join (no shuffle)
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

def smart_join(df_large, df_dimension):
    """Join large table with dimension, using broadcast when possible"""

    dim_size_mb = df_dimension.rdd\
        .map(lambda x: len(str(x)))\
        .sum() / (1024 * 1024)

    if dim_size_mb < 100:  # Less than 100MB
        logger.info(f"Broadcasting dimension table ({dim_size_mb:.1f}MB)")
        return df_large.join(broadcast(df_dimension), "id")
    else:
        logger.warning(f"Dimension too large ({dim_size_mb:.1f}MB), regular join")
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

### Problem: "Data skew" (some partitions much slower than others)

**Diagnosis:**
```
Spark UI → Stages → Look at task duration
├─ Most tasks: 10 seconds
├─ Few tasks: 200 seconds (20x slower!)
└─ YES → Data skew
```

**Solutions:**

```python
# 1. Find skewed column
df.groupBy("category").count().show()
# If one category has 90% of data = skew!

# 2. Pre-filter to remove skewed data
df = df.filter(col("category") != "UNKNOWN")

# 3. Or separate processing for skewed group
normal = df.filter(col("category") != "UNKNOWN")\
    .groupBy("category").sum()

skewed = df.filter(col("category") == "UNKNOWN")\
    .groupBy("sub_category").sum()

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

Regular Join (100GB table + 1GB table):
├─ 100GB shuffled across network
├─ 1GB shuffled across network
└─ Total: 101GB data movement

Broadcast Join (100GB table + 1GB table):
├─ 100GB NOT shuffled
├─ 1GB copied to all executors (fits in memory)
├─ Join happens locally
└─ Total: 1GB data movement (100x less!)

When to use Broadcast:
├─ Small table < 10MB: Always broadcast
├─ Small table 10MB-1GB: Broadcast if memory available
├─ Large table > 1GB: Use regular join or sort-merge
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
# Performance tuning configs
spark.conf.set("spark.sql.shuffle.partitions", "100")  # Default 200
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")  # Default 10MB
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

✅ **Broadcast join when small table < 10MB (no shuffle = 10-100x faster).**

✅ **Pre-filter BEFORE expensive operations (reduce data early).**

✅ **Catalyst automatically optimizes: column pruning, predicate pushdown, join reordering.**

✅ **Shuffle is slow: avoid when possible with broadcast, pre-filter, or coalesce.**

✅ **Use built-in Spark functions instead of UDFs (1000x faster).**

✅ **If must use UDF: pandas_udf (100x faster than row UDF).**

✅ **Coalesce output files before write (10 large files > 1000 small files).**

✅ **Check explain() to see what Catalyst is doing.**

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
