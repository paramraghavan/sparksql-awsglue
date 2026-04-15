# SparkMonitor Metrics Explained

## What Are These Metrics?

When you run `%%measure`, SparkMonitor shows a "Full metrics" table with detailed information about your Spark job. This guide explains each metric, what it means, and when to worry.

---

## The Metrics Table

### 1. Total Time
**What it is:** Wall-clock time from when the cell started to when it finished

**Good range:** Depends on your data size. Any consistent time is fine.

**Example:**
- Reading 10GB → 5-30 seconds is normal
- Reading 1GB → 1-5 seconds is normal

**When to worry:** If unexpectedly slow, check other metrics for clues

---

### 2. Parallel Tasks Used
**What it is:** Number of parallel pieces Spark divided your work into

**Good range:** 50 to 2000 tasks (usually)

**What it means:**
```
High tasks (200+) = Good parallelism, work is distributed
Low tasks (< 10) = Cluster might be underused, could be faster

Example:
- Reading 1000 partitions → 1000 tasks (excellent!)
- Reading 2 partitions → 2 tasks (underusing cluster)
```

**When to worry:** 🟡 **YELLOW WARNING** if:
- You have > 100 MB data AND < 10 tasks
- This means your cluster's CPUs are idle

**How to fix:**
```python
# Increase parallelism
df = spark.read.parquet("s3://...").repartition(50)  # Now 50 tasks
```

---

### 3. Data Read from S3 / HDFS
**What it is:** Total bytes your job read from storage

**Good range:** As small as possible (smaller data = faster)

**Example:**
```
Read 100 MB → Data is reasonably sized
Read 50 GB → Large job, expect slower time
```

**When to worry:** If surprisingly high, you might be:
- Reading entire table instead of filtered portion
- Reading data multiple times (should cache)

**How to fix:**
```python
# Add filters BEFORE expensive operations
df = spark.read.parquet("s3://...") \
    .filter(df.year == 2024)  # Filter first, read less data
```

---

### 4. Data Written to S3 / HDFS
**What it is:** Total bytes your job wrote to storage

**Good range:** Usually zero (unless you're saving data)

**When to worry:** High write usually means:
- You're saving results (that's fine!)
- Or Spark is writing intermediate shuffles to disk (not ideal)

**See also:** Check "Spilled to disk" for intermediate write info

---

### 5. Network Shuffle (join / groupBy cost)
**What it is:** Data moved between cluster nodes during joins and groupBys

**Good range:** Zero is best (no shuffling = fast)

**When to worry:** 🟡 **YELLOW WARNING** if:
- > 500 MB of shuffle

**What it means:**
```
Zero shuffle = Quick operations (filter, select, map)
Small shuffle (< 100 MB) = Simple groupBy or join on small key
Large shuffle (500 MB+) = Expensive operation, network bottleneck

Example timeline:
```
Stage 0: Read → no shuffle
Stage 1: GroupBy region → 100 MB shuffle (each region buckets)
Stage 2: Join → 500 MB shuffle (combining large tables)
```

**How to fix:**
```python
# Broadcast small tables to avoid shuffle
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), on="key")

# Or use skew hints
large_df.hint("skew", "key").join(other_df, on="key")
```

---

### 6. Spilled to Disk
**What it is:** Data that didn't fit in executor memory and was written to disk

**Good range:** **ZERO** (any spill is a problem)

**When to worry:** 🟡 **YELLOW WARNING** if:
- Any value > 0 = You're hitting memory pressure

**What it means:**
```
0 bytes spilled = Excellent, all operations fit in memory
10 MB spilled = Memory was tight, but manageable
100+ MB spilled = Serious memory pressure, job is slow
```

**How to fix:**
```python
# Option 1: Increase executor memory
spark.conf.set("spark.executor.memory", "8g")  # was 4g

# Option 2: Reduce data per executor
df = df.repartition(200)  # More partitions = smaller per-executor chunks

# Option 3: Both!
# Use smaller partitions AND more memory
```

---

### 7. Spilled from Memory (RAM Pressure)
**What it is:** Data shuffled between memory pools because RAM was running low

**Good range:** **ZERO** (any memory pressure is a warning)

**When to worry:** 🟡 **YELLOW WARNING** if:
- Any value > 0 = Memory pressure building up

**What it means:**
```
Timeline of memory problems:
1. Spilled from memory (current state) - RAM pools are crowded
2. Spilled to disk (next state) - if not fixed, will happen next run
3. Job crashes (final state) - if memory still not enough
```

**How to fix (same as disk spill):**
```python
# Increase memory OR reduce data per partition
spark.conf.set("spark.executor.memory", "8g")
df = df.repartition(100)
```

---

### 8. GC Time (Memory Housekeeping)
**What it is:** Time the JVM spent garbage collecting (cleaning up unused objects)

**Format:** Shown as both milliseconds AND percentage of total time

**Example:** `4250 ms  (8% of run time)`

**Good range:** < 10% of run time

**When to worry:** 🟡 **YELLOW WARNING** if:
- > 10% of your run time = Excessive garbage collection

**What it means:**
```
Low GC (< 5%) = Healthy, JVM isn't struggling with memory
Normal GC (5-10%) = Fine, expected background work
High GC (> 10%) = Problem! JVM is constantly cleaning up

Analogy:
- 2% GC = Clean office, brief tidying
- 50% GC = Constantly sweeping, can't work efficiently
```

**Why it happens:**
```
Most common causes:

1. Python UDFs (most common!)
   - Java wrapper creates many temporary objects
   - Fix: Use Scala UDFs or vectorized operations

2. Memory pressure (second most common)
   - JVM trying to make room for more data
   - Fix: Increase executor memory

3. Slow operations
   - Large shuffles create lots of objects
   - Fix: Use broadcast() for joins, reduce groupBy size
```

**How to fix:**
```python
# Option 1: Replace Python UDFs with Scala UDFs
# Instead of:
df = df.withColumn("new_col", udf(my_python_func)(df.col))

# Use Spark SQL functions (native):
df = df.withColumn("new_col", upper(df.col))

# Option 2: Use vectorized pandas UDFs (faster than row-at-a-time)
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def vectorized_func(s: pd.Series) -> pd.Series:
    return s * 2

df = df.withColumn("new_col", vectorized_func(df.col))

# Option 3: Increase executor memory
spark.conf.set("spark.executor.memory", "8g")
```

---

### 9. Peak Executor Memory
**What it is:** The most memory any single executor used at one time

**Format:** Shown as bytes (e.g., "2.4 GB")

**Good range:** < 80% of your executor memory limit

**Example:**
```
Executor memory: 4 GB (default)
Peak used: 2.1 GB = 52% = ✅ Good
Peak used: 3.8 GB = 95% = ⚠️ Danger zone!
```

**When to worry:** 🟡 **YELLOW INDICATOR** if:
- > 80% of executor memory limit

**What it means:**
```
Peak 2 GB / 4 GB limit = Safe, you have 2 GB headroom
Peak 3.8 GB / 4 GB limit = Danger! You're running out of memory

If peak is close to limit:
├─ Next job with same size will SPILL to disk
├─ Next job might crash with OutOfMemoryError
└─ You need to increase memory or reduce data
```

**How to fix:**
```python
# Check current executor memory
print(spark.conf.get("spark.executor.memory"))  # Usually 4g

# Option 1: Increase executor memory
spark.stop()
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.memory", "8g") \  # Double it
    .getOrCreate()

# Option 2: Reduce data per partition
df = df.repartition(200)  # More partitions = smaller chunks = less peak memory

# Option 3: Increase number of executors
# Change cluster size (more executors = smaller work per executor)
```

---

## Summary: Warning Thresholds

| Metric | Green ✅ | Yellow ⚠️ | Red 🔴 |
|--------|---------|----------|--------|
| **Parallel tasks** | 50-2000 | < 10 with > 100 MB data | N/A |
| **Network shuffle** | 0-100 MB | > 500 MB | > 5 GB |
| **Disk spilled** | 0 bytes | Any value > 0 | > 1 GB |
| **Memory spilled** | 0 bytes | Any value > 0 | > 1 GB |
| **GC time** | < 5% | 5-10% is normal, > 10% | > 25% |
| **Peak memory** | < 50% of limit | 50-80% of limit | > 80% of limit |

---

## Real Example: Analyzing a Report

### Bad Job Report
```
⚡ sparkmonitor
🔴 Major performance issues

Full metrics:
├─ Total time: 45 seconds
├─ Parallel tasks: 5        ⚠️ VERY LOW
├─ Data read: 50 GB         ← Large data
├─ Data written: 0 MB
├─ Network shuffle: 2.1 GB  🟡 HIGH (> 500 MB)
├─ Spilled to disk: 500 MB  🔴 HIGH
├─ Spilled from memory: 250 MB 🔴 HIGH
├─ GC time: 25 sec (55%)    🔴 EXCESSIVE
└─ Peak memory: 3.8 GB / 4 GB (95%)  🔴 CRITICAL
```

**What's wrong:**
1. Only 5 parallel tasks but 50 GB data → Cluster underused
2. Peak memory 95% → Running out of memory
3. 55% GC time → JVM constantly struggling
4. Disk spill → Memory overflow

**How to fix:**
```python
spark.stop()
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.memory", "8g") \  # More memory
    .config("spark.executor.cores", "4") \     # More cores
    .config("spark.executor.instances", "10") \ # More executors
    .getOrCreate()

df = spark.read.parquet("s3://data/") \
    .repartition(100)  # More tasks = distribute work
result = df.groupBy("region").count().show()
```

### Good Job Report
```
⚡ sparkmonitor
🟢 Healthy — no issues detected

Full metrics:
├─ Total time: 12 seconds
├─ Parallel tasks: 200      ✅ Good parallelism
├─ Data read: 50 GB
├─ Data written: 0 MB
├─ Network shuffle: 80 MB   ✅ Small shuffle
├─ Spilled to disk: None    ✅ No memory problems
├─ Spilled from memory: None ✅ No pressure
├─ GC time: 0.5 sec (4%)    ✅ Healthy
└─ Peak memory: 1.8 GB / 4 GB (45%)  ✅ Comfortable
```

**What's good:**
- 200 tasks distributing 50 GB across cluster
- Only 80 MB shuffle (simple operation)
- No spilling (good memory management)
- 4% GC (normal background activity)
- 45% peak memory (comfortable headroom)

---

## Quick Reference: What To Do

**If you see yellow warnings:**

1. **Low parallel tasks** → Increase partitions: `df.repartition(100)`
2. **High shuffle** → Use broadcast: `large.join(broadcast(small), on="key")`
3. **Disk spill** → Increase memory or repartition
4. **Memory spill** → Same as disk spill
5. **High GC** → Remove Python UDFs or increase memory
6. **High peak memory** → Increase executor memory or repartition

**Most common fix:**
```python
spark.conf.set("spark.executor.memory", "8g")  # Increase memory
df = df.repartition(100)  # Or increase parallelism
```

---

## Summary

| Metric | Meaning | Good Range | If Bad |
|--------|---------|-----------|--------|
| **Total time** | Wall clock | N/A | Speed up other metrics |
| **Parallel tasks** | Work distribution | 50-2000 | Repartition data |
| **Data read** | Volume input | Smaller better | Filter more |
| **Data written** | Volume output | 0 unless saving | Consider storage |
| **Network shuffle** | Inter-node data | 0-100 MB | Use broadcast() |
| **Disk spilled** | Memory overflow | ZERO | More memory/partitions |
| **Memory spilled** | RAM pressure | ZERO | More memory/partitions |
| **GC time** | Memory overhead | < 10% | Scala UDFs or more memory |
| **Peak memory** | Max used | < 80% limit | More memory or repartition |

**Remember:** If any metric is yellow, read the description in SparkMonitor — it tells you exactly how to fix it! 🎯
