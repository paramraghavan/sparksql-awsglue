# Job Stuck at 99%

## Symptom

- Job appears to hang with 99% progress
- Most tasks complete, but 1-2 tasks run forever
- Spark UI shows task still running after 10+ minutes
- Other executors are idle
- Finally times out or you kill it

## Identification

### Check Spark UI (localhost:4040)

**Jobs tab**:
- Job shows progress: 99% (e.g., "999 of 1000 tasks complete")
- Duration keeps increasing

**Stages tab**:
- Final stage shows uneven task distribution
- Most tasks: 2-5 seconds
- 1-2 tasks: 300+ seconds (still running)

**Executors tab**:
- 1-2 executors busy
- All others idle/no tasks

---

## Common Causes

### 1. **Data Skew** (Most Common - 80% of cases)
One or few partitions have 90%+ of the data.

**Signs**:
- One task processing 90% of rows
- In Spark UI, stage shows min/max/median task duration very different
- Last partition(s) take forever

**Example**:
```
Partition 0: 1,000 rows (2 seconds)
Partition 1: 1,000 rows (2 seconds)
...
Partition 99: 900,000 rows (300 seconds) ← SKEWED
```

### 2. **Garbage Collection (GC) Pause**
Executor running out of memory, spending time garbage collecting.

**Signs**:
- Executors tab shows "GC time" high (> 10%)
- Memory "used" close to max
- Task suddenly slow midway through

### 3. **Network I/O Bottleneck**
Slow network for shuffle read, data is being transmitted slowly.

**Signs**:
- Job slower on large cluster than small cluster (odd!)
- Shuffle read time very high
- Network bandwidth saturated

### 4. **Disk Space Issue**
Executor runs out of disk during shuffle spill.

**Signs**:
- Executor spilling to disk heavily
- `/tmp/` space filling up
- Task slows down dramatically mid-execution

### 5. **Bug in UDF/Complex Logic**
Custom code (UDF, complex Python logic) is slow.

**Signs**:
- Spark functions are fast, UDFs are slow
- Same data, different task durations

---

## Diagnostic Steps

### Step 1: Check Task Duration Distribution

```python
# In Spark UI Stages tab, click the final stage
# Look at "Task Metrics" section

# OR programmatically:
from pyspark.sql.functions import spark_partition_id

# See partition sizes
df.groupBy(spark_partition_id()).agg(count("*")).repartition(1).collect()

# Output:
# partition_id=0, count=1000
# partition_id=1, count=1000
# partition_id=99, count=900000  ← SKEWED!
```

### Step 2: Check Memory & GC

**In Spark UI Executors tab**:
- Look at "GC Time / Task Time" ratio
- If > 10%, memory pressure is problem
- Look at "Memory" column - is it hitting max?

### Step 3: Check Shuffle Statistics

**In Spark UI SQL tab**, click on the job:
- Look for "Shuffle Read Size" and "Shuffle Write Size"
- If very large and one task shows much more, that's skew

### Step 4: Check Logs

```bash
# Get executor logs (EMR)
yarn logs -applicationId <app-id> | grep -A5 "task"

# Look for:
# - "GC overhead limit exceeded" (memory)
# - "Shuffle fetch failed" (network/disk)
# - Error messages from your code
```

### Step 5: Measure Partition Sizes

```python
# Check for skew
from pyspark.sql.functions import spark_partition_id

skew_check = df.groupBy(spark_partition_id()) \
    .agg(count("*").alias("rows")) \
    .orderBy(F.desc("rows"))

skew_check.show(10)

# If one partition has >> more rows, that's your problem
```

---

## Solutions (By Likelihood)

### Solution 1: Repartition with Salting (For Skew)

**When to use**: Data skew detected

```python
from pyspark.sql.functions import rand, floor

# Add random salt to break up skewed partitions
num_partitions = 500
df_rebalanced = df \
    .withColumn("_salt", floor(rand() * num_partitions).cast("int")) \
    .repartition(num_partitions, "_salt") \
    .drop("_salt")

# Now continue processing
result = df_rebalanced.groupBy("key").count()
```

**Why it works**: Randomly distributes data evenly across partitions

**Performance**: Adds one shuffle, but final result is faster

### Solution 2: Increase shuffle.partitions (Parallelism)

**When to use**: Partitions too large

```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Or at submit time:
spark-submit --conf spark.sql.shuffle.partitions=1000 my_job.py
```

**Calculation**:
```
data_size_GB = 100  # Total data
target_per_partition = 0.128  # GB (128MB)
needed_partitions = data_size_GB / target_per_partition  # = 781
# Round up to 1000
```

### Solution 3: Enable Adaptive Query Execution (AQE)

**When to use**: Spark 3.0+, especially for skew

```python
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

**What it does**:
- Automatically detects skewed joins and splits them
- Combines small partitions after shuffle
- Switches to broadcast join if needed

**No code changes needed!**

### Solution 4: Increase Memory & Reduce Parallelism

**When to use**: GC is high (> 10%)

```bash
spark-submit \
    --executor-memory 16g \
    --executor-cores 4 \
    --conf spark.memory.fraction=0.8 \
    my_job.py
```

**Why**:
- More memory = less GC
- Fewer cores = less task contention

### Solution 5: Add Salting for Joins

**When to use**: Skewed join (groupBy or join on hot key)

```python
from pyspark.sql.functions import rand, floor, explode, array, lit

num_salt_buckets = 10

# Salt the large table
large_salted = large_df \
    .withColumn("_salt", floor(rand() * num_salt_buckets).cast("int"))

# Explode small table to all salts
small_exploded = small_df \
    .withColumn("_salt", F.explode(F.array([F.lit(i) for i in range(num_salt_buckets)])))

# Join on key + salt
result = large_salted.join(
    small_exploded,
    ["join_key", "_salt"],
    "inner"
).drop("_salt")
```

**Why**: Each hot key is split across multiple partitions and joined in parallel

---

## Real Case Study

### Problem
EMR job processing 100GB of sales data. Groups by `customer_id` and counts transactions.

```python
result = df.groupBy("customer_id").count()
```

Job stuck at 99% for 45+ minutes. Finally killed it.

### Investigation
Checked partition sizes:
```
customer_id="cust-001": 1,000 rows (across 100 partitions)
customer_id="cust-002": 500 rows
customer_id="cust-999": 50,000 rows
...
customer_id="cust-hotkey": 40,000,000 rows ← HUGE!
```

One customer had 40% of all transactions!

### Root Cause
Data skew: One popular customer's transactions dominated the data.

### Solution Applied

```python
# Add salt to break up hot key
num_partitions = 1000
df_salted = df \
    .withColumn("_salt", F.floor(F.rand() * num_partitions).cast("int")) \
    .repartition(num_partitions, "_salt")

# GroupBy with salt
result = df_salted.groupBy("customer_id", "_salt").count() \
    .drop("_salt") \
    .groupBy("customer_id").agg(F.sum("count").alias("count"))
```

### Results
- Before: 45+ minutes (still running)
- After: 2 minutes
- **22x faster!**

### Lesson Learned
Always check for data skew with `groupBy(spark_partition_id()).count()` before submitting large jobs.

---

## Prevention Checklist

Before running jobs:
- [ ] Check data distribution: `df.groupBy(spark_partition_id()).count()`
- [ ] Is one partition >> others? → Use salting
- [ ] Set appropriate `spark.sql.shuffle.partitions`
- [ ] Enable AQE: `spark.sql.adaptive.enabled=true`
- [ ] Monitor Spark UI from the start
- [ ] Test on sample data first

---

## Quick Fix Summary

| Cause | Quick Fix |
|-------|-----------|
| Data skew | Add salt + repartition |
| Too few partitions | Increase `spark.sql.shuffle.partitions` |
| High GC | Increase memory, reduce cores |
| Slow UDF | Replace with Spark functions |
| Network slow | Enable AQE, reduce shuffle data |

---

## Checking if Fixed

```python
# Before fix
df.groupBy("key").count().show()  # Slow!

# After fix
df.groupBy("key").count().show()  # Fast!

# Verify in Spark UI:
# - All tasks complete in similar time (< 2x difference)
# - No task takes > 10 seconds
# - GC time < 5%
```

---

## See Also
- [Data Skew](05-data-skew.md) - Deep dive on skew handling
- [Performance Degradation](05-performance-degradation.md) - General slowness
- [Configuration Tuning](../04-performance-optimization/02-configuration-tuning.md)
- [Adaptive Query Execution](../04-performance-optimization/04-catalyst-optimizer.md#aqe)
