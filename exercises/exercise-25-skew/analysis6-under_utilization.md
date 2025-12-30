# 🚨 Your Problem Has Shifted: From Skew to Under-Utilization

You **fixed the skew** (equal, small partitions ✅), but now you have a **resource utilization problem**. Only 35% CPU
and 76% memory with 600 executors available means Spark isn't using your cluster efficiently!

## 🔍 Diagnose the Real Problem

Run these checks immediately:

### Check 1: Are Executors Actually Scaling Up?

```python
# In Spark UI → Executors tab, check:
# - How many executors are actually running? (should be close to 600)
# - Are they all active or idle?

# Or check programmatically:
spark.sparkContext.getExecutorMemoryStatus().keys().size
# Should show ~600 if scaled up
```

**Most likely issue:** Dynamic allocation isn't scaling up to 600 executors.

### Check 2: Task Parallelism

```python
# Check actual number of tasks vs available cores
from pyspark.sql.functions import spark_partition_id

# How many partitions/tasks?
num_partitions = result.withColumn("pid", spark_partition_id()).select("pid").distinct().count()
print(f"Number of partitions/tasks: {num_partitions}")

# How many executor cores available?
num_executors = 600  # your max
cores_per_executor = 5  # check your config
total_cores = num_executors * cores_per_executor
print(f"Total available cores: {total_cores}")
print(f"Tasks per core: {num_partitions / total_cores:.2f}")

# Ideal: 2-4 tasks per core for good utilization
```

### Check 3: What Stage is Taking Long?

In Spark UI → Stages tab:

- Which stage is taking 6 hours?
- Is it the join stage or something else?
- How many tasks in that stage?
- Are tasks running sequentially or parallel?

## ✅ Solution: Increase Parallelism Dramatically

```python
# ============================================
# INCREASED PARALLELISM CONFIGURATION
# ============================================

# With 600 executors × 5 cores = 3000 cores available
# You need 6000-12000 tasks for good utilization (2-4x cores)

# Increase shuffle partitions significantly
spark.conf.set("spark.sql.shuffle.partitions", "60000")  # Up from 16K!

# Dynamic allocation settings - make sure it scales up
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "50")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "600")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "100")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")  # Scale up fast

# Increase default parallelism
spark.conf.set("spark.default.parallelism", "60000")

# AQE settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")  # Smaller targets
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")

# Optimize shuffle
spark.conf.set("spark.sql.shuffle.partitions", "60000")
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")  # Smaller input chunks

# ============================================
# Repartition with MUCH higher parallelism
# ============================================
table1_fixed = table1.repartition(60000, "CUSIP", "EFFECTIVEDATE")
table2_fixed = table2.repartition(60000, "CUSIP", "EFFECTIVEDATE")

result = table1_fixed.join(
    table2_fixed,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="left"
)

result.write.mode("overwrite").parquet("s3://bucket/output/")
```

## 📊 Why This Will Help

**Current state (16K partitions):**

```
641M rows / 16K partitions = ~40K rows per partition
600 executors × 5 cores = 3000 cores available
16K tasks / 3000 cores = 5.3 tasks per core (OK, but could be better)

If tasks are finishing quickly → executors idle → low CPU usage
```

**With 60K partitions:**

```
641M rows / 60K partitions = ~10K rows per partition (smaller, faster tasks)
60K tasks / 3000 cores = 20 tasks per core (good parallelism)
More tasks in queue → executors always busy → high CPU usage
```

## 🔧 Alternative: Optimize Executor Configuration

Your current setup might be inefficient:

```python
# ============================================
# OPTIMIZE EXECUTOR SIZING
# ============================================

# Instead of many small executors, use fewer larger ones
spark.conf.set("spark.executor.cores", "8")  # Up from 5
spark.conf.set("spark.executor.memory", "32g")  # Increase
spark.conf.set("spark.executor.memoryOverhead", "8g")

# Recalculate max executors based on cluster capacity
# Example: If you have 100 nodes with 64 cores each
# nodes × cores_per_node / executor_cores = max_executors
# 100 × 64 / 8 = 800 executors possible

spark.conf.set("spark.dynamicAllocation.maxExecutors", "400")  # Adjust based on cluster

# With 400 executors × 8 cores = 3200 cores
# 60K partitions / 3200 cores = 18.75 tasks per core (good!)
```

## ⚡ Check for Pipeline Bottlenecks

```python
# ============================================
# Add caching to avoid re-computation
# ============================================

# Read and repartition once
table1_fixed = table1.repartition(60000, "CUSIP", "EFFECTIVEDATE").cache()
table2_fixed = table2.repartition(60000, "CUSIP", "EFFECTIVEDATE").cache()

# Materialize cached data
print(f"Table1 after repartition: {table1_fixed.count():,}")
print(f"Table2 after repartition: {table2_fixed.count():,}")

# Now join
result = table1_fixed.join(
    table2_fixed,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="left"
)

# Write immediately
result.write.mode("overwrite").parquet("s3://bucket/output/")
```

## 🎯 Quick Wins to Try Right Now

### Quick Win 1: Increase Partitions to 60K

```python
spark.conf.set("spark.sql.shuffle.partitions", "60000")
table1_fixed = table1.repartition(60000, "CUSIP", "EFFECTIVEDATE")
table2_fixed = table2.repartition(60000, "CUSIP", "EFFECTIVEDATE")
result = table1_fixed.join(table2_fixed, on=["CUSIP", "EFFECTIVEDATE"], how="left")
```

### Quick Win 2: Force More Executors

```python
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "1s")
# Forces Spark to request executors aggressively
```

### Quick Win 3: Check Input File Count

```python
# If your input files are large, Spark reads fewer partitions initially
# Force more input partitions
table1 = spark.read.option("maxPartitionBytes", "64MB").parquet("s3://bucket/table1/")
table2 = spark.read.option("maxPartitionBytes", "64MB").parquet("s3://bucket/table2/")
```

## 🔍 Monitor in Spark UI

After making changes, check:

1. **Executors tab:**
    - Target: 500-600 active executors
    - If seeing <100 executors → dynamic allocation issue

2. **Stages tab → Active stages:**
    - Target: 60K tasks total, 3000+ tasks running concurrently
    - If <1000 tasks running → not enough parallelism

3. **Tasks metrics:**
    - Task duration should be 10-60 seconds (sweet spot)
    - If <5 seconds → tasks too small, increase partition size
    - If >5 minutes → tasks too large, increase partition count

## 📈 Expected Improvements

| Metric             | Current (16K partitions) | After (60K partitions) |
|--------------------|--------------------------|------------------------|
| Partitions         | 16,000                   | 60,000                 |
| Rows per partition | 40,000                   | 10,700                 |
| Tasks per core     | 5.3                      | 20                     |
| CPU utilization    | 35%                      | 85-95%                 |
| Memory utilization | 76%                      | 85-95%                 |
| Active executors   | ~200?                    | 500-600                |
| Runtime            | 6 hours                  | **30-60 minutes**      |

## 🎯 Root Cause Summary

Your issue is **NOT skew anymore** - you fixed that! Your issue is:

1. ✅ Partition skew: FIXED (equal partition sizes)
2. ❌ **Task parallelism: TOO LOW** (not enough tasks for 600 executors)
3. ❌ **Executor scaling: NOT REACHING MAX** (only partial cluster usage)

**Increase to 60K partitions immediately** - this should drop your runtime from 6 hours to under 1 hour! 🚀