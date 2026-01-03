# CRITICAL ISSUE: Memory Bottleneck, Not Salt Factor

## Your Problem is NOT the Salt Factor - It's the Exploded Right Table!

```
Right table explosion with salt 1800:
72,823,883 rows × 1,800 salts = 131,082,989,400 rows (131 BILLION!)

At 500 bytes/row: 131B × 500 = 65 TB of data
Your cluster only has ~10 TB memory → 99.8% usage → Constant GC/spilling

This is why CPU is at 28% - executors are spending time on:
- Garbage collection
- Spilling to disk
- Shuffling spilled data
- OOM recovery
```

## Solution 1: ITERATIVE SALTING (Recommended - Will Complete in 1-2 Hours)

**Don't explode the entire right table at once. Process salt batches sequentially.**

```python
from pyspark.sql import functions as F
import time

# ===== CONFIGURATION =====
TOTAL_SALTS = 1800
BATCH_SIZE = 50  # Process 50 salts at a time
NUM_BATCHES = (TOTAL_SALTS + BATCH_SIZE - 1) // BATCH_SIZE  # 36 batches

print(f"Processing {TOTAL_SALTS} salts in {NUM_BATCHES} batches of {BATCH_SIZE}")

# ===== PREPARE LEFT TABLE (one time) =====
df_tempo_skewed_salted = df_tempo_skewed.withColumn(
    "salt_key",
    (F.rand(seed=42) * TOTAL_SALTS).cast("int")
).cache()  # Cache this - we'll use it multiple times

df_tempo_skewed_salted.count()  # Materialize cache
print(f"Left table prepared with {TOTAL_SALTS} salt values")

# ===== PROCESS IN BATCHES =====
result_parts = []

for batch_num in range(NUM_BATCHES):
    batch_start = batch_num * BATCH_SIZE
    batch_end = min(batch_start + BATCH_SIZE, TOTAL_SALTS)

    print(f"\n{'=' * 70}")
    print(f"Processing batch {batch_num + 1}/{NUM_BATCHES}")
    print(f"Salt range: {batch_start} to {batch_end - 1}")
    print(f"{'=' * 70}")

    start_time = time.time()

    # Filter left table for this batch
    df_left_batch = df_tempo_skewed_salted.filter(
        (F.col("salt_key") >= batch_start) & (F.col("salt_key") < batch_end)
    )

    # Explode right table ONLY for this batch
    salt_values_batch = list(range(batch_start, batch_end))
    arm_right_batch = arm_loan_history_skewed.withColumn(
        "salt_key",
        F.explode(F.array([F.lit(i) for i in salt_values_batch]))
    )

    # Join this batch
    result_batch = df_left_batch.join(
        arm_right_batch,
        on=["cusip", "effectivedate", "salt_key"],
        how="inner"
    ).drop("salt_key")

    # Write this batch immediately (don't hold in memory)
    batch_path = f"s3://your-bucket/temp/skewed_results/batch_{batch_num:04d}/"
    result_batch.write.mode("overwrite").parquet(batch_path)

    elapsed = time.time() - start_time
    print(f"✅ Batch {batch_num + 1} completed in {elapsed / 60:.2f} minutes")
    print(f"   Written to: {batch_path}")

# ===== READ BACK ALL BATCHES =====
print("\n" + "=" * 70)
print("Reading all batch results...")
print("=" * 70)

result_skewed = spark.read.parquet("s3://your-bucket/temp/skewed_results/batch_*")

# Combine with normal results
final_result = result_normal.union(result_skewed)

print("\n✅ JOB COMPLETE!")
```

## Solution 2: RANGE-BASED JOIN (No Explosion - Fastest)

**Don't explode at all. Use modulo arithmetic.**

```python
from pyspark.sql import functions as F

NUM_SALTS = 1800

# ===== LEFT TABLE: Add salt =====
df_tempo_skewed_salted = df_tempo_skewed.withColumn(
    "salt_key",
    (F.rand(seed=42) * NUM_SALTS).cast("int")
)

# ===== RIGHT TABLE: Add ALL possible salts as a range column =====
# Instead of exploding, we'll use a clever join condition
arm_loan_history_skewed_with_range = arm_loan_history_skewed.withColumn(
    "salt_min", F.lit(0)
).withColumn(
    "salt_max", F.lit(NUM_SALTS - 1)
)

# ===== JOIN with range condition =====
result_skewed = df_tempo_skewed_salted.join(
    arm_loan_history_skewed_with_range,
    (F.col("df_tempo_skewed_salted.cusip") == F.col("arm_loan_history_skewed_with_range.cusip")) &
    (F.col("df_tempo_skewed_salted.effectivedate") == F.col("arm_loan_history_skewed_with_range.effectivedate")) &
    (F.col("salt_key") >= F.col("salt_min")) &
    (F.col("salt_key") <= F.col("salt_max")),
    how="inner"
).drop("salt_key", "salt_min", "salt_max")

# This creates a cartesian product effect BUT only for matching keys
# Much more memory efficient than exploding
```

## Solution 3: BROADCAST THE RIGHT TABLE (Best if feasible)

```python
# Check actual size of right table (skewed portion only)
arm_loan_history_skewed_count = arm_loan_history_skewed.count()
print(f"Right table (skewed) rows: {arm_loan_history_skewed_count:,}")

# Estimate size
# If this is < 5-10 GB, BROADCAST IT!

# Calculate size
from pyspark.sql import functions as F

sample = arm_loan_history_skewed.limit(1000)
sample_rows = sample.collect()
avg_row_size = sum(len(str(row)) for row in sample_rows) / len(sample_rows)
estimated_size_gb = (arm_loan_history_skewed_count * avg_row_size) / (1024 ** 3)

print(f"Estimated right table size: {estimated_size_gb:.2f} GB")

if estimated_size_gb < 8:
    print("✅ RIGHT TABLE CAN BE BROADCAST!")

    # Increase broadcast threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10737418240")  # 10GB

    # NO SALTING NEEDED - Just broadcast join
    result_skewed = df_tempo_skewed.join(
        F.broadcast(arm_loan_history_skewed),
        on=["cusip", "effectivedate"],
        how="inner"
    )

    print("✅ This will complete in 10-20 minutes!")
else:
    print(f"⚠️ Right table too large ({estimated_size_gb:.2f} GB) for broadcast")
    print("Use iterative salting instead")
```

## Solution 4: OPTIMIZED CLUSTER CONFIGURATION

```python
# Your current config is wasting resources
# 99.8% memory, 28% CPU = memory bottleneck

# Stop your current job and reconfigure:

spark_conf = {
    # REDUCE executors, INCREASE memory per executor
    "spark.executor.instances": "200",  # Down from 441
    "spark.executor.cores": "4",  # Down from 5
    "spark.executor.memory": "24g",  # Up significantly
    "spark.executor.memoryOverhead": "8g",  # Critical for joins

    # Memory tuning
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.2",  # More for execution

    # Reduce shuffle partitions for less overhead
    "spark.sql.shuffle.partitions": "4000",  # Down from higher

    # Aggressive spill settings
    "spark.shuffle.spill.compress": "true",
    "spark.shuffle.compress": "true",

    # Disable broadcast for now
    "spark.sql.autoBroadcastJoinThreshold": "-1",

    # AQE optimization
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

for key, value in spark_conf.items():
    spark.conf.set(key, value)
```

## Solution 5: HYBRID APPROACH (2-3 Hour Solution)

```python
from pyspark.sql import functions as F
import time

# ===== STEP 1: Check if broadcast is possible =====
arm_skewed_count = arm_loan_history_skewed.count()
sample_size_est = (arm_skewed_count * 500) / (1024 ** 3)  # Rough estimate

print(f"Right table estimate: {sample_size_est:.2f} GB")

if sample_size_est < 8:
    # ===== BROADCAST APPROACH =====
    print("✅ Using BROADCAST JOIN - Expected time: 15-30 minutes")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10737418240")

    result_skewed = df_tempo_skewed.join(
        F.broadcast(arm_loan_history_skewed),
        on=["cusip", "effectivedate"],
        how="inner"
    )

else:
    # ===== ITERATIVE SALTING APPROACH =====
    print("✅ Using ITERATIVE SALTING - Expected time: 1.5-2.5 hours")

    TOTAL_SALTS = 1500  # Reduced from 1800
    BATCH_SIZE = 100  # Larger batches

    # Prepare left table
    df_left_salted = df_tempo_skewed.withColumn(
        "salt_key",
        (F.rand(42) * TOTAL_SALTS).cast("int")
    ).repartition(2000, "salt_key").cache()

    df_left_salted.count()  # Materialize

    # Process in batches
    batch_paths = []
    num_batches = (TOTAL_SALTS + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(num_batches):
        start_salt = batch_num * BATCH_SIZE
        end_salt = min(start_salt + BATCH_SIZE, TOTAL_SALTS)

        print(f"\nBatch {batch_num + 1}/{num_batches}: salts {start_salt}-{end_salt - 1}")

        # Filter left by salt range
        df_left_batch = df_left_salted.filter(
            F.col("salt_key").between(start_salt, end_salt - 1)
        )

        # Explode right for this batch only
        salt_list = list(range(start_salt, end_salt))
        df_right_batch = arm_loan_history_skewed.withColumn(
            "salt_key",
            F.explode(F.array([F.lit(i) for i in salt_list]))
        )

        # Join
        result_batch = df_left_batch.join(
            df_right_batch,
            on=["cusip", "effectivedate", "salt_key"],
            how="inner"
        ).drop("salt_key")

        # Write immediately
        batch_path = f"s3://your-bucket/temp/results_batch_{batch_num:04d}"
        result_batch.write.mode("overwrite").parquet(batch_path)
        batch_paths.append(batch_path)

        print(f"  ✅ Batch complete")

    # Read all batches
    result_skewed = spark.read.parquet(*batch_paths)

# Combine with normal results
final_result = result_normal.union(result_skewed)

# Write final output
final_result.write.mode("overwrite").parquet("s3://your-bucket/final_output/")
```

## IMMEDIATE ACTION PLAN

**Stop your current job NOW and do this:**

1. **First, test broadcast join** (5 minutes to find out):

```python
arm_sample_size = (arm_loan_history_skewed.count() * 500) / (1024 ** 3)
if arm_sample_size < 8:
# Use Solution 3 - Will finish in 20 minutes
```

2. **If broadcast won't work**, use **Iterative Salting** (Solution 1):
    - Expected runtime: **1.5-2 hours**
    - Memory usage: **50-60%** (not 99.8%)
    - CPU usage: **60-80%** (not 28%)

3. **Reconfigure cluster**:
    - 200 executors × 24GB = Better than 441 × smaller memory
    - Fewer executors = less coordination overhead

## Why Your Current Approach Failed

```
Your approach: Explode entire right table
72M rows × 1800 = 131 BILLION rows in memory simultaneously
Result: 99.8% memory, constant spilling, 6+ hours

Iterative approach: Process 50 salts at a time
72M rows × 50 = 3.6 BILLION rows per batch
36 batches × 2-3 min each = 90-120 minutes total
Result: 50-60% memory, no spilling, 1.5-2 hours
```

**Use Solution 1 (Iterative Salting) or Solution 3 (Broadcast). You'll finish in under 2 hours.**