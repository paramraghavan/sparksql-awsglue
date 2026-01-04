# Critical Issue: Memory Exhaustion from Table Explosion

## The Problem

Your job is **memory-bound, not CPU-bound**:

```
Memory: 99.8% used (10.06 TB) ❌❌❌ BOTTLENECK
CPU: 28% (367/1132 cores)     ⚠️  Executors waiting for memory
```

**Root cause**: You're exploding the **entire right table** with salting:

```python
# CATASTROPHIC EXPLOSION:
arm_loan_history: 72,823,883 rows
× salt factor 1,800
= 131,082,989,400 rows (131 BILLION!)
× 500 bytes avg row size  
= 65.5 TB of data just from right table explosion
```

**This is why your job won't complete - you're creating 65TB+ of data in memory!**

## Solution: Only Salt Skewed Keys, Not All Data

```python
from pyspark.sql import functions as F

# ===== CRITICAL FIX: Don't explode entire right table =====

print("Step 1: Identify skewed keys...")
skewed_keys = (
    df_tempo.groupBy("cusip", "effectivedate")
    .count()
    .filter(F.col("count") > 1_000_000)  # Only truly skewed keys
    .select("cusip", "effectivedate")
    .cache()
)

skew_count = skewed_keys.count()
print(f"Skewed key combinations: {skew_count:,}")

# ===== SPLIT BOTH TABLES =====
# LEFT TABLE
df_tempo_skewed = df_tempo.join(
    F.broadcast(skewed_keys), 
    on=["cusip", "effectivedate"], 
    how="inner"
)
df_tempo_normal = df_tempo.join(
    F.broadcast(skewed_keys), 
    on=["cusip", "effectivedate"], 
    how="left_anti"
)

# RIGHT TABLE - CRITICAL: Only explode skewed portion
arm_skewed = arm_loan_history.join(
    F.broadcast(skewed_keys), 
    on=["cusip", "effectivedate"], 
    how="inner"
)
arm_normal = arm_loan_history.join(
    F.broadcast(skewed_keys), 
    on=["cusip", "effectivedate"], 
    how="left_anti"
)

# Check sizes before explosion
skewed_left_count = df_tempo_skewed.count()
skewed_right_count = arm_skewed.count()
print(f"\nSkewed portion sizes:")
print(f"  Left (df_tempo): {skewed_left_count:,} rows")
print(f"  Right (arm_loan_history): {skewed_right_count:,} rows")
print(f"  Right will explode to: {skewed_right_count * 1800:,} rows")

# ===== JOIN NORMAL DATA (no salting) =====
print("\nStep 2: Joining normal data (no salting)...")
result_normal = df_tempo_normal.join(
    arm_normal,
    on=["cusip", "effectivedate"],
    how="left"  # Preserving your left join
)

# ===== SALT ONLY SKEWED PORTION =====
print("\nStep 3: Salting skewed portion...")

# Calculate if explosion is feasible
explosion_rows = skewed_right_count * 1800
explosion_size_tb = (explosion_rows * 500) / (1024**4)
print(f"Explosion will create {explosion_size_tb:.2f} TB")

if explosion_size_tb > 5:  # More than 5TB is problematic
    print("⚠️  Explosion too large! Using alternative strategy...")
    # Use Solution 2 below instead
else:
    SALT_FACTOR = 1800
    
    df_tempo_skewed_salted = df_tempo_skewed.withColumn(
        "salt_key",
        (F.rand(seed=42) * SALT_FACTOR).cast("int")
    )
    
    # Only explode skewed portion of right table
    salt_df = spark.range(SALT_FACTOR).select(
        F.col("id").alias("salt_key").cast("int")
    )
    arm_skewed_exploded = arm_skewed.crossJoin(F.broadcast(salt_df))
    
    result_skewed = df_tempo_skewed_salted.join(
        arm_skewed_exploded,
        on=["cusip", "effectivedate", "salt_key"],
        how="left"
    ).drop("salt_key")
    
    # Combine results
    final_result = result_normal.union(result_skewed)
```

## Solution 2: Broadcast Join for Skewed Keys (If Feasible)

```python
# This is MUCH faster if skewed right table portion is small

# Check if we can broadcast the skewed right table
skewed_right_size_gb = (arm_skewed.count() * 500) / (1024**3)
print(f"Skewed right table size: {skewed_right_size_gb:.2f} GB")

if skewed_right_size_gb < 8:  # Can fit in executor memory
    print("✅ Using BROADCAST JOIN for skewed data - much faster!")
    
    # No salting needed!
    result_skewed = df_tempo_skewed.join(
        F.broadcast(arm_skewed),
        on=["cusip", "effectivedate"],
        how="left"
    )
    
    result_normal = df_tempo_normal.join(
        arm_normal,
        on=["cusip", "effectivedate"],
        how="left"
    )
    
    final_result = result_normal.union(result_skewed)
    
    # This should complete in 10-30 minutes instead of 6+ hours
```

## Solution 3: Batch Processing (Most Reliable)

```python
# Process skewed keys in batches to avoid memory explosion

from pyspark.sql import functions as F

# Get list of skewed keys
skewed_keys_list = (
    df_tempo.groupBy("cusip", "effectivedate")
    .count()
    .filter(F.col("count") > 1_000_000)
    .select("cusip", "effectivedate")
    .collect()
)

print(f"Processing {len(skewed_keys_list)} skewed keys in batches...")

# Process in batches of 10 keys at a time
BATCH_SIZE = 10
results = []

for i in range(0, len(skewed_keys_list), BATCH_SIZE):
    batch = skewed_keys_list[i:i+BATCH_SIZE]
    print(f"\nProcessing batch {i//BATCH_SIZE + 1}/{len(skewed_keys_list)//BATCH_SIZE + 1}")
    
    # Create DataFrame for this batch of keys
    batch_keys = spark.createDataFrame(batch)
    
    # Filter to batch
    df_tempo_batch = df_tempo.join(
        F.broadcast(batch_keys),
        on=["cusip", "effectivedate"],
        how="inner"
    )
    
    arm_batch = arm_loan_history.join(
        F.broadcast(batch_keys),
        on=["cusip", "effectivedate"],
        how="inner"
    )
    
    # Apply salting to batch only
    SALT_FACTOR = 1800
    df_tempo_batch_salted = df_tempo_batch.withColumn(
        "salt_key",
        (F.rand(seed=42) * SALT_FACTOR).cast("int")
    )
    
    salt_df = spark.range(SALT_FACTOR).select(F.col("id").alias("salt_key").cast("int"))
    arm_batch_exploded = arm_batch.crossJoin(F.broadcast(salt_df))
    
    # Join batch
    result_batch = df_tempo_batch_salted.join(
        arm_batch_exploded,
        on=["cusip", "effectivedate", "salt_key"],
        how="left"
    ).drop("salt_key")
    
    # Write batch to disk immediately to free memory
    output_path = f"s3://your-bucket/temp/batch_{i//BATCH_SIZE}"
    result_batch.write.mode("overwrite").parquet(output_path)
    results.append(output_path)
    
    print(f"✅ Batch {i//BATCH_SIZE + 1} written")

# Process normal data
result_normal = df_tempo_normal.join(
    arm_normal,
    on=["cusip", "effectivedate"],
    how="left"
)
normal_path = "s3://your-bucket/temp/normal"
result_normal.write.mode("overwrite").parquet(normal_path)
results.append(normal_path)

# Read all results and combine
final_result = spark.read.parquet(*results)
```

## Solution 4: Bucket Join (No Explosion Needed)

```python
# Pre-partition both tables into buckets - no explosion!

print("Creating bucketed tables...")

# Bucket both tables by join keys
df_tempo.write \
    .mode("overwrite") \
    .bucketBy(2000, "cusip", "effectivedate") \
    .sortBy("cusip", "effectivedate") \
    .saveAsTable("df_tempo_bucketed")

arm_loan_history.write \
    .mode("overwrite") \
    .bucketBy(2000, "cusip", "effectivedate") \
    .sortBy("cusip", "effectivedate") \
    .saveAsTable("arm_loan_history_bucketed")

# Enable bucket join
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast

# Read bucketed tables
df_tempo_bucketed = spark.table("df_tempo_bucketed")
arm_bucketed = spark.table("arm_loan_history_bucketed")

# Join - Spark will use bucket join (much faster)
final_result = df_tempo_bucketed.join(
    arm_bucketed,
    on=["cusip", "effectivedate"],
    how="left"
)

# This should complete in 30-60 minutes
```

## Immediate Action Plan

**Do this NOW to complete in 2-3 hours:**

```python
from pyspark.sql import functions as F
import time

start_time = time.time()

# ===== CONFIGURATION =====
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "4000")

# Increase memory overhead
spark.conf.set("spark.executor.memoryOverhead", "8g")

# ===== STEP 1: Check if broadcast is feasible =====
print("Analyzing right table size...")
arm_count = arm_loan_history.count()
arm_size_gb = (arm_count * 500) / (1024**3)  # Rough estimate
print(f"Right table: {arm_count:,} rows, ~{arm_size_gb:.2f} GB")

if arm_size_gb < 8:
    print("\n✅ USING BROADCAST JOIN - No salting needed!")
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10737418240")  # 10GB
    
    final_result = df_tempo.join(
        F.broadcast(arm_loan_history),
        on=["cusip", "effectivedate"],
        how="left"
    )
    
else:
    print("\n⚠️ Right table too large for broadcast, using hybrid approach...")
    
    # Find skewed keys
    skewed_keys = (
        df_tempo.groupBy("cusip", "effectivedate")
        .count()
        .filter(F.col("count") > 2_000_000)  # Increased threshold
        .select("cusip", "effectivedate")
        .cache()
    )
    
    skew_count = skewed_keys.count()
    print(f"Skewed keys found: {skew_count}")
    
    if skew_count == 0:
        # No skew, standard join
        final_result = df_tempo.join(
            arm_loan_history,
            on=["cusip", "effectivedate"],
            how="left"
        )
    else:
        # Split datasets
        df_tempo_skewed = df_tempo.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="inner")
        df_tempo_normal = df_tempo.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="left_anti")
        
        arm_skewed = arm_loan_history.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="inner")
        arm_normal = arm_loan_history.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="left_anti")
        
        # Check if skewed arm portion can be broadcast
        arm_skewed_count = arm_skewed.count()
        arm_skewed_gb = (arm_skewed_count * 500) / (1024**3)
        print(f"Skewed right portion: {arm_skewed_count:,} rows, ~{arm_skewed_gb:.2f} GB")
        
        # Join normal data
        print("Joining normal data...")
        result_normal = df_tempo_normal.join(arm_normal, on=["cusip", "effectivedate"], how="left")
        
        if arm_skewed_gb < 5:
            # Broadcast skewed portion
            print("Broadcasting skewed right table portion...")
            result_skewed = df_tempo_skewed.join(
                F.broadcast(arm_skewed),
                on=["cusip", "effectivedate"],
                how="left"
            )
        else:
            # Use moderate salting (reduced factor)
            print("Salting skewed portion with reduced factor...")
            SALT_FACTOR = 800  # Reduced from 1800
            
            df_tempo_skewed_salted = df_tempo_skewed.withColumn(
                "salt_key",
                (F.rand(42) * SALT_FACTOR).cast("int")
            )
            
            salt_df = spark.range(SALT_FACTOR).select(F.col("id").alias("salt_key").cast("int"))
            arm_skewed_exploded = arm_skewed.crossJoin(F.broadcast(salt_df))
            
            result_skewed = df_tempo_skewed_salted.join(
                arm_skewed_exploded,
                on=["cusip", "effectivedate", "salt_key"],
                how="left"
            ).drop("salt_key")
        
        # Combine
        final_result = result_normal.union(result_skewed)

# Write result
print("\nWriting final result...")
final_result.write.mode("overwrite").parquet("s3://your-bucket/final_output/")

elapsed = (time.time() - start_time) / 60
print(f"\n✅ COMPLETED in {elapsed:.2f} minutes")
```

## Key Changes to Fix Your Job

1. **Don't explode entire right table** - only skewed portion
2. **Try broadcast join first** - if right table < 8GB
3. **Reduce salt factor** - 800 instead of 1800
4. **Increase skew threshold** - 2M instead of 1M to reduce skewed keys
5. **Add memory overhead** - 8GB per executor

**This should complete in 1-3 hours instead of 6+.**