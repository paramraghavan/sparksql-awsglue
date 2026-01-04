# Skewed Key Salting with Batch Processing

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import time

def skewed_left_join(
    df_left,
    df_right,
    join_keys,
    skew_threshold=1_000_000,
    salt_factor=1800,
    broadcast_threshold_gb=5,
    batch_size=20,
    max_explosion_gb=10,
    temp_output_path="s3://your-bucket/temp/skewed_join"
):
    """
    Perform left join with automatic skew handling using salting and optional batching.
    
    Parameters:
    -----------
    df_left : DataFrame
        Left table (e.g., df_tempo)
    df_right : DataFrame
        Right table (e.g., arm_loan_history)
    join_keys : list
        List of column names to join on (e.g., ["cusip", "effectivedate"])
    skew_threshold : int
        Row count threshold to consider a key as skewed (default: 1M)
    salt_factor : int
        Number of salt values to use for skewed data (default: 1800)
    broadcast_threshold_gb : float
        Size threshold in GB for broadcast join (default: 5)
    batch_size : int
        Number of skewed keys to process per batch (default: 20)
    max_explosion_gb : float
        Max GB for explosion before using batching (default: 10)
    temp_output_path : str
        S3 path for temporary batch outputs
    
    Returns:
    --------
    DataFrame : Result of left join
    """
    
    start_time = time.time()
    
    print("="*80)
    print("SKEWED LEFT JOIN - Starting")
    print("="*80)
    
    # ===== STEP 1: IDENTIFY SKEWED KEYS =====
    print("\n[1/6] Identifying skewed keys...")
    skewed_keys_df = (
        df_left.groupBy(*join_keys)
        .count()
        .filter(F.col("count") > skew_threshold)
        .select(*join_keys)
        .cache()
    )
    
    skew_count = skewed_keys_df.count()
    print(f"      Found {skew_count:,} skewed key combinations (threshold: {skew_threshold:,} rows)")
    
    if skew_count == 0:
        print("      No skew detected - performing standard join")
        skewed_keys_df.unpersist()
        result = df_left.join(df_right, on=join_keys, how="left")
        
        elapsed = (time.time() - start_time) / 60
        print(f"\n{'='*80}")
        print(f"✅ COMPLETED in {elapsed:.2f} minutes")
        print(f"{'='*80}\n")
        return result
    
    # ===== STEP 2: SPLIT DATASETS =====
    print("\n[2/6] Splitting datasets into skewed and normal portions...")
    
    # Split left table
    df_left_skewed = df_left.join(
        F.broadcast(skewed_keys_df),
        on=join_keys,
        how="inner"
    )
    df_left_normal = df_left.join(
        F.broadcast(skewed_keys_df),
        on=join_keys,
        how="left_anti"
    )
    
    # Split right table - FILTER ONCE and cache
    df_right_skewed = df_right.join(
        F.broadcast(skewed_keys_df),
        on=join_keys,
        how="inner"
    ).cache()
    
    df_right_normal = df_right.join(
        F.broadcast(skewed_keys_df),
        on=join_keys,
        how="left_anti"
    )
    
    # Get counts
    left_skewed_count = df_left_skewed.count()
    right_skewed_count = df_right_skewed.count()  # Materializes cache
    
    print(f"      Left table - Skewed: {left_skewed_count:,} | Normal: {df_left_normal.count():,}")
    print(f"      Right table - Skewed: {right_skewed_count:,} | Normal: {df_right_normal.count():,}")
    
    # ===== STEP 3: PROCESS NORMAL DATA =====
    print("\n[3/6] Processing normal data (standard join)...")
    result_normal = df_left_normal.join(
        df_right_normal,
        on=join_keys,
        how="left"
    )
    print(f"      ✅ Normal data processed")
    
    # ===== STEP 4: ANALYZE SKEWED DATA =====
    print("\n[4/6] Analyzing skewed data to determine strategy...")
    
    # Estimate sizes
    avg_row_size = 500  # bytes - adjust based on your schema
    right_skewed_gb = (right_skewed_count * avg_row_size) / (1024**3)
    explosion_rows = right_skewed_count * salt_factor
    explosion_gb = (explosion_rows * avg_row_size) / (1024**3)
    
    print(f"      Right skewed portion: {right_skewed_count:,} rows (~{right_skewed_gb:.2f} GB)")
    print(f"      After {salt_factor}x explosion: {explosion_rows:,} rows (~{explosion_gb:.2f} GB)")
    
    # ===== STEP 5: CHOOSE STRATEGY AND PROCESS SKEWED DATA =====
    print("\n[5/6] Processing skewed data...")
    
    if right_skewed_gb < broadcast_threshold_gb:
        # STRATEGY 1: BROADCAST JOIN (fastest - no salting needed)
        print(f"      Strategy: BROADCAST JOIN (right table < {broadcast_threshold_gb} GB)")
        print(f"      ✅ No salting needed - broadcasting right table")
        
        result_skewed = df_left_skewed.join(
            F.broadcast(df_right_skewed),
            on=join_keys,
            how="left"
        )
        
    elif explosion_gb < max_explosion_gb:
        # STRATEGY 2: SINGLE SALTING (all at once)
        print(f"      Strategy: SINGLE SALTING (explosion < {max_explosion_gb} GB)")
        print(f"      Applying salt factor: {salt_factor}")
        
        # Add salt to left table
        df_left_skewed_salted = df_left_skewed.withColumn(
            "salt_key",
            (F.rand(seed=42) * salt_factor).cast(IntegerType())
        )
        
        # Explode right table with all salt values
        salt_df = spark.range(salt_factor).select(
            F.col("id").alias("salt_key").cast(IntegerType())
        )
        df_right_skewed_exploded = df_right_skewed.crossJoin(F.broadcast(salt_df))
        
        # Join
        result_skewed = df_left_skewed_salted.join(
            df_right_skewed_exploded,
            on=join_keys + ["salt_key"],
            how="left"
        ).drop("salt_key")
        
        print(f"      ✅ Salted join completed")
        
    else:
        # STRATEGY 3: BATCH PROCESSING (for very large explosions)
        print(f"      Strategy: BATCH PROCESSING (explosion > {max_explosion_gb} GB)")
        print(f"      Processing in batches of {batch_size} skewed keys")
        
        # Collect skewed keys for batching
        skewed_keys_list = skewed_keys_df.collect()
        num_batches = (len(skewed_keys_list) - 1) // batch_size + 1
        
        print(f"      Total batches: {num_batches}")
        
        batch_results = []
        
        for i in range(0, len(skewed_keys_list), batch_size):
            batch_num = i // batch_size + 1
            batch = skewed_keys_list[i:i+batch_size]
            
            print(f"\n      Processing batch {batch_num}/{num_batches} ({len(batch)} keys)...")
            
            # Create DataFrame for this batch
            batch_keys_df = spark.createDataFrame(batch)
            
            # Filter to batch - LEFT table
            df_left_batch = df_left_skewed.join(
                F.broadcast(batch_keys_df),
                on=join_keys,
                how="inner"
            )
            
            # Filter to batch - RIGHT table (from pre-filtered cached version)
            df_right_batch = df_right_skewed.join(
                F.broadcast(batch_keys_df),
                on=join_keys,
                how="inner"
            )
            
            # Apply salting to batch
            df_left_batch_salted = df_left_batch.withColumn(
                "salt_key",
                (F.rand(seed=42) * salt_factor).cast(IntegerType())
            )
            
            salt_df = spark.range(salt_factor).select(
                F.col("id").alias("salt_key").cast(IntegerType())
            )
            df_right_batch_exploded = df_right_batch.crossJoin(F.broadcast(salt_df))
            
            # Join batch
            result_batch = df_left_batch_salted.join(
                df_right_batch_exploded,
                on=join_keys + ["salt_key"],
                how="left"
            ).drop("salt_key")
            
            # Write batch to temporary location to free memory
            batch_path = f"{temp_output_path}/batch_{batch_num}"
            result_batch.write.mode("overwrite").parquet(batch_path)
            batch_results.append(batch_path)
            
            print(f"      ✅ Batch {batch_num}/{num_batches} completed and written")
        
        # Read all batches
        print(f"\n      Reading all {len(batch_results)} batches...")
        result_skewed = spark.read.parquet(*batch_results)
        print(f"      ✅ All batches loaded")
    
    # ===== STEP 6: COMBINE RESULTS =====
    print("\n[6/6] Combining normal and skewed results...")
    final_result = result_normal.union(result_skewed)
    
    # Cleanup
    df_right_skewed.unpersist()
    skewed_keys_df.unpersist()
    
    elapsed = (time.time() - start_time) / 60
    print(f"\n{'='*80}")
    print(f"✅ SKEWED LEFT JOIN COMPLETED in {elapsed:.2f} minutes")
    print(f"{'='*80}\n")
    
    return final_result


# ===== USAGE =====

# Basic usage with default parameters
final_result = skewed_left_join(
    df_left=df_tempo,
    df_right=arm_loan_history,
    join_keys=["cusip", "effectivedate"]
)

# Advanced usage with custom parameters
final_result = skewed_left_join(
    df_left=df_tempo,
    df_right=arm_loan_history,
    join_keys=["cusip", "effectivedate"],
    skew_threshold=2_000_000,        # Consider keys with >2M rows as skewed
    salt_factor=1500,                 # Use 1500 salt values
    broadcast_threshold_gb=8,         # Broadcast if right table < 8GB
    batch_size=15,                    # Process 15 skewed keys per batch
    max_explosion_gb=15,              # Use batching if explosion > 15GB
    temp_output_path="s3://my-bucket/temp/join_batches"
)

# Write the final result
final_result.write.mode("overwrite").parquet("s3://your-bucket/final_output/")
```

## Additional Helper Function for Diagnostics

```python
def diagnose_skew(df, join_keys, top_n=10):
    """
    Diagnose skew in your data before running the join.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame to analyze
    join_keys : list
        Join key columns
    top_n : int
        Number of top skewed keys to display
    """
    print("="*80)
    print("SKEW ANALYSIS")
    print("="*80)
    
    # Get key distribution
    key_distribution = (
        df.groupBy(*join_keys)
        .count()
        .orderBy(F.col("count").desc())
    )
    
    # Statistics
    stats = key_distribution.agg(
        F.min("count").alias("min"),
        F.max("count").alias("max"),
        F.avg("count").alias("avg"),
        F.stddev("count").alias("stddev"),
        F.expr("percentile(count, 0.5)").alias("median"),
        F.expr("percentile(count, 0.95)").alias("p95"),
        F.expr("percentile(count, 0.99)").alias("p99")
    ).collect()[0]
    
    print(f"\nKey Distribution Statistics:")
    print(f"  Min rows per key:    {stats['min']:,}")
    print(f"  Max rows per key:    {stats['max']:,}")
    print(f"  Avg rows per key:    {stats['avg']:,.0f}")
    print(f"  Median rows per key: {stats['median']:,.0f}")
    print(f"  95th percentile:     {stats['p95']:,.0f}")
    print(f"  99th percentile:     {stats['p99']:,.0f}")
    print(f"  Std deviation:       {stats['stddev']:,.0f}")
    print(f"  Skew factor (max/avg): {stats['max']/stats['avg']:.2f}x")
    
    # Top skewed keys
    print(f"\nTop {top_n} most skewed keys:")
    top_skewed = key_distribution.limit(top_n).collect()
    for i, row in enumerate(top_skewed, 1):
        key_values = [row[k] for k in join_keys]
        print(f"  {i}. {dict(zip(join_keys, key_values))}: {row['count']:,} rows")
    
    # Recommendations
    print(f"\nRecommendations:")
    if stats['max'] > 10_000_000:
        print(f"  ⚠️  SEVERE SKEW: Max key has {stats['max']:,} rows")
        print(f"      → Use salt_factor >= {int(stats['max'] / 300_000)}")
    elif stats['max'] > 1_000_000:
        print(f"  ⚠️  MODERATE SKEW: Max key has {stats['max']:,} rows")
        print(f"      → Use salt_factor >= {int(stats['max'] / 300_000)}")
    else:
        print(f"  ✅ LOW SKEW: Max key has {stats['max']:,} rows")
        print(f"      → Standard join should work fine")
    
    print("="*80 + "\n")
    
    return key_distribution

# Usage
diagnose_skew(df_tempo, ["cusip", "effectivedate"], top_n=20)
diagnose_skew(arm_loan_history, ["cusip", "effectivedate"], top_n=20)
```

## Complete End-to-End Example

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import time

# ===== CONFIGURATION =====
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "4000")
spark.conf.set("spark.executor.memoryOverhead", "8g")

# ===== STEP 1: DIAGNOSE SKEW (OPTIONAL) =====
print("Analyzing left table skew...")
diagnose_skew(df_tempo, ["cusip", "effectivedate"], top_n=10)

print("\nAnalyzing right table skew...")
diagnose_skew(arm_loan_history, ["cusip", "effectivedate"], top_n=10)

# ===== STEP 2: PERFORM SKEWED JOIN =====
print("\nStarting skewed join...")

final_result = skewed_left_join(
    df_left=df_tempo,
    df_right=arm_loan_history,
    join_keys=["cusip", "effectivedate"],
    skew_threshold=1_000_000,          # Keys with >1M rows are skewed
    salt_factor=1500,                   # Reduced from 1800 to reduce explosion
    broadcast_threshold_gb=5,           # Broadcast if right skewed portion < 5GB
    batch_size=20,                      # Process 20 skewed keys per batch
    max_explosion_gb=10,                # Use batching if explosion > 10GB
    temp_output_path="s3://your-bucket/temp/skewed_join_batches"
)

# ===== STEP 3: WRITE FINAL RESULT =====
print("\nWriting final result...")
final_result.write.mode("overwrite").parquet("s3://your-bucket/final_output/")

print("\n✅ ALL DONE!")
```

## Key Features of This Solution

1. **✅ Pre-filters right table once** - no repeated full scans in loop
2. **✅ Automatic strategy selection** - broadcast, single salting, or batching
3. **✅ Memory efficient** - writes batches to disk immediately
4. **✅ Progress tracking** - detailed logging at each step
5. **✅ Configurable** - all thresholds can be tuned
6. **✅ Diagnostic tools** - analyze skew before running

This should complete your job in **1-3 hours** instead of 6+!