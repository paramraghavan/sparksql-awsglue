Systematic approach to debug and fix:

## Step 1: Quick Diagnosis - Find the Real Problem

```python
from pyspark.sql.types import ArrayType, MapType, StructType
from pyspark.sql.functions import (
    spark_partition_id, 
    desc, 
    sum as _sum,  # Alias to avoid Python's built-in sum
    col, 
    min as _min,  # Alias to avoid Python's built-in min
    max as _max,  # Alias to avoid Python's built-in max
    avg, 
    stddev
)
import traceback

def diagnose_failing_job(df, job_name="my_job"):
    """
    Run comprehensive diagnostics to find the real issue
    """
    print(f"\n{'='*70}")
    print(f"DIAGNOSTIC REPORT: {job_name}")
    print(f"{'='*70}\n")
    
    # 1. Basic Stats
    print("📊 [1/6] Basic Statistics...")
    try:
        row_count = df.count()
        num_partitions = df.rdd.getNumPartitions()
        num_columns = len(df.columns)
        
        print(f"   ✓ Rows: {row_count:,}")
        print(f"   ✓ Columns: {num_columns}")
        print(f"   ✓ Partitions: {num_partitions}")
        print(f"   ✓ Rows per partition: {row_count/num_partitions:,.0f}")
        
        # Flag issues
        if row_count/num_partitions > 100000:
            print(f"   ⚠️  WARNING: Partitions too large (>100k rows each)")
        if num_partitions < 10:
            print(f"   ⚠️  WARNING: Too few partitions")
            
    except Exception as e:
        print(f"   ✗ FAILED during count: {e}")
        print(f"   → Issue: DataFrame may have corrupt data or unstable transformations")
        return
    
    # 2. Partition Skew Check
    print("\n📈 [2/6] Checking Partition Skew...")
    try:
        skew_df = df.withColumn("partition_id", spark_partition_id()) \
            .groupBy("partition_id") \
            .count() \
            .orderBy(desc("count"))
        
        skew_stats = skew_df.agg(
            _min("count").alias("min"),      # Use _min
            _max("count").alias("max"),      # Use _max
            avg("count").alias("avg"),
            stddev("count").alias("stddev")
        ).collect()[0]
        
        print(f"   Min rows in partition: {skew_stats['min']:,}")
        print(f"   Max rows in partition: {skew_stats['max']:,}")
        print(f"   Avg rows in partition: {skew_stats['avg']:,.0f}")
        
        if skew_stats['stddev']:
            print(f"   Std deviation: {skew_stats['stddev']:,.0f}")
        
        skew_ratio = skew_stats['max'] / skew_stats['avg'] if skew_stats['avg'] > 0 else 0
        if skew_ratio > 3:
            print(f"   🔴 CRITICAL: Severe data skew detected (ratio: {skew_ratio:.1f}x)")
            print(f"   → This is likely causing task failures!")
            print("\n   Top 5 largest partitions:")
            skew_df.show(5)
        else:
            print(f"   ✓ Skew ratio: {skew_ratio:.1f}x (acceptable)")
            
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        traceback.print_exc()
        skew_ratio = 0  # Set default for later checks
    
    # 3. Memory Estimation
    print("\n💾 [3/6] Estimating Memory Usage...")
    estimated_partition_mb = 0
    try:
        sample_df = df.limit(100)
        sample_df.cache()
        sample_df.count()
        
        rows_per_partition = row_count / num_partitions
        estimated_partition_mb = (rows_per_partition / 100) * 10
        
        print(f"   Estimated partition size: ~{estimated_partition_mb:.0f} MB")
        
        if estimated_partition_mb > 500:
            print(f"   🔴 CRITICAL: Partitions likely too large for memory")
            print(f"   → Increase partitions to reduce memory pressure")
        
        sample_df.unpersist()
        
    except Exception as e:
        print(f"   ⚠️  Could not estimate: {e}")
    
    # 4. Schema Complexity
    print("\n🔍 [4/6] Checking Schema Complexity...")
    complex_types = 0
    nested_fields = []
    
    for field in df.schema.fields:
        if isinstance(field.dataType, (ArrayType, MapType, StructType)):
            complex_types += 1
            nested_fields.append(field.name)
    
    print(f"   Total columns: {num_columns}")
    print(f"   Complex/nested columns: {complex_types}")
    
    if complex_types > 10:
        print(f"   ⚠️  WARNING: Many complex types may cause issues")
        print(f"   Nested fields: {', '.join(nested_fields[:5])}")
    
    if num_columns > 200:
        print(f"   ⚠️  WARNING: Very wide schema (>200 columns)")
    
    # 5. Null Check - FIXED
    print("\n🔎 [5/6] Checking for Nulls (sample)...")
    try:
        null_check_cols = df.columns[:20]
        null_counts = df.select([
            _sum(col(c).isNull().cast("int")).alias(c)  # Use _sum here
            for c in null_check_cols
        ]).collect()[0]
        
        high_null_cols = [(c, null_counts[c]) for c in null_check_cols 
                         if null_counts[c] > row_count * 0.5]
        
        if high_null_cols:
            print(f"   ⚠️  Columns with >50% nulls:")
            for col_name, null_count in high_null_cols[:5]:
                print(f"      - {col_name}: {null_count:,} nulls ({null_count/row_count*100:.1f}%)")
        else:
            print(f"   ✓ No excessive nulls detected")
            
    except Exception as e:
        print(f"   ⚠️  Could not check nulls: {e}")
        traceback.print_exc()
    
    # 6. Write Test
    print("\n✍️  [6/6] Testing Small Write...")
    try:
        test_path = "s3://your-bucket/diagnostic_test_write/"
        df.limit(1000).write.mode("overwrite").parquet(test_path)
        print(f"   ✓ Small write successful")
        print(f"   → S3 permissions are OK")
        
    except Exception as e:
        print(f"   ✗ Small write FAILED: {e}")
        print(f"   → Check S3 permissions or network connectivity")
    
    # Summary
    print(f"\n{'='*70}")
    print("📋 SUMMARY & RECOMMENDATIONS:")
    print(f"{'='*70}")
    
    if skew_ratio > 3:
        print("\n🔴 PRIMARY ISSUE: Data Skew")
        print("   FIX: Use df.repartition(200) or add salt column")
    
    if row_count/num_partitions > 100000:
        print("\n🔴 PRIMARY ISSUE: Partitions Too Large")
        print("   FIX: Increase partitions with df.repartition()")
    
    if estimated_partition_mb > 500:
        print("\n🔴 PRIMARY ISSUE: Memory Pressure")
        print("   FIX: Increase partitions + use maxRecordsPerFile")
    
    print("\n💡 Quick fix to try:")
    print("   df.repartition(200).write.option('maxRecordsPerFile', 50000).parquet(...)")
    print(f"{'='*70}\n")


# Usage
diagnose_failing_job(your_df, "my_complex_job")

# Run diagnostics on your complex dataframe
diagnose_failing_job(your_complex_df, "production_scoring_job")
```

## Step 2: Progressive Isolation Strategy

```python
def isolate_failure_point(df, s3_base_path):
    """
    Progressively test writes to find where it fails
    """
    print("\n🔬 ISOLATION TEST - Finding failure point...\n")
    
    # Test 1: Very small sample
    print("[Test 1/5] Writing 100 rows...")
    try:
        df.limit(100).write.mode("overwrite").parquet(f"{s3_base_path}/test1_100rows/")
        print("✓ PASSED - 100 rows write OK\n")
    except Exception as e:
        print(f"✗ FAILED at 100 rows: {e}")
        print("→ Issue: Basic S3 connectivity or permissions\n")
        return
    
    # Test 2: Medium sample
    print("[Test 2/5] Writing 10,000 rows...")
    try:
        df.limit(10000).write.mode("overwrite").parquet(f"{s3_base_path}/test2_10krows/")
        print("✓ PASSED - 10k rows write OK\n")
    except Exception as e:
        print(f"✗ FAILED at 10k rows: {e}")
        print("→ Issue: Data or schema problems\n")
        return
    
    # Test 3: Large sample with repartition
    print("[Test 3/5] Writing 100,000 rows with repartition...")
    try:
        df.limit(100000).repartition(20) \
            .write.mode("overwrite").parquet(f"{s3_base_path}/test3_100k_repart/")
        print("✓ PASSED - 100k rows with repartition OK\n")
    except Exception as e:
        print(f"✗ FAILED at 100k rows: {e}")
        print("→ Issue: Likely partition size or memory\n")
        return
    
    # Test 4: Full data with aggressive repartitioning
    print("[Test 4/5] Writing FULL data with repartition(200)...")
    try:
        df.repartition(200) \
            .write.mode("overwrite").parquet(f"{s3_base_path}/test4_full_repart/")
        print("✓ PASSED - Full data with repartition OK")
        print("→ Solution: Use repartition(200) in production!\n")
        return
    except Exception as e:
        print(f"✗ FAILED with repartition: {e}")
        print("→ Trying more aggressive fixes...\n")
    
    # Test 5: Full data with all safety measures
    print("[Test 5/5] Writing with ALL safety measures...")
    try:
        df.repartition(300) \
            .write \
            .option("maxRecordsPerFile", 50000) \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .parquet(f"{s3_base_path}/test5_full_safe/")
        print("✓ PASSED - Full data with safety measures OK")
        print("→ Solution: Use these options in production!\n")
    except Exception as e:
        print(f"✗ FAILED even with safety measures: {e}")
        print("→ Need deeper investigation (see advanced debugging below)\n")


# Run isolation test
isolate_failure_point(your_complex_df, "s3://your-bucket/isolation_tests")
```

## Step 3: Find Which Partition is Failing

```python
def find_bad_partition(df, s3_base_path):
    """
    Write partitions one-by-one to find the problematic one
    """
    print("\n🎯 PARTITION-BY-PARTITION TEST\n")
    
    num_partitions = df.rdd.getNumPartitions()
    print(f"Testing {num_partitions} partitions individually...\n")
    
    failed_partitions = []
    
    for i in range(num_partitions):
        try:
            print(f"  Testing partition {i}/{num_partitions}...", end=" ")
            
            partition_df = df.filter(spark_partition_id() == i)
            partition_count = partition_df.count()
            
            partition_df.write.mode("overwrite") \
                .parquet(f"{s3_base_path}/partition_test/part_{i}/")
            
            print(f"✓ OK ({partition_count:,} rows)")
            
        except Exception as e:
            print(f"✗ FAILED")
            failed_partitions.append((i, partition_count, str(e)))
            
            # Show sample of bad data
            print(f"\n    Sample from failed partition {i}:")
            partition_df.show(5, truncate=False)
            print()
    
    if failed_partitions:
        print(f"\n{'='*70}")
        print(f"🔴 Found {len(failed_partitions)} failing partition(s):")
        print(f"{'='*70}")
        for part_id, row_count, error in failed_partitions:
            print(f"\nPartition {part_id}: {row_count:,} rows")
            print(f"Error: {error[:200]}")
        print(f"\n→ These partitions contain problematic data")
        print(f"{'='*70}\n")
    else:
        print("\n✓ All partitions write successfully individually")
        print("→ Issue may be with concurrent writes or resource contention\n")


# Run if other tests pass individually but full write fails
find_bad_partition(your_complex_df, "s3://your-bucket/partition_tests")
```

## Step 4: Production-Ready Fix

```python
def robust_write_to_s3(df, s3_path, job_name="spark_job"):
    """
    Production-ready write function with all safety measures
    """
    from pyspark import StorageLevel
    
    print(f"\n{'='*70}")
    print(f"ROBUST WRITE: {job_name}")
    print(f"Target: {s3_path}")
    print(f"{'='*70}\n")
    
    # Step 1: Get stats
    print("[1/7] Analyzing dataframe...")
    row_count = df.count()
    num_partitions = df.rdd.getNumPartitions()
    num_columns = len(df.columns)
    
    print(f"  Rows: {row_count:,}")
    print(f"  Columns: {num_columns}")
    print(f"  Current partitions: {num_partitions}")
    
    # Step 2: Calculate optimal partitions
    print("\n[2/7] Calculating optimal configuration...")
    target_rows_per_partition = 50000
    optimal_partitions = max(20, int(row_count / target_rows_per_partition))
    
    # Adjust for wide dataframes
    if num_columns > 200:
        optimal_partitions = int(optimal_partitions * 1.5)
    
    print(f"  Target partitions: {optimal_partitions}")
    print(f"  Target rows/partition: ~{row_count/optimal_partitions:,.0f}")
    
    # Step 3: Cache to stabilize transformations
    print("\n[3/7] Caching dataframe...")
    df_cached = df.persist(StorageLevel.MEMORY_AND_DISK)
    df_cached.count()  # Materialize
    print("  ✓ Cache materialized")
    
    # Step 4: Repartition
    print(f"\n[4/7] Repartitioning to {optimal_partitions}...")
    df_repartitioned = df_cached.repartition(optimal_partitions)
    
    # Step 5: Verify partition distribution
    print("\n[5/7] Verifying partition distribution...")
    partition_stats = df_repartitioned.groupBy(spark_partition_id()) \
        .count() \
        .agg(min("count"), max("count"), avg("count")) \
        .collect()[0]
    
    print(f"  Min: {partition_stats[0]:,} rows")
    print(f"  Max: {partition_stats[1]:,} rows")
    print(f"  Avg: {partition_stats[2]:,.0f} rows")
    
    skew_ratio = partition_stats[1] / partition_stats[2] if partition_stats[2] > 0 else 0
    if skew_ratio > 2:
        print(f"  ⚠️  Still some skew (ratio: {skew_ratio:.1f}x)")
        print("  → Using more aggressive partitioning...")
        df_repartitioned = df_cached.repartition(optimal_partitions * 2)
    
    # Step 6: Write with safety options
    print(f"\n[6/7] Writing to S3...")
    try:
        df_repartitioned.write \
            .option("maxRecordsPerFile", 50000) \
            .option("compression", "snappy") \
            .option("parquet.block.size", 134217728) \
            .mode("overwrite") \
            .parquet(s3_path)
        
        print("  ✓ Write completed successfully!")
        
    except Exception as e:
        print(f"  ✗ Write failed: {e}")
        print("\n  Attempting fallback strategy...")
        
        # Fallback: Even more partitions, no compression
        df_repartitioned.repartition(optimal_partitions * 3) \
            .write \
            .option("maxRecordsPerFile", 25000) \
            .option("compression", "none") \
            .mode("overwrite") \
            .parquet(s3_path)
        
        print("  ✓ Fallback write successful")
    
    # Step 7: Cleanup
    print("\n[7/7] Cleaning up...")
    df_cached.unpersist()
    print("  ✓ Cache cleared")
    
    print(f"\n{'='*70}")
    print(f"✓ JOB COMPLETE: {job_name}")
    print(f"{'='*70}\n")


# Use in production
robust_write_to_s3(
    your_complex_df, 
    "s3://your-bucket/production/output/",
    "scoring_job_v2"
)
```

## Step 5: Spark Configuration Tuning

```python
# Add these configurations at the start of your job
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Increase memory and timeouts
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.network.timeout", "800s")
spark.conf.set("spark.executor.heartbeatInterval", "60s")

# S3 specific optimizations
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "200")
spark.conf.set("spark.hadoop.fs.s3a.threads.max", "40")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
```

## Quick Action Plan

**Try these in order:**

1. **First, run diagnostics:**
```python
diagnose_failing_job(your_df)
```

2. **Then try the quick fix:**
```python
your_df.repartition(200).write.option("maxRecordsPerFile", 50000).parquet("s3://...")
```

3. **If that fails, use robust write:**
```python
robust_write_to_s3(your_df, "s3://...")
```

4. **If still failing, isolate the problem:**
```python
isolate_failure_point(your_df, "s3://...")
find_bad_partition(your_df, "s3://...")
```

The diagnostic functions will tell you **exactly** what's wrong and how to fix it!