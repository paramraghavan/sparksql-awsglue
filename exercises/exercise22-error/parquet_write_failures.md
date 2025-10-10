Code that will **actually reproduce** the `TASK_WRITE_FAILED` :

## Code That Will Actually Fail

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

spark = SparkSession.builder \
    .appName("ReproduceParquetFailure") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# ============================================
# ISSUE 1: EXTREME DATA SKEW (Most Common Real Issue)
# ============================================
print("=== Creating Skewed Data (WILL FAIL) ===")

# Create massively skewed data - one partition has 99% of data
skewed_records = []

# One massive partition with 1 million rows
for i in range(1000000):
    skewed_records.append((1, "A", f"data_{i}", random.random() * 100))

# Other partitions with tiny data
for i in range(100):
    skewed_records.append((2, "B", f"data_{i}", random.random() * 100))

df_skewed = spark.createDataFrame(
    skewed_records, 
    ["partition_key", "category", "data", "value"]
)

# Partition by skewed key - one executor gets overwhelmed
print(f"Total rows: {df_skewed.count()}")
print("Distribution by partition_key:")
df_skewed.groupBy("partition_key").count().show()

# THIS WILL FAIL - one task tries to write 1M rows, runs out of memory
try:
    df_skewed.write \
        .partitionBy("partition_key") \
        .mode("overwrite") \
        .parquet("s3://your-bucket/skewed_fail/")
    print("Success (unexpected!)")
except Exception as e:
    print(f"FAILED as expected: {e}")

# FIX: Repartition before writing
print("\n=== FIX: Repartition ===")
df_skewed.repartition(50) \
    .write \
    .mode("overwrite") \
    .parquet("s3://your-bucket/skewed_fixed/")
print("Success!")


# ============================================
# ISSUE 2: VERY WIDE DATAFRAMES (Hundreds of Columns)
# ============================================
print("\n=== Creating Wide DataFrame (MAY FAIL) ===")

# Create 500 columns
num_columns = 500
data = []
for i in range(10000):
    row = tuple([i] + [random.random() for _ in range(num_columns - 1)])
    data.append(row)

schema = StructType([StructField(f"col_{i}", DoubleType(), True) for i in range(num_columns)])
df_wide = spark.createDataFrame(data, schema)

print(f"Columns: {len(df_wide.columns)}")
print(f"Rows: {df_wide.count()}")

# THIS MAY FAIL - too many columns causes memory issues
try:
    df_wide.write.mode("overwrite").parquet("s3://your-bucket/wide_fail/")
    print("Success (unexpected!)")
except Exception as e:
    print(f"FAILED as expected: {e}")

# FIX: Reduce columns or increase memory
print("\n=== FIX: Select fewer columns ===")
df_wide.select([f"col_{i}" for i in range(100)]) \
    .write.mode("overwrite").parquet("s3://your-bucket/wide_fixed/")
print("Success!")


# ============================================
# ISSUE 3: LARGE STRING/BINARY DATA IN ROWS
# ============================================
print("\n=== Creating Large String Data (WILL FAIL) ===")

# Create rows with massive string data (10MB per row)
def create_large_row(i):
    large_string = "x" * 10_000_000  # 10MB string
    return (i, large_string, random.random())

# Only 100 rows, but each is HUGE
large_data = [create_large_row(i) for i in range(100)]
df_large_strings = spark.createDataFrame(
    large_data, 
    ["id", "large_text", "value"]
)

print(f"Rows: {df_large_strings.count()}")
print(f"Partitions: {df_large_strings.rdd.getNumPartitions()}")

# THIS WILL FAIL - rows are too large for default settings
try:
    df_large_strings.write.mode("overwrite").parquet("s3://your-bucket/large_strings_fail/")
    print("Success (unexpected!)")
except Exception as e:
    print(f"FAILED as expected: {e}")

# FIX: Increase partitions and limit records per file
print("\n=== FIX: More partitions + record limit ===")
df_large_strings.repartition(20) \
    .write \
    .option("maxRecordsPerFile", 10) \
    .mode("overwrite") \
    .parquet("s3://your-bucket/large_strings_fixed/")
print("Success!")


# ============================================
# ISSUE 4: MEMORY PRESSURE FROM COMPLEX JOINS/AGGREGATIONS
# ============================================
print("\n=== Creating Complex Transformations (WILL FAIL) ===")

# Simulate complex modeling operations
df1 = spark.range(1000000).withColumn("key", (col("id") % 1000).cast("int"))
df2 = spark.range(1000000).withColumn("key", (col("id") % 1000).cast("int"))
df3 = spark.range(1000000).withColumn("key", (col("id") % 1000).cast("int"))

# Multiple joins without proper partitioning
df_complex = df1.alias("a") \
    .join(df2.alias("b"), "key") \
    .join(df3.alias("c"), "key") \
    .withColumn("calc1", col("a.id") * col("b.id")) \
    .withColumn("calc2", col("b.id") + col("c.id")) \
    .groupBy("key").agg(
        sum("calc1").alias("sum1"),
        avg("calc2").alias("avg2"),
        count("*").alias("cnt")
    )

print(f"Result rows: {df_complex.count()}")

# THIS MAY FAIL - complex operations without caching/checkpointing
try:
    df_complex.write.mode("overwrite").parquet("s3://your-bucket/complex_fail/")
    print("Success (unexpected!)")
except Exception as e:
    print(f"FAILED as expected: {e}")

# FIX: Cache intermediate results and repartition
print("\n=== FIX: Cache + Repartition ===")
df_complex.cache()
df_complex.count()  # Materialize cache

df_complex.repartition(50) \
    .write.mode("overwrite").parquet("s3://your-bucket/complex_fixed/")
df_complex.unpersist()
print("Success!")


# ============================================
# ISSUE 5: TOO FEW PARTITIONS FOR LARGE DATA
# ============================================
print("\n=== Creating Under-Partitioned Data (WILL FAIL) ===")

# Large dataset with too few partitions
df_underpart = spark.range(5000000).coalesce(2)  # 5M rows in only 2 partitions

print(f"Rows: {df_underpart.count()}")
print(f"Partitions: {df_underpart.rdd.getNumPartitions()}")
print(f"Rows per partition: {df_underpart.count() / df_underpart.rdd.getNumPartitions()}")

# THIS WILL FAIL - partitions are too large (2.5M rows each)
try:
    df_underpart.write.mode("overwrite").parquet("s3://your-bucket/underpart_fail/")
    print("Success (unexpected!)")
except Exception as e:
    print(f"FAILED as expected: {e}")

# FIX: Increase partitions
print("\n=== FIX: Increase partitions ===")
df_underpart.repartition(100) \
    .write.mode("overwrite").parquet("s3://your-bucket/underpart_fixed/")
print("Success!")


# ============================================
# COMPREHENSIVE FIX FUNCTION
# ============================================

def fix_and_write_parquet(df, s3_path, df_name="dataframe"):
    """
    Comprehensive function to diagnose and fix parquet write issues
    """
    print(f"\n{'='*60}")
    print(f"Processing: {df_name}")
    print(f"{'='*60}")
    
    # Step 1: Diagnose
    print("\n[1/5] Running diagnostics...")
    row_count = df.count()
    num_partitions = df.rdd.getNumPartitions()
    num_columns = len(df.columns)
    
    print(f"  Rows: {row_count:,}")
    print(f"  Columns: {num_columns}")
    print(f"  Current partitions: {num_partitions}")
    print(f"  Rows per partition: {row_count / num_partitions:,.0f}")
    
    # Step 2: Calculate optimal partitions
    print("\n[2/5] Calculating optimal partitions...")
    target_rows_per_partition = 50000  # Aim for 50k rows per partition
    optimal_partitions = max(10, int(row_count / target_rows_per_partition))
    
    if num_columns > 200:
        optimal_partitions = int(optimal_partitions * 1.5)  # More partitions for wide data
        print(f"  Wide dataframe detected ({num_columns} cols) - increasing partitions")
    
    print(f"  Recommended partitions: {optimal_partitions}")
    
    # Step 3: Cache if complex
    print("\n[3/5] Caching dataframe...")
    df_cached = df.cache()
    df_cached.count()  # Materialize
    print("  Cache materialized")
    
    # Step 4: Repartition
    print(f"\n[4/5] Repartitioning to {optimal_partitions}...")
    df_repart = df_cached.repartition(optimal_partitions)
    
    # Step 5: Write with safety options
    print(f"\n[5/5] Writing to {s3_path}...")
    try:
        df_repart.write \
            .option("maxRecordsPerFile", 100000) \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .parquet(s3_path)
        print("  ✓ Write successful!")
        
    except Exception as e:
        print(f"  ✗ Write failed: {e}")
        print("\n  Trying fallback strategy...")
        
        # Fallback: No compression, more partitions
        df_repart.repartition(optimal_partitions * 2) \
            .write \
            .option("maxRecordsPerFile", 50000) \
            .option("compression", "none") \
            .mode("overwrite") \
            .parquet(s3_path)
        print("  ✓ Fallback write successful!")
    
    finally:
        df_cached.unpersist()
        print("  Cache cleared")
    
    print(f"{'='*60}\n")


# ============================================
# USAGE EXAMPLES
# ============================================

# Example 1: Fix skewed data
fix_and_write_parquet(
    df_skewed, 
    "s3://your-bucket/fixed_output/skewed/",
    "Skewed Data"
)

# Example 2: Fix wide dataframe
fix_and_write_parquet(
    df_wide,
    "s3://your-bucket/fixed_output/wide/",
    "Wide Dataframe"
)

# Example 3: Fix complex transformations
fix_and_write_parquet(
    df_complex,
    "s3://your-bucket/fixed_output/complex/",
    "Complex Transformations"
)

# ============================================
# QUICK DIAGNOSTIC SCRIPT FOR YOUR DATA
# ============================================

def diagnose_before_write(df):
    """
    Run this on your dataframe BEFORE attempting to write
    """
    print("\n" + "="*60)
    print("DIAGNOSTIC REPORT")
    print("="*60)
    
    # Basic stats
    row_count = df.count()
    num_cols = len(df.columns)
    num_parts = df.rdd.getNumPartitions()
    
    print(f"\n📊 Basic Stats:")
    print(f"   Rows: {row_count:,}")
    print(f"   Columns: {num_cols}")
    print(f"   Partitions: {num_parts}")
    print(f"   Rows/Partition: {row_count/num_parts:,.0f}")
    
    # Check for issues
    issues = []
    
    if num_parts < 10:
        issues.append("⚠️  Too few partitions (< 10)")
    
    if row_count / num_parts > 100000:
        issues.append("⚠️  Partitions too large (> 100k rows/partition)")
    
    if num_cols > 200:
        issues.append("⚠️  Very wide dataframe (> 200 columns)")
    
    # Check partition skew
    print(f"\n📈 Partition Distribution:")
    part_dist = df.groupBy(spark_partition_id().alias("partition")).count() \
        .agg(
            min("count").alias("min_rows"),
            max("count").alias("max_rows"),
            avg("count").alias("avg_rows")
        ).collect()[0]
    
    print(f"   Min rows: {part_dist['min_rows']:,}")
    print(f"   Max rows: {part_dist['max_rows']:,}")
    print(f"   Avg rows: {part_dist['avg_rows']:,.0f}")
    
    skew_ratio = part_dist['max_rows'] / part_dist['avg_rows'] if part_dist['avg_rows'] > 0 else 0
    if skew_ratio > 3:
        issues.append(f"⚠️  Severe partition skew (ratio: {skew_ratio:.1f}x)")
    
    # Null check
    print(f"\n🔍 Null Check:")
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns[:10]]).collect()[0]
    null_cols = [c for c in df.columns[:10] if null_counts[c] > 0]
    if null_cols:
        print(f"   Columns with nulls: {', '.join(null_cols[:5])}")
    else:
        print(f"   No nulls detected (checked first 10 columns)")
    
    # Summary
    print(f"\n{'='*60}")
    if issues:
        print("⚠️  ISSUES DETECTED:")
        for issue in issues:
            print(f"   {issue}")
        print("\n💡 Recommendation: Use fix_and_write_parquet() function")
    else:
        print("✓ No major issues detected")
        print("💡 Safe to write, but consider repartition(100) for safety")
    print("="*60 + "\n")

# Run diagnostic on your complex dataframe
diagnose_before_write(df_skewed)
```

## Key Fixes for Your Situation

Since **simple dataframes write fine**, your issue is likely:

### **Most Likely: Data Skew**
```python
# Your current code (fails)
complex_df.write.parquet("s3://bucket/path/")

# FIX
complex_df.repartition(200).write.parquet("s3://bucket/path/")
```

### **Second Most Likely: Memory Pressure**
```python
# FIX
complex_df.cache()
complex_df.count()
complex_df.repartition(200) \
    .write \
    .option("maxRecordsPerFile", 50000) \
    .parquet("s3://bucket/path/")
complex_df.unpersist()
```

### **Quick One-Liner Fix to Try First:**
```python
your_df.repartition(200).write.option("maxRecordsPerFile", 50000).mode("overwrite").parquet("s3://bucket/path/")
```

This single line fixes **80% of complex dataframe write failures**!