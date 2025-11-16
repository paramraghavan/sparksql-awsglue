"""
Example Customizations for Dataset Comparison
Shows different ways to adapt the comparison script for specific needs
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================================
# Example 1: Compare with date partitioning (faster for large datasets)
# ============================================================================

def compare_with_date_partition(spark, ds1_path, ds2_path, start_date, end_date):
    """
    Compare only data within a specific date range
    Much faster for large historical datasets
    """
    print(f"Comparing data from {start_date} to {end_date}")
    
    # Load with date filter
    ds1 = spark.read.parquet(ds1_path).filter(
        (F.col("date") >= start_date) & (F.col("date") <= end_date)
    )
    ds2 = spark.read.parquet(ds2_path).filter(
        (F.col("date") >= start_date) & (F.col("date") <= end_date)
    )
    
    print(f"DS1 rows in range: {ds1.count():,}")
    print(f"DS2 rows in range: {ds2.count():,}")
    
    return ds1, ds2


# ============================================================================
# Example 2: Compare with tolerance for numeric columns
# ============================================================================

def compare_with_numeric_tolerance(df1, df2, numeric_cols, tolerance=0.01):
    """
    Compare numeric columns with a tolerance threshold
    Useful for comparing financial data with rounding differences
    """
    print(f"Comparing with {tolerance} tolerance for: {numeric_cols}")
    
    for col in numeric_cols:
        # Add comparison column that checks if difference is within tolerance
        df1 = df1.withColumn(
            f"{col}_match",
            F.when(
                F.abs(F.col(col) - F.col(f"{col}_ds2")) <= tolerance,
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    
    return df1


# ============================================================================
# Example 3: Compare specific columns only (faster)
# ============================================================================

def compare_critical_columns_only(spark, ds1_path, ds2_path, key_cols, compare_cols):
    """
    Compare only specific columns instead of all columns
    Significantly faster for datasets with 100+ columns
    """
    print(f"Comparing only these columns: {compare_cols}")
    
    # Select only needed columns
    select_cols = key_cols + compare_cols
    ds1 = spark.read.parquet(ds1_path).select(select_cols)
    ds2 = spark.read.parquet(ds2_path).select(select_cols)
    
    print(f"Reduced from many columns to {len(select_cols)} columns")
    
    return ds1, ds2


# ============================================================================
# Example 4: Compare with data quality checks
# ============================================================================

def compare_with_quality_checks(df, dataset_name):
    """
    Add data quality checks before comparison
    """
    print(f"\nData Quality Report for {dataset_name}:")
    print("-" * 60)
    
    total_rows = df.count()
    print(f"Total rows: {total_rows:,}")
    
    # Check for nulls in each column
    null_counts = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Show columns with nulls
    cols_with_nulls = {k: v for k, v in null_counts.items() if v > 0}
    if cols_with_nulls:
        print("\nColumns with NULL values:")
        for col, count in cols_with_nulls.items():
            pct = (count / total_rows) * 100
            print(f"  {col}: {count:,} ({pct:.2f}%)")
    else:
        print("No NULL values found")
    
    # Check for duplicates on key columns
    # (assuming key_columns is defined in scope)
    # dup_count = df.groupBy(key_columns).count().filter("count > 1").count()
    # print(f"\nDuplicate keys: {dup_count:,}")
    
    print("-" * 60)
    
    return df


# ============================================================================
# Example 5: Compare with sampling for very large datasets
# ============================================================================

def quick_sample_comparison(spark, ds1_path, ds2_path, sample_pct=0.1):
    """
    Compare a sample of the data for quick validation
    Use this for initial checks before running full comparison
    """
    print(f"Comparing {sample_pct*100}% sample of data")
    
    ds1 = spark.read.parquet(ds1_path).sample(sample_pct)
    ds2 = spark.read.parquet(ds2_path).sample(sample_pct)
    
    print(f"Sample DS1 rows: {ds1.count():,}")
    print(f"Sample DS2 rows: {ds2.count():,}")
    
    return ds1, ds2


# ============================================================================
# Example 6: Handle schema differences
# ============================================================================

def align_schemas(ds1, ds2):
    """
    Align schemas between two datasets
    Useful when datasets have different columns or column orders
    """
    print("Aligning schemas...")
    
    # Get common columns
    common_cols = set(ds1.columns).intersection(set(ds2.columns))
    only_in_ds1 = set(ds1.columns) - set(ds2.columns)
    only_in_ds2 = set(ds2.columns) - set(ds1.columns)
    
    print(f"Common columns: {len(common_cols)}")
    print(f"Only in DS1: {len(only_in_ds1)} - {list(only_in_ds1)[:5]}...")
    print(f"Only in DS2: {len(only_in_ds2)} - {list(only_in_ds2)[:5]}...")
    
    # Select only common columns in same order
    common_cols_list = sorted(list(common_cols))
    ds1_aligned = ds1.select(common_cols_list)
    ds2_aligned = ds2.select(common_cols_list)
    
    return ds1_aligned, ds2_aligned, common_cols_list


# ============================================================================
# Example 7: Compare with grouped aggregations
# ============================================================================

def compare_aggregates(ds1, ds2, group_cols, agg_cols):
    """
    Compare aggregated data instead of row-by-row
    Useful for comparing summarized reports
    """
    print(f"Comparing aggregates grouped by: {group_cols}")
    
    # Create aggregations
    agg_exprs = [F.sum(col).alias(f"sum_{col}") for col in agg_cols]
    agg_exprs += [F.avg(col).alias(f"avg_{col}") for col in agg_cols]
    agg_exprs += [F.count("*").alias("row_count")]
    
    ds1_agg = ds1.groupBy(group_cols).agg(*agg_exprs)
    ds2_agg = ds2.groupBy(group_cols).agg(*agg_exprs)
    
    # Join and compare
    comparison = ds1_agg.alias("a").join(
        ds2_agg.alias("b"),
        group_cols,
        "full_outer"
    )
    
    # Find mismatches
    for col in agg_exprs:
        col_name = col.name if hasattr(col, 'name') else str(col)
        comparison = comparison.withColumn(
            f"{col_name}_match",
            F.col(f"a.{col_name}") == F.col(f"b.{col_name}")
        )
    
    return comparison


# ============================================================================
# Example 8: Export differences to different formats
# ============================================================================

def export_differences_to_formats(diff_df, output_base_path):
    """
    Export differences to multiple formats for different use cases
    """
    print("Exporting differences to multiple formats...")
    
    # 1. Parquet (best for further Spark processing)
    diff_df.write.mode("overwrite").parquet(f"{output_base_path}/differences.parquet")
    print("✓ Parquet format saved")
    
    # 2. CSV (for Excel/business users) - use coalesce to control file count
    diff_df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_base_path}/differences.csv")
    print("✓ CSV format saved")
    
    # 3. JSON (for APIs/web apps)
    diff_df.coalesce(1).write.mode("overwrite") \
        .option("compression", "gzip") \
        .json(f"{output_base_path}/differences.json")
    print("✓ JSON format saved")
    
    # 4. Delta Lake (if available - best for incremental updates)
    # diff_df.write.format("delta").mode("overwrite") \
    #     .save(f"{output_base_path}/differences.delta")
    # print("✓ Delta format saved")


# ============================================================================
# Example 9: Incremental comparison (compare only new/changed data)
# ============================================================================

def incremental_comparison(spark, ds1_path, ds2_path, last_run_date):
    """
    Compare only data that's changed since last run
    Much faster for daily comparisons
    """
    print(f"Comparing data changed since: {last_run_date}")
    
    # Assuming you have a 'updated_at' or 'modified_date' column
    ds1 = spark.read.parquet(ds1_path).filter(F.col("updated_at") > last_run_date)
    ds2 = spark.read.parquet(ds2_path).filter(F.col("updated_at") > last_run_date)
    
    print(f"DS1 new/changed rows: {ds1.count():,}")
    print(f"DS2 new/changed rows: {ds2.count():,}")
    
    return ds1, ds2


# ============================================================================
# Example 10: Complete example with multiple customizations
# ============================================================================

def custom_comparison_example():
    """
    Complete example showing how to use multiple customizations together
    """
    spark = SparkSession.builder \
        .appName("CustomDatasetComparison") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Configuration
    DS1_PATH = "s3://bucket/dataset1/"
    DS2_PATH = "s3://bucket/dataset2/"
    KEY_COLUMNS = ["customer_id", "date"]
    COMPARE_COLUMNS = ["amount", "status", "quantity"]  # Only compare these
    OUTPUT_PATH = "s3://bucket/comparison-output/"
    
    # Step 1: Load with date filtering (faster)
    print("\n=== Step 1: Loading data with date filter ===")
    ds1, ds2 = compare_with_date_partition(
        spark, DS1_PATH, DS2_PATH, 
        "2025-01-01", "2025-01-31"
    )
    
    # Step 2: Align schemas
    print("\n=== Step 2: Aligning schemas ===")
    ds1, ds2, common_cols = align_schemas(ds1, ds2)
    
    # Step 3: Run quality checks
    print("\n=== Step 3: Data quality checks ===")
    ds1 = compare_with_quality_checks(ds1, "DS1")
    ds2 = compare_with_quality_checks(ds2, "DS2")
    
    # Step 4: Compare only critical columns
    print("\n=== Step 4: Comparing critical columns ===")
    ds1, ds2 = compare_critical_columns_only(
        spark, DS1_PATH, DS2_PATH,
        KEY_COLUMNS, COMPARE_COLUMNS
    )
    
    # Step 5: Run comparison with your main comparison logic here
    # (Use the DatasetComparator class from main script)
    print("\n=== Step 5: Running comparison ===")
    # ... comparison logic ...
    
    # Step 6: Export results
    print("\n=== Step 6: Exporting results ===")
    # export_differences_to_formats(diff_df, OUTPUT_PATH)
    
    spark.stop()


# ============================================================================
# Example 11: Parallel comparison for very large datasets
# ============================================================================

def parallel_comparison_by_partition(spark, ds1_path, ds2_path, partition_col):
    """
    Process comparisons in parallel by partition
    Useful for very large datasets partitioned by date/region/etc.
    """
    print(f"Running parallel comparison by partition: {partition_col}")
    
    # Get unique partition values
    partitions = spark.read.parquet(ds1_path) \
        .select(partition_col).distinct() \
        .rdd.map(lambda x: x[0]).collect()
    
    print(f"Found {len(partitions)} partitions to compare")
    
    # Process each partition separately
    all_diffs = []
    for partition in partitions:
        print(f"Processing partition: {partition}")
        
        ds1_part = spark.read.parquet(ds1_path).filter(
            F.col(partition_col) == partition
        )
        ds2_part = spark.read.parquet(ds2_path).filter(
            F.col(partition_col) == partition
        )
        
        # Run comparison for this partition
        # ... comparison logic ...
        # all_diffs.append(diff_df)
    
    # Union all partition results
    # final_diffs = all_diffs[0]
    # for df in all_diffs[1:]:
    #     final_diffs = final_diffs.union(df)
    
    # return final_diffs


if __name__ == "__main__":
    print("""
    ========================================================================
    Dataset Comparison - Example Customizations
    ========================================================================
    
    This file shows various ways to customize the comparison:
    
    1. compare_with_date_partition()      - Filter by date for faster processing
    2. compare_with_numeric_tolerance()   - Allow small differences in numbers
    3. compare_critical_columns_only()    - Compare only specific columns
    4. compare_with_quality_checks()      - Add data quality validation
    5. quick_sample_comparison()          - Sample data for quick checks
    6. align_schemas()                    - Handle different schemas
    7. compare_aggregates()               - Compare aggregated data
    8. export_differences_to_formats()    - Export to multiple formats
    9. incremental_comparison()           - Compare only changed data
    10. parallel_comparison_by_partition() - Process in parallel
    
    To use these examples:
    - Copy the relevant functions into your main script
    - Modify them to fit your specific use case
    - Combine multiple approaches as needed
    
    For production use, integrate these into the DatasetComparator class
    from parquet_dataset_compare.py
    ========================================================================
    """)
