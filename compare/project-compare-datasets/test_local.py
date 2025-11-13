#!/usr/bin/env python3
"""
Local Testing Script for Parquet Comparison
Use this for development and testing with smaller datasets before running on EMR
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os

def create_test_data():
    """Create sample parquet files for testing"""
    spark = SparkSession.builder \
        .appName("CreateTestData") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("Creating test datasets...")
    
    # Create first dataset
    df1 = spark.range(0, 10000).select(
        F.col("id"),
        (F.col("id") % 100).alias("category"),
        (F.rand() * 1000).cast("int").alias("value1"),
        (F.rand() * 100).cast("int").alias("value2"),
        F.current_timestamp().alias("timestamp"),
        F.lit("A").alias("status"),
        F.concat(F.lit("user_"), F.col("id")).alias("user_id")
    )
    
    # Add many columns to simulate 200+ column scenario
    for i in range(1, 50):
        df1 = df1.withColumn(f"col_{i}", (F.rand() * 100).cast("int"))
    
    df1.write.mode("overwrite").parquet("/tmp/test_data1.parquet")
    print(f"Created test_data1.parquet: {df1.count()} records, {len(df1.columns)} columns")
    
    # Create second dataset with some differences
    df2 = spark.range(0, 10000).select(
        F.col("id"),
        (F.col("id") % 100).alias("category"),
        (F.rand() * 1000).cast("int").alias("value1"),  # Different values
        (F.col("id") % 50).cast("int").alias("value2"),  # Different calculation
        F.current_timestamp().alias("timestamp"),
        F.when(F.col("id") % 10 == 0, "B").otherwise("A").alias("status"),  # 10% different
        F.concat(F.lit("user_"), F.col("id")).alias("user_id")
    )
    
    # Add same columns but with different values
    for i in range(1, 50):
        df2 = df2.withColumn(f"col_{i}", (F.rand() * 100).cast("int"))
    
    # Add a few extra columns to df2
    df2 = df2.withColumn("extra_col1", F.lit("extra"))
    df2 = df2.withColumn("extra_col2", F.rand())
    
    # Remove some records and add new ones to simulate differences
    df2 = df2.filter(F.col("id") < 9900)  # Remove last 100
    
    new_records = spark.range(10000, 10200).select(
        F.col("id"),
        (F.col("id") % 100).alias("category"),
        (F.rand() * 1000).cast("int").alias("value1"),
        (F.col("id") % 50).cast("int").alias("value2"),
        F.current_timestamp().alias("timestamp"),
        F.lit("C").alias("status"),
        F.concat(F.lit("user_"), F.col("id")).alias("user_id")
    )
    
    for i in range(1, 50):
        new_records = new_records.withColumn(f"col_{i}", (F.rand() * 100).cast("int"))
    
    new_records = new_records.withColumn("extra_col1", F.lit("extra"))
    new_records = new_records.withColumn("extra_col2", F.rand())
    
    df2 = df2.union(new_records)
    
    df2.write.mode("overwrite").parquet("/tmp/test_data2.parquet")
    print(f"Created test_data2.parquet: {df2.count()} records, {len(df2.columns)} columns")
    
    spark.stop()
    
    print("\n✅ Test data created successfully!")
    print("Expected differences:")
    print("  - Record count: 10,000 vs 10,100")
    print("  - 100 records only in dataset 1")
    print("  - 200 records only in dataset 2")
    print("  - ~1000 records with different 'status' values")
    print("  - 2 columns only in dataset 2 (extra_col1, extra_col2)")
    print("  - All other columns will have random value differences")


def run_local_comparison():
    """Run comparison locally"""
    print("\n" + "="*80)
    print("RUNNING LOCAL COMPARISON TEST")
    print("="*80 + "\n")
    
    # Import the main script
    sys.path.insert(0, os.path.dirname(__file__))
    from parquet_comparison import ParquetComparator
    
    # Create local Spark session
    spark = SparkSession.builder \
        .appName("LocalParquetComparison") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Define parameters
    key_columns = ["id", "category"]
    skip_columns = ["timestamp"]
    
    # Run comparison
    comparator = ParquetComparator(spark, key_columns, skip_columns)
    results = comparator.run_comparison(
        "/tmp/test_data1.parquet",
        "/tmp/test_data2.parquet",
        "TestDataset1",
        "TestDataset2"
    )
    
    # Generate report
    comparator.generate_html_report(
        "/tmp/test_comparison_report.html",
        "TestDataset1",
        "TestDataset2"
    )
    
    spark.stop()
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
    print("\n📄 Report generated: /tmp/test_comparison_report.html")
    print("🔍 Unique records saved:")
    print("   - /tmp/TestDataset1_unique_records")
    print("   - /tmp/TestDataset2_unique_records")
    print("\n💡 Open the HTML report in your browser to view results!")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Local testing for parquet comparison')
    parser.add_argument('--create-data', action='store_true', help='Create test data')
    parser.add_argument('--run-test', action='store_true', help='Run comparison test')
    parser.add_argument('--full', action='store_true', help='Create data and run test')
    
    args = parser.parse_args()
    
    if args.full or (not args.create_data and not args.run_test):
        create_test_data()
        run_local_comparison()
    elif args.create_data:
        create_test_data()
    elif args.run_test:
        run_local_comparison()
    
    print("\n✅ All done!")


if __name__ == "__main__":
    main()
