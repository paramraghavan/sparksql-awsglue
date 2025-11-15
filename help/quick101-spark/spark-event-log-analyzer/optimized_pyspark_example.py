"""
Sample PySpark Script with Built-in Monitoring and Optimization
This script demonstrates best practices for EMR PySpark jobs with monitoring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf
import time
import json
import os


def create_optimized_spark_session(app_name="OptimizedSparkApp"):
    """
    Create a Spark session with optimized settings for EMR
    """
    conf = SparkConf()
    
    # Core optimizations
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Shuffle optimization
    conf.set("spark.sql.shuffle.partitions", "400")
    conf.set("spark.shuffle.compress", "true")
    conf.set("spark.shuffle.spill.compress", "true")
    
    # Memory optimization
    conf.set("spark.memory.fraction", "0.8")
    conf.set("spark.memory.storageFraction", "0.3")
    
    # S3 optimization for EMR
    conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    conf.set("spark.hadoop.fs.s3a.threads.max", "20")
    conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    
    # Event logging
    conf.set("spark.eventLog.enabled", "true")
    # Use HDFS for event logs (automatically configured on EMR)
    # Or specify S3: conf.set("spark.eventLog.dir", "s3://my-bucket/spark-logs/")
    
    # Dynamic allocation
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.dynamicAllocation.minExecutors", "2")
    conf.set("spark.dynamicAllocation.maxExecutors", "20")
    
    # Create session
    spark = SparkSession.builder \
        .appName(app_name) \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark


def log_application_info(spark):
    """
    Log important application information
    """
    sc = spark.sparkContext
    
    info = {
        "application_id": sc.applicationId,
        "application_name": sc.appName,
        "spark_version": sc.version,
        "master": sc.master,
        "deploy_mode": sc.deployMode,
        "start_time": time.time()
    }
    
    print("=" * 80)
    print("SPARK APPLICATION INFO")
    print("=" * 80)
    for key, value in info.items():
        print(f"{key}: {value}")
    print("=" * 80)
    
    # Save to file for later reference
    with open(f"/tmp/{sc.applicationId}_info.json", "w") as f:
        json.dump(info, f, indent=2)
    
    return info


def monitor_dataframe(df, stage_name="DataFrame"):
    """
    Monitor DataFrame characteristics and provide optimization hints
    """
    print(f"\n--- Monitoring: {stage_name} ---")
    
    # Get partition count
    num_partitions = df.rdd.getNumPartitions()
    print(f"Number of partitions: {num_partitions}")
    
    # Check if data is cached
    is_cached = df.storageLevel.useMemory or df.storageLevel.useDisk
    print(f"Is cached: {is_cached}")
    
    # Optimization hints
    if num_partitions < 10:
        print(f"⚠️  WARNING: Low partition count ({num_partitions}). Consider repartitioning.")
    elif num_partitions > 1000:
        print(f"⚠️  WARNING: High partition count ({num_partitions}). May cause overhead.")
    
    return num_partitions


def optimize_partitions(df, target_partitions=None, target_size_mb=128):
    """
    Optimize DataFrame partitions based on data size
    """
    current_partitions = df.rdd.getNumPartitions()
    
    if target_partitions is None:
        # Estimate optimal partitions (rough estimate: 128MB per partition)
        # This is a heuristic and should be adjusted based on your data
        target_partitions = max(current_partitions, 10)
    
    if current_partitions != target_partitions:
        print(f"Repartitioning from {current_partitions} to {target_partitions} partitions")
        df = df.repartition(target_partitions)
    
    return df


def example_etl_job(spark):
    """
    Example ETL job with optimization best practices
    """
    
    # Example: Read data from S3
    input_path = "s3://my-bucket/input-data/"
    output_path = "s3://my-bucket/output-data/"
    
    print("\n1. Reading data...")
    df = spark.read.parquet(input_path)
    
    # Monitor initial state
    monitor_dataframe(df, "Initial Load")
    
    # Show schema
    print("\nSchema:")
    df.printSchema()
    
    # Example: Filter early to reduce data size
    print("\n2. Filtering data (push down filters)...")
    df_filtered = df.filter(col("date") >= "2024-01-01")
    
    # Example: Optimize partitions
    print("\n3. Optimizing partitions...")
    df_filtered = optimize_partitions(df_filtered, target_partitions=200)
    
    # Example: Aggregation with proper partitioning
    print("\n4. Performing aggregation...")
    df_agg = df_filtered.groupBy("category", "region") \
        .agg(
            sum("revenue").alias("total_revenue"),
            count("*").alias("record_count"),
            avg("price").alias("avg_price")
        )
    
    # For small result sets, consider broadcasting
    # If df_agg is small (< 10MB), you can use it in broadcast joins:
    # df_joined = large_df.join(broadcast(df_agg), "category")
    
    # Example: Handle skewed data
    print("\n5. Checking for data skew...")
    skew_check = df_filtered.groupBy("category").count().orderBy(col("count").desc())
    skew_check.show(10)
    
    # If skew detected, consider salting:
    # df_salted = df_filtered.withColumn("salt", (rand() * 10).cast("int"))
    # Then join on both key and salt
    
    # Example: Cache if reusing DataFrame
    print("\n6. Caching intermediate results...")
    df_agg.cache()
    df_agg.count()  # Materialize the cache
    
    # Example: Coalesce before writing (reduce small files)
    print("\n7. Writing output...")
    df_agg.coalesce(10).write \
        .mode("overwrite") \
        .partitionBy("region") \
        .parquet(output_path)
    
    print("\n✓ ETL job complete")
    
    return df_agg


def get_job_metrics(spark):
    """
    Get basic job metrics (this is a simplified version)
    For detailed metrics, use the event log analyzer
    """
    sc = spark.sparkContext
    status = sc.statusTracker()
    
    metrics = {
        "active_jobs": len(status.getActiveJobIds()),
        "active_stages": len(status.getActiveStageIds()),
    }
    
    print("\n--- Job Metrics ---")
    for key, value in metrics.items():
        print(f"{key}: {value}")
    
    return metrics


def main():
    """
    Main execution function
    """
    print("Starting Optimized PySpark Job on EMR\n")
    
    # Create optimized Spark session
    spark = create_optimized_spark_session("EMR_Optimized_Job")
    
    # Log application info
    app_info = log_application_info(spark)
    
    try:
        # Run your ETL job
        result_df = example_etl_job(spark)
        
        # Get final metrics
        metrics = get_job_metrics(spark)
        
        # Print completion info
        print("\n" + "=" * 80)
        print("JOB COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"Application ID: {spark.sparkContext.applicationId}")
        print("\nTo analyze this job, run:")
        print(f"python spark_optimizer.py <event-log-path> --app-id {spark.sparkContext.applicationId}")
        print("\nEvent logs location:")
        print("  HDFS: hdfs dfs -ls /var/log/spark/apps/")
        print(f"  Specific log: hdfs dfs -get /var/log/spark/apps/{spark.sparkContext.applicationId}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise
    finally:
        # Always stop the Spark session
        spark.stop()


if __name__ == "__main__":
    main()


"""
OPTIMIZATION CHECKLIST FOR PYSPARK ON EMR:

1. DATA READING:
   ✓ Use columnar formats (Parquet, ORC) instead of CSV/JSON
   ✓ Use predicate pushdown (filter early)
   ✓ Specify schema to avoid inference
   
2. TRANSFORMATIONS:
   ✓ Filter and select columns early
   ✓ Avoid UDFs when possible (use built-in functions)
   ✓ Use broadcast joins for small tables (< 10MB)
   ✓ Repartition/coalesce appropriately
   ✓ Use proper partitioning for data locality
   
3. SHUFFLES:
   ✓ Minimize shuffles (combine operations)
   ✓ Use reduceByKey instead of groupByKey
   ✓ Enable adaptive query execution
   ✓ Handle data skew with salting
   
4. MEMORY:
   ✓ Cache/persist only when reusing data
   ✓ Unpersist when done
   ✓ Monitor memory spills
   ✓ Tune executor memory and overhead
   
5. WRITING:
   ✓ Coalesce to reduce small files
   ✓ Use appropriate partitioning
   ✓ Consider bucketing for large tables
   ✓ Use compression (snappy for speed, gzip for size)
   
6. MONITORING:
   ✓ Enable event logging
   ✓ Monitor Spark UI
   ✓ Check for task failures and retries
   ✓ Analyze event logs post-execution
   
7. EMR-SPECIFIC:
   ✓ Use S3 optimizations
   ✓ Enable dynamic allocation
   ✓ Use appropriate instance types
   ✓ Monitor YARN resource utilization
"""


"""
COMMON ANTI-PATTERNS TO AVOID:

1. ❌ collect() on large datasets
   ✓ Use take(n) or write to storage

2. ❌ Count before every action for logging
   ✓ Count only when necessary

3. ❌ Too many small partitions or too few large partitions
   ✓ Aim for 128MB-256MB per partition

4. ❌ Not caching data that's reused multiple times
   ✓ Cache intermediate results used 2+ times

5. ❌ Caching everything
   ✓ Only cache what's reused

6. ❌ Using groupByKey for aggregations
   ✓ Use reduceByKey or aggregateByKey

7. ❌ Writing many small files
   ✓ Coalesce before writing

8. ❌ Not handling data skew
   ✓ Use salting or broadcast joins

9. ❌ Complex UDFs
   ✓ Use built-in functions when possible

10. ❌ Not monitoring job execution
    ✓ Always review Spark UI and event logs
"""
