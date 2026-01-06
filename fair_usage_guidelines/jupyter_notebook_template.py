# EMR Jupyter Notebook Template
# ==============================
# Copy this template for all your PySpark analysis on the shared EMR cluster

## 📝 Instructions:
# 1. Update the "app_name" below with your name and job description
# 2. Choose the right "job_type" for your data size
# 3. Run the cells in order
# 4. Always run the cleanup cell at the end!

# ============================================================================
# CELL 1: Import Helper Module (Recommended Method)
# ============================================================================

# Option A: If you have the helper module uploaded to S3 or local
# !pip install boto3 requests  # If not already installed

try:
    from emr_spark_helper import quick_start, get_application_stats, stop_spark_session
    print("✅ Helper module loaded successfully")
    USE_HELPER = True
except ImportError:
    print("⚠️ Helper module not found, using manual setup")
    print("   Download emr_spark_helper.py to use automated setup")
    USE_HELPER = False


# ============================================================================
# CELL 2A: Create Spark Session (Using Helper - RECOMMENDED)
# ============================================================================

if USE_HELPER:
    # 🎯 UPDATE THESE VALUES FOR YOUR JOB
    spark = quick_start(
        app_name="YourName_DataAnalysis",     # 👈 Change this to your name and job type
        job_type="medium",                    # 👈 Options: "small", "medium", "large"
        max_executors=20                      # 👈 Adjust based on expected data size
    )


# ============================================================================
# CELL 2B: Create Spark Session (Manual Setup - If helper not available)
# ============================================================================

if not USE_HELPER:
    from pyspark.sql import SparkSession
    from datetime import datetime
    import requests
    
    # Health Check
    def check_cluster_health():
        try:
            response = requests.get("http://localhost:8088/cluster/cluster", timeout=10)
            if response.status_code == 200:
                print("✅ Cluster is healthy")
                return True
            else:
                print(f"⚠️ Cluster returned status: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Cluster check failed: {e}")
            raise
    
    # Run health check
    check_cluster_health()
    
    # 🎯 UPDATE THESE VALUES FOR YOUR JOB
    APP_NAME = "YourName_DataAnalysis"        # 👈 Change this
    MAX_EXECUTORS = 20                        # 👈 Adjust based on job size
    
    # Create Spark Session
    spark = SparkSession.builder \
        .appName(f"{APP_NAME}_{datetime.now().strftime('%Y%m%d_%H%M')}") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", str(MAX_EXECUTORS)) \
        .config("spark.dynamicAllocation.initialExecutors", "2") \
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
        .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    print(f"🔗 Spark UI: http://localhost:4040")


# ============================================================================
# CELL 3: Check Application Status
# ============================================================================

if USE_HELPER:
    get_application_stats(spark)
else:
    # Manual status check
    sc = spark.sparkContext
    print(f"📊 Application ID: {sc.applicationId}")
    print(f"📱 Application Name: {sc.appName}")
    print(f"🔗 Spark UI: http://localhost:4040")


# ============================================================================
# CELL 4: YOUR DATA PROCESSING CODE STARTS HERE
# ============================================================================

# Example: Read data from S3
# df = spark.read.parquet("s3://your-bucket/your-data/")

# Example: Read CSV with options
# df = spark.read.option("header", "true").csv("s3://your-bucket/data.csv")

# Example: Create sample data for testing
from pyspark.sql import Row
sample_data = [
    Row(id=1, name="Alice", value=100),
    Row(id=2, name="Bob", value=200),
    Row(id=3, name="Charlie", value=300)
]
df = spark.createDataFrame(sample_data)

# Show sample
print("📊 Sample Data:")
df.show()

# Get basic stats
print(f"Total Rows: {df.count()}")
print(f"Columns: {df.columns}")


# ============================================================================
# CELL 5: Data Transformations
# ============================================================================

# Your transformations here
# Example:
from pyspark.sql.functions import col, sum, avg

result_df = df.groupBy("name").agg(
    sum("value").alias("total_value"),
    avg("value").alias("avg_value")
)

result_df.show()


# ============================================================================
# CELL 6: Write Results (if needed)
# ============================================================================

# Example: Write to S3
# result_df.write.mode("overwrite").parquet("s3://your-bucket/output/")

# Example: Write as CSV
# result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://your-bucket/output.csv")

print("✅ Data processing complete")


# ============================================================================
# CELL 7: Check Resource Usage During Job
# ============================================================================

# Run this cell anytime to see your current resource usage
if USE_HELPER:
    get_application_stats(spark)
    
    # Optional: Show cluster-wide resources
    from emr_spark_helper import show_cluster_resources
    show_cluster_resources()


# ============================================================================
# CELL 8: ⚠️ CLEANUP - ALWAYS RUN THIS WHEN DONE! ⚠️
# ============================================================================

# This is CRITICAL - it releases resources back to the cluster for other users!

if USE_HELPER:
    stop_spark_session(spark)
else:
    # Manual cleanup
    try:
        spark.catalog.clearCache()
        print("✅ Cache cleared")
    except:
        pass
    
    try:
        spark.stop()
        print("✅ Spark session stopped")
        print("♻️ Resources released back to cluster")
    except:
        pass

print("\n" + "="*70)
print("✅ Notebook cleanup complete!")
print("Thank you for being a good cluster citizen! 🎉")
print("="*70)


# ============================================================================
# 📚 Quick Reference Guide
# ============================================================================

"""
COMMON SPARK OPERATIONS:

1. READING DATA:
   - Parquet: spark.read.parquet("s3://bucket/path/")
   - CSV:     spark.read.option("header", "true").csv("s3://bucket/file.csv")
   - JSON:    spark.read.json("s3://bucket/file.json")

2. WRITING DATA:
   - Parquet: df.write.mode("overwrite").parquet("s3://bucket/output/")
   - CSV:     df.coalesce(1).write.mode("overwrite").csv("s3://bucket/output/")

3. CACHING (Use sparingly!):
   - Cache:    df.cache()
   - Persist:  df.persist()
   - Unpersist: df.unpersist()  # Always unpersist when done!

4. PARTITIONING:
   - Repartition:  df.repartition(200)
   - Coalesce:     df.coalesce(10)

5. COMMON ERRORS & FIXES:
   
   Error: "Job is slow"
   Fix: 
   - Check partition count: df.rdd.getNumPartitions()
   - Repartition if needed: df = df.repartition(200)
   - Use Parquet instead of CSV
   
   Error: "Out of Memory"
   Fix:
   - Reduce executor cores in config
   - Increase memoryOverhead
   - Break job into smaller steps
   - Unpersist cached DataFrames you're done with
   
   Error: "Container killed by YARN"
   Fix:
   - Reduce max_executors
   - Lower executor.memory
   - Check if other users are running large jobs

6. PERFORMANCE TIPS:
   - Use .explain() to see query plan
   - Partition on columns used in WHERE clauses
   - Filter early, aggregate late
   - Use broadcast joins for small tables (< 10MB)
   - Prefer Parquet over CSV (5-10x faster)

7. MONITORING:
   - Spark UI: http://localhost:4040
   - YARN UI:  http://localhost:8088
   - Check logs: yarn logs -applicationId <app_id>
"""


# ============================================================================
# 🔗 Useful Links
# ============================================================================

"""
📌 CLUSTER INFORMATION:
- YARN ResourceManager: http://localhost:8088
- Spark UI:             http://localhost:4040
- Spark History:        http://localhost:18080

📚 DOCUMENTATION:
- Spark SQL Guide:      https://spark.apache.org/docs/latest/sql-programming-guide.html
- PySpark API:          https://spark.apache.org/docs/latest/api/python/
- EMR Best Practices:   https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-instances-guidelines.html

📧 SUPPORT:
- Data Team: data-team@company.com
- Cluster Admin: cluster-admin@company.com

💡 TIPS:
1. Name your jobs descriptively (include your name!)
2. Use dynamic allocation (already configured ✅)
3. Monitor your resource usage
4. Always run the cleanup cell when done
5. Be considerate of other users sharing the cluster
"""
