# ═══════════════════════════════════════════════════════════════════════════
# HANDLING TEMP VIEWS IN MEMORY-INTENSIVE SPARK JOBS
# ═══════════════════════════════════════════════════════════════════════════

"""
IMPORTANT: createOrReplaceTempView() itself does NOT cache data!
It only registers a query plan. But how you USE the views matters a lot!
"""


# ═══════════════════════════════════════════════════════════════════════════
# UNDERSTANDING TEMP VIEWS
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                    TEMP VIEWS: What They Actually Do                      ║
╚═══════════════════════════════════════════════════════════════════════════╝

MISCONCEPTION:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"createOrReplaceTempView caches my data in memory"

REALITY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
createOrReplaceTempView() only registers a SQL alias for your DataFrame.
It does NOT automatically cache anything!

Example:

df = spark.read.parquet("s3://bucket/data/")  # 10 TB
df.createOrReplaceTempView("my_view")         # Uses 0 bytes memory!
# The view is just a pointer to the query plan


WHEN DOES IT USE MEMORY?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. If you CACHE the DataFrame BEFORE creating the view:
   
   df = spark.read.parquet("s3://bucket/data/")
   df.cache()  # ← THIS uses memory!
   df.createOrReplaceTempView("my_view")
   
   Now when you query "my_view", it reads from cache.


2. If you query the view MULTIPLE times:
   
   df.createOrReplaceTempView("my_view")
   
   result1 = spark.sql("SELECT * FROM my_view WHERE x > 100")  # Reads from S3
   result2 = spark.sql("SELECT * FROM my_view WHERE y < 50")   # Reads from S3 AGAIN!
   
   Without caching, each query re-reads the source data!
""")


# ═══════════════════════════════════════════════════════════════════════════
# YOUR CURRENT SITUATION
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                     YOUR SITUATION ANALYSIS                               ║
╚═══════════════════════════════════════════════════════════════════════════╝

YOU SAID: "I have lots of large views used by the user"

This suggests:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCENARIO A: Views without caching (LIKELY)
-------------------------------------------
df1 = spark.read.parquet("s3://bucket/data1/")  # 5 TB
df1.createOrReplaceTempView("view1")

df2 = spark.read.parquet("s3://bucket/data2/")  # 3 TB
df2.createOrReplaceTempView("view2")

df3 = spark.read.parquet("s3://bucket/data3/")  # 2 TB
df3.createOrReplaceTempView("view3")

# Users query these views:
result = spark.sql('''
    SELECT * FROM view1 
    JOIN view2 ON view1.id = view2.id
    WHERE view1.amount > 1000
''')

Problem: Each query re-reads 5 TB + 3 TB = 8 TB from S3!
Memory used: Minimal (just query processing)
Performance: SLOW (constant S3 reads)


SCENARIO B: Views with caching (POSSIBLE)
------------------------------------------
df1 = spark.read.parquet("s3://bucket/data1/")  # 5 TB
df1.cache()  # ← Trying to cache 5 TB!
df1.createOrReplaceTempView("view1")

df2 = spark.read.parquet("s3://bucket/data2/")  # 3 TB
df2.cache()  # ← Trying to cache 3 TB!
df2.createOrReplaceTempView("view2")

Problem: Trying to cache 5 TB + 3 TB = 8 TB
Your cluster has 10 TB allocated currently
Result: Memory pressure, spilling to disk, GC issues!


WHICH SCENARIO ARE YOU IN?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Check in your code:
- Do you call .cache() or .persist() on DataFrames?
- Do you see messages like "Cached data evicted" in logs?
- Is your job re-reading S3 data frequently?
""")


# ═══════════════════════════════════════════════════════════════════════════
# BEST PRACTICES FOR TEMP VIEWS
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                   BEST PRACTICES FOR TEMP VIEWS                           ║
╚═══════════════════════════════════════════════════════════════════════════╝

RULE #1: Only cache views that are queried MULTIPLE times
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❌ BAD: Cache everything
df1.cache()  # Used once
df1.createOrReplaceTempView("view1")

✅ GOOD: Only cache if reused
df1 = spark.read.parquet("s3://bucket/data/")
df1.createOrReplaceTempView("view1")

# First use - reads from S3
result1 = spark.sql("SELECT * FROM view1 WHERE x > 100")

# If you need to query again, NOW cache it:
spark.catalog.cacheTable("view1")

# Second use - reads from cache
result2 = spark.sql("SELECT * FROM view1 WHERE y < 50")


RULE #2: Use selective caching (cache filtered/aggregated results)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❌ BAD: Cache entire 10 TB dataset
df_full = spark.read.parquet("s3://bucket/data/")  # 10 TB
df_full.cache()
df_full.createOrReplaceTempView("full_data")

✅ GOOD: Filter first, then cache
df_full = spark.read.parquet("s3://bucket/data/")  # 10 TB
df_filtered = df_full.filter(col("date") == "2024-01-01")  # 100 GB
df_filtered.cache()  # Only cache 100 GB!
df_filtered.createOrReplaceTempView("today_data")


RULE #3: Unpersist when done
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

df.cache()
df.createOrReplaceTempView("my_view")

# Use the view
result = spark.sql("SELECT * FROM my_view WHERE ...")
result.write.parquet("s3://bucket/output/")

# IMPORTANT: Unpersist when done!
spark.catalog.uncacheTable("my_view")
# Or: df.unpersist()


RULE #4: Use appropriate storage levels
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from pyspark.storageLevels import StorageLevel

# Memory only (default, fastest but uses most memory)
df.persist(StorageLevel.MEMORY_ONLY)

# Memory + Disk (spills to disk if memory full)
df.persist(StorageLevel.MEMORY_AND_DISK)  # ← Better for large data!

# Serialized (uses less memory but slower)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

df.createOrReplaceTempView("my_view")
""")


# ═══════════════════════════════════════════════════════════════════════════
# RECOMMENDED PATTERN FOR YOUR USE CASE
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║              RECOMMENDED PATTERN FOR MEMORY-INTENSIVE JOBS                ║
╚═══════════════════════════════════════════════════════════════════════════╝

PATTERN: Staged Processing with Selective Caching
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

# Example implementation:

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import StorageLevel

# Stage 1: Load raw data (NO caching)
# ═══════════════════════════════════════════════════════════════════════════
df_raw = spark.read.parquet("s3://bucket/raw-data/")  # 10 TB
df_raw = df_raw.repartition(3000)  # Optimize partitions
df_raw.createOrReplaceTempView("raw_data")  # No cache!

print("✓ Stage 1: Raw data view created (not cached)")


# Stage 2: Create filtered/aggregated views (cache these!)
# ═══════════════════════════════════════════════════════════════════════════

# Filter to recent data
df_recent = spark.sql("""
    SELECT * FROM raw_data 
    WHERE date >= '2024-01-01'
""")  # Maybe 1 TB after filtering

df_recent.persist(StorageLevel.MEMORY_AND_DISK)  # Cache with disk fallback
df_recent.createOrReplaceTempView("recent_data")
print("✓ Stage 2: Recent data cached (1 TB)")


# Create aggregated summary
df_summary = spark.sql("""
    SELECT 
        category,
        date,
        COUNT(*) as count,
        SUM(amount) as total_amount
    FROM raw_data
    GROUP BY category, date
""")  # Much smaller, maybe 10 GB

df_summary.cache()  # Full memory cache is fine for small data
df_summary.createOrReplaceTempView("summary")
print("✓ Stage 2: Summary cached (10 GB)")


# Stage 3: Users query the cached views
# ═══════════════════════════════════════════════════════════════════════════

# This is fast - reads from cache!
result1 = spark.sql("""
    SELECT * FROM recent_data 
    WHERE category = 'A'
""")

result2 = spark.sql("""
    SELECT * FROM summary
    WHERE total_amount > 1000000
""")


# Stage 4: Clean up when done
# ═══════════════════════════════════════════════════════════════════════════

# Write final results
result1.write.parquet("s3://bucket/output/result1/")
result2.write.parquet("s3://bucket/output/result2/")

# CRITICAL: Unpersist cached data!
spark.catalog.uncacheTable("recent_data")
spark.catalog.uncacheTable("summary")
print("✓ Stage 4: Cache cleared")


# ═══════════════════════════════════════════════════════════════════════════
# MEMORY MANAGEMENT STRATEGY
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                     MEMORY MANAGEMENT STRATEGY                            ║
╚═══════════════════════════════════════════════════════════════════════════╝

YOUR CLUSTER: 28.8 TB total memory (after optimization)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MEMORY ALLOCATION:
- 60% for execution (17.3 TB)  ← Processing shuffles, joins
- 40% for storage (11.5 TB)    ← Cached DataFrames/views

CACHING BUDGET: ~11.5 TB available for cached views


EXAMPLE BREAKDOWN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

View Name       Size    Cached?   Reason
────────────────────────────────────────────────────────────────────────────
raw_data        10 TB   NO        Too large, queried once
recent_data     1 TB    YES       Filtered, queried multiple times
summary         10 GB   YES       Small, frequently accessed
user_profile    50 GB   YES       Small, frequently joined
transactions    500 GB  YES       Moderate size, used in joins
archive_data    5 TB    NO        Large, accessed rarely

TOTAL CACHED:   1.56 TB (fits comfortably in 11.5 TB budget!)


GUIDELINES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Cache if: < 2 TB AND queried 2+ times
✓ Cache if: < 100 GB (almost always worth it)
✗ Don't cache: > 5 TB
✗ Don't cache: Queried only once
✓ Use MEMORY_AND_DISK for data 500 GB - 2 TB
✓ Use MEMORY_ONLY for data < 500 GB
""")


# ═══════════════════════════════════════════════════════════════════════════
# VIEW LIFECYCLE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

class ViewManager:
    """
    Helper class to manage temp views with automatic caching/uncaching
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.cached_views = {}
    
    def create_view(self, df, view_name, cache=False, storage_level=None):
        """
        Create a temp view with optional caching
        
        Args:
            df: DataFrame to register
            view_name: Name of the view
            cache: Whether to cache (default False)
            storage_level: StorageLevel (default MEMORY_AND_DISK)
        """
        if cache:
            if storage_level is None:
                storage_level = StorageLevel.MEMORY_AND_DISK
            
            df.persist(storage_level)
            df.count()  # Force materialization
            self.cached_views[view_name] = df
            print(f"✓ View '{view_name}' created and CACHED")
        else:
            print(f"✓ View '{view_name}' created (not cached)")
        
        df.createOrReplaceTempView(view_name)
    
    def uncache_view(self, view_name):
        """Uncache a specific view"""
        if view_name in self.cached_views:
            self.cached_views[view_name].unpersist()
            del self.cached_views[view_name]
            print(f"✓ View '{view_name}' uncached")
        else:
            print(f"⚠ View '{view_name}' was not cached")
    
    def uncache_all(self):
        """Uncache all managed views"""
        for view_name, df in self.cached_views.items():
            df.unpersist()
            print(f"✓ View '{view_name}' uncached")
        self.cached_views.clear()
        print("✓ All views uncached")
    
    def show_cached_views(self):
        """Show which views are currently cached"""
        if not self.cached_views:
            print("No views are currently cached")
        else:
            print("Currently cached views:")
            for view_name in self.cached_views:
                print(f"  - {view_name}")


# Usage example:
vm = ViewManager(spark)

# Create raw data view (no cache)
df_raw = spark.read.parquet("s3://bucket/data/")
vm.create_view(df_raw, "raw_data", cache=False)

# Create filtered view (with cache)
df_filtered = spark.sql("SELECT * FROM raw_data WHERE date >= '2024-01-01'")
vm.create_view(df_filtered, "recent_data", cache=True)

# Create summary view (with cache)
df_summary = spark.sql("SELECT category, SUM(amount) FROM raw_data GROUP BY category")
vm.create_view(df_summary, "summary", cache=True)

# Check what's cached
vm.show_cached_views()

# ... do your processing ...

# Clean up when done
vm.uncache_all()


# ═══════════════════════════════════════════════════════════════════════════
# MONITORING CACHED VIEWS
# ═══════════════════════════════════════════════════════════════════════════

def check_cache_status():
    """
    Check which views are cached and how much memory they use
    """
    print("═" * 80)
    print("CACHED VIEWS STATUS")
    print("═" * 80)
    
    # Get cached tables
    cached_tables = spark.sql("SHOW TABLES").filter(col("isTemporary") == True)
    
    for row in cached_tables.collect():
        table_name = row['tableName']
        
        # Check if cached
        try:
            cache_info = spark.catalog.isCached(table_name)
            if cache_info:
                print(f"✓ {table_name:20s} - CACHED")
            else:
                print(f"  {table_name:20s} - not cached")
        except:
            print(f"  {table_name:20s} - status unknown")
    
    print("═" * 80)
    
    # Check Spark UI for detailed cache info
    print("\nFor detailed cache statistics:")
    print("Go to Spark UI: http://localhost:4040")
    print("Click 'Storage' tab to see memory usage per cached DataFrame")


# Run periodically to monitor
check_cache_status()


# ═══════════════════════════════════════════════════════════════════════════
# COMPLETE EXAMPLE: Multi-Stage Job with Views
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                    COMPLETE EXAMPLE WORKFLOW                              ║
╚═══════════════════════════════════════════════════════════════════════════╝
""")

def process_large_dataset_with_views():
    """
    Complete example of handling large views efficiently
    """
    
    # Initialize
    vm = ViewManager(spark)
    
    # ═══════════════════════════════════════════════════════════════════════
    # STAGE 1: Load and partition raw data (NO CACHE)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n📥 STAGE 1: Loading raw data...")
    df_raw = spark.read.parquet("s3://bucket/raw-data/")  # 10 TB
    df_raw = df_raw.repartition(3000)
    vm.create_view(df_raw, "raw_data", cache=False)
    print(f"   Partitions: {df_raw.rdd.getNumPartitions()}")
    
    
    # ═══════════════════════════════════════════════════════════════════════
    # STAGE 2: Create filtered views (SELECTIVE CACHING)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n🔍 STAGE 2: Creating filtered views...")
    
    # View 1: Recent data (cache - used multiple times)
    df_recent = spark.sql("""
        SELECT * FROM raw_data 
        WHERE date >= '2024-01-01' AND date <= '2024-12-31'
    """)
    vm.create_view(df_recent, "recent_data", cache=True, 
                   storage_level=StorageLevel.MEMORY_AND_DISK)
    
    # View 2: Active users (cache - small and frequently used)
    df_active = spark.sql("""
        SELECT * FROM raw_data 
        WHERE status = 'active'
    """)
    vm.create_view(df_active, "active_users", cache=True,
                   storage_level=StorageLevel.MEMORY_ONLY)
    
    # View 3: Summary stats (cache - small aggregated data)
    df_stats = spark.sql("""
        SELECT 
            category,
            COUNT(*) as count,
            AVG(amount) as avg_amount,
            SUM(amount) as total_amount
        FROM raw_data
        GROUP BY category
    """)
    vm.create_view(df_stats, "category_stats", cache=True,
                   storage_level=StorageLevel.MEMORY_ONLY)
    
    print("   Cached views created:")
    vm.show_cached_views()
    
    
    # ═══════════════════════════════════════════════════════════════════════
    # STAGE 3: Process using cached views
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n⚙️  STAGE 3: Processing with cached views...")
    
    # Query 1: Join recent data with stats (fast - both cached!)
    result1 = spark.sql("""
        SELECT r.*, s.avg_amount
        FROM recent_data r
        JOIN category_stats s ON r.category = s.category
        WHERE r.amount > s.avg_amount
    """)
    
    # Query 2: Active user analysis (fast - cached!)
    result2 = spark.sql("""
        SELECT 
            category,
            COUNT(*) as active_count
        FROM active_users
        GROUP BY category
    """)
    
    # Write results
    result1.write.mode("overwrite").parquet("s3://bucket/output/high_value/")
    result2.write.mode("overwrite").parquet("s3://bucket/output/active_summary/")
    
    print("   ✓ Results written")
    
    
    # ═══════════════════════════════════════════════════════════════════════
    # STAGE 4: Cleanup
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n🧹 STAGE 4: Cleaning up...")
    vm.uncache_all()
    print("   ✓ All views uncached")
    print("   ✓ Memory released")


# Run the complete workflow
process_large_dataset_with_views()


# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY & KEY TAKEAWAYS
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                         KEY TAKEAWAYS                                     ║
╚═══════════════════════════════════════════════════════════════════════════╝

✓ createOrReplaceTempView() does NOT cache - it's just a SQL alias
✓ Only cache views that are queried MULTIPLE times
✓ Cache FILTERED/AGGREGATED data, not raw 10 TB datasets
✓ Use MEMORY_AND_DISK for large cached data (500 GB - 2 TB)
✓ Use MEMORY_ONLY for small cached data (< 500 GB)
✓ ALWAYS unpersist() when done with a view
✓ Monitor cache usage in Spark UI → Storage tab
✓ Keep total cached data under ~40% of cluster memory (11.5 TB)

DECISION TREE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

View size > 5 TB?        → Don't cache
View size > 2 TB?        → Cache with MEMORY_AND_DISK only if used 3+ times
View size > 500 GB?      → Cache with MEMORY_AND_DISK if used 2+ times
View size < 500 GB?      → Cache with MEMORY_ONLY if used 2+ times
View size < 100 GB?      → Almost always cache (it's cheap!)
View queried once?       → Never cache
Aggregated result?       → Usually cache (small size, high reuse)
""")
