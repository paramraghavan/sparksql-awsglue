"""
=============================================================================
COMPREHENSIVE PYSPARK JOIN TROUBLESHOOTING GUIDE
=============================================================================

PROBLEM: PySpark EMR job hangs on join operation with full dataset but works
with 1M rows. Cluster resources (cores/memory) are not fully utilized.

ROOT CAUSES:
1. Extreme data skew (few keys have disproportionate data)
2. Duplicate join keys causing Cartesian explosion
3. Insufficient parallelization
4. Poor partition distribution

This guide provides diagnostic steps and solutions for all scenarios.
=============================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, expr, broadcast, concat, lit, rand, monotonically_increasing_id,
    row_number, desc, count, explode, array, substring, spark_partition_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from functools import reduce


# =============================================================================
# SECTION 1: INITIAL DIAGNOSTICS
# =============================================================================
def safe_get_conf(spark, key, default='not set'):
    """Safely get Spark config with default value."""
    try:
        return spark.conf.get(key)
    except Exception:
        return default


def diagnose_join_problem(df_tempo, arm_loan_history, spark):
    """
    Run comprehensive diagnostics to identify the root cause.
    Run this BEFORE attempting any fixes.
    """
    print("=" * 80)
    print("RUNNING COMPREHENSIVE DIAGNOSTICS")
    print("=" * 80)

    # 1. Check current Spark configuration
    print("\n1. SPARK CONFIGURATION")
    print("-" * 80)
    print(f"Executors: {spark.conf.get('spark.executor.instances', 'dynamic')}")
    print(f"Executor cores: {spark.conf.get('spark.executor.cores', 'not set')}")
    print(f"Executor memory: {spark.conf.get('spark.executor.memory', 'not set')}")
    print(f"Driver memory: {spark.conf.get('spark.driver.memory', 'not set')}")
    print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions', 'not set')}")
    print(f"Default parallelism: {spark.conf.get('spark.default.parallelism', 'not set')}")
    print(f"AQE enabled: {safe_get_conf(spark, 'spark.sql.adaptive.enabled')}")
    print(f"Broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold', 'not set')}")

    # 2. Check data sizes
    print("\n2. DATA SIZES")
    print("-" * 80)
    df_tempo_count = df_tempo.count()
    arm_count = arm_loan_history.count()
    print(f"df_tempo rows: {df_tempo_count:,}")
    print(f"arm_loan_history rows: {arm_count:,}")

    # 3. Check for duplicate join keys (CRITICAL!)
    print("\n3. DUPLICATE JOIN KEY ANALYSIS")
    print("-" * 80)

    # df_tempo duplicates
    df_tempo_dupes = df_tempo.groupBy('Cusip', 'EFFECTIVEDATE').count() \
        .filter(col('count') > 1)
    df_tempo_dupe_count = df_tempo_dupes.count()
    print(f"\ndf_tempo: {df_tempo_dupe_count:,} duplicate key combinations found")

    if df_tempo_dupe_count > 0:
        print("Top 10 duplicate keys in df_tempo:")
        df_tempo_dupes.orderBy(desc('count')).show(10, truncate=False)
        max_dupes = df_tempo_dupes.agg({'count': 'max'}).collect()[0][0]
        print(f"Maximum records per key in df_tempo: {max_dupes:,}")

    # arm_loan_history duplicates
    arm_dupes = arm_loan_history.groupBy('Cusip', 'EFFECTIVEDATE').count() \
        .filter(col('count') > 1)
    arm_dupe_count = arm_dupes.count()
    print(f"\narm_loan_history: {arm_dupe_count:,} duplicate key combinations found")

    if arm_dupe_count > 0:
        print("Top 10 duplicate keys in arm_loan_history:")
        arm_dupes.orderBy(desc('count')).show(10, truncate=False)
        max_dupes = arm_dupes.agg({'count': 'max'}).collect()[0][0]
        print(f"Maximum records per key in arm_loan_history: {max_dupes:,}")

    # 4. Calculate potential output explosion
    if df_tempo_dupe_count > 0 and arm_dupe_count > 0:
        print("\n⚠️  WARNING: CARTESIAN EXPLOSION DETECTED!")
        print("Both dataframes have duplicate keys. Output size will multiply!")
        print("Estimated worst case: left_count × right_count per key combination")

    # 5. Check distinct keys
    print("\n4. DISTINCT KEY ANALYSIS")
    print("-" * 80)
    df_tempo_distinct = df_tempo.select('Cusip', 'EFFECTIVEDATE').distinct().count()
    arm_distinct = arm_loan_history.select('Cusip', 'EFFECTIVEDATE').distinct().count()
    print(f"Distinct Cusip+EFFECTIVEDATE in df_tempo: {df_tempo_distinct:,}")
    print(f"Distinct Cusip+EFFECTIVEDATE in arm_loan_history: {arm_distinct:,}")
    print(f"Ratio (df_tempo rows / distinct keys): {df_tempo_count / df_tempo_distinct:.2f}")
    print(f"Ratio (arm rows / distinct keys): {arm_count / arm_distinct:.2f}")

    # 6. Check partition distribution
    print("\n5. PARTITION DISTRIBUTION")
    print("-" * 80)
    df_tempo_partition_counts = df_tempo.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    print(f"df_tempo partitions: {len(df_tempo_partition_counts)}")
    print(f"  Min rows per partition: {min(df_tempo_partition_counts):,}")
    print(f"  Max rows per partition: {max(df_tempo_partition_counts):,}")
    print(f"  Avg rows per partition: {sum(df_tempo_partition_counts) / len(df_tempo_partition_counts):,.0f}")
    print(f"  Std dev: {calculate_std_dev(df_tempo_partition_counts):,.0f}")

    # Check for severe skew
    avg_partition_size = sum(df_tempo_partition_counts) / len(df_tempo_partition_counts)
    max_partition_size = max(df_tempo_partition_counts)
    skew_factor = max_partition_size / avg_partition_size if avg_partition_size > 0 else 0
    print(f"  Skew factor (max/avg): {skew_factor:.2f}")

    if skew_factor > 5:
        print("  ⚠️  SEVERE PARTITION SKEW DETECTED!")

    # 7. Recommendations
    print("\n6. RECOMMENDATIONS")
    print("-" * 80)

    recommendations = []

    if arm_count < 10_000_000:  # Less than 10M rows
        recommendations.append("✓ Try BROADCAST JOIN (arm_loan_history is small enough)")

    if df_tempo_dupe_count > 0 or arm_dupe_count > 0:
        recommendations.append("✓ DEDUPLICATE dataframes before joining")

    if skew_factor > 5:
        recommendations.append("✓ Use SALTING to handle data skew")

    if df_tempo_distinct < 1000:
        recommendations.append("✓ Consider ITERATIVE PROCESSING (few distinct keys)")

    if not recommendations:
        recommendations.append("✓ Use STANDARD OPTIMIZATION (repartition + AQE)")

    for rec in recommendations:
        print(rec)

    print("\n" + "=" * 80)

    return {
        'df_tempo_count': df_tempo_count,
        'arm_count': arm_count,
        'df_tempo_dupes': df_tempo_dupe_count,
        'arm_dupes': arm_dupe_count,
        'skew_factor': skew_factor,
        'recommendations': recommendations
    }


def calculate_std_dev(values):
    """Calculate standard deviation of a list of values."""
    n = len(values)
    if n == 0:
        return 0
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    return variance ** 0.5


# =============================================================================
# SECTION 2: SOLUTION 1 - BROADCAST JOIN (Simplest)
# =============================================================================

def solution_1_broadcast_join(df_tempo, arm_loan_history, spark):
    """
    Use when: arm_loan_history is small (< 8GB)
    Pros: Fastest, no shuffle, simple
    Cons: Only works if right side fits in memory
    """
    print("\n" + "=" * 80)
    print("SOLUTION 1: BROADCAST JOIN")
    print("=" * 80)

    # Increase broadcast threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "8GB")

    # Perform broadcast join
    df_result = df_tempo.join(
        broadcast(arm_loan_history),
        on=['Cusip', 'EFFECTIVEDATE'],
        how='left'
    ) \
        .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
        .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
        .withColumn('CURRENTNOTERATE',
                    expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
        .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')

    print("✓ Broadcast join completed")
    return df_result


# =============================================================================
# SECTION 3: SOLUTION 2 - DEDUPLICATION (Handle Cartesian Explosion)
# =============================================================================

def solution_2_deduplicate_and_join(df_tempo, arm_loan_history, spark):
    """
    Use when: Duplicate keys are causing Cartesian explosion
    Pros: Reduces output size dramatically
    Cons: Need to decide which duplicate to keep
    """
    print("\n" + "=" * 80)
    print("SOLUTION 2: DEDUPLICATION + JOIN")
    print("=" * 80)

    # Option A: Deduplicate arm_loan_history (if it should be unique per key)
    print("Deduplicating arm_loan_history...")
    window_spec = Window.partitionBy('Cusip', 'EFFECTIVEDATE').orderBy(desc('some_timestamp_or_priority_column'))
    arm_loan_history_deduped = arm_loan_history \
        .withColumn('rn', row_number().over(window_spec)) \
        .filter(col('rn') == 1) \
        .drop('rn')

    print(f"arm_loan_history: {arm_loan_history.count():,} -> {arm_loan_history_deduped.count():,} rows")

    # Option B: Deduplicate df_tempo (if needed)
    # Uncomment if df_tempo also needs deduplication
    # window_spec2 = Window.partitionBy('Cusip', 'EFFECTIVEDATE').orderBy(desc('priority_column'))
    # df_tempo = df_tempo.withColumn('rn', row_number().over(window_spec2)) \
    #     .filter(col('rn') == 1).drop('rn')

    # Perform join
    df_result = df_tempo.join(
        arm_loan_history_deduped,
        on=['Cusip', 'EFFECTIVEDATE'],
        how='left'
    ) \
        .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
        .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
        .withColumn('CURRENTNOTERATE',
                    expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
        .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')

    print("✓ Deduplicated join completed")
    return df_result


# =============================================================================
# SECTION 4: SOLUTION 3 - SALTING (Handle Extreme Skew)
# =============================================================================

def solution_3_salted_join(df_tempo, arm_loan_history, spark, num_salts=100):
    """
    Use when: Severe data skew, resources underutilized
    Pros: Distributes hot keys across multiple partitions
    Cons: More complex, increases data size temporarily
    """
    print("\n" + "=" * 80)
    print(f"SOLUTION 3: SALTED JOIN (num_salts={num_salts})")
    print("=" * 80)

    # Step 1: Add salt to df_tempo
    print("Step 1: Adding salt to df_tempo...")
    df_tempo_salted = df_tempo.withColumn('salt', (rand() * num_salts).cast(IntegerType()))

    # Step 2: Explode arm_loan_history to match all salt values
    print("Step 2: Exploding arm_loan_history with salt values...")
    arm_loan_history_exploded = arm_loan_history \
        .withColumn('salt', explode(array([lit(i) for i in range(num_salts)])))

    print(f"  arm_loan_history: {arm_loan_history.count():,} -> {arm_loan_history_exploded.count():,} rows")

    # Step 3: Repartition on join keys including salt
    print("Step 3: Repartitioning both dataframes...")
    df_tempo_salted = df_tempo_salted.repartition(4000, 'Cusip', 'EFFECTIVEDATE', 'salt')
    arm_loan_history_exploded = arm_loan_history_exploded.repartition(4000, 'Cusip', 'EFFECTIVEDATE', 'salt')

    # Step 4: Perform join
    print("Step 4: Performing salted join...")
    df_result = df_tempo_salted.join(
        arm_loan_history_exploded,
        on=['Cusip', 'EFFECTIVEDATE', 'salt'],
        how='left'
    ) \
        .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
        .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
        .withColumn('CURRENTNOTERATE',
                    expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
        .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new', 'salt')

    print("✓ Salted join completed")
    return df_result


# =============================================================================
# SECTION 5: SOLUTION 4 - ITERATIVE PROCESSING (Ultimate Control)
# =============================================================================

def solution_4_iterative_processing(df_tempo, arm_loan_history, spark, output_path):
    """
    Use when: All else fails, need maximum control
    Pros: Guaranteed to work, processes in manageable chunks
    Cons: Slower, more complex
    """
    print("\n" + "=" * 80)
    print("SOLUTION 4: ITERATIVE PROCESSING")
    print("=" * 80)

    # Get distinct Cusip values
    cusips = [row.Cusip for row in df_tempo.select('Cusip').distinct().collect()]
    total_cusips = len(cusips)
    print(f"Processing {total_cusips} distinct Cusips...")

    # Process in batches
    batch_size = 100
    for i in range(0, total_cusips, batch_size):
        batch_cusips = cusips[i:i + batch_size]
        print(
            f"\nProcessing batch {i // batch_size + 1} ({i + 1}-{min(i + batch_size, total_cusips)} of {total_cusips})")

        # Filter both dataframes
        df_tempo_batch = df_tempo.filter(col('Cusip').isin(batch_cusips))
        arm_batch = arm_loan_history.filter(col('Cusip').isin(batch_cusips))

        # Perform join
        df_result_batch = df_tempo_batch.join(
            arm_batch,
            on=['Cusip', 'EFFECTIVEDATE'],
            how='left'
        ) \
            .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
            .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
            .withColumn('CURRENTNOTERATE',
                        expr(
                            "case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
            .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')

        # Write batch immediately
        df_result_batch.write.mode('append').parquet(output_path)
        print(f"  ✓ Batch {i // batch_size + 1} written to {output_path}")

    print("\n✓ All batches processed")

    # Read back the complete result
    df_result = spark.read.parquet(output_path)
    return df_result


# =============================================================================
# SECTION 6: SOLUTION 5 - PARALLEL CHUNKS (Balance Speed and Control)
# =============================================================================

def solution_5_parallel_chunks(df_tempo, arm_loan_history, spark, num_chunks=20):
    """
    Use when: Need parallelism but without iterative slowness
    Pros: Good balance of speed and control
    Cons: Still requires multiple passes
    """
    print("\n" + "=" * 80)
    print(f"SOLUTION 5: PARALLEL CHUNKS (num_chunks={num_chunks})")
    print("=" * 80)

    # Add chunk ID to df_tempo
    df_tempo_chunked = df_tempo.withColumn(
        'chunk_id',
        (monotonically_increasing_id() % num_chunks).cast(IntegerType())
    )

    results = []

    for i in range(num_chunks):
        print(f"\nProcessing chunk {i + 1}/{num_chunks}...")

        # Filter chunk
        chunk = df_tempo_chunked.filter(col('chunk_id') == i).drop('chunk_id')

        # Perform join
        result = chunk.join(
            arm_loan_history,
            on=['Cusip', 'EFFECTIVEDATE'],
            how='left'
        ) \
            .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
            .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
            .withColumn('CURRENTNOTERATE',
                        expr(
                            "case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
            .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')

        results.append(result)
        print(f"  ✓ Chunk {i + 1} completed")

    # Union all results
    print("\nUnioning all chunks...")
    df_result = reduce(DataFrame.unionAll, results)

    print("✓ Parallel chunks completed")
    return df_result


# =============================================================================
# SECTION 7: SOLUTION 6 - STANDARD OPTIMIZATION (When No Major Issues)
# =============================================================================

def solution_6_standard_optimization(df_tempo, arm_loan_history, spark):
    """
    Use when: No severe skew or duplicates, just needs optimization
    Pros: Simple, leverages Spark's built-in optimizations
    Cons: May not handle extreme cases
    """
    print("\n" + "=" * 80)
    print("SOLUTION 6: STANDARD OPTIMIZATION")
    print("=" * 80)

    # Enable Adaptive Query Execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

    # Repartition both dataframes on join keys
    print("Repartitioning dataframes...")
    df_tempo = df_tempo.repartition(1000, 'Cusip', 'EFFECTIVEDATE')
    arm_loan_history = arm_loan_history.repartition(1000, 'Cusip', 'EFFECTIVEDATE')

    # Cache if reused
    df_tempo.persist()
    arm_loan_history.persist()

    # Perform join
    print("Performing optimized join...")
    df_result = df_tempo.join(
        arm_loan_history,
        on=['Cusip', 'EFFECTIVEDATE'],
        how='left'
    ) \
        .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
        .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
        .withColumn('CURRENTNOTERATE',
                    expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
        .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')

    print("✓ Standard optimization completed")
    return df_result


# =============================================================================
# SECTION 8: OPTIMAL SPARK CONFIGURATION
# =============================================================================

def configure_spark_optimal(spark, executor_memory="16g", executor_cores=4, num_executors=50):
    """
    Apply optimal Spark configuration for large joins.
    Adjust parameters based on your cluster size.
    """
    print("\n" + "=" * 80)
    print("CONFIGURING SPARK FOR OPTIMAL PERFORMANCE")
    print("=" * 80)

    configs = {
        # Core configurations
        "spark.sql.shuffle.partitions": "4000",
        "spark.default.parallelism": "4000",

        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",

        # Memory management
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.executor.memoryOverhead": "2g",

        # Broadcast
        "spark.sql.autoBroadcastJoinThreshold": "100MB",  # Increase if broadcast is viable

        # Dynamic allocation
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": str(num_executors),
        "spark.dynamicAllocation.initialExecutors": str(num_executors // 2),
        "spark.dynamicAllocation.executorIdleTimeout": "60s",

        # Shuffle optimization
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.compress": "true",
        "spark.shuffle.spill.compress": "true",

        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",
    }

    for key, value in configs.items():
        spark.conf.set(key, value)
        print(f"  {key} = {value}")

    print("\n✓ Spark configuration applied")


# =============================================================================
# SECTION 9: DECISION TREE / MAIN EXECUTION
# =============================================================================

def solve_join_problem(df_tempo, arm_loan_history, spark, output_path=None):
    """
    Main function that diagnoses the problem and applies the best solution.
    """
    print("\n" + "#" * 80)
    print("# PYSPARK JOIN PROBLEM SOLVER")
    print("#" * 80)

    # Step 1: Run diagnostics
    diagnostics = diagnose_join_problem(df_tempo, arm_loan_history, spark)

    # Step 2: Configure Spark optimally
    configure_spark_optimal(spark)

    # Step 3: Choose and execute solution based on diagnostics
    print("\n" + "=" * 80)
    print("SELECTING OPTIMAL SOLUTION")
    print("=" * 80)

    # Decision logic
    if diagnostics['arm_count'] < 10_000_000:
        print("→ Selected: BROADCAST JOIN (arm_loan_history is small)")
        df_result = solution_1_broadcast_join(df_tempo, arm_loan_history, spark)

    elif diagnostics['df_tempo_dupes'] > 0 or diagnostics['arm_dupes'] > 0:
        print("→ Selected: DEDUPLICATION + JOIN (duplicate keys detected)")
        df_result = solution_2_deduplicate_and_join(df_tempo, arm_loan_history, spark)

    elif diagnostics['skew_factor'] > 10:
        print("→ Selected: SALTED JOIN (severe skew detected)")
        df_result = solution_3_salted_join(df_tempo, arm_loan_history, spark, num_salts=100)

    else:
        print("→ Selected: STANDARD OPTIMIZATION")
        df_result = solution_6_standard_optimization(df_tempo, arm_loan_history, spark)

    print("\n" + "#" * 80)
    print("# SOLUTION COMPLETED")
    print("#" * 80)

    return df_result


# =============================================================================
# SECTION 10: USAGE EXAMPLES
# =============================================================================

"""
USAGE EXAMPLE 1: Automatic solution selection
-----------------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinOptimization").getOrCreate()

# Load your data
df_tempo = spark.read.parquet("s3://bucket/df_tempo/")
arm_loan_history = spark.read.parquet("s3://bucket/arm_loan_history/")

# Let the solver diagnose and fix
df_result = solve_join_problem(df_tempo, arm_loan_history, spark)

# Write result
df_result.write.mode('overwrite').parquet("s3://bucket/output/")


USAGE EXAMPLE 2: Manual diagnosis then specific solution
---------------------------------------------------------
# Run diagnostics
diagnostics = diagnose_join_problem(df_tempo, arm_loan_history, spark)

# Apply specific solution based on your judgment
configure_spark_optimal(spark)

if diagnostics['skew_factor'] > 10:
    df_result = solution_3_salted_join(df_tempo, arm_loan_history, spark, num_salts=150)
else:
    df_result = solution_1_broadcast_join(df_tempo, arm_loan_history, spark)


USAGE EXAMPLE 3: Nuclear option - when all else fails
------------------------------------------------------
# Use iterative processing
output_path = "s3://bucket/output/"
df_result = solution_4_iterative_processing(
    df_tempo, 
    arm_loan_history, 
    spark, 
    output_path
)


USAGE EXAMPLE 4: Quick fix for moderate issues
-----------------------------------------------
# Just apply standard optimizations
configure_spark_optimal(spark)
df_result = solution_6_standard_optimization(df_tempo, arm_loan_history, spark)
"""


# =============================================================================
# SECTION 11: MONITORING AND DEBUGGING
# =============================================================================

def monitor_join_progress(spark):
    """
    While join is running, use these commands to monitor progress.
    Run in Spark UI or separate notebook cell.
    """
    print("""
    MONITORING CHECKLIST:
    ---------------------

    1. Spark UI (http://master-node:4040 or EMR console):
       - Check 'Stages' tab for task progress
       - Look for tasks that are stuck at 99%
       - Check 'Executors' tab for resource utilization
       - Review 'SQL' tab for query plan

    2. Resource utilization:
       - Are all executors being used?
       - Is memory/CPU maxed out or idle?
       - Check for GC pressure in executor logs

    3. Data skew indicators:
       - Few tasks taking much longer than others
       - Uneven data distribution in shuffle read/write
       - Some tasks processing GB while others process KB

    4. EMR CloudWatch metrics:
       - YARN memory available vs used
       - CPU utilization across nodes
       - Shuffle read/write metrics

    5. If job is hanging:
       - Check driver logs: /var/log/spark/
       - Check executor logs in Spark UI
       - Look for OOM errors or shuffle fetch failures
    """)


# =============================================================================
# SECTION 12: TROUBLESHOOTING CHECKLIST
# =============================================================================

def troubleshooting_checklist():
    """
    Print comprehensive troubleshooting checklist.
    """
    print("""
    ========================================================================
    TROUBLESHOOTING CHECKLIST
    ========================================================================

    BEFORE STARTING:
    □ Have you run the diagnostics? (diagnose_join_problem)
    □ Is your Spark configuration optimal? (configure_spark_optimal)
    □ Have you checked for duplicate join keys?
    □ Have you verified data skew factor?

    IF USING BROADCAST JOIN:
    □ Is arm_loan_history < 8GB?
    □ Have you increased broadcast threshold?
    □ Are executors running out of memory?

    IF USING SALTING:
    □ Have you chosen appropriate num_salts? (50-200 typical)
    □ Have you repartitioned after salting?
    □ Are you seeing better task distribution in Spark UI?

    IF USING DEDUPLICATION:
    □ Have you verified which duplicate to keep?
    □ Are you using appropriate window ordering?
    □ Did deduplication reduce data size significantly?

    IF STILL HANGING:
    □ Check Spark UI for stuck tasks
    □ Review executor logs for errors
    □ Increase executor memory (add overhead)
    □ Try iterative processing as last resort

    COMMON GOTCHAS:
    □ Typo in join key column names (case sensitive!)
    □ Null values in join keys causing issues
    □ Wrong join type (inner vs left)
    □ Data types not matching between dataframes
    □ Network issues with S3 reads/writes

    ========================================================================
    """)


# =============================================================================
# SECTION 13: QUICK REFERENCE
# =============================================================================

"""
QUICK REFERENCE GUIDE
=====================

PROBLEM: Join hangs, resources underutilized

SOLUTION SELECTION:
1. arm_loan_history < 8GB         → Use BROADCAST JOIN
2. Duplicate keys present          → Use DEDUPLICATION
3. Skew factor > 10                → Use SALTING
4. Few distinct keys               → Use ITERATIVE PROCESSING
5. Moderate complexity             → Use STANDARD OPTIMIZATION
6. All else fails                  → Use PARALLEL CHUNKS or ITERATIVE

KEY COMMANDS:
- Diagnose: diagnose_join_problem(df_tempo, arm_loan_history, spark)
- Auto-solve: solve_join_problem(df_tempo, arm_loan_history, spark)
- Manual: Call specific solution_N_* function

SPARK CONFIG ESSENTIALS:
- spark.sql.shuffle.partitions = 4000
- spark.sql.adaptive.enabled = true
- spark.sql.adaptive.skewJoin.enabled = true
- Increase broadcast threshold if using broadcast

MONITORING:
- Spark UI: Check task distribution and resource usage
- Look for: Tasks stuck, uneven processing, memory issues
- Log location: /var/log/spark/

COMMON FIXES:
- Data skew → Salting or iterative processing
- Cartesian explosion → Deduplication
- Small right table → Broadcast join
- All else → Parallel chunks with write-per-chunk
"""

if __name__ == "__main__":
    print(__doc__)
    print("\nThis is a reference guide. Import functions as needed:")
    print("  from pyspark_join_troubleshooting_guide import *")
    troubleshooting_checklist()
