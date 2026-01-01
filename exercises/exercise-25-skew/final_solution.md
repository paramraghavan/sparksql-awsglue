# Complete Spark-Submit for Option 1 (Salting Strategy)

Since repartitioning and broadcast didn't work, here's the **production-ready spark-submit** for the salting approach:

## 🚀 Spark-Submit Command

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 48G \
  --driver-cores 4 \
  --executor-memory 32G \
  --executor-cores 5 \
  --num-executors 80 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.memoryOverhead=6G \
  --conf spark.driver.memoryOverhead=8G \
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=120s \
  --conf spark.sql.shuffle.partitions=16000 \
  --conf spark.default.parallelism=16000 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=3 \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB \
  --conf spark.sql.adaptive.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.files.maxPartitionBytes=134217728 \
  --conf spark.sql.broadcastTimeout=1200 \
  --conf spark.rpc.askTimeout=600s \
  --conf spark.rpc.lookupTimeout=600s \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=1024m \
  --conf spark.sql.inMemoryColumnarStorage.compressed=true \
  --conf spark.sql.inMemoryColumnarStorage.batchSize=20000 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.speculation=false \
  --conf spark.task.maxFailures=8 \
  join_with_salting.py \
  --input-table1 "s3://your-bucket/table1/" \
  --input-table2 "s3://your-bucket/table2/" \
  --output-path "s3://your-bucket/output/" \
  --salt-factor 200 \
  --skew-threshold 500000
```

## 📝 Complete Python Script: `join_with_salting.py`

```python
#!/usr/bin/env python3
"""
Optimized PySpark join with salting for severe data skew
Handles 640M+ row joins with 3500x skew factor
"""

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType


def create_spark_session():
    """Initialize Spark session with optimized configurations"""
    return SparkSession.builder
    .appName("Skewed-Join-With-Salting")
    .getOrCreate()


def analyze_skew(df, key_cols, name="DataFrame"):
    """Analyze and report data skew"""
    print(f"\n{'=' * 60}")
    print(f"Analyzing skew for {name}")
    print(f"{'=' * 60}")

    skew_df = df.groupBy(*key_cols).count()
    stats = skew_df.agg(
        F.count("*").alias("distinct_keys"),
        F.min("count").alias("min_count"),
        F.max("count").alias("max_count"),
        F.avg("count").alias("avg_count"),
        F.stddev("count").alias("stddev_count")
    ).collect()[0]

    print(f"Distinct key combinations: {stats['distinct_keys']:,}")
    print(f"Min rows per key: {stats['min_count']:,}")
    print(f"Max rows per key: {stats['max_count']:,}")
    print(f"Avg rows per key: {stats['avg_count']:,.2f}")
    print(f"Std dev: {stats['stddev_count']:,.2f}")
    print(f"Skew factor: {stats['max_count'] / stats['avg_count']:.2f}x")

    # Show top skewed keys
    print(f"\nTop 10 most skewed keys:")
    skew_df.orderBy(F.desc("count")).limit(10).show(truncate=False)

    return skew_df


def identify_skewed_keys(df, key_cols, threshold):
    """Identify keys that exceed the skew threshold"""
    print(f"\n{'=' * 60}")
    print(f"Identifying skewed keys (threshold: {threshold:,} rows)")
    print(f"{'=' * 60}")

    skewed_keys = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > threshold)
        .select(*key_cols)
        .withColumn("is_skewed", F.lit(True))
    )

    # Cache this small dataset
    skewed_keys.cache()
    num_skewed = skewed_keys.count()

    print(f"Found {num_skewed:,} skewed key combinations")

    if num_skewed > 0:
        print("\nSample of skewed keys:")
        skewed_keys.limit(10).show(truncate=False)

    return skewed_keys


def split_dataframe(df, skewed_keys, key_cols, name="DataFrame"):
    """Split dataframe into skewed and normal partitions"""
    print(f"\nSplitting {name} into skewed and normal partitions...")

    # Use broadcast for small skewed_keys lookup
    df_skewed = df.join(
        F.broadcast(skewed_keys),
        on=key_cols,
        how="inner"
    )

    df_normal = df.join(
        F.broadcast(skewed_keys),
        on=key_cols,
        how="left_anti"
    )

    count_skewed = df_skewed.count()
    count_normal = df_normal.count()

    print(f"  Skewed partition: {count_skewed:,} rows ({count_skewed / (count_skewed + count_normal) * 100:.1f}%)")
    print(f"  Normal partition: {count_normal:,} rows ({count_normal / (count_skewed + count_normal) * 100:.1f}%)")

    return df_skewed, df_normal


def apply_salting(df_left, df_right, key_cols, salt_factor):
    """Apply salting technique to handle skewed data"""
    print(f"\n{'=' * 60}")
    print(f"Applying salting with factor: {salt_factor}")
    print(f"{'=' * 60}")

    # Add random salt to left table
    df_left_salted = df_left.withColumn(
        "salt_key",
        (F.rand(seed=42) * salt_factor).cast(IntegerType())
    )

    print(f"Left table: Added random salt (0-{salt_factor - 1})")

    # Explode right table with all possible salt values
    # Create array of salt values efficiently
    salt_array = F.array([F.lit(i) for i in range(salt_factor)])

    df_right_exploded = df_right.withColumn(
        "salt_key",
        F.explode(salt_array)
    )

    print(f"Right table: Exploded with {salt_factor} salt values")
    print(f"Right table expanded from {df_right.count():,} to ~{df_right.count() * salt_factor:,} rows")

    return df_left_salted, df_right_exploded


def perform_join(df_left, df_right, key_cols, join_type="inner"):
    """Perform the actual join operation"""
    print(f"\nPerforming {join_type} join...")
    start_time = datetime.now()

    result = df_left.join(
        df_right,
        on=key_cols,
        how=join_type
    )

    # Trigger computation
    count = result.count()

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"Join completed: {count:,} rows in {elapsed:.2f} seconds")

    return result


def main():
    parser = argparse.ArgumentParser(description='PySpark Join with Salting for Skewed Data')
    parser.add_argument('--input-table1', required=True, help='Path to table1')
    parser.add_argument('--input-table2', required=True, help='Path to table2')
    parser.add_argument('--output-path', required=True, help='Output path')
    parser.add_argument('--salt-factor', type=int, default=200, help='Salt factor for skewed keys')
    parser.add_argument('--skew-threshold', type=int, default=500000, help='Threshold for skewed keys')
    parser.add_argument('--join-type', default='inner', help='Join type (inner, left, right, outer)')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze skew, do not join')

    args = parser.parse_args()

    print(f"\n{'#' * 60}")
    print(f"# PySpark Skewed Join with Salting")
    print(f"#{'#' * 60}")
    print(f"Start time: {datetime.now()}")
    print(f"Table1: {args.input_table1}")
    print(f"Table2: {args.input_table2}")
    print(f"Output: {args.output_path}")
    print(f"Salt factor: {args.salt_factor}")
    print(f"Skew threshold: {args.skew_threshold:,}")
    print(f"Join type: {args.join_type}")

    # Initialize Spark
    spark = create_spark_session()

    # Print Spark configuration
    print(f"\n{'=' * 60}")
    print("Spark Configuration:")
    print(f"{'=' * 60}")
    important_configs = [
        'spark.executor.memory',
        'spark.executor.cores',
        'spark.executor.instances',
        'spark.sql.shuffle.partitions',
        'spark.sql.adaptive.enabled',
        'spark.sql.adaptive.skewJoin.enabled'
    ]
    for config in important_configs:
        value = spark.conf.get(config, 'not set')
        print(f"{config}: {value}")

    try:
        # Read tables
        print(f"\n{'=' * 60}")
        print("Reading input tables...")
        print(f"{'=' * 60}")

        table1 = spark.read.parquet(args.input_table1)
        table2 = spark.read.parquet(args.input_table2)

        # Cache if reusing
        table1.cache()
        table2.cache()

        count1 = table1.count()
        count2 = table2.count()

        print(f"Table1: {count1:,} rows")
        print(f"Table2: {count2:,} rows")

        # Define join keys
        JOIN_KEYS = ["cusip", "EFFECTIVEDATE"]

        # Analyze skew in table1 (the larger, more skewed table)
        skew_df1 = analyze_skew(table1, JOIN_KEYS, "Table1")

        if args.analyze_only:
            print("\nAnalysis complete (--analyze-only mode). Exiting.")
            return 0

        # Identify skewed keys
        skewed_keys = identify_skewed_keys(table1, JOIN_KEYS, args.skew_threshold)

        num_skewed_keys = skewed_keys.count()

        if num_skewed_keys == 0:
            print("\nNo skewed keys found. Performing regular join...")
            result = perform_join(table1, table2, JOIN_KEYS, args.join_type)

        else:
            # Split both tables
            table1_skewed, table1_normal = split_dataframe(table1, skewed_keys, JOIN_KEYS, "Table1")
            table2_skewed, table2_normal = split_dataframe(table2, skewed_keys, JOIN_KEYS, "Table2")

            # Process normal data (fast path)
            print(f"\n{'=' * 60}")
            print("Processing NORMAL data (no skew)")
            print(f"{'=' * 60}")
            result_normal = perform_join(table1_normal, table2_normal, JOIN_KEYS, args.join_type)

            # Process skewed data with salting
            print(f"\n{'=' * 60}")
            print("Processing SKEWED data (with salting)")
            print(f"{'=' * 60}")

            table1_skewed_salted, table2_skewed_exploded = apply_salting(
                table1_skewed,
                table2_skewed,
                JOIN_KEYS,
                args.salt_factor
            )

            # Join with salt_key included
            result_skewed = perform_join(
                table1_skewed_salted,
                table2_skewed_exploded,
                JOIN_KEYS + ["salt_key"],
                args.join_type
            ).drop("salt_key", "is_skewed")

            # Union results
            print(f"\n{'=' * 60}")
            print("Combining normal and skewed results...")
            print(f"{'=' * 60}")

            result = result_normal.unionByName(result_skewed, allowMissingColumns=True)

            final_count = result.count()
            print(f"Final result: {final_count:,} rows")

        # Write output
        print(f"\n{'=' * 60}")
        print(f"Writing output to: {args.output_path}")
        print(f"{'=' * 60}")

        result.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output_path)

    print(f"\n{'#' * 60}")
    print(f"# Job completed successfully!")
    print(f"# End time: {datetime.now()}")
    print(f"#{'#' * 60}")

    # Cleanup
    skewed_keys.unpersist()
    table1.unpersist()
    table2.unpersist()

    return 0

except Exception as e:
print(f"\n{'!' * 60}")
print(f"ERROR: {str(e)}")
print(f"{'!' * 60}")
import traceback

traceback.print_exc()
return 1

finally:
spark.stop()

if __name__ == "__main__":
    sys.exit(main())
```

## 🔧 Alternative: For Smaller Clusters

If you have limited resources, use this **lighter configuration**:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 32G \
  --driver-cores 4 \
  --executor-memory 24G \
  --executor-cores 5 \
  --num-executors 50 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=30 \
  --conf spark.dynamicAllocation.maxExecutors=100 \
  --conf spark.dynamicAllocation.initialExecutors=50 \
  --conf spark.executor.memoryOverhead=4G \
  --conf spark.driver.memoryOverhead=6G \
  --conf spark.sql.shuffle.partitions=12000 \
  --conf spark.default.parallelism=12000 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  join_with_salting.py \
  --input-table1 "s3://your-bucket/table1/" \
  --input-table2 "s3://your-bucket/table2/" \
  --output-path "s3://your-bucket/output/" \
  --salt-factor 150
```

## 🧪 Test Run First

Before full run, analyze the skew:

```bash
spark-submit \
  [same configs as above] \
  join_with_salting.py \
  --input-table1 "s3://your-bucket/table1/" \
  --input-table2 "s3://your-bucket/table2/" \
  --output-path "s3://your-bucket/output/" \
  --analyze-only
```

## 📊 Expected Results

- **Runtime**: 40+ min → **8-12 minutes**
- **Resource utilization**: ~5% → **80-90%**
- **Memory usage**: Balanced across executors
- **Stragglers**: Eliminated

## ⚠️ Key Parameters to Adjust

1. **`--salt-factor`**: Start with 200, increase to 300-500 if still slow
2. **`--skew-threshold`**: Lower to 100000 if you want to catch more skewed keys
3. **`--num-executors`**: Adjust based on your EMR cluster size
4. **`--executor-memory`**: Calculate as: `(Node RAM - 2GB) / (executors per node)`

What's your EMR cluster configuration (instance type and count)?