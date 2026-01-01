# PySpark Join Optimization for Severe Data Skew

Your issue is **severe partition skew** (skew factor: 3557x!) - one partition has 570M rows while most have ~160K.
Here's how to fix it:

## 🚨 Immediate Solutions

### Option 1: Salting Strategy 
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Identify skewed keys in table1
skewed_keys_df = (
    table1.groupBy("cusip", "EFFECTIVEDATE")
    .count()
    .filter(F.col("count") > 100000)  # Threshold for "skewed"
    .select("cusip", "EFFECTIVEDATE")
    .withColumn("is_skewed", F.lit(True))
)

# Step 2: Mark skewed keys
table1_marked = table1.join(
    F.broadcast(skewed_keys_df),
    on=["cusip", "EFFECTIVEDATE"],
    how="left"
).fillna({"is_skewed": False})

table2_marked = table2.join(
    F.broadcast(skewed_keys_df),
    on=["cusip", "EFFECTIVEDATE"],
    how="left"
).fillna({"is_skewed": False})

# Step 3: Add salt to skewed keys
SALT_FACTOR = 100  # Increase if needed

table1_salted = table1_marked.withColumn(
    "salt",
    F.when(F.col("is_skewed"), (F.rand() * SALT_FACTOR).cast("int"))
    .otherwise(F.lit(0))
)

table2_salted = table2_marked.withColumn(
    "salt",
    F.when(F.col("is_skewed"), F.explode(F.array([F.lit(i) for i in range(SALT_FACTOR)])))
    .otherwise(F.lit(0))
)

# Step 4: Join on salted keys
result = table1_salted.join(
    table2_salted,
    on=["cusip", "EFFECTIVEDATE", "salt"],
    how="inner"  # or your join type
).drop("salt", "is_skewed")

result.write.mode("overwrite").parquet("s3://your-bucket/output/")
```

### Option 2: Adaptive Repartitioning

```python
# Before join - fix the skew in table1
table1_repartitioned = (
    table1
    .repartition(8000, "cusip", "EFFECTIVEDATE")  # 2x current partitions
    .cache()  # Cache if you'll reuse
)

table2_repartitioned = (
    table2
    .repartition(2000, "cusip", "EFFECTIVEDATE")
    .cache()
)

# Perform join
result = table1_repartitioned.join(
    table2_repartitioned,
    on=["cusip", "EFFECTIVEDATE"],
    how="inner"
)
```

### Option 3: Broadcast Join (if table2 fits in memory)

```python
# Check table2 size first
table2_size_bytes = table2.rdd.map(lambda x: len(str(x))).sum()
print(f"Table2 estimated size: {table2_size_bytes / 1024 ** 3:.2f} GB")

# If < 8GB, broadcast it
if table2_size_bytes < 8 * 1024 ** 3:
    result = table1.join(
        F.broadcast(table2),
        on=["cusip", "EFFECTIVEDATE"],
        how="inner"
    )
```

## ⚙️ Update Spark Configuration

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# Increase broadcast threshold if table2 is small enough
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")  # Default is 10MB

# Increase parallelism for better distribution
spark.conf.set("spark.sql.shuffle.partitions", "8000")  # 2x current
spark.conf.set("spark.default.parallelism", "8000")

# Enable dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "200")
spark.conf.set("spark.dynamicAllocation.minExecutors", "50")
```

## 🔍 Diagnostic Code

```python
# Identify the skewed keys
skew_analysis = (
    table1.groupBy("cusip", "EFFECTIVEDATE")
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
)
skew_analysis.show(truncate=False)


# Check partition distribution after repartition
def check_partition_distribution(df, name):
    partition_counts = df.rdd.glom().map(len).collect()
    print(f"\n{name} Partition Distribution:")
    print(f"  Min: {min(partition_counts):,}")
    print(f"  Max: {max(partition_counts):,}")
    print(f"  Avg: {sum(partition_counts) / len(partition_counts):,.0f}")
    print(f"  Skew: {max(partition_counts) / (sum(partition_counts) / len(partition_counts)):.2f}x")


check_partition_distribution(table1_repartitioned, "Table1")
```

## 🎯 Complete Optimized Solution

```python
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Initialize with optimized config
spark = SparkSession.builder
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
.config("spark.sql.shuffle.partitions", "8000")
.config("spark.sql.autoBroadcastJoinThreshold", "100MB")
.getOrCreate()

# STEP 1: Identify severely skewed keys
print("Identifying skewed keys...")
skewed_threshold = 1000000  # Keys with >1M rows

skewed_keys = (
    table1.groupBy("cusip", "EFFECTIVEDATE")
    .count()
    .filter(F.col("count") > skewed_threshold)
    .select("cusip", "EFFECTIVEDATE")
    .withColumn("is_skewed", F.lit(True))
    .cache()
)

num_skewed = skewed_keys.count()
print(f"Found {num_skewed} skewed key combinations")

# STEP 2: Split data into skewed and non-skewed
table1_skewed = table1.join(F.broadcast(skewed_keys), on=["cusip", "EFFECTIVEDATE"], how="inner")
table1_normal = table1.join(F.broadcast(skewed_keys), on=["cusip", "EFFECTIVEDATE"], how="left_anti")

table2_skewed = table2.join(F.broadcast(skewed_keys), on=["cusip", "EFFECTIVEDATE"], how="inner")
table2_normal = table2.join(F.broadcast(skewed_keys), on=["cusip", "EFFECTIVEDATE"], how="left_anti")

# STEP 3: Handle normal data (fast path)
print("Processing non-skewed data...")
result_normal = table1_normal.join(
    table2_normal,
    on=["cusip", "EFFECTIVEDATE"],
    how="inner"
)

# STEP 4: Handle skewed data with salting
print("Processing skewed data with salting...")
SALT_FACTOR = 200  # Adjust based on skew severity

table1_skewed_salted = table1_skewed.withColumn(
    "salt_key",
    (F.rand() * SALT_FACTOR).cast("int")
)

table2_skewed_exploded = table2_skewed.withColumn(
    "salt_key",
    F.explode(F.array([F.lit(i) for i in range(SALT_FACTOR)]))
)

result_skewed = table1_skewed_salted.join(
    table2_skewed_exploded,
    on=["cusip", "EFFECTIVEDATE", "salt_key"],
    how="inner"
).drop("salt_key", "is_skewed")

# STEP 5: Union results
print("Combining results...")
final_result = result_normal.union(result_skewed)

# STEP 6: Write output
final_result.write
.mode("overwrite")
.option("compression", "snappy")
.parquet("s3://your-bucket/output/")

print("Join completed!")
```

## 📊 Expected Improvements

- **Runtime**: 40+ min → **5-10 min** (estimated)
- **Resource utilization**: 3-5% → **85-95%**
- **Skew factor**: 3557x → **<5x**

## 🚀 Quick Wins to Try First

1. **Increase shuffle partitions**: `8000` instead of `4000`
2. **Enable AQE skew handling** (add configs above)
3. **Try broadcast join** if table2 < 10GB
4. **Use salting** for top 10-20 skewed keys

Which approach would you like to implement first?