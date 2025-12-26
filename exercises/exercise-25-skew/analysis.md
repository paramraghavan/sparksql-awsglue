# PySpark Join Optimization for Severe Data Skew

You have **SEVERE partition skew** (3557x!) - one partition has 570M rows while average is only 160K. This is killing
your performance. Here's how to fix it:

## 🔴 Critical Issues Identified

1. **Massive skew**: Max partition = 570M rows, Avg = 160K rows
2. **Single hot key**: Likely NULL values or a single CUSIP dominating
3. **Underutilized cluster**: One executor doing all the work

## ✅ Solution Code

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, col, lit, expr, rand

# ============================================
# STEP 1: IDENTIFY AND HANDLE NULL KEYS
# ============================================
# Check for NULL keys first - they often cause skew
null_check_t1 = table1.filter(col("CUSIP").isNull() | col("EFFECTIVEDATE").isNull()).count()
null_check_t2 = table2.filter(col("CUSIP").isNull() | col("EFFECTIVEDATE").isNull()).count()

print(f"Table1 nulls: {null_check_t1}, Table2 nulls: {null_check_t2}")

# Separate NULL and non-NULL rows
table1_valid = table1.filter(col("CUSIP").isNotNull() & col("EFFECTIVEDATE").isNotNull())
table1_null = table1.filter(col("CUSIP").isNull() | col("EFFECTIVEDATE").isNull())

table2_valid = table2.filter(col("CUSIP").isNotNull() & col("EFFECTIVEDATE").isNotNull())
table2_null = table2.filter(col("CUSIP").isNull() | col("EFFECTIVEDATE").isNull())

# ============================================
# STEP 2: IDENTIFY SKEWED KEYS
# ============================================
# Find which keys are causing the skew
from pyspark.sql.window import Window

key_counts = table1_valid.groupBy("CUSIP", "EFFECTIVEDATE").count()
threshold = 100000  # Adjust based on your data

skewed_keys = key_counts.filter(col("count") > threshold)
skewed_count = skewed_keys.count()
print(f"Found {skewed_count} skewed keys")

# ============================================
# STEP 3: SPLIT DATA INTO SKEWED AND NON-SKEWED
# ============================================
# Mark skewed keys
skewed_keys_broadcast = broadcast(
    skewed_keys.select("CUSIP", "EFFECTIVEDATE").withColumn("is_skewed", lit(True))
)

table1_marked = table1_valid.join(
    skewed_keys_broadcast,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="left"
).fillna(False, subset=["is_skewed"])

table2_marked = table2_valid.join(
    skewed_keys_broadcast,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="left"
).fillna(False, subset=["is_skewed"])

# Split into skewed and non-skewed
table1_skewed = table1_marked.filter(col("is_skewed") == True).drop("is_skewed")
table1_normal = table1_marked.filter(col("is_skewed") == False).drop("is_skewed")

table2_skewed = table2_marked.filter(col("is_skewed") == True).drop("is_skewed")
table2_normal = table2_marked.filter(col("is_skewed") == False).drop("is_skewed")

# ============================================
# STEP 4: HANDLE NON-SKEWED DATA (Normal Join)
# ============================================
# Try broadcast join if table2_normal is small enough
table2_normal_count = table2_normal.count()

if table2_normal_count < 50000000:  # ~50M rows, adjust based on memory
    print("Using broadcast join for normal data")
    joined_normal = table1_normal.join(
        broadcast(table2_normal),
        on=["CUSIP", "EFFECTIVEDATE"],
        how="inner"  # or "left", "outer" as needed
    )
else:
    # Regular sort-merge join with repartitioning
    table1_normal = table1_normal.repartition(8000, "CUSIP", "EFFECTIVEDATE")
    table2_normal = table2_normal.repartition(8000, "CUSIP", "EFFECTIVEDATE")

    joined_normal = table1_normal.join(
        table2_normal,
        on=["CUSIP", "EFFECTIVEDATE"],
        how="inner"
    )

# ============================================
# STEP 5: HANDLE SKEWED DATA (Salting Technique)
# ============================================
# Apply salting to distribute skewed keys
NUM_SALTS = 100  # Increase if needed

# Add salt to both tables
table1_salted = table1_skewed.withColumn(
    "salt",
    (rand() * NUM_SALTS).cast("int")
)

# Explode table2 to create copies for each salt value
table2_salted = table2_skewed.withColumn(
    "salt",
    expr(f"explode(sequence(0, {NUM_SALTS - 1}))")
)

# Join on keys + salt
joined_skewed = table1_salted.join(
    table2_salted,
    on=["CUSIP", "EFFECTIVEDATE", "salt"],
    how="inner"
).drop("salt")

# ============================================
# STEP 6: HANDLE NULL KEYS SEPARATELY
# ============================================
# Cross join NULL keys (usually small)
if null_check_t1 > 0 or null_check_t2 > 0:
    joined_null = table1_null.crossJoin(table2_null)
else:
    joined_null = None

# ============================================
# STEP 7: UNION ALL RESULTS
# ============================================
result = joined_normal.unionByName(joined_skewed)
if joined_null is not None:
    result = result.unionByName(joined_null)

# ============================================
# STEP 8: OPTIMIZE OUTPUT
# ============================================
# Repartition before writing
result = result.repartition(2000)  # Adjust based on output size

# Write with optimal settings
result.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet("s3://your-bucket/output/")
```

## 📊 Enhanced Spark Configuration

```python
# Update your SparkConf
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# Increase shuffle partitions for large data
spark.conf.set("spark.sql.shuffle.partitions", "8000")  # Increase from 4000

# Optimize broadcasts
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # Increase if you have memory

# Enable auto broadcast for skew
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")

# Optimize shuffle
spark.conf.set("spark.sql.shuffle.service.enabled", "true")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")

# Memory settings
spark.conf.set("spark.executor.memory", "32g")  # Adjust based on instance
spark.conf.set("spark.executor.memoryOverhead", "8g")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")

# Dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "10")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "20")
```

## 🎯 Alternative: Simple AQE Skew Join (Try First)

```python
# Sometimes AQE alone can handle it with right settings
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")  # Lower threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")

# Add skew hint
from pyspark.sql.functions import skewHint

result = table1.hint("skew", ["CUSIP", "EFFECTIVEDATE"]).join(
    table2,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="inner"
)
```

## 🔍 Diagnostic Queries

```python
# Find the actual skewed keys
top_keys = table1.groupBy("CUSIP", "EFFECTIVEDATE").count()
    .orderBy(col("count").desc())
    .limit(20)
top_keys.show()


# Check partition distribution after repartition
def check_partition_distribution(df, partition_col):
    return df.rdd.glom().map(len).collect()


dist = check_partition_distribution(table1, "CUSIP")
print(f"Min: {min(dist)}, Max: {max(dist)}, Avg: {sum(dist) / len(dist)}")
```

## 📈 Expected Improvements

- **Before**: One partition with 570M rows, single executor bottleneck
- **After**:
    - Skewed data split across 100+ partitions via salting
    - Normal data efficiently joined (possibly broadcast)
    - All executors utilized
    - **Expected speedup: 10-50x** depending on skew concentration

## ⚡ Quick Wins to Try First

1. **Increase shuffle partitions**: `8000` → `16000`
2. **Enable aggressive skew join**: Lower thresholds
3. **Filter nulls explicitly** before join
4. **Check if broadcast join works** for table2

Monitor with Spark UI → Stages → Task metrics to see partition distribution improve!