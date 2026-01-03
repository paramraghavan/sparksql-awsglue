# Solving Your Severe Data Skew Problem

## Why Repartitioning Doesn't Work

**The core issue**: You have a **hot key problem**, not a partition distribution problem.

```
Standard repartitioning:
hash("ABC123", "2023-01-01") → Partition X
hash("ABC123", "2023-01-02") → Partition X  
hash("ABC123", "2023-01-03") → Partition X
... (570M more rows) → ALL go to Partition X
```

When you repartition by `(cusip, effectivedate)`, **all rows with the same key still go to the same partition**. Since
one CUSIP has 570M records, they all hash to one partition regardless of whether you use 4K or 100K partitions.

**Why Salting Works**: It artificially breaks the key into multiple sub-keys, distributing skewed data across
partitions.

---

## Solution 1: Salted Join (Recommended)

```python
from pyspark.sql import functions as F
from pyspark.sql import Window

# Configuration
NUM_SALTS = 200  # Adjust based on skew severity (570M rows → use 100-500)

# Add salt to both tables
table1_salted = table1.withColumn("salt", (F.rand() * NUM_SALTS).cast("int"))

table2_salted = (
    table2
    .withColumn("salt_array", F.array([F.lit(i) for i in range(NUM_SALTS)]))
    .withColumn("salt", F.explode("salt_array"))
    .drop("salt_array")
)

# Perform join on salted keys
result = table1_salted.join(
    table2_salted,
    on=["cusip", "effectivedate", "salt"],
    how="inner"  # or your join type
).drop("salt")

# Optimize before join
result = result.repartition(8000, "cusip", "effectivedate")
```

**Why this works**:

- table1's 570M rows for ABC123 are split into 200 groups (2.85M per salt)
- table2 records are replicated 200 times (72M → 14.4B, but filtered during join)
- Each partition processes ~2.85M rows instead of 570M

---

## Solution 2: Isolated Skew Handling (Most Efficient)

```python
from pyspark.sql import functions as F

# Step 1: Identify skewed keys
skewed_keys_df = (
    table1.groupBy("cusip", "effectivedate")
    .count()
    .filter(F.col("count") > 1000000)  # Threshold: 1M rows
)

skewed_keys = skewed_keys_df.select("cusip", "effectivedate")

# Step 2: Split data into skewed and non-skewed
table1_skewed = table1.join(skewed_keys, on=["cusip", "effectivedate"], how="inner")
table1_normal = table1.join(skewed_keys, on=["cusip", "effectivedate"], how="left_anti")

table2_skewed = table2.join(skewed_keys, on=["cusip", "effectivedate"], how="inner")
table2_normal = table2.join(skewed_keys, on=["cusip", "effectivedate"], how="left_anti")

# Step 3: Handle normal data with standard join
result_normal = table1_normal.join(
    table2_normal,
    on=["cusip", "effectivedate"],
    how="inner"
)

# Step 4: Handle skewed data with salting
NUM_SALTS = 200
table1_skewed_salted = table1_skewed.withColumn("salt", (F.rand() * NUM_SALTS).cast("int"))
table2_skewed_salted = (
    table2_skewed
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(NUM_SALTS)])))
)

result_skewed = table1_skewed_salted.join(
    table2_skewed_salted,
    on=["cusip", "effectivedate", "salt"],
    how="inner"
).drop("salt")

# Step 5: Union results
final_result = result_normal.union(result_skewed)
```

---

## Solution 3: Broadcast Join (If Table2 Fits in Memory)

```python
from pyspark.sql import functions as F

# Check if table2 can be broadcast (< 8GB recommended)
# 72M rows × ~100 bytes/row = ~7GB → Feasible

# Increase broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10737418240")  # 10GB

# Force broadcast join
from pyspark.sql.functions import broadcast

result = table1.join(
    broadcast(table2),
    on=["cusip", "effectivedate"],
    how="inner"
)
```

---

## Solution 4: Pre-Processing Optimization

```python
# Add these BEFORE your join code:

# 1. Cache smaller table
table2.cache()
table2.count()  # Trigger cache

# 2. Coalesce partitions for table2 (reduce small file overhead)
table2 = table2.repartition(1000, "cusip", "effectivedate")

# 3. Optimize table1 partitioning AFTER identifying skew
# Use range partitioning instead of hash
from pyspark.sql import Window

table1 = table1.repartitionByRange(8000, "cusip", "effectivedate")

# 4. Enable additional AQE features
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

---

## Complete Recommended Solution

```python
from pyspark.sql import functions as F, Window

# ===== CONFIGURATION =====
NUM_SALTS = 200
SKEW_THRESHOLD = 1000000  # 1M rows

# ===== STEP 1: Optimize Spark Config =====
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8000")

# ===== STEP 2: Identify Skewed Keys =====
print("Identifying skewed keys...")
skewed_keys = (
    table1.groupBy("cusip", "effectivedate")
    .count()
    .filter(F.col("count") > SKEW_THRESHOLD)
    .select("cusip", "effectivedate")
    .cache()
)

skew_count = skewed_keys.count()
print(f"Found {skew_count} skewed key combinations")

# ===== STEP 3: Split Datasets =====
if skew_count > 0:
    print("Splitting into skewed and normal datasets...")

    # Skewed portion
    table1_skewed = table1.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="inner")
    table2_skewed = table2.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="inner")

    # Normal portion
    table1_normal = table1.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="left_anti")
    table2_normal = table2.join(F.broadcast(skewed_keys), on=["cusip", "effectivedate"], how="left_anti")

    # ===== STEP 4: Join Normal Data =====
    print("Joining normal data...")
    result_normal = table1_normal.join(
        table2_normal,
        on=["cusip", "effectivedate"],
        how="inner"
    )

    # ===== STEP 5: Join Skewed Data with Salting =====
    print(f"Joining skewed data with {NUM_SALTS} salts...")

    table1_skewed_salted = table1_skewed.withColumn(
        "salt",
        (F.rand(42) * NUM_SALTS).cast("int")
    )

    table2_skewed_salted = table2_skewed.withColumn(
        "salt",
        F.explode(F.array([F.lit(i) for i in range(NUM_SALTS)]))
    )

    result_skewed = table1_skewed_salted.join(
        table2_skewed_salted,
        on=["cusip", "effectivedate", "salt"],
        how="inner"
    ).drop("salt")

    # ===== STEP 6: Combine Results =====
    print("Combining results...")
    final_result = result_normal.union(result_skewed)

else:
    # No skew detected, standard join
    final_result = table1.join(
        table2,
        on=["cusip", "effectivedate"],
        how="inner"
    )

# ===== STEP 7: Optimize Output =====
final_result = final_result.repartition(4000)
final_result.write.mode("overwrite").parquet("s3://your-bucket/output/")
```

---

## Additional EMR Optimizations

```python
# Add to your Spark session configuration:

spark_config = {
    # Memory optimization
    "spark.executor.memoryOverhead": "4g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",

    # Shuffle optimization
    "spark.shuffle.service.enabled": "true",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "200",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",

    # S3 optimization
    "spark.hadoop.fs.s3a.connection.maximum": "500",
    "spark.hadoop.fs.s3a.threads.max": "256",
    "spark.hadoop.fs.s3a.fast.upload": "true",

    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max": "512m"
}

for key, value in spark_config.items():
    spark.conf.set(key, value)
```

---

## Expected Results

- **Without optimization**: 40+ minutes → timeout
- **With salting (Solution 1)**: 10-20 minutes
- **With isolated skew handling (Solution 2)**: 5-15 minutes
- **With broadcast (Solution 3)**: 2-5 minutes (if feasible)

**Start with Solution 2 (Isolated Skew Handling)** - it's the most efficient for your use case.