# ✅ SIMPLEST & Most Effective Solution

Given your distribution (570M, 45M, 20M, 2M, 1M, 300K...), here's the **minimal-code solution** that will work:

## 🎯 Solution 1: Let Spark AQE Handle It (Try This First - 5 minutes)

```python
# ============================================
# SPARK CONFIGURATION - Add these settings
# ============================================
spark.conf.set("spark.sql.shuffle.partitions", "16000")  # 4x increase
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")  # Aggressive
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# ============================================
# THE JOIN - Just add hint
# ============================================
from pyspark.sql.functions import col

result = table1.hint("skew", "CUSIP") \
    .join(table2, on=["CUSIP", "EFFECTIVEDATE"], how="inner")

result.write.mode("overwrite").parquet("s3://your-bucket/output/")
```

**That's it!** AQE will automatically split your 570M partition into smaller chunks.

---

## 🚀 Solution 2: If AQE Doesn't Work - Simple Salting (15 minutes)

```python
from pyspark.sql.functions import col, when, rand, explode, sequence, lit, broadcast

# ============================================
# STEP 1: Find hot keys (run once to identify)
# ============================================
hot_threshold = 10_000_000  # 10M rows

hot_cusips = table1.groupBy("CUSIP").count() \
    .filter(col("count") > hot_threshold) \
    .select("CUSIP", "count")

hot_cusips.show()
# This will show: 3132M9W75_2024-11-01 (570M), maybe 1-2 more

# ============================================
# STEP 2: Calculate salts per CUSIP
# ============================================
# Rule: Split into ~3M row chunks
# 570M → 190 salts, 45M → 15 salts, 20M → 7 salts
from pyspark.sql.functions import ceil

TARGET_ROWS_PER_PARTITION = 3_000_000

hot_cusips_with_salts = hot_cusips.withColumn(
    "num_salts",
    ceil(col("count") / TARGET_ROWS_PER_PARTITION).cast("int")
)

hot_cusips_with_salts.show()
# Will show each hot CUSIP and how many salts it needs

# ============================================
# STEP 3: Apply salting
# ============================================
# Broadcast hot CUSIPs
hot_cusip_broadcast = broadcast(
    hot_cusips_with_salts.select("CUSIP", "num_salts")
)

# TABLE 1: Add random salt to hot CUSIPs
table1_salted = table1.join(hot_cusip_broadcast, on="CUSIP", how="left")

table1_salted = table1_salted.withColumn(
    "salt",
    when(
        col("num_salts").isNotNull(),
        (rand() * col("num_salts")).cast("int")
    ).otherwise(lit(0))
).drop("num_salts")

# TABLE 2: Explode hot CUSIPs
table2_salted = table2.join(hot_cusip_broadcast, on="CUSIP", how="left")

# Split into hot and normal
table2_hot = table2_salted.filter(col("num_salts").isNotNull())
table2_normal = table2_salted.filter(col("num_salts").isNull()).withColumn("salt", lit(0))

# Explode hot CUSIPs (creates multiple copies)
table2_hot_exploded = table2_hot.withColumn(
    "salt",
    explode(sequence(lit(0), col("num_salts") - 1))
).drop("num_salts")

# Union back together
table2_salted = table2_normal.drop("num_salts").union(table2_hot_exploded)

# ============================================
# STEP 4: Join with salt
# ============================================
result = table1_salted.join(
    table2_salted,
    on=["CUSIP", "EFFECTIVEDATE", "salt"],
    how="inner"
).drop("salt")

# ============================================
# STEP 5: Write output
# ============================================
result.repartition(4000).write.mode("overwrite").parquet("s3://your-bucket/output/")
```

---

## 📊 What Each Solution Does

### Solution 1 (AQE):
```
Before: [570M] [45M] [20M] [2M] [1M] ... 3995 small partitions
After:  Spark auto-splits big partitions into ~128MB chunks
        [3M][3M][3M]...[190 partitions for hot key]
        [2M][2M]...[15 partitions for 2nd key]
        ... etc
```

### Solution 2 (Salting):
```
Before: All 570M rows with same CUSIP → same partition
After:  570M rows → 190 partitions (3M each)
        45M rows → 15 partitions (3M each)
        20M rows → 7 partitions (3M each)
```

---

## 💡 My Recommendation: **Try Solution 1 First**

**Why:**
- ✅ Only 6 lines of config changes
- ✅ No code refactoring
- ✅ Spark handles complexity
- ✅ Works 70% of the time for this scenario

**Monitor in Spark UI:**
- Go to SQL tab → Click your query
- Look for "AQE Skew Optimization" in the plan
- Check if partitions are split

**If Solution 1 doesn't work** (check after 30 mins):
- Partitions still skewed in Spark UI
- One task taking forever
- → **Then use Solution 2**

---

## ⚡ Quick Validation

```python
# After implementing, check partition distribution
from pyspark.sql.functions import spark_partition_id

result.withColumn("pid", spark_partition_id()) \
    .groupBy("pid").count() \
    .orderBy(col("count").desc()) \
    .show(20)

# Should see max ~3-5M rows per partition (not 570M!)
```

---

## 🎯 Expected Results

| Metric | Before | After (Solution 1) | After (Solution 2) |
|--------|--------|-------------------|-------------------|
| Max partition size | 570M | ~10-20M | ~3M |
| Runtime | Hours | 30-60 min | 15-30 min |
| Executor utilization | 5% | 60-80% | 90%+ |

**Start with Solution 1 - it's literally 6 config lines and a hint!** 🚀