# ✅ Fix for Input Data Partitioned by CUSIP

Since your input data is already partitioned by CUSIP (all 570M rows with same CUSIP in one file), you need to **force a shuffle** to redistribute the data BEFORE joining.

## 🎯 SIMPLEST Solution - Force Repartition Before Join

```python
from pyspark.sql.functions import col

# ============================================
# CRITICAL: Repartition BOTH tables before join
# ============================================

# Read your tables (if not already loaded)
table1 = spark.read.parquet("s3://bucket/table1/")
table2 = spark.read.parquet("s3://bucket/table2/")

# Force shuffle to redistribute by COMPOSITE key
table1_redistributed = table1.repartition(16000, "CUSIP", "EFFECTIVEDATE")
table2_redistributed = table2.repartition(16000, "CUSIP", "EFFECTIVEDATE")

# Now join - data is already well-distributed
result = table1_redistributed.join(
    table2_redistributed,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="inner"
)

# Write output
result.write.mode("overwrite").parquet("s3://bucket/output/")
```

**That's it!** This forces Spark to shuffle and distribute your 570M rows across 16,000 partitions based on both CUSIP and EFFECTIVEDATE.

## 📊 What This Does

**Before (Input partitioning):**
```
File 1: cusip=3132M9W75_2024-11-01 → 570M rows → Spark Partition 0
File 2: cusip=ABC123              → 45M rows  → Spark Partition 1
File 3: cusip=DEF456              → 20M rows  → Spark Partition 2
```

**After repartition(16000, "CUSIP", "EFFECTIVEDATE"):**
```
Partition 0:    3132M9W75_2024-11-01 + 2024-01-01 → 35K rows
Partition 1:    3132M9W75_2024-11-01 + 2024-01-02 → 35K rows
Partition 2:    3132M9W75_2024-11-01 + 2024-01-03 → 35K rows
...
Partition 8523: 3132M9W75_2024-11-01 + 2024-05-15 → 35K rows
...
Partition 9000: ABC123 + various dates → ~450K rows
```

Your 570M rows with same CUSIP get split across thousands of partitions because each has a different EFFECTIVEDATE.

## ⚡ Alternative: Repartition by EFFECTIVEDATE Only

If most of your skew is from CUSIP clustering, try this:

```python
# Redistribute primarily by date (spreads hot CUSIP across partitions)
table1_redistributed = table1.repartition(16000, "EFFECTIVEDATE")
table2_redistributed = table2.repartition(16000, "EFFECTIVEDATE")

result = table1_redistributed.join(
    table2_redistributed,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="inner"
)
```

This works because:
- Your 570M rows with same CUSIP have different dates
- Repartitioning by date splits them across partitions
- Each partition will have mixed CUSIPs

## 🔧 Optimized Version with Spark Config

```python
# ============================================
# Step 1: Configure Spark
# ============================================
spark.conf.set("spark.sql.shuffle.partitions", "16000")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# ============================================
# Step 2: Read and immediately repartition
# ============================================
table1 = spark.read.parquet("s3://bucket/table1/") \
    .repartition(16000, "CUSIP", "EFFECTIVEDATE")

table2 = spark.read.parquet("s3://bucket/table2/") \
    .repartition(16000, "CUSIP", "EFFECTIVEDATE")

# Cache after repartitioning if you'll use multiple times
# table1.cache()
# table2.cache()

# ============================================
# Step 3: Join
# ============================================
result = table1.join(table2, on=["CUSIP", "EFFECTIVEDATE"], how="inner")

# ============================================
# Step 4: Write (AQE will coalesce small partitions automatically)
# ============================================
result.write.mode("overwrite").parquet("s3://bucket/output/")
```

## 📈 Performance Expectations

| Metric | Before | After Repartition |
|--------|--------|------------------|
| Max partition | 570M rows | ~35K rows |
| Join stage time | Hours | 20-40 minutes |
| Shuffle read | Minimal (broadcast?) | ~100GB |
| Executor utilization | 5-10% | 80-95% |

## 🔍 Verify It's Working

```python
from pyspark.sql.functions import spark_partition_id

# Check partition distribution after repartition
check_dist = table1_redistributed.withColumn("pid", spark_partition_id()) \
    .groupBy("pid").count() \
    .orderBy(col("count").desc())

check_dist.show(20)

# You should see:
# Max rows per partition: ~100K (not 570M!)
# Relatively even distribution
```

## ⚠️ Important Notes

1. **Repartition = Expensive Shuffle**: This will shuffle ALL your data (640M rows), which takes time but it's necessary.

2. **Memory**: With 16K partitions and 640M rows:
   - Average: 40K rows per partition
   - Very manageable for your executors

3. **Both Tables**: Must repartition BOTH tables by the same keys, otherwise Spark will shuffle again during join.

4. **Don't Cache Before Repartition**: Cache AFTER repartitioning if needed:
   ```python
   table1_redistributed = table1.repartition(16000, "CUSIP", "EFFECTIVEDATE")
   table1_redistributed.cache()  # Cache the redistributed version
   table1_redistributed.count()  # Trigger cache
   ```

## 💡 Why This is Better Than Salting

**Repartitioning:**
- ✅ Simpler (3 lines of code)
- ✅ Works for ALL skewed keys automatically
- ✅ No table explosion (table2 stays same size)
- ✅ Spark optimizes naturally

**Salting:**
- ❌ More complex code
- ❌ Explodes table2 size (if you have matching records)
- ✅ Better for extreme cases where repartition isn't enough

## 🎯 My Recommendation

**Try the repartition solution first** - it's the standard fix for input-partitioned data and should solve your problem completely. Only use salting if:
- Repartitioning doesn't reduce max partition below 5-10M rows
- You still see significant skew in Spark UI after repartitioning
