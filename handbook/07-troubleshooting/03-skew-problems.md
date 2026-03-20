# Data Skew Problems

## Symptom

- Some tasks run 10-100x longer than others
- One executor busy while others idle
- In Spark UI, task duration varies wildly
- CPU utilization low (10-20%) while RAM maxed out (99%)
- One single partition holding 90%+ of the data

## Common Causes

1. **Join on high-cardinality column with skewed distribution**
   - Grouping by user_id where some users have 1M events
   - Joining on product_id where some products are very popular

2. **Filtering creating imbalance**
   - After filter, 90% of data in one partition

3. **Key distribution naturally skewed**
   - Real-world data often has power law distribution

4. **Uneven distribution from previous joins**
   - Multiple joins causing data to concentrate in specific partitions

## Solutions

### Quick Solutions (Start Here)

#### Solution 1: Salting (Proven)
```python
from pyspark.sql.functions import rand, floor

df_salted = df \
    .withColumn("_salt", floor(rand() * 10).cast("int")) \
    .repartition(200, "_salt") \
    .drop("_salt")
```

#### Solution 2: Enable Adaptive Query Execution (AQE)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

#### Solution 3: Repartition on Join Key
```python
# Pre-partition before join
df1 = df1.repartition(500, "join_key")
df2 = df2.repartition(500, "join_key")
result = df1.join(df2, "join_key")
```

---

## Advanced Solutions (For Severe Skew)

### Solution 4: Broadcast Strategy
When a smaller table can fit in memory, avoid shuffle entirely with broadcast join.

**When to use**: If `table2` is <13GB and has 72M rows (fits in executor memory)

```python
from pyspark.sql import functions as F

# Increase broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1048576000")  # 1GB
spark.conf.set("spark.sql.broadcastTimeout", "3600")  # 1 hour

# Force broadcast of smaller table
result = table1.join(F.broadcast(table2), ["cusip", "effectivedate"], "left")
```

**Spark-Submit Config:**
```bash
--executor-memory 32G \
--conf spark.driver.memory=16G \
--conf spark.sql.shuffle.partitions=4000
```

---

### Solution 5: Isolated Re-shuffling (Nuclear Repartition)
Force a complete shuffle based on random value to break existing skew **before** the final join.

**When to use**: When initial reads are balanced but joins create skew

```python
from pyspark.sql.functions import rand

# Break the skew with random repartitioning
table1_balanced = table1.repartition(4000, F.rand())

# Now perform the join
result = table1_balanced.join(table2, ["cusip", "effectivedate"], "left")
```

**Spark-Submit Config:**
```bash
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.skewJoin.enabled=true \
--conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=2 \
--conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
```

---

### Solution 6: Heavy Hitter Isolation (Manual Split)
Identify the 1% of keys causing 90% of the trouble and treat them separately.

**When to use**: When specific keys/cusips are the problem, not overall distribution

```python
from pyspark.sql import functions as F

# 1. Identify heavy hitter CUSIPs (top 50 by count)
heavy_hitters = table1.groupBy("cusip").count().orderBy(F.desc("count")).limit(50)
heavy_list = [row['cusip'] for row in heavy_hitters.collect()]

# 2. Split Table 1 into 'Normal' and 'Monster' data
t1_monster = table1.filter(F.col("cusip").isin(heavy_list))
t1_normal = table1.filter(~F.col("cusip").isin(heavy_list))

# 3. Join Normal data using standard shuffle
res_normal = t1_normal.join(table2, ["cusip", "effectivedate"], "left")

# 4. Join Monster data using broadcast (filter table2 to relevant keys)
t2_monster_lookup = table2.filter(F.col("cusip").isin(heavy_list))
res_monster = t1_monster.join(F.broadcast(t2_monster_lookup),
                               ["cusip", "effectivedate"], "left")

# 5. Union back together
final_df = res_normal.union(res_monster)
```

---

### Solution 7: S3 Checkpoint (Lineage Truncation)
Materialize an intermediate DataFrame to S3 to truncate the lineage and reduce memory pressure.

**When to use**: When lazy evaluation keeps 10+ joins in memory causing OOM

```python
# Step 1: Materialize Table 1 to S3 with high partition count
table1.repartition(8000).write.mode("overwrite").parquet(
    "s3://bucket/temp/table1_checkpoint/"
)

# Step 2: Clear session cache
spark.catalog.clearCache()

# Step 3: Read back as fresh DataFrame
t1_static = spark.read.parquet("s3://bucket/temp/table1_checkpoint/")

# Step 4: Perform the join
result = t1_static.join(table2, ["cusip", "effectivedate"], "left")
```

---

### Solution 8: Bucket Joining (Pre-partitioned Data)
For long-term: write base tables pre-bucketed to S3. Enables "colocated join" with zero shuffle.

**When to use**: You control how data is stored and want permanent solution

```python
# Do this once for both base tables
(table1_base
    .write
    .bucketBy(4000, "cusip", "effectivedate")
    .sortBy("cusip", "effectivedate")
    .saveAsTable("table1_bucketed")
)

(table2_base
    .write
    .bucketBy(4000, "cusip", "effectivedate")
    .sortBy("cusip", "effectivedate")
    .saveAsTable("table2_bucketed")
)

# Join now has ZERO shuffle
t1 = spark.table("table1_bucketed")
t2 = spark.table("table2_bucketed")
result = t1.join(t2, ["cusip", "effectivedate"], "left")
```

---

## Aggressive YARN Configuration (When CPU=10%, RAM=99%)

This indicates **Shuffle Disk Spilling**. Spark is writing shuffle blocks to disk because they don't fit in RAM.

```bash
spark-submit \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=2 \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB \
  --conf spark.shuffle.file.buffer=1m \
  --conf spark.shuffle.spill.compress=true \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.2 \
  --conf spark.executor.memoryOverhead=8G \
  your_job.py
```

**Why this works:**
- **Adaptive SkewJoin**: Detects and splits large partitions automatically
- **Memory Fraction 0.8**: Give 80% RAM to execution, 20% to storage → prevents caching pressure
- **Memory Overhead 8G**: Reserve overhead for JVM, shuffle buffers

---

## Guidelines for Complex Views

When using `createOrReplaceTempView()` on large, multi-join DataFrames:

1. **Lineage Heaviness**: If view is built on 10+ joins, consider materializing to S3 first
2. **Multiple Uses**: If you use the view 2+ times, checkpoint it to avoid recomputation
3. **Caching**: `spark.catalog.cacheTable("view_name")` is safe IF you have memory. Otherwise avoid.

---

**See Also**: [Job Stuck Issues](01-job-stuck-issues.md), [Memory Errors](02-memory-errors.md), [Performance Degradation](05-performance-degradation.md)
