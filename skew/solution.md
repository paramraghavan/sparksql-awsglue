This is a classic "Data Skew" nightmare. Your stats show that **one single partition holds 570 million rows** out of 641
million total. Even with 1,300 cores, Spark is stalling because 99% of your cluster is waiting for the one core
processing that massive partition to finish.

Since your keys are 1:1 (unique), the skew isn't caused by duplicate keys, but likely by the **Spark Shuffle** process
or an uneven distribution of the data coming out of your initial joins.

Here are your "nuclear" options to fix this.

---

## Option 1: The Boadcast Strategy

Since you have 22 nodes with 512GB RAM each, you have massive memory. If `table2` (72M rows) can fit into the memory of
a few executors, we can force a **Broadcast Hash Join**. Even if it's slightly over the default limit, broadcasting
avoids the shuffle entirely, which is where your skew is happening.

**The Logic:** Bypass the shuffle. If Spark doesn't shuffle, it won't group that skewed data into one task.
> Broadcast did not work because tbe table2 sizes is ~13 GB
```python
from pyspark.sql import functions as F

# Increase broadcast timeout and limit (Table 2 is ~5-10GB in memory)
# Set it to $10$ GB ($10,737,418,240 bytes)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1048576000")  # 1GB or more
spark.conf.set("spark.sql.broadcastTimeout", "3600")

# Force broadcast of the smaller table
result = table1.join(F.broadcast(table2), ["cusip", "effectivedate"], "left")

```

**Spark-Submit Configs:**

* `--executor-memory 32G` (Give enough overhead for the broadcast)
* `--conf spark.driver.memory=16G` (Driver handles broadcast)
* `--conf spark.sql.shuffle.partitions=4000`

---

## Option 2: Isolated Re-shuffling (The "Nuclear" Repartition)
**This worked**
If your initial reads from S3 are fine but the "multiple joins" to create `table1` are causing the skew, you must break
the lineage and force a physical redistribution using a high-entropy column or a random seed **before** the final join.

**The Logic:** Use `repartition` on a random expression to ensure the 570M rows are physically moved out of that single
skewed partition before the join logic starts.

```python
# Force a shuffle based on a random seed to break the existing skew
table1_balanced = table1.repartition(4000, F.rand())

# Perform the join
result = table1_balanced.join(table2, ["cusip", "effectivedate"], "left")

```

**Spark-Submit Configs:**

* `--conf spark.sql.adaptive.enabled=true`
* `--conf spark.sql.adaptive.skewJoin.enabled=true`
* `--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128mb`

---

## Option 3: Perspective Change - Join during S3 Read (Bucket Join)

If you have control over how the base data is stored, the most robust "nuclear" option is **Bucket Joining**. This
eliminates the shuffle at runtime because data is pre-sorted and pre-partitioned on S3.

**The Logic:** Both tables are written to S3 partitioned into the same number of buckets. Spark will then perform a "
Colocated Join."

```python
# Do this once for both base tables
(table1_base.write
 .bucketBy(4000, "cusip", "effectivedate")
 .sortBy("cusip", "effectivedate")
 .saveAsTable("table1_bucketed"))

(table2_base.write
 .bucketBy(4000, "cusip", "effectivedate")
 .sortBy("cusip", "effectivedate")
 .saveAsTable("table2_bucketed"))

# The join will now be lightning fast with 0 shuffle
t1 = spark.table("table1_bucketed")
t2 = spark.table("table2_bucketed")
result = t1.join(t2, ["cusip", "effectivedate"], "left")

```

---

## Guidelines for Large Views (`createOrReplaceTempView`)

Using views on large DataFrames is generally safe because a View is just a logical alias for a Plan. However:

1. **Lineage Heaviness:** If your view is built on 10+ joins, every time you call it, Spark re-calculates the plan. If
   you use the view multiple times, **`.checkpoint()`** it to S3 to truncate the lineage.
2. **Memory Pressure:** If you use `spark.catalog.cacheTable("view_name")`, ensure you have enough RAM. Given your 10TB
   YARN usage, you might be accidentally caching or spilling to disk.

## Why your CPU is low and RAM is high

Your 10% CPU usage confirms the skew. 1290 cores are idle because they have no data, while 10 cores are struggling with
the 570M-row partition. The 10TB RAM usage suggests Spark is trying to buffer that massive partition in memory and
likely failing, leading to "Shuffle Fetch Wait" or disk spilling.

# More more

If **Iterative Batching** (processing date-by-date) also failed, it indicates that the skew is not just temporal, but
concentrated within a single key (or a few keys) for a specific date.

When your YARN memory usage hits 99% and the job hangs, Spark is likely suffering from **Shuffle Fetch Failures** or *
*OOM (Out of Memory) during Sort**. Since "Salt and Broaden" failed, the "Nuclear" option left is to physically isolate
the "Monster Key" and process it using a **Map-side Join** logic or a **Cartesian-Broadcast** hybrid.

Here are the 3 best remaining "Nuclear" options:

---

## 1. The "Heavy Hitter" Isolation (Manual Split)

This is the most reliable way to fix a job that won't finish. You identify the 1% of keys causing 90% of the trouble and
treat them differently.

**The Logic:** You join the "clean" data normally. For the "monster" keys, you use a broadcast join. If a single key is
so large that even a broadcast join fails, you perform a **Filtering Join**.

```python
# 1. Identify the top 50 'Monster' CUSIPs
heavy_hitters = table1.groupBy("cusip").count().orderBy(F.desc("count")).limit(50)
heavy_list = [row['cusip'] for row in heavy_hitters.collect()]

# 2. Split Table 1 into 'Normal' and 'Monster'
t1_monster = table1.filter(F.col("cusip").isin(heavy_list))
t1_normal = table1.filter(~F.col("cusip").isin(heavy_list))

# 3. Join Normal data (standard shuffle)
res_normal = t1_normal.join(table2, ["cusip", "effectivedate"], "left")

# 4. Join Monster data (Forced Broadcast of the RIGHT side)
# We filter Table 2 to only include the keys we need for the monsters
t2_monster_lookup = table2.filter(F.col("cusip").isin(heavy_list))
res_monster = t1_monster.join(F.broadcast(t2_monster_lookup), ["cusip", "effectivedate"], "left")

# 5. Union them back
final_df = res_normal.union(res_monster)

```

---

## 2. The "S3 Checkpoint" (Lineage Truncation)

Spark's Lazy Evaluation is likely your enemy here. Because `table1` is built from "multiple joins," Spark keeps the
entire history in memory. If a task fails due to skew, it tries to re-run all previous joins, leading to the 10TB RAM
spike.

**The Logic:** Force Spark to "forget" how `table1` was made by writing it to S3 and reading it back as a static file.

```python
# Step 1: Materialize Table 1 to S3
# Use a high partition count to ensure no single file is too large
table1.repartition(8000).write.mode("overwrite").parquet("s3://bucket/temp/table1_checkpoint/")

# Step 2: Clear the session cache (optional but helpful)
spark.catalog.clearCache()

# Step 3: Read it back as a fresh DataFrame
t1_static = spark.read.parquet("s3://bucket/temp/table1_checkpoint/")

# Step 4: Perform the join
result = t1_static.join(table2, ["cusip", "effectivedate"], "left")

```

---

## 3. The "Broadside" Configuration (YARN Tuning)

If your CPU is at 10% and RAM is at 99%, you are likely hitting **Disk Spilling**. Spark is writing "Shuffle Blocks" to
the local NVMe disks of the EMR nodes because they don't fit in RAM.

**Spark-Submit Configs for "Nuclear" Stability:**
Use these exact settings to force Spark to be more resilient to skew:

```bash
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.skewJoin.enabled=true \
--conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=2 \
--conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB \
--conf spark.shuffle.file.buffer=1m \
--conf spark.shuffle.spill.compress=true \
--conf spark.memory.fraction=0.8 \
--conf spark.memory.storageFraction=0.2 \
--conf spark.executor.memoryOverhead=8G

```

### Why this configuration?

* **Adaptive SkewJoin:** We lower the threshold so Spark detects skew earlier and automatically "splits" large
  partitions for you.
* **Memory Fraction:** We give 80% of the RAM to "Execution" (joins/sorts) and only 20% to "Storage" (caching), which
  prevents the 99% RAM hang.

