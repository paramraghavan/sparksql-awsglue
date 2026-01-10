Analyzing Table Joins

I'm currently focused on a left join between table1 (approximately 641M rows) and table2 (roughly 72.8M rows). The join
keys are cusip and effective_date. Both tables appear to be uniquely keyed on these fields, which should streamline the
process.

Identifying the Bottleneck

I've isolated the core issue: extreme partition skew in table1. A single partition holds nearly 90% of the data,
resulting in a runaway task. My attempts at repartitioning, range partitioning, and salting haven't helped. It appears
the skew is baked into the initial data distribution. This is causing memory exhaustion and underutilization of
resources, leading to the job stalling.

Exploring Join Strategies

Now, I'm evaluating a few new approaches. First, I'm leaning toward explicitly repartitioning table1 on a random key,
a "nuclear" option to break the skew. Alternatively, a broadcast join for table2 is worth a shot. Lastly, I'm
considering Adaptive Query Execution (AQE) tuning. The goal is to avoid the shuffle altogether or to force AQE to handle
any remaining skew.

Considering Nuclear Repartitioning

I'm now zeroing in on a broadcast join strategy for table2, as the size appears manageable on our cluster. This should
sidestep the skew issue entirely by avoiding a shuffle. Alternatively, I'm revisiting the "nuclear" repartitioning on a
random key for table1 as a backup, though the shuffle concerns remain. Digging deeper, I suspect this skew is inherited
from earlier joins in table1's construction. My goal is to maximize cluster utilization.

Optimizing S3 Reads

I've been evaluating different strategies to read from S3. Scenario A, a broadcast join, seems most promising, since the size is manageable. Scenario B involves salting with AQE's skew join. Scenario C is a physical partitioning/bucketing fix, a structural approach. Memory per node is significant at 512GB, which is helpful. Considering executors, 5-6 cores per executor are optimal for S3 I/O. I'll increase spark.sql.autoBroadcastJoinThreshold. The issue isn't hot keys but partition skew, possibly from a past coalesce or repartition, and I'll focus on either a Broadcast join or random repartitioning.
----------------------------------------------------------------
This is a classic "Straggler" problem, but the scale of your skew is extreme. Having 570 million rows in a single
partition while the average is 160k means **one single CPU core** is trying to process 89% of your data while the other
1,299 cores sit idle.

Standard salting often fails here because it addresses "hot keys" (many rows with the same key). Since your keys are
unique (1.0 ratio), your problem isn't key cardinality—it's **physical partition imbalance** inherited from your
upstream joins.

Here are the three "nuclear" options to resolve this.

---

### Option 1: The Broadcast "Cheat" (Recommended)

Since Table 2 is 72M rows, it likely fits in memory. On a cluster with 512GB RAM per node, you can bypass the Shuffle
Merge Join entirely. By broadcasting Table 2, Spark will send a copy of it to every executor. Table 1 will stay exactly
where it is (even with the skew), and Spark will process that 570M-row partition as one task without moving it across
the network.

**Why it works:** It eliminates the "Shuffle" phase where Spark usually chokes on skewed data.

#### PySpark Code

```python
from pyspark.sql.functions import broadcast

# Increase the broadcast timeout for a table of 72M rows
spark.conf.set("spark.sql.broadcastTimeout", "3600")

# Force broadcast of the smaller table
result_df = table1.join(broadcast(table2), ["cusip", "effectivedate"], "left")

result_df.write.format("parquet").save("s3://your-path/output/")

```

#### Spark-Submit

```bash
spark-submit --master yarn \
  --deploy-mode cluster \
  --driver-memory 30G \
  --executor-memory 40G \
  --executor-cores 5 \
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \
  --conf spark.network.timeout=800s \
  your_script.py

```

---

### Option 2: The "Nuclear" Random Redistribution

If Table 2 is too wide/heavy to broadcast, you must break that 570M row partition. Standard `repartition("cusip")` will
just put those rows back into a single partition because they are unique. You need to force Spark to ignore the keys and
distribute data physically across the cluster using a random seed.

**Why it works:** `repartition(n, rand())` forces a full shuffle that ignores column values, ensuring every partition is
exactly the same size.

#### PySpark Code

```python
from pyspark.sql.functions import rand

# Force an even physical distribution across the cluster first
# Use a high number of partitions (e.g., 2-3x your core count)
table1_balanced = table1.repartition(4000, rand(seed=42))

# Now perform the join
result_df = table1_balanced.join(table2, ["cusip", "effectivedate"], "left")

result_df.write.format("parquet").save("s3://your-path/output/")

```

#### Spark-Submit

```bash
spark-submit --master yarn \
  --deploy-mode cluster \
  --executor-memory 48G \
  --executor-cores 5 \
  --conf spark.sql.shuffle.partitions=4000 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=10G \
  your_script.py

```

---

### Option 3: AQE Skew Join Optimization (Spark 3.5 Native)

Since you are on Spark 3.5.3, you have the most advanced version of **Adaptive Query Execution (AQE)**. You can tell
Spark to automatically detect the skew and split the "giant" partition into smaller chunks during the join.

**Why it works:** Spark 3.5 will see that 570M row partition and programmatically split it into multiple tasks, joining
each piece with Table 2 separately.

#### PySpark Code

```python
# Enable AQE and Skew Join features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Set the threshold low enough to catch your 570M row partition
# If avg is 160k, setting this to 1MB or 50k rows will trigger it
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")

result_df = table1.join(table2, ["cusip", "effectivedate"], "left")
result_df.write.format("parquet").save("s3://your-path/output/")

```

#### Spark-Submit

```bash
spark-submit --master yarn \
  --deploy-mode cluster \
  --executor-memory 40G \
  --executor-cores 5 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.runtime.io.maxRetries=10 \
  your_script.py

```

---

### Critical Advice for your EMR Setup:

1. **Executor Cores:** You are using 64-core nodes. **Do not** set `--executor-cores 64`. This leads to massive HDFS/S3
   I/O contention. Use **5 cores per executor**. This allows for better throughput.
2. **Memory Overload:** Your 99% memory usage is likely "Memory Overhead" or "Off-Heap" issues caused by that one
   massive task failing and retrying.
3. **Check Upstream:** If Table 1 is built via multiple joins, look for a `coalesce(1)` or a bad `groupBy` right before
   the join. That is usually where a 570M-row partition is born.

**Would you like me to help you calculate the exact number of executors and memory settings based on your 22-node EMR
instance types?**