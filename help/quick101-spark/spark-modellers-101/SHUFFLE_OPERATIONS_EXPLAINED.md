# Shuffle Operations in Spark: The Most Expensive Operation

> **Key Takeaway**: Shuffle moves data between executors. It's the slowest operation in Spark. Understanding and reducing shuffles is critical for performance.

---

## Quick Answer

| Operation | Type | Shuffles? | Cost |
|-----------|------|-----------|------|
| `df.map()` | Narrow | ❌ No | Fast (in-memory) |
| `df.filter()` | Narrow | ❌ No | Fast (in-memory) |
| `df.select()` | Narrow | ❌ No | Fast (in-memory) |
| `df.groupBy().count()` | Wide | ✅ YES | Expensive (network + disk) |
| `df.join()` | Wide* | ✅ YES (usually) | Expensive (network + disk) |
| `df.sort()` | Wide | ✅ YES | Expensive (network + disk) |
| `df.distinct()` | Wide | ✅ YES | Expensive (network + disk) |
| `df.repartition()` | Wide | ✅ YES | Very Expensive (full network shuffle) |

*Join can be narrow if broadcast join is used

---

## What is a Shuffle?

### Definition

A **shuffle** is when Spark moves data from one executor to another to group or rearrange data.

### Visual: What Happens During a Shuffle

```
WITHOUT Shuffle (Narrow):
┌──────────────┐
│  Executor 1  │  Partition 0: [1,2,3]  → filter → [2]
│  Partition 0 │
└──────────────┘

┌──────────────┐
│  Executor 2  │  Partition 1: [4,5,6]  → filter → [5]
│  Partition 1 │
└──────────────┘

Result: [2, 5] - No data moved between executors ✅


WITH Shuffle (Wide):
┌──────────────┐
│  Executor 1  │  [user_id: 1,2,3]
│  Partition 0 │         ↓ (shuffle)
└──────────────┘         ↓
         ┌────────────────┴────────────────┐
         │ NETWORK + DISK WRITE            │
         └────────────────┬────────────────┘
                          ↓
┌──────────────┐      ┌──────────────┐
│  Executor 1  │      │  Executor 2  │
│ user_id: 1,3 │      │  user_id: 2  │
└──────────────┘      └──────────────┘

Result: Data rearranged, now grouped by user_id ❌ (Expensive!)
```

---

## Why Shuffles Are Expensive

### The 4 Costs of a Shuffle

**1. Network I/O** (Moving data across cluster)
```
Example: 1GB data needs to move from Executor 1 → Executor 2
Network bandwidth: ~1 Gbps = 125 MB/sec
Time = 1000 MB ÷ 125 MB/sec = 8 seconds (just network!)
```

**2. Disk I/O** (Writing intermediate results)
```
Executor 1: Write 1GB to disk before sending (serialize + disk write)
Network: Send 1GB
Executor 2: Read 1GB from disk (deserialize + disk read)

Typical Disk Speed: ~100 MB/sec
Writing: 1000 MB ÷ 100 MB/sec = 10 seconds
Reading: 1000 MB ÷ 100 MB/sec = 10 seconds
```

**3. Serialization** (Converting objects to bytes)
```python
df = spark.read.csv(...)  # Row objects (large in Java)
df.groupBy("id").count()  # Shuffle requires serializing all rows
# Serialization overhead: ~2-5x memory increase
```

**4. GC Pauses** (Memory pressure causes garbage collection)
```
During shuffle:
- Buffering data in memory
- Serialized objects
- Multiple copies (original + serialized)
Result: Garbage collection pauses (STOP THE WORLD)
Example: 2GB shuffle = 500ms GC pause
```

### Total Shuffle Cost Calculation

```
100 MB data shuffle:
├─ Serialize on executor 1: ~2 seconds
├─ Network transfer: ~0.8 seconds (1 Gbps network)
├─ Disk write: ~1 second
├─ Disk read on executor 2: ~1 second
├─ Deserialize: ~2 seconds
└─ GC pause: ~0.5 seconds
────────────────────────────
TOTAL: ~7 seconds for 100MB

Compare to:
Non-shuffle operation on same 100MB: ~0.1 seconds
= 70x SLOWER ❌
```

---

## Narrow vs Wide Transformations

### Narrow Transformations (✅ No Shuffle)

**Definition**: Each partition outputs to ONE partition

```python
# Example 1: map - Each element stays in its partition
df = spark.sparkContext.parallelize([1,2,3,4,5,6], 2)  # 2 partitions
df.map(x => x * 2)  # Partition 0: [1,2,3] → [2,4,6]
                    # Partition 1: [4,5,6] → [8,10,12]
# No shuffle needed ✅

# Example 2: filter - Elements stay in their partition
df.filter(x => x > 3)  # Partition 0: [1,2,3] → []
                       # Partition 1: [4,5,6] → [4,5,6]
# No shuffle needed ✅

# Example 3: select - Column selection from same rows
df.select("name", "age")  # Row order unchanged
# No shuffle needed ✅
```

**Performance Impact**:
```
File size: 10GB
Partitions: 100
Per partition: 100MB

Narrow operation:
├─ Executor 1: Process partitions [0-24]
├─ Executor 2: Process partitions [25-49]
├─ Executor 3: Process partitions [50-74]
└─ Executor 4: Process partitions [75-99]
Result: Parallelized, fast ✅
```

### Wide Transformations (❌ Causes Shuffle)

**Definition**: Each partition outputs to MANY partitions (or changes partition count)

```python
# Example 1: groupBy - Different keys go to different partitions
df.groupBy("user_id").count()
# user_id=1 (partition 0) → might need to go to partition 2
# user_id=2 (partition 0) → might need to go to partition 5
# SHUFFLE REQUIRED ❌

# Example 2: sort - Rearranging all data across cluster
df.sort("salary")
# Element with rank 1 → executor 1, partition 0
# Element with rank 2 → executor 2, partition 1
# Element with rank 3 → executor 1, partition 1
# SHUFFLE REQUIRED ❌

# Example 3: join - Matching keys across DataFrames
df1.join(df2, df1.user_id == df2.user_id)
# All user_id=5 from df1 must meet with user_id=5 from df2
# Might be on different executors
# SHUFFLE REQUIRED ❌

# Example 4: distinct - Removing duplicates from any partition
df.distinct()
# Partition 0: [1,1,2]
# Partition 1: [1,3,3]
# All 1's must go to same partition to detect duplicates
# SHUFFLE REQUIRED ❌
```

**Performance Impact**:
```
File size: 10GB
Partitions: 100 → (wide operation) → 200 partitions

Full Shuffle:
├─ Write: All 100 partitions write to disk (10GB written)
├─ Network: All 10GB transfered across network
├─ Read: All 200 new partitions read from disk
├─ Memory: Buffering during shuffle
└─ GC: Garbage collection pauses
Result: Very slow ❌
```

---

## Common Shuffle Scenarios

### Scenario 1: groupBy().count()

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShuffleDemo").getOrCreate()

# Sample data
data = [
    ("Alice", "Sales"),
    ("Bob", "Sales"),
    ("Charlie", "Engineering"),
    ("David", "Engineering"),
    ("Eve", "Engineering"),
]

df = spark.createDataFrame(data, ["name", "department"])
# Partitions:
# Partition 0: [Alice/Sales, Bob/Sales]
# Partition 1: [Charlie/Eng, David/Eng, Eve/Eng]

# This causes SHUFFLE:
result = df.groupBy("department").count()

# WHY SHUFFLE?
# - Before shuffle: Eng data split across 2 partitions
# - After shuffle: All Eng data goes to 1 partition
#
# Execution:
# Step 1: Executor 1 (Partition 0) - Map phase
#   ├─ Process (Alice, Sales)
#   └─ Create: (Sales, 1)
#
# Step 2: Executor 2 (Partition 1) - Map phase
#   ├─ Process (Charlie, Eng)
#   ├─ Process (David, Eng)
#   ├─ Process (Eve, Eng)
#   └─ Create: (Eng, 3)
#
# Step 3: SHUFFLE (expensive!)
#   ├─ Executor 1: (Sales, 1) → stays with Sales partition
#   └─ Executor 2: (Eng, 3) → stays with Eng partition
#
# Step 4: Reduce phase (after shuffle)
#   ├─ Sales partition: Combine all (Sales, 1) → (Sales, 2)
#   └─ Eng partition: Combine all (Eng, 1) → (Eng, 3)
#
# Result: count()
```

**Output**:
```
+-----+-----+
| dept| count|
+-----+-----+
|Sales|    2|
|Eng  |    3|
+-----+-----+
```

**Shuffle Details**:
```
1. HashPartitioner used (default)
   - hash(department) % numPartitions = target partition
   - hash("Sales") % 200 = 45 → Partition 45
   - hash("Engineering") % 200 = 123 → Partition 123

2. Data written to shuffle files:
   - Executor 1: writes Sales data to disk
   - Executor 2: writes Eng data to disk

3. Network transfer:
   - Executor 1 reads: shuffle data for Sales from disk
   - Executor 2 reads: shuffle data for Eng from disk

4. Final aggregation:
   - All Sales rows together → count
   - All Eng rows together → count
```

### Scenario 2: join()

```python
# Department data
dept_data = [("Sales", "Tom"), ("Eng", "Jerry")]
dept_df = spark.createDataFrame(dept_data, ["department", "manager"])
# Partitions:
# Partition 0: [(Sales, Tom), (Eng, Jerry)]

# Employee data
emp_data = [
    ("Alice", "Sales"),
    ("Bob", "Sales"),
    ("Charlie", "Eng"),
    ("David", "Eng"),
    ("Eve", "Eng"),
]
emp_df = spark.createDataFrame(emp_data, ["name", "department"])
# Partitions:
# Partition 0: [(Alice, Sales), (Bob, Sales)]
# Partition 1: [(Charlie, Eng), (David, Eng), (Eve, Eng)]

# This causes SHUFFLE (unless broadcast join):
result = emp_df.join(dept_df, emp_df.department == dept_df.department)

# Execution Flow:
# Step 1: Map phase
#   emp_df Executor 1: [(Alice, Sales), (Bob, Sales)]
#   emp_df Executor 2: [(Charlie, Eng), (David, Eng), (Eve, Eng)]
#   dept_df Executor 1: [(Sales, Tom), (Eng, Jerry)]

# Step 2: SHUFFLE (expensive!)
#   Both DataFrames shuffle on "department" key
#   emp_df:
#   ├─ (Alice, Sales) → hash(Sales) % partitions → Partition X
#   ├─ (Bob, Sales) → hash(Sales) % partitions → Partition X
#   └─ (Charlie, Eng) → hash(Eng) % partitions → Partition Y
#
#   dept_df:
#   ├─ (Sales, Tom) → hash(Sales) % partitions → Partition X
#   └─ (Eng, Jerry) → hash(Eng) % partitions → Partition Y

# Step 3: Join phase (after shuffle)
#   Partition X: [(Alice, Sales), (Bob, Sales), (Sales, Tom)]
#   ├─ Match (Alice, Sales) with (Sales, Tom) → (Alice, Sales, Tom)
#   └─ Match (Bob, Sales) with (Sales, Tom) → (Bob, Sales, Tom)
#
#   Partition Y: [(Charlie, Eng), (David, Eng), (Eve, Eng), (Eng, Jerry)]
#   ├─ Match (Charlie, Eng) with (Eng, Jerry) → (Charlie, Eng, Jerry)
#   ├─ Match (David, Eng) with (Eng, Jerry) → (David, Eng, Jerry)
#   └─ Match (Eve, Eng) with (Eng, Jerry) → (Eve, Eng, Jerry)
```

**Output**:
```
+-------+----------+-------+
|   name|department|manager|
+-------+----------+-------+
|  Alice|     Sales|    Tom|
|    Bob|     Sales|    Tom|
|Charlie|        Eng| Jerry|
|  David|        Eng| Jerry|
|    Eve|        Eng| Jerry|
+-------+----------+-------+
```

---

## How Spark Manages Shuffles

### Shuffle Files on Disk

```
During shuffle, Spark writes files:

Node 1 (Executor 0):
└─ shuffle_0_0_0.data  ← Partition 0 shuffle data
   shuffle_0_0_1.data  ← Partition 1 shuffle data
   shuffle_0_0_2.data  ← Partition 2 shuffle data
   ...
   shuffle_0_0_199.data ← Partition 199 shuffle data

Node 2 (Executor 1):
└─ shuffle_0_1_0.data  ← Partition 0 shuffle data
   shuffle_0_1_1.data  ← Partition 1 shuffle data
   ...

Node 3 (Executor 2):
└─ shuffle_0_2_0.data
   ...

[This allows cleanup via spark.shuffle.service.enabled]
```

### Shuffle Parameters

```python
spark.conf.set("spark.shuffle.partitions", 200)  # Default
# ↑ Number of partitions after shuffle
# Larger = more parallelism but more tasks
# Smaller = less parallelism but fewer tasks
# Rule of thumb: 1 task per 128MB data = 200-500 typical

spark.conf.set("spark.shuffle.compress", "true")  # Default
# ↑ Compress shuffle data to reduce network I/O

spark.conf.set("spark.shuffle.spill.compress", "true")  # Default
# ↑ Compress spilled data (when data doesn't fit in memory)

spark.conf.set("spark.shuffle.service.enabled", "false")
# ↑ External shuffle service (for dynamic allocation)
# Set true if using dynamic allocation for faster shuffles
```

---

## Reducing Shuffles: Best Practices

### Pattern 1: Avoid groupBy When Possible

```python
# ❌ SLOW - Causes shuffle
result = df.groupBy("category").agg(
    count("*").alias("count"),
    avg("price").alias("avg_price")
)

# ✅ FASTER - Use window functions (no shuffle)
from pyspark.sql.window import Window

w = Window.partitionBy("category")
result = df.select(
    "*",
    count("*").over(w).alias("count"),
    avg("price").over(w).alias("avg_price")
).distinct()

# Why faster?
# groupBy: Shuffle all data
# Window function: Works within partition if possible
```

### Pattern 2: Pre-filter Before groupBy

```python
# ❌ SLOW - Process all 1 million rows, then shuffle
result = df.groupBy("category").count()

# ✅ FASTER - Filter first (90MB → 10MB), then shuffle smaller dataset
result = df.filter(df.price > 100).groupBy("category").count()

# Difference:
# SLOW: 1GB → Shuffle 1GB → group
# FAST: 1GB → Filter (output 100MB) → Shuffle 100MB → group
# ~10x faster!
```

### Pattern 3: Use Broadcast Join Instead of Shuffle Join

```python
# ❌ SLOW - Shuffle join (both DataFrames are large)
result = large_df.join(large_df2, large_df.id == large_df2.id)
# Execution: Both DFs shuffle → join
# Time: 30 seconds
# Network I/O: 5GB + 5GB = 10GB

# ✅ FASTER - Broadcast join (small DF is broadcast)
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), large_df.id == small_df.id)
# Execution: small_df broadcast → join on each executor
# Time: 5 seconds
# Network I/O: 500MB (just the small DF)
# = 6x faster!

# When to use:
# small_df size < spark.sql.autoBroadcastJoinThreshold (10MB default)
```

### Pattern 4: Partition Before Shuffle

```python
# If you know you'll do multiple operations on same key:

# ❌ SLOW - Multiple shuffles
result1 = df.groupBy("user_id").count()      # Shuffle 1
result2 = df.groupBy("user_id").sum("amount") # Shuffle 2
result3 = df.groupBy("user_id").avg("age")   # Shuffle 3
# Total shuffles: 3

# ✅ FASTER - One shuffle, then multiple operations
df_repartitioned = df.repartition("user_id")  # Shuffle 1 (expensive)

result1 = df_repartitioned.groupBy("user_id").count()       # No shuffle
result2 = df_repartitioned.groupBy("user_id").sum("amount") # No shuffle
result3 = df_repartitioned.groupBy("user_id").avg("age")    # No shuffle
# Total shuffles: 1
# Faster if multiple operations needed
```

### Pattern 5: Coalesce Before Writing

```python
# ❌ SLOW - Writing many small files
df.write.mode("overwrite").parquet("s3://bucket/output")
# With 200 partitions = 200 files written
# Network I/O: 200 separate writes

# ✅ FASTER - Coalesce first (no shuffle, just merge)
df.coalesce(10).write.mode("overwrite").parquet("s3://bucket/output")
# With 10 partitions = 10 files written
# Network I/O: 10 writes
# = 20x faster writes

# Important: coalesce() is narrow (no shuffle)
# repartition() is wide (causes shuffle)
```

---

## Understanding Shuffle Partitions

### Default: 200 Partitions

```python
spark.conf.get("spark.shuffle.partitions")  # "200"

# How many should you use?
# Rule: ~1 task per 128MB data
#
# Example calculations:
# Data size: 10GB → 10,240 MB ÷ 128 MB = 80 tasks
# Data size: 100GB → 102,400 MB ÷ 128 MB = 800 tasks
# Data size: 1TB → 1,048,576 MB ÷ 128 MB = 8,192 tasks

# Set appropriately:
if total_data_size_mb < 10000:  # < 10GB
    spark.conf.set("spark.shuffle.partitions", 50)
elif total_data_size_mb < 100000:  # < 100GB
    spark.conf.set("spark.shuffle.partitions", 200)  # Default
else:  # > 100GB
    spark.conf.set("spark.shuffle.partitions", 500)
```

### Impact of Partition Count

```
Data: 10GB
Executor resources: 10 executors × 4 cores each = 40 cores

Scenario 1: 50 partitions (coarse-grained)
├─ Round 1: 40 tasks run in parallel (40 partitions)
├─ Round 2: 10 tasks run in parallel (10 partitions)
└─ Total rounds: 2 (not great parallelization)

Scenario 2: 200 partitions (Spark default)
├─ Round 1: 40 tasks run (40 partitions)
├─ Round 2: 40 tasks run (40 partitions)
├─ Round 3: 40 tasks run (40 partitions)
├─ Round 4: 40 tasks run (40 partitions)
├─ Round 5: 40 tasks run (40 partitions)
└─ Total rounds: 5 (good parallelization)

Scenario 3: 1000 partitions (too fine-grained)
├─ Rounds 1-25: 40 tasks run each
├─ Small tasks take longer to schedule
├─ Overhead per task: ~0.5s scheduling
├─ Total overhead: 1000 × 0.5s = 500s ❌
└─ Not recommended
```

---

## Monitoring Shuffles

### Spark UI Indicators

```
Shuffle Read:
├─ Shuffle Bytes Read: Size of data read during shuffle
├─ Shuffle Records Read: Number of records shuffled
└─ Example: 5GB read = Large shuffle (check if avoidable)

Shuffle Write:
├─ Shuffle Bytes Written: Size of data written
├─ Shuffle Records Written: Number of records written
└─ Example: 5GB written = Data needs to be rearranged

Task Duration:
├─ Long shuffle read time → Network/Disk bottleneck
├─ Long shuffle write time → Serialization bottleneck
└─ Example: 30s shuffle read = network slow
```

### Example: Analyzing Shuffle in Spark UI

```
Job 2: df.groupBy("department").count()

Stage 3 (Map stage - before shuffle):
├─ Tasks: 100 (processing 100 input partitions)
├─ Task Duration: 5 seconds average
├─ Shuffle Write: 500 MB (data to be shuffled)
└─ Status: ✅ Complete

SHUFFLE PROCESS:
├─ Writing: 500MB to disk
├─ Network transfer: 500MB across cluster
├─ Reading: 500MB from disk

Stage 4 (Reduce stage - after shuffle):
├─ Tasks: 200 (processing 200 output partitions)
├─ Task Duration: 15 seconds average (includes shuffle read time)
├─ Shuffle Read: 500 MB (from previous stage)
└─ Status: ✅ Complete

Total job time: 40 seconds (includes shuffle cost)
```

---

## Summary: When to Worry About Shuffles

### ✅ Okay (Small Shuffles)
```
- 100MB shuffle → ~1 second
- Filter before groupBy reducing data 10x
- Single broadcast join with small table
```

### ⚠️ Warning (Medium Shuffles)
```
- 1GB shuffle → ~10 seconds
- Full groupBy on all data
- Multiple joins in sequence
```

### ❌ Critical (Large Shuffles)
```
- 10GB+ shuffle → minutes
- Multiple groupBy operations on full dataset
- Unoptimized joins on large tables
- Should refactor or pre-partition
```

---

## Best Practices Checklist

- [ ] Use `df.explain()` to check for shuffles
- [ ] Filter data BEFORE groupBy/join
- [ ] Use broadcast joins when one DF < 10MB
- [ ] Set `spark.shuffle.partitions` based on data size
- [ ] Pre-partition if multiple operations on same key
- [ ] Use coalesce (not repartition) before write
- [ ] Monitor Spark UI for Shuffle Read/Write metrics
- [ ] Avoid multiple groupBy operations in sequence
- [ ] Use window functions instead of groupBy when possible
- [ ] Cache() intermediate results before multiple shuffles

---

## Interview Questions

**Q1: Why is shuffle expensive?**
A: Shuffle requires: (1) Serialization of objects, (2) Network I/O to transfer data, (3) Disk I/O to write intermediates, (4) Deserialization, (5) GC pressure. Each costs time.

**Q2: How can you reduce shuffle cost?**
A: Pre-filter data, use broadcast joins, set correct partition counts, use window functions, pre-partition on join key.

**Q3: What's the difference between narrow and wide transformations?**
A: Narrow: Output from one partition goes to one partition (no shuffle). Wide: Output from one partition goes to many partitions (requires shuffle).

**Q4: When would you use repartition vs coalesce?**
A: coalesce() when reducing partitions (narrow, no shuffle). repartition() when need to shuffle on new key or increase partitions.

**Q5: How does Spark decide the number of output partitions after shuffle?**
A: Uses `spark.shuffle.partitions` configuration (default 200). You can override with `.repartition(n)`.
