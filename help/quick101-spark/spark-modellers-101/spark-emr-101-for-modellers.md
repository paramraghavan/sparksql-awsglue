# Spark on AWS Glue/EMR - Quick Guide for Modellers

## Why Spark Instead of Pandas?

```
Pandas (Single Node):
├─ 500GB data = CRASH (node only has 64GB)
└─ Can't scale

Spark (Distributed):
├─ 500GB split across 6 nodes = 83GB/node ✓
├─ Parallel processing = 10-100x faster
└─ Can scale to 1000+ nodes
```

---

## The Memory Spill Problem (Critical!)

**When does it happen?**
- Reading 100GB with groupBy/join
- Data exceeds executor memory (4-8GB)
- Spark writes to disk (100x slower!)

**Example**:
```
100GB data ÷ 10 executors = 10GB per executor
Executor memory: 4GB ← FULL!
Spill to disk: 6GB ← SLOW!
Duration: 5-10 minutes ✗
```

**How to fix**:
1. **Upgrade worker type**: G.2X → G.4X (4GB → 8GB per executor)
2. **Add more nodes**: More executors = less data per executor
3. **Filter before operations**: `filter() → groupBy()` (not the reverse)
4. **Increase shuffle partitions**: Distribute work better

See: [disk_memory_spill.md](disk_memory_spill.md) for detailed explanation

---

## How Spark Reads Files (File Splits)

**Default behavior**:
```
100GB file → 128MB chunks → ~780 partitions
Each executor reads 128MB at a time ✓ (no spill during read)

Problem comes during SHUFFLE (groupBy, join) not reading!
```

**Key insight**:
- Reading large files is fine (split into 128MB chunks)
- Problems occur during wide transformations (shuffle operations)

See: [how_spark_read_files.md](how_spark_read_files.md) for detailed explanation

---

## Quick Comparison: Executor Memory & Performance

| Memory | Data Handling | Duration | Spill | Best For |
|--------|---------------|----------|-------|----------|
| 4GB | Tight | 5-10 min | 50GB ✗ | Small clusters |
| **8GB** | **Just right** | **1-2 min** | **~0GB** | **Most jobs** |
| 16GB | Plenty | 30 sec | 0GB | Large data |

**How to get 8GB** (Recommended):
1. Open AWS Glue Console
2. Change worker type: **G.2X → G.4X**
3. Cost: +50% but 5x faster (net savings!)

---

## Cluster Architecture Essentials

```
AWS Glue Job Configuration:
├─ 5 Worker Nodes (G.4X = m5.2xlarge)
├─ Memory per node: 32GB
├─ Executors per node: 2
├─ Memory per executor: 8GB ← THIS IS IMPORTANT!
├─ Total executors: 10
└─ Total parallel tasks: 40 (if 4 cores per executor)

KEY: 1 JVM per executor (NOT per core)
  Each JVM has multiple threads (one per core)
  All threads share memory pool
```

---

## 🟢 Beginner: Key Concepts

### 1. Lazy Evaluation
```python
df.read.csv(...)          # Lazy - nothing happens
df.filter(...)            # Lazy - queued
df.select(...)            # Lazy - queued
df.groupBy(...).count()   # Lazy - returns DataFrame
df.show()                 # ACTION - NOW it executes!
```

**Rule**: Transformations are lazy, Actions are eager

### 2. Transformations vs Actions
```
Transformations (Lazy):
  - select, filter, groupBy, join, repartition
  - Don't execute until action

Actions (Eager):
  - show, count, collect, write
  - Execute immediately
```

### 3. Jobs, Stages, Tasks
```
One Action triggers ONE Job
↓
Job breaks into Stages (separated by wide transformations)
  - Wide: groupBy, join, repartition (need shuffle)
  - Narrow: select, filter (no shuffle)
↓
Stages break into Tasks (one per partition)
  - Each task runs in parallel on executors
```

### 4. Small File Problem
```
❌ BAD: 1000 small files → 1000 partitions
        1000 tasks × scheduling overhead = SLOW

✅ GOOD: 1000 small files → coalesce(50) → 50 partitions
         50 tasks = much faster!

FIX: df.coalesce(50)  # Merge partitions (no shuffle)
    NOT: df.repartition(50)  # Shuffles (slow!)
```

---

## 🟡 Advanced: Deep Dive

### Memory Breakdown Per Node

```
Node Memory (32GB G.4X):
├─ OS/System: 2GB
├─ Spark overhead: 2GB
├─ Executor JVM: 8GB × 2 executors
├─ Buffer: 12GB
└─ Total: 32GB

Per Executor (8GB):
├─ Spark execution memory: 4GB (shuffle, join, groupBy)
├─ Spark storage memory: 2GB (cache)
├─ JVM overhead: 2GB
└─ Total: 8GB
```

### Task Scheduling Math

```
Data: 100GB
Executors: 10 (4 cores each = 40 parallel slots)
Per executor: 100GB ÷ 10 = 10GB

With groupBy (shuffle operation):
├─ Input partitions: ~100GB ÷ 128MB = 780
├─ Output partitions: 200 (from spark.sql.shuffle.partitions)
├─ Shuffle write tasks: 780 (each writes data)
├─ Shuffle read tasks: 200 (each reads grouped data)

Execution timeline:
├─ Read: 780 ÷ 40 slots = 19.5 waves ✓
├─ Shuffle write: 780 ÷ 40 = 19.5 waves (disk I/O!)
├─ Shuffle read: 200 ÷ 40 = 5 waves
└─ Total: ~45 waves × seconds per wave = minutes
```

### File Split Details

```
Reading 100GB:
├─ Default split: 128MB
├─ Partitions: 100GB ÷ 128MB = 781
├─ NOT affected by spark.sql.shuffle.partitions
└─ Read tasks: 781

After groupBy (shuffle):
├─ Shuffle partitions: 200 (default)
├─ GROUP BY creates: 200 output partitions
├─ Write tasks: 781 (map phase)
├─ Read tasks: 200 (reduce phase)
└─ This is where SPILL happens!
```

---

## Spark Shuffle - The Expensive Operation

### What is Shuffle?

**Shuffle** = Moving data between executors to group/sort it

```
Data needs to go from:
Executor 1 → Executor 2 (via network)
Executor 2 → Executor 3 (via network)
Executor 3 → Executor 1 (via network)

This network I/O is expensive!
```

### When Does Shuffle Happen?

Shuffle occurs during **wide transformations**:

```python
df.groupBy("user_id")          # Shuffle: Group by key
df.join(other_df, "key")       # Shuffle: Align by key
df.repartition(200)            # Shuffle: Redistribute data
df.sort("column")              # Shuffle: Sort across partitions
df.distinct()                  # Shuffle: Remove duplicates
```

**NOT during narrow transformations**:
```python
df.select(...)                 # No shuffle
df.filter(...)                 # No shuffle
df.withColumn(...)             # No shuffle
```

### Shuffle Process (3 Phases)

```
Phase 1: WRITE (Map Side)
├─ Each executor writes its data to local disk
├─ Data organized by partition key
├─ Creates shuffle write files
└─ File: /mnt/shuffle/app_xxx/shuffle_0_0_0.data

Phase 2: NETWORK TRANSFER (Shuffle)
├─ Fetch shuffle files from remote executors
├─ Data moves over network
├─ Bottleneck! (Network is slower than disk/RAM)
└─ Time: 100MB/s vs Disk 1GB/s vs RAM 10GB/s

Phase 3: READ (Reduce Side)
├─ Each executor reads its shuffled data
├─ Merges/sorts if needed
├─ Performs final aggregation
└─ Results ready for next stage
```

### Shuffle Strategies in Spark

Spark has multiple shuffle implementations. The strategy chosen affects performance:

#### 1. **Sort-Merge Shuffle** (Default since Spark 2.0)

```
How it works:
├─ Write phase: Sort and write data to disk
├─ Network: Transfer sorted shuffle files
├─ Read phase: Merge-sort from multiple executors
└─ Result: Data arrives pre-sorted and merged

Best for:
├─ Large datasets
├─ When output must be sorted
└─ Memory-constrained executors

Cons:
├─ Sorting overhead
└─ Larger shuffle files

Config:
  spark.shuffle.manager = sort
```

#### 2. **Hash Shuffle** (Legacy, Spark < 2.0)

```
How it works:
├─ Write phase: Hash-partition data (no sorting)
├─ Network: Transfer hash-partitioned files
├─ Read phase: Directly aggregate
└─ Result: Data unsorted but organized by partition

Best for:
├─ Small datasets
├─ When sorting not needed
└─ Fast aggregations (groupBy, join)

Cons:
├─ Many small files created (one per reducer)
├─ File consolidation overhead
└─ Deprecated (not recommended)
```

#### 3. **Tungsten Shuffle** (Off-Heap Memory, Spark 1.5+)

```
How it works:
├─ Uses off-heap memory for shuffle
├─ Reduces GC pressure
├─ Direct buffer allocation
└─ More efficient serialization

Best for:
├─ Reducing garbage collection pauses
├─ Large shuffles with memory pressure
└─ Performance-critical jobs

Enabled by:
  spark.memory.offHeap.enabled = true
  spark.memory.offHeap.size = 2GB
```

### Shuffle Configuration & Optimization

#### Key Configs

```python
# Default shuffle partitions (critical!)
spark.sql.shuffle.partitions = 200
# Rule: 1 partition per 100-200MB of shuffled data

# Bypass merge-sort threshold
spark.shuffle.sort.bypassMergeThreshold = 200
# If reducers <= 200, use hash shuffle (faster!)

# Shuffle file consolidation
spark.shuffle.consolidateFiles = true
# Merge shuffle files to reduce file count

# Off-heap memory
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 2GB
# Reduces GC pause time

# Shuffle spill settings
spark.shuffle.spill = true
# Allow spilling to disk if memory full

spark.shuffle.spill.numPartitionsToTry = 100
# Try N partitions before falling back to disk spill
```

#### Optimization Strategy

```
Rule 1: Match shuffle partitions to data size
├─ Too few (< 50): Big partitions = potential spill
├─ Too many (> 500): Small partitions = overhead
└─ Sweet spot: 100-200 partitions per 100GB

Rule 2: Use bypass merge-sort for small reducers
├─ If reducers < 200: Set bypassMergeThreshold = reducers
├─ Avoids sorting overhead
└─ Config: spark.shuffle.sort.bypassMergeThreshold

Rule 3: Enable off-heap memory
├─ Reduces GC pauses
├─ Better for large shuffles
└─ Config: spark.memory.offHeap.enabled = true

Rule 4: Consolidate shuffle files
├─ Reduces file system pressure
├─ Better for Hadoop NameNode
└─ Config: spark.shuffle.consolidateFiles = true
```

### Example: Optimization Before & After

```python
# BEFORE: Unoptimized shuffle
df = spark.read.parquet("100gb_data.parquet")
result = df.groupBy("user_id").agg(sum("amount"))
result.write.parquet("output/")

# Metrics:
# ├─ Shuffle partitions: 200 (default)
# ├─ Shuffle spill: 20GB (memory pressure)
# ├─ Shuffle duration: 5 minutes
# └─ Shuffle files created: 200 * 2 = 400

# AFTER: Optimized shuffle
spark.conf.set("spark.sql.shuffle.partitions", "400")  # More partitions
spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "400")
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2GB")
spark.conf.set("spark.shuffle.consolidateFiles", "true")

df = spark.read.parquet("100gb_data.parquet") \
    .filter(col("amount") > 0)  # Reduce data before shuffle
result = df.groupBy("user_id").agg(sum("amount"))
result.write.parquet("output/")

# Metrics:
# ├─ Shuffle partitions: 400 (more parallelism)
# ├─ Shuffle spill: 2GB (minimal)
# ├─ Shuffle duration: 1.5 minutes (3x faster!)
# └─ Shuffle files created: 400 consolidated to ~50
```

### Shuffle vs Spill (Don't Confuse!)

```
SHUFFLE = Normal operation
├─ Moving data between executors
├─ Controlled by spark.sql.shuffle.partitions
├─ Expected in wide transformations
└─ Network I/O involved

SPILL = Emergency fallback
├─ Executor memory is full
├─ Writes to disk to free memory
├─ Indicates undersized executors
├─ Disk I/O involved (100x slower!)
└─ BAD - means you need more memory

SHUFFLE without SPILL = Normal (acceptable)
SHUFFLE with SPILL = Problem (need to fix)
```

### Shuffle Anti-Patterns

```python
# ❌ BAD: Multiple shuffles
df.repartition(100)           # Shuffle 1
   .groupBy("id").count()     # Shuffle 2
   .join(other_df, "id")      # Shuffle 3
# Result: 3 shuffles = slow!

# ✅ GOOD: Plan to minimize shuffles
df.repartition(100, "id")     # One shuffle on join key
   .groupBy("id").count()     # No extra shuffle (already partitioned!)
   .join(other_df, "id")      # No shuffle (data already aligned!)
# Result: 1 shuffle = fast!

# ❌ BAD: Shuffle before filtering
df.groupBy("user_id")         # Shuffle all 100GB
   .count()
   .filter(col("count") > 10) # Filter after (too late!)

# ✅ GOOD: Filter before shuffle
df.filter(col("transactions") > 0)  # Reduce to 50GB
   .groupBy("user_id")               # Shuffle smaller data
   .count()
# Result: Less data to shuffle = faster!
```

---

## Common Patterns & Solutions

### Pattern 1: Multiple Aggregations on Same Data

```python
# ✅ GOOD: Read once, reuse
df = spark.read.csv("s3://bucket/data.csv") \
    .select("user_id", "amount", "date")  # Reduce size

# Multiple operations on same DF
result1 = df.groupBy("user_id").agg(sum("amount"))
result2 = df.groupBy("date").agg(count("*"))
result3 = df.groupBy("user_id", "date").agg(sum("amount"))

# Write all results
result1.write.parquet("s3://bucket/output/result1")
result2.write.parquet("s3://bucket/output/result2")
result3.write.parquet("s3://bucket/output/result3")
```

### Pattern 2: Many Small Files

```python
# ❌ BAD: 1000 files → 1000 tasks
df = spark.read.csv("many_files/")
df.write.parquet("output/")  # 1000 write tasks = slow

# ✅ GOOD: Coalesce first
df = spark.read.csv("many_files/").coalesce(50)
df.write.parquet("output/")  # 50 write tasks = fast
```

### Pattern 3: Pre-Filter to Avoid Spill

```python
# ❌ BAD: Group all 100GB
df.groupBy("user_id").agg(sum("amount"))  # Spill!

# ✅ GOOD: Filter first, group smaller data
df.filter(col("amount") > 0) \
  .groupBy("user_id").agg(sum("amount"))  # No spill
```

---

## Detecting & Fixing Problems

### In Spark UI (http://driver-ip:4040)

```
Good Stage:
├─ Spill (Memory): 0B ✓
├─ Spill (Disk): 0B ✓
├─ Duration: < 1 minute ✓
└─ Status: SUCCESS

Bad Stage:
├─ Spill (Memory): 19GB ✗
├─ Spill (Disk): 15GB ✗
├─ Duration: 5+ minutes ✗
└─ Fix: Upgrade worker type or add nodes
```

### Diagnosis Checklist

```
Slow job?
├─ Check Stage with highest Spill > 0
│  └─ Fix: Increase executor memory or add nodes
├─ Check Stage with many tasks (>1000)
│  └─ Fix: Use coalesce() to reduce partitions
├─ Check longest running stage
│  └─ Analyze: May be data skew or join issue
└─ Check Shuffle metrics
   └─ Fix: Increase shuffle partitions if tasks are too large
```

---

## 🎯 Decision Tree: What to Do When Job is Slow

```
Is there Spill (Memory/Disk > 0)?
├─ YES → Go to "Fix Spill"
└─ NO → Check stage duration

Is stage duration > 5 minutes?
├─ YES → Check shuffle data size
│  ├─ Large (>50GB) → Increase executor memory
│  └─ Small but many tasks → Coalesce partitions
└─ NO → Job is okay

To Fix Spill:
├─ Option 1 (Quick): Upgrade worker type G.2X → G.4X
├─ Option 2 (Scale): Add more nodes
├─ Option 3 (Code): Filter before groupBy/join
└─ Option 4 (Advanced): Increase shuffle partitions & tune configs
```

---

## Quick Config Reference

```
# For 100GB data with 4-5 minute SLA
spark.executor.memory = 8GB           # At least this
spark.sql.shuffle.partitions = 200    # Default is fine
spark.sql.files.maxPartitionBytes = 128MB  # Read splits

# For expensive groupBy/join
spark.sql.shuffle.partitions = 400    # More parallelism
spark.shuffle.sort.bypassMergeThreshold = 200  # Optimize

# For many small files
df.coalesce(50)  # Reduce partitions before write
```

---

## Key Takeaways

✅ **Do**:
- Use G.4X worker type for 100GB+ jobs
- Filter before groupBy/join
- Coalesce many small files
- Monitor Spark UI Stages tab
- Cache expensive transformations

❌ **Don't**:
- Leave 1000+ partitions from small files
- Use repartition() to merge (use coalesce!)
- GroupBy on high-cardinality column without filtering
- Ignore spill warnings
- Join without checking data skew

---

## See Also

- [disk_memory_spill.md](disk_memory_spill.md) - Detailed spill explanation with 6 prevention guidelines
- [how_spark_read_files.md](how_spark_read_files.md)- File reading, splits, and partition strategy
- [simple_spark_101_for_modellers.md](simple_spark_101_for_modellers.md) - Simple version for spark modellers
