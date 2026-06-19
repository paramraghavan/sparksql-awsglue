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

### Shuffle Process (3 Phases) - Write Buffer & Read Buffer

**Write Buffer** and **Read Buffer** are the core of the shuffle operation:

```
Phase 1: WRITE (Map Side) → WRITE BUFFER
├─ Each executor accumulates output in WRITE BUFFER
├─ Buffer = In-memory hash map organized by partition key
│  └─ Partition 0 → [row1, row2, row3...]
│  └─ Partition 1 → [row4, row5, row6...]
│  └─ Partition 2 → [row7, row8, row9...]
├─ When buffer full or stage completes:
│  └─ SPILL to disk: /mnt/shuffle/app_xxx/shuffle_0_0_0.data
│  └─ (Creates shuffle write files)
├─ Why disk? → Can't fit all partitions in memory!
└─ Result: Shuffle write files on local executor disk

Phase 2: NETWORK TRANSFER (Shuffle) - Between Write & Read Buffer
├─ Shuffle Manager fetches all shuffle files
├─ Executor 0 requests: "Give me all Partition 0 data"
│  └─ Fetches from Executor 0, 1, 2, 3... disk
│  └─ Combines into READ BUFFER
├─ Executor 1 requests: "Give me all Partition 1 data"
│  └─ Fetches from Executor 0, 1, 2, 3... disk
│  └─ Combines into READ BUFFER
├─ Data moves over NETWORK (expensive!)
│  └─ Speed: ~100MB/s (vs Disk: 1GB/s vs RAM: 10GB/s)
└─ All data merged into unified READ BUFFER at destination

Phase 3: READ (Reduce Side) - READ BUFFER Processing
├─ Each executor reads from its READ BUFFER
├─ Data is now co-located by partition key:
│  └─ Executor 0 has all Partition 0 records
│  └─ Executor 1 has all Partition 1 records
│  └─ Executor 2 has all Partition 2 records
├─ Merges/sorts data if needed (sort-merge join)
├─ Performs final aggregation (groupBy, join result)
└─ Results ready for next stage

Timeline:
  Executor 0        Network         Executor 1
  ┌──────────┐      ┌───────┐       ┌──────────┐
  │WRITE BUF │─────→│FETCH  │──────→│READ BUF  │
  │ Partition│      │& MERGE│       │ Partition│
  │   0,1,2  │      └───────┘       │   0,1,2  │
  └──────────┘                      └──────────┘
  (All executors do this for their assigned partitions)
```

### Visual: Write Buffer → Shuffle Files → Read Buffer

```
EXECUTOR 0 (Map Phase):
  Input: Row1(key=A), Row2(key=B), Row3(key=A)
    ↓
  WRITE BUFFER (in-memory):
    A: [Row1, Row3]
    B: [Row2]
    ↓ (when full or done)
  WRITE to Disk: shuffle_0_0_0.data

EXECUTOR 1 (Map Phase):
  Input: Row4(key=B), Row5(key=C)
    ↓
  WRITE BUFFER (in-memory):
    B: [Row4]
    C: [Row5]
    ↓ (when full or done)
  WRITE to Disk: shuffle_1_0_0.data

NETWORK TRANSFER:
  Shuffle Manager collects:
    └─ All A records from all executors
    └─ All B records from all executors
    └─ All C records from all executors

EXECUTOR 0 (Reduce Phase) - READ BUFFER:
  Receives all A records (from all executors)
    A: [Row1, Row3]  ← From Executor 0
    A: [...]         ← From Executor 2
    A: [...]         ← From Executor 3
    ↓
  READ BUFFER now has ALL A records aggregated
    ↓
  Processes: sum/count/etc on all A records
    ↓
  Output: A=2 (or whatever aggregation)

EXECUTOR 1 (Reduce Phase) - READ BUFFER:
  Receives all B records (from all executors)
    B: [Row2]       ← From Executor 0
    B: [Row4]       ← From Executor 1
    B: [...]        ← From other executors
    ↓
  READ BUFFER now has ALL B records aggregated
    ↓
  Processes: sum/count/etc on all B records
    ↓
  Output: B=3 (or whatever aggregation)

EXECUTOR 2 (Reduce Phase) - READ BUFFER:
  Receives all C records (from all executors)
    C: [Row5]       ← From Executor 1
    ↓
  READ BUFFER now has ALL C records
    ↓
  Processes: sum/count/etc on all C records
    ↓
  Output: C=1 (or whatever aggregation)
```

### Memory Pressure & Spill Connection

**Why does spill happen?**
```
WRITE BUFFER problem:
├─ Too much data for WRITE BUFFER
├─ Buffer: 4GB (executor memory)
├─ Incoming: 10GB of data per executor
├─ Solution: Spill WRITE BUFFER to disk
└─ Result: Slow disk I/O

READ BUFFER problem:
├─ Too many records from remote executors
├─ READ BUFFER can't hold all of them
├─ Solution: Spill READ BUFFER to disk temporarily
└─ Result: Read from disk instead of memory
```

**Example: groupBy with spill**
```
groupBy("user_id") on 100GB data:
├─ WRITE phase:
│  ├─ Executor creates WRITE BUFFER
│  ├─ Accumulates all user_id groups
│  ├─ Buffer fills up (4GB full)
│  └─ SPILLS remaining data to disk → "Memory Spill: 10GB"
│
├─ NETWORK phase:
│  ├─ Shuffle Manager transfers data
│  └─ Network I/O happens
│
└─ READ phase:
   ├─ Executor reads WRITE BUFFER data for its partition
   ├─ Buffer fills up (4GB full)
   └─ SPILLS to disk → "Disk Spill: 8GB"

Result: "Memory Spill: 10GB, Disk Spill: 8GB" (from Spark UI)
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

## Joins in Spark - Choosing the Right Strategy

### What is a Join?

**Join** = Combining two DataFrames on a common key

```python
df1 = spark.read.parquet("customers.parquet")    # 10GB, 1M rows
df2 = spark.read.parquet("orders.parquet")       # 100GB, 50M rows

result = df1.join(df2, "customer_id")            # Combine on customer_id
```

### Join Types (SQL Standard)

```
INNER JOIN:   Only matching rows (default)
LEFT JOIN:    All from left + matching from right
RIGHT JOIN:   All from right + matching from left
FULL JOIN:    All rows from both
CROSS JOIN:   Cartesian product (very expensive!)
```

### When Do Joins Trigger Shuffle?

```python
# ❌ SHUFFLES DATA (expensive)
df1.join(df2, "customer_id")
# Spark must move data to align by customer_id

# ✅ NO SHUFFLE (if already partitioned)
df1_partitioned = df1.repartition(200, "customer_id")
df2_partitioned = df2.repartition(200, "customer_id")
result = df1_partitioned.join(df2_partitioned, "customer_id")
# Data already aligned = co-located join
```

### Join Strategies in Spark

Spark chooses the best strategy automatically, but you should understand each:

#### 1. **Broadcast Hash Join** (Best for small + large)

```
When Spark uses it:
├─ Small DF < spark.sql.autoBroadcastJoinThreshold (default 10MB)
├─ OR you explicitly broadcast: df.join(broadcast(small_df), key)
└─ User triggers it manually

How it works:
├─ Small DF is broadcast to all executors (memory)
├─ Large DF streams through, looks up in broadcast
├─ NO SHUFFLE needed!
└─ Very fast!

Performance:
├─ Time: < 1 second for 10MB * 100GB
├─ Shuffle: 0 (no network transfer!)
└─ Best possible join strategy

Config:
  spark.sql.autoBroadcastJoinThreshold = 10MB (can increase to 100MB)

Example:
  df_large = spark.read.parquet("100gb_orders.parquet")
  df_small = spark.read.parquet("10mb_lookup.parquet")
  result = df_large.join(broadcast(df_small), "key")  # < 1 second!
```

#### 2. **Sort-Merge Join** (Default for large + large)

```
When Spark uses it:
├─ Both DFs > broadcast threshold
├─ Default strategy for large joins
└─ Both DFs must be sorted on join key (happens automatically)

How it works:
Phase 1: SHUFFLE - Partition both DFs by join key
  ├─ DF1: 100GB → 200 partitions
  ├─ DF2: 100GB → 200 partitions
  └─ Network transfer happens here (expensive!)

Phase 2: SORT - Sort each partition locally
  ├─ Partition 0 (DF1) sorted by key
  ├─ Partition 0 (DF2) sorted by key
  └─ Merge-sort pattern (two pointers)

Phase 3: MERGE - Join co-located partitions
  ├─ Partition 0 (DF1) merged with Partition 0 (DF2)
  ├─ Both already sorted
  └─ Merge is O(n) instead of O(n²)

Performance:
├─ Time: 10-60 seconds for 100GB * 100GB
├─ Shuffle: ~200GB network transfer
├─ Best for large-large joins when order needed

Config:
  spark.sql.join.preferSortMergeJoin = true
```

#### 3. **Shuffle Hash Join** (Rarely used)

```
When Spark uses it:
├─ Both DFs > broadcast threshold
├─ If sort-merge not preferred
├─ Memory-heavy but parallelizable

How it works:
├─ Shuffle both DFs by join key
├─ Build hash table in memory for left DF
├─ Probe with right DF rows
└─ Hash lookup: O(1) per row

Performance:
├─ Fast lookups but high memory use
├─ Can cause spill if hash tables too large
└─ Less common now (sort-merge usually better)

Config:
  spark.sql.join.preferSortMergeJoin = false
```

#### 4. **Cartesian Join** (Avoid at all costs!)

```
When it happens:
├─ No join condition specified
├─ OR join condition doesn't use equality
└─ df1.join(df2)  # No ON clause!

What it does:
├─ Combines EVERY row from DF1 with EVERY row from DF2
├─ 1M rows * 50M rows = 50 BILLION rows!
├─ Result: 50B * 1KB per row = 50 TB!
└─ CRASH!

Example:
  # ❌ WRONG - Cartesian product!
  df1.join(df2)  # Missing join condition

  # ✅ CORRECT - Use equality condition
  df1.join(df2, "customer_id")
```

### Join Strategy Selection Rules

```
Rule 1: Check DF sizes first
├─ Small (< 100MB): Use broadcast join
├─ Medium (100MB-10GB): Consider broadcast
├─ Large (> 10GB): Use sort-merge join

Rule 2: Broadcast when possible
  if small_df.size < 100MB:
    result = large_df.join(broadcast(small_df), key)
    # Best performance: ~1 second

Rule 3: Pre-partition before large joins
  df1_part = df1.repartition(200, "join_key")
  df2_part = df2.repartition(200, "join_key")
  result = df1_part.join(df2_part, "join_key")
  # Co-located join = no shuffle

Rule 4: Filter before joining
  df1_filtered = df1.filter(col("active") == True)  # 100GB → 50GB
  df2_filtered = df2.filter(col("valid") == True)   # 100GB → 70GB
  result = df1_filtered.join(df2_filtered, "key")
  # Smaller join = faster

Rule 5: Avoid joins on high-cardinality columns
  # ❌ BAD: Join on timestamp (millions of unique values)
  df1.join(df2, "timestamp")  # Creates 200+ partitions each!

  # ✅ GOOD: Join on customer_id (millions but grouped)
  df1.join(df2, "customer_id")  # Natural grouping
```

### Join Optimization Examples

#### Before: Unoptimized Join

```python
# Reading large files
df_customers = spark.read.parquet("100gb_customers.parquet")  # 100GB
df_orders = spark.read.parquet("500gb_orders.parquet")       # 500GB

# Join without optimization
result = df_customers.join(df_orders, "customer_id")
result.write.parquet("output/")

# What happens:
├─ Read both: 100GB + 500GB
├─ Shuffle both: ~600GB network transfer
├─ Sort both: CPU intensive
├─ Join: Merge-sort operation
├─ Write: 600GB output
└─ Total time: 20-30 minutes, 1.2TB network

Metrics:
├─ Shuffle spill: 50GB
├─ GC pauses: 30+ seconds
└─ Network utilization: 80%
```

#### After: Optimized Join

```python
# Pre-optimization
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Read and filter
df_customers = spark.read.parquet("100gb_customers.parquet") \
    .filter(col("active") == True)  # 100GB → 30GB

df_orders = spark.read.parquet("500gb_orders.parquet") \
    .filter(col("status") == "completed")  # 500GB → 200GB

# Check size - can we broadcast?
if df_customers.estimatedSize < 100MB:
    # Broadcast small table
    result = df_orders.join(broadcast(df_customers), "customer_id")
else:
    # Pre-partition on join key
    df_cust_part = df_customers.repartition(200, "customer_id")
    df_order_part = df_orders.repartition(200, "customer_id")
    result = df_cust_part.join(df_order_part, "customer_id")

result.write.parquet("output/")

# What happens:
├─ Filter first: 100GB + 500GB → 30GB + 200GB
├─ Broadcast (or pre-partition): Minimal shuffle
├─ Join: Efficient sort-merge
├─ Write: 230GB output
└─ Total time: 2-3 minutes (10x faster!)

Metrics:
├─ Shuffle spill: 0GB
├─ GC pauses: < 1 second
└─ Network utilization: 20%
```

### Join Anti-Patterns

```python
# ❌ BAD: Multiple joins in sequence
result = df1.join(df2, "key1") \
            .join(df3, "key2") \
            .join(df4, "key3")
# Result: 3 shuffles!

# ✅ GOOD: Pre-partition on all join keys
df1_part = df1.repartition(200, "key1", "key2", "key3")
df2_part = df2.repartition(200, "key1")
df3_part = df3.repartition(200, "key2")
df4_part = df4.repartition(200, "key3")
result = df1_part.join(df2_part, "key1") \
                 .join(df3_part, "key2") \
                 .join(df4_part, "key3")
# Result: Pre-shuffle once, then join 3 times

# ❌ BAD: Join after distinct/groupBy
df1.distinct().join(df2, "id")  # Shuffles for distinct, then join

# ✅ GOOD: Join first, then distinct
df1.join(df2, "id").distinct()  # One shuffle (join), one for distinct

# ❌ BAD: No join condition (Cartesian!)
df1.join(df2)  # Combines every row!

# ✅ GOOD: Always specify join condition
df1.join(df2, "customer_id")  # Clear, intentional join

# ❌ BAD: Join large tables without filtering
df_large1.join(df_large2, "key")  # Both 500GB!

# ✅ GOOD: Filter before joining
df_large1.filter(col("active") == True) \
         .join(df_large2.filter(col("valid") == True), "key")
         # 500GB → 200GB each!
```

### Join Configuration Reference

```python
# Broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
# Default 10MB - increase if you have memory

# Prefer sort-merge
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
# Default true (recommended)

# Shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
# Affects join partitions too

# Adaptive join selection (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Automatically handles skewed joins

# Skew join config
spark.conf.set("spark.sql.adaptive.skewJoin.skewFactor", "5.0")
# Flag partitions > 5x average size as skewed
```

### Join Performance Checklist

```
Before joining:
☑️ Check both DF sizes
☑️ Can I broadcast (< 100MB)?
☑️ Should I pre-partition on join key?
☑️ Should I filter before joining?
☑️ Is join key low-cardinality?

During join:
☑️ Monitor Spark UI for shuffle metrics
☑️ Check for data skew (uneven partition sizes)
☑️ Look for GC pauses (memory pressure)

After join:
☑️ Verify output data correctness
☑️ Check if spill occurred (means undersized executors)
☑️ Compare shuffle metrics to previous run
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
