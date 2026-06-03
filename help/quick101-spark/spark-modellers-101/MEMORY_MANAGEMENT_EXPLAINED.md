# Spark Memory Management: Understanding the Memory Hierarchy

> **Key Takeaway**: Spark executor memory is split into storage (caching) and execution (processing). Understanding this split prevents OutOfMemory errors.

---

## Quick Answer

| Memory Type | Size | Purpose | Example |
|---|---|---|---|
| **Executor Memory Total** | `spark.executor.memory` (e.g., 4GB) | Everything for one executor | `-Xmx4G` JVM heap |
| **Spark Memory** | 60% of executor memory | Storage + Execution | 2.4GB of 4GB |
| **Storage Memory** | 50% of Spark Memory | Caching with .cache() | 1.2GB for cache |
| **Execution Memory** | 50% of Spark Memory | Shuffle buffers, aggregations | 1.2GB for shuffle |
| **User Memory** | 40% of executor memory | User code, libraries | 1.6GB for custom code |
| **Reserved Memory** | Fixed 300MB | System, networking | Can't use it |

**Total: 4GB = 2.4GB (Spark: 1.2GB Storage + 1.2GB Execution) + 1.6GB (User) + 0.3GB (Reserved)**

---

## The Memory Structure

### Visual: Executor Memory Layout

```
Executor Memory: 4GB (spark.executor.memory)
├─────────────────────────────────────────────────────┐
│ Reserved Memory: 300MB (Fixed)                      │
│ └─ System overhead, networking, etc.                │
├─────────────────────────────────────────────────────┤
│ User Memory: 1.6GB (40% of usable memory)           │
│ ├─ Your custom objects                              │
│ ├─ Python libraries                                 │
│ ├─ Map/reduce structures created by user code       │
│ └─ Third-party libraries                            │
├─────────────────────────────────────────────────────┤
│ Spark Memory: 2.4GB (60% of usable memory)          │
│ ├────────────────────────────────────────────────┐  │
│ │ Storage Memory: 1.2GB (50% of Spark Memory)   │  │
│ │ ├─ cache() / persist() results                │  │
│ │ ├─ broadcast() variables                      │  │
│ │ └─ Other cached data structures               │  │
│ └────────────────────────────────────────────────┘  │
│ ├────────────────────────────────────────────────┐  │
│ │ Execution Memory: 1.2GB (50% of Spark Memory)│  │
│ │ ├─ Shuffle buffers (during sort/join)        │  │
│ │ ├─ Hash aggregations (during groupBy)        │  │
│ │ ├─ Hash table for broadcast join             │  │
│ │ └─ Other temporary structures                │  │
│ └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Memory Configuration Example

```python
# Set executor memory to 4GB
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 4) \
    .appName("MemoryDemo") \
    .getOrCreate()

# Actual memory breakdown:
# Total: 4GB
# Reserved (fixed): 0.3GB
# Usable: 3.7GB

# Spark Memory (60% of usable):
spark_memory = 3.7 * 0.6 = 2.22GB

# Storage Memory (50% of spark memory):
storage = 2.22 * 0.5 = 1.11GB

# Execution Memory (50% of spark memory):
execution = 2.22 * 0.5 = 1.11GB

# User Memory (40% of usable):
user = 3.7 * 0.4 = 1.48GB

# TOTAL CHECK: 1.11 + 1.11 + 1.48 + 0.3 = 4.0GB ✓
```

---

## Storage Memory

### Purpose: Caching Data

**When is storage memory used?**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CacheDemo").getOrCreate()

# Read large dataset
df = spark.read.csv("huge_file.csv", header=True)

# BEFORE cache - no storage memory used
print("Storage memory used: 0MB")

# Cache the data
df.cache()  # Marks for caching, but doesn't load yet

print("Storage memory used: Still 0MB (lazy!)")

# Trigger an action - NOW it loads into cache
result = df.count()

print("Storage memory used: 5GB (data is cached!)")

# Second action - uses cache (much faster!)
result2 = df.filter(col("age") > 30).count()
# ✅ Fast - loads from cache, not from disk

# Third action
result3 = df.groupBy("department").count().collect()
# ✅ Fast - loads from cache
```

**Visual: Caching Process**

```
Execution Timeline:

Before cache:
├─ df.cache() called
├─ df.count() → Reads from CSV (300 seconds)
│  └─ Data loaded into storage memory (5GB)
├─ df.show() → Reads from cache (1 second)
└─ df.groupBy().count() → Reads from cache (2 seconds)

Without cache:
├─ df.count() → Reads from CSV (300 seconds)
├─ df.show() → Reads from CSV again (300 seconds) ❌
└─ df.groupBy().count() → Reads from CSV again (300 seconds) ❌

Total WITHOUT cache: 900 seconds
Total WITH cache: 303 seconds
Speedup: 3x (if you do 3+ operations)
```

### Cache Trade-offs

```python
# ✅ Good candidate for cache:
# - Large dataset
# - Used multiple times
# - Long to compute
# - Fits in memory

large_df = spark.read.parquet("s3://bucket/data.parquet")
large_df.cache()  # Size: 5GB, fits in executor memory

result1 = large_df.groupBy("dept").count()
result2 = large_df.filter(col("salary") > 50k).count()
result3 = large_df.select("name").distinct()
# ✅ Cache pays off: 3 operations all use it

# ❌ Bad candidate for cache:
# - Small dataset
# - Used once
# - Already in memory
# - Doesn't fit

tiny_df = spark.read.parquet("s3://bucket/small.parquet")  # 100MB
tiny_df.cache()  # Wastes cache space!

result = tiny_df.count()
# ❌ Cache doesn't help: only 1 operation

# ❌ Doesn't fit:
huge_df = spark.read.parquet("s3://bucket/huge.parquet")  # 100GB
huge_df.cache()  # Executor memory only 4GB!

# Result: Spilling (writes to disk)
# Storage memory: 4GB (filled)
# Execution: Starts overflowing to disk
# Performance: Slow (disk I/O)
```

---

## Execution Memory

### Purpose: Temporary Data During Operations

**What uses execution memory?**

```python
# 1. SHUFFLE BUFFERS - During sort/join operations
df1 = spark.read.csv("large_file.csv")
df2 = spark.read.csv("other_file.csv")

result = df1.join(df2, "id")  # Uses execution memory for shuffle buffers
# Buffer: Holds data before writing to disk
# Size: Can be multiple GB during large joins

# 2. HASH AGGREGATIONS - During groupBy
df = spark.read.csv("data.csv")
result = df.groupBy("category").agg(sum("amount"))
# Hash table: Holds intermediate aggregations
# Size: Depends on number of unique categories

# 3. BROADCAST VARIABLES - Hashed lookup tables
from pyspark.sql.functions import broadcast

small_df = spark.read.csv("small_lookup.csv")  # 100MB
large_df = spark.read.csv("large_data.csv")

result = large_df.join(broadcast(small_df), "id")
# Broadcast table hashed: Uses execution memory
# Size: 100MB → ~150MB (hashed version)

# 4. SORT OPERATIONS - Buffering during sort
df = spark.read.csv("data.csv")
result = df.sort("name")
# Sort buffer: Holds rows being sorted
# Size: Can grow large for multi-stage sorts
```

### Execution Memory Pressure Example

```
Executor Memory: 4GB
├─ Reserved: 0.3GB
├─ User: 1.5GB
├─ Storage: 1.1GB (cache)
└─ Execution: 1.1GB ← Focus here

Operation 1: groupBy with 10M unique keys
├─ Hash table size: 500MB (fits!)
├─ Execution memory available: 1.1GB
├─ Status: ✅ Success

Operation 2: Join two large DataFrames
├─ Shuffle buffers: 800MB
├─ Execution memory available: 1.1GB - 500MB = 600MB ❌
├─ Status: ❌ Not enough!
└─ Result: Spill to disk (slow) or OutOfMemory error

What happens when execution memory is full:
1. Spark tries to reserve more from storage memory
2. Evicts cached data to make room
3. Spills overflow data to disk
4. Performance degrades significantly
```

---

## User Memory

### Purpose: Your Custom Code

**What uses user memory?**

```python
# 1. MAP operation with custom objects
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def complex_transformation(value):
    # Custom objects created here use user memory
    my_dict = {i: i*2 for i in range(1000)}  # User memory
    my_list = list(range(10000))              # User memory
    result = custom_processing(my_dict, my_list)
    return result

# 2. Accumulator in custom code
accumulator = spark.sparkContext.accumulator(0)

def increment_accumulator(row):
    accumulator.add(1)
    return row

df.rdd.map(increment_accumulator).collect()
# Accumulator state stored in user memory

# 3. Caching custom Python objects
from pyspark.sql.functions import col, lit

# Slow: Creating new object per row
df.select(
    col("name"),
    udf(lambda x: expensive_setup() + x)("value")
)

# Better: Setup once
setup_obj = expensive_setup()
broadcast_setup = spark.broadcast(setup_obj)

df.select(
    col("name"),
    udf(lambda x: broadcast_setup.value + x)("value")
)
```

### User Memory OutOfMemory Example

```python
# ❌ SLOW - User memory leak
@udf(returnType=StringType())
def accumulate_data(value):
    global big_list  # BAD!
    big_list.append(value)  # Keeps growing!
    return value

# After 1 million rows:
# big_list size: 1GB (growing!)
# User memory: 1.5GB
# Status: ❌ OutOfMemory!

# ✅ CORRECT - No memory leak
@udf(returnType=StringType())
def process_data(value):
    # Process and return
    # Don't accumulate in memory
    return value.upper()
```

---

## Fair Share Scheduler

### Dynamic Memory Sharing Between Storage and Execution

**Key concept**: Storage and execution memory can share space!

```
Normal situation (no conflicts):
├─ Storage: Using 0.5GB of available 1.1GB
└─ Execution: Using 0.8GB of available 1.1GB
└─ Total: 1.3GB (all fits!)

Shuffle starts (execution memory pressure):
├─ Storage: Still using 0.5GB
├─ Execution: Needs 1.5GB for shuffle
├─ Available Execution: 1.1GB (not enough!)
├─ Fair Share: Can evict storage to make room?
├─ Evict cached data: 0.4GB freed
├─ Execution now has: 1.1GB + 0.4GB = 1.5GB ✅
└─ Result: Shuffle completes, but cache is partially evicted
```

**Configuration for Fair Share**:

```python
spark = SparkSession.builder \
    .config("spark.memory.fraction", 0.6) \
    .config("spark.memory.storageFraction", 0.5) \
    .appName("MemoryDemo") \
    .getOrCreate()

# spark.memory.fraction = 0.6
# └─ 60% of executor memory is Spark Memory (rest is user)

# spark.memory.storageFraction = 0.5
# └─ 50% of Spark Memory goes to storage (rest to execution)
# └─ But can borrow from each other (Fair Share)
```

---

## Common Memory Issues and Solutions

### Issue 1: OutOfMemory During Shuffle

**Symptom**:
```
java.lang.OutOfMemoryError: Java heap space
during df.groupBy("large_category").count()
```

**Cause**:
```
Executor Memory: 4GB
├─ Reserved: 0.3GB
├─ User: 1.5GB
├─ Storage: 1.1GB (some data cached)
├─ Execution: 1.1GB
└─ Needed for groupBy: 2GB (too much!)
```

**Solutions** (in order of preference):

```python
# Solution 1: Increase executor memory
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .appName("App") \
    .getOrCreate()

# Solution 2: Pre-filter to reduce data size
df_filtered = df.filter(df.price > 100)  # 1/10 original size
result = df_filtered.groupBy("category").count()

# Solution 3: Use more partitions (smaller per-partition aggregates)
spark.conf.set("spark.sql.shuffle.partitions", 500)  # Increase from 200

# Solution 4: Disable caching for that operation
other_df.unpersist()  # Free up storage memory
result = df.groupBy("category").count()  # More execution memory available

# Solution 5: Use approximate algorithms (if acceptable)
df.approxQuantile("salary", [0.25, 0.75], 0.01)  # Faster, less memory
```

### Issue 2: Cache Eviction

**Symptom**:
```
Cached data is being evicted and re-computed
(visible in Spark UI: cached blocks are missing)
```

**Cause**:
```
Cache size: 5GB
Executor memory: 4GB
Storage memory: 1.1GB
└─ Too much data, doesn't fit!
```

**Solutions**:

```python
# Solution 1: Cache less data
# Instead of:
df1.cache()  # 5GB
df2.cache()  # 5GB
df3.cache()  # 5GB
# Only cache what's reused:
df1.cache()  # 5GB (used 10 times)
# Don't cache:
df2  # Used 1 time
df3  # Used 1 time

# Solution 2: Use lower cache level (less memory, slower)
df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
# Stores: RAM first, then overflows to disk (not OutOfMemory)

# Solution 3: Increase executor memory
spark = SparkSession.builder \
    .config("spark.executor.memory", "16g") \
    .appName("App") \
    .getOrCreate()

# Solution 4: Increase storage fraction
spark.conf.set("spark.memory.storageFraction", 0.7)  # Default 0.5
# 70% of spark memory to storage (30% to execution)
# Trade-off: Less execution memory (slower shuffles)
```

### Issue 3: Slow Shuffle (Spilling)

**Symptom**:
```
Shuffle write/read taking 10x longer than expected
Spark UI shows: "Spill (Memory)" metric
```

**Cause**:
```
Shuffle operation needs 2GB for buffers
Execution memory available: 1.1GB
Result: Spills 0.9GB to disk
Disk I/O: Much slower than memory
```

**Solutions**:

```python
# Solution 1: Reduce shuffle size by filtering first
# Instead of:
df.join(other_df, "id")  # Both 10GB

# Do:
df_filtered = df.filter(df.value > 100)  # 1GB
df_filtered.join(other_df, "id")  # Much smaller shuffle

# Solution 2: Pre-partition to avoid shuffle
if df.isPartition:
    result = df.join(other_df, "id")
else:
    df_repartitioned = df.repartition("id")
    result = df_repartitioned.join(other_df, "id")

# Solution 3: Use broadcast join (if possible)
from pyspark.sql.functions import broadcast
result = df.join(broadcast(small_df), "id")  # No shuffle

# Solution 4: Increase execution memory
spark.conf.set("spark.memory.storageFraction", 0.3)  # More to execution
# Trade-off: Less cache memory
```

---

## Memory Tuning Parameters

### Critical Parameters

```python
# 1. Executor Memory - Total RAM per executor
spark.conf.set("spark.executor.memory", "4g")
# Recommendation: 4-16GB (cluster dependent)
# More = more cache, but GC pauses get worse

# 2. Executor Cores - Cores per executor
spark.conf.set("spark.executor.cores", 4)
# Recommendation: 4-8 cores
# More parallelism, but memory contention

# 3. Shuffle Partitions - Output partitions after shuffle
spark.conf.set("spark.sql.shuffle.partitions", 200)
# Rule: ~128MB per partition
# 10GB data → ~80 partitions
# 100GB data → ~800 partitions

# 4. Spark Memory Fraction - Fraction for Spark (vs User) Memory
spark.conf.set("spark.memory.fraction", 0.6)
# Default 0.6 = 60% to Spark, 40% to User
# Increase if user code is simple
# Decrease if user code needs lots of memory

# 5. Storage Fraction - Fraction of Spark Memory for Storage (vs Execution)
spark.conf.set("spark.memory.storageFraction", 0.5)
# Default 0.5 = 50% storage, 50% execution
# Increase if doing lots of caching
# Decrease if shuffles are large

# 6. Max Partition Bytes - Target size per partition when reading
spark.conf.set("spark.sql.files.maxPartitionBytes", 128 * 1024 * 1024)  # 128MB
# Larger = fewer partitions (less overhead, less parallelism)
# Smaller = more partitions (more overhead, more parallelism)
```

### Tuning Example for Large Dataset

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.cores", 8) \
    .config("spark.sql.shuffle.partitions", 400) \
    .config("spark.memory.fraction", 0.7) \
    .config("spark.memory.storageFraction", 0.4) \
    .appName("LargeScale") \
    .getOrCreate()

# Breakdown for 16GB executor:
# Reserved: 0.3GB (fixed)
# Usable: 15.7GB
# Spark Memory (70% of usable): 11GB
#   ├─ Storage (40%): 4.4GB
#   └─ Execution (60%): 6.6GB
# User Memory (30% of usable): 4.7GB

# This configuration:
# ✅ Large cache (4.4GB)
# ✅ Large execution buffers (6.6GB) for shuffles
# ❌ Less user memory (4.7GB)
# Suitable for: SQL operations, minimal custom code

# For custom Python code:
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", 4) \
    .config("spark.memory.fraction", 0.5) \
    .config("spark.memory.storageFraction", 0.3) \
    .appName("CustomCode") \
    .getOrCreate()

# Breakdown for 8GB executor:
# Spark Memory: 3GB
#   ├─ Storage: 0.9GB
#   └─ Execution: 2.1GB
# User Memory: 3GB (for Python objects)

# This configuration:
# ✅ Large user memory (3GB)
# ✅ Reasonable execution (2.1GB)
# ❌ Small cache (0.9GB)
# Suitable for: Custom Python code, UDFs
```

---

## Monitoring Memory

### Spark UI Memory Metrics

```
Executor section shows:
├─ RDD Blocks: Number of cached blocks
├─ Storage Memory: How much memory used for cache
├─ Max Memory: Maximum available
└─ Executor Logs: Detailed memory events

Example:
├─ RDD Blocks: 150
├─ Storage Memory: 1.2GB / 4GB
├─ Max Memory: 4GB
```

### Detecting Memory Issues

```python
# Check if operations are spilling
df.groupBy("category").count().explain(extended=True)
# Look for: "SpillableAggregate" or "Sort" operations

# Monitor executor memory in Spark UI
# http://driver-ip:4040/executors
# Look for:
# - RDD Blocks increasing = cache growing
# - Storage Memory near max = eviction risk
# - Spill (Memory) in logs = spilling to disk
```

---

## Best Practices

- [ ] Set executor memory based on cluster resources (2-8GB typical)
- [ ] Use 4-8 cores per executor (not too many!)
- [ ] Cache only data used multiple times
- [ ] Cache only if it fits (avoid spilling)
- [ ] Monitor Spark UI for evictions and spilling
- [ ] Tune shuffle partitions based on data size
- [ ] Filter before expensive operations
- [ ] Use broadcast joins for small tables
- [ ] Pre-partition if doing multiple joins on same key
- [ ] Disable cache when not needed (`.unpersist()`)

---

## Interview Questions

**Q1: How is executor memory divided?**
A: Executor memory is divided into: Reserved (300MB), User Memory (40%), and Spark Memory (60%). Spark Memory is further divided into Storage (cache) and Execution (shuffles), with Fair Share allowing them to borrow from each other.

**Q2: When should you cache data?**
A: Cache when data is large, used multiple times, and fits in memory. Avoid caching small data (used once) or data that doesn't fit (causes eviction).

**Q3: What causes OutOfMemory errors?**
A: Usually execution memory pressure during shuffles/aggregations, or user code creating large objects. Solutions include increasing executor memory, filtering early, or pre-partitioning.

**Q4: How do you know if cache is being evicted?**
A: Check Spark UI for "RDD Blocks" metric decreasing or "Spill (Memory)" increasing. Also see cache-related evictions in logs.

**Q5: What's the difference between storage and execution memory?**
A: Storage memory holds cached data. Execution memory holds temporary data during shuffle/aggregation. Fair Share allows them to borrow from each other.
