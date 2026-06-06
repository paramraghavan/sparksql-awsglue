# Complete Spark Mastery Guide: From Fundamentals to Production Optimization

> **Purpose**: A comprehensive, integrated guide for data engineers covering Spark fundamentals, optimization techniques, and real-world problem solving. Suitable for beginners, intermediate developers, and interview preparation.

---

## Table of Contents

1. [Introduction & Roadmap](#introduction--roadmap)
2. [Part 1: Fundamentals (Beginner)](#part-1-fundamentals-beginner)
3. [Part 2: Core Concepts (Intermediate)](#part-2-core-concepts-intermediate)
4. [Part 3: Optimization Techniques (Intermediate-Advanced)](#part-3-optimization-techniques-intermediate-advanced)
5. [Part 4: Advanced Topics](#part-4-advanced-topics)
6. [Part 5: Real-World Debugging & Optimization](#part-5-real-world-debugging--optimization)
7. [Part 6: Interview Preparation](#part-6-interview-preparation)

---

# Introduction & Roadmap

## Learning Paths

### Path A: Quick Start (2 hours)
1. What is spark.read? (15 min)
2. Actions vs Transformations (20 min)
3. Narrow vs Wide Transformations (25 min)
4. Basic optimization: Filter, Select, Broadcast (30 min)
5. Real-world example (30 min)

### Path B: Intermediate Mastery (1 week)
- Complete Part 1 + Part 2 + Part 3
- Understand execution model
- Master basic optimizations
- Read Spark UI

### Path C: Expert Level (2 weeks)
- All parts
- Deep dive into Catalyst, Memory Management
- Real-world debugging scenarios
- Interview preparation

---

# PART 1: FUNDAMENTALS (BEGINNER)

## 1.1 What is spark.read? (Is it an Action or Transformation?)

### Quick Answer

| Code | Behavior | Type |
|------|----------|------|
| `spark.read.csv("file.csv")` | Returns DataFrame (no execution) | **Transformation** |
| `spark.read.option("inferSchema", "true").csv("file.csv")` | Executes immediately | **Acts like Action** |
| `spark.read.parquet("file.parquet")` | Returns DataFrame (no execution) | **Transformation** |

### The Default Case: spark.read is a Transformation

**Example: Simple Read (Lazy)**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo").getOrCreate()

# This does NOT execute immediately
df = spark.read.csv("s3://bucket/data.csv")

print(type(df))  # <class 'pyspark.sql.dataframe.DataFrame'>
print("Spark job started? NO - nothing happened yet!")

# NOW execution starts (action)
df.show()  # This triggers the read job
```

**What happens**:
```
Step 1: spark.read.csv() → Creates logical plan (no execution)
Step 2: Return DataFrame object
Step 3: print(type(df)) → Just prints type (no execution)
Step 4: df.show() → FIRST action, NOW execution happens!
```

### Why It's a Transformation (In Most Cases)

Because `spark.read`:
- ✅ Returns a DataFrame (not a result)
- ✅ Doesn't trigger execution
- ✅ Just builds logical plan
- ✅ Can be chained with other transformations

```python
# All of these are lazy (no execution)
df = spark.read.csv("file.csv")               # Lazy
df_filtered = df.filter(df.age > 30)          # Lazy
df_selected = df_filtered.select("name")      # Lazy
df_ordered = df_selected.orderBy("name")      # Lazy

# First action - NOW everything executes
df_ordered.show()  # Triggers all 4 above operations
```

---

## 1.2 The Special Case: inferSchema=true Makes It Act Like an Action

**Example: Read with Schema Inference (Eager)**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo").getOrCreate()

# This DOES execute immediately!
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://bucket/data.csv")

print(f"Schema: {df.schema}")
```

**What happens**:
```
Step 1: spark.read starts reading
Step 2: Spark opens the file and reads sample rows
Step 3: Inspects data to infer column types
Step 4: EXECUTES a job (not lazy!)
Step 5: Returns DataFrame with inferred schema
Step 6: print() shows the schema
```

### Why inferSchema=true Triggers Execution

To infer schema, Spark must:
1. **Open the file** on S3/HDFS
2. **Read sample rows** (usually first 1000 rows)
3. **Analyze data types** (Is "123" an integer or string?)
4. **Create schema** based on analysis
5. **Return DataFrame** with that schema

### inferSchema Performance Deep Dive

#### Why inferSchema is Expensive

**The Problem**: Spark must read sample data before returning the DataFrame

```
WITHOUT inferSchema (Lazy):
Step 1: spark.read.csv() → Create plan (instant) ← No file access!
Step 2: Return DataFrame (instant)
Step 3: df.show() → Read actual file (60 sec)
Total: ~60 seconds

WITH inferSchema (Eager):
Step 1: spark.read.option("inferSchema", "true").csv() → Opens file (instant)
Step 2: Read first 1000+ rows from file (30 sec) ← FILE ACCESS!
Step 3: Analyze each value to detect type (5 sec)
Step 4: Return DataFrame with inferred schema (instant)
Step 5: df.show() → Read actual file again (60 sec)
Total: ~95 seconds (58% slower!)
```

#### Real-World Cost Analysis

**Scenario: 100GB CSV file on S3**

```
File characteristics:
├─ Size: 100GB
├─ Row count: 2 billion rows
├─ Columns: 50
├─ Location: S3 (network latency: 100ms)
└─ Format: CSV (text, ~50 bytes per field)

WITHOUT inferSchema:
├─ spark.read.csv() call: Instant (no file read)
├─ df.count() call: Scans file (300 seconds)
├─ df.show() call: Scans file (300 seconds)
├─ df.filter(...).count(): Scans file (300 seconds)
└─ Total for 3 operations: 900 seconds

WITH inferSchema:
├─ spark.read.option("inferSchema", "true").csv():
│  ├─ S3 request latency: 100ms
│  ├─ Read first 1000 rows: ~50KB (instant)
│  ├─ Parse all 50 columns: ~5 seconds
│  └─ Subtotal: ~5 seconds
├─ df.count() call: Scans file (300 seconds)
├─ df.show() call: Scans file (300 seconds)
├─ df.filter(...).count(): Scans file (300 seconds)
└─ Total for same 3 operations: 905 seconds

Difference: 5 seconds per DataFrame creation
Cost: If you read 10 CSV files → 50 seconds wasted!
```

#### Best Practice: Always Provide Explicit Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df = spark.read.schema(schema).csv("s3://bucket/data.csv")

print("No execution happened yet")
df.show()  # NOW execution happens (1 job only, no inference overhead!)
```

**Benefits**:
- ✅ Faster (no inference overhead)
- ✅ Explicit (everyone knows the schema)
- ✅ Reliable (no surprise type conversions)
- ✅ Predictable (same schema every time)

---

## 1.3 Actions vs Transformations

### Definitions

**Transformation**: Returns a DataFrame, doesn't execute

```python
df.filter(col("age") > 30)      # Transformation
df.select("name", "age")        # Transformation
df.groupBy("category").count()  # Transformation (returns DF)
df.join(other_df, "id")         # Transformation
df.map(lambda x: x * 2)         # Transformation
```

**Action**: Returns a result (not a DataFrame), triggers execution

```python
df.show()                       # Action (displays data)
df.count()                      # Action (returns count)
df.collect()                    # Action (returns all rows)
df.write.parquet(...)           # Action (writes to storage)
df.take(10)                     # Action (returns first 10 rows)
```

### Key Mental Model

```
Transformations: Build a recipe (lazy)
├─ filter(), select(), join()
├─ groupBy(), orderBy()
└─ map(), flatMap()

Actions: Execute the recipe (eager)
├─ show(), count(), collect()
└─ write(), take()

Multiple transformations = Single execution plan
├─ df.filter(...).select(...).groupBy(...)
│  └─ All optimized as ONE plan, executed when action called
```

---

# PART 2: CORE CONCEPTS (INTERMEDIATE)

## 2.1 Narrow vs Wide Transformations

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

**Performance Impact**: Very fast (in-memory, no network I/O)

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
# SHUFFLE REQUIRED ❌

# Example 3: join - Matching keys across DataFrames
df1.join(df2, df1.user_id == df2.user_id)
# All user_id=5 from df1 must meet with user_id=5 from df2
# SHUFFLE REQUIRED ❌

# Example 4: distinct - Removing duplicates
df.distinct()
# All duplicate values must be sent to same partition
# SHUFFLE REQUIRED ❌
```

---

## 2.2 Shuffles: The Most Expensive Operation

### What is a Shuffle?

A **shuffle** is when Spark moves data from one executor to another to group or rearrange data.

### Why Shuffles Are Expensive: The 4 Costs

**1. Network I/O** (Moving data across cluster)
```
Example: 1GB data needs to move from Executor 1 → Executor 2
Network bandwidth: ~1 Gbps = 125 MB/sec
Time = 1000 MB ÷ 125 MB/sec = 8 seconds (just network!)
```

**2. Disk I/O** (Writing intermediate results)
```
Executor 1: Write 1GB to disk before sending
Network: Send 1GB
Executor 2: Read 1GB from disk
Typical: ~1 second per GB each way
```

**3. Serialization** (Converting objects to bytes)
```
Serialization overhead: ~2-5x memory increase
```

**4. GC Pauses** (Memory pressure causes garbage collection)
```
During shuffle, memory usage spikes
Garbage collection pauses: 500ms - 5000ms possible
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

### How Spark Manages Shuffles

```
During shuffle, Spark writes files:

Node 1 (Executor 0):
└─ shuffle_0_0_0.data  ← Partition 0 shuffle data
   shuffle_0_0_1.data  ← Partition 1 shuffle data
   ...

Node 2 (Executor 1):
└─ shuffle_0_1_0.data
   shuffle_0_1_1.data
   ...

These files allow cleanup via spark.shuffle.service.enabled
```

---

## 2.3 The Spark Execution Model

### Jobs, Stages, and Tasks Hierarchy

```
Action (.show(), .count(), .write())
    ↓
Job (One per action)
    ├─ Job 0 (map/filter stage)
    ├─ Job 1 (shuffle stage pair: map + reduce)
    └─ Job 2 (final stage)
        ↓
Stage (Series of transformations without shuffle)
    ├─ Stage 0: read → filter → select (no shuffle)
    ├─ Stage 1: Map-side shuffle (groupBy/join prep)
    └─ Stage 2: Reduce-side shuffle (groupBy/join complete)
        ↓
Task (Work on one partition)
    ├─ Task 0: Process partition 0
    ├─ Task 1: Process partition 1
    └─ Task 2: Process partition 2
```

### Example Execution Flow

```python
df = spark.read.csv("file.csv")  # 100 partitions
filtered = df.filter(col("age") > 30)
result = filtered.groupBy("category").count()
result.show()  # Action!
```

**Execution Timeline**:
```
Step 1: read + filter (Stage 0, 100 tasks)
├─ Each task: Read one partition, filter
└─ Time: 60 seconds

Step 2: Shuffle map phase (Stage 1, 100 tasks)
├─ Each task: Partition filtered data by "category"
├─ Write to disk
└─ Time: 90 seconds

Step 3: Shuffle reduce phase (Stage 2, 200 tasks)
├─ Each task: Aggregate one category
├─ Count occurrences
└─ Time: 30 seconds

Step 4: Collect and display (Final)
├─ Gather results
└─ Time: 5 seconds

Total: ~185 seconds
```

---

# PART 3: OPTIMIZATION TECHNIQUES (INTERMEDIATE-ADVANCED)

## 3.1 Join Strategies: Broadcast vs Shuffle

### Broadcast Joins: The Fast Track

**What is it?**
A broadcast join copies a small table to every executor, then each executor does a local join without shuffling.

### Visual: Broadcast vs Shuffle Join

```
SHUFFLE JOIN (❌ Slow):
┌──────────────────────────┐
│  Large DF1 (100GB)       │
│  Repartition by key      │
│         ↓                │
│    NETWORK I/O 150GB     │
│         ↓                │
│  Large DF2 (50GB)        │
│  Repartitioned           │
└──────────────────────────┘
Network transfer: 150GB ❌

BROADCAST JOIN (✅ Fast):
┌──────────────────────────┐
│  Broadcast: 100MB only   │
│  To each executor        │
│         ↓                │
│  Local joins on each     │
│  executor (fast!)        │
└──────────────────────────┘
Network transfer: 100MB ✅
```

### When to Use Broadcast Joins

**Criteria**:
```python
small_df = spark.read.csv("small_table.csv")  # < 10MB
large_df = spark.read.csv("large_table.csv")  # 100GB

# Catalyst will auto-broadcast small_df
result = large_df.join(small_df, "key")  # Broadcast join!

# Or explicitly force broadcast
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

**Auto-Broadcast Threshold**:
```python
# Default: 10MB
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # "10485760"

# Increase to 100MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

### Performance Comparison: Broadcast vs Shuffle

**Scenario: Joining 100GB with 1GB**

```
Shuffle Join:
├─ Map: Partition both tables (5 sec)
├─ Write to disk: 101GB (40 sec)
├─ Network transfer: 101GB (45 sec)
├─ Read from disk: 101GB (40 sec)
└─ Reduce: Join (10 sec)
└─ Total: 140 seconds

Broadcast Join:
├─ Collect 1GB to driver (5 sec)
├─ Broadcast to 4 executors: 4GB (8 sec)
└─ Local joins: (3 sec)
└─ Total: 16 seconds

Speedup: 8.75x faster!
Network I/O: 303x less ⚡⚡⚡
```

### Broadcast Join Limitations

**Driver Memory**:
- Broadcast must fit in driver memory
- Practical limit: 1-2GB (leave room for other tasks)
- Safe broadcast table size: < 100MB always, < 500MB usually

**Executor Memory**:
```
Executor Memory: 8GB
├─ Running normal task: 5GB
├─ Add broadcast copy: + 1GB
└─ GC overhead: - increases
└─ Result: Memory pressure
```

**When NOT to Use Broadcast**:
```python
# ❌ Both tables are large
df1 = spark.read.csv("large_100gb.csv")
df2 = spark.read.csv("large_50gb.csv")
result = df1.join(broadcast(df2), "id")  # ❌ Wastes memory

# ✅ Use shuffle join instead
result = df1.join(df2, "id")

# ✅ Pre-partitioned data
# Use sort-merge join (data already co-located)
```

---

## 3.2 Repartitioning Strategies

### Understanding Partitions

```python
df = spark.read.parquet("file.parquet")

# Check current partitions
num_partitions = df.rdd.getNumPartitions()
print(f"Partitions: {num_partitions}")

# Verify partition sizes
df.rdd.glom().map(len).collect()
# Output: [100000, 120000, 80000, ...]
# Shows rows per partition
```

### Repartition vs Coalesce

**Repartition** (Shuffles data):
```python
df_repart = df.repartition(200)  # SHUFFLE

# When to use:
├─ Increasing partitions (only option)
├─ Changing partition key (must shuffle)
└─ Rebalancing severely unbalanced data

# Time cost: Expensive (full shuffle)
```

**Coalesce** (Merges partitions):
```python
df_coal = df.coalesce(50)  # NO SHUFFLE

# When to use:
├─ Reducing partitions (100x faster!)
├─ Before final write
└─ No need to repartition on different key

# Time cost: Minimal (just merges in-place)

# Important: Cannot increase partitions!
df.coalesce(1000)  # ❌ Won't work, stays at current
```

### Repartition Strategies

**Strategy 1: Increase Partitions for Parallelism**

```python
df = spark.read.parquet("file.parquet")
# Default partitions: 100 (based on file size)

# If you have 50 cores available:
df_repartitioned = df.repartition(200)  # 200 partitions
# Now 50 tasks run in 4 waves (good parallelism)

# Rule of thumb: 1-2 tasks per core
Cores: 50 → Partitions: 100-200
```

**Strategy 2: Repartition on Join Key**

```python
df1 = spark.read.parquet("df1.parquet")
df2 = spark.read.parquet("df2.parquet")

# Will join multiple times on "id"
df1_repart = df1.repartition(200, "id")
df2_repart = df2.repartition(200, "id")

# Now joins are fast (data already co-located)
result1 = df1_repart.join(df2_repart, "id")  # No shuffle!
result2 = df1_repart.join(df2_repart.filter(...), "id")  # No shuffle!
```

**Strategy 3: Repartition by Multiple Columns**

```python
df_repart = df.repartition(200, "customer_id", "order_date")
# Partitions by customer_id first, then order_date
# Useful for range queries or complex joins
```

### Best Practice Pattern

```python
df = spark.read.parquet("s3://bucket/data.parquet")

# Step 1: Filter first (reduces size)
df_filtered = df.filter(col("status") == "active")

# Step 2: Repartition on key you'll use repeatedly
df_repart = df_filtered.repartition(500, "customer_id")

# Step 3: Cache if using multiple times
df_repart.cache()

# Step 4: Do multiple operations (no shuffles!)
result1 = df_repart.groupBy("customer_id").agg(sum("amount"))
result2 = df_repart.join(other_df, "customer_id")
result3 = df_repart.filter(col("amount") > 1000)

# Step 5: Write with coalesce (no shuffle!)
result1.coalesce(10).write.parquet("s3://bucket/output1")
result2.coalesce(10).write.parquet("s3://bucket/output2")
```

---

## 3.3 Caching Best Practices

### What to Cache

**✅ CACHE: Large intermediate results used multiple times**

```python
df = spark.read.csv("huge_file.csv")  # 100GB
df_filtered = df.filter(col("status") == "active")  # 50GB

df_filtered.cache()

# Use multiple times (cache helps all):
result1 = df_filtered.groupBy("region").count()
result2 = df_filtered.filter(col("amount") > 1000).count()
result3 = df_filtered.select("customer_id").distinct()

# Without cache: 3 full table scans (300 seconds)
# With cache: 1 scan + 2 cache reads (100 seconds)
```

**❌ DON'T CACHE: Small datasets or single-use data**

```python
df_small = spark.read.csv("small_file.csv")  # 100MB
df_small.cache()  # ❌ Wastes cache space!

# Scan takes 5 seconds anyway, caching adds overhead
```

### Cache Storage Levels

```python
# Level 1: MEMORY_ONLY (Default - Fastest)
df.cache()
df.persist(pyspark.StorageLevel.MEMORY_ONLY)
# If doesn't fit: Data is recomputed (slow)

# Level 2: MEMORY_AND_DISK (Safer)
df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
# Overflows to disk if needed (slower than MEMORY but no recompute)

# Level 3: DISK_ONLY (Last resort)
df.persist(pyspark.StorageLevel.DISK_ONLY)
# Slower than reading from source usually
```

### Caching SQL Queries

```python
spark.sql("""
    CREATE TEMPORARY VIEW cached_view AS
    SELECT customer_id, SUM(amount) as total_spent
    FROM orders
    WHERE year = 2024
    GROUP BY customer_id
""")

spark.sql("CACHE TABLE cached_view")

# Use multiple times (cache helps all):
result1 = spark.sql("SELECT * FROM cached_view WHERE total_spent > 1000")
result2 = spark.sql("SELECT COUNT(*) FROM cached_view")

# Uncache when done:
spark.sql("UNCACHE TABLE cached_view")
```

### When Cache Goes Wrong

```python
# Problem 1: Cache doesn't fit, causes spilling
df = spark.read.parquet("file.parquet")  # 100GB
df.cache()  # ❌ Too large!
# Result: Cache spills to disk, slower than not caching!

# Solution: Check size first, or use MEMORY_AND_DISK

# Problem 2: Cache evicted by other operations
df1 = spark.read.parquet("file1.parquet")  # 5GB
df1.cache()
df2 = spark.read.parquet("file2.parquet")  # 5GB
df2.cache()
# If executor memory < 10GB: One evicts the other!

# Solution: df2.unpersist() when done

# Problem 3: Caching too late
df = spark.read.parquet("file.parquet")
df = df.groupBy(...).count()  # Expensive!
df.cache()  # ❌ Too late!
# Cache doesn't help past expensive operation

# Solution: Cache right after read
df = spark.read.parquet("file.parquet")
df.cache()  # Cache early
```

---

# PART 4: ADVANCED TOPICS

## 4.1 Catalyst Optimizer: How Spark Optimizes Your Queries

### The Optimizer Engine

**Catalyst** is Spark's SQL query optimizer that automatically rewrites your code to execute faster.

```
Your Code (Logical Plan)
         ↓
    CATALYST
    ├─ Parse SQL/DataFrame API
    ├─ Create logical plan
    ├─ Apply optimization rules
    ├─ Create optimized logical plan
    └─ Create physical plan
         ↓
Optimized Code (Physical Plan)
         ↓
    Execution
```

### Key Optimization Techniques

#### 1. Predicate Pushdown

Moving filters as early as possible in the execution plan:

```python
# ❌ SLOW - Original (without optimization)
df = spark.read.csv("huge_employees.csv")
filtered = df.filter(df.age > 30).select("name")

# Execution:
# Step 1: Read ALL columns
# Step 2: Filter rows where age > 30
# Step 3: Select only name column

# ✅ FAST - After Catalyst optimization
# Execution:
# Step 1: Read only "name" and "age" columns
# Step 2: Filter rows where age > 30
# Step 3: Select name column

# Performance difference:
# File size: 100GB
# Columns: 50 (each ~2GB)
# SLOW: Read 100GB
# FAST: Read ~4GB (name + age only)
# = 25x faster!
```

#### 2. Column Pruning

Reading only columns that are needed:

```python
# ❌ SLOW - Reads all columns
df = spark.read.csv("employees.csv")
result = df.select("name").collect()

# ✅ FAST - Catalyst prunes unused columns
# Only reads: name column (not salary, address, etc.)
# = 50x faster for 50-column file!
```

#### 3. Constant Folding

Pre-computing expressions that don't depend on data:

```python
# ❌ SLOW - Computed during execution
result = df.select(
    (col("salary") * (1 + 0.1 + 0.05)).alias("total")
)

# ✅ FAST - Catalyst pre-computes (1 + 0.1 + 0.05) = 1.15
# Actual execution:
result = df.select((col("salary") * 1.15).alias("total"))
```

#### 4. Join Reordering

Optimizing the order of joins:

```python
# ❌ SLOW - Wrong join order (larger table first)
df1 = spark.read.csv("large_orders.csv")      # 100GB, 1 billion rows
df2 = spark.read.csv("medium_customers.csv")  # 10GB, 10 million rows
df3 = spark.read.csv("small_products.csv")    # 1GB, 1 million rows

result = (df1.join(df2, "customer_id")
             .join(df3, "product_id"))

# Execution:
# Step 1: df1 (100GB) JOIN df2 (10GB): 110GB network I/O
# Step 2: Result (110GB) JOIN df3 (1GB): 111GB network I/O
# Total: 221GB network I/O

# ✅ FAST - Catalyst reorders (smaller tables first)
# Step 1: df1 (100GB) JOIN df3 (1GB) [broadcast]: 0GB network I/O
# Step 2: Result (100.1GB) JOIN df2 (10GB): 110GB network I/O
# Total: 110GB network I/O
# = 2x faster!
```

#### 5. Null Propagation & Expression Simplification

```python
# ❌ SLOW - Evaluates full expression
result = df.filter(
    (col("age") > 30) & (col("salary") > 50000)
)

# ✅ FAST - Catalyst optimizes null logic
# For NULL age: Skip salary evaluation
# = 2x faster if 50% NULL ages
```

### Using explain() to See the Plan

**Basic Explain**:
```python
df = spark.read.csv("employees.csv", header=True)
result = df.filter(df.age > 30).select("name", "age")

print(result.explain())
```

**Extended Explain** (shows all optimization stages):
```python
print(result.explain(extended=True))
# Shows:
# - Parsed Logical Plan
# - Analyzed Logical Plan
# - Optimized Logical Plan
# - Physical Plan
```

### Catalyst Limitations

```python
# ❌ Catalyst can't optimize UDFs
@udf(returnType=IntegerType())
def complex_calculation(salary):
    # Catalyst doesn't know what this does!
    result = 0
    for i in range(1000):
        result += salary * i
    return result

df.select(complex_calculation(col("salary")))
# Catalyst can't optimize this!

# ✅ Better - Use built-in functions
result = df.select((col("salary") * 1000).alias("result"))
# Catalyst sees this and optimizes!

# ❌ Catalyst can't optimize RDD operations
rdd = spark.sparkContext.parallelize(range(1000000))
result = rdd.map(lambda x: x * 2).collect()
# Catalyst doesn't touch RDDs!

# ✅ Better - Use DataFrames
df = spark.createDataFrame([(i,) for i in range(1000000)], ["value"])
result = df.select((col("value") * 2))
```

---

## 4.2 Memory Management: Understanding the Memory Hierarchy

### Memory Structure

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

### Storage Memory: Caching Data

**When is storage memory used?**

```python
df = spark.read.csv("huge_file.csv")

# BEFORE cache - no storage memory used
print("Storage memory used: 0MB")

df.cache()  # Marks for caching
print("Storage memory used: Still 0MB (lazy!)")

result = df.count()  # Action - NOW it loads into cache
print("Storage memory used: 5GB (data is cached!)")

result2 = df.filter(col("age") > 30).count()
# ✅ Fast - loads from cache, not from disk
```

### Execution Memory: Shuffle Buffers

**What uses execution memory?**

```python
# 1. SHUFFLE BUFFERS - During sort/join operations
df1.join(df2, "id")  # Uses execution memory for shuffle buffers

# 2. HASH AGGREGATIONS - During groupBy
df.groupBy("category").agg(sum("amount"))
# Hash table: Holds intermediate aggregations

# 3. BROADCAST VARIABLES - Hashed lookup tables
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "id")
# Broadcast table hashed: Uses execution memory

# 4. SORT OPERATIONS - Buffering during sort
df.sort("name")
# Sort buffer: Holds rows being sorted
```

### Fair Share Scheduler

**Dynamic Memory Sharing Between Storage and Execution**

```
Normal situation (no conflicts):
├─ Storage: Using 0.5GB of available 1.1GB
└─ Execution: Using 0.8GB of available 1.1GB
└─ Total: 1.3GB (all fits!)

Shuffle starts (execution memory pressure):
├─ Storage: Still using 0.5GB
├─ Execution: Needs 1.5GB for shuffle
├─ Available Execution: 1.1GB (not enough!)
├─ Fair Share: Can evict storage to make room
├─ Evict cached data: 0.4GB freed
├─ Execution now has: 1.1GB + 0.4GB = 1.5GB ✅
└─ Result: Shuffle completes, but cache is partially evicted
```

### Memory Configuration

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "16g") \
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

### Common Memory Issues and Solutions

#### Issue 1: OutOfMemory During Shuffle

**Symptom**: `java.lang.OutOfMemoryError: Java heap space`

**Solutions** (in order of preference):

```python
# Solution 1: Increase executor memory (Simplest)
spark = SparkSession.builder \
    .config("spark.executor.memory", "32g") \
    .appName("App") \
    .getOrCreate()

# Solution 2: Increase shuffle partitions (Better)
spark.conf.set("spark.sql.shuffle.partitions", 1000)
# More partitions = smaller per-partition aggregates
# = Less memory per task

# Solution 3: Filter first (BEST)
filtered_df = df.filter(df.amount > 0)  # Reduce data size
result = filtered_df.groupBy("category").count()

# Solution 4: Use approximate algorithms (Last resort)
result = df.approx_percentile("salary", [0.25, 0.75], 0.01)
```

---

# PART 5: REAL-WORLD DEBUGGING & OPTIMIZATION

## 5.1 Scenario: Slow Job Optimization (5 minutes → 30 seconds)

### The Original Problem

```
Production Job Timeline:
├─ Starts: 2024-01-15 09:00:00
├─ Expected: 5 minutes
├─ Actual: 25+ minutes (timeout in 30 min!)
└─ Status: ❌ FAILING
```

### Original Code (SLOW)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DailyReport").getOrCreate()

# Load all data
orders = spark.read.parquet("s3://bucket/orders/")  # 100GB
customers = spark.read.parquet("s3://bucket/customers/")  # 5GB
products = spark.read.parquet("s3://bucket/products/")  # 1GB

# Step 1: Join orders with customers
order_customer = orders.join(customers, "customer_id")
# Result size: 100GB (all columns from both tables)

# Step 2: Join with products
full_data = order_customer.join(products, "product_id")
# Result size: 105GB+ (all columns combined)

# Step 3: Filter and aggregate
result = (full_data
    .filter(col("order_date") >= "2024-01-01")
    .groupBy("region").agg(sum("amount"))
)

result.write.mode("overwrite").parquet("s3://bucket/output/")
```

**What's Wrong?**

```
Step 1: Join 100GB + 5GB
├─ Shuffle both tables by customer_id
├─ Data moved: 105GB across network
└─ Time: 10 minutes (shuffle overhead)

Step 2: Join result (105GB) + 1GB
├─ Shuffle 105GB + 1GB
├─ Data moved: 106GB
└─ Time: 15 minutes (more shuffle!)

Step 3: Filter (105GB)
├─ Filter on order_date AFTER all joins
├─ Could have filtered BEFORE!
└─ Time: 5 minutes (processing huge dataset)

Total: 30 minutes (TIMEOUT!)
```

### Optimized Code (30 seconds)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DailyReport").getOrCreate()

# Load with column selection (Optimization 1: Column Pruning)
orders = spark.read.parquet("s3://bucket/orders/") \
    .select("order_id", "customer_id", "product_id", "amount",
            "order_date", "region")

customers = spark.read.parquet("s3://bucket/customers/") \
    .select("customer_id", "customer_name")

products = spark.read.parquet("s3://bucket/products/") \
    .select("product_id", "product_name")

# Filter first (Optimization 2: Predicate Pushdown)
filtered_orders = orders.filter(col("order_date") >= "2024-01-01")
# Reduced from 100GB to ~30GB (70% reduction!)

# Use broadcast joins for small tables (Optimization 3: Broadcast)
from pyspark.sql.functions import broadcast

order_customer = filtered_orders.join(
    broadcast(customers),
    "customer_id",
    how="inner"
)

order_customer_product = order_customer.join(
    broadcast(products),
    "product_id",
    how="inner"
)

# Aggregate after data reduction
result = (order_customer_product
    .groupBy("region").agg(sum("amount"))
)

result.write.mode("overwrite").parquet("s3://bucket/output/")
```

**Optimizations Applied**:
```
Optimization 1: Column Selection
├─ Before: Read all columns (100GB)
└─ After: Read only needed columns (30GB)
└─ Savings: 70% of input data!

Optimization 2: Filter First
├─ Before: Filter 100GB dataset
└─ After: Filter 30GB dataset
└─ Savings: Time proportional to data reduction

Optimization 3: Broadcast Joins (BIGGEST WIN!)
├─ Before: Shuffle 100GB + 5GB + 1GB = 206GB network I/O
└─ After: Broadcast 6GB total network I/O
└─ Savings: 200GB less network transfer!

Results:
├─ Before: 30 minutes
├─ After: 30 seconds
└─ Speedup: 60x faster! ⚡⚡⚡
```

---

## 5.2 Scenario: Data Skew in Joins

### The Problem

```
Job Stats:
├─ Total tasks: 200
├─ Tasks completed: 195 (97.5%)
├─ Slowest task: 600 seconds
├─ Fastest task: 10 seconds
└─ Status: Still running (waiting for last 5 tasks)

The problem: 5 tasks taking 60x longer than others!
This is DATA SKEW.
```

### Root Cause Analysis

```
Real-world user distribution (power law):
├─ Top 1% of users: 90% of all events
│  ├─ User ID "admin": 2 billion events
│  ├─ User ID "test": 1 billion events
│  └─ User ID "support": 500 million events
├─ Next 9% of users: 9% of events
└─ Bottom 90% of users: 1% of events

When partitioning by user_id:
├─ Partition 1: admin (2B) + test (1B) = 3B events
├─ Partition 2: support (500M) + 100 others = 700M events
├─ Partitions 3-200: Regular data (50M events each)

Task times:
├─ Task 1: 600 seconds (3B events)
├─ Task 2: 150 seconds (700M events)
├─ Tasks 3-200: 10 seconds (50M events each)
```

### Solution: Add Salt to Skewed Key

```python
from pyspark.sql.functions import rand, concat, lit, when
import random

# Identify skewed key
skewed_user_id = "admin"

# Split skewed key into multiple partitions
salt_count = 10  # Split into 10 sub-partitions

# Add salt to events table
events_salted = events.withColumn(
    "user_id_salted",
    when(col("user_id") == skewed_user_id,
         concat(col("user_id"), lit("_"), (rand() * salt_count).cast("int")))
    .otherwise(concat(col("user_id"), lit("_0")))
)

# Replicate skewed users in users table
from pyspark.sql.functions import array, explode

users_expanded = users.withColumn(
    "user_id_salted",
    when(col("user_id") == skewed_user_id,
         array([concat(col("user_id"), lit("_"), lit(i))
                for i in range(salt_count)]))
    .otherwise(array(concat(col("user_id"), lit("_0"))))
).select("user_id_salted", col("*")).filter(col("user_id_salted").isNotNull()) \
 .select(explode(col("user_id_salted")).alias("user_id_salted"), "*")

# Join on salted key
result = events_salted.join(users_expanded, "user_id_salted", how="inner")
```

**Impact**:
```
Before (skewed):
├─ Partition 0: 3B events (600s)
├─ Partition 1: 700M events (150s)
└─ Partitions 2-199: 50M events (10s each)
└─ Total time: 600 seconds (slowest task) ❌

After (with salt, split into 10):
├─ Partitions 0-9: 300M events each (60s each)
├─ Partitions 10-19: 70M events each (14s each)
└─ Partitions 20-199: 50M events each (10s each)
└─ Total time: 60 seconds (slowest task) ⚡
└─ Speedup: 10x faster!
```

---

## 5.3 Reading Spark History Server UI

### Accessing the UI

```
1. Access: http://driver-ip:18080
2. Click: Applications
3. Find your application
4. Click: Application ID
```

### Jobs Tab (Most Important)

```
Shows all jobs (one per action):

Job ID | Description | Duration | Stages | Tasks
-------|-------------|----------|--------|------
0      | csv at code | 5 sec    | 1      | 100
1      | filter at   | 2 sec    | 1      | 100
2      | join at     | 120 sec  | 2      | 200 ← SLOW!
3      | groupBy at  | 90 sec   | 3      | 300 ← Also slow

To find bottleneck: Sort by Duration (descending)
Click on Job 2 to drill down.
```

### Stages Tab (Drill Into Problem)

```
Job 2 Details:
├─ Number of Stages: 2
├─ Stage 0: 100 tasks, 30 seconds
└─ Stage 1: 100 tasks, 90 seconds ← This stage is slow

Click on Stage 1:

Stage Details:
├─ Task Count: 100
├─ Task Duration (avg): 900ms
├─ Task Duration (min): 50ms
├─ Task Duration (max): 5000ms ← One task is 100x slower!
├─ GC Time: 500ms average
├─ Shuffle Read: 500MB total
└─ Shuffle Write: 0MB

Finding: Uneven task times (50ms to 5000ms)
This indicates DATA SKEW
Some partition has way more data!
```

### Executors Tab (Memory Analysis)

```
Shows memory usage per executor:

Executor | Memory Used | Peak Memory | GC Time
---------|------------|------------|--------
0        | 6GB / 8GB  | 7GB / 8GB  | 1000ms
1        | 7.5GB / 8GB| 7.8GB / 8GB| 5000ms ← GC heavy!
2        | 5GB / 8GB  | 6GB / 8GB  | 800ms
3        | 7.8GB / 8GB| 8GB / 8GB  | 4500ms ← GC heavy!

Observations:
├─ Executors 1 and 3 have high GC time
├─ Memory usage near limit (7.8GB of 8GB)
└─ High GC pauses indicate memory pressure

Solutions:
├─ Increase executor memory
├─ Reduce data before operations
└─ Increase shuffle partitions
```

### Key Metrics to Monitor

```
❌ Red Flags:
├─ Task duration variance > 10x (data skew)
├─ GC time > 2 seconds per task (memory pressure)
├─ Spill (Memory) metrics (cache overflowing to disk)
├─ Failed tasks (check logs)
└─ Executor lost (memory/timeout issues)

✅ Good Signs:
├─ Task durations similar across partitions
├─ GC time < 500ms per task
├─ All tasks complete successfully
├─ Job duration stable across runs
└─ Memory usage 60-80% of available
```

---

## 5.4 Reading YARN Logs

### Accessing YARN Logs

```
Method 1: YARN Web UI
1. Access: http://yarn-resource-manager:8088
2. Click: Cluster
3. Find your application
4. Click: Logs link

Method 2: Command Line
yarn logs -applicationId application_xxx_yyyy

Method 3: Specific Executor
yarn logs -applicationId application_xxx_yyyy -containerId container_xxx
```

### What to Search For

```
Search for: ERROR
Example:
[ERROR] 2024-01-15 09:30:45 OutOfMemoryError: Java heap space
    at org.apache.spark.memory.MemoryPool.allocate

Indicates: Task ran out of memory
Solution:
├─ Increase --executor-memory
├─ Increase --shuffle-partitions
└─ Filter data before operation


Search for: TIMEOUT
Example:
[WARN] Container killed due to timeout
[ERROR] Executor lost

Indicates: Task took too long
Check:
├─ Is there a network issue?
├─ Is GC paused for too long?
└─ Is task stuck in infinite loop?


Search for: GC
Example:
[DEBUG] GC (G1 Young Generation) 1200ms
[DEBUG] GC (Full GC) 5000ms ← Long GC pause!

Indicates: Memory pressure
Solution:
├─ Increase executor memory
├─ Reduce data size
└─ Reduce GC pressure with better code
```

---

## 5.5 Quick Debug Checklist

### When Job Fails: Step-by-Step

```
□ 1. Check error message from spark-submit output
    └─ Most errors are obvious (OutOfMemory, timeout, etc.)

□ 2. Check Spark History Server UI
    └─ Go to Jobs tab, find failed job
    └─ Click through to Stages to see which stage failed

□ 3. Check YARN logs for executor that failed
    yarn logs -applicationId <app_id> | grep ERROR

□ 4. Identify root cause:
    ├─ OutOfMemory? → Increase memory or reduce data
    ├─ Timeout? → Optimize job or increase timeout
    ├─ Data skew? → Salt join key or use broadcast
    ├─ Network issue? → Check node connectivity
    └─ Other? → Search logs for "ERROR"

□ 5. Apply fix and re-run

□ 6. Verify fix using Spark UI
```

### When Job is Slow: Step-by-Step

```
□ 1. Measure current duration (from history server)
    └─ Example: 25 minutes

□ 2. Identify bottleneck:
    ├─ Look at Jobs tab, find slowest job
    ├─ Look at Stages, find slowest stage
    └─ Click into slowest stage to see task details

□ 3. Diagnose bottleneck type:
    ├─ Long stage + all tasks slow? → Data size issue
    │  Solution: Add .filter() or .select()
    ├─ Long stage + some tasks slow? → Data skew
    │  Solution: Add salt or pre-aggregate
    ├─ Shuffle stage slow? → Large shuffle
    │  Solution: Increase partitions or pre-aggregate
    └─ All stages slow? → System issue
       Solution: Check GC, network, disk

□ 4. Apply optimization and measure improvement

□ 5. Repeat if needed (move to next bottleneck)
```

### Common Issues and Quick Fixes

```
Issue: OutOfMemoryError
Spark UI shows: Memory at 100%, GC overhead
YARN logs show: "OutOfMemory: Java heap space"

Fix (try in order):
1. spark-submit --executor-memory 16g my_job.py
2. spark.conf.set("spark.sql.shuffle.partitions", 500)
3. df.filter(col("status") != "invalid")  # Pre-filter


Issue: Task much slower than others
Spark UI shows: Task time 600s vs avg 10s
Likely cause: DATA SKEW

Fix:
1. Add salt to join key: df.withColumn("key_salted", ...)
2. Or use broadcast join: broadcast(small_df)
3. Or pre-aggregate skewed data


Issue: "Spill (Memory)" in Spark UI
Meaning: Cache overflowing to disk

Fix:
1. Reduce cache size
2. Use MEMORY_AND_DISK instead of MEMORY_ONLY
3. Increase executor memory
4. Or don't cache at all
```

---

# PART 6: INTERVIEW PREPARATION

## Interview Questions & Answers

### Q1: You have a 100GB CSV, your job times out. Walk through your debugging approach.

**Answer**:
First, I'd check the Spark History Server UI to find which job/stage is slowest. Looking at task durations, I'd check for:

1. **Data size**: Are all columns needed? Use `.select()` to reduce. Are there rows that can be filtered early? Use `.filter()` before expensive operations like joins/groupBy.

2. **Join strategy**: If there's a join that's slow, I'd check if one table < 10MB. If yes, use broadcast join with `broadcast()`. If both are large, maybe pre-partition on join key.

3. **Data skew**: If task time variance is > 10x (some tasks 600s, others 10s), there's data skew. I'd salt the join key with a random suffix, or pre-aggregate the skewed data separately.

4. **Shuffle partitions**: Default is 200, which might be too low for large data. I'd increase to 500-1000 to distribute work more evenly.

5. **YARN logs**: Check for GC pauses or OutOfMemory errors. If OOM, either increase executor memory or increase shuffle partitions.

6. **Executor memory**: If GC pauses > 2 seconds, increase executor memory or reduce data before operations.

Finally, I'd apply one fix at a time and measure improvement using Spark UI.

---

### Q2: Explain the difference between narrow and wide transformations. Why does it matter?

**Answer**:
**Narrow transformations** (map, filter, select) don't shuffle data. Each partition outputs to one partition. Examples:
- `df.map()`: Transform each element, stays in partition
- `df.filter()`: Keep some rows, stays in partition
- `df.select()`: Choose columns, stays in partition

**Wide transformations** (groupBy, join, sort, distinct) shuffle data between executors. Each partition outputs to many partitions. Examples:
- `df.groupBy()`: Must send all same keys to one partition
- `df.join()`: Must send matching keys to same partition
- `df.sort()`: Must rearrange all data

**Why it matters**:
Shuffles are expensive (network I/O, disk I/O, serialization, GC). A narrow operation takes microseconds, a shuffle takes minutes. So if a job is slow, I first check if there are wide transformations. If there are, I ask:
1. Can I use broadcast join instead? (narrow)
2. Can I pre-filter to reduce data before shuffle?
3. Can I combine multiple shuffles into one?

Narrow operations run fast even on huge data (100GB), but wide operations on the same data take minutes.

---

### Q3: When would you use repartition vs coalesce? Give examples.

**Answer**:
**Repartition**: Shuffles data to create new partitions.
- Use when: Increasing partitions, changing partition key, balancing severely skewed data
- Example: `df.repartition(200, "customer_id")` to repartition by customer_id
- Cost: Expensive (full shuffle)
- Time: Minutes for 100GB data

**Coalesce**: Merges partitions without shuffling.
- Use when: Reducing partitions, before final write
- Example: `df.coalesce(10)` to reduce from 200 to 10 partitions
- Cost: Minimal (just merges in-place)
- Time: Seconds for 100GB data
- Limitation: Cannot increase partitions

**Examples**:
```python
# ❌ WRONG: Use repartition to reduce
df_small = df.repartition(10)  # Shuffles! Minutes!

# ✅ RIGHT: Use coalesce to reduce
df_small = df.coalesce(10)  # No shuffle! Seconds!

# ✅ RIGHT: Use repartition to increase
df_more = df.repartition(500)  # Necessary, must shuffle

# ✅ RIGHT: Repartition on join key for multiple joins
df_repart = df.repartition(200, "customer_id")
result1 = df_repart.join(other_df, "customer_id")  # No shuffle!
result2 = df_repart.join(other_df2, "customer_id")  # No shuffle!
```

---

### Q4: What is a broadcast join and when would you use it?

**Answer**:
A **broadcast join** copies a small table to every executor, then each executor does a local join without shuffling the large table.

**When to use**:
- When one table is < 10MB, the other is large (100GB+)
- The small table is dimension data (lookup table, reference data)
- Spark won't auto-broadcast if the small table is > `spark.sql.autoBroadcastJoinThreshold` (default 10MB)

**Example**:
```python
from pyspark.sql.functions import broadcast

orders = spark.read.parquet("orders.parquet")  # 100GB
customers = spark.read.parquet("customers.parquet")  # 5GB

# Auto-broadcast (if < 10MB)
result = orders.join(customers, "customer_id")

# Explicit broadcast (recommended)
result = orders.join(broadcast(customers), "customer_id")
```

**Performance**:
- Shuffle join: Network I/O = 150GB (100GB + 50GB shuffled)
- Broadcast join: Network I/O = 50MB (just the broadcast)
- Speedup: 3000x less network I/O!

**Limitations**:
- Small table must fit in driver memory (practical limit: 1-2GB)
- Each executor gets a copy, uses memory
- Don't use if both tables are large

---

### Q5: Explain the spark.read.option("inferSchema", "true") behavior and why it matters for performance.

**Answer**:
**Behavior**: `spark.read.csv()` without inferSchema is **lazy** (no execution). But with `inferSchema=true`, it's **eager** (executes immediately).

**Why?**: To infer schema, Spark must:
1. Open the file
2. Read sample rows (first 1000)
3. Analyze each field's type (is "123" an int or string?)
4. Create schema

This requires reading the file, which is a job execution.

**Code example**:
```python
# Without inferSchema (lazy):
df = spark.read.csv("file.csv")  # Instant, no execution
result = df.count()  # NOW it reads the file

# With inferSchema (eager):
df = spark.read.option("inferSchema", "true").csv("file.csv")
# Spark reads file here! (eager)
result = df.count()  # Reads file again!
```

**Performance impact** (100GB CSV):
```
Without inferSchema:
├─ read.csv() call: Instant
├─ count() call: 300 seconds
└─ Total: 300 seconds

With inferSchema:
├─ read.csv() call: 30 seconds (inference)
├─ count() call: 300 seconds
└─ Total: 330 seconds (10% slower!)

If reading 10 files: 300 seconds wasted on inference!
```

**Best practice**: Always provide explicit schema
```python
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
])
df = spark.read.schema(schema).csv("file.csv")  # Fast!
```

---

### Q6: How do you detect and fix data skew in a join?

**Answer**:
**Detection**:
In Spark UI, look at task durations for a stage:
- If slowest task is 10-100x faster than others, there's skew
- Example: Avg task 10s, but one task 600s
- Check "Shuffle Bytes by Partition" - if one is much larger, that's the skew

**Root cause**: Real-world data has power-law distribution
- Top 1% of users = 90% of events (skewed)
- When joining/grouping on user_id, "admin" user gets millions of events in one partition

**Fix 1: Add salt to skewed key**
```python
from pyspark.sql.functions import when, concat, lit, rand

# Split skewed key into 10 sub-partitions
salt_count = 10

events_salted = events.withColumn(
    "user_id_salted",
    when(col("user_id") == "admin",
         concat(col("user_id"), lit("_"), (rand() * salt_count).cast("int")))
    .otherwise(concat(col("user_id"), lit("_0")))
)

# Replicate "admin" in other table 10 times
# Then join on salted key
result = events_salted.join(users_salted, "user_id_salted")
```

**Fix 2: Use broadcast join** (if small table)
```python
result = large_df.join(broadcast(small_df), "id")
# No shuffle needed!
```

**Fix 3: Pre-aggregate skewed data**
```python
# Separate skewed and normal
skewed = events.filter(col("user_id") == "admin")
normal = events.filter(col("user_id") != "admin")

# Process differently
skewed_result = skewed.repartition(100, "event_id").join(users, ...)
normal_result = normal.join(broadcast(users), ...)

# Union results
result = skewed_result.union(normal_result)
```

---

## Key Takeaways for Interview

1. **Always check Spark UI first**: 80% of optimization is visible there
2. **Understand lazy vs eager**: spark.read is lazy (except inferSchema), actions trigger execution
3. **Narrow vs wide transformations**: Shuffles are expensive, narrow operations are fast
4. **Three most common optimizations**:
   - Filter/select before expensive operations (reduce data)
   - Use broadcast joins (no shuffle for small tables)
   - Fix data skew (salt key or pre-aggregate)
5. **Memory matters**: OutOfMemory usually means too much shuffle data or cache too large
6. **YAML logs + Spark UI**: Together they tell you exactly what's wrong
7. **Test before and after**: Always measure improvement with Spark UI

---

## Summary: The 90% Rule

**90% of on-field Spark issues fall into these categories**:

| Problem | Symptom | Solution |
|---------|---------|----------|
| **Slow job** | Takes 30+ min for small data | Filter early, column select, broadcast join |
| **OutOfMemory** | OOM error during shuffle | Increase memory, increase partitions, pre-filter |
| **Data skew** | Few tasks take 60x longer | Salt join key or pre-aggregate |
| **Multiple shuffles** | Job has 3+ shuffle stages | Combine operations or repartition once |
| **Cache not helping** | Cache takes memory but doesn't speedup | Cache only large data used multiple times |
| **Wrong join type** | Join is slow | Use broadcast if small table < 10MB |

**Debug flow**:
1. Check job duration in Spark UI
2. Find slowest stage
3. Look at task durations (check for skew)
4. Check GC time (memory pressure?)
5. Check YARN logs for errors
6. Apply one fix at a time
7. Measure improvement in Spark UI

---

## Final Interview Tips

1. **Show your thinking process**: "I would first check Spark UI to identify..."
2. **Ask clarifying questions**: "Is the data size fixed or variable?" "Do we care about cost or speed?"
3. **Provide multiple solutions**: "Option 1: broadcast join, Option 2: pre-partition, Option 3: increase memory"
4. **Mention trade-offs**: "Broadcast is fast but uses driver memory"
5. **Reference real tools**: "I'd look at Spark History Server UI, check the Executors tab for GC time"
6. **Be specific with code**: Show actual examples, not just concepts
7. **Mention monitoring**: "I would measure before and after using Spark UI to verify improvement"
