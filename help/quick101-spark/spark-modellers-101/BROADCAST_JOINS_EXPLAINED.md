# Broadcast Joins: The Fast Track to Joining Data

> **Key Takeaway**: Broadcast joins are 10-100x faster than shuffle joins. Use them whenever one table is small (< 10MB).

---

## Quick Answer

| Join Type | When to Use | Speed | Memory Usage | Network I/O |
|-----------|-----------|-------|--------------|-------------|
| **Broadcast Join** | Small DF < 10MB | ⚡⚡⚡ FAST | High on driver | Low (just small DF) |
| **Shuffle Join** | Both DFs large | 🐢 SLOW | Low | Very High (both DFs) |
| **Sort-Merge Join** | Pre-partitioned | ⚡ MEDIUM | Medium | High (both DFs) |

---

## What is a Broadcast Join?

### Definition

A **broadcast join** copies a small table to every executor, then each executor does a local join without shuffling.

### Visual: Broadcast vs Shuffle Join

```
SHUFFLE JOIN (❌ Slow):
┌─────────────────────────────────────────────────────┐
│                      NETWORK                        │
│  Large DF1    Network    Large DF2                 │
│  (100GB)   ←─────────→   (50GB)                    │
│  Repartitioned  by key  Repartitioned              │
│  Executor 1         Executor 2                     │
│  Executor 3         Executor 4                     │
└─────────────────────────────────────────────────────┘
Data moved: 150GB across network ❌

BROADCAST JOIN (✅ Fast):
┌──────────────────────────────────────────────────────┐
│                      BROADCAST                      │
│  Small DF       Broadcast        Large DF1          │
│  (100MB)   ────→ to all          (100GB)            │
│            executors             (stays in place)   │
│                                                     │
│ Executor 1  Executor 2  Executor 3  Executor 4   │
│ [Small]     [Small]     [Small]     [Small]       │
│ [Local DF]  [Local DF]  [Local DF]  [Local DF]   │
│   ↓           ↓           ↓           ↓            │
│  JOIN       JOIN        JOIN        JOIN          │
│   ↓           ↓           ↓           ↓            │
│ Result      Result      Result      Result        │
└──────────────────────────────────────────────────────┘
Data moved: 100MB broadcast + 100GB DF partitions ✅
```

---

## How Broadcast Joins Work

### Step-by-Step Execution

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastDemo").getOrCreate()

# Small lookup table
dept_df = spark.read.csv("departments.csv")
# Data:
# ├─ dept_id: 1, name: Sales
# ├─ dept_id: 2, name: Engineering
# └─ ...
# Size: 100MB

# Large fact table
emp_df = spark.read.csv("employees.csv")
# Data:
# ├─ emp_id: 1, dept_id: 2, name: Alice
# ├─ emp_id: 2, dept_id: 1, name: Bob
# └─ ... (1 billion rows)
# Size: 100GB

# Broadcast join
result = emp_df.join(broadcast(dept_df), emp_df.dept_id == dept_df.dept_id)
```

**Execution Flow**:

```
Phase 1: Broadcast Preparation (on driver)
├─ Collect dept_df to driver memory
├─ Serialize dept_df to bytes
└─ Size: 100MB

Phase 2: Distribute (driver → executors)
├─ Executor 1: Receive and deserialize dept_df (100MB)
├─ Executor 2: Receive and deserialize dept_df (100MB)
├─ Executor 3: Receive and deserialize dept_df (100MB)
├─ Executor 4: Receive and deserialize dept_df (100MB)
└─ Network: 4 × 100MB = 400MB (one copy to each executor)

Phase 3: Local Join (on each executor, in parallel)
├─ Executor 1:
│  ├─ Has local partition of emp_df: 250M rows
│  ├─ Has copy of dept_df: all rows
│  ├─ Performs hash join locally
│  └─ Result: 250M joined rows (executor 1's share)
│
├─ Executor 2: Same process (250M rows + broadcast)
├─ Executor 3: Same process (250M rows + broadcast)
└─ Executor 4: Same process (250M rows + broadcast)

Phase 4: Combine Results
├─ Each executor has its local join results
├─ No reshuffling needed
└─ Final result: 1B rows (joined)
```

---

## When to Use Broadcast Joins

### Criteria for Using Broadcast

```python
# ✅ BROADCAST if:
# 1. One table is small (< spark.sql.autoBroadcastJoinThreshold)

small_df = spark.read.csv("small_table.csv")  # 50MB
large_df = spark.read.csv("large_table.csv")  # 100GB

# Catalyst will auto-broadcast small_df
result = large_df.join(small_df, "key")  # Broadcast join!

# 2. Need to explicitly force broadcast
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "key")

# 3. Table is dimension table (reference data)
# Examples:
# - Country lookup (200 countries, 200 bytes each)
# - Currency conversion (180 currencies)
# - Product master (10k products, 10MB)
# - Department codes (100 depts, 100KB)

# ❌ DON'T BROADCAST if:
# - Table is too large (> 10GB typically)
# - Driver doesn't have enough memory
# - Network bandwidth is limited
# - Need to avoid memory overhead on executors
```

### Auto-Broadcast Threshold

```python
# Default: 10MB
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # "10485760" (bytes)

# Spark automatically broadcasts if table < 10MB
small_df = spark.read.csv("small.csv")  # 5MB
large_df = spark.read.csv("large.csv")  # 100GB

result = large_df.join(small_df, "id")
# Catalyst sees: small_df < 10MB
# Automatically uses broadcast join ✅

# Override the threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100MB

# Now Spark broadcasts tables up to 100MB
# ⚠️ Be careful! Executor memory must accommodate

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
# Catalyst won't auto-broadcast
# Must explicitly use broadcast() or get shuffle join
```

---

## Performance Comparison: Broadcast vs Shuffle Join

### Scenario: Joining 100GB with 1GB

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", 4) \
    .appName("JoinComparison") \
    .getOrCreate()

large_df = spark.read.csv("large_100gb.csv")      # 100GB
medium_df = spark.read.csv("medium_1gb.csv")      # 1GB

# Test 1: Shuffle Join (default)
result1 = large_df.join(medium_df, "customer_id")
# result1.collect()  # ~60 seconds

# Test 2: Broadcast Join
result2 = large_df.join(broadcast(medium_df), "customer_id")
# result2.collect()  # ~6 seconds

# Comparison:
# Shuffle join: 60 seconds
# Broadcast join: 6 seconds
# Speedup: 10x faster! ⚡
```

### Execution Time Breakdown

**Shuffle Join (❌ Slower)**:
```
Shuffle Phase:
├─ Map: Partition large_df & medium_df by key (5 sec)
├─ Write to disk: large + medium = 101GB (40 sec)
├─ Network transfer: 101GB across cluster (45 sec)
├─ Read from disk: 101GB (40 sec)
└─ Reduce: Join matching keys (10 sec)

Total: 140 seconds
```

**Broadcast Join (✅ Faster)**:
```
Broadcast Phase:
├─ Collect medium_df to driver (5 sec)
├─ Broadcast to 4 executors: 1GB × 4 = 4GB (8 sec)
└─ Local hash joins on each executor (3 sec)

Total: 16 seconds

Speedup: 140 / 16 = 8.75x faster
```

### Network I/O Comparison

```
100GB + 1GB = 101GB total data

Shuffle Join:
├─ Driver → Executors: 101GB broadcast (for repartitioning)
├─ Executors → Disk: 101GB write
├─ Disk → Executors: 101GB read
└─ Total I/O: 303GB ❌

Broadcast Join:
├─ Driver → Executors: 1GB broadcast (only small table)
├─ Executors → Disk: 0GB (no shuffle)
├─ Disk → Executors: 0GB (no shuffle)
└─ Total I/O: 1GB ✅

Difference: 303x less I/O! ⚡⚡⚡
```

---

## Broadcast Join Examples

### Example 1: Simple Broadcast Join

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()

# Read data
orders = spark.read.parquet("s3://bucket/orders.parquet")  # 500GB
customers = spark.read.parquet("s3://bucket/customers.parquet")  # 100MB

# Broadcast the smaller table
result = orders.join(
    broadcast(customers),
    orders.customer_id == customers.id
)

# Result contains all orders with customer information
result.show()

# Performance:
# Without broadcast: 2 minutes (shuffle)
# With broadcast: 10 seconds (broadcast) ⚡
```

### Example 2: Broadcast with Multiple Joins

```python
# When joining multiple tables, broadcast the small ones

# Tables:
# - orders: 500GB
# - customers: 100MB (broadcast)
# - products: 50MB (broadcast)
# - categories: 5MB (broadcast)

from pyspark.sql.functions import broadcast

result = (orders
    .join(broadcast(customers), "customer_id")
    .join(broadcast(products), "product_id")
    .join(broadcast(categories), "category_id")
)

# All 3 broadcasts happen:
# 1. customers (100MB) → broadcast to executors
# 2. products (50MB) → broadcast to executors
# 3. categories (5MB) → broadcast to executors
# Total broadcast: 155MB (tiny!)
# Result: All joins are fast, no shuffle

# Compare to shuffle join:
# Shuffle join would move: 500GB + 100MB + 50MB + 5MB
# = 500GB network transfer (slow!)
# Broadcast: 155MB network transfer (fast!)
# Speedup: 3200x less network I/O
```

### Example 3: Broadcast Lookup Table

```python
# Use broadcast for dimension tables (lookups)

spark = SparkSession.builder.appName("LookupExample").getOrCreate()

# Fact table: Sales transactions (10GB)
sales = spark.read.csv("sales.csv", header=True)

# Dimension tables (small)
products = spark.read.csv("products.csv", header=True)  # 50MB
regions = spark.read.csv("regions.csv", header=True)    # 2MB
channels = spark.read.csv("channels.csv", header=True)  # 500KB

from pyspark.sql.functions import broadcast, col

# Broadcast multiple dimension tables
enriched_sales = (sales
    .join(broadcast(products), "product_id")
    .join(broadcast(regions), "region_id")
    .join(broadcast(channels), "channel_id")
    .select(
        col("sales.amount"),
        col("products.name").alias("product_name"),
        col("regions.country").alias("country"),
        col("channels.name").alias("channel_name")
    )
)

enriched_sales.show(10)
enriched_sales.write.parquet("s3://bucket/enriched_sales")
```

### Example 4: Broadcast with Conditions

```python
# Sometimes you need to check if a value exists in a table

from pyspark.sql.functions import broadcast, col

# Sales transactions
sales = spark.read.csv("sales.csv", header=True)  # 50GB

# Valid products
valid_products = spark.read.csv("valid_products.csv", header=True)  # 10MB

# Join and filter
valid_sales = sales.join(
    broadcast(valid_products),
    (sales.product_id == valid_products.id) &
    (valid_products.status == "ACTIVE"),
    how="inner"  # Inner join = only matching products
)

valid_sales.show()

# Alternative: Use isin() with broadcast set
from pyspark.sql.functions import isin

valid_product_ids = spark.broadcast(
    set(valid_products.select("id").rdd.map(lambda x: x[0]).collect())
)

valid_sales = sales.filter(col("product_id").isin(valid_product_ids.value))
```

---

## Broadcast Join Limitations

### Limitation 1: Driver Memory

**Problem**: Driver must collect entire table before broadcasting

```python
# ❌ FAILS - Driver runs out of memory
huge_df = spark.read.csv("huge_table.csv")  # 50GB

# Trying to broadcast:
result = large_df.join(broadcast(huge_df), "id")
# Error: java.lang.OutOfMemoryError
# Reason: Driver tried to collect 50GB table

# ✅ SOLUTION - Don't broadcast large tables
# Use shuffle join instead:
result = large_df.join(huge_df, "id")  # Shuffle join
```

**Driver Memory Limits**:
```
Typical driver memory: 4GB (spark.driver.memory)

Broadcast limit: ~1-2GB
└─ Leave room for other driver tasks

Safe broadcast table size:
├─ 100MB: Always safe ✅
├─ 500MB: Usually safe ✅
├─ 1GB: Getting risky ⚠️
├─ 2GB+: Too large ❌
└─ 5GB+: Definitely fails ❌
```

### Limitation 2: Network Bandwidth

**Problem**: Broadcasting to many executors can saturate network

```
Scenario: 1GB broadcast to 100 executors
├─ 1GB × 100 executors = 100GB total data transfer
├─ Network bandwidth: 10 Gbps = 1.25 GB/sec
├─ Transfer time: 100 GB ÷ 1.25 GB/sec = 80 seconds
└─ Join time: Minimal (2 seconds)
└─ Total: 82 seconds (broadcast time dominates)

Better to shuffle if:
├─ Broadcast table: > 500MB
├─ Number of executors: > 50
├─ Network: < 10 Gbps
```

### Limitation 3: Executor Memory

**Problem**: Each executor gets a copy, can exhaust memory

```python
# ❌ PROBLEM - Memory overflow on executors
broadcast_df = spark.read.csv("lookup.csv")  # 500MB

large_df = spark.read.csv("huge_file.csv")  # 100GB

# Each executor gets:
# - Its partition of large_df: ~25GB (4 executors)
# - Copy of broadcast_df: 500MB
# - Shuffle buffers: ~2GB
# - JVM overhead: ~1GB
# Total per executor: 28.5GB

# But executor memory: 8GB ❌ RUNS OUT OF MEMORY!

# ✅ SOLUTION - Be aware of executor memory pressure
# Use smaller broadcast tables or increase executor memory
spark = SparkSession.builder \
    .config("spark.executor.memory", "32g") \
    .appName("App") \
    .getOrCreate()
```

---

## When NOT to Use Broadcast Joins

### Case 1: Both Tables Are Large

```python
# ❌ DON'T broadcast
df1 = spark.read.csv("large_100gb.csv")   # 100GB
df2 = spark.read.csv("large_50gb.csv")    # 50GB

result = df1.join(broadcast(df2), "id")
# ❌ Broadcasting 50GB to all executors is wasteful!

# ✅ Use shuffle join instead
result = df1.join(df2, "id")
# Shuffle join is designed for large-large joins
```

### Case 2: Pre-Partitioned Data

```python
# ❌ WASTEFUL - Broadcasting when data is already partitioned
df1 = spark.read.csv("df1.csv", mode="OVERWRITE") \
    .repartition("customer_id")  # Pre-partitioned

df2 = spark.read.csv("df2.csv") \
    .repartition("customer_id")  # Pre-partitioned

result = df1.join(broadcast(df2), "customer_id")
# ❌ Broadcasting unnecessary when data is co-located!

# ✅ Use sort-merge join instead
result = df1.sortByKey().join(df2.sortByKey())
# Data is already in the right place
# No shuffle needed
# Faster than broadcast + join
```

### Case 3: Joining Stream + Batch

```python
# Broadcasting to streaming jobs is tricky
# (beyond scope of this guide, but avoid if possible)
```

---

## Monitoring Broadcast Joins

### Spark UI Indicators

```
Broadcast section shows:
├─ Blocks Broadcast: Number of broadcast variables
├─ Broadcast Bytes: Total size of broadcast variables
└─ Example: 5 broadcasts, 2GB total

If you see large broadcast sizes:
├─ Check if tables are appropriate size
├─ Consider whether broadcast is optimal
└─ Monitor driver memory usage
```

### Checking Join Type in Execution Plan

```python
df1 = spark.read.csv("large.csv")
df2 = spark.read.csv("small.csv")

result = df1.join(df2, "id")

# See what join type Spark chose:
result.explain()

# Look for in output:
# ✅ BroadcastHashJoin ← Good!
# ✅ BroadcastExchange ← Broadcasting small table
# ❌ SortMergeJoin ← Shuffle join (slower)
# ❌ ShuffledHashJoin ← Shuffle join (slower)
```

---

## Best Practices

- [ ] Always broadcast tables < 10MB
- [ ] Explicitly use `broadcast()` for clarity
- [ ] Check driver memory before broadcasting large tables
- [ ] Use broadcast for dimension/lookup tables
- [ ] Monitor broadcast size in Spark UI
- [ ] Test both broadcast and shuffle join to compare
- [ ] Pre-filter large table before joining
- [ ] Use multiple broadcasts for multiple dimension tables
- [ ] Don't broadcast if table is larger than driver memory
- [ ] Avoid broadcasting to very large clusters (> 100 nodes)

---

## Interview Questions

**Q1: When would you use a broadcast join?**
A: When one table is small (< 10MB) and the other is large. Broadcast copies the small table to each executor, allowing local joins without shuffling. This is 10-100x faster than shuffle joins.

**Q2: What's the size limit for broadcast joins?**
A: Practically 1-2GB (limited by driver memory). Spark's default auto-broadcast threshold is 10MB, but you can increase it. Beyond 2GB, you risk driver OutOfMemory.

**Q3: What's the difference between broadcast and shuffle joins?**
A: Broadcast join: copies small table to all executors, local joins (fast, no shuffle). Shuffle join: repartitions both tables by key, then joins (slow, network intensive).

**Q4: When would you NOT use a broadcast join?**
A: When both tables are large (avoid memory overhead), when data is pre-partitioned (use sort-merge join), or when broadcast table is > 2GB (driver memory limit).

**Q5: How do you force a broadcast join?**
A: Use `broadcast()` function: `df1.join(broadcast(df2), join_condition)`. This explicitly tells Spark to broadcast df2 regardless of size.

**Q6: What happens if you broadcast a table that's too large?**
A: Driver runs out of memory and the job fails with OutOfMemoryError. Each executor also needs a copy, so executor memory pressure increases.

---

## Performance Summary

| Scenario | Best Join | Time | Why |
|---|---|---|---|
| 100GB ⛔ 100MB | Broadcast | 10s | No shuffle, broadcast is small |
| 100GB ⛔ 50GB | Shuffle | 60s | Both large, must shuffle |
| 100GB ⛔ 1GB (but pre-partitioned) | Sort-Merge | 30s | Data co-located, no shuffle |
| 100GB ⛔ 5MB | Broadcast | 5s | Tiny broadcast, very fast |
| 100GB ⛔ 5GB | Shuffle | 120s | Broadcast too large for driver |
