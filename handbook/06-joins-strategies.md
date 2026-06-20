# 06 - Join Strategies

## Join Types & Strategies

When you write:
```python
df_large.join(df_medium, "key")
```

Spark chooses how to execute this. The **strategy** determines:
- How much data moves (shuffle)
- How much memory is needed
- How fast the job completes

**Key insight:** Picking the RIGHT strategy = 10-100x faster!

---

## Table of Contents
1. [Join Strategy Overview](#join-strategy-overview)
2. [Broadcast Hash Join](#broadcast-hash-join)
3. [Sort-Merge Join](#sort-merge-join)
4. [Shuffle Hash Join](#shuffle-hash-join)
5. [Cartesian Join](#cartesian-join)
6. [Choosing the Right Strategy](#choosing-the-right-strategy)
7. [Real Examples](#real-examples)

---

## Join Strategy Overview

🔗 **Related:** Section 04 - Memory Spill | Section 05 - Shuffle Optimization

### The Four Main Strategies

```
┌─────────────────────────────────────────────┐
│          JOIN STRATEGIES IN SPARK            │
├─────────────────────────────────────────────┤
│                                              │
│ 1. BROADCAST HASH JOIN                       │
│    ├─ When: Small table (< 1GB)              │
│    ├─ How: Broadcast to all executors        │
│    ├─ Cost: Low (no shuffle small table)     │
│    └─ Speed: Fastest!                        │
│                                              │
│ 2. SORT-MERGE JOIN                          │
│    ├─ When: Both tables large, already sorted│
│    ├─ How: Sort both, merge in sequence      │
│    ├─ Cost: Medium (shuffle + sort)          │
│    └─ Speed: Fast (especially if pre-sorted) │
│                                              │
│ 3. SHUFFLE HASH JOIN                         │
│    ├─ When: Both tables large, not sorted    │
│    ├─ How: Shuffle both, hash match          │
│    ├─ Cost: High (shuffle both)              │
│    └─ Speed: Slower                          │
│                                              │
│ 4. CARTESIAN JOIN                            │
│    ├─ When: No join condition (cross join)   │
│    ├─ How: Every row with every row          │
│    ├─ Cost: VERY HIGH (n × m rows!)          │
│    └─ Speed: VERY SLOW or crashes!           │
│                                              │
└─────────────────────────────────────────────┘
```

---

## Broadcast Hash Join

### When to Use

- **Small table:** < 1GB typically
- **Large table:** Any size
- **Goal:** Avoid shuffling large table

### How It Works

```
Small Table (500MB)
    ↓
Serialize & Broadcast to all executors
    ↓ (Each executor receives a copy)
Load into hash map in memory
    ↓
Large Table (100GB)
    ↓
Shuffle and partition? NO! Stay as-is
    ↓
Hash lookup for each record
    ↓
Result: Join complete!
```

### Memory Requirement

```
Per executor:
├─ Small table (broadcast): 500MB
├─ Execution memory: 200MB (for hashing)
├─ Other overhead: 100MB
└─ Total: ~800MB (safe with 4GB executor)

With 10 executors:
├─ Broadcast copies: 10 × 500MB = 5GB total (but same data on each)
└─ Network: Broadcast efficient
```

### Code

```python
from pyspark.sql.functions import broadcast

small_table = spark.read.parquet("small.parquet")  # 500MB
large_table = spark.read.parquet("large.parquet")  # 100GB

result = large_table.join(
    broadcast(small_table),
    "key"
)

# Execution:
# 1. small_table broadcasted to all executors (5GB network)
# 2. large_table stays in current partitions
# 3. Hash lookup happens locally (NO shuffle!)
# 4. Result assembled
```

### Spark Automatic Detection

```python
# Spark AUTOMATICALLY uses broadcast if small table detected
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB

result = large_table.join(small_table, "key")
# If small_table < 10MB: Automatic broadcast
# If >= 10MB: Uses different strategy

# You can increase threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024 * 1024 * 1024)  # 1GB

# Or disable automatic broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

### Advantages & Disadvantages

| Aspect | Details |
|--------|---------|
| **Speed** | Fastest (hash lookup O(1)) |
| **Memory** | Low per executor (broadcast cached) |
| **Network** | Broadcast to all (one-time cost) |
| **Best for** | Small dimension tables |
| **Drawback** | Only works if one table fits in memory |

---

## Sort-Merge Join

### When to Use

- **Both tables large** (both > 1GB)
- **Pre-sorted data:** Even better
- **Repeated joins:** Especially efficient

### How It Works

```
Table 1 (100GB)
    ↓
SHUFFLE to 200 partitions
    ↓
SORT each partition by key
    ↓
                    Table 2 (50GB)
                        ↓
                    SHUFFLE to 200 partitions
                        ↓
                    SORT each partition by key
                        ↓
Merge join:
├─ Partition 1: [1,2,3] from Table1 + [1,2,4] from Table2 = [1-1,2-2,3-null]
├─ Partition 2: [4,5,6] from Table1 + [5,6,7] from Table2 = [4-null,5-5,6-6]
└─ ...
```

### Memory Requirement

```
Per executor during merge:
├─ Execution memory for merge: Minimal (2 pointers)
├─ Storage for current partition: ~200MB
├─ Buffers: ~50MB
└─ Total: ~300MB (safe with 4GB executor)

Key: No need to hold entire table in memory!
```

### Code

```python
table1 = spark.read.parquet("table1.parquet")  # 100GB
table2 = spark.read.parquet("table2.parquet")  # 50GB

# Default: Spark chooses sort-merge for large + large joins
result = table1.join(table2, "key")

# Explicit sort-merge (rarely needed)
spark.conf.set("spark.sql.join.preferSortMergeJoin", True)

# Pre-sorted data (even faster)
table1_presorted = table1.sortWithinPartitions("key")
table2_presorted = table2.sortWithinPartitions("key")

result = table1_presorted.join(table2_presorted, "key")
```

### Advantages & Disadvantages

| Aspect | Details |
|--------|---------|
| **Speed** | Fast (O(n log n) due to sort) |
| **Memory** | Low (streaming merge) |
| **Network** | Both tables shuffle (one-time cost) |
| **Best for** | Large + large joins |
| **Benefit** | Scales to 1TB+ joins |
| **If pre-sorted** | Avoid sort cost (much faster!) |

---

## Shuffle Hash Join

### When to Use

- **Both tables large**, one moderately so
- **No broadcast viable** (both too large)
- **Not pre-sorted** (no benefit to sort-merge)

### How It Works

```
Table 1 (100GB)
    ↓
SHUFFLE to 200 partitions
    ↓
BUILD hash table in memory
    ↓
                    Table 2 (50GB)
                        ↓
                    SHUFFLE to 200 partitions
                        ↓
Probe:
├─ For each record in Table2, lookup in hash table
└─ If found, emit joined record
```

### Memory Requirement

```
Per executor:
├─ Hash table (from smaller table): ~250MB
├─ Probe buffer: ~50MB
└─ Total: ~300MB

Per partition:
├─ Must fit build table partition in hash table
└─ If partition too large, spill occurs
```

### Code

```python
table1 = spark.read.parquet("table1.parquet")  # 100GB
table2 = spark.read.parquet("table2.parquet")  # 40GB

# Spark uses this by default when broadcast isn't viable
result = table1.join(table2, "key")

# Force shuffle hash join
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
result = table1.join(table2, "key")
```

### Advantages & Disadvantages

| Aspect | Details |
|--------|---------|
| **Speed** | Medium (hash lookup O(1), but build cost) |
| **Memory** | Medium (build table in hash) |
| **Network** | Both tables shuffle |
| **Best for** | When sort-merge isn't preferred |
| **Drawback** | Memory spill if partition too large |

---

## Cartesian Join

### What It Is (And Why It's Dangerous!)

```python
# No join condition = Cartesian product
df1.join(df2)  # Joins on entire rows! (ERROR or extremely slow)

# Or explicit:
df1.crossJoin(df2)  # Every row from df1 with every row from df2
```

### Example Disaster

```
df1: 1,000,000 rows
df2: 100,000 rows

Result: 1,000,000 × 100,000 = 100 BILLION rows!

If 50MB per row: 5 Exabytes!
```

### When It's Needed (Rare)

```python
# Scenario: Generate all combinations
customers = spark.read.parquet("customers.parquet")  # 100K
products = spark.read.parquet("products.parquet")    # 1M

# Generate all customer-product pairs (for recommendations)
combinations = customers.crossJoin(products)

# Results: 100K × 1M = 100B rows
# You MUST be intentional about this!

# Safer: Add filtering
combinations = customers.crossJoin(
    products.select("product_id", "category")
).filter(col("customer_tier") == col("product_category"))
# Reduces result size significantly
```

### Avoiding Accidental Cartesian

```python
# BAD: Forgot join condition
result = df1.join(df2)  # Cartesian!

# GOOD: Always specify join key
result = df1.join(df2, "customer_id")

# If you must do cartesian, be explicit
result = df1.crossJoin(df2)  # Clear intent
```

---

## Choosing the Right Strategy

### Decision Tree

```
Start: You need to join two tables

1. Is one table < 1GB?
   YES → Use BROADCAST HASH JOIN
         ├─ Fastest
         ├─ Lowest memory
         └─ Best option!

   NO → Continue to 2

2. Are both tables already sorted by join key?
   YES → Use SORT-MERGE JOIN (skip the sort!)
         ├─ Very fast
         ├─ Safe for huge tables
         └─ Ideal for repeated joins

   NO → Continue to 3

3. Is one table only slightly larger than broadcast threshold?
   YES → Still use BROADCAST if possible
         └─ Increase autoBroadcastJoinThreshold

   NO → Use SORT-MERGE JOIN (Spark default)
        ├─ Safe, predictable
        ├─ Works for any size
        └─ Recommended for large tables
```

### Configuration Summary

```python
# Increase broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024 * 1024 * 1024)  # 1GB

# Prefer sort-merge over shuffle hash
spark.conf.set("spark.sql.join.preferSortMergeJoin", True)

# Shuffle partitions for large joins
spark.conf.set("spark.sql.shuffle.partitions", 200)
```

---

## Real Examples

### Example 1: Customer-Dimension Join (Broadcast)

```python
# Scenario: Join 1B transactions with 100K customer dimensions
# Expected: Fast!

from pyspark.sql.functions import broadcast

transactions = spark.read.parquet("s3://data/transactions/")  # 500GB
customers = spark.read.parquet("s3://data/customers/")        # 100MB

# Explicit broadcast
result = transactions.join(
    broadcast(customers),
    "customer_id"
).select("transaction_id", "customer_id", "name", "amount")

# Execution:
# 1. Broadcast customers to all executors (~100MB per executor)
# 2. Transactions stay in partitions
# 3. Hash lookup (fast!)
# 4. Result: Customer names joined with transactions

result.write.parquet("s3://output/transactions_with_names/")

# Time: ~2 minutes (broadcast is instant, join is fast)
```

### Example 2: Large-Large Join (Sort-Merge)

```python
# Scenario: Join two 100GB datasets
# Expected: Medium speed, safe memory

orders = spark.read.parquet("s3://data/orders/")        # 100GB
shipments = spark.read.parquet("s3://data/shipments/")  # 100GB

# Configure for this job
spark.conf.set("spark.sql.join.preferSortMergeJoin", True)
spark.conf.set("spark.sql.shuffle.partitions", 200)

# Join (Spark auto-selects sort-merge)
result = orders.join(
    shipments,
    ["order_id", "date"],
    "inner"
).select("order_id", "customer_id", "amount", "ship_date")

# Execution:
# 1. Shuffle both tables to 200 partitions by join key
# 2. Sort each partition by key
# 3. Stream merge (partition 1 from orders + partition 1 from shipments)
# 4. Continue for all 200 partitions

result.write.parquet("s3://output/orders_with_shipments/")

# Time: ~10-15 minutes (safe even with 100GB each)
```

### Example 3: Pre-Sorted Join (Fast!)

```python
# Scenario: Same as Example 2, but we already have sorted data
# Expected: Much faster!

# Read pre-sorted data
orders_sorted = spark.read.parquet("s3://data/orders_by_date/")
shipments_sorted = spark.read.parquet("s3://data/shipments_by_date/")

# Both already partitioned and sorted by join key
# Spark can skip shuffle and sort!

result = orders_sorted.join(
    shipments_sorted,
    ["order_id", "date"],
    "inner"
)

# Execution:
# 1. Skip shuffle (already partitioned!)
# 2. Skip sort (already sorted!)
# 3. Directly merge
# 4. Much faster!

result.write.parquet("s3://output/orders_with_shipments_fast/")

# Time: ~3-5 minutes (5x faster than Example 2!)
```

### Example 4: Detecting Join Strategy

```python
# See which strategy Spark chose

orders = spark.read.parquet("orders.parquet")  # 100GB
customers = spark.read.parquet("customers.parquet")  # 200MB

result = orders.join(customers, "customer_id")

# View execution plan
result.explain(extended=False)

# Output includes join strategy:
# BroadcastHashJoin - Fast!
# or
# SortMergeJoin - Medium speed
# or
# ShuffledHashJoin - Slower

# If result is SortMergeJoin but you want broadcast:
result2 = orders.join(
    broadcast(customers),
    "customer_id"
)

# Now it's BroadcastHashJoin (faster!)
```

### Example 5: Avoiding Accidental Cartesian

```python
# BAD: Forgot join condition
result = df1.join(df2)  # Cartesian! 100M × 50M = 5T rows

# Better: Always explicit
result = df1.join(df2, "key")  # Specific join condition

# If you actually need cartesian:
result = df1.crossJoin(df2)  # Clear intent

# With filtering:
result = df1.crossJoin(df2)\
    .filter(col("category") == col("product_type"))\
    .select("id", "name", "product_id", "price")
# More efficient as Spark knows to filter early
```

---

## Join Performance Checklist

```
□ Is one table < 1GB?
  YES → Use broadcast (explicit or auto)

□ Are tables pre-sorted or pre-partitioned?
  YES → Use sort-merge (very fast)

□ Are both tables > 1GB?
  YES → Configure for sort-merge join

□ Verify no accidental cartesian joins
  □ All joins have explicit ON condition

□ Check join strategy in explain()
  □ Expect BroadcastHashJoin or SortMergeJoin

□ Adjust shuffle.partitions if slow
  □ Typical: 200 per 100GB of data

□ Monitor network I/O during join
  □ Watch for network saturation
```

---

## Key Takeaways

✅ **Broadcast < 1GB tables** - 10x faster than regular join
✅ **Sort-merge for large joins** - Safe, scales to 1TB+
✅ **Pre-sort if possible** - Avoids sort cost
✅ **Avoid accidental cartesian** - Always explicit join condition
✅ **Check explain() output** - Verify strategy choice
✅ **Match partitions before join** - If possible

---

## Next Steps

1. **Audit your current joins** - Are they using right strategy?
2. **Add broadcast hints** - For tables < 1GB
3. **Pre-sort large tables** - If reused in joins
4. **Move to Section 07** - Real-world ETL pipelines

---

**Remember:** A badly chosen join strategy can turn a 2-minute job into a 2-hour job. Invest in choosing wisely!
