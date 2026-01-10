> Ratio table1 rows/ distinct keys): 1.00 Ratio table2 rows / distinct keys): 1.00 how can we have skew

Lets understand what's actually happening:

## 🎯 The Real Culprit: Hash Partitioning Skew (Not Data Skew)

With unique keys, your skew is coming from **HOW Spark partitions the data**, not from duplicate keys.

### Why Hash Partitioning Causes Skew

```python
# Spark's partitioning logic:
partition_id = hash(CUSIP, EFFECTIVEDATE) % num_partitions

# Problem: Hash functions don't guarantee uniform distribution!
# Even with 641M unique keys distributed across 4000 partitions:
# - Expected: ~160K rows per partition
# - Reality: Some partitions get 570M rows due to hash collisions
```

**Your specific case:**

- 641M unique keys → 4000 partitions
- Expected: 641M / 4000 = ~160K per partition ✅ (matches your average)
- Actual max: 570M rows in ONE partition ❌
- **This means ~89% of your data is hashing to a single partition!**

## 🔴 Why NULL Values Cause Extreme Skew

```python
# Example with NULLs:
# Table1: 570M rows with NULL CUSIP or NULL EFFECTIVEDATE
# Table1: 71M rows with valid keys

# What happens:
hash(NULL, "2024-01-01") → partition_X
hash(NULL, "2024-01-02") → partition_X  # Same partition!
hash(NULL, "2024-01-03") → partition_X  # Same partition!
# ... 570 million times → ALL go to partition_X

hash("ABC123", "2024-01-01") → partition_Y
hash("DEF456", "2024-01-02") → partition_Z
# ... 71 million rows distributed across other 3999 partitions
```

**Why this happens:**

1. `hash(NULL) = constant value` (usually 0 or a specific hash)
2. All NULL keys → same hash → same partition
3. Even though each row is "distinct" (different row IDs), they partition together
4. Your ratio stays 1.00 because you're counting rows, not partition distribution

## 🔍 Diagnostic: Find the Actual Cause

```python
# Check 1: Are NULLs the problem?
null_count_cusip = table1.filter(col("CUSIP").isNull()).count()
null_count_date = table1.filter(col("EFFECTIVEDATE").isNull()).count()
null_count_both = table1.filter(
    col("CUSIP").isNull() | col("EFFECTIVEDATE").isNull()
).count()

print(f"NULL CUSIP: {null_count_cusip:,}")
print(f"NULL DATE: {null_count_date:,}")
print(f"NULL either: {null_count_both:,}")

# If null_count_both ≈ 570M → NULLs are your problem ✅

# Check 2: Hash distribution analysis
from pyspark.sql.functions import spark_partition_id

partition_dist = table1.withColumn("partition_id", spark_partition_id())
.groupBy("partition_id")
.count()
.orderBy(col("count").desc())

partition_dist.show(20)

# Check 3: What's in the biggest partition?
biggest_partition_id = partition_dist.first()["partition_id"]

sample_from_big = table1.withColumn("partition_id", spark_partition_id())
.filter(col("partition_id") == biggest_partition_id)
.select("CUSIP", "EFFECTIVEDATE")
.limit(100)

sample_from_big.show(100, truncate=False)
# Look for patterns: Are they all NULL? All same value? Random?
```

## ✅ Solutions Based on Root Cause

### Solution 1: If NULLs Are the Problem (~570M nulls)

```python
# Separate NULL handling - don't let them join!
table1_valid = table1.filter(col("CUSIP").isNotNull() & col("EFFECTIVEDATE").isNotNull())
table1_null = table1.filter(col("CUSIP").isNull() | col("EFFECTIVEDATE").isNull())

table2_valid = table2.filter(col("CUSIP").isNotNull() & col("EFFECTIVEDATE").isNotNull())

# Join only valid data - NULLs won't match anyway in inner join
result = table1_valid.join(table2_valid, on=["CUSIP", "EFFECTIVEDATE"], how="inner")

# If you need outer join, handle NULLs separately and union
```

### Solution 2: If Hash Function Is Causing Skew (Rare)

```python
# Use range partitioning instead of hash partitioning
from pyspark.sql.functions import expr, concat_ws

# Create a composite string key for better distribution
table1_repart = table1.withColumn(
    "partition_key",
    concat_ws("_", col("CUSIP"), col("EFFECTIVEDATE").cast("string"))
).repartitionByRange(8000, "partition_key").drop("partition_key")

table2_repart = table2.withColumn(
    "partition_key",
    concat_ws("_", col("CUSIP"), col("EFFECTIVEDATE").cast("string"))
).repartitionByRange(8000, "partition_key").drop("partition_key")

result = table1_repart.join(table2_repart, on=["CUSIP", "EFFECTIVEDATE"])
```

### Solution 3: If One CUSIP Has Many Dates (Unlikely with 1.00 ratio)

```python
# Check if specific CUSIPs dominate
cusip_dist = table1.groupBy("CUSIP").count().orderBy(col("count").desc())
cusip_dist.show(20)

# If one CUSIP has millions of rows, use salting on that CUSIP
```

### Solution 4: Force Repartitioning with More Partitions

```python
# Increase partitions dramatically to spread the hash collision
spark.conf.set("spark.sql.shuffle.partitions", "20000")  # 5x increase

# Or manually repartition
table1_balanced = table1.repartition(20000, "CUSIP", "EFFECTIVEDATE")
table2_balanced = table2.repartition(20000, "CUSIP", "EFFECTIVEDATE")

result = table1_balanced.join(table2_balanced, on=["CUSIP", "EFFECTIVEDATE"])
```

## 🎯 Most Likely Scenario

Given your numbers:

- Max partition: **570M rows** (88.9% of 641M)
- This is NOT random - it's too extreme

**I'm 90% confident you have ~570M rows with NULL keys.**

Run this immediately:

```python
print(f"Total rows: {table1.count():,}")
print(f"NULL keys: {table1.filter(col('CUSIP').isNull() | col('EFFECTIVEDATE').isNull()).count():,}")
print(f"Valid keys: {table1.filter(col('CUSIP').isNotNull() & col('EFFECTIVEDATE').isNotNull()).count():,}")
```

**If NULLs ≈ 570M**, just filter them out before the join - they won't match anything anyway!

```python
# Simple fix:
result = table1.filter(col("CUSIP").isNotNull() & col("EFFECTIVEDATE").isNotNull())
.join(
    table2.filter(col("CUSIP").isNotNull() & col("EFFECTIVEDATE").isNotNull()),
    on=["CUSIP", "EFFECTIVEDATE"],
    how="inner"
)
```