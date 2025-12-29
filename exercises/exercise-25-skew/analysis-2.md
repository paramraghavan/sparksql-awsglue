# What happens when Not Null Cusip, will it get clustered to same partition ?

## ❌ Question 1: NO - Different Dates Should NOT Hash to Same Partition

Your assumption is **INCORRECT**. Here's what actually happens:

```python
# When joining on BOTH columns, Spark hashes the COMPOSITE key:

hash(('SameCusip111', '2024-01-01')) → partition_157
hash(('SameCusip111', '2024-01-02')) → partition_892  # DIFFERENT!
hash(('SameCusip111', '2024-01-03')) → partition_3  # DIFFERENT!
hash(('DifferentCusip', '2024-01-01')) → partition_2456  # DIFFERENT!
```

The hash function operates on **BOTH values together**, not just CUSIP alone.

## 🤔 So Why Are Your 570M Rows in ONE Partition?

Given that you have:

- 570M rows with CUSIP `'3132M9W75_2024-11-01'`
- Each with DIFFERENT `EFFECTIVEDATE` values
- Ratio = 1.00 (all unique keys)

**There are only 3 possible explanations:**

### Explanation 1: Input Data Already Partitioned by CUSIP (MOST LIKELY)

```python
# Your source data (Parquet/table) might be partitioned like:
# s3://bucket/table/cusip=3132M9W75_2024-11-01/part-001.parquet  (570M rows)
# s3://bucket/table/cusip=ABC123/part-002.parquet
# s3://bucket/table/cusip=DEF456/part-003.parquet

# When Spark reads this, it creates partitions based on file structure
# → Partition 0: All 570M rows from cusip=3132M9W75_2024-11-01 file
# → Partition 1: Rows from cusip=ABC123 file
# etc.
```

**Check this:**

```python
# See how your input data is partitioned
print("Table1 input partitions:", table1.rdd.getNumPartitions())
print("Table1 partition sizes:")
table1.rdd.glom().map(len).collect()
```

### Explanation 2: Spark Optimizer Using Broadcast/Different Strategy

```python
# Spark might decide:
# "Table2 is small, I'll broadcast it instead of shuffling"
# → Doesn't repartition Table1
# → Table1 keeps its original skewed partitioning
```

**Check this in Spark UI:**

- Look for "BroadcastHashJoin" in the physical plan
- If you see this, one table isn't being shuffled

### Explanation 3: Hash Collision (VERY UNLIKELY)

Extremely rare that 570M different composite keys all hash to the same partition.

## ✅ Question 2: Does Join Key Order Matter?

### Short Answer: **NO** (in theory), **MAYBE** (in practice)

```python
# These should produce identical results and same partitioning:
on = ["CUSIP", "EFFECTIVEDATE"]
on = ["EFFECTIVEDATE", "CUSIP"]
```

**Why it shouldn't matter:**

- Spark's hash function should be order-independent for the join keys
- The physical plan should be equivalent

**Why it MIGHT matter:**

- Some older Spark versions had hash function quirks
- Optimizer might make different decisions based on statistics
- Different code paths could be triggered

**Test it:**

```python
# Compare both approaches
result1 = table1.join(table2, on=["CUSIP", "EFFECTIVEDATE"])
result2 = table1.join(table2, on=["EFFECTIVEDATE", "CUSIP"])

# Check physical plans
result1.explain()
result2.explain()

# If plans are identical, order doesn't matter
```

## 🎯 What You Should Actually Do

**Don't rely on join key order to fix this.** Instead:

### Verify the Real Problem:

```python
from pyspark.sql.functions import spark_partition_id, col

# Check input partition distribution
table1_input_dist = table1.withColumn("pid", spark_partition_id())
.groupBy("pid")
.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("CUSIP").alias("distinct_cusips")
)
.orderBy(col("row_count").desc())

table1_input_dist.show(20)

# If you see:
# pid | row_count | distinct_cusips
# 0   | 570M      | 1               ← Your problem!
# 1   | 45M       | 1
# Then your INPUT data is already badly partitioned
```

### Fix It:

```python
# Force repartition by BOTH keys before join
table1_repart = table1.repartition(16000, "CUSIP", "EFFECTIVEDATE")
table2_repart = table2.repartition(16000, "CUSIP", "EFFECTIVEDATE")

# Now join
result = table1_repart.join(table2_repart, on=["CUSIP", "EFFECTIVEDATE"])
```

This **forces** Spark to shuffle and redistribute by the composite key, breaking up your hot CUSIP across multiple
partitions based on the varying EFFECTIVEDATE values.

## 🔥 Bottom Line

Your 570M rows are in one partition because of **HOW the input data is organized**, not because of join key hashing. The
solution is:

1. **Explicitly repartition before joining** (16K partitions on both keys)
2. **OR use salting** (Solution 2 from earlier) if repartitioning doesn't help

Changing join key order won't solve your problem! 



