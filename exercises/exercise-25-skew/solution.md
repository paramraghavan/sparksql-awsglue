**Work on  REAL problem!**

You have **ONE hot CUSIP** (`3132M9W75_2024-11-01`) appearing **570 million times** with different effective dates. Even
though each (CUSIP, EFFECTIVEDATE) pair is unique, all 570M rows with this CUSIP are landing in the **same partition
during the shuffle**.

This is classic **hot key skew** - one CUSIP dominates your dataset.

## 🔥 The Problem

```
Table1:
- CUSIP '3132M9W75_2024-11-01' → 570M rows (different dates)
- Other CUSIPs → 71M rows

During join shuffle:
- All 570M rows with CUSIP '3132M9W75_2024-11-01' → Partition X
- Other 71M rows → distributed across 3999 partitions
```

## ✅ Solution: Salting Strategy for Hot Keys

### Step 1: Identify the Hot CUSIP(s)

```python
from pyspark.sql.functions import col, broadcast, lit, expr, rand, concat

# Find which CUSIPs are causing skew
hot_cusips = table1.groupBy("CUSIP").count()
    .filter(col("count") > 1000000)
    .orderBy(col("count").desc())

hot_cusips.show()

# Create a set of hot CUSIPs for filtering
hot_cusip_list = [row["CUSIP"] for row in hot_cusips.collect()]
print(f"Hot CUSIPs: {hot_cusip_list}")
```

### Step 2: Split and Salt Hot Keys

```python
from pyspark.sql.functions import when, col, rand, lit, concat

# Configuration
NUM_SALTS = 200  # Distribute hot CUSIP across 200 partitions
HOT_CUSIP_THRESHOLD = 1000000

# Identify hot CUSIPs
hot_cusips_df = table1.groupBy("CUSIP").count()
    .filter(col("count") > HOT_CUSIP_THRESHOLD)
    .select("CUSIP")

# Broadcast small hot CUSIP list
hot_cusips_broadcast = broadcast(
    hot_cusips_df.withColumn("is_hot", lit(True))
)

# ============================================
# PROCESS TABLE 1: Add salt to hot keys
# ============================================
table1_marked = table1.join(
    hot_cusips_broadcast,
    on="CUSIP",
    how="left"
).fillna(False, subset=["is_hot"])

# Add random salt only to hot CUSIPs
table1_salted = table1_marked.withColumn(
    "salt",
    when(col("is_hot"), (rand() * NUM_SALTS).cast("int"))
    .otherwise(lit(0))
).drop("is_hot")

# ============================================
# PROCESS TABLE 2: Explode hot keys
# ============================================
table2_marked = table2.join(
    hot_cusips_broadcast,
    on="CUSIP",
    how="left"
).fillna(False, subset=["is_hot"])

# For hot CUSIPs: create NUM_SALTS copies
# For normal CUSIPs: keep as is
from pyspark.sql.functions import explode, sequence, array

table2_salted = table2_marked.withColumn(
    "salt",
    when(col("is_hot"), explode(sequence(lit(0), lit(NUM_SALTS - 1))))
    .otherwise(lit(0))
).drop("is_hot")

# ============================================
# JOIN with salt
# ============================================
result = table1_salted.join(
    table2_salted,
    on=["CUSIP", "EFFECTIVEDATE", "salt"],
    how="inner"  # or left, outer as needed
).drop("salt")

# ============================================
# Optimize partitioning
# ============================================
result = result.repartition(4000)
```

### Step 3: Alternative - Hardcode if You Know the Hot Key

```python
# If you know the specific hot CUSIP
HOT_CUSIP = "3132M9W75_2024-11-01"
NUM_SALTS = 200

# Table 1: Add salt to hot CUSIP
table1_salted = table1.withColumn(
    "salt",
    when(col("CUSIP") == HOT_CUSIP, (rand() * NUM_SALTS).cast("int"))
    .otherwise(lit(0))
)

# Table 2: Explode hot CUSIP
table2_normal = table2.filter(col("CUSIP") != HOT_CUSIP).withColumn("salt", lit(0))

table2_hot = table2.filter(col("CUSIP") == HOT_CUSIP)
    .withColumn("salt", explode(sequence(lit(0), lit(NUM_SALTS - 1))))

table2_salted = table2_normal.union(table2_hot)

# Join
result = table1_salted.join(
    table2_salted,
    on=["CUSIP", "EFFECTIVEDATE", "salt"],
    how="inner"
).drop("salt")
```

## 📊 Why This Works

**Before Salting:**

```
Partition 123: 570M rows (CUSIP '3132M9W75_2024-11-01')
Partition 0-4000: 71M rows (other CUSIPs)
Result: 1 executor doing 89% of work
```

**After Salting:**

```
Partition 0: 2.85M rows (CUSIP '3132M9W75_2024-11-01', salt=0)
Partition 1: 2.85M rows (CUSIP '3132M9W75_2024-11-01', salt=1)
...
Partition 199: 2.85M rows (CUSIP '3132M9W75_2024-11-01', salt=199)
Partition 200-4000: 71M rows (other CUSIPs)
Result: All executors utilized evenly
```

## ⚡ Quick Fix - Increase Partitions First

Sometimes just increasing partitions helps:

```python
# Try this first - simple and might work
spark.conf.set("spark.sql.shuffle.partitions", "20000")  # 5x more partitions

table1_repart = table1.repartition(20000, "CUSIP", "EFFECTIVEDATE")
table2_repart = table2.repartition(20000, "CUSIP", "EFFECTIVEDATE")

result = table1_repart.join(
    table2_repart,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="inner"
)
```

## 🎯 Best Practice: Dynamic Salting

```python
from pyspark.sql import functions as F


def salted_join(left_df, right_df, join_keys, how="inner", skew_threshold=1000000, num_salts=200):
    """
    Perform a join with automatic salting for skewed keys
    """
    # Find hot keys
    hot_keys = left_df.groupBy(*join_keys).count()
        .filter(F.col("count") > skew_threshold)
        .select(*join_keys)
        .withColumn("is_hot", F.lit(True))

    # Mark hot keys
    left_marked = left_df.join(broadcast(hot_keys), on=join_keys, how="left")
        .fillna(False, subset=["is_hot"])

    right_marked = right_df.join(broadcast(hot_keys), on=join_keys, how="left")
        .fillna(False, subset=["is_hot"])

    # Add salt
    left_salted = left_marked.withColumn(
        "salt",
        F.when(F.col("is_hot"), (F.rand() * num_salts).cast("int")).otherwise(F.lit(0))
    ).drop("is_hot")

    right_salted = right_marked.withColumn(
        "salt",
        F.when(F.col("is_hot"), F.explode(F.sequence(F.lit(0), F.lit(num_salts - 1))))
        .otherwise(F.lit(0))
    ).drop("is_hot")

    # Join with salt
    result = left_salted.join(
        right_salted,
        on=join_keys + ["salt"],
        how=how
    ).drop("salt")

    return result


# Usage
result = salted_join(
    table1,
    table2,
    join_keys=["CUSIP", "EFFECTIVEDATE"],
    how="inner",
    skew_threshold=1000000,
    num_salts=200
)
```

## 📈 Expected Performance Improvement

- **Before**: 570M rows in 1 partition → 1 executor → **Hours**
- **After**: 570M rows across 200 partitions → 200 executors → **Minutes**
- **Speedup**: **50-100x** for the hot key portion

## 🔧 Tuning Parameters

```python
# Adjust based on your cluster
NUM_SALTS = 200  # Rule: num_salts ≈ (hot_key_rows / 2-5M rows)

# For 570M rows:
# NUM_SALTS = 570M / 3M ≈ 190-200 ✅

# Too few salts → still skewed
# Too many salts → overhead in table2 explosion
```

Try the hardcoded solution first since you know the hot CUSIP - it's the fastest to implement!