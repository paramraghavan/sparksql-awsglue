# ✅ Solution MOSTLY Stays the Same for LEFT JOIN

The **repartitioning solution works identically** for LEFT JOIN. The **salting solution works too** but needs a small
adjustment. Here's what changes:

## 🔄 What Stays the Same

### Solution 1: Repartition (RECOMMENDED - No Changes!)

```python
# ==============================            ==============
# Works EXACTLY the same for LEFT JOIN
# ============================================
spark.conf.set("spark.sql.shuffle.partitions", "16000")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Repartition both tables
table1_fixed = table1.repartition(16000, "CUSIP", "EFFECTIVEDATE")
table2_fixed = table2.repartition(16000, "CUSIP", "EFFECTIVEDATE")

# LEFT JOIN - same approach
result = table1_fixed.join(
    table2_fixed,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="left"  # ← Only this changes
)

result.write.mode("overwrite").parquet("s3://your-bucket/output/")
```

**Why it works the same:**

- Repartitioning distributes both tables evenly
- LEFT JOIN just keeps unmatched table1 rows (with NULLs)
- No additional logic needed

---

## ⚠️ What Changes: Salting Approach

### Issue with LEFT JOIN + Salting:

**Salting still works correctly**, but you need to identify hot keys from **table1 (left table)** since that drives the
output size.

```python
from pyspark.sql.functions import when, rand, explode, sequence, lit, broadcast, ceil

# ============================================
# STEP 1: Identify Hot CUSIPs from TABLE1 (LEFT TABLE)
# This is critical for LEFT JOIN!
# ============================================
print("=== Identifying hot CUSIPs from TABLE1 (left table) ===")
hot_threshold = 10_000_000

# Analyze TABLE1 for hot keys (not table2!)
hot_cusips = table1.groupBy("CUSIP").count()
    .filter(col("count") > hot_threshold)
    .withColumn("num_salts", ceil(col("count") / 3_000_000).cast("int"))

print("Hot CUSIPs from table1:")
hot_cusips.show()

hot_cusips_broadcast = broadcast(hot_cusips.select("CUSIP", "num_salts"))

# ============================================
# STEP 2: Salt TABLE1 (add random salt to hot keys)
# ============================================
table1_with_salts = table1.join(hot_cusips_broadcast, on="CUSIP", how="left")

table1_salted = table1_with_salts.withColumn(
    "salt",
    when(col("num_salts").isNotNull(), (rand() * col("num_salts")).cast("int"))
    .otherwise(lit(0))
).drop("num_salts")

# ============================================
# STEP 3: Salt TABLE2 (explode hot keys to match all salts)
# ============================================
table2_with_salts = table2.join(hot_cusips_broadcast, on="CUSIP", how="left")

# Split into hot and normal
table2_hot = table2_with_salts.filter(col("num_salts").isNotNull())
table2_normal = table2_with_salts.filter(col("num_salts").isNull()).withColumn("salt", lit(0))

# Explode hot CUSIPs - create copies for each salt value
table2_hot_exploded = table2_hot.withColumn(
    "salt",
    explode(sequence(lit(0), col("num_salts") - 1))
).drop("num_salts")

# Combine
table2_salted = table2_normal.drop("num_salts").union(table2_hot_exploded)

# ============================================
# STEP 4: LEFT JOIN with salt
# ============================================
result = table1_salted.join(
    table2_salted,
    on=["CUSIP", "EFFECTIVEDATE", "salt"],
    how="left"  # ← LEFT JOIN
).drop("salt")

result.write.mode("overwrite").parquet("s3://your-bucket/output/")
```

---

## 📊 How LEFT JOIN + Salting Works

### Scenario 1: Matching Rows (Hot CUSIP exists in both tables)

```
Table1: 
  CUSIP='3132M9W75_2024-11-01', DATE='2024-01-01', salt=47 (random)

Table2 (after explosion):
  CUSIP='3132M9W75_2024-11-01', DATE='2024-01-01', salt=0
  CUSIP='3132M9W75_2024-11-01', DATE='2024-01-01', salt=1
  ...
  CUSIP='3132M9W75_2024-11-01', DATE='2024-01-01', salt=47  ← Matches!
  ...
  CUSIP='3132M9W75_2024-11-01', DATE='2024-01-01', salt=199

LEFT JOIN Result:
  CUSIP='3132M9W75_2024-11-01', DATE='2024-01-01' + table2 columns ✓
```

### Scenario 2: Non-Matching Rows (Date exists in table1 but not table2)

```
Table1:
  CUSIP='3132M9W75_2024-11-01', DATE='2024-12-31', salt=123 (random)

Table2:
  No rows with DATE='2024-12-31' at all

LEFT JOIN Result:
  CUSIP='3132M9W75_2024-11-01', DATE='2024-12-31' + NULL for table2 columns ✓
  (Only ONE row, not duplicated!)
```

### Scenario 3: Normal CUSIP (Not Hot)

```
Table1:
  CUSIP='ABC123', DATE='2024-01-01', salt=0

Table2:
  CUSIP='ABC123', DATE='2024-01-01', salt=0

LEFT JOIN Result:
  Normal behavior, no salting effect ✓
```

---

## 🎯 Key Differences for LEFT JOIN

| Aspect                     | INNER JOIN                | LEFT JOIN                                                   |
|----------------------------|---------------------------|-------------------------------------------------------------|
| **Hot key identification** | From either table         | **Must use table1** (left table)                            |
| **Table2 explosion cost**  | Only affects matched rows | Same explosion, but unmatched table1 rows still appear once |
| **Output row count**       | ≤ table1 rows             | **= table1 rows** (guaranteed)                              |
| **Salting correctness**    | Always correct            | Correct if hot keys identified from table1                  |

---

## ✅ Complete LEFT JOIN Solution

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col, spark_partition_id

# ============================================
# SPARK CONFIGURATION
# ============================================
spark.conf.set("spark.sql.shuffle.partitions", "16000")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")

# ============================================
# READ TABLES
# ============================================
table1 = spark.read.parquet("s3://bucket/table1/")
table2 = spark.read.parquet("s3://bucket/table2/")

# ============================================
# APPROACH 1: Simple Repartition (TRY THIS FIRST)
# ============================================
table1_fixed = table1.repartition(16000, "CUSIP", "EFFECTIVEDATE")
table2_fixed = table2.repartition(16000, "CUSIP", "EFFECTIVEDATE")

result = table1_fixed.join(
    table2_fixed,
    on=["CUSIP", "EFFECTIVEDATE"],
    how="left"
)

# Verify output row count matches table1
print(f"Table1 rows: {table1.count():,}")
print(f"Result rows: {result.count():,}")
# These should be EQUAL for LEFT JOIN

result.write.mode("overwrite").parquet("s3://bucket/output/")
```

---

## 🔍 Verification for LEFT JOIN

```python
# Critical checks for LEFT JOIN
table1_count = table1.count()
result_count = result.count()

print(f"Table1 rows: {table1_count:,}")
print(f"Result rows: {result_count:,}")

# For LEFT JOIN: result_count MUST equal table1_count
if result_count == table1_count:
    print("✅ LEFT JOIN preserved all table1 rows")
else:
    print(f"❌ ERROR: Lost {table1_count - result_count:,} rows!")

# Check how many rows matched
matched = result.filter(col("table2_column").isNotNull()).count()
print(f"Matched rows: {matched:,} ({matched / table1_count * 100:.1f}%)")
print(f"Unmatched rows: {table1_count - matched:,} ({(table1_count - matched) / table1_count * 100:.1f}%)")
```

---

## 💡 Bottom Line for LEFT JOIN

**Use the exact same repartitioning approach - it works perfectly for LEFT JOIN with zero code changes!**

Only if repartitioning doesn't work (which is rare), then use the salting approach with the adjustment to identify hot
keys from **table1** (the left table).

The repartitioning solution is still your best bet! 🚀