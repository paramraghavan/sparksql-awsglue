# 📏 Partition Management for Data Scientists

## The Simple Truth About 128MB Partitions

**Question:** "Isn't 128MB the default?"

**Answer:** Yes for READING files, but NO for everything else!

---

## 🎯 When You DO and DON'T Need to Act

### ✅ NO ACTION NEEDED (Automatic 128MB)

```python
# Just reading files - Spark handles this automatically
df = spark.read.parquet("s3://bucket/data/")
# ✅ Partitions are already ~128MB each
```

### ⚠️ ACTION REQUIRED (You must fix!)

**After Filtering:**
```python
# Problem:
df = spark.read.parquet("s3://bucket/100gb-data/")  # 800 partitions
df_filtered = df.filter(col("date") == "2024-01-01")  # Now only 1GB!
# ❌ Still has 800 partitions! Each is only ~1.25MB (too small!)

# Solution:
df_filtered = df_filtered.coalesce(8)  # 1GB ÷ 128MB = 8 partitions
# ✅ Now 8 partitions × 128MB each
```

**Before Aggregations:**
```python
# Problem:
df_agg = df.groupBy("category").agg(sum("amount"))
# ❌ Uses default 200 partitions regardless of data size!

# Solution (if expecting 10GB output):
spark.conf.set("spark.sql.shuffle.partitions", "80")  # (10 × 1024) ÷ 128
df_agg = df.groupBy("category").agg(sum("amount"))
# ✅ Now ~128MB per partition
```

**Before Joins:**
```python
# Problem:
df_joined = large_df.join(other_df, "id")
# ❌ Uses default 200 partitions, might create huge partitions!

# Solution (if expecting 50GB result):
spark.conf.set("spark.sql.shuffle.partitions", "400")  # (50 × 1024) ÷ 128
df_joined = large_df.join(other_df, "id")
# ✅ Now ~128MB per partition
```

**Before Writing:**
```python
# Problem:
df.write.parquet("s3://bucket/output/")
# ❌ Might create too many tiny files or too few huge files

# Solution (if writing 5GB):
df.coalesce(40).write.parquet("s3://bucket/output/")  # (5 × 1024) ÷ 128
# ✅ Creates ~40 files, each ~128MB
```

---

## 📋 Quick Decision Tree

```
Did you just READ data?
    → NO ACTION (already optimized)

Did you FILTER and reduce data size significantly?
    → YES ACTION: coalesce(new_size_GB * 8)

Are you about to groupBy/join?
    → YES ACTION: set shuffle.partitions = expected_output_GB * 8

Are you about to WRITE results?
    → YES ACTION: coalesce(output_size_GB * 8)
```

*Note: Multiply by 8 is shorthand for (GB × 1024) ÷ 128*

---

## 🚀 Easy Mode: Use Adaptive Query Execution

**Add this once at the start of your notebook:**

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128MB
```

**Benefits:**
- Spark automatically optimizes partition sizes during job execution
- You still need to coalesce after major filtering and before writing
- But most other cases are handled automatically!

---

## 📊 Simple Formula

```
Partitions = (Data Size in GB × 8)
```

**Examples:**
- 1 GB → 8 partitions
- 10 GB → 80 partitions
- 100 GB → 800 partitions

**In Code:**
```python
# After filtering to 10GB:
df = df.coalesce(80)

# Before groupBy expecting 5GB output:
spark.conf.set("spark.sql.shuffle.partitions", "40")

# Before writing 20GB:
df.coalesce(160).write.parquet("...")
```

---

## 🔍 Check Your Partitions

```python
# See how many partitions you have
print(f"Partitions: {df.rdd.getNumPartitions()}")

# If the number seems wrong for your data size, adjust it!
```

---

## ✅ Complete Example

```python
# 1. Read (automatic 128MB partitions)
df = spark.read.parquet("s3://bucket/100gb-data/")  # ~800 partitions
print(f"Initial: {df.rdd.getNumPartitions()}")

# 2. Filter (data shrinks to 10GB)
df = df.filter(col("date") == "2024-01")
df = df.coalesce(80)  # 10 × 8 = 80 partitions
print(f"After filter: {df.rdd.getNumPartitions()}")

# 3. Aggregate (expect 5GB output)
spark.conf.set("spark.sql.shuffle.partitions", "40")  # 5 × 8 = 40
df = df.groupBy("category").agg(sum("amount"))
print(f"After groupBy: {df.rdd.getNumPartitions()}")

# 4. Write (5GB output)
df.coalesce(40).write.parquet("s3://bucket/output/")  # 5 × 8 = 40 files
```

---

## 🎓 Key Takeaways

1. **Reading files** → Already optimized, do nothing
2. **After filtering** → Always check and adjust partitions
3. **Before shuffle ops** → Set `spark.sql.shuffle.partitions`
4. **Before writing** → Use `coalesce()` to control output files
5. **Easy button** → Enable Adaptive Query Execution

**Remember:** The goal is to keep each partition around 128MB throughout your entire pipeline!
