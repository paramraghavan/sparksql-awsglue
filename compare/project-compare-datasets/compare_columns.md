# **Function Overview**

`compare_column_values` compares column values **only for records that exist in both datasets** (matching on key
columns). It identifies which columns have differences and how many.

## 📋 **Step-by-Step Breakdown**

### **Step 1: Determine Which Columns to Compare**

```python
common_cols = list(set(df1.columns).intersection(set(df2.columns)))
compare_cols = [c for c in common_cols
                if c not in self.key_columns
                and c not in self.skip_columns
                and c != "_source"]
```

**What's happening:**

- Finds columns that exist in BOTH dataframes
- Excludes key columns (already used for matching)
- Excludes skip columns (user-specified to ignore)
- Excludes the internal `_source` column

**Example:**

```
DF1 columns: [id, name, price, status, created_at, version]
DF2 columns: [id, name, price, status, updated_at, version]
Key columns: [id]
Skip columns: [created_at, updated_at]

→ Compare columns: [name, price, status, version]
```

---

### **Step 2: Create Composite Keys for Joining**

```python
key_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                               for c in self.key_columns])
```

**What's happening:**

- Combines multiple key columns into a single string
- Handles NULL values by converting them to "NULL" string
- Uses "||" as separator (unlikely to appear in actual data)

**Example:**

```python
Key
columns: [customer_id, order_date]

Row
1: customer_id = 123, order_date = 2024 - 01 - 15
→ Composite
key: "123||2024-01-15"

Row
2: customer_id = 456, order_date = NULL
→ Composite
key: "456||NULL"
```

**Why this matters:**

- PySpark joins are much faster with a single key column
- Handles multi-column keys efficiently
- NULL handling prevents match failures

---

### **Step 3: Prepare DataFrames for Join**

```python
df1_compare = df1.withColumn("_key", key_expr)
df2_compare = df2.withColumn("_key", key_expr)

df1_compare = df1_compare.select("_key", *self.key_columns, *compare_cols)
df2_compare = df2_compare.select("_key", *compare_cols)
```

**What's happening:**

- Add the composite key to both dataframes
- Select only needed columns (performance optimization)
- DF1 keeps key columns for reporting
- DF2 only needs the comparison columns

**Why column pruning matters:**

```
Before: 200 columns × 50GB = lots of shuffle data
After: ~50 columns × 50GB = much less shuffle data
→ Faster joins, less memory
```

---

### **Step 4: Inner Join to Get Matching Records**

```python
joined = df1_compare.join(
    df2_compare,
    on="_key",
    how="inner"
)
```

**What's happening:**

- Inner join keeps only records that exist in BOTH datasets
- Joined on the composite key
- Result has columns from both dataframes

**Resulting Schema:**

```
_key          (join key)
id            (from df1)
order_date    (from df1)
name          (from df1)
price         (from df1)
status        (from df1)
name          (from df2) ← Same name, different source!
price         (from df2)
status        (from df2)
```

**Critical**: After the join, columns with the same name from df2 are accessible via `joined[col]` while df1 columns use
`F.col(col)`.

---

### **Step 5: Cache the Joined DataFrame**

```python
joined.cache()
```

**Why this is crucial:**

- We'll iterate through ALL columns comparing them
- Without caching, Spark would re-execute the join for each column
- With caching: Join once, compare 200+ times ✅

**Performance impact:**

```
Without cache: 200 columns × 30 seconds/join = 100 minutes
With cache: 1 join (30 sec) + 200 comparisons (10 min) = 11 minutes
```

---

### **Step 6: Compare Each Column**

```python
for col in compare_cols:
    # Cast both columns to string for comparison
    col_df1 = F.coalesce(F.col(col).cast("string"), F.lit("NULL"))
    col_df2 = F.coalesce(joined[col].cast("string"), F.lit("NULL"))

    # Count differences
    diff_count = joined.filter(col_df1 != col_df2).count()
```

**What's happening:**

1. **Cast to String**:
    - Handles different data types uniformly
    - Prevents type mismatch errors
    - Dates, numbers, booleans all become strings

2. **NULL Handling**:
    - `coalesce(..., "NULL")` converts NULL to string "NULL"
    - Without this: NULL != NULL = NULL (not true/false!)
    - With this: "NULL" != "NULL" = false ✅

3. **Difference Detection**:
    - Filter where values don't match
    - Count the filtered records

**Example:**

```python
DF1: price = 100.00
DF2: price = 100.0
Cast
to
string:
DF1: "100.0"
DF2: "100.0"
→ Match ✅

DF1: status = "Active"
DF2: status = "active"
Cast
to
string:
DF1: "Active"
DF2: "active"
→ Different! (case - sensitive)
```

---

### **Step 7: Calculate Percentages and Store Results**

```python
if diff_count > 0:
    diff_pct = round((diff_count / matching_count) * 100, 2)
    column_differences.append({
        "column": col,
        "differences": diff_count,
        "percentage": diff_pct
    })
```

**What's happening:**

- Only records columns that have differences
- Calculates what % of matching records differ
- Stores for reporting

**Example output:**

```python
{
    "column": "price",
    "differences": 1234,
    "percentage": 0.12  # 0.12% of records differ
}
```

---

### **Step 8: Collect Sample Differences**

```python
for col_diff in sorted(column_differences, key=lambda x: x['differences'], reverse=True)[:10]:
    col = col_diff['column']

    sample_df = joined.filter(
        F.coalesce(F.col(col).cast("string"), F.lit("NULL")) !=
        F.coalesce(joined[col].cast("string"), F.lit("NULL"))
    ).select(
        *self.key_columns,
        F.col(col).alias(f"{col}_df1"),
        joined[col].alias(f"{col}_df2")
    ).limit(100).collect()
```

**What's happening:**

- Takes top 10 columns with most differences
- For each, filters to rows with differences
- Selects key columns + both versions of the value
- Collects up to 100 samples
- `.collect()` brings data from cluster to driver (actual rows)

**Sample output:**

```python
[
    {
        "id": 123,
        "order_date": "2024-01-15",
        "price_df1": "99.99",
        "price_df2": "100.00"
    },
    {
        "id": 456,
        "order_date": "2024-01-16",
        "price_df1": "50.00",
        "price_df2": "49.99"
    }
]
```

---

### **Step 9: Cleanup**

```python
joined.unpersist()
```

**Why:**

- Releases cached data from memory
- Important for large datasets
- Prevents memory issues

---

## 🎯 **Key Design Decisions**

### **1. Why Cast Everything to String?**

```python
# Problem: Different types can't be compared
123(int)
vs
123.0(float)
vs
"123"(string)

# Solution: Convert all to strings
"123"
vs
"123.0"
vs
"123"
→ Now
comparable!
```

### **2. Why Use Composite Keys?**

```python
# Inefficient: Multiple join conditions
.join(df2, (df1.id == df2.id) & (df1.date == df2.date))

# Efficient: Single join condition
.join(df2, df1._key == df2._key)
→ Much
faster
for 200 + columns!
```

### **3. Why Cache the Joined DataFrame?**

```python
# Without cache: Spark re-computes join for EACH column
# Join operation: Read 50GB + Read 50GB + Shuffle = expensive!

# With cache: Compute once, reuse 200 times
→ 10 - 20
x
speedup
```

### **4. Why Limit Samples to 100?**

```python
# .collect() brings data to driver memory
# 1M differences × all columns = driver OOM!
# 100 samples = enough for debugging, won't crash
```

---

## 📊 **Performance Characteristics**

For a **50GB dataset with 200 columns** and **10M matching records**:

| Operation              | Time      | Why                         |
|------------------------|-----------|-----------------------------|
| Join                   | 5-10 min  | Shuffle 50GB across cluster |
| Cache                  | 2-3 min   | Store in executor memory    |
| Each column comparison | 2-5 sec   | Simple filter + count       |
| Total (200 cols)       | 15-25 min | Mostly parallel             |
| Sample collection      | 1-2 min   | Limited to 100 rows         |

**Total function time: ~20-30 minutes**

---

## 💡 **Common Issues & Solutions**

### **Issue 1: Different Data Types**

```python
# Problem:
DF1: timestamp = Timestamp("2024-01-15 10:30:00")
DF2: timestamp = String("2024-01-15 10:30:00")
→ Would
fail
comparison

# Solution: Cast to string
→ Both
become
"2024-01-15 10:30:00"
```

### **Issue 2: NULL Handling**

```python
# Problem: 
DF1: status = NULL
DF2: status = NULL
NULL != NULL → NULL(neither
true
nor
false!)

# Solution:
"NULL" != "NULL" → False(they
match!)
```

### **Issue 3: Memory Issues with Large Samples**

```python
# Problem: Collecting 1M differences
→ Driver
OOM!

# Solution: Limit to 100
.limit(100).collect()
```

---

## 🔧 **How to Customize**

### **Add Statistical Comparison**

```python
# Add after diff_count calculation:
if diff_count > 0:
    stats = joined.filter(col_df1 != col_df2).agg(
        F.count(col).alias("count"),
        F.approx_count_distinct(F.col(col)).alias("distinct_df1"),
        F.approx_count_distinct(joined[col]).alias("distinct_df2")
    ).collect()[0]
```

### **Add Threshold Filtering**

```python
# Only report if >1% different
if diff_count > 0 and diff_pct > 1.0:
    column_differences.append(...)
```

### **Compare Numerically Instead of Strings**

```python
# For numeric columns, compare with tolerance
from pyspark.sql.types import NumericType

if isinstance(df1.schema[col].dataType, NumericType):
    diff_count = joined.filter(
        F.abs(F.col(col) - joined[col]) > 0.01  # 1 cent tolerance
    ).count()
```


> This function is the computational workhorse - it's where most of the time and resources are spent, which is why proper
optimization (caching, column pruning, partitioning) is so critical!