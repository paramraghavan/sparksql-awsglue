# Real-World PySpark Patterns: Interview & Production

> Common patterns you'll see in interviews and production systems. Master these and you'll solve 80% of real-world problems.

---

## Pattern 1: Deduplication (Keep First/Latest)

**Scenario**: Customer data has duplicates. Keep the most recent record.

### ❌ Wrong Way (Too Slow)
```python
# This uses a window function that scans the entire partition multiple times
df.groupBy("customer_id").agg(F.max("updated_at"))
```

### ✅ Right Way (Interview Answer)
```python
from pyspark.sql.window import Window

# Deduplicate: Keep row with latest timestamp
window = Window.partitionBy("customer_id").orderBy(F.desc("updated_at"))
df_dedup = df.withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

# Alternative: Keep first (e.g., by insertion order)
window = Window.partitionBy("customer_id").orderBy(F.col("id"))
df_dedup = df.withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")
```

**Why this works**:
- Window function partitions by ID, orders by timestamp
- `row_number()` assigns 1 to first, 2 to second, etc.
- Filter keeps only row_number == 1
- One pass, efficient

**Time complexity**: O(n log n) due to sort
**Space complexity**: O(n) for window state

---

## Pattern 2: Top N Per Group

**Scenario**: Find top 3 highest-paying employees per department.

### Solution
```python
from pyspark.sql.window import Window

window = Window.partitionBy("department").orderBy(F.desc("salary"))
result = df.withColumn("rank", F.rank().over(window)) \
    .filter(F.col("rank") <= 3) \
    .drop("rank")

result.show()
# Output:
# +----------+------+--------+----+
# |department| name |salary  |rank|
# +----------+------+--------+----+
# |Engineering| Alice|120000 | 1  |
# |Engineering| Bob  |110000 | 2  |
# |Engineering| Charlie|100000| 3 |
# |Sales     | David| 90000 | 1  |
# |Sales     | Eve  | 80000 | 2  |
# +----------+------+--------+----+
```

**Interview Follow-ups**:
- **Q**: What's the difference between rank(), row_number(), and dense_rank()?
  - **A**: `rank()` skips numbers after ties, `row_number()` is unique, `dense_rank()` doesn't skip

  ```python
  # With ties at salary 100000:
  # rank():        1, 2, 2, 4 (skips 3)
  # row_number():  1, 2, 3, 4 (unique)
  # dense_rank():  1, 2, 2, 3 (no skip)
  ```

- **Q**: How would you optimize if departments are huge?
  - **A**: Use `filter()` before window to reduce data

---

## Pattern 3: Running Total / Cumulative Sum

**Scenario**: Show cumulative sales for each store by date.

### Solution
```python
from pyspark.sql.window import Window

window = Window.partitionBy("store_id") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = df.withColumn("running_total", F.sum("sales").over(window)) \
    .orderBy("store_id", "date")

result.show()
# Output:
# +--------+----------+-----+--------------+
# |store_id|date      |sales|running_total |
# +--------+----------+-----+--------------+
# |store-1 |2024-01-01|100  |100           |
# |store-1 |2024-01-02|150  |250           |
# |store-1 |2024-01-03|200  |450           |
# |store-2 |2024-01-01|80   |80            |
# |store-2 |2024-01-02|120  |200           |
# +--------+----------+-----+--------------+
```

**Key Concept**: Window frame
- `unboundedPreceding`: Start from first row in partition
- `currentRow`: Up to current row
- Result: Sum of all rows from start to current = running total

---

## Pattern 4: Lead & Lag (Previous/Next Values)

**Scenario**: Track stock price changes day-to-day.

### Solution
```python
from pyspark.sql.window import Window

window = Window.partitionBy("stock_id").orderBy("date")

result = df.withColumn("prev_price", F.lag("price", 1).over(window)) \
    .withColumn("next_price", F.lead("price", 1).over(window)) \
    .withColumn("price_change", F.col("price") - F.col("prev_price"))

result.show()
# +--------+----------+-----+----------+----------+------------+
# |stock_id|date      |price|prev_price|next_price|price_change|
# +--------+----------+-----+----------+----------+------------+
# |AAPL    |2024-01-01|150  |null      |155       |null        |
# |AAPL    |2024-01-02|155  |150       |158       |5           |
# |AAPL    |2024-01-03|158  |155       |160       |3           |
# |AAPL    |2024-01-04|160  |158       |null      |2           |
# +--------+----------+-----+----------+----------+------------+
```

**Interview Tip**: Mention null handling
```python
# Handle nulls explicitly
.withColumn("price_change", F.coalesce(F.col("price") - F.col("prev_price"), F.lit(0)))
```

---

## Pattern 5: Pivot (Rows → Columns)

**Scenario**: Convert quarterly sales by product into columns.

### Solution
```python
# Input:
# +-------+--------+------+
# |product|quarter |sales |
# +-------+--------+------+
# |Apple  |Q1      |100   |
# |Apple  |Q2      |150   |
# |Banana |Q1      |80    |
# |Banana |Q2      |120   |

result = df.groupBy("product").pivot("quarter").agg(F.sum("sales"))

# Output:
# +-------+---+---+
# |product|Q1 |Q2 |
# +-------+---+---+
# |Apple  |100|150|
# |Banana |80 |120|
# +-------+---+---+
```

**Advanced**: Specify column order
```python
result = df.groupBy("product") \
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]) \
    .agg(F.sum("sales"))
```

**When to use**:
- ✅ Few pivot values (Q1-Q4, True/False)
- ❌ Many distinct values (avoid - creates many columns)

---

## Pattern 6: Explode (Array → Rows)

**Scenario**: Customer has multiple phone numbers stored as array. Separate into rows.

### Solution
```python
# Input:
# +-----+---------------------+
# |name |phones               |
# +-----+---------------------+
# |Alice|[123-456, 789-012]  |
# |Bob  |[345-678]            |

result = df.select("name", F.explode("phones").alias("phone"))

# Output:
# +-----+--------+
# |name |phone   |
# +-----+--------+
# |Alice|123-456 |
# |Alice|789-012 |
# |Bob  |345-678 |
# +-----+--------+
```

**With Struct**: Handle both name and phone
```python
result = df.select("name", F.explode("phones").alias("phone"))
```

**Interview Tip**: Mention explode_outer for null handling
```python
# If phones is null, still include the row
df.select("name", F.explode_outer("phones").alias("phone"))
```

---

## Pattern 7: Coalesce (Multiple Columns to One)

**Scenario**: Handle missing data by trying multiple columns.

### Solution
```python
# Get first non-null value from multiple columns
result = df.withColumn("email",
    F.coalesce(F.col("primary_email"), F.col("work_email"), F.col("personal_email"))
)

# Also useful: Handle nulls in display
result = df.select(
    "id",
    F.coalesce(F.col("name"), F.lit("Unknown")).alias("name")
)
```

---

## Pattern 8: Complex Joins with Multiple Conditions

**Scenario**: Join customers to transactions, but must match on customer_id AND date range.

### Solution
```python
# Simple equi-join
result = customers.join(
    transactions,
    customers.customer_id == transactions.customer_id,
    "left"
)

# Complex join with date range
result = customers.join(
    transactions,
    (customers.customer_id == transactions.customer_id) & \
    (transactions.transaction_date >= customers.account_open_date) & \
    (transactions.transaction_date <= customers.account_close_date),
    "left"
)

result.show()
```

**Performance Tip**: Broadcast smaller table
```python
from pyspark.sql.functions import broadcast

result = customers.join(
    broadcast(transactions),
    (customers.customer_id == transactions.customer_id),
    "left"
)
```

---

## Pattern 9: Anti Join (A NOT IN B)

**Scenario**: Find customers who haven't made any purchases.

### Solution
```python
# Customers with no transactions
no_purchases = customers.join(
    transactions.select("customer_id").distinct(),
    customers.customer_id == transactions.customer_id,
    "left_anti"  # Keep rows from left with NO match in right
)

no_purchases.show()
# Output: Only customers with no transactions
```

**Why left_anti is better than filter(col.isNull())**:
- More efficient
- Clearer intent
- Avoids null handling complexity

---

## Pattern 10: Group By with Multiple Aggregations

**Scenario**: Complex reporting - count, sum, average, and min max per category.

### Solution
```python
from pyspark.sql.functions import (
    count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, collect_list
)

result = df.groupBy("category", "subcategory").agg(
    count("*").alias("total_records"),
    spark_sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    spark_min("amount").alias("min_amount"),
    spark_max("amount").alias("max_amount"),
    count(F.when(F.col("amount") > 1000, 1)).alias("high_value_count"),
    collect_list("customer_name").alias("customers")  # Collect as list
)

result.show(truncate=False)
```

**Interview Tip**: Mention performance considerations
- `collect_list` can cause memory issues (keeps all values in memory)
- Use only for small groups
- Alternative: use `collect_set` for unique values

---

## Pattern 11: Conditional Aggregation

**Scenario**: Count total orders, but separately count high-value orders (> $1000).

### Solution
```python
from pyspark.sql.functions import when

result = df.groupBy("customer_id").agg(
    count("*").alias("total_orders"),
    sum(when(F.col("amount") > 1000, 1).otherwise(0)).alias("high_value_orders"),
    sum("amount").alias("total_amount"),
    sum(when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_count")
)

result.show()
# +-------------+-------------+----------------+-----------+----------------+
# |customer_id  |total_orders |high_value_orders|total_amount|cancelled_count|
# +-------------+-------------+----------------+-----------+----------------+
# |cust-1       |5            |2                |5500       |1               |
# |cust-2       |3            |0                |800        |0               |
# +-------------+-------------+----------------+-----------+----------------+
```

---

## Pattern 12: Date/Time Operations

**Scenario**: Analyze time-based data.

### Solution
```python
from pyspark.sql.functions import (
    to_date, to_timestamp, year, month, dayofmonth,
    datediff, date_add, date_format, current_date
)

result = df.select(
    "transaction_date",
    to_date("transaction_date").alias("date_only"),
    year("transaction_date").alias("year"),
    month("transaction_date").alias("month"),
    dayofmonth("transaction_date").alias("day"),
    datediff("transaction_date", "current_date()").alias("days_ago"),
    date_add("transaction_date", 30).alias("plus_30_days"),
    date_format("transaction_date", "yyyy-MM-dd HH:mm:ss").alias("formatted")
)

result.show()
```

---

## Pattern 13: String Operations

**Scenario**: Clean and process text data.

### Solution
```python
from pyspark.sql.functions import (
    upper, lower, trim, length, substring,
    concat, regexp_replace, split, explode
)

result = df.select(
    lower(trim(F.col("name"))).alias("name_clean"),
    upper(F.col("email")).alias("email_upper"),
    length("description").alias("desc_length"),
    substring("code", 1, 3).alias("prefix"),
    concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
    regexp_replace("phone", r"[^0-9]", "").alias("phone_digits"),  # Remove non-digits
    split("tags", ",").alias("tag_array")
)

result.show(truncate=False)
```

---

## Pattern 14: Null Handling

**Scenario**: Clean data with many nulls.

### Solution
```python
# Drop rows with any null
df_clean = df.na.drop()

# Drop rows where specific columns are null
df_clean = df.na.drop(subset=["customer_id", "amount"])

# Drop only if ALL columns are null
df_clean = df.na.drop("all")

# Fill nulls with default values
df_clean = df.na.fill({
    "status": "unknown",
    "amount": 0,
    "description": "N/A"
})

# Fill using forward fill (use previous value)
window = Window.partitionBy("customer_id").orderBy("date")
df_clean = df.withColumn("status",
    F.last(F.col("status"), ignorNulls=True).over(window)
)
```

---

## Pattern 15: Data Validation & Quality Checks

**Scenario**: Validate data before processing.

### Solution
```python
# Check for issues
print(f"Total records: {df.count()}")
print(f"Unique customers: {df.select('customer_id').distinct().count()}")

# Find nulls per column
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Find duplicates
duplicates = df.groupBy(df.columns).count().filter(F.col("count") > 1)
if duplicates.count() > 0:
    print("⚠️ Duplicates found!")

# Validate range
invalid = df.filter((F.col("amount") < 0) | (F.col("amount") > 999999))
if invalid.count() > 0:
    print(f"⚠️ {invalid.count()} invalid amounts")

# Assert
assert df.count() > 0, "DataFrame is empty!"
assert df.filter(F.col("customer_id").isNull()).count() == 0, "Nulls in customer_id!"
```

---

## Pattern 16: Union & Concatenation

**Scenario**: Combine data from multiple sources.

### Solution
```python
# Union (append rows) - must have same schema
df_all = df1.union(df2).union(df3)

# Union by name (handles reordering)
df_all = df1.unionByName(df2)

# Concatenate columns horizontally
from pyspark.sql.functions import concat
df_wide = df1.crossJoin(df2)  # Cartesian - be careful!

# Safe join with same number of rows
df_combined = df1.join(df2, F.monotonically_increasing_id() == F.monotonically_increasing_id())
```

**Interview Tip**: Union vs Join
- `union()`: Stack tables vertically (append)
- `join()`: Link tables horizontally (match rows)

---

## Pattern 17: Sample & Stratified Sampling

**Scenario**: Test on subset of data without reading all.

### Solution
```python
# Random sample
sample = df.sample(fraction=0.1)  # 10% of rows

# Reproducible sample
sample = df.sample(fraction=0.1, seed=42)

# Stratified sample - maintain distribution
sample = df.sampleBy("category", fractions={
    "electronics": 0.5,
    "clothing": 0.3,
    "food": 0.2
})
```

---

## Pattern 18: Cache When Reusing

**Scenario**: Multiple transformations from same source.

### Solution
```python
# ❌ Wrong: Re-reads from source for each action
counts = df.groupBy("category").count()
totals = df.groupBy("category").sum()

# ✅ Right: Cache after first action
df.cache()
counts = df.groupBy("category").count()
totals = df.groupBy("category").sum()
df.unpersist()  # Clean up
```

**When to cache**:
- ✅ Reused in 2+ actions
- ✅ Expensive to recompute
- ❌ Large and memory-constrained
- ❌ Used only once

---

## Pattern 19: Write with Partitioning

**Scenario**: Write data organized by date for efficient querying.

### Solution
```python
# Partition by year/month
df.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://bucket/data/")

# Result structure:
# s3://bucket/data/
#   year=2024/
#     month=01/
#       part-001.parquet
#       part-002.parquet
#     month=02/
#       part-001.parquet
#   year=2025/
#     month=01/
#       part-001.parquet

# Querying is faster - reads only relevant partitions
df_jan_2024 = spark.read.parquet("s3://bucket/data/") \
    .filter((F.col("year") == 2024) & (F.col("month") == 1))
```

**Best Practices**:
- Partition on low-cardinality columns (date, region, category)
- Not on high-cardinality (customer_id, transaction_id)
- Aim for 128MB-512MB per partition

---

## Pattern 20: Error Handling & Graceful Failure

**Scenario**: Handle missing/corrupt data gracefully.

### Solution
```python
# Try-catch for reading
try:
    df = spark.read.parquet("s3://bucket/data/")
except Exception as e:
    print(f"Failed to read: {e}")
    df = spark.createDataFrame([], "column_name STRING")  # Empty DF

# Validate and filter bad data
df_clean = df.filter(
    (F.col("amount").isNotNull()) &
    (F.col("amount") > 0) &
    (F.col("amount") < 1000000) &
    (F.col("customer_id").isNotNull())
)

bad_data = df.filter(~(
    (F.col("amount").isNotNull()) &
    (F.col("amount") > 0)
))

print(f"Processed: {df_clean.count()} rows")
print(f"Dropped: {bad_data.count()} rows")

# Continue with valid data
if df_clean.count() > 0:
    result = process(df_clean)
else:
    print("⚠️ No valid data to process")
```

---

## Interview Question: Which Pattern?

**Q**: "Write code to find the top 3 products by revenue, only for the year 2024, with total revenue > $100K."

**Answer Structure**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, year

# Step 1: Filter year 2024
df_2024 = df.filter(year("transaction_date") == 2024)

# Step 2: Aggregate by product
by_product = df_2024.groupBy("product").agg(
    spark_sum("revenue").alias("total_revenue")
)

# Step 3: Filter for revenue > 100K
by_product = by_product.filter(F.col("total_revenue") > 100000)

# Step 4: Rank and get top 3
window = Window.orderBy(F.desc("total_revenue"))
result = by_product.withColumn("rank", F.rank().over(window)) \
    .filter(F.col("rank") <= 3) \
    .drop("rank") \
    .orderBy("rank")

result.show()
```

**Patterns Used**: Filter (1), GroupBy Aggregation (10), Window Functions (2)

---

## Practice Problems

1. **Dedup with Business Logic**: Keep the latest record, but only if status != "cancelled"
2. **Top N with Ties**: Show top 3 salaries, include all employees at tied salaries
3. **Multi-Window**: Running total AND month-over-month growth in one query
4. **Complex Join**: Customer + Latest Transaction + Latest Address (3-way join)
5. **Pivot with Aggregation**: Pivot by quarter, showing count AND sum

---

## Key Takeaways

1. **Master window functions** - They solve 50% of real problems
2. **Understand join types** - Especially anti/semi joins
3. **Think in groups** - Most transformations are groupBy + aggregate
4. **Cache strategically** - Only when reused
5. **Validate early** - Check data at each step
6. **Use SQL when comfortable** - It's just as fast as DataFrame API

---

## Next Steps

- Practice these patterns on [Exercises](../../exercises/)
- Review [Interview Q&A](../08-interview-preparation/) for deeper questions
- Check [Troubleshooting](../07-troubleshooting/) when code gets slow

---

**Tips for Interview**:
- ✅ Ask clarifying questions ("Should I handle nulls?")
- ✅ Code incrementally (read → filter → agg → output)
- ✅ Explain your approach before coding
- ✅ Test with examples (show expected output)
- ✅ Optimize if time allows (broadcast, cache, partition)
