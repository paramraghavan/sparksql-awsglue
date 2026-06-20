# 09 - Interview Questions & Answers

## How to Use This Section

This is a comprehensive collection of **40+ real interview questions** asked at tech companies (Amazon, Google, Meta, Microsoft, etc.) for Big Data/PySpark positions.

**Best practice:** Don't just read answers. Try to answer first, then compare.

---

## Table of Contents
1. [Fundamental Concepts](#fundamental-concepts)
2. [Performance & Optimization](#performance--optimization)
3. [Architecture & Design](#architecture--design)
4. [Coding Problems](#coding-problems)
5. [System Design](#system-design)

---

## Fundamental Concepts

### Q1: What's the difference between RDD and DataFrame?

**Answer:** RDD = low-level (no optimization), DataFrame = high-level (Catalyst optimizes). Use DataFrame 95% of the time. RDD only for unstructured data or complex custom logic.

**See:** Section 01 - RDD vs DataFrame for detailed comparison and examples.

---

### Q2: Explain lazy evaluation in PySpark

**Answer:** Transformations don't execute immediately. Spark builds a plan and only executes on actions (show, collect, write). This allows Catalyst to optimize the entire pipeline before running.

**See:** Section 01 - Lazy Evaluation section with timeline and examples.

---

### Q3: What is a partition in Spark?

**Answer:** A logical data division (default 128MB per partition). More partitions = more parallelism. Calculated as `ceil(file_size / 128MB)`.

**See:** Section 01 - Partitioning Basics.

---

### Q4: Repartition vs Coalesce - When to use each?

**Answer:** Repartition = shuffle (expensive, fixes skew). Coalesce = merge (cheap, reduces partitions). Use coalesce after filtering, repartition for skew/parallelism.

**See:** Section 01 - Repartition vs Coalesce.

---

### Q5: What is shuffle and why is it expensive?

**Answer:** Shuffle = moving data between partitions via network (slow). Occurs in GroupBy, Join, Repartition, Distinct, OrderBy. Minimize with pre-filtering and broadcast joins.

**See:** Section 05 - Shuffle Optimization.

---

## Performance & Optimization

### Q6: What causes memory spill and how do you prevent it?

**Answer:** Spill = data exceeds executor memory, goes to disk (slow). Prevent: pre-filter, increase executor memory, reduce shuffle partitions, use broadcast, repartition for skew.

**See:** Section 04 - Memory Spill Optimization.

---

### Q7: How would you optimize a slow Spark job?

**Answer:** Measure (df.explain, Spark UI) → Identify bottleneck (read/shuffle/computation) → Optimize: Pre-filter, broadcast joins, reduce shuffle partitions, avoid UDFs.

**See:** Sections 04-06 for detailed optimization strategies.

---

### Q8: Explain the Catalyst Optimizer

**Answer:** Catalyst automatically optimizes execution plans via: predicate pushdown (filter early), column pruning (select needed columns), constant folding, join reordering. Trust it to optimize SQL operations.

**See:** Section 01 - Catalyst Optimizer for detailed explanation and examples.

---

### Q9: What is data skew and how do you handle it?

**Answer:** Skew = some partitions 10-100x larger (hot keys, geography). Causes bottleneck. Solutions: separate hot keys, salt keys (add randomness), increase partitions, repartition.

**See:** Section 04 - Data Skew handling in memory optimization.

---

### Q10: How do you choose the right join strategy?

**Answer:** <1GB → broadcast (fastest). Both >1GB & sorted → sort-merge. Default → let Catalyst choose. Use broadcast hint for small tables.

**See:** Section 06 - Join Strategies with decision tree and performance comparison.

---

## Architecture & Design

### Q11: Design a data pipeline to process 500GB+ daily data

**Answer:**

```
Architecture:

Data Source (S3)
    ↓ [Extract]
Raw Zone (s3://lake/raw/)
    ↓ [Transform]
Processed Zone (s3://lake/processed/)
    ↓ [Load]
Snowflake Warehouse
    ↓ [Query]
Analytics / BI Tools
```

**Key design decisions:**

1. **Partitioning:** By date + category for efficient queries
2. **Format:** Parquet (compressed, columnar)
3. **Cluster:** Auto-scaling based on data size
4. **Error handling:** Retry logic, validation, alerts
5. **Monitoring:** Metrics, logs, SLA tracking

**Implementation:**
```python
class DataPipeline:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Pipeline").getOrCreate()

    def extract(self, date_str):
        # Read from S3
        return self.spark.read.parquet(f"s3://raw/{date_str}/")

    def validate(self, df):
        # Check data quality
        assert df.count() > 0
        assert all(col in df.columns for col in required_cols)
        return df

    def transform(self, df):
        # Clean and enrich
        return df.filter(...).withColumn(...)

    def load(self, df, path):
        # Write to processed zone
        df.write.mode("overwrite").partitionBy("date").parquet(path)

    def run(self, date_str):
        df = self.extract(date_str)
        df = self.validate(df)
        df = self.transform(df)
        self.load(df, f"s3://processed/{date_str}/")

pipeline = DataPipeline()
pipeline.run("2024-01-15")
```

---

### Q12: How would you handle late-arriving data?

**Answer:**

**Late data:** Data arriving after expected time window (e.g., transaction recorded 2 days later).

**Strategies:**

```python
# Strategy 1: Separate late window
def process_with_late_window(spark, date_str, late_window_days=7):
    """Process with lookback window for late data"""

    from datetime import datetime, timedelta
    from pyspark.sql.functions import col, from_unixtime

    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    lookback_date = (target_date - timedelta(days=late_window_days)).strftime("%Y-%m-%d")

    # Read current + late data
    df = spark.read.parquet(f"s3://raw/")
    df = df.filter((col("event_date") >= lookback_date) & (col("event_date") <= date_str))

    # Deduplicate (keep latest)
    from pyspark.sql.window import Window
    df_dedup = df.withColumn(
        "row_num",
        row_number().over(Window.partitionBy("event_id").orderBy(col("timestamp").desc()))
    ).filter(col("row_num") == 1)

    return df_dedup

# Strategy 2: Append late data separately
def append_late_data(spark, date_str):
    """Append late-arriving records to existing results"""

    # Read what was already processed
    existing = spark.read.parquet(f"s3://processed/{date_str}/")

    # Find late data (arrived after initial load)
    late_data = spark.read.parquet(f"s3://raw/").filter(
        col("load_timestamp") > existing.select(F.max("load_timestamp"))
    )

    # Union and deduplicate
    combined = existing.union(late_data).dropDuplicates(["event_id"])

    return combined
```

---

### Q13: Design a real-time analytics system

**Answer:**

**For streaming data (different from batch!):**

```python
from pyspark.sql.streaming import AvailableNow

# Read from Kafka/Kinesis (real-time source)
df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "events")\
    .load()

# Parse events
events = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Windowed aggregation (sliding 5-minute windows)
from pyspark.sql.functions import window

result = events.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute")
).agg(
    F.sum("amount").alias("total"),
    F.count("*").alias("count")
)

# Write to sink (Kafka, console, storage)
query = result.writeStream\
    .format("console")\
    .outputMode("update")\
    .option("checkpointLocation", "/tmp/checkpoint")\
    .start()

query.awaitTermination()
```

**Key differences from batch:**
- Streams process continuous data
- Windowing for temporal aggregations
- Checkpointing for fault tolerance
- Late data handling with watermaks

---

## Coding Problems

### Q14: Count distinct elements in a 1TB dataset

**Answer:**

**Problem:** Count distinct user IDs in 1TB data (1B+ records).

**Naive approach (fails):**
```python
df.select("user_id").distinct().count()  # Out of memory!
```

**Solution:**
```python
# Approximate count (fast, accurate enough)
from pyspark.sql.functions import approx_count_distinct

count = df.select(approx_count_distinct("user_id")).collect()[0][0]
print(f"~{count} unique users")  # Faster, uses less memory

# Exact count (if must have)
# Uses HyperLogLog internally, still memory-efficient
count_exact = df.select("user_id").distinct().count()
```

**Why it works:** HyperLogLog algorithm provides accurate distinct counts with O(1) memory.

---

### Q15: Find top-K elements efficiently

**Answer:**

**Problem:** Find top-10 users by spending (in 500GB data).

**Naive approach:**
```python
# Bad: sorts entire dataset
df.orderBy(col("spending").desc()).limit(10)  # Slow!
```

**Optimized approaches:**

```python
# Approach 1: Use window function
from pyspark.sql.window import Window

result = df.withColumn(
    "rank",
    row_number().over(Window.orderBy(col("spending").desc()))
).filter(col("rank") <= 10)

# Approach 2: Use aggregation (if grouping)
result = df.groupBy("user_id").agg(
    F.sum("spending").alias("total_spending")
).orderBy(col("total_spending").desc()).limit(10)

# Approach 3: Use heap (in-memory, efficient for small k)
# Only practical if k is very small (< 1000)
```

---

### Q16: Join two large datasets without full shuffle

**Answer:**

**Problem:** Join 100GB + 50GB datasets, minimize shuffle.

**Solution:**

```python
# Pre-sort and pre-partition for sort-merge join
df1 = spark.read.parquet("df1/").repartition(200, "key").sortWithinPartitions("key")
df2 = spark.read.parquet("df2/").repartition(200, "key").sortWithinPartitions("key")

# Join (no shuffle because already partitioned & sorted!)
result = df1.join(df2, "key")

# Even better: Broadcast if one table < 1GB
result = df1.join(broadcast(df2), "key")  # No shuffle!
```

---

### Q17: Detect duplicate records efficiently

**Answer:**

**Problem:** Find duplicate transactions (same ID, amount, timestamp).

```python
# Solution 1: dropDuplicates (built-in)
df_dedup = df.dropDuplicates(["transaction_id"])

# Solution 2: Window + row_number (more control)
from pyspark.sql.window import Window

df_dedup = df.withColumn(
    "row_num",
    row_number().over(Window.partitionBy("transaction_id").orderBy("timestamp"))
).filter(col("row_num") == 1)

# Solution 3: Find AND remove duplicates
df_with_dup_count = df.groupBy("transaction_id").agg(
    F.count("*").alias("dup_count")
).filter(col("dup_count") > 1)

# These are duplicates:
duplicates = df.join(df_with_dup_count, "transaction_id")
```

---

### Q18: Calculate cumulative sum per partition

**Answer:**

**Problem:** For each user, calculate running total of spending over time.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

df = spark.createDataFrame([
    (1, "2024-01-01", 100),
    (1, "2024-01-02", 50),
    (1, "2024-01-03", 75),
    (2, "2024-01-01", 200),
], ["user_id", "date", "amount"])

# Window: partition by user, order by date
window = Window.partitionBy("user_id").orderBy("date")

# Cumulative sum
result = df.withColumn(
    "running_total",
    spark_sum("amount").over(window.rangeBetween(Window.unboundedPreceding, 0))
)

result.show()
# user_id | date       | amount | running_total
# 1       | 2024-01-01 | 100    | 100
# 1       | 2024-01-02 | 50     | 150
# 1       | 2024-01-03 | 75     | 225
# 2       | 2024-01-01 | 200    | 200
```

---

### Q19: Handle null values in aggregations

**Answer:**

**Problem:** NULLs break aggregations.

```python
# Without handling NULLs
df.groupBy("dept").agg(F.sum("salary"))
# Result: NULL if any salary is NULL

# Solution: Handle NULLs explicitly
df.groupBy("dept").agg(
    F.sum(col("salary")).alias("total"),  # Ignores NULLs automatically
    F.count(col("salary")).alias("count"),  # Ignores NULLs
    F.count("*").alias("total_rows")  # Counts all including NULLs
)

# Or replace NULLs before aggregation
df.fillna({"salary": 0}).groupBy("dept").agg(F.sum("salary"))

# For more control
df.withColumn(
    "salary_filled",
    when(col("salary").isNull(), 0).otherwise(col("salary"))
).groupBy("dept").agg(F.sum("salary_filled"))
```

---

### Q20: Convert wide to long format (unpivot)

**Answer:**

**Problem:** Transform table from wide to long format.

```python
# Input (wide)
# month_01 | month_02 | month_03
# 100      | 200      | 300

# Output (long)
# month   | value
# month_01| 100
# month_02| 200
# month_03| 300

from pyspark.sql.functions import col, explode, arrays_zip

df_wide = spark.createDataFrame([
    ("Product1", 100, 200, 300),
    ("Product2", 150, 250, 350),
], ["product", "month_01", "month_02", "month_03"])

# Convert to long
df_long = df_wide.select(
    col("product"),
    explode(arrays_zip(
        *[lit(col_name).alias("month") for col_name in ["month_01", "month_02", "month_03"]],
        *[col(col_name) for col_name in ["month_01", "month_02", "month_03"]]
    )).alias("data")
).select(
    col("product"),
    col("data.month"),
    col("data.month_01").alias("value")  # Adjust naming
)

# Simpler approach using stack
df_long = df_wide.selectExpr(
    "product",
    "stack(3, 'month_01', month_01, 'month_02', month_02, 'month_03', month_03) "
    "as (month, value)"
)
```

---

## System Design

### Q21: Design a recommendation system using Spark

**Answer:**

```
User Behavior Data (100GB/day)
    ↓
Feature Engineering (Spark)
├─ User embeddings
├─ Item embeddings
└─ Interaction features
    ↓
Model Training (MLlib)
├─ Collaborative filtering
└─ Matrix factorization
    ↓
Serving
├─ Real-time predictions (Spark Streaming)
└─ Batch reranking
```

**Implementation sketch:**
```python
# User-item interactions
interactions = spark.read.parquet("user_events/")

# Create user and item features
from pyspark.mllib.recommendation import ALS

# Convert to rating format
ratings = interactions.select(
    col("user_id"),
    col("item_id"),
    col("rating")
)

# Train collaborative filtering model
model = ALS.train(ratings.rdd, rank=10, iterations=10)

# Make predictions for all users
predictions = model.recommendForAllUsers(10)  # Top 10 items per user

# Save for serving
predictions.write.mode("overwrite").parquet("recommendations/")
```

---

### Q22: Design a fraud detection system

**Answer:**

```
Transaction Data
    ↓
Feature Engineering
├─ User spending patterns
├─ Merchant patterns
├─ Geographic patterns
└─ Temporal patterns
    ↓
Anomaly Detection
├─ Statistical (z-score)
├─ ML models
└─ Rule-based
    ↓
Real-time Alerting
└─ Block / Review
```

---

### Q23: Design a data warehouse schema

**Answer:**

**Schema (Star schema):**

```
FACT_SALES (large table)
├─ fact_id (PK)
├─ customer_id_fk (FK)
├─ product_id_fk (FK)
├─ date_id_fk (FK)
├─ amount
└─ quantity

DIM_CUSTOMER
├─ customer_id (PK)
├─ name
├─ segment
└─ country

DIM_PRODUCT
├─ product_id (PK)
├─ name
├─ category
└─ price

DIM_DATE
├─ date_id (PK)
├─ date
├─ year
├─ month
└─ day
```

**Why star schema?**
- Fact table is central hub
- Dimensions are small and reusable
- Efficient joins (FK relationships)
- Easy to query and understand

---

## Practice Tips

### For Phone Interviews

1. **Read out loud** - Explain your thinking
2. **Ask clarifying questions** - "How much data? What latency?"
3. **Start simple** - Then optimize
4. **Code on whiteboard** - Don't worry about syntax perfection
5. **Test your logic** - Walk through with example data

### For Take-Home Assignments

1. **Focus on correctness first** - Then optimize
2. **Add error handling** - Production code
3. **Include tests** - Show you think about quality
4. **Document assumptions** - Why you chose approach X
5. **Provide benchmarks** - Data size, performance metrics

### For System Design

1. **Draw architecture** - Boxes and arrows first
2. **Discuss tradeoffs** - Cost vs latency, accuracy vs speed
3. **Consider scale** - "What if 10x more data?"
4. **Mention monitoring** - How do you know it's working?
5. **Think about failure** - What breaks? How do you recover?

---

## Common Mistakes to Avoid

❌ **Calling collect() on large DataFrames** - Will crash!
```python
# Bad
data = df.collect()  # Brings all to driver

# Good
data = df.toLocalIterator()  # Stream one partition at a time
```

❌ **Not filtering before expensive operations**
```python
# Bad
df.groupBy().sum()  # Process all data first

# Good
df.filter(...).groupBy().sum()  # Filter first
```

❌ **Using RDD when DataFrame exists**
```python
# Bad (slow)
rdd = df.rdd.map(...).filter(...).map(...)

# Good (fast)
df.filter(...).select(...).withColumn(...)
```

❌ **Not partitioning data appropriately**
```python
# Bad: 10,000 tiny partitions
df.write.parquet("output/")

# Good: Reasonable partition count
df.coalesce(100).write.parquet("output/")
```

❌ **Ignoring join strategies**
```python
# Bad: Both tables shuffle
df_large.join(df_medium, "key")

# Good: Broadcast small table
df_large.join(broadcast(df_medium), "key")
```

---

## Additional Resources

**Books:**
- "Learning Spark" by Jules S. Damji et al.
- "High Performance Spark" by Rachel Warren and Matei Zaharia

**Online:**
- Spark documentation: https://spark.apache.org/docs/latest/
- Databricks Academy: Free PySpark courses
- LeetCode: SQL and Spark coding problems

**Practice:**
- Build ETL pipelines on local Spark
- Optimize slow jobs (production-like scenarios)
- Teach someone else what you learned

---

## Final Tips

✅ **Understand fundamentals deeply** - RDD, DataFrame, lazy evaluation
✅ **Know when to optimize** - Profile first, then optimize
✅ **Practice coding** - Write code regularly
✅ **Explain your thinking** - Clear communication matters
✅ **Think about production** - Error handling, monitoring, scale
✅ **Learn from failures** - Understand what went wrong

---

**Remember:** Interviews test both technical knowledge and problem-solving ability. The ability to think through a problem systematically matters as much as knowing the right answer!

Good luck! 🚀
