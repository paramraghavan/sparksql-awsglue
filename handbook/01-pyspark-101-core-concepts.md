# 01 - PySpark 101: Core Concepts

## What is PySpark?

**Simple Answer:** Python interface to Apache Spark, a distributed computing framework.

**Real-World Analogy:**
- **Traditional Python:** Like cooking alone in a small kitchen
- **PySpark:** Like managing a kitchen with 10 chefs, where:
  - Driver (you) gives instructions
  - Executors (chefs) do the actual work in parallel
  - Partitions = cutting boards (work divided evenly)

---

## Table of Contents
1. [RDD vs DataFrame](#rdd-vs-dataframe)
2. [Lazy Evaluation](#lazy-evaluation)
3. [Transformations vs Actions](#transformations-vs-actions)
4. [Partitioning Basics](#partitioning-basics)
5. [Real-World Examples](#real-world-examples)
6. [Interview Questions](#interview-questions)

---

## RDD vs DataFrame

### RDD (Resilient Distributed Dataset)

**What it is:** Low-level abstraction - collection of objects split across cluster

**When you'd use it:**
- Unstructured data
- Complex transformations (non-SQL)
- When you need fine-grained control

```python
# Creating RDD
rdd = sc.textFile("data.txt")  # Each line = 1 element
rdd2 = rdd.map(lambda x: x.upper())
```

**Pros:** Maximum flexibility
**Cons:** Slower (no optimizations), verbose code

---

### DataFrame

**What it is:** High-level abstraction - table with rows/columns + schema

**When you'd use it:** Almost always! (95% of real projects)

```python
# Creating DataFrame
df = spark.read.csv("data.csv", header=True)
df2 = df.filter(df.age > 25)
```

**Pros:** Fast (Catalyst optimizer), clean syntax, SQL support
**Cons:** Must fit data into structured format

---

### Key Difference: The Catalyst Optimizer

```python
# DataFrame query
df.filter(df.age > 25)\
  .select("name", "salary")\
  .groupBy("dept")\
  .sum()

# What Catalyst does:
# 1. Pushes filter DOWN (reduce data early)
# 2. Selects only needed columns
# 3. Optimizes group by order
# 4. Chooses best execution plan
```

**RDD doesn't get this optimization!**

---

## Lazy Evaluation

### The Critical Concept

PySpark doesn't execute immediately. It builds a **execution plan** and only runs when you call an **action**.

### Transformations (Lazy)

```python
# None of these execute immediately!
df = spark.read.csv("100GB_file.csv")  # Just planning
filtered = df.filter(df.salary > 100000)  # Still planning
selected = filtered.select("name", "dept")  # Still planning
grouped = selected.groupBy("dept").sum()  # Still planning
```

**Execution plan built but NOT executed yet.**

### Actions (Execute!)

```python
# NOW it actually runs!
result = grouped.show()  # Shows first 20 rows
result = grouped.collect()  # Brings all data to driver (dangerous!)
result = grouped.write.parquet("output_path")  # Saves to disk
count = grouped.count()  # Counts total rows
```

### Why This Matters

**Scenario:** Processing 500GB file

```python
# Bad approach (RDD thinking)
rdd = sc.textFile("500GB_file.txt")
rdd2 = rdd.map(parse_complex_logic)  # What if file is corrupted?
rdd3 = rdd2.filter(some_condition)
rdd4 = rdd3.map(expensive_transformation)
rdd5 = rdd4.collect()  # NOW it reads the file - and fails partway through!
```

**Better approach (DataFrame thinking)**

```python
# Build correct plan first
df = spark.read.csv("500GB_file.csv")
df_clean = df.filter(df.name.isNotNull())
df_transformed = df_clean.select("id", "amount")
df_grouped = df_transformed.groupBy("id").sum()

# Execute once
df_grouped.write.mode("overwrite").parquet("output")

# Or multiple actions on SAME plan:
print(df_grouped.count())  # Executes the plan
df_grouped.show()  # Executes the same plan again (wasteful!)
```

### The Execution Plan Visualization

```python
# View the plan before execution
df_grouped.explain(extended=True)

# Output shows:
# 1. Logical plan
# 2. Physical plan
# 3. Actual stage breakdown
```

---

## Transformations vs Actions

### Transformations (Lazy)

Return new DataFrame/RDD without executing

```python
# Narrow transformations (data stays on partition)
df.map(lambda x: x * 2)
df.filter(df.age > 25)
df.select("name", "salary")
df.withColumn("new_col", df.old_col + 1)
df.dropDuplicates()

# Wide transformations (requires shuffle - data moves between partitions)
df.groupBy("dept").sum()
df.join(df2, "key")
df.repartition(10)
df.distinct()
df.orderBy("salary")
```

### Actions (Execute!)

```python
df.show()  # Display first 20 rows
df.collect()  # Get all data as array (DANGER: must fit in driver memory!)
df.count()  # Count rows
df.first()  # Get first row
df.take(10)  # Get first 10 rows
df.write.parquet("path")  # Save to storage
df.foreach(lambda x: print(x))  # Execute function per row
df.foreachPartition(lambda part: process_partition(part))  # Per partition
```

---

## Partitioning Basics

### What is a Partition?

**Definition:** A partition is a logical division of data that lives on ONE executor.

**Visual:**
```
Original File: 500GB
       ↓
    Split into 100 partitions
       ↓
  100MB per partition (roughly)
       ↓
  Distributed across executors
```

### Why Partitions Matter

```python
# 100GB file with 128MB partitions = ~800 partitions
df = spark.read.parquet("100GB_file.parquet")
print(df.rdd.getNumPartitions())  # Likely ~800

# Processing happens IN PARALLEL:
# - Partition 1 on Executor 1
# - Partition 2 on Executor 2
# - Partition 3 on Executor 3
# ... etc
```

### Default Partitioning

```python
# Reading creates partitions automatically
df = spark.read.csv("data.csv")  # Partitions based on file size

# Default: 128MB per partition (HDFS block size)
# For 1GB file: ~8 partitions
# For 500GB file: ~4000 partitions
```

### Repartition vs Coalesce

```python
# REPARTITION: Shuffle all data (expensive!)
df_repartitioned = df.repartition(50)  # Forces 50 partitions

# Use when:
# - Need fewer partitions for writing
# - Need to fix skew
# - Need specific partition count for performance

# COALESCE: Merge partitions (cheap!)
df_coalesced = df.coalesce(10)  # Merge to 10 partitions

# Use when:
# - Have too many small partitions
# - Don't want shuffle cost
# - Know data fits in fewer partitions
```

### Real Example: Partition Impact

```python
# Scenario: Join 100GB with 1GB
df_large = spark.read.parquet("100GB_file.parquet")  # ~800 partitions
df_small = spark.read.parquet("1GB_file.parquet")    # ~8 partitions

# Problem: Uneven work
# Executors with small partitions finish early, wait idle

# Solution:
df_large_repart = df_large.coalesce(50)  # Reduce to 50 partitions
df_small_repart = df_small.repartition(50)  # Increase to 50 partitions

joined = df_large_repart.join(df_small_repart, "key")
# Now each executor gets similar work
```

---

## Real-World Examples

### Example 1: Reading and Filtering (Beginner)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicETL").getOrCreate()

# Read CSV
df = spark.read.csv(
    "s3://my-bucket/customer_data.csv",
    header=True,
    inferSchema=True  # Automatically detect column types
)

# Quick check
print(f"Rows: {df.count()}")
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Filter and transform
active_customers = df.filter(
    (df.status == "ACTIVE") & (df.signup_date >= "2023-01-01")
).select("customer_id", "email", "signup_date")

# Save result
active_customers.write.mode("overwrite").parquet(
    "s3://my-bucket/active_customers.parquet"
)

print("Done! Active customers saved.")
```

### Example 2: Aggregation (Intermediate)

```python
# Reading a larger dataset
sales_df = spark.read.parquet("s3://data-lake/sales/2024/")

# Group by and aggregate
monthly_revenue = sales_df.groupBy("month", "region").agg(
    F.sum("amount").alias("total_revenue"),
    F.count("*").alias("transaction_count"),
    F.avg("amount").alias("avg_transaction")
)

# Show results
monthly_revenue.show(100, truncate=False)

# Save results
monthly_revenue.write.mode("overwrite").parquet(
    "s3://analytics/monthly_revenue"
)
```

### Example 3: Join with Broadcast (Performance)

```python
# Large fact table
transactions = spark.read.parquet("s3://data-lake/transactions/")  # 100GB

# Small dimension table
customers = spark.read.parquet("s3://data-lake/customers/")  # 500MB

# Broadcast small table to avoid shuffle
from pyspark.sql.functions import broadcast

result = transactions.join(
    broadcast(customers),
    "customer_id",
    how="inner"
)

# With broadcast: Only transactions table shuffles
# Without broadcast: Both tables would shuffle (expensive!)
```

### Example 4: RDD with Unstructured Data (Apache Logs)

**Scenario:** Process raw Apache web server logs (unstructured text data)

```python
# Raw log file (unstructured):
# 192.168.1.1 - - [01/Jan/2024:12:00:01 +0000] "GET /api/users HTTP/1.1" 200 1024
# 192.168.1.2 - - [01/Jan/2024:12:00:02 +0000] "POST /api/login HTTP/1.1" 401 512
# 192.168.1.1 - - [01/Jan/2024:12:00:03 +0000] "GET /api/data HTTP/1.1" 500 256

import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LogProcessing").getOrCreate()
sc = spark.sparkContext

# Read as RDD (text file)
logs_rdd = sc.textFile("s3://logs/apache_logs/2024-01-01/*.log")

# Custom parsing function (complex logic, not easy with DataFrame)
def parse_apache_log(line):
    """Parse Apache log line - returns dict with extracted fields"""
    try:
        # Regex pattern for Apache logs
        pattern = r'(\S+) \S+ \S+ \[(.*?)\] "(\S+) (\S+) (\S+)" (\d+) (\d+)'
        match = re.match(pattern, line)

        if match:
            return {
                "ip": match.group(1),
                "timestamp": match.group(2),
                "method": match.group(3),
                "path": match.group(4),
                "protocol": match.group(5),
                "status_code": int(match.group(6)),
                "response_bytes": int(match.group(7))
            }
        return None
    except:
        return None

# Transform RDD: Map to parsed records
parsed_logs = logs_rdd.map(parse_apache_log).filter(lambda x: x is not None)

# Business logic: Find error rate per path
def extract_key_value(log_dict):
    """Group by path, create tuple (path, status_code)"""
    return (log_dict["path"], 1 if log_dict["status_code"] >= 400 else 0)

errors_by_path = parsed_logs.map(extract_key_value)\
    .reduceByKey(lambda a, b: a + b)  # Sum errors per path

# Top 10 endpoints with most errors
top_errors = errors_by_path.sortBy(lambda x: x[1], ascending=False).take(10)

for path, error_count in top_errors:
    print(f"{path}: {error_count} errors")

# RDD advantages here:
# 1. Custom regex parsing (not easy with DataFrame schema)
# 2. Flexible record structure (some logs might be malformed)
# 3. Complex nested transformations
# 4. No schema assumptions needed
```

**Why RDD works here:**
- Apache logs are unstructured text with variable formats
- Custom parsing logic (regex) works better with RDD.map()
- Some records might be malformed - RDD.filter() handles this easily
- No rigid schema required

**Compare with DataFrame approach (won't work well):**
```python
# This would fail with malformed logs or complex parsing:
df = spark.read.csv("logs.txt", header=False)
# Problem: Can't apply custom regex easily, schema validation fails
```

---

## Interview Questions

### Q1: What's the difference between Transformations and Actions?

**Answer:**
- **Transformations:** Return new RDD/DataFrame, evaluated lazily (map, filter, groupBy)
- **Actions:** Trigger execution and return results to driver or storage (collect, show, write)

**Why it matters:** Lazy evaluation allows Spark to optimize the entire plan before execution.

```python
# Transformation (lazy)
df2 = df.filter(df.age > 25)  # Nothing happens yet

# Action (executes)
df2.show()  # Now the filter actually runs
```

---

### Q2: Why do you prefer DataFrame over RDD?

**Answer:**
- **Catalyst Optimizer:** Automatically optimizes execution plan (predicate pushdown, column pruning)
- **SQL Support:** Can use SQL queries directly
- **Performance:** 10-100x faster than RDD operations
- **Schema Management:** Built-in schema validation

**When RDD is needed:** Unstructured data, custom partitioning logic, non-SQL transformations (rare)

---

### Q3: What happens when you read a 500GB file?

**Answer:**
```
500GB file → Split into ~4000 partitions (128MB each)
           → Partitions distributed across executors
           → Lazy evaluation: No data loaded yet!
           → First action: Spark reads partitions only as needed
```

**Key:** Reading creates a plan, not actual data loading.

---

### Q4: Explain Lazy Evaluation

**Answer:**
- Spark builds an execution plan without executing it
- Plan only executes when an action is called
- Allows Catalyst to optimize the entire pipeline before running

**Example:**
```python
df.filter(...).select(...).groupBy(...).sum()  # All lazy, no execution
df.write.parquet(...)  # Action - now it executes
```

---

### Q5: When would you use Repartition vs Coalesce?

**Answer:**

| Scenario | Use | Why |
|----------|-----|-----|
| Too many small partitions (1000→50) | Coalesce | Cheap, no shuffle |
| Need even distribution | Repartition | Forces shuffle, even distribution |
| Fixing data skew | Repartition | Redistributes skewed data |
| Preparing for save (many→few) | Coalesce | Cheaper than repartition |
| Need specific count (unpredictable) | Repartition | Guarantees partition count |

```python
# Too many tiny partitions from small files
df = spark.read.csv("small_files/*.csv")  # 1000 partitions!
df_optimized = df.coalesce(100)  # Merge to 100 (cheap)

# Data is skewed (some partitions have 10x data)
df_fixed = df.repartition(50)  # Redistribute evenly (expensive but necessary)
```

---

## Key Takeaways

✅ **DataFrame over RDD** - Better performance and optimization
✅ **Lazy Evaluation** - Build plan, then execute
✅ **Partitions = Parallelism** - More partitions = more parallel work
✅ **Actions trigger execution** - Transformations don't do anything
✅ **Catalyst optimizes** - Trust it to optimize your queries

---

## Next Steps

1. **Run the examples** on local Spark (docker or miniconda)
2. **Check the execution plan** with `.explain()`
3. **Move to Section 02** - Python optimization for big data

---

**Remember:** Master these fundamentals before moving to advanced topics. They explain 80% of PySpark issues in production!
