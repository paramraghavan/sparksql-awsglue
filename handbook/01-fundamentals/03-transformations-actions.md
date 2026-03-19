# Transformations & Actions

## Overview

This is one of the **most critical concepts** in Spark: the distinction between **transformations** (lazy) and **actions** (eager). Lazy evaluation is the engine that makes Spark efficient. Understanding when computation actually happens is crucial for debugging slow jobs and understanding job DAGs.

**Key Insight**: Spark doesn't execute anything until an action forces it to. Transformations just build a logical execution plan that Spark optimizes before running.

---

## Core Concepts

- **Lazy Evaluation**: Transformations return immediately without computing anything. Spark builds a DAG (Directed Acyclic Graph) of operations.
- **Transformations**: Return new DataFrames. Examples: map, filter, select, flatMap, groupBy, join. These are "promises" of future computation.
- **Actions**: Trigger actual computation and return results to driver or write to storage. Examples: count, collect, take, show, write.
- **Narrow Dependencies**: Each partition of output depends on one partition of input (filter, map, select). No shuffling needed.
- **Wide Dependencies**: Output partition depends on multiple input partitions (groupBy, join). Requires shuffling.
- **Stages**: Spark groups operations between wide transformations into stages. Each stage runs in parallel.
- **Tasks**: Stages are divided into tasks, one per partition. Tasks run in parallel on executors.

---

## Simple Code Examples

### Transformations (Lazy)

```python
from pyspark.sql import functions as F

# These all return instantly - no computation happens yet
df_filtered = df.filter(F.col("age") > 30)  # Narrow
df_mapped = df.withColumn("age_group", F.when(F.col("age") < 40, "young").otherwise("senior"))  # Narrow
df_selected = df.select("name", "age")  # Narrow
df_grouped = df.groupBy("department").agg(F.count("*").alias("count"))  # Wide

# Chain transformations - still lazy
result = df \
    .filter(F.col("age") > 30) \
    .select("name", "department", "salary") \
    .groupBy("department") \
    .agg(F.avg("salary").alias("avg_salary"))

print("No computation has happened yet!")
```

### Actions (Eager)

```python
# These FORCE computation to happen
df.show()              # Display results
df.count()             # Compute row count
df.collect()           # Bring all data to driver
df.take(10)            # Get first 10 rows
df.write.parquet(...)  # Write to storage
df.foreach(fn)         # Execute function on each partition
```

---

## Real-World Examples

### Example 1: Understanding Lazy Evaluation with Execution Plans

```python
from pyspark.sql import functions as F

# Create sample data
employees_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://data-bucket/employees.csv")

# Build complex transformation pipeline (all lazy!)
result_df = employees_df \
    .filter(F.col("salary") > 50000) \
    .withColumn("bonus", F.col("salary") * 0.1) \
    .select("id", "name", "department", "salary", "bonus") \
    .filter(F.col("department").isin(["Sales", "Engineering"])) \
    .groupBy("department") \
    .agg(
        F.count("*").alias("emp_count"),
        F.avg("salary").alias("avg_salary"),
        F.sum("bonus").alias("total_bonus")
    )

# Examine execution plan WITHOUT running it
print("Logical Plan:")
result_df.explain(mode="extended")  # Shows optimization

# NOW computation happens
result_df.show()  # This triggers actual execution
```

### Example 2: Narrow vs Wide Transformations Impact on Performance

```python
from pyspark.sql import functions as F
import time

# Narrow transformation - efficient, no shuffling
start = time.time()
df_narrow = large_df \
    .filter(F.col("status") == "active") \
    .select("id", "name", "amount") \
    .withColumn("amount_doubled", F.col("amount") * 2)
df_narrow.count()  # Forces execution
narrow_time = time.time() - start
print(f"Narrow transformation: {narrow_time:.2f}s")

# Wide transformation - requires shuffle, more expensive
start = time.time()
df_wide = large_df \
    .filter(F.col("status") == "active") \
    .groupBy("region") \
    .agg(F.sum("amount").alias("total_amount"))
df_wide.count()  # Forces execution with shuffle
wide_time = time.time() - start
print(f"Wide transformation: {wide_time:.2f}s")  # Usually much slower

# Wide transformation with join
start = time.time()
result = df1 \
    .join(df2, df1.customer_id == df2.customer_id, "inner")  # Wide
result.count()
join_time = time.time() - start
print(f"Join operation: {join_time:.2f}s")
```

### Example 3: Production ETL Job with Multiple Actions

```python
from pyspark.sql import functions as F

def process_sales_data(input_path, output_path):
    """
    Production ETL: Read, validate, transform, quality check, write
    Multiple actions at different stages
    """

    # Read data
    sales_df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .parquet(input_path)

    # Action 1: Quality check - count to validate source
    total_records = sales_df.count()
    print(f"Loaded {total_records} records")

    # Transformations (all lazy)
    transformed = sales_df \
        .filter(F.col("amount") > 0) \
        .withColumn("processed_date", F.current_date()) \
        .withColumn("amount_usd", F.col("amount") * F.col("exchange_rate")) \
        .select("order_id", "customer_id", "amount_usd", "processed_date")

    # Action 2: Data quality - count invalid
    null_amounts = transformed.filter(F.col("amount_usd").isNull()).count()
    print(f"Records with null amount after conversion: {null_amounts}")

    # More transformations
    final_df = transformed \
        .filter(F.col("amount_usd").isNotNull()) \
        .groupBy(F.to_date(F.col("processed_date")).alias("date")) \
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount_usd").alias("total_amount"),
            F.avg("amount_usd").alias("avg_amount")
        )

    # Action 3: Preview (show doesn't write anywhere)
    print("Daily summary:")
    final_df.show()

    # Action 4: Write results
    final_df.write.mode("overwrite").parquet(output_path)

    # Action 5: Final verification
    written_count = spark.read.parquet(output_path).count()
    print(f"Successfully wrote {written_count} summary records")

    return final_df

# Execute the pipeline
process_sales_data("s3://input/sales/", "s3://output/sales-summary/")
```

### Example 4: Detecting Narrow vs Wide Operations

```python
from pyspark.sql import functions as F

df = spark.createDataFrame([
    ("Alice", "Engineering", 100000),
    ("Bob", "Sales", 80000),
    ("Charlie", "Engineering", 110000),
    ("Diana", "Sales", 90000)
], ["name", "department", "salary"])

# NARROW OPERATIONS - single partition → single partition
# No shuffle, efficient
narrow_ops = df \
    .filter(F.col("salary") > 85000) \
    .withColumn("bonus", F.col("salary") * 0.1) \
    .select("name", "department", "bonus")

print("Narrow operations execution plan:")
narrow_ops.explain()  # Shows no shuffle

# WIDE OPERATIONS - multiple partitions → single partition (shuffle)
# Expensive, watch out!
wide_ops = df \
    .groupBy("department") \
    .agg(F.avg("salary").alias("avg_salary"))

print("\nWide operations execution plan:")
wide_ops.explain()  # Shows Exchange (shuffle)

# Running both
narrow_ops.show()
wide_ops.show()
```

### Example 5: Stages and Tasks in Spark Job

```python
from pyspark.sql import functions as F

# Create a job that will have multiple stages
df1 = spark.read.parquet("s3://data/transactions/")
df2 = spark.read.parquet("s3://data/customers/")

# Stage 1: Read both datasets (no shuffle)
# Stage 2: Filter df1 (narrow, same stage)
filtered = df1.filter(F.col("amount") > 100)

# Stage 3: GroupBy df1 (wide, new stage - SHUFFLE)
grouped = filtered.groupBy("customer_id").agg(F.sum("amount").alias("total"))

# Stage 4: Join with df2 (wide, new stage - SHUFFLE)
result = grouped.join(df2, "customer_id", "inner")

# Stage 5: Final aggregation (wide, new stage - SHUFFLE)
final = result.groupBy("region").agg(F.avg("total").alias("avg_total"))

# Look at Spark UI: http://localhost:4040
# It will show:
# - Job 0: Everything up to first action
# - Multiple stages separated by wide operations
# - Each stage has tasks = number of output partitions
final.write.parquet("s3://output/result/")
```

---

## Best Practices

### 1. Chain Transformations Before Actions

```python
# INEFFICIENT - Multiple transformations and actions
df.filter(F.col("age") > 30).count()  # Action 1
df.filter(F.col("age") > 30).select("name").count()  # Action 2 - recomputes!

# EFFICIENT - Single chain with one action
df.filter(F.col("age") > 30) \
    .select("name") \
    .count()  # One action at the end
```

### 2. Use explain() to Understand Your Query Plan

```python
# Always check the execution plan before running on huge datasets
df_result = df.filter(...).groupBy(...).agg(...)

# View logical and physical plans
df_result.explain(mode="extended")

# Modes available:
# - "simple": logical plan only
# - "extended": both logical and physical
# - "codegen": shows generated code
# - "cost": shows estimated costs
```

### 3. Minimize Wide Transformations

```python
# GOOD - Minimize wide operations
df_result = df \
    .filter(F.col("valid") == True) \
    .groupBy("region") \
    .agg(F.sum("amount").alias("total"))

# AVOID - Multiple wide operations
df_result = df \
    .groupBy("region").agg(F.sum("amount")) \
    .join(df2, ...) \
    .groupBy("category").agg(F.count("*"))  # Extra shuffle
```

### 4. Cache DataFrames Used Multiple Times

```python
# If a DataFrame is used in multiple actions, cache it
df_cached = df.filter(F.col("status") == "active").cache()

# Now these actions use cached data
count1 = df_cached.count()
avg_amount = df_cached.agg(F.avg("amount")).collect()[0][0]
max_amount = df_cached.agg(F.max("amount")).collect()[0][0]
```

### 5. Use Repartition Before Wide Operations

```python
# If data is heavily skewed or has unequal partitions
df_balanced = df.repartition(200, F.col("customer_id"))

# Now groupBy/join will be more efficient
result = df_balanced \
    .groupBy("customer_id") \
    .agg(F.sum("amount").alias("total"))
```

---

## Common Pitfalls

### 1. Forgetting that Transformations are Lazy

```python
# WRONG - Expects data to be processed immediately
df.filter(F.col("age") > 30)  # This does nothing!
print("Filter applied")  # Prints, but no computation happened
df.show()  # Shows unfiltered data

# CORRECT
df_filtered = df.filter(F.col("age") > 30)
df_filtered.show()  # Now shows filtered data
```

### 2. Multiple Actions on Same DataFrame

```python
# INEFFICIENT - Computes twice
df_filtered = df.filter(F.col("status") == "active")
count = df_filtered.count()  # Computation 1
result = df_filtered.collect()  # Computation 2 (redundant!)

# EFFICIENT - Compute once, reuse
df_filtered = df.filter(F.col("status") == "active").cache()
count = df_filtered.count()  # Computation 1
result = df_filtered.collect()  # Uses cached data
```

### 3. Not Understanding Wide vs Narrow Performance

```python
# User expects this to be fast - it's not (wide operation!)
large_df.groupBy("customer_id").agg(F.sum("amount")).count()  # SLOW - requires shuffle

# Much faster (narrow operations)
large_df.filter(F.col("amount") > 0).select("customer_id", "amount").count()  # FAST
```

### 4. Collecting Large DataFrames

```python
# DANGER - Brings entire DataFrame to driver
large_df.collect()  # OOM if DataFrame > available driver memory

# SAFE - Limit first
large_df.limit(1000).collect()
# Or use take
large_df.take(100)  # Returns array of first 100 rows
```

### 5. Ignoring Shuffle and Partitioning

```python
# UNOPTIMIZED - Default partitions may not match data skew
df.groupBy("country").agg(F.count("*")).write.parquet("output/")

# OPTIMIZED - Repartition on grouping key first
df.repartition(100, F.col("country")) \
    .groupBy("country") \
    .agg(F.count("*")) \
    .write.parquet("output/")
```

---

## Interview Q&A

### Q1: Explain lazy evaluation. Why is it important?

**A**: Lazy evaluation means transformations don't execute immediately. They return immediately, and Spark builds a logical execution plan (DAG). Computation only happens when an action is called.

**Why it matters**:
1. **Optimization**: Spark sees the entire pipeline before executing, allowing Catalyst to optimize globally
2. **Efficiency**: Unnecessary transformations can be eliminated (e.g., column pruning)
3. **Performance**: Spark can combine multiple narrow operations into a single stage
4. **Control**: You decide when computation happens by choosing when to call actions

**Example**:
```python
# These three chains are different even though they look similar:
df.filter(...).groupBy(...).agg(...)  # Just builds DAG, no computation
df.filter(...).groupBy(...).agg(...).show()  # Computes everything

# Spark can optimize differently each time, depending on the action
```

**Follow-up**: What's the difference between a transformation and an action?
- Transformations return DataFrames and are lazy (map, filter, select)
- Actions return results or write to storage and force execution (count, show, write)

---

### Q2: What's the difference between narrow and wide transformations? Impact on performance?

**A**:

| Aspect | Narrow | Wide |
|--------|--------|------|
| **Input-Output Relationship** | 1 input partition → 1 output partition | Multiple input partitions → 1+ output partitions |
| **Shuffling** | No shuffle required | Requires data shuffling across network |
| **Examples** | filter, select, map, withColumn | groupBy, join, orderBy, repartition |
| **Performance** | Fast (no network I/O) | Slow (network + disk I/O) |
| **Cost** | Low - runs in same stage | High - creates new stage |

**Performance Impact**:
```python
# Narrow (fast) - all 3 operations run in same stage
df.filter(...).select(...).withColumn(...)

# Wide (slow) - creates 2 stages with shuffle between
df.filter(...).groupBy(...).select(...)  # Shuffle at groupBy
```

**Key insight**: Each wide operation forces a stage boundary and expensive shuffle. Minimize wide operations.

---

### Q3: How many stages will this job have? Why?

```python
df1 = spark.read.parquet("path1")  # Read - no stage
df2 = spark.read.parquet("path2")  # Read - no stage
result = df1 \
    .filter(F.col("amount") > 100) \       # Stage 1: narrow
    .withColumn("bonus", F.col("amount") * 0.1) \  # Stage 1: narrow
    .groupBy("customer_id") \              # Stage boundary - WIDE
    .agg(F.sum("amount")) \                # Stage 2
    .join(df2, "customer_id") \            # Stage boundary - WIDE
    .filter(F.col("status") == "active")   # Stage 3: narrow
result.write.parquet("output")
```

**A**: **3 stages**

1. **Stage 0**: filter + withColumn (narrow operations, run together)
2. **Stage 1**: groupBy + agg (wide - shuffle stage)
3. **Stage 2**: join + final filter (wide - shuffle stage)

Each wide operation (groupBy, join) creates a stage boundary because it requires shuffling. Narrow operations before the next wide op are combined into the same stage.

---

## See Also

- [01-fundamentals/02-dataframes-basics.md](02-dataframes-basics.md) - DataFrame creation and immutability
- [01-fundamentals/04-sparksql-basics.md](04-sparksql-basics.md) - SQL expressions and Catalyst optimization
- [02-data-operations/03-aggregations.md](../02-data-operations/03-aggregations.md) - GroupBy and aggregation functions
- [02-data-operations/04-window-functions.md](../02-data-operations/04-window-functions.md) - Advanced analytics with windows
- [03-joins-partitioning/02-join-strategies.md](../../03-joins-partitioning/02-join-strategies.md) - How joins are executed
- [04-performance-optimization/03-catalyst-optimizer.md](../../04-performance-optimization/03-catalyst-optimizer.md) - How Spark optimizes queries
