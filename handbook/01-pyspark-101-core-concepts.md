# 01 - PySpark 101: Core Concepts

## What is PySpark?

**Simple Answer:** Python interface to Apache Spark, a distributed computing framework.

**Real-World Analogy:**
- **Traditional Python:** Cooking alone in a small kitchen
- **PySpark:** Managing 2 kitchens with 4 chefs each = 8 parallel workers:
  - **Driver (you):** Kitchen manager coordinating work
  - **Executors (kitchens):** Physical machines with resources
  - **Task Slots (chef stations):** 4 per kitchen = 8 parallel work slots
  - **Partitions (dishes):** Data divided into independent chunks
  - **Tasks:** Partitions being processed on task slots

  ```
  100 dishes (partitions) to cook
  8 parallel slots (4 chefs × 2 kitchens)
  → Batch 1: 8 dishes cook simultaneously
  → Batch 2: Next 8 dishes (slots freed up)
  → Continue until all 100 are done
  ```

---

## Table of Contents
1. [RDD vs DataFrame](#rdd-vs-dataframe)
2. [Lazy Evaluation](#lazy-evaluation)
3. [Transformations vs Actions](#transformations-vs-actions)
4. [Partitioning Basics](#partitioning-basics)
5. [Real-World Examples](#real-world-examples)
6. [Interview Questions](#interview-questions)

**See Also:** [011-transformations-rdd-vs-dataframe.md](011-transformations-rdd-vs-dataframe.md) - Deep dive into all transformation options with side-by-side comparisons

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

**See detailed RDD transformations:** [File 02 - RDD Transformations](./02-transformations-rdd-vs-dataframe.md#rdd-transformations)

### How RDD Reading & Transformation Works (Task Nodes)

**Key Question:** Does the file get read into multiple task nodes?

**Short Answer:** NO at lines 1-2 (lazy). YES at line 3 (action triggers execution).

**Detailed Flow:**

```
Step 1: sc.textFile("data.txt")      ← LAZY (no reading yet!)
├─ Spark splits 1GB file into 8 partitions (128MB each)
├─ Creates RDD metadata (file location, byte offsets)
└─ Returns immediately (no data in memory)

Step 2: rdd.map(lambda x: x.upper()) ← LAZY (no execution yet!)
├─ Adds transformation to execution plan
├─ Lambda function is NOT called yet
└─ Returns immediately

Step 3: rdd2.collect()               ← ACTION! Execution starts NOW!
├─ Driver creates job with 8 tasks (1 per partition)
├─ Sends tasks to executors in parallel
└─ Each executor: Read partition + apply map → Return results
```

**What Happens on Task Nodes:**

```
Cluster Layout:
Master (Driver)
├─ Executor 1: Task 1 (partition 0) + Task 2 (partition 1) + Task 3 (partition 2)
├─ Executor 2: Task 4 (partition 3) + Task 5 (partition 4) + Task 6 (partition 5)
└─ Executor 3: Task 7 (partition 6) + Task 8 (partition 7)

What Each Task Does:
Task 1 on Executor 1:
  1. Read partition 0 from disk → ["hello", "world"]
  2. Apply map: lambda x: x.upper() → ["HELLO", "WORLD"]
  3. Return results to driver

Task 2 on Executor 1:
  1. Read partition 1 from disk → ["foo", "bar"]
  2. Apply map: lambda x: x.upper() → ["FOO", "BAR"]
  3. Return results to driver

(All 8 tasks execute SIMULTANEOUSLY on different executors)
```

**Timeline:**

```
T=0ms:   rdd = sc.textFile()          ← Plan only, no reading
T=1ms:   rdd2 = rdd.map()             ← Plan updated, no execution
T=2ms:   result = rdd2.collect()      ← EXECUTION STARTS!
T=2-50ms: 8 tasks run in parallel on executors
T=50ms:  All tasks complete, results returned to driver
T=51ms:  Driver combines results and returns to user
```

**Key Insight:** Lazy evaluation lets Spark send the full "Read partition + Apply map" plan to all executors at once, so they execute in parallel efficiently!

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

**See detailed DataFrame transformations:** [011-transformations-rdd-vs-dataframe.md](011-transformations-rdd-vs-dataframe.md)

---

### Key Difference: The Catalyst Optimizer

**What it does:** Automatically optimizes by reading only filtered rows and needed columns

**How:** Two main optimizations:
1. **Predicate Pushdown:** Filter conditions applied during read (not after)
2. **Column Pruning:** Only read columns you use

**Real Impact:**

```python
# Query: Get average salary by dept for age > 25
result = df.filter(df.age > 25)\
  .select("dept", "salary")\
  .groupBy("dept")\
  .avg("salary")

# WITHOUT Catalyst (RDD-style):
# Read 500GB → Filter → Select → GroupBy
# Memory: 500GB peak, Time: 2 hours

# WITH Catalyst:
# Read only [age, dept, salary] WHERE age > 25 = ~150GB
# Memory: 15GB peak, Time: 30 minutes
# → 91% less memory, 70% faster!
```

**Proof: Check explain() output**

```python
result.explain(extended=False)

# Shows: PushedFilters: [IsNotNull(age), GreaterThan(age, 25)]
#        ↑ IsNotNull(age) is implicit (null > 25 = null, not true)
#        ↑ All filters applied during FileScan, not after!
```

**Other Implicit Optimizations Catalyst Does:**

1. **IsNotNull checks** - Added automatically for comparisons (null > X = null)
2. **Constant folding** - `5 + 3` evaluated to `8` before execution
3. **Dead code elimination** - Unused columns never read from disk
4. **Join reordering** - Optimizes join order for efficiency
5. **Boolean simplification** - `true AND x` becomes just `x`
6. **Null propagation** - `null * anything` = `null` eliminated early
7. **Type coercion** - Implicit conversions applied intelligently
8. **Common sub-expression elimination** - Duplicate expressions computed once
9. **Partition pruning** - Entire partitions skipped if they can't match filters

**For detailed explanations with examples:** [0111-catalyst-implicit-optimizations.md](0111-catalyst-implicit-optimizations.md)

**Bottom line:** DataFrames with Catalyst are 10-100x faster than equivalent RDD code.

---

## Lazy Evaluation

**Key concept:** Transformations don't execute immediately. Spark builds a plan and only runs on actions.

```python
# Transformations (lazy - builds plan):
df = spark.read.csv("file.csv")           # Planning
df = df.filter(df.salary > 100000)        # Planning
df = df.select("name", "dept")            # Planning

# Actions (execute - run the plan):
df.show()              # NOW it runs!
df.collect()           # Returns results
df.write.parquet(...)  # Saves to disk
df.count()             # Counts rows
```

**Why it matters:** Catalyst optimizer sees the entire plan before execution, enabling optimizations that aren't possible with RDD's eager evaluation.

```python
# Check execution plan before running
df.explain(extended=False)  # Shows optimization details
```

---

### df.map() vs df.rdd.map() - Key Distinction

**IMPORTANT:** `df.map()` doesn't exist! Use `df.rdd.map()` instead. When using `df.rdd.map()`, the lambda receives a **Row object** (all columns), not a single value.

```python
# WRONG: df.map() doesn't exist
df.map(lambda x: x * 2)  # ERROR!

# RIGHT: Use df.rdd.map() but row has ALL columns
df.rdd.map(lambda row: row.salary * 2).collect()  # row.salary to extract
# Output: [100000, 120000, 150000]

# BEST: Use DataFrame methods (Catalyst optimized)
df.withColumn("double_salary", df.salary * 2)  # 10-100x faster!
```

**Transformation Types:**
- **Narrow:** Filter, select, withColumn (data stays on partition)
- **Wide:** GroupBy, join, repartition (shuffles data between partitions)

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

### Monitoring Partition Size & Count

**How to inspect partitions before write:**

```python
# Get partition count
num_partitions = df.rdd.getNumPartitions()
print(f"Partitions: {num_partitions}")

# Get records per partition
partition_records = df.rdd.glom().map(lambda x: len(x)).collect()
print(f"Records per partition: {partition_records}")

# Complete diagnostic function
def analyze_partitions(df):
    """Analyze partition count, records, and estimated size"""
    num_partitions = df.rdd.getNumPartitions()
    total_records = df.count()
    partition_records = df.rdd.glom().map(lambda x: len(x)).collect()

    # Estimate size: sample 1000 rows
    sample_df = df.limit(1000)
    sample_count = sample_df.count()

    if sample_count > 0:
        sample_bytes = len(sample_df.toPandas().to_csv().encode())
        avg_row_size = sample_bytes / sample_count
        partition_sizes_mb = [(count * avg_row_size) / (1024*1024)
                             for count in partition_records]
    else:
        partition_sizes_mb = [0] * num_partitions

    # Print results
    print("=" * 60)
    print(f"PARTITION ANALYSIS")
    print("=" * 60)
    print(f"Total Partitions:          {num_partitions}")
    print(f"Total Records:             {total_records:,}")
    print(f"Total Size (est.):         {sum(partition_sizes_mb):.2f} MB")
    print(f"\nPartition Details:")
    print("-" * 60)

    for i, (records, size_mb) in enumerate(zip(partition_records, partition_sizes_mb)):
        print(f"  Partition {i:2d}: {records:8,} records | {size_mb:8.2f} MB")

    print("-" * 60)
    print(f"Min: {min(partition_records):,} records ({min(partition_sizes_mb):.2f} MB)")
    print(f"Max: {max(partition_records):,} records ({max(partition_sizes_mb):.2f} MB)")
    print(f"Avg: {total_records//num_partitions:,} records ({sum(partition_sizes_mb)/len(partition_sizes_mb):.2f} MB)")
    print("=" * 60)

    return {
        "num_partitions": num_partitions,
        "partition_records": partition_records,
        "partition_sizes_mb": partition_sizes_mb
    }

# Usage: Inspect before write
stats = analyze_partitions(df)

# If too many/large partitions, fix them
if stats["num_partitions"] > 16:
    df = df.coalesce(8)
elif max(stats["partition_sizes_mb"]) > 256:
    needed = int(sum(stats["partition_sizes_mb"]) / 128) + 1
    df = df.repartition(needed)

# Now write with controlled partitions
df.write.parquet("s3://bucket/output/")
```

**Key Functions:**
- `df.rdd.getNumPartitions()` - Get partition count
- `df.rdd.glom().map(len).collect()` - Records per partition
- `df.coalesce(N)` - Merge to N partitions (cheap, no shuffle)
- `df.repartition(N)` - Redistribute to N partitions (expensive, uses shuffle)

---

## How Spark Reads Files: The Small File Problem

**Rule:** Spark splits files by block size (default 128MB). Formula: `# partitions = ceil(file_size / 128MB)`

**The Problem:**
```
10GB file:        ~78 partitions (efficient ✓)
1GB split into 1000× 1MB files: 1000 partitions (overhead ✗)

Why 1000 partitions is bad:
- Cluster has only ~80 task slots (10 executors × 8 cores)
- Tasks complete in 0.5 seconds, then wait for next task (0.2s overhead)
- Scheduling overhead = 95% of time!
- Result: 1GB takes ~200s vs ~50s with large files (4x slower!)
```

**Solutions:**

```python
# Option 1: Coalesce after reading (cheap, no shuffle)
df = spark.read.parquet("s3://small_files/").coalesce(100)

# Option 2: Consolidate during write (prevents future problems)
df.coalesce(50).write.parquet("s3://output/")

# Option 3: Monitor before operations
stats = analyze_partitions(df)  # Check partition distribution
```

**Best practice:** Aim for 128MB-512MB per partition

---

### Real-World Scenario: AWS S3

```python
# S3 doesn't have concept of "block size" like HDFS
# Instead, Spark uses: spark.sql.files.maxPartitionBytes (default 128MB)

# Reading from S3 with different file sizes:

# Scenario A: 100GB file on S3
df = spark.read.parquet("s3://bucket/huge_file.parquet")
# Partitions: 100GB ÷ 128MB = 781 partitions ✅ Good

# Scenario B: 100 files of 1MB each on S3
df = spark.read.parquet("s3://bucket/many_small_files/")
# Partitions: 100 (one per file) ✅ OK

# Scenario C: 10000 files of 100KB each on S3
df = spark.read.parquet("s3://bucket/tiny_files/")
# Partitions: 10000 ❌ Bad! Massive overhead

# Fix Scenario C:
df_fixed = df.coalesce(100)  # Merge 10000→100
# Or during write:
df.coalesce(100).write.parquet("s3://bucket/consolidated/")
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
3. **Study transformations** - See [011-transformations-rdd-vs-dataframe.md](011-transformations-rdd-vs-dataframe.md) for all RDD & DataFrame transformation options
4. **Explore real-world code** -  [11-real-world-pyspark-examples.md](11-real-world-pyspark-examples.md)with production ETL patterns
5. **Deep dive into Catalyst** - [0111-catalyst-implicit-optimizations.md](0111-catalyst-implicit-optimizations.md)

