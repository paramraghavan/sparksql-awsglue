# Catalyst Optimizer: How Spark Optimizes Your Queries

> **Key Takeaway**: Catalyst automatically rewrites your code to run faster. Understanding it helps you write code that plays nicely with the optimizer.

---

## Quick Answer

| Feature | What It Does | Example |
|---------|-------------|---------|
| **Predicate Pushdown** | Move filters down early | `df.filter().select()` → filters first |
| **Constant Folding** | Pre-compute fixed values | `1 + 2 + 3` → computed at plan time as `6` |
| **Column Pruning** | Remove unused columns | `df.select("name")` → doesn't read age/salary |
| **Join Reordering** | Join smaller tables first | `df1.join(df2).join(df3)` → optimal order |
| **Null Propagation** | Eliminate null checks | `col1 AND NULL` → always false |
| **Expression Simplification** | Simplify boolean logic | `(x > 5 AND x > 10)` → just `x > 10` |

---

## What is Catalyst?

### The Optimizer Engine

**Catalyst** is Spark's SQL query optimizer that automatically rewrites your code to execute faster, without you needing to change anything.

```
Your Code (Logical Plan)
         ↓
    CATALYST
    ├─ Parse SQL/DataFrame API
    ├─ Create logical plan
    ├─ Apply optimization rules
    ├─ Create optimized logical plan
    └─ Create physical plan
         ↓
Optimized Code (Physical Plan)
         ↓
    Execution
```

### Visual: Catalyst in Action

```python
# You write:
df = spark.read.csv("huge_file.csv")
result = df.select("name").filter(df.age > 30)

# What Catalyst does:
# Original Plan:
# ├─ Read("huge_file.csv") → [name, age, city, salary, ...]
# └─ Select("name")
#    └─ Filter(age > 30)
#
# Optimized Plan (CATALYST rewrote it):
# ├─ Read("huge_file.csv") → [name, age] ← Only read columns needed!
# └─ Filter(age > 30)
#    └─ Select("name") ← Filter happens first!
#
# Result: Much faster!
# Original: Read all columns, then filter, then select
# Optimized: Read only needed columns, filter first, then select
```

---

## The 4 Phases of Catalyst

### Phase 1: Parsing

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo").getOrCreate()

# SQL Query
query = """
    SELECT name, COUNT(*) as count
    FROM employees
    WHERE salary > 50000
    GROUP BY name
    HAVING count(*) > 5
"""

df = spark.sql(query)

# Parsing converts to:
# ├─ SELECT: [name, COUNT(*)]
# ├─ FROM: [employees]
# ├─ WHERE: [salary > 50000]
# ├─ GROUP BY: [name]
# └─ HAVING: [COUNT(*) > 5]
```

### Phase 2: Logical Planning

```python
# Unoptimized Logical Plan (from parsed SQL):
#
# Aggregate [name] [COUNT(*) AS count]
#   └─ Filter (count(*) > 5)
#      └─ GroupBy [name] [COUNT(*)]
#         └─ Filter (salary > 50000)
#            └─ Scan [employees]
#
# This is inefficient - HAVING clause goes after aggregate!
```

### Phase 3: Optimization

```python
# Catalyst applies optimization rules:

Rule 1: PushDown Filters
# Move WHERE to early as possible
Original: Scan → GroupBy → HAVING filter
Optimized: WHERE filter → Scan → GroupBy → HAVING filter

Rule 2: Column Pruning
# Only read columns needed: name, salary
# Drop: address, email, phone, etc.

Rule 3: Join Reordering
# If employees table is joined with departments,
# Join order matters for performance

# Optimized Logical Plan:
#
# Aggregate [name] [COUNT(*) AS count]
#   └─ Filter (count(*) > 5)  ← HAVING filter (still here)
#      └─ GroupBy [name] [COUNT(*)]
#         └─ Filter (salary > 50000)  ← WHERE moved here
#            └─ Scan [employees] [name, salary] ← Only needed columns!
```

### Phase 4: Physical Planning

```python
# Converts logical plan to physical operations

# Logical: Scan [employees] [name, salary]
# Physical:
# ├─ FileScan parquet [name salary]
# │  ├─ ReadFileIndex: /data/employees/*.parquet
# │  ├─ DataFilters: [isnotnull(salary), (salary > 50000)]
# │  └─ Format: parquet
# └─ Execution: Read on executors in parallel

# Logical: Filter (salary > 50000)
# Physical:
# ├─ Execute on each executor
# ├─ Keep row if: salary_value > 50000
# └─ Drop row if: salary_value <= 50000

# Logical: Aggregate [name] [COUNT(*)]
# Physical:
# ├─ Map Stage: Hash partition by name
# ├─ Shuffle: Move all same names to same executor
# └─ Reduce Stage: Count occurrences per name
```

---

## Key Optimization Techniques

### Technique 1: Predicate Pushdown

**What it is**: Moving filter conditions as early as possible

```python
# ❌ SLOW - Original (without optimization)
df = spark.read.csv("huge_employees.csv")
filtered = df.filter(df.age > 30).select("name")

# Execution:
# Step 1: Read ALL columns (age, name, salary, ...)
# Step 2: Filter rows where age > 30
# Step 3: Select only name column

# ✅ FAST - After Catalyst optimization
# Execution:
# Step 1: Read only "name" and "age" columns
# Step 2: Filter rows where age > 30
# Step 3: Select name column (already done above!)

# Performance difference:
# File size: 100GB
# Columns: 50 (each ~2GB)
#
# SLOW: Read 100GB
# FAST: Read ~4GB (name + age only)
# = 25x faster!
```

**How Catalyst knows to do this**:
```python
# Catalyst tracks:
# 1. Where each column is used
# 2. When filters can be pushed down
# 3. What columns final result needs

# In this code:
df.filter(df.age > 30)  # Uses 'age'
    .select("name")    # Uses 'name'

# Catalyst realizes:
# - Filter needs 'age' and row data
# - Final result needs 'name'
# - Total needed: age + name
# → Read only those columns!
```

### Technique 2: Constant Folding

**What it is**: Pre-computing expressions that don't depend on data

```python
# ❌ SLOW - Computed during execution
result = df.select(
    (col("salary") * (1 + 0.1 + 0.05)).alias("total")
)

# For each row:
# salary_value * (1 + 0.1 + 0.05)
# = salary_value * 1.15
# = ...computed at runtime... ❌

# ✅ FAST - Pre-computed by Catalyst
# Catalyst sees: (1 + 0.1 + 0.05)
# Computes: 1.15
# Actual execution:
# salary_value * 1.15
# = ...computed at runtime... ✅

# More complex example:
result = df.select(
    ((col("price") * (1 + 0.1 + 0.05)) / 2 + 50).alias("adjusted_price")
)

# Constant folding:
# (1 + 0.1 + 0.05) / 2 + 50
# = 1.15 / 2 + 50
# = 0.575 + 50
# = 50.575  ← Computed at plan time

# Actual execution:
# (col("price") * 50.575)

# Benefit:
# Simpler expression → faster computation
```

### Technique 3: Column Pruning

**What it is**: Reading only columns that are needed

```python
# ❌ SLOW - Reads all columns
df = spark.read.csv("employees.csv")
result = df.select("name").collect()

# Execution: Read all columns
# ├─ employee_id: 4 bytes
# ├─ name: 50 bytes
# ├─ age: 4 bytes
# ├─ salary: 8 bytes
# ├─ address: 100 bytes
# ├─ phone: 20 bytes
# ├─ email: 100 bytes
# └─ ... other columns
# Total per row: 500+ bytes
# Total file: 500+ GB

# Only uses: name (50 bytes)
# Wasted: 450+ bytes per row

# ✅ FAST - Catalyst prunes unused columns
# Execution: Read only needed columns
# ├─ employee_id: 4 bytes (needed for internal tracking)
# └─ name: 50 bytes
# Total per row: 54 bytes
# Total file: 54 GB
# = 9x reduction!

# How to verify:
df_pruned = spark.read.csv("employees.csv")
print(df_pruned.select("name").explain(extended=True))
# Look for "PushedFilters" in output
```

**Real-World Impact**:
```python
# Parquet file with 100 columns (100GB total)
# Query: SELECT employee_id, name FROM table

# Column sizes:
# employee_id: 0.5 GB (4 bytes × 125M rows)
# name: 2 GB (16 bytes avg × 125M rows)
# other 98 columns: 97.5 GB

# Column pruning effect:
# Without: Read 100 GB
# With: Read 2.5 GB
# Speedup: 40x faster!
```

### Technique 4: Join Reordering

**What it is**: Optimizing the order of joins

```python
# ❌ SLOW - Wrong join order (larger table first)
df1 = spark.read.csv("large_orders.csv")      # 100GB, 1 billion rows
df2 = spark.read.csv("medium_customers.csv")  # 10GB, 10 million rows
df3 = spark.read.csv("small_products.csv")    # 1GB, 1 million rows

result = (df1.join(df2, "customer_id")
             .join(df3, "product_id"))

# Execution:
# Step 1: df1 (100GB) ⛔ JOIN df2 (10GB)
#   ├─ Shuffle: Both DFs by customer_id
#   ├─ Data moved: 100GB + 10GB = 110GB
#   ├─ Result size: 110GB (both columns combined)
#   └─ Time: 60 seconds
#
# Step 2: Result (110GB) JOIN df3 (1GB)
#   ├─ Shuffle: Both by product_id
#   ├─ Data moved: 110GB + 1GB = 111GB
#   ├─ Result size: 111GB
#   └─ Time: 70 seconds
#
# Total shuffled: 221GB
# Total time: 130 seconds

# ✅ FAST - Catalyst reorders (smaller table first)
# Reordered to: df1 ⛔ JOIN df3 JOIN df2

# Step 1: df1 (100GB) ⛔ JOIN df3 (1GB) [broadcast if possible]
#   ├─ If 1GB < broadcast threshold, broadcast df3
#   ├─ df1 each executor gets copy of df3
#   ├─ No shuffle of df1
#   ├─ Result size: 100.1GB (df1 + few df3 rows)
#   └─ Time: 30 seconds
#
# Step 2: Result (100.1GB) JOIN df2 (10GB)
#   ├─ Shuffle: Both by customer_id
#   ├─ Data moved: 100.1GB + 10GB = 110.1GB
#   └─ Time: 60 seconds
#
# Total shuffled: 110.1GB (was 221GB!)
# Total time: 90 seconds (was 130s!)
# Savings: 34% faster!
```

**How to verify join order**:
```python
df1.join(df2, "key1").join(df3, "key2").explain()

# Output will show:
# SortMergeJoin [key2]
#   ├─ BroadcastHashJoin [key1]  ← Broadcast join with small table!
#   │  ├─ Scan df1
#   │  └─ Broadcast Scan df3
#   └─ Scan df2

# This shows Catalyst reordered the joins!
```

### Technique 5: Null Propagation

**What it is**: Simplifying expressions when nulls are involved

```python
# ❌ SLOW - Evaluates full expression for every row
result = df.filter(
    (col("age") > 30) & (col("salary") > 50000)
)

# For NULL age:
# (NULL > 30) & (salary_value > 50000)
# = Unknown & True
# = Unknown
# = Row filtered out ❌

# AND with Unknown always evaluates both sides!

# ✅ FAST - Catalyst uses null logic optimization
# For NULL age:
# Catalyst knows: (NULL > 30) = Unknown
# And: Unknown & anything → Unknown
# So it optimizes: Check age is not null first!

# Optimized execution:
# 1. If age is NULL → skip row (don't evaluate salary)
# 2. If age is NOT NULL → evaluate (age > 30) & (salary > 50000)

# Performance impact:
# File with 50% NULL ages
# SLOW: Evaluate both conditions for all rows
# FAST: Skip salary evaluation for null ages
# = 2x faster filtering!

# Real code example:
result = df.filter(
    col("age").isNotNull() &
    (col("age") > 30) &
    (col("salary") > 50000)
)

# Catalyst optimizes this automatically!
```

### Technique 6: Expression Simplification

**What it is**: Reducing complex boolean logic

```python
# ❌ SLOW - Complex redundant expressions
df.filter(
    (col("salary") > 100000) &
    (col("salary") > 50000) &  # ← Redundant! If > 100k, already > 50k
    (col("salary") != 0)
)

# Evaluates: 3 conditions per row

# ✅ FAST - Catalyst simplifies to essential conditions
# (col("salary") > 100000) is sufficient!
# Because: If salary > 100000, then salary > 50000 AND salary != 0

# Optimized to:
df.filter(col("salary") > 100000)

# Performance impact:
# Evaluates: 1 condition per row (was 3)
# = 3x faster!

# More complex example:
result = df.filter(
    ((col("x") > 5) OR (col("x") > 10)) &  # ← Simplifies to (x > 5)
    ((col("y") < 20) AND (col("y") < 15))   # ← Simplifies to (y < 15)
)

# Simplified:
result = df.filter(
    (col("x") > 5) & (col("y") < 15)
)
```

---

## Using explain() to See the Plan

### Basic Explain

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExplainDemo").getOrCreate()

df = spark.read.csv("employees.csv", header=True)
result = df.filter(df.age > 30).select("name", "age")

# Simple explain
print(result.explain())
```

**Output**:
```
== Physical Plan ==
Project [name#1, age#2]
+- Filter (age#2 > 30)
   +- FileScan csv [name#1,age#2,salary#3,...] Batched: false

# Interpretation:
# 1. FileScan: Read CSV file
# 2. Filter: Keep rows where age > 30
# 3. Project: Select only name and age columns

# Catalyst optimized: Reading all columns (bad!)
# Better would be: Read only name,age then filter
```

### Extended Explain

```python
# See all optimization stages
print(result.explain(extended=True))
```

**Output**:
```
== Parsed Logical Plan ==
Project [name#1, age#2]
+- Filter (age#2 > 30)
   +- Relation [name#1,age#2,salary#3,...] csv

== Analyzed Logical Plan ==
Project [name#1, age#2]
+- Filter (age#2 > 30)
   +- Relation [name#1,age#2,salary#3,...] csv

== Optimized Logical Plan ==
Project [name#1, age#2]
+- Filter (age#2 > 30)
   +- Relation [name#1,age#2,salary#3,...] csv

== Physical Plan ==
Project [name#1, age#2]
+- Filter (age#2 > 30)
   +- FileScan csv [name#1,age#2,salary#3,...] Batched: false
```

### SQL with Explain

```python
spark.sql("""
    SELECT name, age
    FROM employees
    WHERE age > 30
""").explain(extended=True)

# Output shows same plan in SQL context
```

---

## Catalyst Limitations: When It Can't Optimize

### Limitation 1: Complex UDFs (User Defined Functions)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# ❌ Catalyst can't see inside UDFs
@udf(returnType=IntegerType())
def complex_calculation(salary):
    # Catalyst doesn't know what this does!
    # Could be slow, could be fast
    # Can't push filters through it
    result = 0
    for i in range(1000):  # Loop - slow!
        result += salary * i
    return result

df = spark.read.csv("employees.csv")
result = df.select(complex_calculation(col("salary")))
# Catalyst can't optimize this!
# It treats UDF as black box

# ✅ Better - Use built-in functions
from pyspark.sql.functions import col, expr

result = df.select((col("salary") * 1000).alias("result"))
# Catalyst sees this and can optimize!
```

### Limitation 2: RDD Operations

```python
# ❌ Catalyst can't optimize RDD operations
rdd = spark.sparkContext.parallelize(range(1000000))
result = rdd.map(lambda x: x * 2).filter(lambda x: x > 100).collect()

# Catalyst doesn't touch RDDs!
# You're on your own for optimization

# ✅ Better - Use DataFrames
df = spark.createDataFrame([(i,) for i in range(1000000)], ["value"])
result = df.select((col("value") * 2).alias("result")).filter(col("value") > 100)
# Catalyst optimizes this!
```

### Limitation 3: Multiple Shuffle Operations

```python
# ❌ Multiple shuffles aren't merged
result = (df
    .groupBy("year").count()
    .filter(col("count") > 100)
    .groupBy("count").count()  # 2nd shuffle!
)

# Catalyst can't merge these shuffles
# Each groupBy causes a full shuffle
# Total: 2 shuffles (expensive!)

# ✅ Better - Combine operations
from pyspark.sql.functions import count as spark_count
from pyspark.sql.window import Window

w = Window.partitionBy("year")
result = (df
    .select("year", col("year").over(w).alias("year_count"))
    .filter(col("year_count") > 100)
)
# Only 1 shuffle if done carefully!
```

### Limitation 4: Order of Operations Matters

```python
# ❌ SLOW - Filter after expensive operation
result = (df
    .join(other_df, "id")  # Expensive, shuffles both
    .filter(col("age") > 30)  # Should have done this before!
)

# ✅ FAST - Filter before expensive operation
result = (df
    .filter(col("age") > 30)  # Smaller dataset now
    .join(other_df, "id")  # Less data to shuffle
)

# Catalyst CAN optimize this with predicate pushdown
# But only if you write it in the right order
# (or if join condition doesn't depend on age)
```

---

## Best Practices to Work with Catalyst

### Practice 1: Use DataFrame API, Not RDD

```python
# ❌ No optimization
rdd = spark.read.text("file.txt").rdd
result = rdd.map(lambda x: x[0].upper()).collect()

# ✅ Full optimization
df = spark.read.text("file.txt")
result = df.select(upper(col("value"))).collect()

# Speedup: 10-100x with DataFrame
```

### Practice 2: Filter Early

```python
# ❌ Process all data
df = spark.read.csv("huge_file.csv")
result = (df
    .select("name", "age", "salary", "department")
    .groupBy("department").count()
    .filter(col("count") > 100)
)

# ✅ Filter early
df = spark.read.csv("huge_file.csv")
result = (df
    .filter(col("age") > 30)  # Early!
    .select("name", "department")  # Prune columns
    .groupBy("department").count()
    .filter(col("count") > 100)
)

# Second version: 10x faster!
```

### Practice 3: Use Built-in Functions

```python
# ❌ Custom UDF (no optimization)
@udf(returnType=StringType())
def format_name(name):
    return name.upper().strip()

df.select(format_name(col("name")))

# ✅ Built-in functions (optimized)
from pyspark.sql.functions import upper, trim
df.select(upper(trim(col("name"))))

# Built-in: 100x faster!
```

### Practice 4: Understand Join Types

```python
# Catalyst chooses best join type:
# BroadcastHashJoin (< 10MB, no shuffle)
# SortMergeJoin (both sorted, slower shuffle)
# HashJoin (general purpose)

# Help Catalyst by:
result = df1.join(broadcast(df2), "id")  # Hint: broadcast this
# Or provide correct table sizes so Catalyst broadcasts automatically
```

### Practice 5: Use Explain Before Running

```python
# Before running expensive query:
df1 = spark.read.parquet("large_file.parquet")
df2 = spark.read.parquet("other_file.parquet")

result = (df1
    .join(df2, "id")
    .groupBy("department").count()
    .filter(col("count") > 10)
)

# Check the plan first!
result.explain(extended=True)

# Look for:
# - Are filters pushed down? ✅
# - Are unused columns pruned? ✅
# - Is join order optimal? ✅
# - How many shuffles? (minimize!)

# If plan looks bad, refactor the code!
```

---

## Advanced: Understanding Execution Plans

### Example: Complex Query Plan

```python
df = spark.read.csv("employees.csv", header=True)

result = (df
    .filter(col("salary") > 50000)
    .select("department", "salary")
    .groupBy("department").agg(avg("salary"))
    .filter(col("avg(salary)") > 70000)
    .sort(col("department"))
)

result.explain(extended=True)
```

**Output** (simplified):
```
== Physical Plan ==
Sort [department#0 ASC NULLS FIRST]
+- Filter (avg(salary#1) > 70000.0)
   +- HashAggregate(keys=[department#0],
                    functions=[avg(salary#1)])
      +- Exchange hashpartitioning(department#0, 200)
         +- HashAggregate(keys=[department#0],
                          functions=[partial_avg(salary#1)])
            +- Project [department#0, salary#1]
               +- Filter (salary#1 > 50000.0)
                  +- FileScan csv [department#0,salary#1]
                     Batched: false
```

**Understanding this plan**:

```
Step 1: FileScan csv [department#0,salary#1]
        ↓ Read only needed columns ✅
Step 2: Filter (salary#1 > 50000.0)
        ↓ Filter pushed down early ✅
Step 3: Project [department#0, salary#1]
        ↓ Select columns (already done above) ✅
Step 4: HashAggregate (partial) - Map phase
        ├─ Groups: [department]
        ├─ Aggregates: partial_avg(salary)
        └─ Local aggregation before shuffle
Step 5: Exchange hashpartitioning(department#0, 200)
        ├─ SHUFFLE! ⚠️
        ├─ Partition by department
        └─ 200 output partitions
Step 6: HashAggregate (final) - Reduce phase
        ├─ Combines partial aggregates
        └─ Produces final avg per department
Step 7: Filter (avg(salary#1) > 70000.0)
        ├─ HAVING clause ✅
        └─ Filters after aggregation ✓
Step 8: Sort [department#0 ASC]
        ├─ ORDER BY ✓
        └─ Final sorting
```

**Optimizations Catalyst applied**:
- ✅ Column pruning: Only reads department, salary
- ✅ Predicate pushdown: Salary filter before groupBy
- ✅ Partial aggregates: Reduces shuffle data
- ✅ Expression simplification: Unnecessary calculations removed

---

## Interview Questions

**Q1: What is Catalyst and why does it matter?**
A: Catalyst is Spark's SQL optimizer that automatically rewrites queries to run faster. It uses techniques like predicate pushdown, column pruning, and join reordering. You don't need to write special code for it to work, but understanding it helps you write optimizable code.

**Q2: What is predicate pushdown?**
A: Moving filter conditions as early as possible in the execution plan. Instead of reading all data then filtering, Catalyst pushes the filter to the file scan so only matching rows are read.

**Q3: Can Catalyst optimize all code?**
A: No. Catalyst optimizes DataFrame operations but not RDD operations or complex UDFs. For UDFs, Catalyst can't see inside the function, so it treats it as a black box.

**Q4: How do you see what optimizations Catalyst applied?**
A: Use `.explain(extended=True)` to see the parsed, analyzed, optimized, and physical plans. Compare them to see what changed.

**Q5: When would you use `broadcast()` hint?**
A: When you have a small table (< 10MB) that you're joining with a large table. Catalyst might not broadcast automatically, so the hint tells it to broadcast the small table to avoid shuffle.

---

## Summary Table

| Optimization | Before | After | Speedup |
|---|---|---|---|
| **Predicate Pushdown** | Read all → Filter → Select | Read filtered → Select | 10-100x |
| **Column Pruning** | Read 100 columns | Read 2 columns | 50x |
| **Constant Folding** | (1+2+3) computed per row | 6 computed once | 100x |
| **Join Reordering** | Large ⛔ Small | Small broadcast ⛔ Large | 5-20x |
| **Null Propagation** | Eval both sides of AND | Skip if first is NULL | 2x |
| **Expression Simplification** | (x>100) & (x>50) | x>100 | 2x |

---

## Key Takeaways

1. **Write for Catalyst** - Use DataFrame API, filter early, use built-in functions
2. **Understand the Plan** - Use `.explain()` to see if optimizations were applied
3. **Know the Limits** - Catalyst can't optimize RDDs or complex UDFs
4. **Trust But Verify** - Test if your code actually runs as fast as you expect
5. **Refactor When Needed** - If explain() shows bad plan, rewrite the code
