# 02 - Transformations: RDD vs DataFrame

## Overview

Transformations are operations that create new RDD/DataFrame from existing ones. They are **lazy** - nothing executes until an action is called.

**Key insight:** RDD and DataFrame support different transformation APIs. Choose the right one for your use case.

---

## Table of Contents

1. [RDD Transformations](#rdd-transformations)
2. [DataFrame Transformations](#dataframe-transformations)
3. [Side-by-Side Comparison](#side-by-side-comparison)
4. [Decision Tree](#decision-tree)
5. [Real Examples](#real-examples)

---

## RDD Transformations

RDD transformations work with untyped, unstructured data. Low-level, flexible, but no automatic optimization.

### Common RDD Transformations

```python
rdd = sc.textFile("data.txt")  # Each line is an element

# 1. MAP - Transform each element
rdd_upper = rdd.map(lambda x: x.upper())
# Input: ["hello", "world"]
# Output: ["HELLO", "WORLD"]

# 2. FLATMAP - Map then flatten result
words = rdd.flatMap(lambda x: x.split())
# Input: ["hello world", "foo bar"]
# Output: ["hello", "world", "foo", "bar"]

# 3. FILTER - Keep matching elements
long_lines = rdd.filter(lambda x: len(x) > 10)
# Input: ["short", "this is a long line"]
# Output: ["this is a long line"]

# 4. MAPPARTITIONS - Transform entire partition
def process_partition(partition):
    yield f"Partition has {sum(1 for _ in partition)} items"

summary = rdd.mapPartitions(process_partition)

# 5. DISTINCT - Remove duplicates
unique = rdd.distinct()
# Input: [1, 2, 2, 3, 3, 3]
# Output: [1, 2, 3]

# 6. UNION - Combine two RDDs
rdd2 = sc.textFile("data2.txt")
combined = rdd.union(rdd2)

# 7. GROUPBYKEY - Group by key (for pair RDDs)
pairs = rdd.map(lambda x: (x[0], x))  # Create key-value
grouped = pairs.groupByKey()

# 8. REDUCEBYKEY - Aggregate by key
word_counts = rdd.flatMap(lambda x: x.split())\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda a, b: a + b)

# 9. SORTBY - Sort elements
sorted_rdd = rdd.sortBy(lambda x: len(x))

# 10. COALESCE - Reduce partitions (no shuffle)
coalesced = rdd.coalesce(4)

# 11. REPARTITION - Change partitions (with shuffle)
repartitioned = rdd.repartition(100)

# 12. JOIN - Join two pair RDDs
rdd1 = sc.parallelize([("A", 1), ("B", 2)])
rdd2 = sc.parallelize([("A", 10), ("B", 20)])
joined = rdd1.join(rdd2)
# Output: [("A", (1, 10)), ("B", (2, 20))]

# 13. CARTESIAN - Cross join (EXPENSIVE!)
cross = rdd1.cartesian(rdd2)
# Creates all combinations

# 14. COLLECT TRANSFORMATIONS (Less common)
sample = rdd.sample(withReplacement=False, fraction=0.1)  # Random sample
take_three = rdd.take(3)  # First 3 (but this is an action!)
```

### RDD Transformation Characteristics

| Aspect | Detail |
|--------|--------|
| **Optimization** | None - you control execution |
| **Data Structure** | Untyped, any type per element |
| **Schema** | No schema required |
| **Parallel** | Some operations require shuffle |
| **Memory** | No automatic memory management |
| **When to Use** | Unstructured data, custom logic |

---

## DataFrame Transformations

DataFrame transformations work with structured, typed data. High-level API with automatic Catalyst optimization.

### Common DataFrame Transformations

```python
df = spark.read.csv("data.csv", header=True)

# COLUMN OPERATIONS (main DataFrame operations)

# 1. SELECT - Choose columns
df_select = df.select("name", "age", "salary")

# Select with expressions
df_select = df.select(df.name, (df.salary * 1.1).alias("salary_with_bonus"))

# 2. FILTER (or WHERE) - Keep matching rows
df_young = df.filter(df.age < 30)
df_young = df.where((df.age < 30) & (df.salary > 50000))

# 3. WITHCOLUMN - Add or transform column
df_with_bonus = df.withColumn("bonus", df.salary * 0.1)
df_upper = df.withColumn("name_upper", F.upper(df.name))

# 4. DROP - Remove column
df_no_id = df.drop("id")

# 5. RENAME - Rename column
df_renamed = df.withColumnRenamed("salary", "annual_salary")

# 6. DISTINCT - Remove duplicate rows
unique_depts = df.select("department").distinct()

# 7. GROUPBY - Aggregate by group
summary = df.groupBy("department").agg(
    F.sum("salary").alias("total_salary"),
    F.count("*").alias("count"),
    F.avg("age").alias("avg_age")
)

# 8. ORDERBY (or SORT) - Sort rows
df_sorted = df.orderBy(df.salary.desc(), df.age.asc())

# 9. LIMIT - Get first N rows
top_10 = df.orderBy(df.salary.desc()).limit(10)

# 10. JOIN - Combine two DataFrames
df1 = spark.read.csv("employees.csv")
df2 = spark.read.csv("departments.csv")

joined = df1.join(df2, df1.dept_id == df2.id, "inner")
# "inner", "outer", "left", "right", "cross"

# 11. UNION / UNIONBYNAME - Combine rows
df_all = df1.union(df2)
df_all = df1.unionByName(df2)  # Match by column name

# 12. EXPLODE - Flatten array columns
df_with_array = spark.createDataFrame([
    ("Alice", ["A", "B", "C"]),
    ("Bob", ["X", "Y"])
], ["name", "skills"])

exploded = df_with_array.select(
    df_with_array.name,
    F.explode(df_with_array.skills).alias("skill")
)
# Output: Alice-A, Alice-B, Alice-C, Bob-X, Bob-Y

# 13. GROUPBY WITH AGGS
grouped = df.groupBy("dept").agg(
    F.max("salary"),
    F.min("age"),
    F.collect_list("name")
)

# 14. WINDOW FUNCTIONS - Ranking, running totals
from pyspark.sql.window import Window

window_spec = Window.partitionBy("dept").orderBy("salary")
df_with_rank = df.withColumn(
    "rank",
    F.rank().over(window_spec)
)

# 15. PIVOT - Reshape data
pivoted = df.groupBy("name").pivot("quarter").sum("sales")
# Converts: (name, quarter, sales) → (name, Q1, Q2, Q3, Q4)

# 16. COALESCE / REPARTITION
df_coalesced = df.coalesce(10)
df_repartitioned = df.repartition(100)

# RDD ACCESS (last resort for complex logic)
df_with_custom = df.rdd.map(lambda row: (row.name, row.salary * 1.1)).toDF()
# But better to use withColumn!
```

### DataFrame Transformation Characteristics

| Aspect | Detail |
|--------|--------|
| **Optimization** | Catalyst optimizer automatically optimizes |
| **Data Structure** | Typed columns with schema |
| **Schema** | Required, enforced at read |
| **Parallel** | All operations leverage parallelism |
| **Memory** | Smart memory management, column pruning |
| **When to Use** | Structured data, standard operations |

---

## Side-by-Side Comparison

| Operation | RDD | DataFrame | Performance |
|-----------|-----|-----------|-------------|
| **Transform each** | `.map(fn)` | `.withColumn(col, expr)` | DF 10-100x faster |
| **Filter** | `.filter(pred)` | `.filter(pred)` | DF optimizes filtering |
| **Flatten** | `.flatMap(fn)` | `.explode(col)` | DF 5-10x faster |
| **Aggregate** | `.reduce(fn)` | `.groupBy().agg()` | DF 10-50x faster |
| **Group** | `.groupByKey()` | `.groupBy(col)` | DF optimizes grouping |
| **Sort** | `.sortBy(fn)` | `.orderBy(col)` | DF optimizes sorting |
| **Join** | `.join(rdd2)` | `.join(df2, on)` | DF 5-20x faster |
| **Distinct** | `.distinct()` | `.distinct()` | DF 10x faster |
| **Partitioning** | `.coalesce()` / `.repartition()` | `.coalesce()` / `.repartition()` | Same |
| **Custom logic** | `.map(custom_fn)` | `.rdd.map()` + `.toDF()` | RDD required |

---

## Decision Tree

```
Do you need to transform data?

1. Is it STRUCTURED DATA (CSV, Parquet, JSON)?
   └─ YES → Use DataFrame transformations ✓
      - Faster (Catalyst optimizer)
      - Easier to use
      - Better performance

   └─ NO → Is it SIMPLE TEXT/NUMERIC DATA?
      └─ YES → Can you use DataFrame? (if parseable)
               └─ YES → Use DataFrame ✓
               └─ NO → Use RDD

      └─ NO → Use RDD transformations
         - Unstructured data
         - Complex custom logic
         - No schema needed

2. Do you need CUSTOM PYTHON LOGIC on each row?
   └─ YES → Can you use Catalyst functions?
      └─ YES → Use DataFrame.withColumn() ✓ (faster)
      └─ NO → Use df.rdd.map() (last resort)

   └─ NO → Use DataFrame/SQL operations

3. Performance Critical?
   └─ YES → Always use DataFrame (100x+ faster)
   └─ NO → Choose based on data structure
```

---

## Real Examples

### Example 1: Word Count - RDD vs DataFrame

**RDD Approach (Lower-level):**
```python
text_rdd = sc.textFile("book.txt")

word_counts = text_rdd\
    .flatMap(lambda line: line.split())\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)\
    .sortBy(lambda x: x[1], ascending=False)

top_10 = word_counts.take(10)
for word, count in top_10:
    print(f"{word}: {count}")
```

**DataFrame Approach (Higher-level):**
```python
from pyspark.sql.functions import split, explode, col, desc

text_df = spark.read.text("book.txt")

word_counts = text_df\
    .select(explode(split(col("value"), " ")).alias("word"))\
    .filter(col("word") != "")\
    .groupBy("word").count()\
    .orderBy(desc("count"))

word_counts.limit(10).show()
```

**Performance:** DataFrame is 10-20x faster!

---

### Example 2: Processing Web Logs

**RDD (Custom parsing required):**
```python
def parse_log(line):
    # Custom regex parsing
    pattern = r'(\S+) \S+ \S+ \[(.*?)\] "(\S+) (\S+).*?" (\d+) (\d+)'
    match = re.match(pattern, line)
    if match:
        return {
            "ip": match.group(1),
            "method": match.group(3),
            "path": match.group(4),
            "status": int(match.group(5))
        }
    return None

logs = sc.textFile("access.log")
parsed = logs.map(parse_log).filter(lambda x: x is not None)

# Find errors per path
errors = parsed\
    .filter(lambda x: x["status"] >= 400)\
    .map(lambda x: (x["path"], 1))\
    .reduceByKey(lambda a, b: a + b)
```

**DataFrame (Structured data):**
```python
# Assuming logs are in structured format (Parquet, etc.)
logs_df = spark.read.parquet("logs.parquet")

errors = logs_df\
    .filter(col("status") >= 400)\
    .groupBy("path")\
    .count()\
    .orderBy(desc("count"))

errors.show()
```

**When to use RDD:** Unstructured, custom parsing required
**When to use DataFrame:** Already structured or can be structured

---

### Example 3: Data Pipeline - Best Practices

```python
# ✅ GOOD: Use DataFrame for most operations
df = spark.read.parquet("input.parquet")

result = df\
    .filter(df.amount > 0)\
    .select("customer_id", "amount", "date")\
    .groupBy("customer_id")\
    .agg(F.sum("amount").alias("total"))\
    .filter(col("total") > 1000)\
    .orderBy(desc("total"))\
    .limit(100)

result.write.parquet("output.parquet")

# ❌ AVOID: RDD for structured data
rdd = spark.read.parquet("input.parquet").rdd
result = rdd\
    .map(lambda x: (x.customer_id, x.amount))\
    .filter(lambda x: x[1] > 0)\
    .reduceByKey(lambda a, b: a + b)\
    .filter(lambda x: x[1] > 1000)
# Much slower, harder to read!

# ⚡ HYBRID: Use RDD only for custom logic
df = spark.read.parquet("input.parquet")
df_with_custom = df.rdd\
    .map(lambda row: (row.customer_id, apply_custom_logic(row)))\
    .toDF(["customer_id", "result"])\
    .filter(col("result") > threshold)
```

---

## Key Takeaways

✅ **Use DataFrame for:**
- Structured, typed data
- Standard operations (select, filter, groupBy, etc.)
- Performance-critical code
- Most production workloads

✅ **Use RDD for:**
- Unstructured data (raw text, logs)
- Custom Python logic that can't be expressed in Catalyst
- Complex nested transformations
- When you need fine-grained control

✅ **Performance Rule of Thumb:**
- DataFrame operations: 10-100x faster than equivalent RDD
- Use DataFrame by default, RDD only when necessary

✅ **Best Practice:**
- Read as DataFrame when possible
- Use RDD only for custom parsing/logic
- Convert back to DataFrame for final operations

---

## Next Steps

1. **Master DataFrame transformations** - They're faster and easier
2. **Use RDD only when necessary** - For unstructured data or custom logic
3. **Understand Catalyst** - See Section 01 for how DataFrame optimization works
4. **Profile your code** - Measure DataFrame vs RDD performance for your use case

---
