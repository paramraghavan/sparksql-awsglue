# Beginner's Quick Start: PySpark in 30 Minutes

> For someone who has never used Spark before. You'll run real code and understand the basics.

---

## What is Apache Spark?

**Simple Definition**: A powerful tool for processing large amounts of data in parallel across many computers.

**Why it matters**:
- Process data **100x faster** than traditional tools
- Work with **terabytes of data** that don't fit in memory
- Use **Python, SQL, or Scala** (not just MapReduce)
- **One unified platform** for batch, streaming, SQL, ML, and graphs

---

## The Problem Spark Solves

### Without Spark (Traditional Approach)
```python
# Read a 10GB CSV file - might take 5 minutes just to load!
with open("huge_file.csv") as f:
    for line in f:
        process_line(line)  # Process one row at a time

# Problem: Takes forever on one computer
```

### With Spark (Distributed Approach)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.csv("huge_file.csv")  # Spark handles loading

# Process 10GB across 10 computers (each processes 1GB) = 10x faster!
df.filter(df.value > 100).count()
```

**Key insight**: Spark **divides work** across multiple computers.

---

## Three Core Concepts (That's All!)

### 1️⃣ DataFrame (Like a SQL Table)
```python
# A structured collection of data with columns and rows
# Think: Excel spreadsheet or SQL table, but distributed

df = spark.createDataFrame([
    ("Alice", 25, 50000),
    ("Bob", 30, 60000),
], ["Name", "Age", "Salary"])

# It looks like this:
# +-----+---+------+
# | Name|Age|Salary|
# +-----+---+------+
# |Alice| 25| 50000|
# |  Bob| 30| 60000|
# +-----+---+------+
```

### 2️⃣ Transformations (Changes to Data)
```python
# Transformations are "plans" for what to do with data
# They DON'T actually do anything yet (lazy!)

df_filtered = df.filter(df.Age > 25)  # Plan to filter
df_selected = df_filtered.select("Name", "Salary")  # Plan to select

# Think of it like: "I'm going to do A, then B, then C"
# But A, B, C haven't happened yet!
```

### 3️⃣ Actions (Actually Run the Work)
```python
# Actions are "Execute now!"
# This is when Spark actually does the processing

df_filtered.show()  # NOW Spark filters and shows results
df_filtered.count()  # NOW Spark counts all rows
df_selected.write.parquet("output/")  # NOW Spark writes to disk
```

**The Pattern**:
```
Transformations (Planning)  →  Transformations (Planning)  →  Action (Execute!)
     filter()                      select()                     show()
```

---

## Your First Spark Program

### Step 1: Install PySpark
```bash
pip install pyspark
```

### Step 2: Create a File (first_spark.py)
```python
from pyspark.sql import SparkSession

# Create a Spark session (your connection to Spark)
spark = SparkSession.builder \
    .appName("FirstApp") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
data = [
    ("Alice", "Sales", 50000),
    ("Bob", "Engineering", 80000),
    ("Charlie", "Sales", 45000),
    ("David", "Engineering", 90000),
]

# Define column names
columns = ["Name", "Department", "Salary"]

# Create a DataFrame (like a table)
employees = spark.createDataFrame(data, columns)

# Show the data
print("=== All Employees ===")
employees.show()

# Filter: Only engineering employees
print("\n=== Engineering Only ===")
engineers = employees.filter(employees.Department == "Engineering")
engineers.show()

# Aggregate: Average salary by department
print("\n=== Average Salary by Department ===")
avg_salary = employees.groupBy("Department").avg("Salary")
avg_salary.show()

# SQL: Same thing, but using SQL
employees.createOrReplaceTempView("emp")
print("\n=== Using SQL ===")
spark.sql("""
    SELECT Department, AVG(Salary) as AvgSalary
    FROM emp
    GROUP BY Department
""").show()

# Stop Spark when done
spark.stop()
```

### Step 3: Run It
```bash
python first_spark.py
```

### Output
```
=== All Employees ===
+-------+-------------+------+
|   Name|    Department|Salary|
+-------+-------------+------+
|  Alice|        Sales| 50000|
|    Bob|  Engineering| 80000|
|Charlie|        Sales| 45000|
|  David|  Engineering| 90000|
+-------+-------------+------+

=== Engineering Only ===
+----+-------------+------+
|Name|    Department|Salary|
+----+-------------+------+
| Bob|  Engineering| 80000|
|David|  Engineering| 90000|
+----+-------------+------+

=== Average Salary by Department ===
+----------+-----------+
|Department|avg(Salary)|
+----------+-----------+
|        Sales|    47500.0|
|  Engineering|    85000.0|
+----------+-----------+

=== Using SQL ===
+----------+-----------+
|Department| AvgSalary |
+----------+-----------+
|        Sales|    47500.0|
|  Engineering|    85000.0|
+----------+-----------+
```

✅ **Congratulations!** You just processed data with Spark!

---

## Key Vocabulary (5 Terms to Know)

### 1. Partition
Data is split into chunks. Each chunk is processed by one task.

```
Data: [1,2,3,4,5,6,7,8]
Split into 4 partitions:
  Partition 0: [1,2]
  Partition 1: [3,4]
  Partition 2: [5,6]
  Partition 3: [7,8]

Process in parallel:
  Computer 1 processes [1,2]
  Computer 2 processes [3,4]
  Computer 3 processes [5,6]
  Computer 4 processes [7,8]

  → Much faster!
```

### 2. Action vs Transformation
- **Transformation**: "I plan to do X" (lazy, not executed yet)
  - `filter()`, `select()`, `map()`, `groupBy()`
- **Action**: "Do it now!" (executed immediately)
  - `show()`, `count()`, `write()`, `collect()`

### 3. Lazy Evaluation
Transformations don't run until you call an action.

```python
# None of this has been executed yet
df1 = df.filter(df.age > 30)      # Just a plan
df2 = df1.select("name", "salary")  # Just a plan
df3 = df2.orderBy("salary")        # Just a plan

# NOW it executes (when you call an action)
df3.show()  # Action: Execute everything above
```

### 4. Schema
The structure of your data (column names and types).

```python
df.printSchema()  # Show the schema

# Output:
# root
#  |-- Name: string (nullable = true)
#  |-- Age: long (nullable = true)
#  |-- Salary: long (nullable = true)
```

### 5. Driver and Executor
- **Driver**: Your computer (runs your code)
- **Executor**: Worker computers (process data)

```
Your Computer (Driver)         Cloud (Executors)
┌──────────────────┐           ┌─────────────────┐
│ spark.createDF() │──sends───>│ Process data    │
│ df.filter()      │  tasks    │ on Worker 1     │
│ df.show()        │<─results──│                 │
└──────────────────┘           └─────────────────┘
                               ┌─────────────────┐
                               │ Process data    │
                               │ on Worker 2     │
                               └─────────────────┘
```

---

## Common DataFrame Operations

### Reading Data
```python
# From CSV
df = spark.read.csv("data.csv", header=True)

# From Parquet (faster, compressed)
df = spark.read.parquet("data.parquet")

# From JSON
df = spark.read.json("data.json")

# From SQL database
df = spark.read.jdbc(url, table, properties=connection_properties)
```

### Viewing Data
```python
df.show()              # First 20 rows
df.show(100)           # First 100 rows
df.printSchema()       # Column names and types
df.count()             # Total number of rows
df.describe().show()   # Statistics (mean, stddev, etc)
```

### Filtering & Selecting
```python
# Filter: Keep rows that match condition
df.filter(df.age > 30).show()
df.filter((df.age > 30) & (df.salary > 50000)).show()  # Multiple conditions
df.filter(df.name.like("A%")).show()  # Name starts with A

# Select: Choose columns
df.select("name", "salary").show()
df.select(df.name, (df.salary * 1.1).alias("new_salary")).show()

# Drop: Remove columns
df.drop("unnecessary_column").show()

# Distinct: Remove duplicates
df.select("department").distinct().show()
```

### Transforming Data
```python
# Add a new column
df = df.withColumn("bonus", df.salary * 0.1)

# Rename a column
df = df.withColumnRenamed("old_name", "new_name")

# Change data type
df = df.withColumn("age", df.age.cast("int"))

# SQL expressions
from pyspark.sql.functions import upper, lower
df = df.withColumn("name_upper", upper(df.name))
```

### Aggregating & Grouping
```python
# Group and count
df.groupBy("department").count().show()

# Group and aggregate
df.groupBy("department").agg({"salary": "avg", "age": "max"}).show()

# Multiple aggregations
from pyspark.sql.functions import avg, max as spark_max
df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    spark_max("age").alias("max_age")
).show()
```

### Joining Dataframes
```python
# Join two DataFrames
df1 = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])
df2 = spark.createDataFrame([(1, "Sales"), (2, "Engineering")], ["id", "dept"])

# Inner join (keep matching rows)
df1.join(df2, "id").show()
# Output:
# +---+-----+------+
# | id| name|  dept|
# +---+-----+------+
# |  1|Alice| Sales|
# |  2|  Bob|Engineering|
```

### Sorting
```python
df.orderBy("salary").show()           # Ascending
df.orderBy(df.salary.desc()).show()   # Descending
df.sort("age", "salary").show()       # Multiple columns
```

### Writing Data
```python
# Write to Parquet (recommended)
df.write.parquet("output/data.parquet")

# Write to CSV
df.write.csv("output/data.csv", header=True)

# Write to JSON
df.write.json("output/data.json")

# Overwrite if exists
df.write.mode("overwrite").parquet("output/")

# Append to existing
df.write.mode("append").parquet("output/")
```

---

## SQL vs DataFrame API

Both do the same thing. Pick whichever you prefer!

### Option 1: SQL (Feel like SQL already)
```python
employees.createOrReplaceTempView("emp")

result = spark.sql("""
    SELECT department, AVG(salary) as avg_sal
    FROM emp
    WHERE salary > 50000
    GROUP BY department
    ORDER BY avg_sal DESC
""")
result.show()
```

### Option 2: DataFrame API (Chaining)
```python
result = employees \
    .filter(employees.salary > 50000) \
    .groupBy("department") \
    .agg(avg("salary").alias("avg_sal")) \
    .orderBy(desc("avg_sal"))

result.show()
```

Both produce identical results. SQL is good if you know SQL; DataFrame API is more Pythonic.

---

## When Data is Big: Understanding Partitions

When your data is huge (1TB+), Spark divides it:

```
1TB CSV File
     ↓
Spark divides into 1000 partitions
     ↓
Each partition processed by one task on a different computer
     ↓
Results combined
     ↓
Done!
```

**Why partitions matter**:
- More partitions = more parallel work = faster processing
- But too many = overhead
- Default is usually fine, but you can tune it

```python
# Check how many partitions
df.rdd.getNumPartitions()  # Returns number

# Change partition count
df = df.repartition(200)  # Increase to 200 partitions

# Partition by column (useful for data organization)
df.write.partitionBy("year", "month").parquet("output/")
```

---

## Common Beginner Mistakes

### ❌ Mistake 1: Forgetting it's Lazy
```python
# This doesn't do anything!
df.filter(df.age > 30)

# This actually processes
df.filter(df.age > 30).show()
```

### ❌ Mistake 2: Using collect() on Big Data
```python
# This loads ALL data into your computer's memory!
# Don't do this with 1TB of data!
big_result = df.collect()  # ← Can crash your computer!

# Instead, use show() or write to file
df.show(100)  # Show 100 rows
df.write.parquet("output/")  # Write to disk
```

### ❌ Mistake 3: Forgetting to Stop Spark
```python
spark.stop()  # Always call this at the end!
```

### ❌ Mistake 4: Too Many Partitions
```python
# Don't do this!
df.repartition(10000)  # Creates 10000 tiny tasks - slow!

# Do this instead:
df.repartition(200)  # Reasonable number of partitions
```

### ❌ Mistake 5: Not Specifying Schema for CSV
```python
# Spark might guess wrong data types
df = spark.read.csv("data.csv")

# Better: Specify schema
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

---

## Next Steps: What to Learn

### Level 1: Beginner (You are here ✓)
- ✅ What is Spark
- ✅ DataFrames and basic operations
- ✅ Transformations vs Actions
- ✅ Read/filter/select/write data

### Level 2: Intermediate (Next 1-2 weeks)
- [ ] Read: [Spark Architecture](01-spark-architecture.md)
- [ ] Learn: Window functions, complex aggregations
- [ ] Learn: Joins and how to optimize them
- [ ] Learn: Working with files (Parquet, JSON, CSV)
- [ ] Do: 5+ practice problems from [Exercises](../../exercises/)

### Level 3: Advanced (4-8 weeks)
- [ ] Performance tuning
- [ ] Data skew handling
- [ ] Troubleshooting slow jobs
- [ ] Production best practices

---

## Key Takeaways

1. **Spark processes data in parallel** - That's why it's fast
2. **Use DataFrames** - Not RDDs (that's old)
3. **Transformations are lazy** - Nothing happens until you call an action
4. **Actions make it real** - show(), count(), write()
5. **Partitions are key** - Data is split and processed in parallel
6. **Schema matters** - Know your column names and types
7. **Use parquet** - Faster and more reliable than CSV

---

## Resources

- **Official Docs**: https://spark.apache.org/docs/latest/sql/
- **Your Handbook**: Start with [01-spark-architecture.md](01-spark-architecture.md)
- **Practice**: Go to [../../exercises/](../../exercises/)

---

## Practice Exercise

**Try this**:
1. Create a CSV file with 10 people (name, age, city)
2. Read it into Spark
3. Filter for people over age 25
4. Group by city and count
5. Show results
6. Save to parquet

**Time**: 10 minutes
**What you'll learn**: Real data workflow

---

## You're Ready!

You now understand the basics. Time to go deeper!

👉 Next: Read [Spark Architecture](01-spark-architecture.md) for deeper understanding

---

**Questions?** Check:
- [Glossary](../09-reference/glossary.md) - Terms explained
- [Cheatsheet](../09-reference/cheatsheet.md) - Code snippets
- [Master Guide](../00-START-HERE.md) - Full navigation
