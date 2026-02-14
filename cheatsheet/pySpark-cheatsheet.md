# PySpark Cheat Sheet
### For Data Engineers & Data Scientists

---

## Part 1: Local Development Setup

### Option A: Docker (Recommended - Works on Windows & Mac)

The easiest way to run PySpark locally with zero configuration hassles.

#### Step 1: Install Docker Desktop
- **Windows**: Download from docker.com/products/docker-desktop
- **Mac**: Download from docker.com or use: `brew install --cask docker`

#### Step 2: Run Jupyter with PySpark

```bash
# Pull and run the official Jupyter PySpark image
docker run -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  jupyter/pyspark-notebook

# For Windows PowerShell, use:
docker run -p 8888:8888 -p 4040:4040 `
  -v ${PWD}:/home/jovyan/work `
  jupyter/pyspark-notebook
```

Open the URL shown in terminal (e.g., http://127.0.0.1:8888/?token=...) to access Jupyter.

---

### Option B: Native Installation (Windows)

#### Step 1: Install Java 8 or 11

```bash
# Using winget (Windows 11/10)
winget install -e --id EclipseAdoptium.Temurin.11.JDK

# Set JAVA_HOME environment variable
# System Properties > Environment Variables > New System Variable
# JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11...
```

#### Step 2: Install Python & PySpark

```bash
# Install Python 3.9+ from python.org

# Create virtual environment
python -m venv pyspark_env
pyspark_env\Scripts\activate

# Install PySpark and Jupyter
pip install pyspark jupyterlab pandas pyarrow findspark
```

#### Step 3: Download Hadoop winutils (Windows Only)

```bash
# Download winutils.exe for your Hadoop version from:
# https://github.com/steveloughran/winutils

# Create folder: C:\hadoop\bin
# Place winutils.exe there
# Set environment variable: HADOOP_HOME = C:\hadoop
```

---

### Option C: Native Installation (Mac)

```bash
# Install Homebrew if not installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Java and Apache Spark
brew install openjdk@11 apache-spark

# Install Python packages
pip3 install pyspark jupyterlab pandas pyarrow findspark

# Add to ~/.zshrc or ~/.bash_profile:
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/*/libexec
export PATH=$SPARK_HOME/bin:$PATH
```

---

### Verify Installation

```bash
# In terminal/command prompt
spark-submit --version

# Start Jupyter Lab
jupyter lab
```

---

## Part 2: PySpark Fundamentals

### Initialize Spark Session

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Local development session
spark = SparkSession.builder \
    .appName("LocalDev") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Access Spark UI at http://localhost:4040
```

### Creating DataFrames

```python
# From Python list
data = [("Alice", 34, "NYC"), ("Bob", 45, "LA")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# ---- Sample datasets (from Archive.zip) ----
# 1) Extract Archive.zip next to your notebook as: data/
DATA_DIR = "data"  # "./data" locally, or "/dbfs/FileStore/data" in Databricks

customers = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/customers.csv"))

sales = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/sales.csv"))

products = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/products.csv"))

departments = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/departments.csv"))

employees = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/employees.csv"))

web_logs = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/web_logs.csv"))

skewed = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/skewed_data.csv"))

ml = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA_DIR}/ml_features.csv"))

# JSON (newline-delimited JSON)
users_json = spark.read.json(f"{DATA_DIR}/sample_data.json")

# Parquet (same datasets are included as parquet too)
customers_pq = spark.read.parquet(f"{DATA_DIR}/customers.parquet")
```

### DataFrame Inspection

```python
df.show(10)                    # Display first 10 rows
df.show(truncate=False)        # Show full content
df.printSchema()               # Show schema with data types
df.dtypes                      # List of (column, type) tuples
df.columns                     # List of column names
df.count()                     # Number of rows
df.describe().show()           # Summary statistics
df.explain()                   # Show execution plan
df.explain(True)               # Detailed execution plan
```

### Column Selection & Manipulation

```python
# Select columns
df.select("name", "age")
df.select(F.col("name"), F.col("age"))
df.select(df["name"], df.age)

# Select with expressions
df.select(
    F.col("name"),
    (F.col("age") + 10).alias("age_plus_10"),
    F.upper(F.col("name")).alias("name_upper")
)

# Add new columns
df.withColumn("new_col", F.lit("constant"))
df.withColumn("age_doubled", F.col("age") * 2)

# Rename columns
df.withColumnRenamed("old_name", "new_name")

# Drop columns
df.drop("column1", "column2")
```

---

## Part 3: Common Transformations

### Filtering Data

```python
# Basic filters
df.filter(F.col("age") > 30)
df.filter("age > 30")                    # SQL expression
df.where(F.col("city") == "NYC")

# Multiple conditions
df.filter((F.col("age") > 30) & (F.col("city") == "NYC"))
df.filter((F.col("age") < 25) | (F.col("age") > 60))

# IN clause
df.filter(F.col("city").isin(["NYC", "LA", "Chicago"]))

# NULL handling
df.filter(F.col("name").isNotNull())
df.filter(F.col("name").isNull())

# String matching
df.filter(F.col("name").like("%Alice%"))
df.filter(F.col("name").rlike("^A.*"))  # Regex
```

### Aggregations

```python
# Basic aggregations
df.groupBy("city").count()
df.groupBy("city").agg(
    F.count("*").alias("total"),
    F.avg("age").alias("avg_age"),
    F.max("age").alias("max_age"),
    F.min("age").alias("min_age"),
    F.sum("salary").alias("total_salary"),
    F.countDistinct("department").alias("unique_depts")
)

# Multiple grouping columns
df.groupBy("city", "department").agg(...)

# Collect values into list
df.groupBy("city").agg(
    F.collect_list("name").alias("names"),
    F.collect_set("department").alias("unique_depts")
)
```

### Joins

```python
# Sample dataset example
# sales enriched with customer + product + sales rep attributes
sales_enriched = (sales
    .join(customers.select("customer_id", "name", "city", "state", "tier"), on="customer_id", how="left")
    .join(products.select("product", "category", "msrp", "cost"), on="product", how="left")
    .join(employees.select(F.col("id").alias("sales_rep_id"), "first_name", "last_name", "department"),
          on="sales_rep_id", how="left")
)

# Anti-join: customers with no purchases
customers_no_sales = customers.join(sales.select("customer_id").distinct(), on="customer_id", how="left_anti")

# Quick reference for join types
df1.join(df2, on="id", how="inner")   # inner (default)
df1.join(df2, on="id", how="left")    # left
df1.join(df2, on="id", how="right")   # right
df1.join(df2, on="id", how="outer")   # full outer
df1.join(df2, on="id", how="left_semi")
df1.join(df2, on="id", how="left_anti")
```

### Window Functions

```python
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))

# Ranking functions
df.withColumn("rank", F.rank().over(window_spec))
df.withColumn("dense_rank", F.dense_rank().over(window_spec))
df.withColumn("row_number", F.row_number().over(window_spec))
df.withColumn("ntile", F.ntile(4).over(window_spec))  # Quartiles

# Analytic functions
df.withColumn("prev_salary", F.lag("salary", 1).over(window_spec))
df.withColumn("next_salary", F.lead("salary", 1).over(window_spec))
df.withColumn("first_val", F.first("salary").over(window_spec))
df.withColumn("last_val", F.last("salary").over(window_spec))

# Running totals
running_window = Window.partitionBy("dept").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(running_window))
```

---

## Part 4: String & Date Functions

### String Functions

```python
# Case conversion (sample: customers)
customers.select(F.upper("name"), F.lower("name"), F.initcap("name")).show(5, truncate=False)

# Trim and padding
customers.select(
    F.trim("name").alias("name_trim"),
    F.lpad(F.col("customer_id").cast("string"), 8, "0").alias("customer_id_padded")
).show(5, truncate=False)

# Substring and length
customers.select(
    "name",
    F.substring("name", 1, 3).alias("name_prefix"),
    F.length("name").alias("name_len")
).show(5, truncate=False)

# Split and explode (customers.tags is a comma-separated string)
customers.select(F.split("tags", ",").alias("tag_array")).show(5, truncate=False)
customers.select(F.explode(F.split("tags", ",")).alias("tag")).groupBy("tag").count().show()

# Regex examples
customers.select(
    F.regexp_extract("email", r"(.+)@(.+)", 1).alias("email_user"),
    F.regexp_replace("phone", r"[^0-9]", "").alias("clean_phone")
).show(5, truncate=False)

web_logs.select(
    "page",
    F.regexp_extract("page", r"^/([^/]+)", 1).alias("route")
).show(5, truncate=False)
```

### Date & Timestamp Functions

```python
# Current date/time
spark.range(1).select(F.current_date().alias("today"), F.current_timestamp().alias("now")).show()

# Parse strings to dates/timestamps (sample datasets)
sales2 = sales.withColumn("sale_date", F.to_date("date", "yyyy-MM-dd"))
logs2 = web_logs.withColumn("ts", F.to_timestamp("timestamp"))

# Extract components
sales2.select(
    "sale_id",
    "sale_date",
    F.year("sale_date").alias("year"),
    F.month("sale_date").alias("month"),
    F.dayofmonth("sale_date").alias("day")
).show(5)

logs2.select(
    "log_id",
    "ts",
    F.hour("ts").alias("hour"),
    F.minute("ts").alias("minute")
).show(5)

# Date arithmetic
sales2.select(
    "sale_date",
    F.date_add("sale_date", 7).alias("plus_7d"),
    F.date_sub("sale_date", 30).alias("minus_30d")
).show(5)

# Format dates
sales2.select(F.date_format("sale_date", "MMM dd, yyyy").alias("pretty_date")).show(5, truncate=False)
```

---

## Part 5: Data Engineering Patterns

### Handling Nulls & Duplicates

```python
# Fill nulls
df.na.fill(0)                          # Fill all nulls with 0
df.na.fill({"age": 0, "name": "Unknown"})
df.fillna({"salary": df.agg(F.avg("salary")).first()[0]})

# Drop nulls
df.na.drop()                           # Drop rows with any null
df.na.drop("all")                      # Drop only if all values null
df.na.drop(subset=["name", "age"])     # Check specific columns

# Coalesce (return first non-null)
df.select(F.coalesce("preferred_name", "name").alias("display_name"))

# Remove duplicates
df.dropDuplicates()                    # All columns
df.dropDuplicates(["email"])           # Based on specific columns

# Keep first/last duplicate based on ordering
window = Window.partitionBy("email").orderBy(F.desc("created_at"))
df.withColumn("rn", F.row_number().over(window)) \
  .filter(F.col("rn") == 1).drop("rn")
```

### Schema Definition & Enforcement

```python
from pyspark.sql.types import *

# Define explicit schema
schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("hire_date", DateType(), nullable=True),
    StructField("metadata", MapType(StringType(), StringType())),
    StructField("tags", ArrayType(StringType())),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType())
    ]))
])

# Read with schema (faster than inferSchema)
df = spark.read.schema(schema).json("data.json")

# Cast column types
df.withColumn("age", F.col("age").cast(IntegerType()))
df.withColumn("amount", F.col("amount").cast("decimal(10,2)"))
```

### Writing Data

```python
# Write to Parquet (recommended)
df.write.mode("overwrite").parquet("output/data.parquet")

# Write modes: overwrite, append, ignore, error (default)
df.write.mode("append").parquet("output/")

# Partitioned write (critical for large datasets)
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("output/partitioned/")

# Control number of output files
df.coalesce(1).write.csv("single_file/")   # Single file
df.repartition(10).write.parquet("output/") # 10 files

# Write to CSV with options
df.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("delimiter", ",") \
    .csv("output.csv")

# Write to Delta (if using Delta Lake)
df.write.format("delta").mode("overwrite").save("delta_table/")
```

---

## Part 6: PySpark ML for Data Scientists

### Feature Engineering

```python
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, StringIndexer,
    OneHotEncoder, Bucketizer, Imputer
)

# Combine features into vector
assembler = VectorAssembler(
    inputCols=["age", "salary", "experience"],
    outputCol="features"
)
df = assembler.transform(df)

# Scale features
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withMean=True, withStd=True
)
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)

# Encode categorical variables
indexer = StringIndexer(inputCol="category", outputCol="category_idx")
encoder = OneHotEncoder(inputCol="category_idx", outputCol="category_vec")

# Handle missing values
imputer = Imputer(
    inputCols=["age", "salary"],
    outputCols=["age_imputed", "salary_imputed"],
    strategy="median"
)
```

### ML Pipeline Example

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

# Sample dataset: ml_features.csv (loaded as `ml`)
df = ml.dropna(subset=["label"])

train, test = df.randomSplit([0.8, 0.2], seed=42)

cat_cols = ["category", "region"]
num_cols = ["age", "income", "credit_score", "years_employed", "num_accounts"]

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in cat_cols]

assembler = VectorAssembler(
    inputCols=num_cols + [f"{c}_vec" for c in cat_cols],
    outputCol="features"
)

rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=200, seed=42)

pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

model = pipeline.fit(train)
pred = model.transform(test)

auc = BinaryClassificationEvaluator(labelCol="label").evaluate(pred)
print("AUC:", auc)
```

### Pandas UDFs for Advanced Analytics

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# Scalar UDF - applied row by row (vectorized)
@pandas_udf(DoubleType())
def normalize(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std()

df.select(normalize("salary").alias("normalized_salary"))

# Grouped Map UDF - apply function to each group
@pandas_udf(df.schema, functionType=PandasUDFType.GROUPED_MAP)
def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["salary"] = pdf["salary"] - pdf["salary"].mean()
    return pdf

df.groupby("department").apply(subtract_mean)
```

---

## Part 7: Performance Optimization

### Caching & Persistence

```python
from pyspark import StorageLevel

# Cache in memory (use when DF is reused multiple times)
df.cache()            # Same as persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist when done
df.unpersist()

# Check if cached
df.is_cached

# Force evaluation and cache
df.cache().count()    # Triggers computation and caches
```

### Partitioning Strategies

```python
# Check current partitions
df.rdd.getNumPartitions()

# Repartition (shuffle - expensive but even distribution)
df.repartition(200)                    # By number
df.repartition("key_column")           # By column (hash)
df.repartition(200, "key_column")      # Both

# Coalesce (no shuffle - for reducing partitions)
df.coalesce(10)                        # Combine into fewer

# Random repartition (for skew issues)
df.withColumn("salt", F.rand()) \
  .repartition(200, "salt") \
  .drop("salt")

# Partition size recommendation: 128 MB per partition
# Calculate: total_data_size_MB / 128 = num_partitions
```

### Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Force broadcast (small table joins large table)
# Best when small table < 10MB (configurable)
result = large_df.join(
    broadcast(small_df),
    "join_key"
)

# Configure auto broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100*1024*1024)  # 100MB
```

### Key Configuration Parameters

| Parameter | Default | Recommendation |
|-----------|---------|----------------|
| `spark.sql.shuffle.partitions` | 200 | Set to data_size_GB * 4 (target 128MB/partition) |
| `spark.default.parallelism` | Total cores | 2-3x total cores for better parallelism |
| `spark.sql.adaptive.enabled` | true (3.0+) | Enable for automatic optimization |
| `spark.executor.memory` | 1g | 4-8g per executor typically |
| `spark.executor.cores` | 1 | 4-5 cores per executor is optimal |
| `spark.memory.fraction` | 0.6 | Increase to 0.8 for memory-heavy jobs |

---

## Part 8: EMR Job Troubleshooting Guide

*Common issues encountered when running PySpark jobs on AWS EMR and their solutions.*

### Issue 1: Data Skew - Uneven Partition Sizes

**Symptom:** One or few tasks take much longer than others. Some executors are idle while others are overloaded. Job appears stuck at 99% for a long time.

#### Diagnosis

```python
# Sample dataset: skewed_data.csv (loaded as `skewed`)
from pyspark.sql.functions import spark_partition_id

# Check partition sizes
(skewed.groupBy(spark_partition_id().alias("partition_id"))
  .count()
  .orderBy(F.desc("count"))
  .show(50))

# Check for skewed keys
(skewed.groupBy("join_key").count()
  .orderBy(F.desc("count"))
  .show(20))
```

#### Solution: Salted Repartition

```python
# Add random salt and repartition evenly
from pyspark.sql.functions import rand, floor

num_partitions = 500  # Adjust based on data size

df_rebalanced = df \
    .withColumn("_salt", floor(rand() * num_partitions).cast("int")) \
    .repartition(num_partitions, "_salt") \
    .drop("_salt")

# For skewed joins, salt both sides
salt_buckets = 10

# Salt the large/skewed table
df_large_salted = df_large \
    .withColumn("_salt", floor(rand() * salt_buckets).cast("int"))

# Explode the small table to match all salts
df_small_exploded = df_small \
    .withColumn("_salt", F.explode(F.array([F.lit(i) for i in range(salt_buckets)])))

# Join on original key + salt
result = df_large_salted.join(
    df_small_exploded,
    ["join_key", "_salt"],
    "inner"
).drop("_salt")
```

---

### Issue 2: Jobs Taking Too Long - Insufficient Parallelism

**Symptom:** Jobs run slowly even with large clusters. Spark UI shows few active tasks. Shuffle operations are bottlenecked.

#### Root Cause

Default shuffle partitions (200) is too low for large datasets, resulting in partitions that are too large (> 200MB each).

#### Solution: Increase Shuffle Partitions

```python
# Calculate optimal partitions: target 128MB per partition
# For 100GB dataset: 100 * 1024 / 128 = 800 partitions

# Set at session level
spark.conf.set("spark.sql.shuffle.partitions", 800)

# Or in spark-submit
spark-submit \
    --conf spark.sql.shuffle.partitions=800 \
    --conf spark.default.parallelism=800 \
    your_job.py

# Enable Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

---

### Issue 3: Out of Memory Errors

**Symptom:** java.lang.OutOfMemoryError, Container killed by YARN for exceeding memory limits, GC overhead limit exceeded.

#### Solutions

```python
# 1. Increase executor memory and overhead
spark-submit \
    --executor-memory 8g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.memory.fraction=0.8 \
    your_job.py

# 2. Reduce executor cores (less concurrent tasks per executor)
--executor-cores 4  # Instead of 5

# 3. Increase partitions to reduce per-partition memory
spark.conf.set("spark.sql.shuffle.partitions", 1000)

# 4. Avoid collect() on large datasets
# BAD:
all_data = df.collect()  # Brings all data to driver!

# GOOD:
df.write.parquet("output/")  # Write to storage instead

# 5. Use approximate distinct instead of exact
df.select(F.approx_count_distinct("user_id"))  # Instead of countDistinct
```

---

### Issue 4: Slow Job Startup / Long Scheduling Delays

**Symptom:** Jobs pending for long time before starting. Spark UI shows scheduling delay.

#### Solutions

```python
# 1. Reduce number of small files (small files problem)
# Compact input files first
df = spark.read.parquet("input/")
df.coalesce(100).write.parquet("compacted/")

# 2. Increase YARN resources
# In EMR cluster configuration:
# yarn.nodemanager.resource.memory-mb: 60000
# yarn.scheduler.maximum-allocation-mb: 60000

# 3. Use fewer, larger executors
--num-executors 20 \
--executor-memory 16g \
--executor-cores 5

# 4. Enable dynamic allocation
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=100
```

---

### Issue 5: Shuffle Spill to Disk

**Symptom:** Excessive shuffle read/write. Jobs slow due to disk I/O. Spark UI shows 'Shuffle Spill (Disk)'.

#### Solutions

```python
# 1. Increase memory for shuffle
spark.conf.set("spark.memory.fraction", 0.8)  # Default 0.6
spark.conf.set("spark.memory.storageFraction", 0.3)  # Default 0.5

# 2. Reduce shuffle data by filtering early
# BAD: Filter after join
result = df1.join(df2, "key").filter("date > '2024-01-01'")

# GOOD: Filter before join
df1_filtered = df1.filter("date > '2024-01-01'")
result = df1_filtered.join(df2, "key")

# 3. Use broadcast for small dimension tables
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_dim), "key")

# 4. Select only needed columns before shuffle
df.select("key", "needed_col1", "needed_col2") \
  .groupBy("key").agg(...)
```

---

### EMR Best Practices Summary

| Scenario | Configuration |
|----------|---------------|
| General ETL | shuffle.partitions = data_size_GB * 4, executor-memory 8g, executor-cores 5 |
| Skewed Joins | Enable AQE: adaptive.skewJoin.enabled = true, or use salting technique |
| Many Small Files | Pre-compact to 128-256MB files, use coalesce() in writes |
| Large Aggregations | Increase shuffle.partitions, use 2-stage aggregation for high cardinality |
| Memory Issues | Reduce executor-cores to 4, increase memoryOverhead, avoid collect() |

---

## Part 9: Quick Reference Card

### Common Import Template

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws,
    sum, avg, count, max, min, countDistinct,
    year, month, dayofmonth, date_format, to_date,
    split, explode, array, struct, collect_list,
    row_number, rank, dense_rank, lead, lag,
    broadcast, spark_partition_id, rand
)
```

### Spark UI Tabs Reference

| Tab | What to Look For |
|-----|------------------|
| Jobs | Failed jobs, skipped stages (good - means data was cached) |
| Stages | Task duration variance, shuffle read/write sizes, skew indicators |
| Storage | Cached DataFrames, memory usage per partition |
| Executors | GC time (>10% is bad), shuffle spill, failed tasks |
| SQL | Query plans, join strategies (broadcast vs shuffle), scan sizes |

### Useful Spark Shell Commands

```bash
# Start PySpark shell with custom config
pyspark --master local[4] \
        --driver-memory 4g \
        --conf spark.sql.shuffle.partitions=8

# Start with Jupyter notebook
PYSPARK_DRIVER_PYTHON=jupyter \
PYSPARK_DRIVER_PYTHON_OPTS='notebook' \
pyspark

# Submit job to EMR
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-memory 8g \
    --executor-cores 5 \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.sql.adaptive.enabled=true \
    s3://bucket/scripts/my_job.py
```

---

> 💡 **Pro Tip:** Always check the Spark UI (port 4040) to understand your job's behavior. The SQL tab shows execution plans that reveal exactly how Spark processes your queries.
