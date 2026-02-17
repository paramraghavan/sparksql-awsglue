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

---

## Part 10: Core Concepts & Interview Questions

### Spark Architecture Fundamentals

**Q: What is Apache Spark and how does it differ from MapReduce?**

Spark is a distributed computing engine for large-scale data processing. Key differences from MapReduce:

| Feature | MapReduce | Spark |
|---------|-----------|-------|
| Processing | Disk-based between stages | In-memory (up to 100x faster) |
| Programming Model | Map → Reduce only | Map, Reduce, Join, Window, ML, Graph, Streaming |
| Languages | Java | Python, Scala, Java, R, SQL |
| Iterative Jobs | Writes to disk each iteration | Keeps data in memory across iterations |
| Real-time | Batch only | Batch + Streaming (micro-batch & continuous) |

**Q: Explain Spark's cluster architecture.**

```
Driver Program (SparkContext / SparkSession)
    │
    ├── Cluster Manager (YARN / Mesos / K8s / Standalone)
    │
    ├── Executor 1 (Worker Node)
    │   ├── Task 1  ── Partition 1
    │   ├── Task 2  ── Partition 2
    │   └── Cache (Block Manager)
    │
    └── Executor 2 (Worker Node)
        ├── Task 3  ── Partition 3
        ├── Task 4  ── Partition 4
        └── Cache (Block Manager)
```

- **Driver**: Creates SparkContext, builds the DAG, schedules tasks, collects results
- **Cluster Manager**: Allocates resources across the cluster
- **Executors**: JVM processes on worker nodes that run tasks and cache data
- **Tasks**: Smallest unit of work, each processing one partition

---

### RDD vs DataFrame vs Dataset

**Q: What are the differences between RDD, DataFrame, and Dataset?**

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| Type Safety | Compile-time | Runtime | Compile-time (Scala/Java only) |
| Optimization | No Catalyst/Tungsten | Full Catalyst + Tungsten | Full Catalyst + Tungsten |
| Schema | No schema | Schema (StructType) | Schema (case class / Encoder) |
| API | Functional (map, filter) | Declarative (select, where) | Both |
| Serialization | Java serialization | Tungsten binary (off-heap) | Tungsten binary |
| Use Case | Low-level control, unstructured data | Structured/semi-structured data | Type-safe structured (Scala) |
| Python Support | Yes | Yes | No (Scala/Java only) |

```python
# RDD example (low-level, avoid in modern PySpark)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd.map(lambda x: x * 2).filter(lambda x: x > 4).collect()  # [6, 8, 10]

# DataFrame example (preferred in PySpark)
df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["num"])
df.withColumn("doubled", F.col("num") * 2).filter(F.col("doubled") > 4).show()

# Convert between RDD and DataFrame
rdd_from_df = df.rdd                            # DataFrame → RDD
df_from_rdd = rdd.toDF(["value"])               # RDD → DataFrame
```

---

### Lazy Evaluation & DAG

**Q: What is lazy evaluation in Spark? Why is it important?**

Spark uses **lazy evaluation** — transformations are not executed immediately. Instead, Spark builds a **DAG (Directed Acyclic Graph)** of transformations. Execution only happens when an **action** is called.

**Transformations** (lazy — return a new DataFrame):
- `select()`, `filter()`, `groupBy()`, `join()`, `withColumn()`, `orderBy()`
- `map()`, `flatMap()`, `union()`, `distinct()`, `repartition()`

**Actions** (trigger execution — return results):
- `show()`, `count()`, `collect()`, `take()`, `first()`
- `write.*`, `foreach()`, `reduce()`, `toPandas()`

```python
# Nothing executes yet — Spark just records the plan
df_filtered = df.filter(F.col("age") > 30)          # Transformation
df_selected = df_filtered.select("name", "salary")   # Transformation
df_sorted = df_selected.orderBy(F.desc("salary"))    # Transformation

# NOW Spark executes the entire chain (optimized by Catalyst)
df_sorted.show(10)  # Action — triggers execution

# View the execution plan
df_sorted.explain(True)
```

**Benefits of lazy evaluation:**
1. **Optimization**: Catalyst can optimize the entire chain (predicate pushdown, column pruning)
2. **Efficiency**: Avoids unnecessary intermediate computation
3. **Pipelining**: Multiple transformations are combined into single-stage tasks

---

### Narrow vs Wide Transformations

**Q: What is the difference between narrow and wide transformations?**

| Narrow Transformations | Wide Transformations |
|----------------------|---------------------|
| Each input partition contributes to at most one output partition | Input partitions contribute to multiple output partitions |
| No data shuffle across the network | Requires shuffle (data exchange between executors) |
| `map`, `filter`, `select`, `withColumn`, `union` | `groupBy`, `join`, `orderBy`, `repartition`, `distinct` |
| Fast, pipelined within a stage | Expensive, creates a new stage (stage boundary) |

```python
# Narrow — no shuffle, stays within same partition
df.filter(F.col("age") > 30).withColumn("senior", F.lit(True))

# Wide — triggers shuffle, creates stage boundary
df.groupBy("department").agg(F.avg("salary"))

# Check number of stages in Spark UI (each wide transformation = new stage)
```

---

### Catalyst Optimizer & Tungsten

**Q: What is the Catalyst Optimizer?**

Catalyst is Spark SQL's query optimizer that transforms logical plans into optimized physical plans:

1. **Analysis**: Resolves column names, types, tables using the catalog
2. **Logical Optimization**: Applies rule-based optimizations
   - **Predicate pushdown**: Push filters closer to data source
   - **Column pruning**: Read only needed columns
   - **Constant folding**: Pre-compute constant expressions
   - **Boolean simplification**: Simplify boolean expressions
3. **Physical Planning**: Choose join strategies (broadcast vs sort-merge), scan methods
4. **Code Generation (Tungsten)**: Generate optimized Java bytecode at runtime

```python
# Catalyst automatically optimizes this:
df.select("name", "age", "salary", "department") \
  .filter(F.col("age") > 30) \
  .groupBy("department") \
  .agg(F.avg("salary"))

# Catalyst pushes filter BEFORE the select, reads only 4 columns
# even if the source has 50 columns (column pruning)

# See the optimized plan:
df.filter(F.col("age") > 30).select("name").explain(True)
```

**What is Tungsten?**

Tungsten is Spark's memory management and code generation engine:
- **Off-heap memory**: Bypasses JVM garbage collection
- **Cache-aware computation**: Optimizes CPU cache usage
- **Whole-stage code generation**: Fuses operators into single Java function
- **Binary encoding**: Compact representation, avoids Java object overhead

---

### Shuffle Deep Dive

**Q: What is a shuffle and why is it expensive?**

A shuffle redistributes data across partitions (and executors) over the network. It is the most expensive operation in Spark.

```
Stage 1 (Map side)                    Stage 2 (Reduce side)
┌─────────────────┐                   ┌─────────────────┐
│ Partition 1     │──── shuffle ────▶│ Partition A     │
│ (key: a, b, c)  │    write/read     │ (all key=a)     │
├─────────────────┤                   ├─────────────────┤
│ Partition 2     │──── shuffle ────▶│ Partition B     │
│ (key: a, c, d)  │                   │ (all key=b)     │
├─────────────────┤                   ├─────────────────┤
│ Partition 3     │──── shuffle ────▶│ Partition C     │
│ (key: b, d, e)  │                   │ (all key=c,d,e) │
└─────────────────┘                   └─────────────────┘
```

**Why expensive:**
1. Data is serialized and written to disk (shuffle write)
2. Data is transferred over the network
3. Data is read and deserialized (shuffle read)
4. Can cause OOM if a partition receives too much data (skew)

**How to minimize shuffles:**
```python
# 1. Use broadcast joins for small tables
result = large_df.join(broadcast(small_df), "key")

# 2. Filter early to reduce shuffle data
df_filtered = df.filter(F.col("date") > "2024-01-01")
df_filtered.groupBy("key").count()

# 3. Use coalesce instead of repartition when reducing partitions
df.coalesce(10)  # No shuffle vs df.repartition(10) which shuffles

# 4. Pre-partition data on join keys
df.write.partitionBy("join_key").parquet("output/")
```

---

### Spark Memory Management

**Q: Explain Spark's memory model.**

```
Executor Memory (spark.executor.memory, e.g., 8g)
├── Reserved Memory (300MB fixed)
├── User Memory (1 - spark.memory.fraction) × (Total - 300MB)
│   └── User data structures, UDF variables, metadata
└── Unified Memory (spark.memory.fraction, default 0.6) × (Total - 300MB)
    ├── Storage Memory (for cache/persist)
    │   └── Can borrow from Execution if available
    └── Execution Memory (for shuffles, sorts, joins, aggregations)
        └── Can evict Storage data if needed

Memory Overhead (spark.executor.memoryOverhead, default max(384MB, 0.10 × executor.memory))
└── Off-heap memory, thread stacks, NIO, interned strings
```

---

### Common Interview Coding Questions

**Q: Find the second highest salary per department.**

```python
window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))
df.withColumn("rank", F.dense_rank().over(window_spec)) \
  .filter(F.col("rank") == 2) \
  .select("department", "name", "salary") \
  .show()
```

**Q: Remove duplicate rows keeping the latest record.**

```python
window = Window.partitionBy("user_id").orderBy(F.desc("updated_at"))
df_deduped = df.withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")
```

**Q: Pivot data — rows to columns.**

```python
# Total sales per product per quarter
sales.groupBy("product") \
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]) \
    .agg(F.sum("amount")) \
    .show()

# Unpivot (melt) — columns to rows
from pyspark.sql.functions import expr
df.selectExpr("product", "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, amount)")
```

**Q: Explode nested arrays/structs.**

```python
# Array column → one row per element
df.select("id", F.explode("tags").alias("tag"))

# Nested struct → flatten
df.select("id", "address.city", "address.zip")
df.select("id", F.col("address.*"))  # Expand all struct fields

# Array of structs
df.select("id", F.explode("orders").alias("order")) \
  .select("id", "order.product", "order.amount")

# Posexplode — includes position index
df.select("id", F.posexplode("tags").alias("position", "tag"))
```

**Q: Sessionize user clickstream data.**

```python
# Define session boundary: gap > 30 minutes = new session
window_user = Window.partitionBy("user_id").orderBy("timestamp")

df_with_gap = df.withColumn(
    "prev_ts", F.lag("timestamp").over(window_user)
).withColumn(
    "gap_minutes", (F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")) / 60
).withColumn(
    "new_session", F.when(
        (F.col("gap_minutes") > 30) | F.col("gap_minutes").isNull(), 1
    ).otherwise(0)
)

# Assign session IDs using cumulative sum
session_window = Window.partitionBy("user_id").orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sessions = df_with_gap.withColumn(
    "session_id", F.concat(F.col("user_id"), F.lit("_"), F.sum("new_session").over(session_window))
)
```

**Q: Implement SCD Type 2 (Slowly Changing Dimension).**

```python
from pyspark.sql.functions import current_timestamp, lit

# existing = current dimension table, incoming = new/changed records
existing = spark.read.parquet("dim_customer/")
incoming = spark.read.parquet("staging/customers/")

# Find changed records
changed = incoming.join(existing.filter(F.col("is_current") == True),
    on="customer_id", how="inner"
).filter(
    (incoming["name"] != existing["name"]) |
    (incoming["address"] != existing["address"])
).select(incoming["*"])

# Close old records
closed = existing.join(changed.select("customer_id"), on="customer_id", how="left_semi") \
    .withColumn("is_current", lit(False)) \
    .withColumn("end_date", current_timestamp())

# New versions of changed records
new_versions = changed \
    .withColumn("is_current", lit(True)) \
    .withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp"))

# New customers (not in existing)
brand_new = incoming.join(existing.select("customer_id").distinct(),
    on="customer_id", how="left_anti") \
    .withColumn("is_current", lit(True)) \
    .withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp"))

# Unchanged records
unchanged = existing.join(
    changed.select("customer_id").union(brand_new.select("customer_id")),
    on="customer_id", how="left_anti"
)

# Final dimension table
final_dim = unchanged.unionByName(closed).unionByName(new_versions).unionByName(brand_new)
final_dim.write.mode("overwrite").parquet("dim_customer/")
```

---

### Key Concepts Quick Reference

**Q: What is the difference between `repartition()` and `coalesce()`?**

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Yes (full shuffle) | No (merges partitions locally) |
| Can increase partitions | Yes | No (only decrease) |
| Data distribution | Even | Uneven (combines adjacent) |
| Use when | Increasing partitions, distributing by key | Reducing partitions before write |

**Q: What is predicate pushdown?**

Predicate pushdown pushes filter conditions down to the data source level so only matching rows are read from disk/S3. It works with Parquet, ORC, JDBC, and Delta Lake.

```python
# With Parquet, this only reads row groups where age > 30
# (Parquet stores min/max statistics per row group)
spark.read.parquet("data.parquet").filter(F.col("age") > 30)

# Partition pruning — only reads partition directories matching the filter
spark.read.parquet("data/year=2024/month=*/").filter(F.col("year") == 2024)
```

**Q: What is speculative execution?**

Spark can launch backup copies of slow-running tasks on different executors. Whichever copy finishes first wins. Useful when some nodes are slower (hardware issues, noisy neighbors).

```python
spark.conf.set("spark.speculation", True)           # Enable speculation
spark.conf.set("spark.speculation.multiplier", 1.5)  # Task 1.5x slower than median
spark.conf.set("spark.speculation.quantile", 0.75)   # 75% of tasks must complete first
```

**Q: Explain `cache()` vs `persist()` vs `checkpoint()`.**

| Method | Storage | Lineage | Use Case |
|--------|---------|---------|----------|
| `cache()` | Memory only | Preserved | Quick reuse of DataFrame |
| `persist(level)` | Memory, disk, or both | Preserved | Control storage level |
| `checkpoint()` | Disk (reliable storage) | Truncated | Break long lineage chains, fault tolerance |

```python
# Checkpoint breaks the DAG lineage — useful for iterative algorithms
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
df.checkpoint()  # Materializes and truncates lineage

# Local checkpoint — faster but not fault-tolerant
df.localCheckpoint()
```

**Q: What are Accumulators and Broadcast Variables?**

```python
# Accumulators — write-only shared variables for counters/sums
error_count = spark.sparkContext.accumulator(0)

def process_row(row):
    global error_count
    if row["status"] == "ERROR":
        error_count.add(1)
    return row

df.rdd.foreach(process_row)
print(f"Errors found: {error_count.value}")

# Broadcast Variables — read-only shared lookup data
lookup_dict = {"NY": "New York", "CA": "California", "TX": "Texas"}
broadcast_lookup = spark.sparkContext.broadcast(lookup_dict)

@F.udf(StringType())
def resolve_state(code):
    return broadcast_lookup.value.get(code, "Unknown")

df.withColumn("state_name", resolve_state(F.col("state_code")))
```

**Q: What is Adaptive Query Execution (AQE)?**

AQE (Spark 3.0+) optimizes queries at runtime based on statistics collected during execution:

1. **Dynamically coalesce shuffle partitions**: Combines small post-shuffle partitions
2. **Dynamically switch join strategies**: Switches to broadcast join if a table is smaller than expected
3. **Dynamically handle skewed joins**: Splits skewed partitions into smaller sub-partitions

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

---

### Common Mistakes to Avoid

1. **Using `collect()` on large DataFrames** — brings all data to driver, causes OOM
2. **Not caching reused DataFrames** — recomputes from scratch each time
3. **Using Python UDFs instead of built-in functions** — UDFs serialize data to Python and back (10-100x slower)
4. **Not repartitioning after heavy filtering** — leaves many empty partitions
5. **Using `count()` just to check if empty** — use `df.head(1)` or `df.isEmpty()` instead
6. **Ignoring data skew** — one large partition blocks entire stage
7. **Reading with `inferSchema=True` in production** — requires extra pass over data; define schemas explicitly
8. **Calling actions inside loops** — each action triggers full DAG re-evaluation
9. **Not specifying partition columns when writing** — leads to slow reads on large datasets
10. **Using `orderBy()` globally** — shuffles all data to single partition; use Window functions instead
