# DataFrames Basics

## Overview

DataFrames are the primary abstraction in PySpark for working with structured data. They represent data as a **distributed table** with named columns and typed values. Think of them as SQL tables or Pandas DataFrames, but distributed across a cluster and optimized for parallel processing.

Unlike RDDs (Resilient Distributed Datasets), DataFrames provide:
- **Schema awareness** - column names and types
- **Query optimization** - Catalyst optimizer applies intelligent transformations
- **SQL support** - query using SQL syntax
- **Better performance** - Tungsten off-heap memory management

---

## Core Concepts

- **Immutability**: Once created, DataFrames cannot be modified. All transformations return new DataFrames.
- **Lazy Evaluation**: DataFrame transformations are not executed until an action is called (show, count, collect, write).
- **Schema**: Defines the structure - column names, types, and nullability. Can be inferred or explicitly defined.
- **Partitions**: DataFrames are split into logical partitions for parallel processing across executors.
- **Schema Inference**: PySpark can automatically detect column types by scanning data (slower but convenient).
- **Explicit Schema**: Define schema upfront for better performance and validation in production.

---

## Simple Code Examples

### Creating DataFrames from Lists

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DataFrameDemo").master("local[*]").getOrCreate()

# Simple list with schema inference
data = [("Alice", 30, "NYC"), ("Bob", 35, "LA")]
df = spark.createDataFrame(data, ["name", "age", "city"])
df.show()
```

### Reading CSV with Schema Inference

```python
# Schema inference - slower but convenient for exploration
df = spark.read.option("header", "true").option("inferSchema", "true").csv("employees.csv")
df.show(5)
df.printSchema()
```

### Reading CSV with Explicit Schema

```python
# Explicit schema - faster, validates data on read
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("salary", IntegerType(), True)
])
df = spark.read.schema(schema).option("header", "true").csv("employees.csv")
```

### Basic Operations

```python
# Show first 10 rows
df.show(10)

# Get count without showing data
count = df.count()  # Action - triggers execution

# Print schema
df.printSchema()

# Get column names and types
print(df.columns)
print(df.dtypes)

# Summary statistics
df.describe("age", "salary").show()
```

---

## Real-World Examples

### Example 1: ETL Pipeline - Read, Transform, Write

```python
from pyspark.sql import functions as F

# Read sales data with explicit schema
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("order_date", DateType(), False),
    StructField("status", StringType(), True)
])

sales_df = spark.read.schema(schema).option("header", "true").csv("s3://data-bucket/sales.csv")

# Add processing date column
sales_df_processed = sales_df.withColumn("processed_date", F.current_date())

# Filter valid orders
valid_orders = sales_df_processed.filter(F.col("status").isin(["completed", "shipped"]))

# Show sample
valid_orders.show(5)

# Write to Parquet (production-ready format)
valid_orders.write.mode("overwrite").parquet("s3://output-bucket/valid-orders/")
```

### Example 2: Data Quality Check with DataFrame Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Define strict schema for customer data
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),  # Not nullable
    StructField("name", StringType(), False),           # Required
    StructField("email", StringType(), True),           # Nullable
    StructField("signup_date", DateType(), False),      # Required
    StructField("country_code", StringType(), True)     # Optional
])

# Read with strict schema - fails if data doesn't match
customers = spark.read \
    .schema(customer_schema) \
    .option("mode", "FAILFAST") \
    .parquet("s3://data/customers/")

# Validation: Check for null in critical columns
critical_nulls = customers.filter(
    F.col("customer_id").isNull() | F.col("email").isNull()
)

if critical_nulls.count() > 0:
    print(f"CRITICAL: {critical_nulls.count()} customers have null IDs or emails")
    # Take action: write to quarantine, log, alert, etc.

print(f"Data quality check passed. Processed {customers.count()} valid customers.")
customers.show(3)
```

### Example 3: Converting to Pandas (with Performance Warnings)

```python
# SMALL DATASETS ONLY - Use for exploration/reporting
small_df = df.limit(100000)  # Cap at 100k rows first

# Method 1: Convert entire DataFrame to Pandas (memory intensive)
pandas_df = small_df.toPandas()  # Danger: materializes all data on driver
print(f"Converted {len(pandas_df)} rows to Pandas")

# Method 2: Better - use Spark native operations first
# Aggregate or filter before converting to Pandas
aggregated = df.groupBy("category").agg(
    F.count("*").alias("count"),
    F.avg("amount").alias("avg_amount")
)
pandas_agg = aggregated.toPandas()  # Now only a few rows

# Method 3: Use Arrow for better performance (if available)
# toPandas() uses PyArrow if available for faster conversion
pandas_df = df.limit(50000).toPandas()

# After analysis, write results back to Spark
result_df = spark.createDataFrame(pandas_df)
```

### Example 4: Production Schema Registry Pattern

```python
# Define centralized schema definitions (best practice for production)
class DataSchemas:
    """Production-grade schema definitions"""

    @staticmethod
    def orders_schema():
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("amount", DecimalType(12, 2), False),
            StructField("status", StringType(), False),
            StructField("shipped_date", TimestampType(), True)
        ])

    @staticmethod
    def customers_schema():
        return StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("signup_date", DateType(), False),
            StructField("last_order_date", DateType(), True),
            StructField("lifetime_value", DecimalType(12, 2), True)
        ])

# Use in your code
orders_df = spark.read.schema(DataSchemas.orders_schema()).parquet("orders_path")
customers_df = spark.read.schema(DataSchemas.customers_schema()).parquet("customers_path")

print(f"Loaded {orders_df.count()} orders and {customers_df.count()} customers")
```

---

## Best Practices

### 1. Always Use Explicit Schemas in Production

```python
# GOOD - Explicit schema ensures data validation
schema = StructType([...])
df = spark.read.schema(schema).parquet("path")

# AVOID - Schema inference is slow and unreliable at scale
df = spark.read.option("inferSchema", "true").csv("path")  # Scans entire data!
```

**Why**: Schema inference requires a full scan of data to determine types, which is expensive on large datasets. Explicit schemas validate on read and fail fast if data doesn't match expectations.

### 2. Inspect Schema Before Processing

```python
# Always print schema for debugging
df.printSchema()

# Verify column names and types before transformations
print(df.columns)
for col_name, col_type in df.dtypes:
    print(f"{col_name}: {col_type}")
```

### 3. Handle Null Values Explicitly

```python
# Check for nulls
null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Drop rows with critical nulls
df_clean = df.dropna(subset=["customer_id", "order_id"])

# Fill nulls appropriately by column type
df_filled = df.fillna({
    "amount": 0,
    "description": "Unknown",
    "status": "pending"
})
```

### 4. Avoid Unnecessary Conversions to Pandas

```python
# DON'T do this on large DataFrames
large_df.toPandas()  # Brings entire dataset to driver memory

# DO this instead - use Spark operations
result = large_df.groupBy("category").count()  # Aggregates in Spark
result.show()

# Only convert small aggregated results
small_result = large_df.groupBy("category").count().toPandas()
```

### 5. Use Appropriate Serialization Formats

```python
# For repeated reads: use Parquet (columnar, compressed)
df.write.mode("overwrite").parquet("path")

# For one-time reads: CSV is acceptable (but slower)
df.write.mode("overwrite").csv("path")

# For temporary data: use Parquet for performance
df.cache()  # Keep in memory during session
```

---

## Common Pitfalls

### 1. Immutability Mistakes

```python
# WRONG - Forgetting to assign the result
df.filter(F.col("age") > 30)  # This does nothing!
result = df  # Still unfiltered

# CORRECT
df_filtered = df.filter(F.col("age") > 30)
# or chaining
df_result = df \
    .filter(F.col("age") > 30) \
    .select("name", "age")
```

### 2. Schema Inference Performance Issues

```python
# SLOW - Scans entire dataset to infer schema
df = spark.read.option("inferSchema", "true").csv("huge_file.csv")

# FAST - Use explicit schema defined once
schema = StructType([...])
df = spark.read.schema(schema).csv("huge_file.csv")
```

### 3. DataFrame Count Performance

```python
# DON'T - Repeated counts compute from scratch each time
if df.count() > 0:
    print(f"Count: {df.count()}")  # Scanned twice!

# DO - Cache if counting multiple times
df.cache()
count = df.count()
if count > 0:
    print(f"Count: {count}")
```

### 4. Pandas Conversion on Driver

```python
# DANGEROUS - OOM on driver for large dataframes
df_pandas = huge_df.toPandas()  # Loads entire dataset on driver

# SAFE - Limit data before conversion
df_pandas = huge_df.limit(10000).toPandas()
```

### 5. Type Casting Without Error Handling

```python
# RISKY - Silent failures with incorrect data
df = spark.read.schema(schema).csv("file")  # If data type is wrong, nulls inserted

# SAFER - Validate after read
df.filter(F.col("amount").isNotNull()).show()  # Check for unexpected nulls
```

---

## Interview Q&A

### Q1: Why are DataFrames immutable in PySpark? What are the advantages?

**A**: DataFrames are immutable by design for several reasons:

1. **Fault Tolerance**: If a transformation fails, the original data is preserved, making recovery easier.
2. **Parallel Safety**: Multiple threads can safely read the same DataFrame without locking, since no thread can modify it.
3. **Optimization**: The Catalyst optimizer can make aggressive transformations knowing the data won't change externally.
4. **Caching**: Cached DataFrames don't need to be locked or synchronized.
5. **Lineage Tracking**: The system maintains a complete history of transformations (DAG), enabling recovery from failures.

Every transformation returns a new DataFrame, creating a chain of operations that Spark can optimize as a whole.

**Follow-up**: How do you modify a DataFrame?
- You don't. You create a new one: `df_modified = df.withColumn(...).filter(...).select(...)`

---

### Q2: What's the difference between schema inference and explicit schema definition? When should you use each?

**A**:

| Aspect | Schema Inference | Explicit Schema |
|--------|-----------------|-----------------|
| **Performance** | Slow - scans data to determine types | Fast - validates against schema |
| **Reliability** | Can be wrong with mixed types | Guaranteed correct type detection |
| **Use Case** | Interactive exploration, Jupyter notebooks | Production ETL pipelines |
| **Data Size** | OK for <100MB | Required for GB+ datasets |
| **Fail Mode** | Silently converts wrong types | Fails fast if data doesn't match |

**Example Use Cases**:
```python
# Exploration: Schema inference is fine
df = spark.read.option("inferSchema", "true").csv("new_data.csv")
df.printSchema()
df.show(10)

# Production: Always explicit schema
schema = StructType([...])
df = spark.read.schema(schema).csv("production_data.csv")
```

**Follow-up**: What happens if you read data with the wrong schema?
- Data that doesn't match gets converted to null or fails depending on mode (permissive/failfast).

---

### Q3: When should you convert a DataFrame to Pandas, and what precautions should you take?

**A**: Convert to Pandas only when:
1. You need pandas-specific functionality (scikit-learn, visualization, etc.)
2. The DataFrame is small enough to fit in memory (< 1-2GB)
3. You're on the driver node and can handle the memory

**Safety Rules**:
```python
# 1. Always limit first
small_df = large_df.filter(...).limit(50000)

# 2. Check memory available
import psutil
available_gb = psutil.virtual_memory().available / (1024**3)

# 3. Convert with awareness
if df.count() < 100000:  # Safe threshold
    pandas_df = df.toPandas()
else:
    print("DataFrame too large for Pandas conversion")

# 4. Use coalesce to reduce partitions before toPandas
df_coalesced = df.coalesce(1)  # Merge to 1 partition on driver
pandas_df = df_coalesced.toPandas()
```

**Never do this**:
```python
# DANGER: Will OOM
entire_dataset.toPandas()

# DANGER: Blocking operation on driver
very_large_df.collect()  # Same as toPandas() - brings everything to driver
```

---

## See Also

- [01-fundamentals/03-transformations-actions.md](03-transformations-actions.md) - Lazy evaluation and how transformations work
- [01-fundamentals/04-sparksql-basics.md](04-sparksql-basics.md) - Using SQL with DataFrames
- [02-data-operations/01-data-sources.md](../02-data-operations/01-data-sources.md) - Reading from various file formats
- [02-data-operations/02-dataframe-operations.md](../02-data-operations/02-dataframe-operations.md) - Select, filter, withColumn operations
- [04-performance-optimization/01-caching-persistence.md](../../04-performance-optimization/01-caching-persistence.md) - When and how to cache DataFrames
- [09-reference/cheatsheet.md](../../09-reference/cheatsheet.md) - Quick DataFrame operations reference
