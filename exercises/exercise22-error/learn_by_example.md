Here's sample code to recreate common parquet writing issues:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random

spark = SparkSession.builder.appName("ParquetIssues").getOrCreate()

# ============================================
# 1. NULL VALUE ROWS - Can cause issues with certain parquet readers
# ============================================
print("=== Issue 1: Null Value Rows ===")

# Create dataframe with all-null rows
data_with_nulls = [
    (1, "Alice", 100),
    (None, None, None),  # Entire row is null
    (2, "Bob", 200),
    (None, None, None),
    (3, None, 300)  # Partial nulls
]

df_nulls = spark.createDataFrame(data_with_nulls, ["id", "name", "amount"])
df_nulls.show()

# This might fail or create corrupted files
try:
    df_nulls.write.mode("overwrite").parquet("s3://bucket/null_rows/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Remove all-null rows or fill nulls
df_fixed = df_nulls.na.drop(how="all")  # Drop rows where ALL values are null
# OR
df_fixed = df_nulls.na.fill({"id": 0, "name": "unknown", "amount": 0})

# ============================================
# 2. NULL COLUMNS - Columns with all null values
# ============================================
print("\n=== Issue 2: Null Columns ===")

# Create dataframe where entire column is null
data_null_col = [
    (1, "Alice", None),
    (2, "Bob", None),
    (3, "Charlie", None)
]

df_null_col = spark.createDataFrame(data_null_col, ["id", "name", "missing_col"])
df_null_col.show()

# Parquet may struggle to infer schema for all-null columns
try:
    df_null_col.write.mode("overwrite").parquet("s3://bucket/null_columns/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Drop null columns or specify explicit schema
df_fixed = df_null_col.drop("missing_col")
# OR define explicit schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("missing_col", StringType(), True)  # Explicit type
])

# ============================================
# 3. DATA SKEW - Uneven partition distribution
# ============================================
print("\n=== Issue 3: Data Skew ===")

# Create highly skewed data (99% in one partition)
skewed_data = [(1, "GroupA", i) for i in range(990)]  # 990 records
skewed_data += [(2, "GroupB", i) for i in range(10)]  # 10 records

df_skewed = spark.createDataFrame(skewed_data, ["group_id", "group_name", "value"])

# Writing with partitionBy on skewed column creates massive imbalance
print("Partition distribution:")
df_skewed.groupBy("group_id").count().show()

# This causes one executor to do 99% of work -> timeout/OOM
try:
    df_skewed.write.partitionBy("group_id").mode("overwrite").parquet("s3://bucket/skewed/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Repartition before writing
df_fixed = df_skewed.repartition(10)  # Even distribution
# OR use salting for skewed keys
df_fixed = df_skewed.withColumn("salt", (rand() * 10).cast("int"))
df_fixed.write.partitionBy("group_id", "salt").parquet("s3://bucket/fixed_skew/")

# ============================================
# 4. EXTREMELY LARGE PARTITIONS
# ============================================
print("\n=== Issue 4: Large Partitions ===")

# Create dataframe with too few partitions for large data
large_data = [(i, f"User{i}", i * 100) for i in range(1000000)]
df_large = spark.createDataFrame(large_data, ["id", "name", "amount"])

print(f"Number of partitions: {df_large.rdd.getNumPartitions()}")
print(f"Rows per partition: {df_large.count() / df_large.rdd.getNumPartitions()}")

# Default partitions might be too large -> memory issues
try:
    df_large.write.mode("overwrite").parquet("s3://bucket/large_partitions/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Increase partitions
df_fixed = df_large.repartition(200)  # Aim for 100-200MB per partition
# OR limit records per file
df_large.write.option("maxRecordsPerFile", 100000).parquet("s3://bucket/fixed_large/")

# ============================================
# 5. COMPLEX NESTED STRUCTURES
# ============================================
print("\n=== Issue 5: Complex Nested Types ===")

# Create deeply nested structures
complex_data = [
    (1, {"nested": {"deep": {"value": [1, 2, None, 4]}}}),
    (2, {"nested": {"deep": {"value": [5, None, 7]}}}),
    (3, {"nested": None})  # Null in nested structure
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("data", MapType(StringType(),
                                MapType(StringType(),
                                        MapType(StringType(), ArrayType(IntegerType())))), True)
])

df_complex = spark.createDataFrame(complex_data, schema)
df_complex.show(truncate=False)

# Complex nulls in nested structures can cause issues
try:
    df_complex.write.mode("overwrite").parquet("s3://bucket/complex_nested/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Flatten the structure
df_fixed = df_complex.selectExpr("id", "data.nested.deep.value as values")

# ============================================
# 6. INVALID COLUMN NAMES
# ============================================
print("\n=== Issue 6: Invalid Column Names ===")

# Column names with special characters
df_invalid_cols = spark.createDataFrame(
    [(1, "A", 100)],
    ["id", "name with spaces", "amount,comma"]
)

# Parquet doesn't like spaces, commas, etc. in column names
try:
    df_invalid_cols.write.mode("overwrite").parquet("s3://bucket/invalid_cols/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Sanitize column names
df_fixed = df_invalid_cols.toDF(*[c.replace(" ", "_").replace(",", "_")
                                  for c in df_invalid_cols.columns])

# ============================================
# 7. SCHEMA INCONSISTENCIES (Dynamic Partitions)
# ============================================
print("\n=== Issue 7: Schema Inconsistencies ===")

# Different schema in different partitions
df1 = spark.createDataFrame([(1, "A", 100)], ["id", "name", "amount"])
df2 = spark.createDataFrame([(2, "B", "200")], ["id", "name", "amount"])  # amount is string!

df_inconsistent = df1.union(df2)
df_inconsistent.printSchema()

# Mixed types in same column cause issues
try:
    df_inconsistent.write.mode("overwrite").parquet("s3://bucket/inconsistent/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Cast to consistent types
df2_fixed = df2.withColumn("amount", col("amount").cast("int"))
df_fixed = df1.union(df2_fixed)

# ============================================
# 8. ZERO-BYTE PARTITIONS
# ============================================
print("\n=== Issue 8: Empty Partitions ===")

# Create dataframe that results in empty partitions after filtering
df_filtered = spark.range(1000).filter(col("id") > 10000)  # Empty result

# Writing empty dataframes can cause issues
try:
    df_filtered.write.mode("overwrite").parquet("s3://bucket/empty/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Check before writing
if df_filtered.count() > 0:
    df_filtered.write.mode("overwrite").parquet("s3://bucket/empty/")
else:
    print("Skipping write - no data")

# ============================================
# 9. TOO MANY SMALL FILES
# ============================================
print("\n=== Issue 9: Too Many Small Files ===")

# High number of partitions for small dataset
df_small = spark.range(100).repartition(1000)  # 1000 partitions for 100 rows

print(f"Files to create: {df_small.rdd.getNumPartitions()}")

# Creates 1000 tiny files -> S3 API throttling
try:
    df_small.write.mode("overwrite").parquet("s3://bucket/many_small_files/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Coalesce to fewer partitions
df_fixed = df_small.coalesce(5)  # Combine to 5 files

# ============================================
# 10. ENCODING ISSUES
# ============================================
print("\n=== Issue 10: Character Encoding Issues ===")

# Special characters and emojis
special_data = [
    (1, "Hello 世界"),
    (2, "Emoji 😀🎉"),
    (3, "Special \x00\x01\x02")  # Control characters
]

df_encoding = spark.createDataFrame(special_data, ["id", "text"])

# Control characters can cause issues
try:
    df_encoding.write.mode("overwrite").parquet("s3://bucket/encoding/")
except Exception as e:
    print(f"Error: {e}")

# FIX: Clean special characters
df_fixed = df_encoding.withColumn(
    "text",
    regexp_replace(col("text"), "[\x00-\x1F]", "")
)


# ============================================
# DIAGNOSTIC FUNCTION
# ============================================
def diagnose_dataframe(df, name="dataframe"):
    """Run diagnostics before writing parquet"""
    print(f"\n=== Diagnostics for {name} ===")

    # Check row count
    row_count = df.count()
    print(f"Total rows: {row_count}")

    # Check partitions
    num_partitions = df.rdd.getNumPartitions()
    print(f"Number of partitions: {num_partitions}")
    print(f"Avg rows per partition: {row_count / num_partitions if num_partitions > 0 else 0}")

    # Check for null columns
    print("\nNull counts per column:")
    df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

    # Check schema
    print("\nSchema:")
    df.printSchema()

    # Check for all-null rows
    all_null_rows = df.filter(
        " AND ".join([f"{c} IS NULL" for c in df.columns])
    ).count()
    print(f"\nAll-null rows: {all_null_rows}")

    # Check partition distribution (if small enough)
    if row_count < 10000:
        print("\nPartition distribution:")
        df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show(10)


# Example usage
diagnose_dataframe(df_skewed, "skewed_data")
```

## Quick Reference: Common Issues & Fixes

| Issue                    | Symptom                | Fix                                          |
|--------------------------|------------------------|----------------------------------------------|
| **Null rows**            | TASK_WRITE_FAILED      | `df.na.drop(how="all")` or `df.na.fill()`    |
| **Null columns**         | Schema inference error | Drop column or explicit schema               |
| **Data skew**            | Task timeout, OOM      | `df.repartition(N)` or add salt column       |
| **Large partitions**     | Memory errors          | `df.repartition(200)` or `maxRecordsPerFile` |
| **Invalid column names** | Write failure          | Sanitize: `replace(" ", "_")`                |
| **Too many files**       | S3 throttling          | `df.coalesce(N)`                             |
| **Empty partitions**     | Unexpected errors      | Check `df.count() > 0` before writing        |

Run the `diagnose_dataframe()` function on your complex dataframe before writing to identify issues!