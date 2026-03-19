# Data Comparison & Reconciliation Patterns

## Overview

Large-scale data processing requires verification that:
- Output data matches expected results
- No data was lost in transformation
- Schema changes don't break downstream systems
- Data quality meets requirements

This section covers production patterns for comparing and validating large datasets.

---

## CompareDataFrames Framework

The **CompareDataFrames** class compares two DataFrames for equality, handling:
- Column validation
- Row count comparison
- Floating-point precision
- Column dropping
- Detailed difference reporting

```python
from spdek.spark import CompareDataFrames
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataComparison").getOrCreate()

# Load two DataFrames to compare
df_expected = spark.read.parquet("s3://bucket/expected/")
df_actual = spark.read.parquet("s3://bucket/actual/")

# Compare
CompareDataFrames.left(df_expected)
CompareDataFrames.right(df_actual)
CompareDataFrames.compare(name="TestComparison")

if CompareDataFrames.equal:
    print("✓ Data matches!")
else:
    print(f"✗ Data mismatch: {CompareDataFrames.equal_reason}")
```

---

## Comparison Features

### 1. **Column Validation**
```python
# Automatically checks columns match
CompareDataFrames.left(df_expected)
CompareDataFrames.right(df_actual)

if CompareDataFrames.equal_reason == CompareDataFrames.FRAMES_COL_MSG:
    print("Columns don't match between DataFrames")
    print(f"Expected: {[f.name for f in CompareDataFrames.left_cols]}")
    print(f"Actual: {[f.name for f in CompareDataFrames.right_cols]}")
```

### 2. **Row Count Comparison**
```python
CompareDataFrames.left(df_expected)
CompareDataFrames.right(df_actual)

if CompareDataFrames.left_amt != CompareDataFrames.right_amt:
    print(f"Row count mismatch:")
    print(f"  Expected: {CompareDataFrames.left_amt}")
    print(f"  Actual: {CompareDataFrames.right_amt}")
```

### 3. **Floating-Point Precision Handling**
```python
# Round doubles to specified decimal places before comparing
CompareDataFrames.left(
    df_expected,
    double_places_to_round_to=15  # Round to 15 decimals
)
CompareDataFrames.right(
    df_actual,
    double_places_to_round_to=15
)

# Handles floating-point rounding differences
# Without this, 0.1 + 0.2 != 0.3 in floating-point math
```

### 4. **Column Dropping**
```python
# Ignore specific columns during comparison
CompareDataFrames.left(
    df_expected,
    drop_cols=["internal_id", "processing_date", "checksum"]
)
CompareDataFrames.right(
    df_actual,
    drop_cols=["internal_id", "processing_date", "checksum"]
)

# Useful for:
# - Ignoring generated IDs
# - Skipping timestamp columns
# - Excluding internal metadata
```

---

## Real-World Examples

### Example 1: Verify ETL Output

```python
from spdek.spark import CompareDataFrames
import pyspark.sql.functions as F

# After ETL transformation
customers_original = spark.read.parquet("s3://bucket/raw/customers/")
customers_processed = spark.read.parquet("s3://bucket/processed/customers/")

# Remove internal columns that differ
CompareDataFrames.left(
    customers_original.select("id", "name", "email", "signup_date"),
    double_places_to_round_to=15
)

CompareDataFrames.right(
    customers_processed.select("id", "name", "email", "signup_date"),
    double_places_to_round_to=15
)

CompareDataFrames.compare(name="CustomerETL")

if CompareDataFrames.equal:
    print("✓ ETL transformation verified")
else:
    print(f"✗ ETL failed: {CompareDataFrames.equal_reason}")
    # Investigate differences
    if CompareDataFrames.left_diff is not None:
        CompareDataFrames.left_diff.show()
    if CompareDataFrames.right_diff is not None:
        CompareDataFrames.right_diff.show()
```

### Example 2: Schema Migration Testing

```python
# Test that schema migration doesn't lose data
old_schema_data = spark.read.parquet("s3://bucket/v1/data/")
new_schema_data = spark.read.parquet("s3://bucket/v2/data/")

# Compare common columns
common_cols = set(old_schema_data.columns) & set(new_schema_data.columns)

CompareDataFrames.left(old_schema_data.select(*common_cols))
CompareDataFrames.right(new_schema_data.select(*common_cols))
CompareDataFrames.compare(name="SchemaUpgrade")

if not CompareDataFrames.equal:
    print("❌ Schema migration lost data!")
else:
    print("✓ Schema migration successful")
```

### Example 3: Incremental Load Verification

```python
# Verify incremental load against full load
full_load = spark.read.parquet("s3://bucket/full_refresh/2024-03-16/")
incremental_load = spark.read.parquet("s3://bucket/incremental/2024-03-16/")

# Union all incremental loads from start
from functools import reduce

incremental_union = reduce(
    lambda df1, df2: df1.union(df2),
    [spark.read.parquet(f"s3://bucket/incremental/{dt}/")
     for dt in date_range]
)

# Compare final result
CompareDataFrames.left(full_load.select("id", "amount", "date"))
CompareDataFrames.right(incremental_union.select("id", "amount", "date"))
CompareDataFrames.compare(name="IncrementalLoadVerification")

if CompareDataFrames.equal:
    print("✓ Incremental loads match full refresh")
else:
    print("✗ Incremental loads differ from full refresh")
```

---

## Additional Comparison Utilities

### Repartition for Even Writing

Control output file sizes:

```python
from spdek.spark import repartition_frame_for_even_write

df = spark.read.parquet("input/")

# Repartition to get ~1M rows per file
df_balanced = repartition_frame_for_even_write(df, rows_per_file=1000000)

# Results in 10 files for 10M row DataFrame
df_balanced.write.parquet("output/")
```

**Why useful:**
- Avoids small files problem (many tiny files slow down subsequent reads)
- Avoids huge files (slow to read, memory pressure)
- Optimal file size ~128MB (depends on data density)

### Salting for Skew

Handle skewed keys in joins:

```python
from spdek.spark import salt_frame

df_skewed = spark.read.parquet("data/")

# Add random salt column to break up skewed partitions
df_salted = salt_frame(df_skewed, salt_bins=10)

# Now do join with salt
result = df_salted.join(
    other_df.withColumn("_salt", F.explode(F.array([F.lit(i) for i in range(10)]))),
    ["join_key", "_salt"],
    "inner"
).drop("_salt")
```

---

## Complete Data Validation Framework

### Step 1: Load & Transform
```python
df_raw = spark.read.parquet("input/")
df_transformed = df_raw \
    .filter(df_raw.value > 0) \
    .withColumn("normalized_value", df_raw.value / df_raw.max_value) \
    .dropDuplicates(["id"])
```

### Step 2: Validate Schema
```python
expected_schema = StructType([
    StructField("id", LongType(), False),
    StructField("normalized_value", DoubleType(), True),
    # ...
])

if df_transformed.schema != expected_schema:
    raise Exception("Schema mismatch!")
```

### Step 3: Validate Row Counts
```python
input_count = df_raw.count()
output_count = df_transformed.count()

if output_count > input_count:
    raise Exception("More rows in output than input!")

if output_count < input_count * 0.9:
    print(f"⚠️ Warning: Lost 10% of data ({output_count} vs {input_count})")
```

### Step 4: Sample Comparison
```python
# For huge datasets, sample and compare instead of comparing entire dataset
sample_expected = spark.read.parquet("expected_sample/")
sample_actual = df_transformed.sample(fraction=0.001, seed=42)

CompareDataFrames.left(sample_expected)
CompareDataFrames.right(sample_actual.select(*sample_expected.columns))
CompareDataFrames.compare(name="SampleValidation")

if not CompareDataFrames.equal:
    print("❌ Sample comparison failed!")
```

### Step 5: Statistical Validation
```python
import pyspark.sql.functions as F

stats = df_transformed.select(
    F.count("*").alias("row_count"),
    F.countDistinct("id").alias("unique_ids"),
    F.avg("normalized_value").alias("avg_value"),
    F.min("normalized_value").alias("min_value"),
    F.max("normalized_value").alias("max_value")
)

stats_dict = stats.toPandas().to_dict(orient='records')[0]
print(f"Row count: {stats_dict['row_count']}")
print(f"Unique IDs: {stats_dict['unique_ids']}")

# Validate ranges
if stats_dict['min_value'] < 0 or stats_dict['max_value'] > 1:
    raise Exception("Normalized values out of range!")
```

---

## Best Practices

### 1. **Always Validate After Transform**
```python
# Bad: Trust the transformation
result = df.join(other_df, "key").groupBy("category").agg(...)
result.write.parquet("output/")

# Good: Validate first
result = df.join(other_df, "key").groupBy("category").agg(...)
assert result.count() > 0, "No rows in result!"
assert len(result.columns) == expected_col_count, "Wrong number of columns!"
result.write.parquet("output/")
```

### 2. **Use Checksums for Large Datasets**
```python
# For huge datasets, comparing entire dataset is slow
# Use checksums/hashes instead

df1_hash = df1.select(F.md5(F.concat(*df1.columns)).alias("hash")) \
    .groupBy().agg(F.max("hash").alias("dataset_hash")) \
    .collect()[0][0]

df2_hash = df2.select(F.md5(F.concat(*df2.columns)).alias("hash")) \
    .groupBy().agg(F.max("hash").alias("dataset_hash")) \
    .collect()[0][0]

if df1_hash == df2_hash:
    print("✓ Datasets are identical")
else:
    print("✗ Datasets differ")
```

### 3. **Document Expected Behavior**
```python
class DataValidation:
    """Validates customer data transformations"""

    EXPECTED_COLUMNS = ["id", "name", "email", "created_date"]
    MIN_ROWS = 1000  # Should have at least 1000 customers
    MAX_NULL_PCT = 0.05  # Allow 5% nulls max

    @staticmethod
    def validate(df):
        # Check columns
        assert set(df.columns) == set(EXPECTED_COLUMNS), "Columns mismatch"

        # Check row count
        row_count = df.count()
        assert row_count >= MIN_ROWS, f"Too few rows: {row_count}"

        # Check nulls
        null_count = df.where(F.col("email").isNull()).count()
        null_pct = null_count / row_count
        assert null_pct <= MAX_NULL_PCT, f"Too many nulls: {null_pct}"
```

---

## See Also
- [Incremental Processing](03-incremental-processing.md)
- [Error Handling](04-error-handling.md)
- [Data Skew](../03-joins-partitioning/05-data-skew.md)
