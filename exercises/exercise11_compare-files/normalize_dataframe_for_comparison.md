Support comparison across heterogeneous data types including JSON and XML-like strings in Parquet files on
Spark.

### 1. Detect Column Data Types Dynamically

You can inspect the schema of a loaded Parquet DataFrame to get column names and data types:

```python
df = spark.read.parquet("s3://bucket/path/file.parquet")
schema = df.schema

# Extract column names and types as dict
col_types = {field.name: field.dataType for field in schema.fields}
```

***

### 2. Convert Complex Types to String for Comparison

- For `StringType`, `IntegerType`, `FloatType`, etc., you can compare directly or cast as needed.
- For complex types (e.g., JSON stored as string, MapType, StructType), you should convert them to a normalized string
  representation before comparison.
- For XML stored as string, treat it similarly by normalizing or simply casting to string.
- Use Spark functions like `from_json()` for JSON parsing and then back to string if normalization needed, or
  `to_json()` to convert structs/maps to string.

***

### 3. Example Code Snippet for Normalizing Columns

```python
from pyspark.sql.types import StructType, MapType, ArrayType, StringType
from pyspark.sql.functions import col, to_json


def normalize_column(df, column, dtype):
    """Convert complex types to string for robust comparison."""
    if isinstance(dtype, (StructType, MapType, ArrayType)):
        # Convert complex type to JSON string for comparison
        return to_json(col(column)).alias(column)
    elif dtype == StringType():
        # Assuming JSON strings, try parsing and re-serializing for normalization
        # (optional - apply only if desired)
        # from pyspark.sql.functions import from_json
        # return to_json(from_json(col(column), schema_of_json)).alias(column)
        return col(column)
    else:
        # For primitive types, cast to string for uniformity in comparison
        return col(column).cast("string").alias(column)


# Load Parquet files
df1 = spark.read.parquet("s3://bucket/path/file1.parquet")
df2 = spark.read.parquet("s3://bucket/path/file2.parquet")

schema1 = df1.schema
schema2 = df2.schema

# Intersect columns for comparison
common_cols = list(set(df1.columns).intersection(df2.columns))

# Normalize columns in df1
normalized_df1 = df1.select(
    [normalize_column(df1, c, schema1[c].dataType) for c in common_cols]
)

# Normalize columns in df2
normalized_df2 = df2.select(
    [normalize_column(df2, c, schema2[c].dataType) for c in common_cols]
)

# Proceed with comparison logic on normalized_df1 and normalized_df2...
```

***

### Summary

- Read schema dynamically to get data types.
- Convert complex columns (structs, maps, arrays) to JSON strings before comparison.
- Optionally parse and normalize JSON strings if stored as plain strings.
- Cast all compared columns consistently (e.g., to string) to avoid type mismatches.
- Then implement your row-wise or key-wise comparison on the normalized columns.


