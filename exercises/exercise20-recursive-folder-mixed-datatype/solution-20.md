There are several ways to handle the data type mismatches:

## Solution 1: Cast All Columns to String (Simplest)

```python
# Read with string schema enforcement
df = spark.read
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")
    .parquet("model_out/")
    .select([col(c).cast("string").alias(c) for c in df.columns])
```

However, since you need the schema before reading, use this approach:

```python
from pyspark.sql.functions import col

# First, get a sample to determine column names
sample_df = spark.read.option("recursiveFileLookup", "true").parquet("model_out/iter_2/")  # Use a non-empty folder
column_names = sample_df.columns

# Now read all data and cast to string
df = spark.read
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")
    .parquet("model_out/")
    .select([col(c).cast("string").alias(c) for c in column_names])
```

## Solution 2: Define a Common Schema

```python
from pyspark.sql.types import StructType, StructField, StringType

# Define your schema with all columns as strings
# Replace with your actual column names
common_schema = StructType([
    StructField("column1", StringType(), True),
    StructField("column2", StringType(), True),
    StructField("column3", StringType(), True),
    # ... add all your columns
])

df = spark.read
    .option("recursiveFileLookup", "true")
    .schema(common_schema)
    .parquet("model_out/")
```


## Solution 3: Union DataFrames with Schema Alignment

```python
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType


def read_and_standardize(path, target_columns):
    """Read parquet and standardize all columns to string type"""
    df = spark.read.parquet(path)

    # Cast existing columns to string
    select_exprs = []
    for col_name in target_columns:
        if col_name in df.columns:
            select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias(col_name))

    return df.select(select_exprs)


# Get column names from a non-empty folder
sample_df = spark.read.parquet("model_out/iter_2/")
target_columns = sample_df.columns

# Read all folders and union them
all_dfs = []
for i in range(1, 16):  # iter_1 to iter_15
    folder_path = f"model_out/iter_{i}/"
    try:
        df_iter = read_and_standardize(folder_path, target_columns)
        all_dfs.append(df_iter)
    except:
        print(f"Skipping {folder_path} - might be empty or not exist")

# Union all DataFrames
final_df = all_dfs[0]
for df in all_dfs[1:]:
    final_df = final_df.union(df)
```

## Solution 4: Most Robust Approach

```python
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Read with mergeSchema first to get all possible columns
df = spark.read
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")
    .parquet("model_out/")

# Convert all columns to string to avoid type conflicts
string_df = df.select([col(c).cast(StringType()).alias(c) for c in df.columns])
```

## Recommendations:

1 **For immediate solution**: Use Solution 4 - it's the most straightforward
2 **For production**: Use Solution 2 with a predefined schema

The `mergeSchema` option is still useful even with empty datasets because it helps Spark understand the complete schema
across all files. Empty parquet files still contain schema information, so they won't cause issues - the problem is
specifically with data type mismatches between different iterations.
