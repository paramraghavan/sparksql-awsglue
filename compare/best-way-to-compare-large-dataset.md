# Best Ways to Read and Compare Large Data Files in Spark and Pandas

When dealing with large data files (parquet or CSV), choosing the right approach is crucial for both performance and
memory efficiency.

## File Format Considerations

**Parquet vs. CSV for Big Data:**

| Format      | Advantages                                                                                                                         | Disadvantages                                                                                      |
|-------------|------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| **Parquet** | - Column-oriented storage<br>- Built-in compression<br>- Schema preservation<br>- Up to 4x faster than CSV<br>- Smaller file sizes | - Not human-readable<br>- Requires specialized tools to view                                       |
| **CSV**     | - Human-readable<br>- Universal compatibility<br>- Easy to create/modify                                                           | - Slower for big data<br>- Larger file sizes<br>- No schema enforcement<br>- No native compression |

**For big data, Parquet is strongly recommended** over CSV due to significantly better performance and resource
utilization.

## Reading Large Files

### With Apache Spark

```python
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder
    .appName("Large File Processing")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

# Read Parquet (preferred for big data)
df1 = spark.read.parquet("path/to/file1.parquet")

# Read CSV (with optimizations)
df2 = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .option("compression", "gzip") \  # If compressed
.load("path/to/file2.csv")
```

**Spark Best Practices for Big Files:**

- Use partitioned reads: `spark.read.parquet("data_dir/year=*/month=*/")`
- Adjust partition size: `.config("spark.sql.files.maxPartitionBytes", "128m")`
- Enable predicate pushdown: `df.filter("column > value").select("col1", "col2")`
- Control parallelism: `.config("spark.default.parallelism", "100")`

### With Pandas (and helpers)

For truly big data, pure pandas will struggle. Use these approaches:

```python
# 1. Use pandas with chunking for CSV
import pandas as pd

chunk_size = 1000000  # Adjust based on your memory
chunks = []
for chunk in pd.read_csv("huge_file.csv", chunksize=chunk_size):
    # Process each chunk
    chunks.append(chunk.summary_calculation())  # Replace with your processing
result = pd.concat(chunks)

# 2. Use pyarrow (much faster for parquet)
import pyarrow.parquet as pq

table = pq.read_table("large_file.parquet")
df = table.to_pandas()

# 3. Use Dask for out-of-memory processing
import dask.dataframe as dd

ddf = dd.read_parquet("huge_file.parquet")  # Lazy loading
```

## Comparing Large Datasets

### Using Spark for Comparison

```python
# 1. Basic comparison of counts and schema
print("Dataset 1 count:", df1.count())
print("Dataset 2 count:", df2.count())
df1.printSchema()
df2.printSchema()

# 2. Compare column distributions
from pyspark.sql import functions as F

for col in df1.columns:
    if col in df2.columns:
        print(f"Column: {col}")
        df1.select(F.min(col), F.max(col), F.avg(col), F.count(col)).show()
        df2.select(F.min(col), F.max(col), F.avg(col), F.count(col)).show()

# 3. Find differences between datasets
# Create temporary views
df1.createOrReplaceTempView("dataset1")
df2.createOrReplaceTempView("dataset2")

# Records in df1 but not in df2 (using key columns)
diff_query = """
SELECT * FROM dataset1 
WHERE key_column NOT IN (SELECT key_column FROM dataset2)
"""
differences = spark.sql(diff_query)
differences.show()

# 4. Join and compare all columns
from pyspark.sql.functions import col

join_cols = ["id"]  # Key columns to join on

# Full outer join
comparison = df1.join(
    df2,
    on=join_cols,
    how="full_outer"
).select(
    *[col(f"df1.{c}").alias(f"{c}_1") for c in df1.columns if c not in join_cols],
    *[col(f"df2.{c}").alias(f"{c}_2") for c in df2.columns if c not in join_cols],
    *join_cols
)

# Find rows with differences
mismatch_conditions = [col(f"{c}_1") != col(f"{c}_2") for c in df1.columns
                       if c not in join_cols and c in df2.columns]
mismatches = comparison.filter(reduce(lambda a, b: a | b, mismatch_conditions))
mismatches.show()
```

### Practical Comparison Strategy

For extremely large datasets, a phased approach works best:

1. **Quick summary comparison**
    - Compare row counts, column counts, and schema
    - Calculate and compare basic statistics (min, max, mean, null counts)

2. **Column-level comparisons**
    - Compare the distribution of values in important columns
    - Check for null/empty value distributions

3. **Sampling-based deep comparison**
    - Use `.sample(0.01)` to create smaller representative datasets
    - Perform detailed comparisons on samples

4. **Hash-based comparisons**
    - Create a hash of rows or blocks for quick comparison
   ```python
   from pyspark.sql.functions import sha2, concat_ws
   df1_hash = df1.withColumn("row_hash", sha2(concat_ws("", *df1.columns), 256))
   df2_hash = df2.withColumn("row_hash", sha2(concat_ws("", *df2.columns), 256))
   ```

## Optimizing Memory Usage

- **Spark**: Control memory with `.config("spark.memory.fraction", "0.8")`
- **Pandas**: Use appropriate dtypes:
  ```python
  df = pd.read_csv("large.csv", dtype={
      'id': 'int32',       # Instead of int64
      'category': 'category',  # For text with few unique values
      'text': 'string'     # Instead of object
  })
  ```
