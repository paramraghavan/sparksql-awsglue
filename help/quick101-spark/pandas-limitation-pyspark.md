# Pandas vs Spark Dataframe

Certainly! Pandas DataFrames and Spark DataFrames have some key differences in terms of their design, functionality, and
use cases. Let's explore these differences with examples.

```python
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Create a Pandas DataFrame
pandas_df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': [25, 30, 35, 40],
    'salary': [50000, 60000, 75000, 90000]
})

# Create a Spark Session
spark = SparkSession.builder.appName("PandasVsSpark").getOrCreate()

# Create a Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# 1. Basic operations
print("Pandas: First 2 rows")
print(pandas_df.head(2))

print("\nSpark: First 2 rows")
spark_df.show(2)

# 2. Filtering
print("Pandas: Filter age > 30")
print(pandas_df[pandas_df['age'] > 30])

print("\nSpark: Filter age > 30")
spark_df.filter(col('age') > 30).show()

# 3. Aggregation
print("Pandas: Average salary")
print(pandas_df['salary'].mean())

print("\nSpark: Average salary")
spark_df.select(spark_sum('salary') / spark_df.count()).show()

# 4. Adding a new column
pandas_df['bonus'] = pandas_df['salary'] * 0.1
spark_df = spark_df.withColumn('bonus', col('salary') * 0.1)

print("Pandas: With bonus column")
print(pandas_df)

print("\nSpark: With bonus column")
spark_df.show()

# Clean up
spark.stop()

```

## The key differences between Pandas and Spark DataFrames:

1. Data Processing Model:
    - Pandas: In-memory, single-node processing. Suitable for smaller datasets that fit in memory.
    - Spark: Distributed processing across multiple nodes. Designed for big data that doesn't fit in a single machine's
      memory.

2. Performance:
    - Pandas: Faster for smaller datasets due to in-memory processing.
    - Spark: More efficient for large-scale data processing, leveraging distributed computing.

3. API and Syntax:
    - Pandas: More intuitive for users familiar with Python and NumPy.
    - Spark: SQL-like API, often requiring more verbose syntax.

4. Lazy Evaluation:
    - Pandas: Executes operations immediately.
    - Spark: Uses lazy evaluation, only executing when an action is called (e.g., `show()`, `collect()`).

5. Data Types:
    - Pandas: Rich set of data types, including Python and NumPy types.
    - Spark: More limited set of data types, focused on distributed computing needs.

6. Memory Management:
    - Pandas: Loads entire dataset into memory.
    - Spark: Can work with datasets larger than available memory through partitioning.

7. Ecosystem:
    - Pandas: Tightly integrated with Python scientific computing stack (NumPy, Matplotlib, etc.).
    - Spark: Part of the larger Apache Spark ecosystem, including machine learning (MLlib) and graph processing.

8. Use Cases:
    - Pandas: Data analysis, small to medium-sized datasets, ad-hoc analysis.
    - Spark: Big data processing, ETL jobs, large-scale machine learning.

# Pandas Limitations and how to overcome with spark
When working with PySpark, there are several limitations of Pandas DataFrames,
especially when dealing with large-scale data processing. Here are the limitations:

1. Memory Constraints:

Limitation: Pandas DataFrames are in-memory structures, limited by the RAM of a single machine.

How to overcome:

- Use Spark DataFrames instead, which can handle data larger than available memory.
- Convert Pandas DataFrames to Spark DataFrames for processing large datasets:

```python
"""
Following Still requires the entire dataset to be in memory as a Pandas DataFrame before converting it to a Spark 
DataFrame. This approach doesn't solve the fundamental memory limitation problem when dealing with large datasets.
@see section 9, Using Pandas on Spark.
"""
spark_df = spark.createDataFrame(pandas_df)
```

2. Lack of Distributed Processing:

Limitation: Pandas operates on a single machine, not utilizing distributed computing capabilities.

How to overcome:

- Use Spark's distributed processing capabilities by working with Spark DataFrames.
- Parallelize operations using Spark:

```python
distributed_df = spark.parallelize(pandas_df.to_numpy()).toDF(pandas_df.columns)
```

3. Performance with Large Datasets:

Limitation: Pandas performance degrades with very large datasets.

How to overcome:

- Process data in Spark, then collect results back to Pandas if needed:

```python
result_pandas_df = spark_df.filter(spark_df.column > value).toPandas()
```

4. Limited Scalability:

Limitation: Pandas doesn't scale well for operations on large datasets.

How to overcome:

- Use Spark's scalable operations and only convert to Pandas for final analysis or visualization:

```python
spark_result = spark_df.groupBy("column").agg({"value": "mean"})
pandas_result = spark_result.toPandas()
```

5. Lack of Lazy Evaluation:

Limitation: Pandas executes operations immediately, which can be inefficient for complex pipelines.

How to overcome:

- Leverage Spark's lazy evaluation for complex data pipelines:

```python
transformed_df = spark_df.select("col1", "col2").filter(spark_df.col3 > 100)
# No computation happens until an action is called
result = transformed_df.collect()
```

6. Different APIs:

Limitation: Pandas and PySpark have different APIs, which can lead to code inconsistencies.

How to overcome:

- Use PySpark's Pandas API on Spark:

```python
import pyspark.pandas as ps

ps_df = ps.DataFrame(spark_df)
result = ps_df.groupby("column").mean()
```

7. UDFs (User Defined Functions) Performance:

Limitation: Applying Pandas UDFs to Spark DataFrames can be slow due to serialization overhead.

How to overcome:

- Use Spark SQL functions when possible.
- For custom logic, use Pandas UDFs with Arrow optimization:

```python
from pyspark.sql.functions import pandas_udf


@pandas_udf("double")
def custom_function(v: pd.Series) -> pd.Series:
    return v * 2


result = spark_df.select(custom_function("column").alias("doubled"))
```

8. Join Operations:

Limitation: Joining large Pandas DataFrames can be memory-intensive and slow.

How to overcome:

- Perform joins in Spark, which can handle large-scale joins more efficiently:

```python
joined_df = spark_df1.join(spark_df2, on="key_column")
```

9. Using Pandas on Spark:

For a Pandas-like API with Spark's distributed computing capabilities, you can use Pandas on Spark.
This allows you to work with large datasets without loading them entirely into memory first. They leverage Spark's
distributed computing capabilities to process data efficiently, even when it exceeds the memory capacity of a single
machine.

```python
import pyspark.pandas as ps

# Create a Pandas on Spark DataFrame
psdf = ps.read_csv("path/to/large_file.csv")

# Perform operations using Pandas-like syntax
result = psdf.groupby("column").mean()

# Convert back to Spark DataFrame if needed
spark_df = result.to_spark()
```

The key is to use Spark for heavy lifting and distributed processing, while using Pandas for smaller-scale analysis and visualization tasks.

Would you like me to elaborate on any of these points or provide more detailed examples?