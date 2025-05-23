There are several issues causing your performance problems. Let me address them:

## Issues with Your Current Setup

### 1. Spark-Submit Command Problems

Your command has syntax errors:

```bash
# Your command (with errors):
spark-submit --deploy-mode client --master yarn ==driver-memeory 32g --executorc-cores 4 --num-executors 72 --conf.spark.dynamicAllocatiob.enabled=false ./myjob.py

# Corrected command:
spark-submit \
  --deploy-mode client \
  --master yarn \
  --driver-memory 32g \
  --executor-cores 4 \
  --num-executors 72 \
  --executor-memory 16g \
  --conf spark.dynamicAllocation.enabled=false \
  ./myjob.py
```

### 2. Why Only 3-4 Nodes Are Used

**Root Cause**: Your **loop with pandas conversion** is forcing everything to run on the driver node sequentially.

```python
# This is the problem:
dfm = df_sample(['AssignedModel']).distinct().toPandas()  # Brings data to driver
for n in dfm.AssignedModel.values:  # Sequential loop on driver
# 70 lines of logic per iteration
```

This pattern:

- Collects all data to the driver (single node)
- Processes sequentially, not in parallel
- Doesn't utilize your 72 executors across 122 nodes

## Solution: Convert to Distributed Processing

### Option 1: Use Spark DataFrame Operations

```python
# Instead of pandas loop, use Spark operations
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Get distinct values (stay in Spark)
distinct_models = df_sample.select('AssignedModel').distinct()

# Use Spark operations instead of pandas loop
# Example transformations:
result = distinct_models.withColumn(
    "processed_data",
    # Your 70 lines of logic converted to Spark functions
    F.when(F.col('AssignedModel') > 15, your_logic())
    .otherwise(other_logic())
)

# Or use join instead of loop
processed_df = df_sample.join(
    distinct_models.withColumn("computed_value", your_spark_logic()),
    on="AssignedModel"
)
```

### Option 2: Use `foreachPartition` for Complex Logic

```python
def process_partition(iterator):
    """Process each partition independently"""
    import pandas as pd

    # Convert partition to pandas for complex operations
    partition_df = pd.DataFrame(iterator)

    results = []
    for row in partition_df.itertuples():
        # Your 70 lines of logic here
        result = complex_processing(row)
        results.append(result)

    return iter(results)


# Apply to each partition in parallel
result_rdd = df_sample.rdd.mapPartitions(process_partition)
result_df = spark.createDataFrame(result_rdd, schema)
```

### Option 3: Use `groupBy` + UDF for Model-specific Processing

```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *


@udf(returnType=YourResultType())
def process_model_data(assigned_model, other_columns):
    """Your 70 lines of logic as a UDF"""
    if assigned_model > 15:
        # Your processing logic
        return processed_result
    return default_result


# Apply UDF in parallel across cluster
result_df = df_sample.withColumn(
    "processed_result",
    process_model_data(col("AssignedModel"), col("other_cols"))
)
```

## Performance Optimizations

### 1. Increase Parallelism

```bash
spark-submit \
  --deploy-mode cluster \  # Use cluster mode
  --master yarn \
  --driver-memory 16g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 72 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  ./myjob.py
```

### 2. Optimize Data Partitioning

```python
# Repartition based on your processing logic
df_sample = df_sample.repartition("AssignedModel")

# Or increase partitions for more parallelism
df_sample = df_sample.repartition(200)  # More partitions = more parallel tasks
```

### 3. Cache Intermediate Results

```python
# Cache frequently accessed DataFrames
df_sample.cache()
distinct_models = df_sample.select('AssignedModel').distinct().cache()
```

## Key Takeaway

**The main issue**: Your `toPandas()` + loop pattern forces single-threaded processing on the driver. Converting to
distributed Spark operations will utilize all your cluster nodes and dramatically improve performance.

