```text
If all the threads use the same dataframe, is there a way to clone or some other way for these thread to use the
dataframe as if they are single dataframe and what happens when it is a large dataframe
```

## What Actually Happens with Shared DataFrames

### DataFrame References vs Data

```python
# Important: DataFrames are just "execution plans", not the actual data
large_df = spark.read.parquet("/huge/dataset/")  # This is just metadata!

# When multiple threads use the same DataFrame:
thread1_result = large_df.filter(col("A") > 10).collect()  # Executes plan
thread2_result = large_df.filter(col("B") < 5).collect()  # Executes plan separately
```

**Key insight**: Spark DataFrames are **immutable execution plans**, not in-memory data structures. Each operation
creates a new execution plan.

## Solutions for Thread-Safe DataFrame Usage

### Solution 1: DataFrame References Are Already Thread-Safe (Simplest)

```python
# This actually works fine! DataFrames are immutable
large_df = spark.read.parquet("/huge/dataset/").cache()


def thread_function(dict1, dict2, dict3, dict4, dict5, iteration):
    # Each thread can safely use the same DataFrame reference
    # because operations create new execution plans
    filtered_df = large_df.filter(large_df.category == dict1['category'])

    # Different aggregations per thread
    if iteration % 3 == 0:
        result = filtered_df.groupBy('region').sum('amount')
    elif iteration % 3 == 1:
        result = filtered_df.groupBy('product').avg('price')
    else:
        result = filtered_df.groupBy('customer').count()

    return result.collect()


# This is safe and works well
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(thread_function, dict1, dict2, dict3, dict4, dict5, i)
        for i in range(15)
    ]
    results = [f.result() for f in futures]
```

### Solution 2: Explicit DataFrame "Cloning" via Checkpointing

```python
def create_dataframe_copies(base_df, num_copies=5):
    """Create independent DataFrame copies via checkpointing"""
    import tempfile
    import os

    # Cache the base DataFrame first
    base_df.cache()
    base_df.count()  # Trigger caching

    # Create temporary checkpoint directories
    checkpoint_dirs = []
    df_copies = []

    for i in range(num_copies):
        # Create temporary checkpoint location
        checkpoint_dir = f"/tmp/spark_checkpoint_{i}_{os.getpid()}"
        checkpoint_dirs.append(checkpoint_dir)

        # Set checkpoint directory
        spark.sparkContext.setCheckpointDir(checkpoint_dir)

        # Create independent copy via checkpoint
        df_copy = base_df.checkpoint(eager=True)  # Forces materialization
        df_copies.append(df_copy)

        print(f"Created DataFrame copy {i}")

    return df_copies, checkpoint_dirs


# Usage
large_df = spark.read.parquet("/huge/dataset/")
df_copies, checkpoint_dirs = create_dataframe_copies(large_df, num_copies=5)


def thread_function_with_copy(df_copy, dict1, dict2, dict3, dict4, dict5, iteration):
    """Each thread gets its own DataFrame copy"""
    result = df_copy.filter(df_copy.value > dict1['threshold'])
        .groupBy('category')
        .agg({'amount': 'sum'})
        .collect()
    return result


# Assign DataFrame copies to threads
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for i in range(15):
        df_copy = df_copies[i % len(df_copies)]  # Round-robin assignment
        future = executor.submit(
            thread_function_with_copy, df_copy,
            dict1, dict2, dict3, dict4, dict5, i
        )
        futures.append(future)

    results = [f.result() for f in futures]

# Cleanup checkpoint directories
for checkpoint_dir in checkpoint_dirs:
    import shutil

    shutil.rmtree(checkpoint_dir, ignore_errors=True)
```

### Solution 3: Pre-partition Strategy (Most Efficient for Large DataFrames)

```python
def partition_dataframe_by_thread(large_df, num_partitions=15):
    """Partition DataFrame for thread-specific processing"""

    # Add partition column for thread assignment
    from pyspark.sql.functions import hash, col, abs as spark_abs

    partitioned_df = large_df.withColumn(
        "thread_partition",
        spark_abs(hash(col("id"))) % num_partitions  # Distribute by hash of ID
    ).cache()

    # Trigger caching
    partitioned_df.count()

    # Create thread-specific DataFrames
    thread_dataframes = {}
    for i in range(num_partitions):
        thread_df = partitioned_df.filter(col("thread_partition") == i).cache()
        thread_dataframes[i] = thread_df
        print(f"Thread {i} DataFrame: {thread_df.count()} rows")

    return thread_dataframes


# Create partitioned DataFrames
large_df = spark.read.parquet("/huge/dataset/")
thread_dfs = partition_dataframe_by_thread(large_df, num_partitions=15)


def thread_function_partitioned(thread_df, dict1, dict2, dict3, dict4, dict5, iteration):
    """Each thread processes its own data partition"""
    result = thread_df.filter(thread_df.amount > dict1['min_amount'])
        .groupBy('category')
        .agg({'sales': 'sum', 'count': 'count'})
        .collect()
    return result


# Execute with partitioned data
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for i in range(15):
        future = executor.submit(
            thread_function_partitioned,
            thread_dfs[i],  # Each iteration gets its own partition
            dict1, dict2, dict3, dict4, dict5, i
        )
        futures.append(future)

    results = [f.result() for f in futures]
```

### Solution 4: Broadcast Variables for Read-Heavy Operations

```python
def create_broadcast_dataframe(large_df):
    """Convert DataFrame to broadcast variable for read-heavy operations"""

    # Convert to Pandas (only for smaller DataFrames < 8GB)
    if large_df.count() < 1000000:  # Adjust threshold based on memory
        pandas_df = large_df.toPandas()
        broadcast_df = spark.sparkContext.broadcast(pandas_df)
        return broadcast_df
    else:
        raise ValueError("DataFrame too large for broadcasting")


def thread_function_with_broadcast(broadcast_df, dict1, dict2, dict3, dict4, dict5, iteration):
    """Use broadcast DataFrame in each thread"""

    # Access broadcast data
    pandas_df = broadcast_df.value

    # Perform pandas operations (single-threaded but fast)
    filtered_df = pandas_df[pandas_df['category'] == dict1['category']]
    result = filtered_df.groupby('region')['amount'].sum().to_dict()

    return result


# Only for smaller DataFrames
if large_df.count() < 1000000:
    broadcast_df = create_broadcast_dataframe(large_df)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(thread_function_with_broadcast, broadcast_df,
                            dict1, dict2, dict3, dict4, dict5, i)
            for i in range(15)
        ]
        results = [f.result() for f in futures]
```

## What Happens with Large DataFrames?

### Memory Considerations

```python
# Large DataFrame scenarios
huge_df = spark.read.parquet("/1TB_dataset/")  # 1TB dataset


# Problem: Multiple threads triggering expensive operations
def memory_intensive_operation(df, iteration):
    # Each thread might trigger full dataset scan
    result = df.groupBy('all_columns').count().collect()  # Expensive!
    return result


# Solution: Pre-compute expensive operations
def optimized_large_df_approach():
    # Pre-compute common aggregations once
    summary_df = huge_df.groupBy('category', 'region')
        .agg({'amount': 'sum', 'count': 'count'})
        .cache()

    summary_df.count()  # Trigger caching

    # Now threads can work on smaller summary
    def thread_function(summary_df, dict1, dict2, dict3, dict4, dict5, iteration):
        filtered = summary_df.filter(summary_df.category == dict1['category'])
        return filtered.collect()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(thread_function, summary_df,
                            dict1, dict2, dict3, dict4, dict5, i)
            for i in range(15)
        ]
        results = [f.result() for f in futures]
```

### Performance Comparison for Large DataFrames (1TB+):

| Approach             | Memory Usage | Performance                    | Complexity |
|----------------------|--------------|--------------------------------|------------|
| **Shared DataFrame** | Low          | Good (if different operations) | Simple ⭐⭐⭐ |
| **Checkpointing**    | High         | Medium                         | Medium ⭐⭐  |
| **Pre-partitioning** | Medium       | Excellent                      | Medium ⭐⭐  |
| **Broadcast**        | Very High    | Excellent (small data only)    | Simple ⭐⭐⭐ |

## Recommended Approach:

For most cases with large DataFrames, **use the shared DataFrame approach** (Solution 1) because:

1. **DataFrames are immutable** - thread-safe by design
2. **Lazy evaluation** - operations don't interfere
3. **Spark handles optimization** - automatic query planning
4. **Minimal memory overhead** - no data duplication

```python
# Recommended: Simple shared DataFrame approach
large_df = spark.read.parquet("/huge/dataset/").cache()


def optimized_thread_function(shared_df, dict1, dict2, dict3, dict4, dict5, iteration):
    # Each thread does different analysis - no conflicts
    if iteration % 3 == 0:
        result = shared_df.filter(shared_df.type == 'A').groupBy('region').sum('amount')
    elif iteration % 3 == 1:
        result = shared_df.filter(shared_df.type == 'B').groupBy('product').avg('price')
    else:
        result = shared_df.filter(shared_df.type == 'C').groupBy('customer').count()

    return result.collect()


# This works efficiently even with 1TB+ DataFrames
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(optimized_thread_function, large_df,
                        dict1, dict2, dict3, dict4, dict5, i)
        for i in range(15)
    ]
    results = [f.result() for f in futures]
```

**Bottom line**: For large DataFrames, the shared DataFrame approach is usually best. Only use partitioning or
checkpointing if you're experiencing specific performance issues with the simple approach.