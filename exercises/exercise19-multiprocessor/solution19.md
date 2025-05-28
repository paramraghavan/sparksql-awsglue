The error occurs because you're trying to pass a **Spark DataFrame (`df`) directly between processes**, which contains
references to the SparkContext that can't be serialized/deserialized across process boundaries.

## Root Cause

```python
# This is the problem:
params_list = [
    (dict1, dict2, dict3, dict4, dict5, df, i)  # ← df contains SparkContext!
    for i in range(15)
]

# When multiprocessing tries to serialize this:
with Pool(processes=max_workers) as pool:
    results = pool.map(your_function_wrapper, params_list)  # ← Serialization fails!
```

**Spark DataFrames cannot be pickled/serialized across processes** because they contain references to the SparkContext.

## Solutions

### Solution 1: Pass Data Path Instead of DataFrame (Recommended)

```python
def your_function_wrapper(params):
    """Wrapper that recreates DataFrame from path"""
    dict1, dict2, dict3, dict4, dict5, data_path, int_param = params

    # Initialize Spark session in each process
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"Process_{os.getpid()}").getOrCreate()

    try:
        # Recreate DataFrame from data source in each process
        df = spark.read.parquet(data_path)  # or whatever your data source is

        result = your_function(dict1, dict2, dict3, dict4, dict5, df, int_param)
        return (int_param, result)
    finally:
        spark.stop()


def run_parallel_processes_fixed(params_list, data_path, max_workers=None):
    """Fixed version that passes data path instead of DataFrame"""
    if max_workers is None:
        max_workers = min(cpu_count(), len(params_list))

    # Create params with data path instead of DataFrame
    process_params = [
        (dict1, dict2, dict3, dict4, dict5, data_path, i)
        for dict1, dict2, dict3, dict4, dict5, _, i in params_list
    ]

    with Pool(processes=max_workers) as pool:
        results = pool.map(your_function_wrapper, process_params)

    return results


# Usage:
data_path = "/path/to/your/data.parquet"  # Your data source
results = run_parallel_processes_fixed(params_list, data_path, max_workers=4)
```

### Solution 2: Convert DataFrame to Serializable Format

```python
def your_function_wrapper_with_data(params):
    """Wrapper that recreates DataFrame from serialized data"""
    dict1, dict2, dict3, dict4, dict5, pandas_data, schema_json, int_param = params

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    import pandas as pd
    import json

    spark = SparkSession.builder.appName(f"Process_{os.getpid()}").getOrCreate()

    try:
        # Recreate DataFrame from pandas data
        pandas_df = pd.DataFrame(pandas_data)

        # Convert back to Spark DataFrame with original schema
        schema = StructType.fromJson(json.loads(schema_json))
        df = spark.createDataFrame(pandas_df, schema)

        result = your_function(dict1, dict2, dict3, dict4, dict5, df, int_param)
        return (int_param, result)
    finally:
        spark.stop()


def prepare_serializable_data(df):
    """Convert Spark DataFrame to serializable format"""
    # Convert to Pandas (only works for smaller DataFrames)
    pandas_data = df.toPandas().to_dict('records')
    schema_json = df.schema.json()

    return pandas_data, schema_json


def run_parallel_with_serialized_data(params_list, df, max_workers=None):
    """Version that serializes DataFrame data"""
    if max_workers is None:
        max_workers = min(cpu_count(), len(params_list))

    # Prepare serializable data once
    pandas_data, schema_json = prepare_serializable_data(df)

    # Create params with serialized data
    process_params = [
        (dict1, dict2, dict3, dict4, dict5, pandas_data, schema_json, i)
        for dict1, dict2, dict3, dict4, dict5, _, i in params_list
    ]

    with Pool(processes=max_workers) as pool:
        results = pool.map(your_function_wrapper_with_data, process_params)

    return results


# Usage (only for smaller DataFrames):
results = run_parallel_with_serialized_data(params_list, df, max_workers=4)
```

### Solution 3: Use Shared Storage (Best for Large DataFrames)

```python
import tempfile
import uuid


def your_function_wrapper_shared_storage(params):
    """Wrapper that reads DataFrame from shared storage"""
    dict1, dict2, dict3, dict4, dict5, temp_path, int_param = params

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"Process_{os.getpid()}").getOrCreate()

    try:
        # Read DataFrame from temporary shared storage
        df = spark.read.parquet(temp_path)

        result = your_function(dict1, dict2, dict3, dict4, dict5, df, int_param)
        return (int_param, result)
    finally:
        spark.stop()


def run_parallel_with_shared_storage(params_list, df, max_workers=None):
    """Version that uses shared temporary storage"""
    if max_workers is None:
        max_workers = min(cpu_count(), len(params_list))

    # Write DataFrame to temporary shared location
    temp_path = f"/tmp/multiprocess_df_{uuid.uuid4()}"
    df.write.mode('overwrite').parquet(temp_path)

    try:
        # Create params with temp path
        process_params = [
            (dict1, dict2, dict3, dict4, dict5, temp_path, i)
            for dict1, dict2, dict3, dict4, dict5, _, i in params_list
        ]

        with Pool(processes=max_workers) as pool:
            results = pool.map(your_function_wrapper_shared_storage, process_params)

        return results
    finally:
        # Clean up temporary files
        import shutil
        try:
            shutil.rmtree(temp_path)
        except:
            pass


# Usage:
results = run_parallel_with_shared_storage(params_list, df, max_workers=4)
```

### Solution 4: Fix Broadcast Issue in your_function

If you're using `broadcast()` inside `your_function`, you also need to fix that:

```python
def your_function_fixed(dict1, dict2, dict3, dict4, dict5, df, int_param):
    """Fixed version that handles broadcast correctly in multiprocessing"""

    # Get current Spark session (each process has its own)
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    # If you need to broadcast data, recreate the broadcast in each process
    if df.count() < 1000000:  # Only broadcast small DataFrames
        # Convert to pandas for broadcasting (if small enough)
        pandas_df = df.toPandas()
        broadcast_df = spark.sparkContext.broadcast(pandas_df)

        # Use broadcast data
        broadcast_data = broadcast_df.value

        # Your processing logic here
        # ...

        result = process_with_broadcast_data(broadcast_data, dict1, dict2, dict3, dict4, dict5)
    else:
        # For large DataFrames, don't broadcast - use regular operations
        result = df.filter(...).groupBy(...).agg(...).collect()

    return result
```

## Recommended Approach

**For most cases, use Solution 1 (Pass Data Path):**

```python
def multiprocess_solution_recommended():
    """Recommended approach using data paths"""

    # Save your DataFrame to a shared location first
    data_path = "/shared/storage/your_data.parquet"
    df.write.mode('overwrite').parquet(data_path)

    def process_wrapper(params):
        dict1, dict2, dict3, dict4, dict5, data_path, int_param = params

        from pyspark.sql import SparkSession
        spark = SparkSession.builder
            .appName(f"Process_{os.getpid()}")
            .config("spark.driver.memory", "4g")
            .getOrCreate()

        try:
            # Read data in each process
            df = spark.read.parquet(data_path)

            # Your processing logic
            result = your_processing_logic(df, dict1, dict2, dict3, dict4, dict5, int_param)

            return (int_param, result)
        finally:
            spark.stop()

    # Prepare parameters without DataFrame
    process_params = [
        (dict1, dict2, dict3, dict4, dict5, data_path, i)
        for i in range(15)
    ]

    # Run multiprocessing
    with Pool(processes=4) as pool:
        results = pool.map(process_wrapper, process_params)

    return results


# Execute
results = multiprocess_solution_recommended()
```

## Alternative: Stick with ThreadPoolExecutor

**Consider whether multiprocessing is really necessary:**

```python
# ThreadPoolExecutor doesn't have serialization issues
from concurrent.futures import ThreadPoolExecutor


def thread_safe_function(dict1, dict2, dict3, dict4, dict5, df, int_param):
    """Works perfectly with threads - no serialization needed"""

    # Can use broadcast safely in threads
    if df.count() < 1000000:
        pandas_df = df.toPandas()
        broadcast_df = spark.sparkContext.broadcast(pandas_df)
        broadcast_data = broadcast_df.value

    # Your processing logic
    result = process_with_data(df, dict1, dict2, dict3, dict4, dict5, int_param)
    return result


# Much simpler - no DataFrame serialization issues
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(thread_safe_function, dict1, dict2, dict3, dict4, dict5, df, i)
        for i in range(15)
    ]
    results = [f.result() for f in futures]
```

**Bottom line**: The multiprocessing approach requires recreating DataFrames from data sources in each process. Unless
you have specific requirements that threads can't meet, ThreadPoolExecutor is usually simpler and more efficient for
PySpark workloads.