"""
I am running this multiprocessing code.
In function your_function, I am performing broadcast(df) in conjunction with dataframe df_temp

We get error exceptions.base.PySparkRuntimeError: [context_only_valid_on_driver] it appears youare attempting to
refrence sparkcontext from awithin worker node which is not allowed. how do i fix it
"""

```python
from multiprocessing import Pool, cpu_count
import os
def your_function_wrapper(params):
"""Wrapper to unpack parameters"""
dict1, dict2, dict3, dict4, dict5, df, int_param = params

    # Initialize Spark session in each process
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"Process_{os.getpid()}").getOrCreate()
    
    try:
        result = your_function(dict1, dict2, dict3, dict4, dict5, df, int_param)
        return (int_param, result)
    finally:
        spark.stop()

def run_parallel_processes(params_list, max_workers=None):
if max_workers is None:
max_workers = min(cpu_count(), len(params_list))

    with Pool(processes=max_workers) as pool:
        results = pool.map(your_function_wrapper, params_list)
    
    return results

# Execute

results = run_parallel_processes(params_list, max_workers=4)
```
