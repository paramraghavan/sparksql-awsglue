Get various memory configurations from a Spark session.

```python
def get_spark_memory_configs(spark):
    """
    Get current Spark memory configurations including executor and driver memory
    
    Args:
        spark: SparkSession object
    Returns:
        dict: Dictionary containing memory configurations
    """
    # Get Spark configuration
    conf = spark.sparkContext.getConf()

    # Create dictionary to store memory configs
    memory_configs = {
        'spark.executor.memory': conf.get('spark.executor.memory', 'not set'),
        'spark.driver.memory': conf.get('spark.driver.memory', 'not set'),
        'spark.executor.memoryOverhead': conf.get('spark.executor.memoryOverhead', 'not set'),
        'spark.memory.fraction': conf.get('spark.memory.fraction', 'not set'),
        'spark.memory.storageFraction': conf.get('spark.memory.storageFraction', 'not set'),
        'spark.executor.cores': conf.get('spark.executor.cores', 'not set'),
        'spark.driver.cores': conf.get('spark.driver.cores', 'not set')
    }

    return memory_configs


# Usage example:
memory_info = get_spark_memory_configs(spark)
for key, value in memory_info.items():
    print(f"{key}: {value}")
```

Alternatively, you can also get all configurations at once:

```python
# Get all configurations
all_conf = spark.sparkContext.getConf().getAll()

# Filter for memory-related configs
memory_configs = [conf for conf in all_conf if 'memory' in conf[0]]
for key, value in memory_configs:
    print(f"{key}: {value}")
```

Above can accessed directly in your Spark UI under the "Environment" tab.
