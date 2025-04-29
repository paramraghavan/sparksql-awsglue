I'll explain how to configure Spark for each of these specific scenarios in your AWS EMR cluster.

## For Very Large Datasets (500GB-1TB)

To optimize processing for large datasets, adjust your shuffle partitions:

```python
spark = SparkSession.builder
    .appName("Large Dataset Processing")
    .config("spark.sql.shuffle.partitions", 2000)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", 200)
    .getOrCreate()
```

The key settings here:

- Higher shuffle partitions (2000) for better parallelism with large data
- Adaptive query execution to dynamically adjust partitions based on data size
- Minimum partition count to prevent over-consolidation

## For Memory-Intensive Operations (Large Joins/Aggregations)

For complex operations that require significant memory:

```python
spark = SparkSession.builder
    .appName("Memory Intensive Operations")
    .config("spark.memory.fraction", 0.8)
    .config("spark.memory.storageFraction", 0.3)
    .config("spark.sql.autoBroadcastJoinThreshold", "100MB")
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .config("spark.speculation", "true")
    .getOrCreate()
```

These settings:

- Increase memory fraction available to execution and storage (0.8)
- Adjust storage fraction for caching (0.3)
- Increase broadcast join threshold to 100MB (broadcasts smaller tables)
- Allow hash joins when beneficial
- Enable speculation to handle straggler tasks

## For Long-Running Notebooks

For notebooks that run over extended periods:

```python
spark = SparkSession.builder
    .appName("Long Running Notebook")
    .config("spark.driver.memory", "30g")
    .config("spark.driver.maxResultSize", "8g")
    .config("spark.cleaner.periodicGC.interval", "15min")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .getOrCreate()
```

These settings:

- Increase driver memory to handle accumulated results (30GB)
- Increase maximum result size to prevent failures when collecting large results
- Enable periodic garbage collection every 15 minutes
- Extend network timeout to handle long-running tasks
- Increase heartbeat interval for better stability

## Combined Configuration for Complex Workloads

For workloads that involve large datasets, complex operations, and long-running scripts:

```python
spark = SparkSession.builder
    .appName("Complex EMR Workload")
    .config("spark.driver.memory", "30g")
    .config("spark.driver.maxResultSize", "8g")
    .config("spark.sql.shuffle.partitions", 2000)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.memory.fraction", 0.8)
    .config("spark.network.timeout", "800s")
    .config("spark.cleaner.periodicGC.interval", "15min")
    .getOrCreate()
```

## Performance Monitoring

Remember to monitor your job performance through the Spark UI to determine if these settings are effective. Look for:

1. Executor memory usage and GC time
2. Shuffle spill metrics (disk vs. memory)
3. Task skew and duration
4. Stage completion times

Adjust the configurations based on these metrics for further optimization based on your specific workload patterns.