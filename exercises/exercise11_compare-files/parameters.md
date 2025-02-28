The configuration parameters for Spark are crucial for performance optimization, especially with a 25GB dataset. Here's
how to determine appropriate values:

### `spark.executor.memory`

- **What it does**: Memory allocated to each executor process
- **How to size it**:
    - Calculate: (Node memory - OS overhead) / Number of executors per node
    - For a 25GB file, 20GB per executor allows processing large partitions
    - If using r5.4xlarge instances (128GB RAM), you might have 2-3 executors per node, making 20GB per executor
      reasonable

### `spark.driver.memory`

- **What it does**: Memory allocated to the driver program
- **How to size it**:
    - For client mode: Based on client machine's available memory
    - For 25GB data, 10GB is sufficient to handle aggregated results and operations
    - Rule of thumb: 50% of executor memory for medium-sized operations

### `spark.executor.cores`

- **What it does**: Number of cores for each executor to use
- **How to size it**:
    - For data processing: 4-5 cores per executor is optimal
    - Too few: Underutilizes memory
    - Too many: Creates excessive parallelism overhead

### `spark.sql.shuffle.partitions`

- **What it does**: Number of partitions during shuffling operations
- **How to size it**:
    - Rule of thumb: 2-3× the total executor cores in your cluster
    - For 10-node cluster with 4 cores per executor and 3 executors per node: ~240-360 partitions
    - 400 would be appropriate for slightly larger clusters or ensuring enough parallelism

To determine the best values for your specific EMR cluster:

1. Count total cluster cores: `(# nodes) × (cores per node)`
2. Calculate memory per node: Check instance type specifications
3. Determine executors per node: Typically 1 per 5-7 cores, leaving 1 core for YARN

For a 25GB file on a typical EMR cluster (e.g., 10 r5.2xlarge nodes), these values are reasonable starting points that
you can tune based on job performance metrics.

## how to determine the appropriate shuffle size

Setting `spark.sql.shuffle.partitions=400` controls how many partitions Spark creates during operations that require
shuffling data across the cluster (like joins, groupBy, or repartition operations).

For a 25GB dataset, here's how to determine the appropriate value:

### How to calculate `spark.sql.shuffle.partitions`

1. **Based on total cores**:
    - Rule of thumb: Use 2-3× the total number of cores in your cluster
    - Example: If you have 10 nodes with 8 cores each (80 total cores), a value between 160-240 would be reasonable

2. **Based on data size**:
    - Target partition size: ~128MB per partition is often optimal
    - For 25GB: 25GB ÷ 128MB ≈ 200 partitions
    - Round up for skewed data

3. **Based on memory pressure**:
    - Too few partitions: Each requires more memory, risking OOM errors
    - Too many: Creates excessive task scheduling overhead

The value of 400 partitions would be appropriate if:

- You have a large cluster with approximately 130-200 cores
- Your data has significant skew (some partitions much larger than others)
- You're performing complex joins that increase data size

If you're seeing long execution times or out-of-memory errors, you might need to adjust this value. Monitor your Spark
UI during execution to see if:

- Many small tasks (reduce the number)
- A few very long-running tasks (increase the number)
- Memory pressure (increase the number)

For a more precise calculation, you can divide your dataset size by your target partition size (e.g., 25GB / 64MB = ~400
partitions).