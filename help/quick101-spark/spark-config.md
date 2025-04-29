# Spark Configuration Parameters

## 1. executor-memory

This parameter sets the amount of memory to use per executor process.

Setting the right amount of memory for your executors is crucial for optimal performance. The default is typically
around 1GB, but with your large datasets (up to 1TB), you'll likely need more.

For your cluster with r6a.8xlarge, r5.12xlarge, and r5.16xlarge instances, you should consider the following:

- These instances have between 256-512GB of memory
- Reserve about 10-15% for OS and Hadoop daemons
- For example, on an r5.16xlarge (512GB), you might set executor-memory to 43-45GB

## 2. num-executors

This parameter sets the number of executor processes to use across all nodes.

With dynamic allocation enabled (recommended), this parameter is less important but still serves as an initial
allocation. The default might be too low for your large datasets.

For your 58 task nodes plus 1 core node:

- Calculate based on cores per node and memory per executor
- For example, if using 1 executor per 5 cores on r5.16xlarge (64 cores), you'd have about 12 executors per node
- This would give you approximately 708 executors total (12 × 59)

## 3. dynamicAllocation.minExecutors

Sets the lower bound for the number of executors if dynamic allocation is enabled.

This ensures your application always has a minimum number of executors, even when resource demands are low. It's useful
to set this to a non-zero value to maintain some level of parallelism at all times.

Recommendation: Set to 10-20% of your max executors to ensure core functionality always has resources.

## 4. dynamicAllocation.maxExecutors

Sets the upper bound for the number of executors with dynamic allocation.

This limits how many executors your application can request from the cluster manager. If set too low, your application
might be resource-starved; if too high, it might take too many resources from other applications.

For your large cluster, consider setting this to about 80-90% of your total available executors to leave some headroom
for other processes.

## 5. executor-cores

Specifies the number of cores to use per executor.

This controls the number of tasks an executor can run in parallel. Setting the right value helps optimize resource
utilization.

Best practices:

- For memory-intensive workloads: 4-5 cores per executor
- For compute-intensive workloads: 2-3 cores per executor
- For your R-series instances, which are memory-optimized, 4-5 cores is often optimal

### In details

Yes, `spark.executor.cores` indirectly determines how many executors you'll have per task node, but it works in
conjunction with other parameters.

Let me explain how it works:

The number of executors per node is calculated based on the available cores and how many cores you allocate to each
executor. The formula is:

```
Number of executors per node = (Total cores per node - cores for overhead) ÷ spark.executor.cores
```

For example, with your r6a.8xlarge instance (32 vCPUs):

- If `spark.executor.cores = 4`: You get approximately 7 executors per node (28÷4, reserving 4 cores for overhead)
- If `spark.executor.cores = 8`: You get approximately 3 executors per node (24÷8, reserving 8 cores for overhead)
- If `spark.executor.cores = 16`: You get approximately 1 executor per node (reserving some cores for overhead)

However, to directly control the number of executors per node, you should use:

1. `spark.executor.instances`: Sets the total number of executors for your application
2. `spark.dynamicAllocation.enabled`: When set to true, Spark can dynamically adjust executor count

To precisely control executors per node, you'd typically need to use a combination of these settings along with cluster
manager configurations specific to your environment (like YARN, Kubernetes, or Mesos).

The key is understanding that `spark.executor.cores` sets how many cores each executor gets, and consequently, how many
executors fit on each node.

The number of executors per task node is typically specified in the configuration settings of your distributed computing
framework, such as Apache Spark, Apache Hadoop, or other similar systems. This isn't directly an AWS configuration
parameter but rather a setting in your computing framework.

For Apache Spark, for example, you would specify this in your Spark configuration, often through:

1. The `--executor-cores` parameter when submitting a job
2. The `spark.executor.cores` configuration property
3. In frameworks like Amazon EMR, this might be in the `maximizeResourceAllocation` setting

The number of executors you can run per node depends on:

- The vCPU count (32 in the r6a.8xlarge)
- The memory requirements of your workload
- Whether you need to reserve resources for the system or other processes

For a 32 vCPU instance like r6a.8xlarge, common configurations might be:

- 1 executor with 32 cores (if your workload benefits from shared memory)
- 4 executors with 8 cores each
- 8 executors with 4 cores each
- 16 executors with 2 cores each

When we say "task node of type r6a.8xlarge and 58 task nodes," it means there are 58 separate instances (or virtual
machines) of the r6a.8xlarge type being used.

The r6a.8xlarge refers to a specific Amazon EC2 instance type with particular computing characteristics. Each of these
58 instances would have identical specifications, including:

- The same amount of CPU cores
- The same amount of RAM
- The same networking capabilities
- The same performance characteristics

This configuration would be used in distributed computing environments where you need multiple identical machines to
process workloads in parallel.

## 6. driver-memory

Amount of memory to use for the driver process.

The driver coordinates the job execution and collects results. For large datasets, especially when collecting results to
the driver, this needs to be sufficiently large.

Recommendation: For your TB-scale datasets, set driver-memory to at least 20-30GB. Increase this if you're collecting
large results to the driver.

## 7. maxResultSize

Maximum size of the result of a Spark action that can be collected to the driver.

This is a safety measure to prevent out-of-memory errors on the driver. The default is usually 1GB, which may be too
small for your large datasets.

For your use case, set this to at least 2-4GB, and increase if you're collecting large results.

# How to Decide These Values

1. **Start with cluster capacity calculation**:
    - Calculate total memory and cores available
    - Reserve 10-15% for OS and overhead
    - Divide remaining resources among executors

2. **Consider your workload**:
    - Memory-intensive vs. compute-intensive
    - Data size and distribution
    - Join operations and shuffle requirements

3. **Monitor and adjust**:
    - Check Spark UI for resource utilization
    - Watch for executor failures or poor performance
    - Adjust parameters based on observed behavior

# Resource Management

To properly relinquish resources after completing tasks, consider these approaches:

1. **Use dynamic allocation with proper cleanup**:
    - Enable `spark.dynamicAllocation.enabled=true`
    - Set `spark.dynamicAllocation.executorIdleTimeout` (default 60s)
    - Ensure `spark.shuffle.service.enabled=true` for proper cleanup

2. **Explicitly release resources**:
    - Use `spark.stop()` to shut down the SparkContext
    - In Jupyter notebooks, disconnect kernel when done

3. **Clear cached data**:
    - Use `df.unpersist()` for DataFrames you no longer need
    - Call `sc.clearCache()` to clear all cached data

# Fixing "Initial job has not accepted any resources" Error

This error typically occurs when:

1. Your cluster doesn't have enough resources to satisfy your requested configuration
2. Your executors are requesting more resources than available per node
3. Other applications are consuming cluster resources

Solutions:

- Reduce executor memory or cores requests
- Increase the number of task nodes
- Check Spark UI to see if other applications are hogging resources
- Ensure YARN has enough capacity for your job

# Example Configuration for Your Cluster

```python
spark = SparkSession.builder
.appName("Large Data Processing")
.config("spark.executor.memory", "40g")
.config("spark.driver.memory", "25g")
.config("spark.executor.cores", 4)
.config("spark.dynamicAllocation.enabled", "true")
.config("spark.dynamicAllocation.minExecutors", 50)
.config("spark.dynamicAllocation.maxExecutors", 600)
.config("spark.driver.maxResultSize", "3g")
.config("spark.shuffle.service.enabled", "true")
.config("spark.dynamicAllocation.executorIdleTimeout", "60s")
.getOrCreate()
```

> When you set spark.executor.memory to 40g, you're specifying that each executor should be allocated 40g of memory. The
> total memory used across your cluster would be:
> Total Memory Used = Number of Executors × Executor Memory

## Shuffle Service and Locality Wait - spark.shuffle.service.enabled/spark.sql.shuffle.partitions)

### `spark.shuffle.service.enabled` = "true"

This parameter enables the external shuffle service in Spark, which has several important benefits:

- **Resource Release**: When executors are terminated (especially with dynamic allocation), the shuffle service allows
  Spark to continue accessing shuffle data from completed tasks, enabling faster resource release after tasks complete
- **Performance**: It improves shuffle reliability by handling shuffle data independently from executor lifecycle
- **Dynamic Allocation**: This is practically required for effective dynamic allocation, as it allows executors to be
  added/removed without losing shuffle data

### `spark.locality.wait` = "10s"

This parameter controls how long Spark will wait to launch a task on a preferred location before giving up and
scheduling it elsewhere:

- **Data Locality**: Spark prefers to process data on the same node where it's stored (HDFS data locality)
- **Balancing Speed vs. Locality**: Setting to 10 seconds is a moderate setting - Spark will wait up to 10 seconds for
  an ideal data-local slot before compromising
- **Throughput Impact**: Lower values prioritize immediate processing over data locality, while higher values prioritize
  data locality at the cost of potential delays

## YARN Resource Allocation

### Adjusting `yarn.scheduler.maximum-allocation-mb`

This YARN configuration controls the maximum memory that can be allocated to a single container:

- **Error Prevention**: The "Initial job has not accepted any resources" error often occurs when your executor memory
  request exceeds this YARN limit
- **Container Sizing**: If set too low, your Spark executors may request more memory than YARN allows in a single
  container
- **Resolution**: Increasing this value allows larger executor memory allocations per container
- **Typical Fix**: This parameter should be set higher than your executor memory plus overhead

For example, if you're requesting 330GB for executor memory, you'd need:

- `yarn.scheduler.maximum-allocation-mb` ≥ (330GB × 1024) + overhead ≈ 340,000 MB

You would adjust this in your EMR configuration by:

1. Adding it to the `yarn-site.xml` configuration
2. Or setting it as an EMR configuration parameter when launching the cluster

## Shuffle Partitions

### `spark.sql.shuffle.partitions`

This parameter controls the number of partitions used during shuffles for SQL operations:

- **Default Value**: The default is 200, which is often too low for large datasets
- **Performance Impact**:
    - Too few partitions: Each partition becomes too large, causing executor memory pressure, spills to disk, and uneven
      workload distribution
    - Too many partitions: Excess overhead from managing many small tasks, network connections, and shuffle files

- **Sizing Formula**: A good rule of thumb is:
    - For datasets up to 10GB: 200-400 partitions
    - For 50-200GB: 400-1000 partitions
    - For 500GB-1TB: 1000-2000+ partitions

- **Adaptive Sizing**: For your larger datasets (500GB-1TB), consider enabling adaptive query execution:
  ```python
  .config("spark.sql.adaptive.enabled", "true")
  .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
  ```
  This allows Spark to dynamically adjust partition count based on actual data size

The proper setting for shuffle partitions is particularly important for your larger datasets as it directly affects
memory pressure during shuffles and the overall parallelism of your computation. Insufficient partitions for large
datasets is a common cause of executor memory pressure and out-of-memory errors.

## What if we use default configurations instead of specific settings for AWS EMR clusters.

Yes, you can use default configurations instead of specific settings for AWS EMR clusters. This is often a good approach
when starting out, as EMR comes with sensible defaults tuned for many common workloads.

## Default Configuration Benefits

EMR provides intelligent defaults that:

1. **Dynamic Resource Allocation**: By default, EMR enables dynamic allocation, which automatically scales executors
   based on workload

2. **Instance-Aware Settings**: EMR sets executor memory and cores based on the instance types in your cluster

3. **Adaptive Execution**: Newer EMR versions enable adaptive query execution by default, which optimizes partition
   sizes at runtime

## Simplified Configuration Example

If you want to use mostly defaults, your Spark session creation could be as simple as:

```python
spark = SparkSession.builder
    .appName("EMR Job")
    .enableHiveSupport()
    .getOrCreate()
```

## When Defaults Might Need Adjustment

While defaults work well for many scenarios, you may still need custom configurations in these cases:

1. **Very Large Datasets**: For your 500GB-1TB files, you might need to increase shuffle partitions

2. **Memory-Intensive Operations**: For operations like large joins or aggregations, you might need to tune memory
   settings

3. **Long-Running Applications**: For notebooks that run for extended periods, you might want to set a higher driver
   memory

## Monitoring Performance

Instead of pre-configuring everything, a better approach is often to:

1. Start with defaults
2. Monitor job performance through Spark UI
3. Address specific bottlenecks as they appear

For example, if you see excessive spill to disk, you might increase memory; if you see task skew, you might increase
partitions.

EMR makes it easier to run Spark workloads by providing pre-configured cluster settings optimized for most common
scenarios.

If you do encounter the "initial job has not accepted any resources" error with default settings, it usually indicates
high cluster utilization rather than configuration issues. In this case, try again later or request additional task
nodes.