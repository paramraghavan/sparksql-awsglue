When submitting PySpark jobs with `spark-submit`, you should configure executor settings through command-line options
rather than in your code. Here's how to properly set these configurations:

## Spark-Submit Configuration Options

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  --driver-cores 2 \
  --conf spark.executor.memoryFraction=0.8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  myjob.py
```

Key parameters:

- `--num-executors`: Fixed number of executors (use with `--conf spark.dynamicAllocation.enabled=false`)
- `--executor-cores`: CPU cores per executor
- `--executor-memory`: Memory per executor
- `--driver-memory`: Driver memory
- `--driver-cores`: Driver CPU cores

## Dynamic Allocation Behavior

The executor scaling you're observing (adding up to 90+ then removing) suggests **Dynamic Allocation** is enabled. This
is actually normal behavior when:

1. **Dynamic Allocation is ON** (default in many clusters)
2. Spark automatically scales executors based on workload
3. Executors are removed when idle to free up cluster resources

## Why Executors Are Being Removed

Executors get removed when:

- They've been idle for `spark.dynamicAllocation.executorIdleTimeout` (default: 60s)
- The job has fewer active tasks
- Spark determines fewer executors are needed

## Solutions

**Option 1: Disable Dynamic Allocation (Fixed Executors)**

```bash
spark-submit \
  --conf spark.dynamicAllocation.enabled=false \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  myjob.py
```

**Option 2: Tune Dynamic Allocation**

```bash
spark-submit \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.dynamicAllocation.initialExecutors=10 \
  --conf spark.dynamicAllocation.executorIdleTimeout=300s \
  myjob.py
```

**Option 3: Configuration File**
Create `spark-defaults.conf`:

```
spark.executor.memory=8g
spark.executor.cores=4
spark.driver.memory=4g
spark.dynamicAllocation.enabled=false
```

## Best Practices

1. **Don't set these in your Python code** - use spark-submit options
2. **Monitor resource usage** in Spark UI to optimize settings
3. **Consider your cluster size** when setting executor counts
4. **Use dynamic allocation** for variable workloads, disable for predictable ones

The scaling behavior you're seeing is likely Dynamic Allocation working as intended, optimizing resource usage based on
your job's current needs.