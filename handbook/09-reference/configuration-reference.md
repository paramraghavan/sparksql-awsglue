# Spark Configuration Reference

## Core Memory Settings

### spark.executor.memory
- **Default**: 1g
- **Recommended**: 4-8g per executor
- **Description**: Amount of memory to allocate per executor JVM
- **Example**: `--executor-memory 8g`
- **Notes**: Does NOT include memory overhead

### spark.executor.memoryOverhead
- **Default**: max(384MB, 0.10 × executor.memory)
- **Recommended**: 1-2g for PySpark jobs
- **Description**: Off-heap memory for Python processes, thread stacks, NIO buffers
- **Critical for**: PySpark with UDFs (needs Python process overhead)
- **Example**: `--conf spark.executor.memoryOverhead=2g`

### spark.driver.memory
- **Default**: 1g
- **Recommended**: 4-8g (especially with large collect() operations)
- **Description**: JVM heap memory for driver program
- **Example**: `--driver-memory 4g`

### spark.driver.memoryOverhead
- **Default**: max(384MB, 0.10 × driver.memory)
- **Description**: Off-heap memory for driver program
- **Example**: `--conf spark.driver.memoryOverhead=2g`

---

## Execution Settings

### spark.executor.cores
- **Default**: 1
- **Recommended**: 4-5 cores per executor
- **Why**: Reduces thread contention, better performance
- **Example**: `--executor-cores 5`
- **Note**: More cores = more concurrent tasks, more memory overhead

### spark.executor.instances (or --num-executors)
- **Default**: Varies by cluster manager
- **Recommended**: 20-50 for medium jobs
- **Calculation**: Total executors = data_size_GB / (executor.memory × data_density)
- **Example**: `--num-executors 20`

### spark.dynamicAllocation.enabled
- **Default**: false
- **Recommended**: true for YARN/Kubernetes
- **Description**: Automatically add/remove executors based on load
- **Example**: `--conf spark.dynamicAllocation.enabled=true`

### spark.dynamicAllocation.minExecutors
- **Default**: 0
- **Recommended**: 2-5 (keep some warm)
- **Example**: `--conf spark.dynamicAllocation.minExecutors=5`

### spark.dynamicAllocation.maxExecutors
- **Default**: Unlimited
- **Recommended**: 100+ for large jobs
- **Example**: `--conf spark.dynamicAllocation.maxExecutors=100`

---

## Shuffle & Partitioning

### spark.sql.shuffle.partitions
- **Default**: 200
- **Recommended**: 2-4 × total executor cores, or data_size_GB × 4
- **Calculation for 100GB dataset**: 100 × 4 = 400
- **Impact**: Controls post-shuffle partition count for groupBy, join, etc.
- **Example**: `--conf spark.sql.shuffle.partitions=500`

### spark.default.parallelism
- **Default**: Total cores in cluster
- **Recommended**: 2-3 × total cores
- **Description**: Default number of partitions for RDD operations
- **Example**: `--conf spark.default.parallelism=200`

### spark.files.maxPartitionBytes
- **Default**: 128MB
- **Description**: Max bytes per partition when reading files
- **Tune**: Decrease for large clusters (avoid small files), increase for few large files

### spark.sql.files.maxRecordsPerFile
- **Default**: 0 (unlimited)
- **Description**: Max records per output file when writing
- **Use**: Control number of output files
- **Example**: `--conf spark.sql.files.maxRecordsPerFile=1000000`

---

## Memory Management

### spark.memory.fraction
- **Default**: 0.6
- **Recommended**: 0.7-0.8 for memory-heavy jobs
- **Description**: Fraction of (total - reserved) for unified memory (storage + execution)
- **Example**: `--conf spark.memory.fraction=0.8`

### spark.memory.storageFraction
- **Default**: 0.5
- **Description**: Fraction of unified memory for storage (cache)
- **Tune**: Decrease to favor execution (shuffles), increase to favor storage (cache)

### spark.memory.offHeap.enabled
- **Default**: false
- **Description**: Enable off-heap memory management (Tungsten)
- **Recommended**: true for large datasets
- **Example**: `--conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=8g`

---

## Performance Optimization

### spark.sql.adaptive.enabled
- **Default**: false (true in Spark 3.0+)
- **Recommended**: true (huge performance gains)
- **Description**: Enable Adaptive Query Execution
- **Benefits**:
  - Coalesce small partitions after shuffle
  - Switch to broadcast join if data is smaller than expected
  - Handle skewed joins automatically
- **Example**: `--conf spark.sql.adaptive.enabled=true`

### spark.sql.adaptive.coalescePartitions.enabled
- **Default**: true (if AQE enabled)
- **Description**: Combine small shuffle partitions into larger ones
- **Impact**: Reduces task overhead, I/O

### spark.sql.adaptive.skewJoin.enabled
- **Default**: true (if AQE enabled)
- **Description**: Automatically detect and handle skewed joins
- **Impact**: Splits large partitions into smaller sub-partitions

### spark.sql.autoBroadcastJoinThreshold
- **Default**: 10MB
- **Recommended**: 100MB+ for larger tables
- **Description**: Maximum size of broadcasted table in joins
- **Example**: `--conf spark.sql.autoBroadcastJoinThreshold=100MB`

### spark.sql.broadcastTimeout
- **Default**: 300 seconds
- **Recommended**: 600 for slow networks
- **Description**: Timeout for broadcast operations

---

## Caching & Persistence

### spark.sql.inMemoryColumnarStorage.compressed
- **Default**: true
- **Description**: Compress cached data in memory
- **Impact**: Uses less memory, slightly slower access

### spark.sql.inMemoryColumnarStorage.batchSize
- **Default**: 10000
- **Description**: Batch size for columnar caching
- **Tune**: Higher = better compression, more memory per batch

---

## Data Writing

### spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version
- **Default**: 1
- **Recommended**: 2 (faster)
- **Description**: Algorithm for committing output files
- **Impact**: v2 is much faster for S3

### spark.speculation
- **Default**: false
- **Description**: Enable speculative execution (backup tasks)
- **When to use**: With heterogeneous clusters (slow nodes)
- **Example**: `--conf spark.speculation=true`

### spark.speculation.multiplier
- **Default**: 1.5
- **Description**: Tasks 1.5× slower than median get backup copies
- **Example**: `--conf spark.speculation.multiplier=1.5`

---

## AWS/EMR Specific

### spark.hadoop.fs.s3a.access.key & spark.hadoop.fs.s3a.secret.key
- **Note**: Use IAM roles instead (not hardcoded keys)

### spark.hadoop.fs.s3a.committer.name
- **Default**: "magic"
- **Description**: S3 commit algorithm for faster, safer writes
- **Example**: `--conf spark.hadoop.fs.s3a.committer.name=magic`

### spark.hadoop.fs.s3a.committer.staging.tmp.path
- **Description**: Staging directory for S3 writes
- **Example**: `/tmp/staging`

---

## YARN Specific

### yarn.scheduler.maximum-allocation-mb
- **Description**: Maximum memory allocatable per container
- **EMR Default**: Usually matches node memory
- **Note**: Executor memory cannot exceed this

### yarn.scheduler.maximum-allocation-vcores
- **Description**: Maximum virtual cores per container

### yarn.nodemanager.resource.memory-mb
- **Description**: Total memory available on node

---

## Network & Communication

### spark.rpc.message.maxSize
- **Default**: 128MB
- **Description**: Maximum message size for RPC
- **Tune**: Increase for large broadcast variables or collect()

### spark.network.timeout
- **Default**: 120 seconds
- **Recommended**: 300+ for slow networks or large shuffles
- **Example**: `--conf spark.network.timeout=300s`

---

## Logging & Debugging

### spark.executor.logs.rolling.maxSize
- **Default**: 104857600 (100MB)
- **Description**: Max size of executor log files before rolling

### spark.executor.logs.rolling.maxRetainedFiles
- **Default**: 5
- **Description**: How many rolled log files to keep

### spark.eventLog.enabled
- **Default**: true
- **Recommended**: true for production (enable Spark UI)
- **Example**: `--conf spark.eventLog.enabled=true`

### spark.eventLog.dir
- **Default**: file:///tmp/spark-events
- **Recommended**: HDFS or S3 path for persistent logs
- **Example**: `--conf spark.eventLog.dir=s3://my-bucket/spark-logs/`

---

## Configuration Best Practices

### For General ETL
```bash
spark-submit \
    --num-executors 20 \
    --executor-memory 8g \
    --executor-cores 5 \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.memory.fraction=0.8 \
    my_job.py
```

### For Skewed Data
```bash
spark-submit \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB \
    --conf spark.sql.shuffle.partitions=1000 \
    my_job.py
```

### For Memory-Constrained Clusters
```bash
spark-submit \
    --num-executors 50 \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.executor.memoryOverhead=1g \
    --conf spark.sql.shuffle.partitions=800 \
    my_job.py
```

---

## Checking Current Configuration

```python
# Get all configs
spark.sparkContext.getConf().getAll()

# Get specific config
spark.conf.get("spark.sql.shuffle.partitions")

# Set config (must be before creating SparkSession)
spark.conf.set("spark.sql.shuffle.partitions", "500")
```

---

## Summary Table

| Purpose | Parameter | Default | Recommended |
|---------|-----------|---------|-------------|
| Executor Memory | spark.executor.memory | 1g | 8g |
| Driver Memory | spark.driver.memory | 1g | 4g |
| Shuffle Partitions | spark.sql.shuffle.partitions | 200 | 500-1000 |
| Broadcast Threshold | spark.sql.autoBroadcastJoinThreshold | 10MB | 100MB |
| Adaptive Execution | spark.sql.adaptive.enabled | false | true |
| Memory Fraction | spark.memory.fraction | 0.6 | 0.8 |
| Cores per Executor | spark.executor.cores | 1 | 5 |

---

**Pro Tip**: Start with default configs, measure (using Spark UI), then tune based on actual bottlenecks.
