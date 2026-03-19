# SparkSession Configuration

## Creating a SparkSession

### Minimal Setup
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

### Typical Development Setup
```python
spark = SparkSession.builder \
    .appName("MyAnalysis") \
    .master("local[*]") \
    .getOrCreate()
```

### Production Setup (YARN/EMR)
```python
spark = SparkSession.builder \
    .appName("ETLJob") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "5") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .getOrCreate()
```

---

## Master URL Options

### Local Development
```python
.master("local")        # Single thread
.master("local[2]")     # 2 threads
.master("local[*]")     # All CPU cores
.master("local[4, 2]")  # 4 cores, 2 max retries
```

### Cluster Deployment
```python
.master("spark://master-node:7077")      # Standalone cluster
.master("yarn")                          # YARN (EMR default)
.master("mesos://mesos-master:5050")     # Mesos cluster
.master("k8s://https://kmaster:443")     # Kubernetes
```

---

## Key Configuration Parameters

### Memory Settings

```python
# Executor JVM memory
.config("spark.executor.memory", "8g")  # Default 1g

# Driver JVM memory
.config("spark.driver.memory", "4g")    # Default 1g

# Overhead memory (Python processes, buffers)
.config("spark.executor.memoryOverhead", "2g")  # Default 10% of executor.memory
.config("spark.driver.memoryOverhead", "2g")

# Memory for computation
.config("spark.memory.fraction", "0.8")  # Default 0.6 (more memory for shuffles)
```

### Executor & Parallelism

```python
# Number of executor instances
.config("spark.executor.instances", "20")  # or --num-executors

# Cores per executor
.config("spark.executor.cores", "5")  # Default 1

# Shuffle partitions (critical!)
.config("spark.sql.shuffle.partitions", "500")  # Default 200
# Recommendation: data_size_GB * 4, target 128MB per partition

# Default parallelism for RDD operations
.config("spark.default.parallelism", "200")
```

### Dynamic Allocation (YARN only)

```python
.config("spark.dynamicAllocation.enabled", "true") \
.config("spark.dynamicAllocation.minExecutors", "2") \
.config("spark.dynamicAllocation.maxExecutors", "100") \
.config("spark.dynamicAllocation.initialExecutors", "10")

# Spark adds/removes executors as needed
# Saves money on cloud (scale down when idle)
```

### Optimization

```python
# Adaptive Query Execution (huge performance boost!)
.config("spark.sql.adaptive.enabled", "true") \
.config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
.config("spark.sql.adaptive.skewJoin.enabled", "true")

# Broadcast join threshold
.config("spark.sql.autoBroadcastJoinThreshold", "100MB")  # Default 10MB

# Speculation (run backup tasks for slow stragglers)
.config("spark.speculation", "true")
```

### SQL & Caching

```python
# Parquet compression
.config("spark.sql.parquet.compression.codec", "snappy")  # Default snappy

# Columnar cache compression
.config("spark.sql.inMemoryColumnarStorage.compressed", "true")

# Cache batch size
.config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
```

---

## Getting & Setting Configs at Runtime

### Get Configurations
```python
# Get specific config
spark.conf.get("spark.sql.shuffle.partitions")  # "500"

# Get all configs
all_configs = spark.sparkContext.getConf().getAll()
for key, value in all_configs:
    print(f"{key} = {value}")

# Get with default
spark.conf.get("some.config", "default_value")
```

### Set Configurations at Runtime
```python
# Set before creating SparkSession (works)
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "500") \
    .getOrCreate()

# Set after creating SparkSession (some work, some don't)
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Note: Some configs only work at initialization
# (e.g., spark.executor.memory, spark.driver.memory)
```

---

## SparkContext Access

```python
# Get SparkContext from SparkSession
sc = spark.sparkContext

# SparkContext methods
sc.getConf()  # Configuration object
sc.defaultMinPartitions  # Recommended partitions
sc.setLogLevel("ERROR")  # Set logging level

# Create RDD (rarely used in modern Spark)
rdd = sc.parallelize([1, 2, 3, 4, 5])
```

---

## Accessing Underlying Resources

```python
spark.version          # Spark version
spark.catalog          # Database/table metadata
spark.sparkContext     # SparkContext object
spark.sql             # Run SQL queries
spark.read            # Read data
```

---

## Configuration Best Practices

### Development
```python
spark = SparkSession.builder \
    .appName("dev_analysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()
```

### EMR Production
```python
spark = SparkSession.builder \
    .appName("etl_job") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "100") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()
```

### For Skewed Data
```python
spark = SparkSession.builder \
    .appName("skew_job") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .getOrCreate()
```

---

## Logging

```python
spark.sparkContext.setLogLevel("ERROR")   # ERROR, WARN, INFO, DEBUG

# View logs
# - Spark UI: http://localhost:4040 (local) or http://master:8088 (EMR)
# - Driver logs: stdout in terminal
# - Executor logs: EMR console or S3 (if event log enabled)

# Enable event log (for persistent Spark UI)
.config("spark.eventLog.enabled", "true") \
.config("spark.eventLog.dir", "s3://my-bucket/spark-logs/")
```

---

## Real-World Example

```python
from pyspark.sql import SparkSession

def get_spark_session(app_name, env="dev"):
    """Create SparkSession with environment-specific config"""

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true")

    if env == "dev":
        builder.master("local[*]")
        builder.config("spark.sql.shuffle.partitions", "50")

    elif env == "prod":
        builder.config("spark.sql.shuffle.partitions", "500")
        builder.config("spark.dynamicAllocation.enabled", "true")
        builder.config("spark.dynamicAllocation.minExecutors", "5")
        builder.config("spark.dynamicAllocation.maxExecutors", "100")
        builder.config("spark.executor.memory", "8g")
        builder.config("spark.executor.cores", "5")
        builder.config("spark.executor.memoryOverhead", "2g")
        builder.config("spark.eventLog.enabled", "true")
        builder.config("spark.eventLog.dir", "s3://my-bucket/spark-logs/")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    return spark

# Usage
spark = get_spark_session("MyETLJob", env="prod")

# Read, process, write
df = spark.read.parquet("s3://my-bucket/input/")
result = df.filter(df.amount > 100).groupBy("category").count()
result.write.mode("overwrite").parquet("s3://my-bucket/output/")
```

---

## Interview Questions

### Q: What's the difference between spark.executor.memory and spark.executor.memoryOverhead?

**Answer**:
- `spark.executor.memory`: JVM heap memory (default 1g)
- `spark.executor.memoryOverhead`: Off-heap memory for Python, buffers, NIO (default 10% of executor.memory)
- Total: executor.memory + memoryOverhead
- PySpark needs more overhead because of Python process overhead

### Q: How do you configure Spark for different environments?

**Answer**: Use builder pattern with conditional config:
```python
if env == "dev":
    .master("local[*]")
else:  # production
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.sql.shuffle.partitions", "500")
```

### Q: What happens if you set shuffle.partitions too high or too low?

**Answer**:
- **Too low** (e.g., 10): Large partitions → memory pressure, OOM, slow
- **Too high** (e.g., 10000): Too many small tasks → scheduling overhead, slow startup
- **Goldilocks** (e.g., 500): ~128MB per partition for your data size

---

## Key Takeaways

1. SparkSession is entry point to Spark
2. Configuration is builder pattern (before `.getOrCreate()`)
3. Memory configs: executor.memory, driver.memory, memoryOverhead
4. Parallelism: shuffle.partitions, executor.cores, num-executors
5. AQE is huge win: enable it in production

---

## See Also
- [Configuration Reference](../09-reference/configuration-reference.md)
- [Performance Optimization](../04-performance-optimization/)
- [Troubleshooting](../07-troubleshooting/)
