The error you’re seeing while reading a large Parquet file (50 GB) from S3 in Spark is:

`Py4JJavaError: An error occurred while calling o553.count. org.apache.spark.SparkException: Job aborted due to stage failure: Exception while gettin... at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages`

**Possible causes and solutions:**

**1. Out Of Memory (OOM) / Executor Failure**

- Large counts trigger a full scan across all partitions and can easily exhaust executor memory on big files.
- **Solution:** Increase `spark.executor.memory` and `spark.driver.memory`. Try using more/smaller partitions:
  ```python
  df1 = df1.repartition(200)  # or higher partition count
  ```

***

**2. Number of Partitions / Shuffle Issues**

- If your file is too few/large partitions, tasks may crash on big data, especially with default settings.
- **Solution:** Set:
    - `spark.sql.shuffle.partitions` (e.g., 200–500)
    - `spark.default.parallelism` to a similar value

***

**3. S3/Filesystem Errors**

- Unstable connections, permissions, or throttling with S3 can abort jobs.
- **Solution:**
    - Check S3 permissions and reliability.
    - Set S3 configs for retries/timeouts:
      ```
      spark.hadoop.fs.s3a.connection.maximum=100
      spark.hadoop.fs.s3a.connection.timeout=500ms
      spark.hadoop.fs.s3a.retry.limit=10
      ```

***

**4. Corrupt or Incompatible Parquet Files**

- Even a single corrupt file/row can kill a Spark job.
- **Solution:**
    - Test reading a subset (e.g., a single file from your S3 path)
    - Use `.show(10)` or `.limit(10).collect()` before `.count()` to debug

***

**5. Skewed Data / Problematic Row Groups**

- Highly skewed data in partitions can cause tasks to fail.
- **Solution:**
    - Repartition DataFrame if feasible (`df1 = df1.repartition(200)`)

***

**6. Spark Version / Compatibility**

- Issues can arise with incompatible library/codec versions or PyArrow misconfigurations when reading Parquet.
- **Solution:**
    - Ensure consistent Spark/PyArrow versions across cluster nodes

***

**7. Cluster Resource Constraints**

- Ensure enough nodes, cores, and RAM for 50GB file.
- EMR: Check instance size/count.

***

**Best troubleshooting steps:**

- Try reading just a single partition/file from S3 first.
- Run `df1.show(10)` or `df1.take(10)`—if this fails, error is earlier (not just count).
- Check detailed Spark logs for a “Caused by” message indicating OOM, S3, or codec issues.
- Test with increased memory and partition count.
- Review S3 permissions and job IAM roles.

***

## Spark config

Here are **sample Spark configs and troubleshooting steps for EMR/S3 Parquet jobs** with large files:

***

**Recommended Spark Configurations for Large Parquet File Jobs (EMR/S3):**

```python
# Python's SparkSession builder example
spark = (
    SparkSession.builder
    .appName("Large Parquet S3 Read")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.default.parallelism", "400")
    .config("spark.executor.memory", "8g")  # or higher if nodes allow
    .config("spark.driver.memory", "8g")  # or higher
    .config("spark.executor.cores", "4")  # tune according to cluster size
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.hadoop.fs.s3a.connection.maximum", "100")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.retry.limit", "10")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .getOrCreate()
)
```

**Best Practices for Parquet Reads from S3:**

- **Increase partition count** before action:
  ```python
  df = spark.read.parquet("s3a://bucket/path/")
  df = df.repartition(400)  # Make sure each partition is not enormous
  ```
- **Try sampling or show before full count:**
  ```python
  df.show(10)
  df.limit(1000).count()
  ```
- **Check S3 permissions and network reliability.**
- **Verify Spark/Parquet version compatibility (all nodes/EMR/driver should match).**

***

**Log Analysis Tips:**

- Look in the Spark stderr/stdout logs for `Caused by:` line after your stack trace.
    - If OOM, see memory error or gc overhead.
    - If S3, may see 403, throttling, or timeout messages.

***

**Action Plan:**

1. **Start with a small read/sample.**
2. **Adjust memory and partition configs as above.**
3. **Check Spark logs on your EMR cluster for the root exception (most informative is often several lines
   after `Stage failed`).**
4. **If one specific file fails, corruption or schema drift may be present—test reading S3 subfiles directly.**
5. **Optionally enable dynamic allocation for large jobs.**

# **Immediate Diagnostics**

First, check the **full error message** in your Spark UI (usually port 4040) or YARN logs to see the actual cause:

- Look for "OutOfMemoryError"
- Check for "lost executor" messages
- See if there are S3 connection timeouts

## Common Solutions

### 1. **Increase Executor Memory**

```python
spark = SparkSession.builder
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memoryOverhead", "2g")
    .getOrCreate()
```

### 2. **Repartition After Reading** (Most Effective)

```python
# Read and immediately repartition to increase parallelism
df = spark.read.parquet("s3://bucket/file.parquet")
    .repartition(200)  # Adjust based on cluster size

count = df.count()
```

### 3. **Read with Specific Columns** (if you don't need all)

```python
df = spark.read.parquet("s3://bucket/file.parquet")
    .select("col1", "col2")  # Only columns you need
```

### 4. **Increase Partitions During Read**

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB chunks
```

### 5. **S3 Optimization**

```python
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
spark.conf.set("spark.hadoop.fs.s3a.threads.max", "50")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
```

### 6. **Add Retry Logic for S3**

```python
spark.conf.set("spark.hadoop.fs.s3a.retry.limit", "10")
spark.conf.set("spark.hadoop.fs.s3a.connection.timeout", "600000")
```

## Quick Fix to Try First

```python
# Most likely to work for 50 GB file
df = spark.read.parquet("s3://bucket/file.parquet").repartition(400)
count = df.count()
```

**What partition count?** Rule of thumb: `(file_size_gb * 2)` to `(file_size_gb * 10)` partitions. For 50 GB, try
200-400 partitions.
