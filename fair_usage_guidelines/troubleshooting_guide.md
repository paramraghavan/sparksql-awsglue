# 🔧 EMR Spark Troubleshooting Guide

Quick solutions to common issues when running PySpark jobs on shared EMR cluster.

---

## 📋 Table of Contents
1. [Resource Allocation Issues](#resource-allocation-issues)
2. [Performance Problems](#performance-problems)
3. [Memory Errors](#memory-errors)
4. [Connection Issues](#connection-issues)
5. [Data Reading/Writing Problems](#data-readingwriting-problems)
6. [Notebook Issues](#notebook-issues)

---

## 🚨 Resource Allocation Issues

### Problem: "Application is waiting for resources"
**Symptoms**: Job stays in "ACCEPTED" state, no executors starting

**Causes**:
- Cluster is at capacity
- Other users have allocated all resources
- YARN queue is full

**Solutions**:
```python
# 1. Check cluster capacity
import requests
response = requests.get("http://localhost:8088/ws/v1/cluster/metrics")
print(response.json()["clusterMetrics"]["availableVirtualCores"])

# 2. Reduce your maxExecutors
spark = SparkSession.builder \
    .config("spark.dynamicAllocation.maxExecutors", "10")  # Lower from 20
    .getOrCreate()

# 3. Wait for resources (check YARN UI)
# Visit: http://localhost:8088/cluster/apps/RUNNING
```

**Prevention**:
- Always use dynamic allocation
- Set appropriate maxExecutors based on job needs
- Run large jobs during off-peak hours (evenings/weekends)

---

### Problem: "Container killed by YARN for exceeding memory limits"
**Symptoms**: Executors keep dying, "Container killed on request"

**Causes**:
- Executor memory + overhead > node capacity
- Memory overhead too small

**Solutions**:
```python
# Increase memory overhead
spark = SparkSession.builder \
    .config("spark.executor.memory", "6g")  # Reduced from 8g
    .config("spark.executor.memoryOverhead", "3g")  # Increased from 2g
    .getOrCreate()

# OR reduce executor cores to use less memory per executor
    .config("spark.executor.cores", "2")  # Reduced from 4
```

**Calculation**:
- Total executor memory = executor.memory + executor.memoryOverhead
- Should be < 80% of node memory (~25GB for 32GB nodes)

---

### Problem: "Dynamic allocation not working"
**Symptoms**: Executors stay at minExecutors count, never scale up

**Causes**:
- Shuffle tracking not enabled
- External shuffle service not running

**Solutions**:
```python
# Ensure both configs are set
spark = SparkSession.builder \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .getOrCreate()

# Check if external shuffle service is running (should be on EMR)
# If issues persist, contact cluster admin
```

---

## ⏱️ Performance Problems

### Problem: "Job is running very slowly"
**Symptoms**: Job takes hours when it should take minutes

**Diagnostic Steps**:
```python
# 1. Check number of partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")

# 2. Look at data skew in Spark UI
# Go to http://localhost:4040 → Stages → Look for uneven task times

# 3. Check file format
print(f"Data format: {df.sql_ctx.sql('DESCRIBE FORMATTED table_name')}")
```

**Solutions**:

**A. Too Few Partitions** (< 200 for large data):
```python
# Repartition data
df = df.repartition(200)

# Or set global shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

**B. Data Skew**:
```python
# Add salt to skewed keys
from pyspark.sql.functions import concat, lit, rand
df = df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))

# Or filter outliers before join
df_filtered = df.filter(col("key").isNotNull())
```

**C. Wrong File Format**:
```python
# Convert CSV to Parquet (5-10x faster)
df = spark.read.csv("s3://bucket/data.csv")
df.write.parquet("s3://bucket/data.parquet")

# Then read Parquet
df = spark.read.parquet("s3://bucket/data.parquet")
```

**D. Inefficient Joins**:
```python
# Use broadcast for small tables (< 10MB)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Check join plan
result.explain()
```

---

### Problem: "Stages with thousands of tasks"
**Symptoms**: Jobs have 10,000+ tasks, overwhelming scheduler

**Cause**: Too many small files or over-partitioning

**Solutions**:
```python
# 1. Coalesce output files
df.coalesce(20).write.parquet("s3://bucket/output/")

# 2. Repartition before write
df.repartition(100).write.parquet("s3://bucket/output/")

# 3. Use dynamic partition pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

---

## 💾 Memory Errors

### Problem: "OutOfMemoryError: Java heap space"
**Symptoms**: Driver crashes with OOM error

**Causes**:
- Collecting too much data to driver
- Result size exceeds driver memory

**Solutions**:
```python
# 1. Don't collect large datasets
# BAD:
data = df.collect()  # Don't do this for large data!

# GOOD:
df.write.parquet("s3://bucket/output/")  # Write to S3 instead

# 2. Increase driver memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g")  # Increased from 4g
    .config("spark.driver.maxResultSize", "4g")  # Increased from 2g
    .getOrCreate()

# 3. Sample data before collecting
sampled = df.sample(0.01)  # 1% sample
data = sampled.collect()
```

---

### Problem: "Container killed for exceeding physical memory"
**Symptoms**: Executors crash during shuffle operations

**Solutions**:
```python
# 1. Increase memory overhead
spark = SparkSession.builder \
    .config("spark.executor.memoryOverhead", "4g")  # Increased
    .getOrCreate()

# 2. Reduce partition size
spark.conf.set("spark.sql.files.maxPartitionBytes", "67108864")  # 64MB

# 3. Spill to disk instead of OOM
spark.conf.set("spark.memory.fraction", "0.6")  # Default 0.6
spark.conf.set("spark.memory.storageFraction", "0.5")  # Default 0.5
```

---

### Problem: "GC overhead limit exceeded"
**Symptoms**: Executors spend all time in garbage collection

**Solutions**:
```python
# 1. Use fewer cores per executor (more memory per core)
spark = SparkSession.builder \
    .config("spark.executor.cores", "2")  # Reduced from 4
    .config("spark.executor.memory", "8g")
    .getOrCreate()

# 2. Increase partition size to reduce objects
spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB

# 3. Use Kryo serialization (already in template)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

---

## 🔌 Connection Issues

### Problem: "Cannot connect to cluster"
**Symptoms**: Health check fails, cannot reach YARN

**Diagnostic**:
```bash
# SSH to master node
ssh hadoop@<master-node-dns>

# Check if YARN is running
sudo systemctl status hadoop-yarn-resourcemanager

# Check logs
sudo tail -f /var/log/hadoop-yarn/yarn-yarn-resourcemanager-*.log
```

**Solutions**:
1. Wait 2-3 minutes (cluster may be starting)
2. Restart notebook kernel
3. Check if cluster is in "Waiting" state in EMR console
4. Contact cluster admin if issue persists

---

### Problem: "Lost connection to Spark"
**Symptoms**: Spark session becomes unresponsive

**Solutions**:
```python
# 1. Stop and recreate session
spark.stop()
# Wait 30 seconds
spark = SparkSession.builder...getOrCreate()

# 2. Increase timeouts
spark = SparkSession.builder \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

# 3. Check if master node is healthy
import requests
response = requests.get("http://localhost:8088/cluster/cluster")
print(response.status_code)
```

---

## 📁 Data Reading/Writing Problems

### Problem: "Path does not exist: s3://bucket/path"
**Symptoms**: FileNotFoundException when reading from S3

**Solutions**:
```python
# 1. Verify path exists
import boto3
s3 = boto3.client('s3')
try:
    s3.head_object(Bucket='your-bucket', Key='your-path/file.parquet')
    print("✅ File exists")
except:
    print("❌ File not found")

# 2. Check IAM permissions
# Verify EMR cluster role has S3 read access

# 3. Use correct path format
# Correct:
df = spark.read.parquet("s3://bucket/path/")
# Incorrect:
df = spark.read.parquet("s3:/bucket/path/")  # Missing /
```

---

### Problem: "S3 write is very slow"
**Symptoms**: Writing to S3 takes hours

**Solutions**:
```python
# 1. Reduce number of output files
df.coalesce(10).write.parquet("s3://bucket/output/")

# 2. Use appropriate partition size
df.repartition(50).write.parquet("s3://bucket/output/")

# 3. Enable S3 committers for faster writes
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
    .getOrCreate()

# 4. Write in parallel by partitioning
df.write.partitionBy("date").parquet("s3://bucket/output/")
```

---

### Problem: "Failed to read Parquet file"
**Symptoms**: "Parquet column cannot be converted" error

**Solutions**:
```python
# 1. Use schema merging (slower but handles schema changes)
df = spark.read.option("mergeSchema", "true").parquet("s3://bucket/path/")

# 2. Explicitly set schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
df = spark.read.schema(schema).parquet("s3://bucket/path/")

# 3. Handle corrupt files
df = spark.read.option("mode", "DROPMALFORMED").parquet("s3://bucket/path/")
```

---

## 📓 Notebook Issues

### Problem: "Kernel keeps dying"
**Symptoms**: Jupyter kernel crashes frequently

**Solutions**:
```python
# 1. Reduce memory usage in driver
spark = SparkSession.builder \
    .config("spark.driver.memory", "2g")  # Lower if kernel crashes
    .getOrCreate()

# 2. Don't collect large datasets
# Instead of:
data = df.collect()
# Use:
df.limit(1000).show()

# 3. Clear output frequently
from IPython.display import clear_output
clear_output(wait=True)

# 4. Restart kernel and clear all outputs
# Kernel → Restart & Clear Output
```

---

### Problem: "Cannot import PySpark"
**Symptoms**: ImportError when importing pyspark

**Solutions**:
```bash
# 1. Verify PySpark is installed
pip list | grep pyspark

# 2. Check SPARK_HOME
echo $SPARK_HOME

# 3. Reinstall if needed (on master node)
pip install pyspark

# 4. Restart Jupyter kernel
```

---

## 🆘 Emergency Procedures

### Your job is hanging the cluster
```python
# 1. Get your application ID
import requests
response = requests.get("http://localhost:8088/ws/v1/cluster/apps")
apps = response.json()["apps"]["app"]
my_apps = [app for app in apps if "YourName" in app["name"]]
print([app["id"] for app in my_apps])

# 2. Kill your application
# yarn application -kill <application_id>
# (Run from terminal on master node)

# 3. Stop Spark session
spark.stop()
```

---

## 📊 Monitoring Commands

### Check Cluster Health
```python
import requests

def get_cluster_status():
    """Get current cluster metrics"""
    try:
        r = requests.get("http://localhost:8088/ws/v1/cluster/metrics")
        metrics = r.json()["clusterMetrics"]
        
        print("🖥️  Cluster Status:")
        print(f"  Available Memory: {metrics['availableMB']} MB")
        print(f"  Available VCores: {metrics['availableVirtualCores']}")
        print(f"  Running Apps: {metrics['appsRunning']}")
        print(f"  Pending Apps: {metrics['appsPending']}")
        
        # Calculate % available
        total_memory = metrics['totalMB']
        available_pct = (metrics['availableMB'] / total_memory) * 100
        print(f"  Cluster Utilization: {100 - available_pct:.1f}%")
        
    except Exception as e:
        print(f"❌ Could not fetch metrics: {e}")

get_cluster_status()
```

### Check Your Application Status
```python
def check_my_app(spark):
    """Check status of your Spark application"""
    sc = spark.sparkContext
    
    print("📱 Your Application:")
    print(f"  App ID: {sc.applicationId}")
    print(f"  App Name: {sc.appName}")
    print(f"  Spark UI: http://localhost:4040")
    
    try:
        executor_info = sc._jsc.sc().getExecutorMemoryStatus()
        num_executors = len(executor_info) - 1  # Subtract driver
        print(f"  Active Executors: {num_executors}")
    except:
        print(f"  Executors: Initializing...")

check_my_app(spark)
```

---

## 📞 When to Contact Support

Contact cluster-admin@company.com if:
- Cluster is down for > 10 minutes
- Multiple users experiencing same issue
- Persistent errors after trying solutions
- Need emergency resource allocation
- Cluster appears to be in bad state

Provide:
- Your application ID
- Screenshot of error
- Spark UI link
- Steps you've already tried

---

## 🔍 Useful Log Locations

**On Master Node (via SSH):**
```bash
# YARN logs
/var/log/hadoop-yarn/

# Spark logs
/var/log/spark/

# Application logs
yarn logs -applicationId <application_id>

# List all apps
yarn application -list
```

**In Jupyter:**
```python
# View Spark logs
sc = spark.sparkContext
print(sc._jvm.org.apache.log4j.LogManager.getRootLogger().getAllAppenders())
```

---

## ✅ Pre-flight Checklist

Before running any Spark job, verify:

- [ ] Dynamic allocation is enabled
- [ ] maxExecutors is appropriate for job size
- [ ] Cluster health check passes
- [ ] Sufficient resources available
- [ ] Input data paths are correct
- [ ] Output paths are writable
- [ ] You've budgeted time for the job

**Remember**: Most issues are resolved by reducing resource requests, repartitioning data, or using better file formats!

---

**Last Updated**: January 2026
**Need Help?** data-team@company.com | #data-engineering on Slack
