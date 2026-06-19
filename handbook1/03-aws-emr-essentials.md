# 03 - AWS EMR Essentials

## What is AWS EMR?

**EMR = Elastic MapReduce** - AWS service for running big data frameworks (Spark, Hadoop, Hive, Flink).

**Simple:** You provide code + data location. EMR creates a cluster, runs the job, destroys the cluster. You pay only for what you use.

---

## Table of Contents
1. [Cluster Basics](#cluster-basics)
2. [Instance Types & Sizing](#instance-types--sizing)
3. [Spark-Submit Configuration](#spark-submit-configuration)
4. [Creating Your First Cluster](#creating-your-first-cluster)
5. [Cost Optimization](#cost-optimization)
6. [Troubleshooting](#troubleshooting)

---

## Cluster Basics

### What is an EMR Cluster?

```
Master Node (1)
├─ Driver (runs your code)
├─ Resource Manager
└─ HDFS NameNode

Core Nodes (usually 2-10)
├─ Executors (run tasks)
├─ HDFS DataNodes
└─ Storage for intermediate data

Task Nodes (optional, can scale up/down)
└─ Additional executors (spot instances for cost savings)
```

### Master vs Core vs Task Nodes

| Node Type | Purpose | Cost | Scalability |
|-----------|---------|------|-------------|
| **Master** | Driver, scheduler, monitoring | On-Demand only | 1 node (fixed) |
| **Core** | Executors + HDFS storage | On-Demand or Spot | 1-10 nodes (fixed during job) |
| **Task** | Additional executors only | Spot usually | Scale up/down dynamically |

### Key Components

```python
# When you create an EMR cluster with Spark:
1. Hadoop - Distributed file system & storage
2. Spark - Data processing engine
3. YARN - Resource manager (allocates CPU/memory to jobs)
4. Hive - SQL interface (optional)
5. Presto - Fast SQL queries (optional)
```

---

## Instance Types & Sizing

### Instance Families

```
General Purpose (m5, m6)
├─ Balanced CPU/memory
├─ Good for most workloads
└─ Example: m5.2xlarge (8 CPU, 32GB RAM)

Memory Optimized (r5, r6)
├─ More RAM (good for caching, joins)
├─ Example: r5.2xlarge (8 CPU, 64GB RAM)
└─ Cost: ~30% more than m5

Compute Optimized (c5, c6)
├─ More CPU (good for transformations)
├─ Example: c5.2xlarge (8 CPU, 16GB RAM)
└─ Tradeoff: Less memory

Storage Optimized (i3, d2)
├─ NVMe SSD or HDD
├─ Good for caching, sorting
└─ Example: i3.2xlarge (8 CPU, 64GB RAM, 1.9TB NVMe)
```

### Choosing Instance Type

```
Processing heavy transformations? → c5.2xlarge
Large in-memory joins? → r5.4xlarge
Mixed workload? → m5.2xlarge
Cost-sensitive? → m5.xlarge + m5.large task nodes
```

### CPU and Memory Mapping

```python
# m5.2xlarge instance breakdown:
# 8 CPU cores, 32GB RAM

# Default Spark allocation per instance:
executor_memory = 32 * 0.6  # ~19GB per executor
executor_cores = 8 - 1  # 7 cores (1 reserved for OS)

# With 5 core nodes + 1 master:
# Total executors: 5 nodes × 2 executors = 10 executors
# Total cores: 10 executors × 7 cores = 70 cores
# Total memory: 10 executors × 19GB = 190GB

print("Cluster capacity: 70 cores, 190GB")
```

---

## Spark-Submit Configuration

### Basic Spark-Submit Structure

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 20G \
  --driver-memory 4G \
  s3://my-bucket/my_script.py
```

### Parameter Breakdown

```
--master yarn                  # Use YARN (EMR's resource manager)
--deploy-mode cluster          # Run driver on cluster (not local machine)
--num-executors 10            # Number of executor JVMs
--executor-cores 5            # CPU cores per executor (1-8)
--executor-memory 20G         # RAM per executor
--driver-memory 4G            # RAM for driver (on master node)
--conf spark.default.parallelism=100  # Default partitions
--conf spark.sql.shuffle.partitions=100  # Shuffle partitions
```

### Real-World Configuration Examples

#### Small Cluster (Development)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 3 \
  --executor-cores 4 \
  --executor-memory 16G \
  --driver-memory 4G \
  s3://my-bucket/script.py

# Cluster: 4 nodes (1 master + 3 core), m5.xlarge
# Cost: ~$1.50/hour
# Processing: Up to 1GB files, quick turnaround
```

#### Medium Cluster (Production)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 20G \
  --driver-memory 8G \
  --conf spark.default.parallelism=200 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.shuffle.partitions=200 \
  s3://my-bucket/script.py

# Cluster: 11 nodes (1 master + 10 core), m5.2xlarge
# Cost: ~$8/hour
# Processing: 100GB files, complex joins, aggregations
```

#### Large Cluster (Scale Processing)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 50 \
  --executor-cores 8 \
  --executor-memory 30G \
  --driver-memory 16G \
  --conf spark.default.parallelism=500 \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.executor.memoryOverhead=3G \
  s3://my-bucket/script.py

# Cluster: 51 nodes (1 master + 50 core), r5.4xlarge
# Cost: ~$80/hour
# Processing: 1TB+ files, heavy aggregations, multiple joins
```

---

## Creating Your First Cluster

### Using AWS CLI

```bash
# Create a simple EMR cluster
aws emr create-cluster \
  --name "PySpark-Demo" \
  --release-label emr-6.10.0 \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --ec2-key-name my-key \
  --applications Name=Spark Name=Hadoop \
  --log-uri s3://my-bucket/emr-logs/ \
  --no-termination-protected

# Returns: cluster ID like j-1K2H3L4M5N6
```

### Using AWS Console (Beginner-Friendly)

1. Go to **EMR Dashboard**
2. Click **Create Cluster**
3. Set these values:
   - **Cluster name:** my-spark-cluster
   - **Release:** emr-6.10.0
   - **Applications:** Spark, Hadoop (default)
   - **Instance type:** m5.xlarge
   - **Number of instances:** 3
   - **Key pair:** select your EC2 key
4. Click **Create Cluster**
5. Wait 5-10 minutes for cluster to be ready

### Checking Cluster Status

```bash
# Get cluster details
aws emr describe-cluster --cluster-id j-1K2H3L4M5N6

# Check if cluster is running
aws emr describe-cluster --cluster-id j-1K2H3L4M5N6 \
  --query 'Cluster.Status.State' \
  --output text
# Output: RUNNING (or WAITING)
```

### Submitting a Job

```bash
# Step 1: Copy script to S3
aws s3 cp my_script.py s3://my-bucket/scripts/

# Step 2: Submit job to cluster
aws emr add-steps \
  --cluster-id j-1K2H3L4M5N6 \
  --steps Type=spark,Name="MyJob",ActionOnFailure=CONTINUE,Args=[s3://my-bucket/scripts/my_script.py]

# Step 3: Check job status
aws emr describe-step \
  --cluster-id j-1K2H3L4M5N6 \
  --step-id s-1K2H3L4M5N6

# Status: PENDING → RUNNING → COMPLETED (or FAILED)
```

### Terminating the Cluster

```bash
# Delete cluster (stops all running jobs!)
aws emr terminate-cluster --cluster-id j-1K2H3L4M5N6

# Verify termination
aws emr describe-cluster --cluster-id j-1K2H3L4M5N6 \
  --query 'Cluster.Status.State'
# Output: TERMINATING → TERMINATED
```

---

## Cost Optimization

### On-Demand vs Spot Instances

```
ON-DEMAND
├─ Cost: Full price (~$0.50/hour for m5.xlarge)
├─ Reliability: 99.95% uptime
└─ Use for: Master nodes, critical workloads

SPOT INSTANCES
├─ Cost: ~70% discount (~$0.15/hour for m5.xlarge)
├─ Reliability: Can be interrupted with 2-minute warning
└─ Use for: Task nodes, non-critical processing, scale-out jobs
```

### Cost-Saving Strategy

```bash
# Master: On-Demand (always running)
# Core: Mix of On-Demand + Spot
# Task: All Spot (scale dynamically)

aws emr create-cluster \
  --name "Cost-Optimized" \
  --instance-type m5.xlarge \
  --instance-count 4 \
  --master-instance-type m5.xlarge \
  --core-instance-type m5.xlarge \
  --core-instance-count 2 \
  --task-instance-type m5.xlarge \
  --task-instance-count 2 \
  --task-price 0.15 \
  --bid-price 0.15 \
  --release-label emr-6.10.0

# Cost breakdown:
# Master (1 m5.xlarge on-demand): $0.50/hour
# Core (2 m5.xlarge on-demand): $1.00/hour
# Task (2 m5.xlarge spot): $0.30/hour
# Total: ~$1.80/hour (vs $2.50 all on-demand)
```

### Scaling to Reduce Cost

```bash
# Auto-scaling: Start with 2 nodes, scale to 10 if needed

aws emr create-cluster \
  --name "Auto-Scaling" \
  --release-label emr-6.10.0 \
  --instance-type m5.xlarge \
  --instance-count 2 \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --service-role EMR_DefaultRole

# With auto-scaling:
# Small job: 2 nodes = $1/hour
# Large job: 10 nodes = $5/hour
# (Automatically scales up/down based on queue)
```

### S3 Optimization

```python
# Store data in S3, not on cluster
df = spark.read.parquet("s3://my-bucket/data/2024/")

# Don't write to HDFS (cluster ephemeral storage!)
# DO write to S3 (persists after cluster deletion)
df.write.parquet("s3://my-bucket/output/")

# S3 costs: ~$0.023/GB/month (vs EC2 storage ~$0.10/GB/month)
# Plus: Data persists even after cluster is deleted!
```

---

## Troubleshooting

### Common Issues

#### 1. OutOfMemory Errors

```
ERROR: java.lang.OutOfMemoryError: Java heap space

Solution:
--executor-memory 16G → --executor-memory 24G
--num-executors 10 → --num-executors 5
```

#### 2. Task Failure - Executor Lost

```
ERROR: Lost executor 1 on ... (OOM)

Solution:
Increase executor memory OR
Reduce num-executors (each gets more memory)
```

#### 3. Slow Jobs

```
Check logs:
aws emr ssh --cluster-id j-xxx -i my-key.pem

On master:
tail -f /mnt/var/log/spark/apps/spark-<app-id>.log
```

#### 4. S3 Timeout

```
ERROR: Timeout reading from S3

Solution:
--conf spark.hadoop.fs.s3a.connection.timeout=120000 \
--conf spark.hadoop.fs.s3a.socket.timeout=120000 \
```

### Monitoring

```bash
# Check cluster status
aws emr describe-cluster --cluster-id j-xxx

# Get step status
aws emr describe-step --cluster-id j-xxx --step-id s-xxx

# SSH into master node
aws emr ssh --cluster-id j-xxx -i my-key.pem

# View Spark UI (on master)
# Browser: http://<master-public-ip>:8080/
```

---

## Real-World Example: Complete ETL Job

```python
# my_etl_script.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, sum as spark_sum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("DailyETL").getOrCreate()

try:
    logger.info("Starting ETL pipeline")

    # Read raw data from S3
    df = spark.read.parquet(
        "s3://my-bucket/raw/sales/2024-01-01/"
    )
    logger.info(f"Read {df.count()} records")

    # Clean data
    df_clean = df.filter(col("amount") > 0)\
        .dropDuplicates(["transaction_id"])

    # Transform
    df_transformed = df_clean.withColumn(
        "date",
        from_unixtime(col("timestamp"), "yyyy-MM-dd")
    ).groupBy("date", "store_id")\
    .agg(spark_sum("amount").alias("daily_total"))

    # Write to S3
    df_transformed.write.mode("overwrite").parquet(
        "s3://my-bucket/processed/daily_summary/"
    )

    logger.info("ETL completed successfully")

except Exception as e:
    logger.error(f"ETL failed: {e}")
    raise
finally:
    spark.stop()
```

### Submit the Job

```bash
# Using CLI
aws emr add-steps \
  --cluster-id j-xxx \
  --steps Type=spark,\
Name="DailyETL",\
ActionOnFailure=CONTINUE,\
Args=[--class,ETL,s3://my-bucket/my_etl_script.py]

# Or using spark-submit directly
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 5 \
  --executor-cores 4 \
  --executor-memory 16G \
  s3://my-bucket/my_etl_script.py
```

---

## Key Takeaways

✅ **Master = Driver, Cores = Executors** - Understand the topology
✅ **Right-size instances** - Balance cost vs performance
✅ **Use Spot for scale** - Save 70% on task nodes
✅ **S3 is permanent storage** - Don't rely on cluster storage
✅ **Monitor and log** - Catch issues before production
✅ **Test configuration locally** - Then scale to cluster

---

## Next Steps

1. **Create your first cluster** (2-5 nodes, test)
2. **Submit a simple job** (count records, basic transformation)
3. **Monitor execution** (check logs, execution time)
4. **Move to Section 04** - Memory optimization

---

**Remember:** EMR costs money! Delete clusters when done, use spot instances, and monitor job execution times.
