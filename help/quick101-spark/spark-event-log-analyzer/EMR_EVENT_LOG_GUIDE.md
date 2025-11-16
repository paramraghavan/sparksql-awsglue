# Spark Event Log Access on AWS EMR - Complete Guide

## Default Spark Event Log Location on EMR

### where is value for spark.eventLog.dir  defined or set

The **spark-defaults.conf** file on the Spark master node is typically located in the **$SPARK_HOME**/conf/ directory.
This is the default directory where Spark expects configuration files, including spark-defaults.conf, which sets default
Spark properties like _**spark.eventLog.dir**_.

**spark-defaults.conf** is located in master node

```shell
/path/to/spark/conf/spark-defaults.conf
```

Here **/path/to/spark** is your Spark installation directory (often referred to as **SPARK_HOME**).
In managed environments like AWS EMR, this path might be similar to /etc/spark/conf/spark-defaults.conf.

### Event Log Directory

```bash
# Default spark.eventLog.dir on AWS EMR
spark.eventLog.dir = hdfs:///var/log/spark/apps

# Alternative locations depending on EMR version:
# EMR 5.x and 6.x: hdfs:///var/log/spark/apps
# Older EMR versions: hdfs:///tmp/spark-events
```

### S3 Event Log Location (If Enabled)

If you've configured S3 logging, logs will be at:

```bash
s3://your-bucket/logs/spark/
# Configure in EMR cluster settings or spark-defaults.conf
```

## Methods to Access Spark Event Logs

### Method 1: Direct HDFS Access (Recommended)

```bash
# SSH into EMR master node
ssh -i your-key.pem hadoop@<master-public-dns>

# List all event logs
hdfs dfs -ls /var/log/spark/apps

# Find specific application log
hdfs dfs -ls /var/log/spark/apps | grep <application-id>

# Copy event log to local filesystem
hdfs dfs -get /var/log/spark/apps/<application-id> ./

# Or copy directly to S3
hdfs dfs -cp /var/log/spark/apps/<application-id> s3://your-bucket/spark-logs/
```

### Method 2: Spark History Server

```bash
# Access Spark History Server UI
http://<master-public-dns>:18080

# The UI provides:
# - List of all applications
# - DAG visualization
# - Stage and task details
# - Environment variables
# - Executor information

# To download event log from History Server:
# 1. Navigate to application in UI
# 2. Click "Download Event Log" link
# Or use curl:
curl -o event-log.gz "http://<master-dns>:18080/api/v1/applications/<app-id>/logs"
```

### Method 3: S3 Logs (If Configured)

```bash
# If EMR logging to S3 is enabled, logs are automatically saved
# Configure during cluster creation:
aws emr create-cluster \
  --name "My Cluster" \
  --log-uri s3://my-bucket/emr-logs/ \
  ...

# Access logs from S3
aws s3 ls s3://my-bucket/emr-logs/cluster-id/containers/
aws s3 cp s3://my-bucket/emr-logs/cluster-id/spark/ ./ --recursive
```

### Method 4: YARN Logs

```bash
# List YARN applications
yarn application -list -appStates ALL

# Get specific application logs
yarn logs -applicationId <application_id> > app_logs.txt

# Save to file
yarn logs -applicationId application_1234567890_0001 -log_files stdout > stdout.log
yarn logs -applicationId application_1234567890_0001 -log_files stderr > stderr.log

# Get logs for specific container
yarn logs -applicationId <app_id> -containerId <container_id>
```

## Finding Your Application ID

### Method 1: From Spark UI

```python
# In your PySpark code
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
print(f"Application ID: {spark.sparkContext.applicationId}")
```

### Method 2: From YARN

```bash
# List all applications
yarn application -list -appStates ALL

# List with specific states
yarn application -list -appStates FINISHED,FAILED,KILLED
```

### Method 3: From Spark History Server

```bash
# Access History Server API
curl http://<master-dns>:18080/api/v1/applications

# Get specific app info
curl http://<master-dns>:18080/api/v1/applications/<app-id>
```

## Using the Analyzer Script

### Basic Usage

```bash
# Download event log from HDFS
hdfs dfs -get /var/log/spark/apps/application_1234567890_0001 ./

# Run analyzer
python spark_optimizer.py application_1234567890_0001 --app-id application_1234567890_0001

# Or with gzipped log
python spark_optimizer.py application_1234567890_0001.gz --app-id application_1234567890_0001
```

### From S3

```bash
# Copy from S3
aws s3 cp s3://my-bucket/spark-logs/application_1234567890_0001 ./

# Analyze
python spark_optimizer.py application_1234567890_0001 -o recommendations.json
```

### Automated Script for EMR

```bash
#!/bin/bash
# analyze_spark_job.sh

APP_ID=$1
LOG_DIR="/tmp/spark-logs"
mkdir -p $LOG_DIR

# Get event log from HDFS
echo "Fetching event log for $APP_ID..."
hdfs dfs -get /var/log/spark/apps/$APP_ID $LOG_DIR/

# Run analyzer
python3 spark_optimizer.py $LOG_DIR/$APP_ID --app-id $APP_ID

# Upload results to S3
aws s3 cp spark_recommendations.json s3://my-bucket/recommendations/$APP_ID.json
```

## Enabling and Configuring Event Logs

### In spark-defaults.conf (EMR Configuration)

```properties
# Enable event logging
spark.eventLog.enabled=true
# Set log directory (HDFS)
spark.eventLog.dir=hdfs:///var/log/spark/apps
# Or use S3
spark.eventLog.dir=s3://my-bucket/spark-event-logs/
# Compress logs to save space
spark.eventLog.compress=true
# Rolling event logs (Spark 3.x+)
spark.eventLog.rolling.enabled=true
spark.eventLog.rolling.maxFileSize=128m
```

### In EMR Cluster Configuration (at creation)

```json
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.eventLog.enabled": "true",
      "spark.eventLog.dir": "hdfs:///var/log/spark/apps",
      "spark.history.fs.logDirectory": "hdfs:///var/log/spark/apps",
      "spark.eventLog.compress": "true"
    }
  }
]
```

### In PySpark Code

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder
.appName("MyApp")
.config("spark.eventLog.enabled", "true")
.config("spark.eventLog.dir", "s3://my-bucket/spark-logs/")
.getOrCreate()
```

## Can You Use YARN Logs Instead?

### Short Answer: **Partially**

YARN logs contain:

- ✅ Application stdout/stderr
- ✅ Container logs
- ✅ Executor logs
- ✅ Driver logs
- ❌ **NOT** structured event data for analysis
- ❌ **NOT** detailed task metrics
- ❌ **NOT** DAG information

### What YARN Logs Are Good For:

1. Debugging errors and exceptions
2. Viewing application output
3. Investigating executor failures
4. Checking driver logs

### What YARN Logs Don't Have:

1. Structured JSON events
2. Detailed task metrics (shuffle, spill, GC time)
3. DAG visualization data
4. Stage-level statistics

### Example: Getting Both

```bash
#!/bin/bash
APP_ID=$1

# Get Spark event log (for metrics and optimization)
hdfs dfs -get /var/log/spark/apps/$APP_ID ./event-log/

# Get YARN logs (for debugging and errors)
yarn logs -applicationId $APP_ID > yarn-logs.txt

# Analyze Spark event log
python spark_optimizer.py ./event-log/$APP_ID

# Search for errors in YARN logs
grep -i "error\|exception\|failed" yarn-logs.txt > errors.txt
```

## EMR-Specific Tips

### 1. Event Log Retention

```bash
# Event logs in HDFS are deleted when EMR cluster terminates
# To preserve logs, copy to S3:
hdfs dfs -cp /var/log/spark/apps/* s3://my-bucket/spark-logs/

# Or configure automatic S3 logging when creating cluster
```

### 2. Accessing Terminated Cluster Logs

```bash
# If cluster is terminated, logs are in S3 (if configured)
aws s3 ls s3://my-log-bucket/cluster-id/

# Container logs
aws s3 ls s3://my-log-bucket/cluster-id/containers/

# Spark logs (if S3 logging was enabled)
aws s3 ls s3://my-log-bucket/cluster-id/spark/
```

### 3. Persistent History Server

```bash
# Set up persistent history server with S3 backend
# This allows viewing history after cluster termination

# In EMR configuration:
{
  "Classification": "spark-defaults",
  "Properties": {
    "spark.history.fs.logDirectory": "s3://my-bucket/spark-logs/",
    "spark.eventLog.dir": "s3://my-bucket/spark-logs/"
  }
}
```

### 4. Quick Analysis Command

```bash
# One-liner to analyze latest application
LATEST_APP=$(hdfs dfs -ls /var/log/spark/apps | tail -1 | awk '{print $8}' | xargs basename)
hdfs dfs -get /var/log/spark/apps/$LATEST_APP ./
python spark_optimizer.py $LATEST_APP --app-id $LATEST_APP
```

## Troubleshooting

### Event Logs Not Found

```bash
# Check if event logging is enabled
spark-submit --conf spark.eventLog.enabled=true ...

# Verify directory exists
hdfs dfs -ls /var/log/spark/apps

# Check Spark History Server configuration
cat /etc/spark/conf/spark-defaults.conf | grep eventLog
```

### Permission Issues

```bash
# Ensure proper HDFS permissions
hdfs dfs -chmod -R 777 /var/log/spark/apps

# For S3 access, check IAM role permissions
aws s3 ls s3://my-bucket/spark-logs/
```

### Large Log Files

```bash
# Use compression
spark.eventLog.compress=true

# Use rolling logs (Spark 3.0+)
spark.eventLog.rolling.enabled=true
spark.eventLog.rolling.maxFileSize=128m

# Filter large logs before analysis
hdfs dfs -cat /var/log/spark/apps/app_id | head -n 10000 > sample.log
```
