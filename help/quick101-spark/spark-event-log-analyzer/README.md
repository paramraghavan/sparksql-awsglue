# Spark Event Log Analyzer for AWS EMR

A comprehensive Python-based tool for analyzing Spark event logs on AWS EMR, generating DAG insights, and providing actionable optimization recommendations for both Spark configurations and PySpark code.

## 📋 Overview

This toolkit helps you:
- Parse and analyze Spark history event logs
- Identify performance bottlenecks (memory spills, shuffle issues, GC pressure, data skew)
- Get specific Spark configuration recommendations
- Receive PySpark code optimization suggestions
- Automatically detect issues and provide severity levels
- Work seamlessly with AWS EMR clusters

## 🚀 Quick Start

### Prerequisites
```bash
# Python 3.6+
python3 --version

# AWS CLI (for S3 access)
aws --version

# Optional: jq for JSON parsing
sudo yum install jq  # On EMR
```

### Installation
```bash
# Copy files to EMR master node or your local machine
scp -i your-key.pem spark_optimizer.py hadoop@<emr-master-dns>:~/
scp -i your-key.pem analyze_emr_job.sh hadoop@<emr-master-dns>:~/
chmod +x analyze_emr_job.sh
```

### Basic Usage

#### Option 1: On EMR Master Node
```bash
# SSH to EMR master
ssh -i your-key.pem hadoop@<emr-master-dns>

# Get your application ID
yarn application -list -appStates ALL

# Fetch event log from HDFS
hdfs dfs -get /var/log/spark/apps/application_1234567890_0001 ./

# Run analyzer
python3 spark_optimizer.py application_1234567890_0001 \
    --app-id application_1234567890_0001 \
    -o recommendations.json
```

#### Option 2: Using Automation Script
```bash
# On EMR master node
./analyze_emr_job.sh application_1234567890_0001

# From local machine with SSH access
./analyze_emr_job.sh -m <emr-master-dns> \
    -k ~/my-key.pem \
    -s my-results-bucket \
    application_1234567890_0001
```

#### Option 3: Analyze Local Log File
```bash
python3 spark_optimizer.py /path/to/event-log.gz \
    --app-id application_1234567890_0001
```

## 📁 Files Included

### Core Tools
- **`spark_optimizer.py`** - Main analyzer script
- **`analyze_emr_job.sh`** - Automation wrapper script
- **`EMR_EVENT_LOG_GUIDE.md`** - Complete guide for accessing logs on EMR
- **`optimized_pyspark_example.py`** - Example PySpark job with best practices

### How They Work Together
```
1. Run your PySpark job → Generates event log in HDFS
2. Use analyze_emr_job.sh → Fetches log and runs optimizer
3. Review recommendations → Apply configs and code changes
4. Run optimized job → Better performance!
```

## 🔍 What Gets Analyzed

### Metrics Tracked
- ✅ Task execution times and failures
- ✅ Memory spills (to memory and disk)
- ✅ Shuffle read/write volumes
- ✅ GC time and pressure
- ✅ Data skew detection
- ✅ Parallelism levels
- ✅ Input/output data sizes
- ✅ Stage and job durations

### Recommendations Provided
1. **Spark Configurations** - Specific settings for your workload
2. **Code Optimizations** - PySpark code improvements
3. **Severity Levels** - INFO, WARNING, or CRITICAL
4. **Observations** - What issues were detected and why

## 📊 Sample Output

```
================================================================================
SPARK EVENT LOG ANALYSIS REPORT
================================================================================

Application Metrics:
  Total Duration: 345.67 seconds
  Total Stages: 12
  Total Tasks: 1,234
  Failed Tasks: 5
  Number of Executors: 8

Memory Metrics:
  Memory Spilled: 15.3 GB
  Disk Spilled: 2.1 GB
  Peak Execution Memory: 8,192 MB

Shuffle Metrics:
  Shuffle Read: 45.6 GB
  Shuffle Write: 23.4 GB

================================================================================
RECOMMENDATIONS (Severity: WARNING)
================================================================================

Observations:
  • Memory spilling detected: 15.30 GB to memory, 2.10 GB to disk
  • High shuffle detected: 45.60 GB read, 23.40 GB written
  • Data skew detected: 62 tasks are significantly slower

Recommended Spark Configurations:
  spark.executor.memory = 16g
  spark.executor.memoryOverhead = 3g
  spark.sql.shuffle.partitions = 400
  spark.sql.adaptive.enabled = true
  spark.sql.adaptive.skewJoin.enabled = true

Code Optimization Suggestions:
  1. Consider repartitioning data to reduce memory pressure: df.repartition(200)
  2. Optimize joins: Use broadcast joins where possible
  3. Handle data skew: Add salt keys for skewed joins
  4. Reduce shuffle: Use reduceByKey instead of groupByKey
```

## 🔧 Accessing Event Logs on EMR

### Default Locations

```bash
# HDFS (default on EMR)
spark.eventLog.dir = hdfs:///var/log/spark/apps

# List all event logs
hdfs dfs -ls /var/log/spark/apps

# Get specific log
hdfs dfs -get /var/log/spark/apps/application_1234567890_0001 ./
```

### From S3 (if configured)
```bash
# If you set up S3 logging
aws s3 ls s3://my-bucket/spark-event-logs/

# Download
aws s3 cp s3://my-bucket/spark-event-logs/application_1234567890_0001 ./
```

### Using Spark History Server
```bash
# Access UI
http://<emr-master-dns>:18080

# Download via API
curl "http://<emr-master-dns>:18080/api/v1/applications/<app-id>/logs" -o log.gz
```

## 📝 Complete Workflow Example

### 1. Enable Event Logging (in your PySpark job)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyJob") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs:///var/log/spark/apps") \
    .getOrCreate()

# Your job code here...
print(f"Application ID: {spark.sparkContext.applicationId}")
```

### 2. Run Your Job
```bash
spark-submit \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///var/log/spark/apps \
    my_job.py
```

### 3. Analyze After Completion
```bash
# Note the application ID from job output
APP_ID="application_1234567890_0001"

# Run analyzer
./analyze_emr_job.sh $APP_ID
```

### 4. Apply Recommendations
```bash
# The script generates a config file
# Use it in your next run:
spark-submit \
    --properties-file spark-analysis/${APP_ID}_spark_configs.conf \
    my_job.py
```

## 🎯 Common Issues Detected

### 1. Memory Spilling
**Detected:** `Memory spilling: 15 GB to disk`  
**Recommendation:** Increase executor memory and memory overhead  
**Config:** 
```
spark.executor.memory=16g
spark.executor.memoryOverhead=3g
```

### 2. High Shuffle
**Detected:** `High shuffle: 50 GB read`  
**Recommendation:** Enable adaptive query execution, optimize joins  
**Code:** Use broadcast joins for small tables

### 3. Data Skew
**Detected:** `62 tasks significantly slower`  
**Recommendation:** Enable skew join handling, use salting  
**Code:** Add salt column to distribute data evenly

### 4. GC Pressure
**Detected:** `GC time: 15% of executor time`  
**Recommendation:** Increase memory, tune GC settings  
**Config:** Use G1GC with optimized parameters

### 5. Low Parallelism
**Detected:** `Only 5 tasks per executor`  
**Recommendation:** Increase partitions  
**Code:** `df.repartition(200)`

## 🛠️ Advanced Usage

### Analyze Multiple Applications
```bash
# Get list of recent applications
hdfs dfs -ls /var/log/spark/apps | tail -5 | while read -r line; do
    APP_ID=$(echo $line | awk '{print $8}' | xargs basename)
    ./analyze_emr_job.sh $APP_ID
done
```

### Automated Daily Analysis
```bash
# Add to cron for daily analysis of failed jobs
0 2 * * * /home/hadoop/analyze_failed_jobs.sh
```

### Compare Before/After Optimization
```bash
# Before optimization
python3 spark_optimizer.py app_before -o before.json

# After applying recommendations
python3 spark_optimizer.py app_after -o after.json

# Compare metrics
jq -s '.[0].metrics.memory_spilled_gb - .[1].metrics.memory_spilled_gb' before.json after.json
```

## 📚 Additional Resources

### Configuration References
- **EMR Event Logs:** See `EMR_EVENT_LOG_GUIDE.md`
- **Best Practices:** See `optimized_pyspark_example.py`
- **Spark Tuning:** https://spark.apache.org/docs/latest/tuning.html

### Troubleshooting

**Q: Event log not found?**  
A: Check `spark.eventLog.enabled=true` and verify HDFS directory

**Q: Can I use YARN logs instead?**  
A: YARN logs are good for debugging but lack detailed metrics. Use Spark event logs for optimization.

**Q: How long are logs retained?**  
A: HDFS logs are deleted when EMR cluster terminates. Use S3 for persistence.

**Q: Analysis takes too long?**  
A: Event logs can be large. Consider using compressed logs or sampling.

## 🔒 EMR-Specific Notes

### Instance Types
- **Memory-intensive jobs:** r5 series
- **CPU-intensive jobs:** c5 series  
- **Balanced:** m5 series

### Cluster Configuration
```json
{
  "Classification": "spark-defaults",
  "Properties": {
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "hdfs:///var/log/spark/apps",
    "spark.dynamicAllocation.enabled": "true"
  }
}
```

### Persistent History Server
For viewing logs after cluster termination:
```json
{
  "Classification": "spark-defaults",
  "Properties": {
    "spark.eventLog.dir": "s3://my-bucket/spark-logs/",
    "spark.history.fs.logDirectory": "s3://my-bucket/spark-logs/"
  }
}
```

## 📈 Best Practices

1. **Always enable event logging** in production jobs
2. **Store logs in S3** for long-term retention
3. **Analyze failed jobs** immediately to prevent recurrence
4. **Baseline your jobs** - analyze first, then optimize
5. **Monitor trends** - track improvements over time
6. **Test recommendations** in dev before production
7. **Review Spark UI** during job execution for live insights

## 🤝 Contributing

Suggestions for improvement:
- Additional metrics to track
- New optimization patterns
- EMR-specific recommendations
- Integration with monitoring tools

## 📄 License

This tool is provided as-is for analyzing Spark applications on AWS EMR.

## 📞 Support

For issues:
1. Check `EMR_EVENT_LOG_GUIDE.md` for log access help
2. Review example in `optimized_pyspark_example.py`
3. Verify Spark event logging is enabled
4. Check HDFS/S3 permissions

---

**Happy Optimizing! 🚀**
