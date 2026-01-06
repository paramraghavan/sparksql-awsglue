# EMR Jupyter Notebook Guidelines for Data Scientists

## 🎯 Purpose
This guide helps you run PySpark jobs efficiently on our shared EMR cluster without causing resource conflicts or impacting other team members.

---

## ⚠️ Quick Start Checklist

Before running any Spark job:
- ✅ Use **Dynamic Allocation** (no fixed executor counts)
- ✅ Add cluster health checks to your notebook
- ✅ Set appropriate memory and core limits
- ✅ Monitor your job's resource usage

---

## 📋 Standard Notebook Template

Copy and paste this at the **beginning of every Jupyter notebook**:

```python
import os
import sys
from pyspark.sql import SparkSession
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# ============================================
# SECTION 1: CLUSTER HEALTH CHECK
# ============================================

def check_cluster_health():
    """
    Check if EMR cluster is healthy before starting Spark job
    Returns: (is_healthy, message)
    """
    try:
        # Get EMR cluster ID from environment
        cluster_id = os.environ.get('CLUSTER_ID', 'j-XXXXXXXXXXXXX')
        
        # Check if master node is responding
        master_url = "http://localhost:8088/cluster/cluster"  # YARN ResourceManager
        response = requests.get(master_url, timeout=10)
        
        if response.status_code == 200:
            print("✅ Cluster is healthy and responding")
            return True, "Cluster OK"
        else:
            return False, f"Cluster returned status code: {response.status_code}"
            
    except requests.exceptions.RequestException as e:
        return False, f"Cannot reach cluster: {str(e)}"
    except Exception as e:
        return False, f"Health check failed: {str(e)}"

def send_alert_email(subject, message, recipient="your-team@company.com"):
    """
    Send email alert if cluster has issues
    """
    try:
        # Configure your SMTP settings
        sender = "emr-alerts@company.com"
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        
        # Send email (configure your SMTP server)
        # smtp = smtplib.SMTP('your-smtp-server.com', 587)
        # smtp.sendmail(sender, [recipient], msg.as_string())
        # smtp.quit()
        
        print(f"📧 Alert sent: {subject}")
    except Exception as e:
        print(f"⚠️ Could not send email: {e}")

# Run health check
print("🔍 Checking cluster health...")
is_healthy, health_msg = check_cluster_health()

if not is_healthy:
    print(f"❌ CLUSTER ISSUE: {health_msg}")
    send_alert_email("EMR Cluster Health Alert", health_msg)
    raise Exception(f"Cluster is not healthy: {health_msg}")

print("=" * 60)

# ============================================
# SECTION 2: SPARK SESSION WITH DYNAMIC ALLOCATION
# ============================================

# IMPORTANT: Always use these configurations!
spark = SparkSession.builder \
    .appName(f"YourName_JobDescription_{datetime.now().strftime('%Y%m%d_%H%M')}") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.dynamicAllocation.initialExecutors", "2") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session created successfully!")
print(f"📊 App Name: {spark.sparkContext.appName}")
print(f"🔗 Spark UI: http://localhost:4040")
print("=" * 60)
```

---

## 🔧 Understanding the Configuration

### **Dynamic Allocation (CRITICAL!)**
```python
.config("spark.dynamicAllocation.enabled", "true")
```
- **Why**: Allows Spark to automatically add/remove executors based on workload
- **Benefit**: No resource hogging! Executors are released when idle

### **Executor Limits**
```python
.config("spark.dynamicAllocation.minExecutors", "1")
.config("spark.dynamicAllocation.maxExecutors", "20")
```
- **minExecutors**: Start small (1-2)
- **maxExecutors**: Your fair share = 100 task nodes ÷ expected concurrent users
  - If 5 users typically run jobs: max = 20 executors per user
  - Adjust based on your team size

### **Resource Sizing**
```python
.config("spark.executor.memory", "8g")
.config("spark.executor.cores", "4")
```
- **executor.memory**: 8g is good for most analytics tasks
- **executor.cores**: 4-5 cores per executor is optimal
- **Rule**: Each task node has ~32GB RAM and 8 cores
  - 1 executor per node with these settings

---

## 📊 Resource Allocation Guide

### **For Different Job Types:**

#### 🔹 Small Exploratory Analysis (< 10 GB data)
```python
.config("spark.dynamicAllocation.maxExecutors", "10")
.config("spark.executor.memory", "4g")
.config("spark.executor.cores", "2")
```

#### 🔹 Medium ETL Jobs (10-100 GB data)
```python
.config("spark.dynamicAllocation.maxExecutors", "20")
.config("spark.executor.memory", "8g")
.config("spark.executor.cores", "4")
```

#### 🔹 Large Processing Jobs (> 100 GB data)
```python
.config("spark.dynamicAllocation.maxExecutors", "40")
.config("spark.executor.memory", "12g")
.config("spark.executor.cores", "4")
```

---

## 🚨 Best Practices

### **DO's ✅**

1. **Always use Dynamic Allocation** - Never set a fixed number of executors
2. **Use descriptive app names** - Include your name and date
3. **Run health check** - Before starting your Spark session
4. **Monitor your job** - Check Spark UI at http://localhost:4040
5. **Stop your session** - Always run `spark.stop()` when done
6. **Cache wisely** - Only cache DataFrames you'll reuse multiple times
7. **Release caches** - Run `df.unpersist()` when done with cached data

### **DON'Ts ❌**

1. **DON'T use static allocation** - `.config("spark.executor.instances", "50")` ❌
2. **DON'T request all resources** - Leave room for others
3. **DON'T keep idle sessions** - Stop Spark when not in use
4. **DON'T cache everything** - Memory is shared
5. **DON'T ignore errors** - Check logs if jobs fail

---

## 🔍 Monitoring Your Job

Add this cell to monitor your Spark application:

```python
def get_application_stats():
    """
    Display current application resource usage
    """
    sc = spark.sparkContext
    
    print("📊 Current Application Stats")
    print("=" * 60)
    print(f"Application ID: {sc.applicationId}")
    print(f"Application Name: {sc.appName}")
    print(f"Spark UI: http://localhost:4040")
    print(f"Default Parallelism: {sc.defaultParallelism}")
    
    # Try to get executor info
    try:
        status = sc.statusTracker()
        executor_info = sc._jsc.sc().getExecutorMemoryStatus()
        print(f"Active Executors: {len(executor_info)}")
    except:
        print("Executor info not available yet")
    
    print("=" * 60)

# Call this to check your job status
get_application_stats()
```

---

## 🛑 Shutting Down Gracefully

**Always add this at the end of your notebook:**

```python
# Clean up cached data
try:
    spark.catalog.clearCache()
    print("✅ Cache cleared")
except:
    pass

# Stop Spark session
try:
    spark.stop()
    print("✅ Spark session stopped")
    print("♻️ Resources released back to the cluster")
except:
    pass
```

---

## 🔔 Advanced: Automatic Cluster Monitoring

Add this cell to periodically check cluster health:

```python
import time
from IPython.display import clear_output

def monitor_cluster_continuous(duration_minutes=60, check_interval_seconds=300):
    """
    Monitor cluster health for specified duration
    duration_minutes: How long to monitor (default 60 min)
    check_interval_seconds: Check every N seconds (default 5 min)
    """
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    while time.time() < end_time:
        clear_output(wait=True)
        
        is_healthy, msg = check_cluster_health()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"🕐 Cluster Health Check - {current_time}")
        print("=" * 60)
        
        if is_healthy:
            print(f"✅ Status: HEALTHY")
        else:
            print(f"❌ Status: UNHEALTHY - {msg}")
            send_alert_email(
                f"EMR Cluster Alert - {current_time}",
                f"Cluster health issue detected:\n{msg}"
            )
        
        print(f"\n⏳ Next check in {check_interval_seconds} seconds...")
        print(f"⏹️ Monitoring will stop in {int((end_time - time.time()) / 60)} minutes")
        
        time.sleep(check_interval_seconds)

# Run in background while your job executes
# Uncomment to use:
# monitor_cluster_continuous(duration_minutes=30, check_interval_seconds=300)
```

---

## 📞 Getting Help

### Cluster Information
- **Cluster ID**: j-XXXXXXXXXXXXX (check with your admin)
- **Master Node**: ec2-xx-xx-xx-xx.compute.amazonaws.com
- **YARN ResourceManager**: http://localhost:8088
- **Spark History Server**: http://localhost:18080

### When You Need More Resources
If your job legitimately needs more resources:
1. Document why you need them
2. Contact: [cluster-admin@company.com]
3. Consider running during off-peak hours (evenings/weekends)

### Common Issues

**Problem**: "Job is slow"
- ✅ Check if you're using dynamic allocation
- ✅ Reduce data shuffling with proper partitioning
- ✅ Use appropriate file formats (Parquet > CSV)

**Problem**: "Out of Memory error"
- ✅ Increase `spark.executor.memoryOverhead`
- ✅ Reduce `spark.executor.cores` to 2-3
- ✅ Repartition your data: `df.repartition(200)`

**Problem**: "Resources not available"
- ✅ Wait a few minutes for resources to free up
- ✅ Check if other users have idle sessions
- ✅ Use smaller maxExecutors value

---

## 📝 Summary

1. **Copy the template** at the top of this guide
2. **Paste it in every notebook** before running Spark code
3. **Customize app name** with your name and job description
4. **Adjust maxExecutors** based on your job size
5. **Always call `spark.stop()`** when done
6. **Monitor cluster health** if running long jobs

---

**Questions?** Contact: [your-data-team@company.com]

**Last Updated**: January 2026

---

Remember: We all share this cluster! 🤝 Following these guidelines ensures everyone can work efficiently.
