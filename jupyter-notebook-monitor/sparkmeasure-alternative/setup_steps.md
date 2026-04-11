# Spark Monitor Setup Guide

Complete guide to set up and use sparkmonitor on EMR with Jupyter.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Enable Event Logging](#enable-event-logging)
3. [Using sparkmonitor](#using-sparkmonitor)
4. [Analyzing Completed Jobs](#analyzing-completed-jobs)
5. [Architecture & Troubleshooting](#architecture--troubleshooting)

---

## Quick Start

### In Your Jupyter Notebook

**Cell 1: Load sparkmonitor**

```python
%run s3://your-bucket/tools/sparkmonitor.py
```

**Cell 2: Use %%measure to profile any cell**

```python
%%measure
df = spark.read.parquet("s3://your-bucket/data/")
df.groupBy("region").count().show()
```

After the cell runs, you'll see:
- 🟢 **Green** — no issues, cell ran efficiently
- 🟡 **Yellow** — warning, see the fix below
- 🔴 **Red** — serious issue, apply the fix now

---

## Enable Event Logging

Event logging is **required** for sparkmonitor to work. It allows the Spark History Server to provide real-time metrics.

### Option A: Quick Setup in Jupyter (This Cell)

Copy this into a cell and run it **before** your first Spark job:

```python
from pyspark.sql import SparkSession

# Create SparkSession with event logging enabled
spark = SparkSession.builder \
    .appName("my-data-analysis") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "s3://your-bucket/spark-events/") \
    .config("spark.eventLog.compression.codec", "snappy") \
    .getOrCreate()

# Verify it's enabled
conf = spark.sparkContext.getConf()
print("✓ Spark Configuration:")
print(f"  Event logging: {conf.get('spark.eventLog.enabled')}")
print(f"  Event log dir: {conf.get('spark.eventLog.dir')}")
print("\nSparkmonitor is ready to use! Try:")
print("  %%measure")
print("  df = spark.read.parquet('s3://...')")
print("  df.show()")
```

### Option B: Permanent Setup (EMR Bootstrap)

If you want this on every cluster, create a bootstrap script.

**File: `enable-spark-logging.sh`**

```bash
#!/bin/bash

BUCKET="your-bucket"
LOG_DIR="s3://$BUCKET/spark-events/"

# Update spark-defaults.conf
sudo tee -a /etc/spark/conf/spark-defaults.conf > /dev/null <<EOF

# Spark event logging for History Server
spark.eventLog.enabled           true
spark.eventLog.dir               $LOG_DIR
spark.eventLog.compression.codec snappy
spark.history.fs.logDirectory    $LOG_DIR
EOF

# Restart History Server to pick up new config
sudo systemctl restart spark-history-server

echo "✓ Event logging enabled to $LOG_DIR"
```

Upload to S3:

```bash
aws s3 cp enable-spark-logging.sh s3://your-bucket/scripts/
```

Create cluster with this bootstrap:

```bash
aws emr create-cluster \
  --name my-cluster \
  --bootstrap-actions Path=s3://your-bucket/scripts/enable-spark-logging.sh \
  --master-instance-type m5.xlarge \
  --slave-instance-type m5.xlarge \
  --num-core-instances 2 \
  ...
```

### Verify Event Logging is Working

Run this in Jupyter to check:

```python
import subprocess
import json

# 1. Check configuration
conf = spark.sparkContext.getConf()
print("Configuration:")
print(f"  spark.eventLog.enabled = {conf.get('spark.eventLog.enabled')}")
print(f"  spark.eventLog.dir = {conf.get('spark.eventLog.dir')}")

# 2. Run a simple job
print("\nRunning test job...")
spark.range(1000).repartition(10).count()

# 3. Check S3 for event logs
print("\nChecking S3 for event logs...")
log_dir = conf.get('spark.eventLog.dir')
result = subprocess.run(
    ["aws", "s3", "ls", log_dir, "--recursive"],
    capture_output=True, text=True
)

if result.stdout:
    print("✓ Event logs found!")
    print(result.stdout[:500])  # Show first few entries
else:
    print("✗ No event logs found. Check:")
    print(f"  • S3 bucket exists: {log_dir}")
    print("  • EMR has S3 access")
    print("  • spark.eventLog.enabled is true")

# 4. Check History Server
print("\nChecking History Server...")
try:
    app_id = spark.sparkContext.applicationId
    url = f"http://localhost:18080/api/v1/applications/{app_id}"
    with __import__('urllib.request').request.urlopen(url, timeout=5) as r:
        data = json.loads(r.read())
        print(f"✓ History Server connected!")
        print(f"  App: {data.get('name')}")
        print(f"  Status: {data.get('status')}")
except Exception as e:
    print(f"✗ History Server error: {e}")
```

---

## Using sparkmonitor

### Basic Usage

Add `%%measure` to any cell with Spark operations:

```python
%%measure
df = spark.read.parquet("s3://my-bucket/large-data/")
df = df.filter(df.year == 2024)
df = df.groupBy("region", "product").agg({"revenue": "sum"})
df.show()
```

### Understanding the Report

The report shows:

**Metrics Table**
- `Total time` — wall-clock duration
- `Parallel tasks used` — how many cores were active
- `Data read from S3` — total input size
- `Network shuffle` — data moved between nodes (join/groupBy cost)
- `Spilled to disk` — memory pressure problems
- `GC time` — Java garbage collection overhead

**Advice Cards**
- 🔴 **Red (Error)** — serious problem, fix it now
- 🟡 **Yellow (Warning)** — potential bottleneck
- 🟢 **Green (OK)** — no issues detected

### Common Issues & Fixes

#### Issue: Data Spilled to Disk 🔴

```python
# SLOW — without cache, data spills
df = spark.read.parquet("s3://...")
for i in range(10):
    print(f"Iteration {i}:", df.count())  # Reads from S3 every time

# FAST — with cache
df = spark.read.parquet("s3://...")
df = df.cache()
df.count()  # Force cache to load
for i in range(10):
    print(f"Iteration {i}:", df.count())  # Reads from memory
```

#### Issue: Large Shuffle (join/groupBy) 🟡

```python
# SLOW — shuffles all data across network
big_df = spark.read.parquet("s3://big-data/")
small_df = spark.read.parquet("s3://small-data/")
result = big_df.join(small_df, "key")  # Full shuffle!

# FAST — broadcast small table
from pyspark.sql.functions import broadcast
result = big_df.join(broadcast(small_df), "key")  # No shuffle
```

#### Issue: Few Parallel Tasks 🟡

```python
# SLOW — 5 tasks processing 50 GB
df = spark.read.parquet("s3://huge-dataset/")

# FAST — 200 tasks, much better parallelization
df = spark.read.parquet("s3://huge-dataset/")
df = df.repartition(200)
```

---

## Analyzing Completed Jobs

Use `analyze_job.py` to analyze any past job without `%%measure`.

### In Jupyter

```python
from analyze_job import analyze_job

# Analyze a specific job by ID
analyze_job("app-20240115-0001")

# Or browse recent jobs
analyze_job()  # Shows list of last 10 jobs
```

### Command Line

```bash
# Analyze a specific job
python analyze_job.py --app-id app-20240115-0001

# Show recent applications
python analyze_job.py --recent

# Custom History Server location
python analyze_job.py --app-id app-123 --host emr-master --port 18080
```

---

## Detecting ML Library Usage & Caching

If you're using scikit-learn, XGBoost, or other ML libraries that call `.fit()` on DataFrames, sparkmonitor can detect this pattern and recommend caching.

### Common ML Library Patterns

```python
%%measure
from sklearn.ensemble import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel

# Pattern 1: Converting to Pandas for sklearn
df = spark.read.parquet("s3://...")
pdf = df.toPandas()  # Spark → Pandas (triggers full read)
model = RandomForestClassifier()
model.fit(pdf, y)
# ... later ...
pdf2 = df.toPandas()  # Reads again! Should have cached.

# Pattern 2: Using Spark ML with fit()
df = spark.read.parquet("s3://...")
pipeline = Pipeline(stages=[...])
model = pipeline.fit(df)  # Expensive operation
# ... later ...
predictions = model.transform(df)  # Needs original df again
```

### Caching Strategy for ML

```python
# BEFORE ML operations, cache the DataFrame

df = spark.read.parquet("s3://your-data/")
df = df.filter(df.year == 2024)  # Apply filters first

# Cache BEFORE passing to ML
df = df.cache()
df.count()  # Force cache to load into memory

# Now ML operations use cached data
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[...])
model = pipeline.fit(df)  # Uses cached df

# Transform also uses cached data (no re-read)
predictions = model.transform(df)

# Later, if you reuse:
more_predictions = model.transform(df)  # Still uses cache
```

### SparkMonitor ML Detection Example

```python
%%measure
# This cell tells if your DataFrame should be cached

import pandas as pd
from sklearn.ensemble import RandomForestClassifier

df = spark.read.parquet("s3://data/")

# Cache recommendation: detected that you're using sklearn.fit()
# on a large DataFrame — should cache before this!
pdf = df.toPandas()
model = RandomForestClassifier()
model.fit(pdf, y)

# Later, if you do this again:
pdf2 = df.toPandas()  # ⚠️ Warning: DataFrame read twice (not cached)
```

**sparkmonitor advice for this pattern:**
```
🟡 Memory pressure detected
  Your DataFrame was converted to Pandas twice.
  If you're using ML libraries, cache the DataFrame first:

  df = df.cache()
  df.count()  # Force load

  Then pass df to model.fit() or other ML operations.
```

---

## Architecture

For details on how sparkmonitor, History Server, and YARN work together, see [ARCHITECTURE.md](ARCHITECTURE.md).

**Quick version:**
- **sparkmonitor** reads live metrics from Spark History Server (port 18080)
- **Spark History Server** reads event logs in real-time
- **YARN** (port 8088) helps locate the Spark driver if needed
- **Event logging** must be enabled for any of this to work

---

## Troubleshooting

### sparkmonitor says "could not connect"

**Solution:** Enable event logging first

```python
spark = SparkSession.builder \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "s3://bucket/spark-events/") \
    .getOrCreate()
```

### History Server shows no data

**Check:**
1. Is `spark.eventLog.enabled = true`?
2. Is the event log directory readable (S3 permissions)?
3. Is History Server running?

```bash
# SSH to master, check if running:
ps aux | grep history

# Restart if needed:
sudo systemctl restart spark-history-server
```

### Metrics are slow/delayed

Normal behavior — History Server has ~1-5 second lag. For instant metrics, sparkmonitor will try the live Spark UI if accessible.

### Cannot access History Server from remote Jupyter

Set up SSH tunnel:

```bash
ssh -L 18080:localhost:18080 hadoop@emr-master
```

Then access via `http://localhost:18080` locally.

---

## Next Steps

1. ✅ Enable event logging (above)
2. ✅ Load sparkmonitor in your Jupyter: `%run s3://bucket/sparkmonitor.py`
3. ✅ Add `%%measure` to cells you want to profile
4. ✅ Use `analyze_job()` for completed jobs
5. ✅ Apply the advice to fix performance issues

Happy analyzing! 🚀
