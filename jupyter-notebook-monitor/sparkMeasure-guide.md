# sparkMeasure on EMR — Installation & Usage Guide

> **Who this is for:** Platform engineers setting up the cluster, and data modellers using Jupyter notebooks on the EMR master node (port 8888).

---

## Contents

1. [One-time cluster setup (platform team)](#1-one-time-cluster-setup)
2. [First cell every modeller adds](#2-first-cell-every-modeller-adds)
3. [How to measure a cell](#3-how-to-measure-a-cell)
4. [Reading the results](#4-reading-the-results)
5. [What each metric actually means](#5-what-each-metric-actually-means)
6. [Quick diagnosis table](#6-quick-diagnosis-table)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. One-time cluster setup

**Done once by the platform team. Modellers do not need to do this.**

### Option A — Bootstrap action (recommended, new clusters)

Create the file below and upload it to S3 before creating your cluster.

**`s3://your-bucket/bootstrap/install-sparkmeasure.sh`**

```bash
#!/bin/bash
set -e

# Install the Python wrapper on the master node
sudo pip3 install sparkmeasure

# Download the JAR from Maven Central
sudo mkdir -p /opt/sparkmeasure
sudo curl -L \
  "https://repo1.maven.org/maven2/ch/cern/sparkmeasure/spark-measure_2.12/0.24/spark-measure_2.12-0.24.jar" \
  -o /opt/sparkmeasure/spark-measure.jar

# Register the JAR so every Spark session picks it up automatically
echo "spark.jars /opt/sparkmeasure/spark-measure.jar" \
  | sudo tee -a /etc/spark/conf/spark-defaults.conf
```

When creating the cluster, add the bootstrap action:

```bash
aws emr create-cluster \
  --name "analytics-cluster" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --bootstrap-actions Path=s3://your-bucket/bootstrap/install-sparkmeasure.sh \
  # ... rest of your cluster config
```

---

### Option B — Running cluster (SSH once into master)

SSH into the master node and run these four commands:

```bash
sudo pip3 install sparkmeasure

sudo mkdir -p /opt/sparkmeasure
sudo curl -L \
  "https://repo1.maven.org/maven2/ch/cern/sparkmeasure/spark-measure_2.12/0.24/spark-measure_2.12-0.24.jar" \
  -o /opt/sparkmeasure/spark-measure.jar

echo "spark.jars /opt/sparkmeasure/spark-measure.jar" \
  | sudo tee -a /etc/spark/conf/spark-defaults.conf
```

> **After this:** Ask all modellers to restart their Jupyter kernels. The JAR only loads when a Spark session starts.

---

### Verify the install

On the master node:

```bash
pip3 show sparkmeasure           # should show version 0.24 or later
ls /opt/sparkmeasure/            # should show spark-measure.jar
grep sparkmeasure /etc/spark/conf/spark-defaults.conf   # should show the jar path
```

---

## 2. First cell every modeller adds

**Copy and paste this as Cell 1 of every notebook. Nothing else is required.**

```python
# ─────────────────────────────────────────────────────────────
#  sparkMeasure setup — paste this as Cell 1 of every notebook
# ─────────────────────────────────────────────────────────────
from sparkmeasure import StageMetrics
from IPython.core.magic import register_cell_magic

stagemetrics = StageMetrics(spark)   # 'spark' is already available on EMR

@register_cell_magic
def measure(line, cell):
    """Run a cell and print a performance report afterwards."""
    stagemetrics.begin()
    exec(cell, globals())
    stagemetrics.end()
    stagemetrics.print_report()

print("✅  sparkMeasure ready  — add %%measure to any cell you want to profile")
```

When this runs successfully you will see:

```
✅  sparkMeasure ready  — add %%measure to any cell you want to profile
```

---

## 3. How to measure a cell

Add `%%measure` as the very first line of any cell you want to profile.

**Without measurement (normal):**

```python
df = spark.read.parquet("s3://bucket/data/")
df.filter(df.year == 2024).groupBy("region").agg({"revenue": "sum"}).show()
```

**With measurement:**

```python
%%measure
df = spark.read.parquet("s3://bucket/data/")
df.filter(df.year == 2024).groupBy("region").agg({"revenue": "sum"}).show()
```

The cell runs exactly as before. The report appears automatically underneath the output.

---

## 4. Reading the results

After a measured cell completes, you will see a report like this:

```
Scheduling mode = FIFO
Elapsed time    = 47.3 s
numStages       = 3
numTasks        = 847

Aggregated Spark task metrics:
sum(duration)             = 6 min 12 s
sum(executorRunTime)      = 6 min 10 s
sum(executorCpuTime)      = 5 min 44 s
sum(jvmGCTime)            = 8 s
sum(shuffleWriteBytes)    = 4.2 GB       ← large: join or groupBy is expensive
sum(shuffleReadBytes)     = 4.1 GB
sum(diskBytesSpilled)     = 890 MB       ← data spilled to disk: not enough memory
sum(memoryBytesSpilled)   = 1.1 GB       ← data evicted from RAM
max(peakExecutionMemory)  = 3.8 GB
sum(resultSize)           = 2.4 KB
```

Focus on the lines marked with `←`. The others are informational.

---

## 5. What each metric actually means

| Metric | Plain English | What to do if it is high |
|---|---|---|
| **Elapsed time** | Total wall-clock time the cell took | The main number. Everything else explains *why* it was slow. |
| **numTasks** | How many parallel pieces Spark split the work into | If very low (< 10) on a big dataset, add `.repartition(200)` before heavy transforms. |
| **sum(shuffleWriteBytes)** | Data moved across the network for a join or groupBy | Use `broadcast()` for small tables. Repartition on the join key before joining. |
| **sum(diskBytesSpilled)** | Data that did not fit in RAM and was written to disk | Cache less, or ask the platform team for a larger executor memory config. |
| **sum(memoryBytesSpilled)** | Data evicted from RAM during processing | Same as above. Also check if you have an uncached DataFrame being reused. |
| **sum(jvmGCTime)** | Time the JVM spent on garbage collection | If > 10% of elapsed time, there is memory pressure. Reduce object creation or increase executor memory. |
| **max(peakExecutionMemory)** | Highest memory used at any point | Compare against executor memory (`spark.executor.memory`). If close, you are near OOM. |

---

## 6. Quick diagnosis table

Use this table to match what you see in the report to a fix.

| What you see in the report | Likely cause | Suggested fix |
|---|---|---|
| `shuffleWriteBytes` > 1 GB | Large join or groupBy without repartitioning | `df = df.repartition(200, "join_key")` before the join |
| `shuffleWriteBytes` > 1 GB on a small-table join | Missing broadcast hint | `from pyspark.sql.functions import broadcast` then `df1.join(broadcast(df2), "key")` |
| `diskBytesSpilled` > 0 | Not enough executor RAM for the operation | Cache upstream DataFrames to reduce working set, or contact platform team |
| `elapsed time` >> `executorRunTime` | Most time spent scheduling, not computing | DataFrame has too many small partitions — add `.repartition(200)` |
| Cell is slow but all spill metrics are 0 | DataFrame lineage replayed from source | Add `.cache()` after building the DataFrame, before calling actions |
| `jvmGCTime` > 5 s | Heavy object churn in Python UDFs | Replace Python UDFs with built-in Spark SQL functions where possible |
| `numTasks` is 1 or 2 on a big dataset | Data is not partitioned — single-threaded | Read with a partitioned format or add `.repartition(200)` early |

---

## 7. Troubleshooting

**`ModuleNotFoundError: No module named 'sparkmeasure'`**

The Python wrapper is not installed on the master. Ask the platform team to run:
```bash
sudo pip3 install sparkmeasure
```
Then restart your kernel.

---

**`java.lang.ClassNotFoundException: ch.cern.sparkmeasure`**

The JAR is not in the Spark classpath. Ask the platform team to verify:
```bash
grep sparkmeasure /etc/spark/conf/spark-defaults.conf
```
If that line is missing, they need to add it and you need to restart your kernel.

---

**`%%measure` magic is not defined**

Cell 1 did not run, or ran with an error. Re-run Cell 1 and check for any error messages.

---

**The report shows but all values are zero**

Your code has no Spark actions (`.show()`, `.count()`, `.collect()`, etc.). sparkMeasure only records metrics when a Spark job actually executes. Add an action to your cell.

---

**I want to save the report to compare across runs**

Add this after the `%%measure` cell:

```python
# Save metrics as a DataFrame for later comparison
metrics_df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
metrics_df.show()

# Or save to S3
stagemetrics.save_data(
    metrics_df.orderBy("jobId", "stageId"),
    "s3://your-bucket/metrics/run_001",
    "parquet"
)
```

---

*sparkMeasure is open source (Apache 2.0). Source: [github.com/LucaCanali/sparkMeasure](https://github.com/LucaCanali/sparkMeasure)*
