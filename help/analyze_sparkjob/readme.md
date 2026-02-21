# Manual Analysis of Spark Logs: Step-by-Step Guide

## Overview

Manual analysis involves examining the Spark History Server UI and parsing through log files to identify bottlenecks,
resource waste, and optimization opportunities.

---

## Part 1: Spark History Server UI Analysis

### Step 1: Access the Application Summary

Open the Spark History Server UI (typically at `http://<history-server>:18080`) and select your application.

* **Duration** — Total runtime.
* **Completed Stages** — Number of stages.
* **Spark Properties** — Current configuration used.

### Step 2: Analyze the Executors Tab

This is the most important tab for resource analysis.

* **Disk Used**: Indicates spill to disk; any value > 0 signifies memory issues.
* **GC Time**: Time spent in garbage collection; > 10% of task time suggests heap issues.
* **Task Time**: Time spent on tasks; large variance indicates skew.

### Step 3: Analyze the Stages Tab

Click on each stage to examine task-level details.

* **Task Duration Skew**: If Max >> Median (e.g., 10x), you have data skew.
* **Task Count**: If tasks = 200, you are likely using default `spark.sql.shuffle.partitions`.

### Step 4: The "Stuck Job" Protocol (EMR Specific)

When a job is stuck (e.g., at 99% for hours), use this checklist:

1. **Stages Tab**: Click the description of the active stage and sort the **Tasks table by Duration (Descending)**. If
   one task is vastly slower than the median, you have a straggler.
2. **Executors Tab**: Click **Thread Dump** for an active executor. Search for `BLOCKED` to find code-level hangs (e.g.,
   waiting for JDBC).
3. **Locality Level**: Check if tasks are marked `ANY`. This means the job is stuck waiting for network transfers.

---

## Part 2: YARN Log Analysis (EMR)

### Step 5: Search for Key Patterns

Run these on your EMR Master node:

```bash
# Check for S3 Throttling (Common cause of slowness on EMR)
yarn logs -applicationId <app_id> | grep -i "SlowDown"

# Check for Physical Memory Overages (Container killed by YARN)
yarn logs -applicationId <app_id> | grep -i "beyond physical memory limits"
```

### Manual Diagnostic Steps 

When you log into the Spark UI or YARN to verify these values, use this guide:

1. **Stages Tab -> Task Table**: Sort by "Duration". If the **Max** is significantly larger than the **Median**, you have data skew. This confirms you need to enable `spark.sql.adaptive.skewJoin.enabled`.
2. **Executors Tab**: Check the **Disk Used** column. Any value greater than 0 confirms your `spark.executor.memory` is too low for the current `spark.executor.cores`.
3. **Environment Tab**: Verify the actual values of `spark.sql.shuffle.partitions`. If it is at the default 200 and your job is slow, use the script's "Optimum Partition" suggestion based on total shuffle bytes.
4. **YARN Logs (EMR Master)**: Run `yarn logs -applicationId <id> | grep -i "SlowDown"` to see if S3 throttling is the cause of long runtimes rather than Spark configuration.

### Summary of Optimization Logic

* **Shuffle Partitions**: Calculated as `Total Shuffle Read / 128MB`.
* **Executor Memory**: Increased by 50% if Disk Spill or high GC is detected.
* **Executor Cores**: If utilization is low, the script recommends enabling **Dynamic Allocation** to free up unused EMR resources.



#### 1. The "Executors" Tab: Resource Efficiency

This tab is your primary source for cluster-wide health. Look for the **"Summary Metrics"** table and the **"Executors"** list.

| Row/Column | Optimal Range | High/Low (Warning) | Fix if Bottlenecked |
| --- | --- | --- | --- |
| **GC Time** | < 5% of Task Time | **High:** > 10% | Increase `spark.executor.memory` or reduce `spark.executor.cores`. |
| **Disk Used (Spill)** | 0 B | **High:** > 0 B | Increase memory; reduce cores; or increase `spark.memory.fraction`. |
| **Storage Memory** | 40% - 60% Used | **High:** > 90% | Increase memory or clear unneeded cached DataFrames. |
| **Task Time (Variance)** | Uniform across IDs | **High:** One ID >> Others | This is **Data Skew**. Check Part 5 of this guide for "Salting". |
| **Input / Shuffle Read** | Balanced GBs | **High:** > 200MB/task | Increase `spark.sql.shuffle.partitions`. |


#### 2. The "Stages" Tab: Performance Bottlenecks

Click into the **Stage Description** to see the **"Summary Metrics for Tasks"** quantile table (Min, Median, Max).

| Metric (Quantile Table) | Optimal Threshold | Red Flag | Fix if Bottlenecked |
| --- | --- | --- | --- |
| **Duration (Max vs Median)** | Max < 2x Median | **High:** Max > 5x Median | **Skew**: Enable `spark.sql.adaptive.skewJoin.enabled`. |
| **Scheduler Delay** | < 100ms | **High:** > 1s | Cluster is overloaded or YARN queue is full. Check RM logs. |
| **Locality Level** | `NODE_LOCAL` | **Low:** `ANY` | Data is moving over network. Increase `spark.locality.wait`. |
| **Shuffle Read Size** | 128MB - 200MB / task | **High:** > 1GB / task | Increase partitions to lower task memory pressure. |

---

#### 3. Calculating Optimal Configuration

Use these formulas and thresholds to determine your `--conf` overrides:

* **Executor Memory & Overhead**:
* **Logic**: If "Disk Used" > 0, your execution memory is insufficient.
* **The Fix**: Increase `spark.executor.memory` by 50%.
* **Memory Overhead**: Should be 10-15% of total executor memory to prevent YARN from killing containers (OOM).


* **Executor Cores**:
* **Optimal**: 4 to 5 cores per executor.
* **Low (< 3)**: High HDFS overhead.
* **High (> 5)**: Excessive I/O contention and GC pressure.


* **Shuffle Partitions**:
* **Optimal Calculation**: `Total Shuffle Read Data / 128MB`.
* **Current Value Check**: If you see exactly `200` partitions in the UI, it means you are using the default and likely under-partitioned for large EMR jobs.


* **Dynamic Allocation**:
* **Fix**: If executors are idle (0 active tasks) but still reserved, set `spark.dynamicAllocation.enabled=true` to release resources back to the EMR cluster.


### How to Fix Bottlenecks

| Issue | Target Configuration Fix                                                                   |
| --- |--------------------------------------------------------------------------------------------|
| **Stuck at 99%** | `--conf spark.sql.adaptive.enabled=true`, `--conf spark.sql.adaptive.skewJoin.enabled=true` |
| **Container Killed by YARN** | `--conf spark.executor.memoryOverhead=2g` (or 15% of memory)                               |
| **Constant "Spill to Disk"** | `--conf spark.executor.memory` (Increase)`, `--conf spark.memory.fraction=0.8`             |
| **S3 Throttling (SlowDown)** | Reduce parallelism or increase S3 prefix partitioning (See EMR Log section).               |


## To determine if your Spark job is suffering from specific bottlenecks 
Like 99% hangs or S3 throttling, you can use the following indicators from the Spark History Server and EMR logs.

### 1. Identifying a "Stuck at 99%" Job

A job "stuck at 99%" usually means one or a few tasks are taking significantly longer than the rest (stragglers).

* **Stages Tab Observation**: Look for a stage that has almost all tasks completed but is still "Active".
* **Task Duration Skew**: Open the specific stage and look at the **Summary Metrics for Tasks** table.
* **The Sign**: If the **Max** duration is significantly higher than the **Median** (e.g., Max > 5x Median), you have a straggler caused by data skew.


* **Executor Thread Dump**: If no tasks seem to be making progress, go to the **Executors** tab and click **Thread Dump** for an active executor. Search for `BLOCKED` threads to see if code is hanging on an external resource or deadlock.

### 2. Identifying S3 Throttling

S3 throttling happens when your Spark job makes too many requests to S3 simultaneously, exceeding the prefix limits.

* **EMR Log Search**: This cannot be seen directly in the Spark UI. You must log into your EMR Master node and search the YARN application logs.
* **The Command**: Run `yarn logs -applicationId <app_id> | grep -i "SlowDown"`.


* **The Indicator**: If you see `503 Service Unavailable` or `SlowDown` error messages in the logs, S3 is throttling your requests.

### 3. Understanding "Input / Shuffle Read Balanced GBs"

In the manual analysis guide, this metric helps you determine if your data is partitioned correctly across tasks.

* **Balanced GBs (The Goal)**: This refers to having an even distribution of data across all tasks in a stage. Ideally, each task should process a similar amount of data (e.g., 128MB to 200MB per task).
* **High: > 200MB/task (The Warning)**:
* **What it means**: If the amount of data per task is consistently higher than 200MB, your tasks are "too heavy."
* **The Risk**: Large tasks increase memory pressure, leading to long Garbage Collection (GC) pauses or "Disk Spills".
* **The Fix**: You should increase your parallelism (e.g., increase `spark.sql.shuffle.partitions`) to break the data into smaller, more manageable chunks.