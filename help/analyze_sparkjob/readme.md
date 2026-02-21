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