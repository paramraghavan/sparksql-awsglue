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