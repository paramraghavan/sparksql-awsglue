# Manual Analysis of Spark Logs: Step-by-Step Guide

## Overview

Manual analysis involves examining the Spark History Server UI and parsing through log files to identify bottlenecks,
resource waste, and optimization opportunities.

---

## Part 1: Spark History Server UI Analysis

### Step 1: Access the Application Summary

Open the Spark History Server UI (typically at `http://<history-server>:18080`) and select your application.

**What to note on the summary page:**

- **Duration** — Total runtime
- **Completed Stages** — Number of stages
- **Failed Stages** — Any failures indicate issues
- **Spark Properties** — Current configuration used

---

### Step 2: Analyze the Executors Tab

This is the most important tab for resource analysis.

**Key metrics to capture:**

| Metric                  | What It Tells You                   | Red Flags                                       |
|-------------------------|-------------------------------------|-------------------------------------------------|
| **Storage Memory Used** | How much memory is used for caching | Near max = memory pressure                      |
| **Disk Used**           | Spill to disk                       | Any value > 0 = memory issues                   |
| **Task Time**           | Time spent on tasks per executor    | Large variance = skew                           |
| **GC Time**             | Time spent in garbage collection    | > 10% of task time = heap issues                |
| **Shuffle Read/Write**  | Data shuffled                       | Very large values = shuffle optimization needed |
| **Input/Output Size**   | Data read/written                   | Compare to expected data size                   |

**What to record:**

```
Total Executors: ___
Executor Memory (each): ___
Executor Cores (each): ___
Total GC Time: ___
Total Task Time: ___
GC Time Percentage: ___ (GC Time / Task Time × 100)
Total Shuffle Read: ___
Total Shuffle Write: ___
Total Disk Spill: ___
```

**Analysis questions:**

- Is GC time > 10% of total task time? → Need more memory or fewer cores per executor
- Is there disk spill? → Need more executor memory
- Are some executors doing much more work than others? → Data skew

---

### Step 3: Analyze the Stages Tab

Click on each stage to examine task-level details.

**For each major stage, record:**

```
Stage ID: ___
Stage Name/Description: ___
Duration: ___
Tasks: ___ / ___ (succeeded/total)
Input Size: ___
Output Size: ___
Shuffle Read: ___
Shuffle Write: ___
```

**Task Metrics Summary (from the stage detail page):**

| Metric        | Min | 25th % | Median | 75th % | Max |
|---------------|-----|--------|--------|--------|-----|
| Duration      |     |        |        |        |     |
| GC Time       |     |        |        |        |     |
| Shuffle Read  |     |        |        |        |     |
| Shuffle Write |     |        |        |        |     |

**What to look for:**

1. **Task Duration Skew**
    - If Max >> Median (e.g., max is 10x median), you have data skew
    - Calculate skew ratio: `Max Duration / Median Duration`
    - Skew ratio > 3 indicates significant skew

2. **Task Count vs Parallelism**
    - If tasks = 200, you're using default `spark.sql.shuffle.partitions`
    - If tasks << total cores available, you're under-utilizing
    - If tasks are very small (< 100ms), you have too many partitions

3. **Shuffle Size per Task**
    - Ideal: 100MB - 200MB per partition
    - Too small (< 10MB): Too many partitions
    - Too large (> 1GB): Too few partitions, risk of OOM

---

### Step 4: Analyze the SQL Tab (for Spark SQL / DataFrame jobs)

Click on a query to see the physical plan and metrics.

**What to examine:**

1. **Scan Nodes**
    - `number of files read`
    - `size of files read`
    - `number of output rows`

2. **Exchange Nodes (Shuffles)**
    - `data size`
    - `number of partitions`

3. **Aggregate/Join Nodes**
    - `number of output rows`
    - Look for row count explosions (joins producing many more rows)

4. **Filter Nodes**
    - Check if filters are pushed down to scan
    - Large row reduction after scan = filter pushdown not working

**Record for each query:**

```
Query ID: ___
Duration: ___
Number of Shuffles: ___
Total Shuffle Data Size: ___
Largest Shuffle Stage: ___
Any Broadcast Joins: Yes/No
Broadcast Size: ___
```

---

### Step 5: Analyze the Environment Tab

Record the current Spark configuration:

```
spark.executor.memory: ___
spark.executor.cores: ___
spark.executor.instances: ___
spark.executor.memoryOverhead: ___
spark.driver.memory: ___
spark.sql.shuffle.partitions: ___
spark.default.parallelism: ___
spark.memory.fraction: ___
spark.memory.storageFraction: ___
spark.sql.adaptive.enabled: ___
spark.dynamicAllocation.enabled: ___
```

---

## Part 2: YARN Log Analysis

### Step 6: Get YARN Logs

```bash
# Download logs for a specific application
yarn logs -applicationId <application_id> > yarn_app_logs.txt

# Or for a specific container
yarn logs -applicationId <application_id> -containerId <container_id>
```

### Step 7: Search for Key Patterns

**Memory Issues:**

```bash
# Container killed by YARN
grep -i "killed by YARN" yarn_app_logs.txt
grep -i "Container killed on request" yarn_app_logs.txt
grep -i "beyond physical memory limits" yarn_app_logs.txt
grep -i "is running beyond virtual memory limits" yarn_app_logs.txt
```

If you find these, note:

```
Memory limit exceeded: ___
Actual memory used: ___
Configured memory: ___
```

**OOM Errors:**

```bash
grep -i "OutOfMemoryError" yarn_app_logs.txt
grep -i "Java heap space" yarn_app_logs.txt
grep -i "GC overhead limit exceeded" yarn_app_logs.txt
```

**Executor Failures:**

```bash
grep -i "ExecutorLostFailure" yarn_app_logs.txt
grep -i "executor.*lost" yarn_app_logs.txt
grep -i "Executor heartbeat timed out" yarn_app_logs.txt
```

**Shuffle Failures:**

```bash
grep -i "FetchFailedException" yarn_app_logs.txt
grep -i "shuffle.*failed" yarn_app_logs.txt
```

---

### Step 8: Analyze GC Logs (if enabled)

If GC logging is enabled (`-verbose:gc`), search for:

```bash
# Long GC pauses
grep -E "GC.*pause.*[0-9]{4,}ms" yarn_app_logs.txt

# Full GC events (bad)
grep -i "Full GC" yarn_app_logs.txt
```

Record:

```
Number of Full GCs: ___
Longest GC pause: ___
Average GC pause: ___
```

---

## Part 3: Calculate Optimal Settings

### Step 9: Memory Calculations

**Current usage analysis:**

```
A. Peak Executor Memory Used: ___ (from Executors tab)
B. Configured Executor Memory: ___
C. Memory Utilization: A/B × 100 = ___%

D. Total Disk Spill: ___ (from Executors tab)
E. Total Shuffle Data: ___
F. Spill Ratio: D/E × 100 = ___%
```

**Recommendations based on findings:**

| Finding                  | Recommendation                                                           |
|--------------------------|--------------------------------------------------------------------------|
| Memory utilization < 50% | Reduce `spark.executor.memory` or add more cores                         |
| Memory utilization > 90% | Increase `spark.executor.memory`                                         |
| Spill ratio > 0%         | Increase memory or reduce `spark.executor.cores`                         |
| Container killed by YARN | Increase `spark.executor.memoryOverhead` (try 10-15% of executor memory) |
| High GC time             | Increase memory, or reduce cores per executor                            |

---

### Step 10: Parallelism Calculations

**Calculate optimal shuffle partitions:**

```
Total Shuffle Data (largest stage): ___ GB
Target partition size: 128-200 MB

Optimal partitions = Total Shuffle Data / Target partition size
                   = ___ GB / 0.15 GB
                   = ___
```

**Calculate optimal executor count:**

```
Total Data Size: ___ GB
Target data per executor: 2-4 GB

Minimum executors = Total Data / 4 GB = ___

Total Cores Needed = Optimal partitions / 2 (for some parallelism buffer)
Cores per Executor: 4-5 (recommended)

Optimal Executors = Total Cores / Cores per Executor = ___
```

---

### Step 11: Create Optimization Checklist

Based on your analysis, fill out this checklist:

```
MEMORY OPTIMIZATION:
[ ] GC time > 10%? → Increase executor memory from ___ to ___
[ ] Disk spill present? → Increase executor memory from ___ to ___
[ ] Container killed? → Increase memoryOverhead from ___ to ___
[ ] Memory underutilized? → Reduce executor memory from ___ to ___

PARALLELISM OPTIMIZATION:
[ ] Task skew detected? → Enable AQE, consider salting keys
[ ] Tasks too small? → Reduce shuffle.partitions from ___ to ___
[ ] Tasks too large? → Increase shuffle.partitions from ___ to ___
[ ] Under-utilizing cluster? → Increase parallelism

SHUFFLE OPTIMIZATION:
[ ] Large shuffles? → Consider broadcast joins for small tables
[ ] Many shuffle stages? → Review query plan for unnecessary shuffles
[ ] Shuffle fetch failures? → Increase memory, check network

GENERAL:
[ ] Enable AQE if not enabled (Spark 3.0+)
[ ] Consider dynamic allocation for variable workloads
```

---

## Part 4: Document Findings

### Step 12: Create Analysis Report

```
APPLICATION ANALYSIS REPORT
===========================

Application ID: ___
Application Name: ___
Date Analyzed: ___
Total Duration: ___

CURRENT CONFIGURATION:
---------------------
Executors: ___
Executor Memory: ___
Executor Cores: ___
Driver Memory: ___
Shuffle Partitions: ___

RESOURCE UTILIZATION:
--------------------
Peak Memory Usage: ___% 
GC Time Ratio: ___%
Disk Spill: ___ GB
Total Shuffle: ___ GB

ISSUES IDENTIFIED:
-----------------
1. ___
2. ___
3. ___

RECOMMENDED CONFIGURATION:
-------------------------
spark.executor.memory: ___ (was: ___)
spark.executor.cores: ___ (was: ___)
spark.executor.instances: ___ (was: ___)
spark.executor.memoryOverhead: ___ (was: ___)
spark.sql.shuffle.partitions: ___ (was: ___)
spark.sql.adaptive.enabled: true

EXPECTED IMPROVEMENT:
--------------------
- Memory efficiency: ___
- Reduced GC time: ___
- Faster shuffle: ___
- Estimated runtime improvement: ___%
```

---

## Quick Reference: Common Patterns

| Symptom                  | Likely Cause                  | Solution                        |
|--------------------------|-------------------------------|---------------------------------|
| Long GC pauses, Full GCs | Heap too small                | Increase `executor.memory`      |
| Container killed         | Off-heap/overhead too small   | Increase `memoryOverhead`       |
| Disk spill > 0           | Execution memory insufficient | Increase memory or reduce cores |
| Max task >> median task  | Data skew                     | Salting, AQE, repartition       |
| Many small tasks         | Over-partitioned              | Reduce `shuffle.partitions`     |
| Few very long tasks      | Under-partitioned             | Increase `shuffle.partitions`   |
| Executor lost/timeout    | Memory or network issues      | Check memory, increase timeout  |
