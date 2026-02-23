# Spark Application Manual Analysis Guide

> A step-by-step guide based on the actual Spark History Server UI

---

## Part 1: Executor Tab Analysis

### Step 1: Understand the Summary Section

The Executor tab shows a **Summary** table at the top with aggregated metrics:

| Row | Description |
|-----|-------------|
| **Active(N)** | Currently running executors |
| **Dead(N)** | Executors that have terminated |
| **Total(N)** | Sum of active + dead |

**Summary Columns:**

| Column | What It Shows | What to Look For |
|--------|---------------|------------------|
| **RDD Blocks** | Cached RDD partitions | Usually 0 unless you're caching |
| **Storage Memory** | Memory used/available for caching (e.g., "0.0 B / 3.9 TiB") | If "0 B / X TiB", no caching is happening |
| **Disk Used** | Spill to disk | **Any value > 0 is bad** - indicates memory pressure |
| **Cores** | Total executor cores | |
| **Active Tasks** | Currently running tasks | 0 if job completed |
| **Failed Tasks** | Tasks that failed | **Should be 0** |
| **Complete Tasks** | Successfully finished tasks | |
| **Total Tasks** | Failed + Complete tasks | |
| **Task Time (GC Time)** | Total task time with GC in parentheses | e.g., "335.0 h (3.9 h)" |
| **Input** | Total data read from sources | |
| **Shuffle Read** | Data read during shuffle | |
| **Shuffle Write** | Data written during shuffle | |
| **Exec Loss Reason** | Count of executor losses | **Should be 0** |

**What to Record from Summary:**

```
EXECUTOR SUMMARY
================
Active Executors: ___
Dead Executors: ___
Total Executors: ___

Storage Memory Used/Total: ___ / ___
Disk Used: ___

Total Cores: ___
Total Tasks: ___
Failed Tasks: ___

Task Time: ___
GC Time: ___
GC Percentage: ___ (calculate: GC Time / Task Time × 100)

Total Input: ___
Total Shuffle Read: ___
Total Shuffle Write: ___

Executor Losses: ___
```

**Red Flags in Summary:**
- [ ] Dead executors > 0 with Exec Loss > 0 → Container/memory issues
- [ ] Disk Used > 0 → Memory pressure, need more executor memory
- [ ] GC Time > 10% of Task Time → Heap too small
- [ ] Failed Tasks > 0 → Check individual executor logs
- [ ] Storage Memory shows "0 B / X" → Consider if caching would help

---

### Step 2: Analyze Individual Executors

Below the Summary, you'll see a table of individual executors.

**Executor Table Columns:**

| Column | What It Shows |
|--------|---------------|
| **Executor ID** | Numeric ID (driver is separate) |
| **Address** | Host:port (e.g., "ip-10-148-197-237.ec2.internal:35681") |
| **Status** | Active or Dead |
| **RDD Blocks** | Cached partitions on this executor |
| **Storage Memory** | Used/Max (e.g., "0.0 B / 21.2 GiB") |
| **Disk Used** | Spill on this executor |
| **Cores** | Cores assigned (e.g., 5) |
| **Active Tasks** | Currently running |
| **Failed Tasks** | Failed on this executor |
| **Complete Tasks** | Completed on this executor |
| **Total Tasks** | Total handled |
| **Task Time (GC Time)** | Time with GC in parentheses |
| **Input** | Data read |
| **Shuffle Read** | Shuffle data read |
| **Shuffle Write** | Shuffle data written |
| **Logs** | Links to stdout/stderr |
| **Add Time** | When executor was added |
| **Remove Time** | When executor died (if Dead) |

**What to Look For:**

1. **Work Distribution** — Are tasks evenly distributed?
   - Look at "Complete Tasks" column
   - If some executors have 10x more tasks, there may be data locality issues

2. **GC Time per Executor**
   - Calculate: GC Time ÷ Task Time × 100 for each executor
   - If any executor > 10%, that executor is memory-starved

3. **Failed Tasks** — Should be 0 for all executors

4. **Dead Executors** — Click on "stderr" logs to see why they died

5. **Executor Memory** (from Storage Memory column)
   - Format: "Used / Max"
   - Max tells you executor memory allocation (e.g., "0.0 B / 21.2 GiB" means ~21 GB)

**Record for Problem Executors:**

```
EXECUTOR ISSUES
===============
Executor ID: ___
Status: Dead
Died at: ___
Tasks Completed: ___
Failed Tasks: ___
GC Percentage: ___
Last Error (from stderr): ___
```

---

### Step 3: Enable Additional Metrics (Optional)

At the top of the Executors page, there are checkboxes for additional metrics:

**Recommended to Enable:**
- [ ] **Peak JVM Memory OnHeap / OffHeap** — Shows actual peak memory usage
- [ ] **Peak Execution Memory OnHeap/OffHeap** — Shows memory used for computation
- [ ] **Peak Storage Memory OnHeap/OffHeap** — Shows memory used for caching
- [ ] **Exec Loss Reason** — Shows why executors died

These give you insight into actual memory consumption vs. allocated memory.

---

## Part 2: Stages Tab Analysis

### Step 4: Review Stage Summary

At the top of the Stages tab:

```
Completed Stages: ___
Failed Stages: ___ (should be 0!)
```

**If Failed Stages > 0:** Click "+details" next to failed stages to see the error.

---

### Step 5: Analyze the Stage Table

**Stage Table Columns:**

| Column | What It Shows |
|--------|---------------|
| **Id** | Stage ID |
| **Description** | Code location (e.g., "sum at GeneralizedLinearRegression.scala:753") |
| **Submitted** | When stage started |
| **Duration** | How long the stage took |
| **Tasks: Succeeded/Total** | Task completion (e.g., "1000/1000") |
| **Input** | Data read from source |
| **Output** | Data written to sink |
| **Shuffle Read** | Data read from shuffle |
| **Shuffle Write** | Data written to shuffle |

**What to Record for Each Major Stage:**

```
STAGE ANALYSIS
==============
Stage ID: ___
Description: ___
Duration: ___
Tasks: ___ / ___
Input: ___
Output: ___
Shuffle Read: ___
Shuffle Write: ___
```

---

### Step 6: Identify Stage Patterns

**Pattern 1: Iterative Algorithms (ML jobs)**
- Same stage name repeats multiple times
- Example: `treeAggregate at WeightedLeastSquares.scala:107` appearing 10+ times
- This is normal for ML algorithms (gradient descent, IRLS, etc.)

**Pattern 2: Shuffle Stages**
- One stage writes shuffle (e.g., 386.9 MiB Shuffle Write)
- Next stage reads shuffle (e.g., 386.9 MiB Shuffle Read)
- This is the map → reduce pattern

**Pattern 3: Skewed Stages**
- Look for stages where not all tasks succeeded: "950/1000"
- Or click "+details" to see task time distribution

---

### Step 7: Click "+details" for Deep Stage Analysis

Clicking "+details" on any stage shows:

**Summary Metrics:**
- Scheduler Delay
- Task Deserialization Time
- GC Time
- Result Serialization Time
- Shuffle Read/Write Time
- Peak Execution Memory

**Task Metrics Distribution (Quantiles):**

| Metric | Min | 25th % | Median | 75th % | Max |
|--------|-----|--------|--------|--------|-----|
| Duration | | | | | |
| GC Time | | | | | |
| Shuffle Read | | | | | |
| Shuffle Write | | | | | |

**What to Look For:**

1. **Task Skew** = Max ÷ Median
   - Skew > 3x indicates data skew
   - Skew > 10x is severe

2. **GC Time Distribution**
   - If Max GC >> Median GC, some tasks are memory-starved

3. **Shuffle Read Size per Task**
   - Ideal: 100-200 MB per task
   - Too small (< 10 MB): Too many partitions
   - Too large (> 1 GB): Too few partitions

**Record Task Distribution:**

```
STAGE DETAIL: Stage ___
=======================
Task Count: ___
Duration: Min=___ | Median=___ | Max=___
Skew Ratio: ___ (Max/Median)
GC Time: Min=___ | Median=___ | Max=___
Shuffle Read per Task: ___
```

---

## Part 3: Calculating Optimal Settings

### Step 8: Memory Analysis

**From Executor Tab:**

```
A. Total Executor Memory (from Storage Memory "/ X GiB"): ___ GiB per executor
B. Number of Executors: ___
C. Total Cluster Memory: A × B = ___ GiB

D. Disk Spill Total: ___
E. GC Time Percentage: ___
```

**Memory Recommendations:**

| Finding | Current | Recommendation |
|---------|---------|----------------|
| GC > 10% | ___ | Increase `spark.executor.memory` by 50% |
| Disk Spill > 0 | ___ | Increase `spark.executor.memory` by 50% |
| Executor Loss > 0 | ___ | Increase `spark.executor.memoryOverhead` to 15% of executor memory |
| Storage Memory unused | ___ | Consider caching hot DataFrames, or reduce `spark.memory.storageFraction` |

---

### Step 9: Parallelism Analysis

**From Stages Tab:**

```
F. Largest Stage Input Size: ___ GiB
G. Tasks in Largest Stage: ___
H. Data per Task: F ÷ G = ___ MB

I. Largest Shuffle Write: ___ MB
J. Current Shuffle Partitions: ___ (check Environment tab)
K. Data per Shuffle Partition: I ÷ J = ___ MB
```

**Parallelism Recommendations:**

| Finding | Current | Recommendation |
|---------|---------|----------------|
| Data per task < 10 MB | ___ | Reduce partitions (over-parallelized) |
| Data per task > 1 GB | ___ | Increase partitions (under-parallelized) |
| Shuffle partition < 10 MB | ___ | Reduce `spark.sql.shuffle.partitions` |
| Shuffle partition > 500 MB | ___ | Increase `spark.sql.shuffle.partitions` |
| Task skew > 3x | ___ | Enable AQE or salt keys |

**Optimal Shuffle Partitions Formula:**

```
Optimal = Total Shuffle Data (MB) ÷ 150 MB

Example: 4200 MB shuffle ÷ 150 MB = 28 partitions
```

---

### Step 10: Executor Sizing Analysis

**From Executor Tab:**

```
L. Cores per Executor: ___
M. Memory per Executor: ___ GiB
N. Memory per Core: M ÷ L = ___ GiB

O. Total Input Data: ___ GiB
P. Total Cores: ___
Q. Data per Core: O ÷ P = ___ GB
```

**Executor Sizing Recommendations:**

| Finding | Recommendation |
|---------|----------------|
| Memory per core < 2 GB | Reduce cores per executor or increase memory |
| Memory per core > 8 GB | Can increase cores per executor |
| Data per core < 100 MB | Cluster is over-provisioned; reduce executors |
| Data per core > 4 GB | Cluster is under-provisioned; add executors |
| Executor losses | Reduce cores (each core = more memory pressure) |

**Recommended Executor Configurations:**

| Workload Type | Cores | Memory | Memory Overhead |
|---------------|-------|--------|-----------------|
| General | 4-5 | 16-20g | 2-3g |
| Memory-heavy (ML, joins) | 3-4 | 20-24g | 3-4g |
| Shuffle-heavy | 4-5 | 16-20g | 2-3g |
| With executor losses | 3-4 | 16g | 4g (increase!) |

---

## Part 4: Common Issues & Solutions

### Issue: Executor Loss / Container Killed

**Symptoms:**
- Dead executors in Executor tab
- "Exec Loss Reason" count > 0
- YARN logs show "Container killed"

**Diagnosis:**
1. Click "stderr" link on dead executor
2. Look for: `Container killed by YARN for exceeding memory limits`

**Solution:**
```
--conf spark.executor.memoryOverhead=3g  # or 15-20% of executor memory
```

---

### Issue: High GC Time

**Symptoms:**
- GC Time > 10% of Task Time in Summary
- Individual executors show high GC

**Diagnosis:**
- Too many objects created
- Heap too small for workload

**Solution:**
```
--conf spark.executor.memory=<current × 1.5>
# OR reduce cores to lower parallelism per executor:
--conf spark.executor.cores=3  # instead of 5
```

---

### Issue: Disk Spill

**Symptoms:**
- "Disk Used" > 0 in Summary or individual executors

**Diagnosis:**
- Execution memory exhausted during shuffle/aggregation
- Data doesn't fit in memory

**Solution:**
```
--conf spark.executor.memory=<current × 1.5>
--conf spark.memory.fraction=0.7  # increase execution memory portion
```

---

### Issue: Task Skew

**Symptoms:**
- Stage takes long time
- In stage details: Max task time >> Median task time

**Diagnosis:**
- Data is unevenly distributed across partitions
- One partition has much more data than others

**Solution:**
```
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
# OR manually salt the skewed key in your code
```

---

### Issue: Small Tasks (Over-parallelized)

**Symptoms:**
- Median task duration < 100ms
- Thousands of tasks for small data

**Diagnosis:**
- Too many partitions
- Scheduling overhead > actual work

**Solution:**
```
--conf spark.sql.shuffle.partitions=<smaller number>
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
```

---

### Issue: Failed Stages

**Symptoms:**
- "Failed Stages: N" at top of Stages tab

**Diagnosis:**
1. Find failed stage in the list
2. Click "+details"
3. Look at "Failure Reason"

**Common Causes:**
- OOM during task
- Fetch failure (shuffle service issue)
- Task timeout
- Data corruption

---

## Part 5: Analysis Report Template

```
SPARK APPLICATION ANALYSIS REPORT
=================================
Application ID: ___
Date Analyzed: ___

EXECUTOR METRICS (from Executor Tab Summary)
--------------------------------------------
Total Executors: ___ (Active: ___, Dead: ___)
Total Cores: ___
Executor Memory: ___ GiB each
Total Cluster Memory: ___ GiB

Task Metrics:
  Total Tasks: ___
  Failed Tasks: ___
  Task Time: ___
  GC Time: ___ (___ %)
  
I/O Metrics:
  Total Input: ___
  Total Output: ___
  Shuffle Read: ___
  Shuffle Write: ___
  
Issues:
  Disk Spill: ___
  Executor Losses: ___

STAGE METRICS (from Stages Tab)
-------------------------------
Completed Stages: ___
Failed Stages: ___

Heaviest Stages:
  1. Stage ___: ___ tasks, ___ duration, ___ input
  2. Stage ___: ___ tasks, ___ duration, ___ input
  3. Stage ___: ___ tasks, ___ duration, ___ input

Task Skew (worst stage):
  Max/Median Ratio: ___

ISSUES IDENTIFIED
-----------------
[ ] High GC (>10%): ___
[ ] Disk Spill: ___
[ ] Executor Loss: ___
[ ] Task Skew (>3x): ___
[ ] Failed Stages: ___
[ ] Over-parallelized: ___
[ ] Under-parallelized: ___

RECOMMENDED CONFIGURATION
-------------------------
spark.executor.memory: ___ (was: ___)
spark.executor.cores: ___ (was: ___)
spark.executor.memoryOverhead: ___ (was: ___)
spark.executor.instances: ___ (was: ___)
spark.sql.shuffle.partitions: ___ (was: ___)
spark.sql.adaptive.enabled: true

SPARK-SUBMIT COMMAND
--------------------
spark-submit \
  --conf spark.executor.memory=___ \
  --conf spark.executor.cores=___ \
  --conf spark.executor.memoryOverhead=___ \
  --conf spark.sql.adaptive.enabled=true \
  your_app.py
```

---

## Quick Reference Card

| Metric | Good | Warning | Bad |
|--------|------|---------|-----|
| GC Time % | < 5% | 5-10% | > 10% |
| Disk Spill | 0 | < 1 GB | > 1 GB |
| Executor Loss | 0 | 1-2 | > 2 |
| Task Skew | < 2x | 2-5x | > 5x |
| Failed Stages | 0 | 1 | > 1 |
| Failed Tasks | 0 | < 1% | > 1% |
| Task Duration | 0.5-2 min | 10s-30s or 2-5 min | < 10s or > 5 min |
| Partition Size | 100-200 MB | 50-500 MB | < 10 MB or > 1 GB |
