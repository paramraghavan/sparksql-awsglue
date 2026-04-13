# Understanding Stages, Tasks, and Executor Slots

How Spark execution works and how sparkmonitor reports on it.

---

## Architecture: Job → Stages → Tasks → Executor Slots

```
Spark Job (e.g., df.groupBy().count())
    │
    ├─ Stage 0: Read from S3
    │   ├─ Task 0 (executor slot 1)
    │   ├─ Task 1 (executor slot 2)
    │   ├─ Task 2 (executor slot 3)
    │   └─ Task 3 (executor slot 4)
    │
    ├─ Stage 1: Shuffle (exchange data between nodes)
    │   ├─ Task 0 (executor slot 1) ← waits for stage 0 to complete
    │   ├─ Task 1 (executor slot 2)
    │   └─ Task 2 (executor slot 3)
    │
    └─ Stage 2: Final aggregation
        ├─ Task 0 (executor slot 1)
        └─ Task 1 (executor slot 2)
```

---

## Definitions

### Stage
- A **set of tasks that run in parallel**
- Stages are separated by shuffle boundaries (wide transformations)
- Example: Read, Filter, Aggregate each might be a stage

### Task
- A **single unit of work** executed on one executor slot
- Each task processes a partition of data
- Example: "Read partition 0 from S3", "Count rows in partition 0"

### Executor Slot
- A **physical core/thread** where a task runs
- Limited by cluster size: if cluster has 4 executors × 2 cores = 8 slots
- Tasks queue if more tasks than slots

### Partition
- A **chunk of data** that a task processes
- More partitions = more tasks = better parallelization
- 1 task per partition

---

## Real Example: DataFrame Operations

### Code
```python
df = spark.read.parquet("s3://data/")        # 100GB data
df = df.filter(df.year == 2024)               # Filter rows
result = df.groupBy("region").count()         # Group and count
result.show()                                 # Action (triggers execution)
```

### Execution Plan

```
Stage 0: Read from S3 (Read + Filter)
├─ Input: 100GB from S3 (1000 partitions)
├─ Tasks: 1000 (one per partition)
├─ Parallelism: 8 executor slots
│  Time: 1000 tasks ÷ 8 slots = 125 batches = ~125s (if each 1s)
├─ Output: Filtered data (50GB)
└─ Metrics:
   ├─ numTasks: 1000
   ├─ inputBytes: 100GB (from S3)
   ├─ executorRunTime: 125s total
   └─ shuffleWriteBytes: 0 (no shuffle yet)

Stage 1: Shuffle (GroupBy - wide transformation)
├─ Input: 50GB filtered data
├─ Tasks: 200 (repartition to 200 buckets)
├─ Parallelism: 8 slots
│  Time: 200 ÷ 8 = 25 batches = ~25s
├─ Output: Grouped data
└─ Metrics:
   ├─ numTasks: 200
   ├─ shuffleWriteBytes: 50GB (data moved across network!)
   ├─ shuffleReadBytes: 50GB (received from stage 0)
   └─ executorRunTime: 25s total

Stage 2: Final Count
├─ Input: Grouped data
├─ Tasks: 1 (just final aggregation)
├─ Parallelism: 1 slot
├─ Output: Single count number
└─ Metrics:
   ├─ numTasks: 1
   ├─ executorRunTime: 1s
   └─ shuffleReadBytes: 50GB
```

---

## What Spark History Server Returns (Stages Data)

```json
[
  {
    "stageId": 0,
    "status": "COMPLETE",
    "numTasks": 1000,
    "numCompleteTasks": 1000,
    "inputBytes": 107374182400,      // 100GB in bytes
    "outputBytes": 53687091200,       // 50GB filtered
    "shuffleReadBytes": 0,
    "shuffleWriteBytes": 0,
    "executorRunTime": 125000,        // milliseconds
    "executorCpuTime": 110000,
    "jvmGcTime": 3000,
    "diskBytesSpilled": 0,
    "memoryBytesSpilled": 0,
    "peakExecutionMemory": 536870912  // 512MB peak
  },
  {
    "stageId": 1,
    "status": "COMPLETE",
    "numTasks": 200,
    "numCompleteTasks": 200,
    "inputBytes": 0,
    "outputBytes": 0,
    "shuffleReadBytes": 53687091200,  // 50GB received
    "shuffleWriteBytes": 53687091200, // 50GB sent
    "executorRunTime": 25000,
    "executorCpuTime": 24000,
    "jvmGcTime": 500,
    "diskBytesSpilled": 0,
    "memoryBytesSpilled": 0,
    "peakExecutionMemory": 268435456   // 256MB peak
  }
]
```

---

## How _render_report() Works

### Input: List of Stages

```python
def _render_report(stages, elapsed_s, ml_libs=None):
    """
    stages: List of stage dictionaries from History Server
    [
      {"stageId": 0, "numTasks": 1000, "executorRunTime": 125000, ...},
      {"stageId": 1, "numTasks": 200, "shuffleWriteBytes": 53687091200, ...}
    ]
    """
```

### Step 1: Aggregate Metrics Across All Stages

```python
# Sum up metrics from all stages
t = {
    "numTasks":           sum(s.get("numTasks", 0) for s in stages),
    "inputBytes":         sum(s.get("inputBytes", 0) for s in stages),
    "shuffleWriteBytes":  sum(s.get("shuffleWriteBytes", 0) for s in stages),
    "diskBytesSpilled":   sum(s.get("diskBytesSpilled", 0) for s in stages),
    "memoryBytesSpilled": sum(s.get("memoryBytesSpilled", 0) for s in stages),
    "executorRunTime":    sum(s.get("executorRunTime", 0) for s in stages),
    "jvmGcTime":          sum(s.get("jvmGcTime", 0) for s in stages),
    ...
}

# Example results:
# numTasks: 1000 + 200 = 1200 total tasks
# shuffleWriteBytes: 0 + 50GB = 50GB moved across network
# executorRunTime: 125s + 25s = 150s total execution
```

### Step 2: Check for Performance Issues

```python
# Using aggregated metrics, check advice rules
for rule in _ADVICE:
    if rule["trigger"](t, elapsed_s):  # Does this condition match?
        fired.append(rule)

# Examples:
# Rule: "Large shuffle"
#   trigger: shuffleWriteBytes > 500MB?
#   Check: 50GB > 500MB? → YES! Add warning

# Rule: "Data spilled to disk"
#   trigger: diskBytesSpilled > 0?
#   Check: 0 > 0? → NO, pass

# Rule: "Few tasks"
#   trigger: numTasks < 10 AND inputBytes > 100MB?
#   Check: 1200 < 10? → NO, pass
```

### Step 3: Generate Report

```python
# Create HTML table with metrics
Metrics:
├─ Total time: 150s
├─ Parallel tasks: 1200
├─ Data read: 100GB
├─ Network shuffle: 50GB ← Highlighted (warning)
├─ Spilled to disk: None ✓
└─ GC time: 3.5s (2.3%)

Advice Cards:
🟡 Large shuffle — join or groupBy moved a lot of data
   Your cell moved 50GB of data across the network.
   Why it matters: Network transfers are expensive.

   Solution: Use broadcast() if one table is small
```

---

## Visual: How Executor Slots Affect Performance

### Scenario: 1000 tasks, 8 executor slots

```
Timeline with 8 slots:
Slot 1: [Task 0] [Task 8] [Task 16] ... [Task 992]
Slot 2: [Task 1] [Task 9] [Task 17] ... [Task 993]
Slot 3: [Task 2] [Task 10] [Task 18] ... [Task 994]
Slot 4: [Task 3] [Task 11] [Task 19] ... [Task 995]
Slot 5: [Task 4] [Task 12] [Task 20] ... [Task 996]
Slot 6: [Task 5] [Task 13] [Task 21] ... [Task 997]
Slot 7: [Task 6] [Task 14] [Task 22] ... [Task 998]
Slot 8: [Task 7] [Task 15] [Task 23] ... [Task 999]

Time: 1000 ÷ 8 = 125 batches
```

### Scenario: 100 tasks, 8 executor slots

```
Timeline with 8 slots:
Slot 1: [Task 0] [Task 8] [Task 16] ...
Slot 2: [Task 1] [Task 9] [Task 17] ...
Slot 3: [Task 2] [Task 10] [Task 18] ...
Slot 4: [Task 3] [Task 11] [Task 19] ...
Slot 5: [Task 4] [Task 12]
Slot 6: [Task 5] [Task 13]
Slot 7: [Task 6] [Task 14]
Slot 8: [Task 7] [Task 15]

Time: 100 ÷ 8 = 12.5 batches
STATUS: ⚠️ Few parallel tasks — cluster underused!
```

---

## Metrics Explained

### numTasks
- **What:** Total number of tasks across all stages
- **From stages:** Sum of `numTasks` in each stage
- **Example:** Stage 0: 1000 + Stage 1: 200 = 1200 total
- **Metric:** Low (< 10) with big data = cluster underused

### inputBytes
- **What:** Total data read from storage (S3, HDFS)
- **From stages:** Sum of `inputBytes` in each stage
- **Example:** 100GB read from S3
- **Metric:** Combined with numTasks: if low tasks, low parallelization

### shuffleWriteBytes
- **What:** Total data moved between nodes (network)
- **From stages:** Sum of `shuffleWriteBytes` in each stage
- **Example:** 50GB moved in groupBy shuffle
- **Metric:** High (> 1GB) = major bottleneck

### executorRunTime
- **What:** Total execution time across all tasks
- **From stages:** Sum of `executorRunTime` in each stage
- **Example:** 125s (stage 0) + 25s (stage 1) = 150s total
- **Metric:** Not elapsed time, but sum of all executor work

### jvmGcTime
- **What:** Time spent in garbage collection
- **From stages:** Sum of `jvmGcTime` in each stage
- **Example:** 3.5s total GC time in 150s execution
- **Metric:** High (> 10% of runtime) = Python UDFs or memory pressure

### diskBytesSpilled
- **What:** Data written to disk due to memory pressure
- **From stages:** Sum of `diskBytesSpilled` in each stage
- **Example:** 0 (good) or 100GB (bad)
- **Metric:** Any value > 0 = PROBLEM (disk is slow)

---

## Complete Flow: Code to Report

```
1. USER WRITES CODE
   %%measure
   df = spark.read.parquet("s3://...")
   df.groupBy("region").count().show()

2. SPARKMONITOR CAPTURES BEFORE STATE
   before_stages = [0, 1, 2]  # Already completed stages

3. CODE EXECUTES
   Stage 3 starts (read) → 1000 tasks
   Stage 4 starts (shuffle) → 200 tasks
   Both complete in 150 seconds

4. SPARKMONITOR CAPTURES AFTER STATE
   after_stages = [0, 1, 2, 3, 4]
   new_stages = [3, 4]  # Just created

5. HISTORY SERVER PROVIDES STAGE DATA
   {
     "stageId": 3,
     "numTasks": 1000,
     "inputBytes": 107374182400,
     "shuffleWriteBytes": 0,
     "executorRunTime": 125000,
     ...
   },
   {
     "stageId": 4,
     "numTasks": 200,
     "shuffleWriteBytes": 53687091200,
     "executorRunTime": 25000,
     ...
   }

6. _render_report() PROCESSES STAGES
   ├─ Aggregates: 1000 + 200 = 1200 tasks
   ├─ Checks: 50GB shuffle > 500MB? YES → Add warning
   ├─ Formats: "Shuffle write: 50GB"
   └─ Creates HTML report with:
      ├─ Metrics table
      ├─ Health banner (🟡 warning)
      └─ Advice cards (how to optimize)

7. REPORT DISPLAYED TO USER
   ⚡ sparkmonitor
   🟡 Warnings found — review the advice cards

   Metrics:
   ├─ Total time: 150s
   ├─ Parallel tasks: 1200
   ├─ Network shuffle: 50GB ⚠️
   └─ Spilled to disk: None ✓

   Advice:
   🟡 Large shuffle — join or groupBy moved a lot of data
      Solution: Use broadcast() if one table is small
```

---

## How Executor Slots Affect Metrics

### Example: Same job, different cluster sizes

**Cluster A: 2 executors × 2 cores = 4 slots**
```
1000 tasks ÷ 4 slots = 250 batches
executorRunTime: 250 × 0.1s = 25s total
elapsed: 100s (with overhead)
```

**Cluster B: 8 executors × 2 cores = 16 slots**
```
1000 tasks ÷ 16 slots = 62.5 batches
executorRunTime: 62.5 × 0.1s = 6.25s total
elapsed: 10s (with overhead)
```

**Same metrics different performance!**
- `numTasks`: 1000 (always)
- `executorRunTime`: 25s vs 6.25s (depends on slots)
- `elapsed`: 100s vs 10s (actual wall-clock time)

---

## Summary: How Stages → Tasks → Slots

```
Spark Job
   ↓
Multiple Stages (separated by shuffles)
   ↓
Each Stage has Multiple Tasks
   ↓
Tasks Scheduled onto Executor Slots
   ↓
Tasks Execute in Parallel (limited by slot count)
   ↓
Metrics Collected:
   ├─ numTasks: How many tasks ran
   ├─ executorRunTime: Total executor time (sum of all tasks)
   ├─ shuffleBytes: Network traffic (expensive)
   ├─ spillBytes: Disk writes (very expensive)
   └─ etc.
   ↓
_render_report() Aggregates & Analyzes
   ↓
HTML Report with Advice
```

This is what sparkmonitor displays when you use `%%measure`! 🚀
