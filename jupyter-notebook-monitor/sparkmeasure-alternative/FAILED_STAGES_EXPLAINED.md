# Including FAILED Stages in SparkMonitor Reports

## What Changed

**Before:** SparkMonitor only reported on **COMPLETE** stages (successful jobs)

**Now:** SparkMonitor reports on both **COMPLETE** and **FAILED** stages

This means if a stage fails during your cell execution, SparkMonitor will now capture it and show you what went wrong.

---

## Why This Matters

### Problem: Silent Failures

```python
%%measure
# Job has a problem that causes Stage 3 to fail
spark.range(1000000).groupBy("id").count().show()

# Old behavior:
# Stage 1: COMPLETE ✓
# Stage 2: COMPLETE ✓
# Stage 3: FAILED ✗   ← This was ignored!
#
# SparkMonitor would wait 3 minutes, timeout, and show:
# ⚠️ Timeout waiting for new stages
# (But Stage 3 actually failed - we just didn't report it)
```

### Solution: See All Terminal States

```python
%%measure
# Same job with stage failure
spark.range(1000000).groupBy("id").count().show()

# New behavior:
# Stage 1: COMPLETE ✓
# Stage 2: COMPLETE ✓
# Stage 3: FAILED ✗   ← Now we capture this!
#
# SparkMonitor shows:
# ⚡ sparkmonitor
# ⚠️ Stage 3 failed - see Spark UI for details
# [Report with stages 1, 2, 3 metrics]
```

---

## How to Interpret FAILED Stages

### What It Means

A **FAILED stage** is a stage that started but didn't complete successfully. This could be due to:

- **Task failure**: One or more tasks in the stage threw an exception
- **Out of memory**: Executor ran out of memory during stage execution
- **Network issue**: Connection lost during stage execution
- **Timeout**: Task took too long and timed out
- **Data format error**: File corruption, invalid data format, etc.
- **User code error**: Exception in your map/reduce function

### What You'll See in the Report

```
⚡ sparkmonitor
🟡 Stage 3 failed (see Spark UI for error details)
   Root cause: Task failed or executor lost connection

📊 Metrics:
Stage | Status   | Tasks | Duration | Shuffle Write
------|----------|-------|----------|---------------
  1   | COMPLETE |  100  | 1.2 sec  | 50 MB
  2   | COMPLETE |  200  | 2.5 sec  | 100 MB
  3   | FAILED   |   50  | 1.0 sec  | 0 MB (partial)
```

---

## What Happens With FAILED Stages

### Stage Metrics Are Partial

When a stage fails, only partial metrics are available:

```
Stage 3 failed at task 50 of 100
├─ Tasks completed: 50 (50%)
├─ Tasks failed: 1 (caused whole stage to fail)
└─ Metrics reflect only the 50 completed tasks
```

### Shuffle Data May Be Incomplete

```
Stage 2 (COMPLETE) → Shuffle output: 100 MB
Stage 3 (FAILED)   → Shuffle output: 50 MB (partial)
                      (Never completed shuffle phase)
```

### Report Still Generates

Unlike before, SparkMonitor doesn't timeout waiting for FAILED stages:

```
Old behavior:
├─ Wait 3 minutes for Stage 3 to become COMPLETE
└─ Timeout (because Stage 3 is FAILED, not COMPLETE)
→ Show report with warning ⚠️

New behavior:
├─ Detect Stage 3 is FAILED (terminal state)
└─ Include it in report immediately
→ Show report with failure info 🟡
```

---

## Debugging FAILED Stages

### Step 1: Check the Spark UI

```
Stage 3 failed → Check Spark History Server:
├─ http://localhost:18080/history/
├─ Find your application
├─ Look at Stage 3 details
└─ See which task failed and why
```

### Step 2: Check Error Message

The Spark UI shows:

```
Stage 3:
├─ Status: FAILED
├─ Failure reason: "Task 47 failed (lost executor)"
├─ Or: "Task 23 failed: OutOfMemoryError: Java heap space"
├─ Or: "Task 5 failed: FileNotFoundException: /path/to/file"
```

### Step 3: Common Fixes

| Error | Cause | Fix |
|---|---|---|
| OutOfMemoryError | Too much data in memory | Increase executor memory, reduce partition size |
| FileNotFoundException | File doesn't exist or path wrong | Check S3 path, verify file exists |
| Lost executor | Executor crashed | Increase executor memory, check logs for OOM |
| Task timeout | Task taking too long | Increase timeout, optimize code, repartition |
| Deserialization error | Invalid data format | Check data format, use appropriate reader |

---

## Example: Job With Failed Stage

### What You Run

```python
%%measure
# This job will fail on shuffle because of memory
df = spark.read.csv("s3://data/large-file.csv")
df.groupBy("region").agg(count("*")).show()
```

### What You See

```
[CSV reading output...]

⏳ Waiting for new stages to complete and be indexed by History Server...
(Timeout: 3 minutes maximum)

⚡ sparkmonitor
🟡 Stage 1 completed, but Stage 2 failed
   See Spark UI for failure reason

📊 Metrics (partial - Stage 2 incomplete):
Stage | Status | Tasks | Duration | Shuffle
------|--------|-------|----------|--------
  0   | RUN    |   1   | 0.1 sec  | -
  1   | COMPLETE |  500 | 2.3 sec  | 180 MB
  2   | FAILED |  100  | 0.8 sec  | 50 MB (partial)

⚠️ Failure reason (from Spark):
   executor was lost
   Check: Are executors running out of memory?
```

---

## Implications of This Change

### Benefits

✅ **See failures immediately** — No more 3-minute timeout for failed stages

✅ **Faster debugging** — Know which stage failed within seconds

✅ **Complete picture** — Report shows both successful and failed stages

✅ **Better insights** — Partial metrics help identify where failure occurred

### Considerations

⚠️ **Metrics are partial** — FAILED stages have incomplete task runs

⚠️ **Shuffle data incomplete** — Data produced by FAILED stage is partial/invalid

⚠️ **Downstream stages blocked** — Stages depending on FAILED stage won't run

### Example: Impact on Metrics

```
Without FAILED stages (old):
├─ Timeout waiting
├─ Show only stages 1, 2
└─ Miss that stage 3 failed

With FAILED stages (new):
├─ Immediately see stages 1, 2, 3
├─ Know stage 3 failed
└─ Can debug faster
```

---

## When Do Stages Fail?

### Common Scenarios

#### Scenario 1: Out of Memory

```python
%%measure
# Loading huge file into memory
df = spark.read.parquet("s3://huge-data/")
df.collect()  # ← Tries to fit all data in memory

# Stage 0 (read): COMPLETE ✓
# Stage 1 (collect): FAILED ✗ (OOM)
```

#### Scenario 2: Data Format Error

```python
%%measure
# Reading corrupted file
df = spark.read.csv("s3://data/file.csv", header=True)
df.select(df.age.cast("int")).show()  # ← Fails if age has non-numeric

# Stage 0 (read): COMPLETE ✓
# Stage 1 (cast): FAILED ✗ (type error)
```

#### Scenario 3: Lost Executor

```python
%%measure
# Long job causes executor to crash
df.groupBy("region").agg(sum("amount")).show()

# Stage 0 (groupBy): COMPLETE ✓
# Stage 1 (shuffle): FAILED ✗ (executor lost)
```

#### Scenario 4: File Not Found

```python
%%measure
# Reading from wrong path
df = spark.read.parquet("s3://bucket/wrong-path/")
df.count()

# Stage 0 (read): FAILED ✗ (path doesn't exist)
```

---

## How It Works

### Detection Logic

```python
# OLD:
_new_stages = [s for s in stages if s.status == "COMPLETE" and s.id not in before]

# NEW:
_new_stages = [s for s in stages if s.status in ["COMPLETE", "FAILED"] and s.id not in before]
```

### Timeline

```
t=0.0s  Cell starts, BEFORE = {0, 1}
t=2.0s  Cell finishes
        Stage 2: COMPLETE ✓
        Stage 3: FAILED ✗
t=2.1s  ⏳ Waiting for new stages...
t=2.5s  NEW stages detected: [2 (COMPLETE), 3 (FAILED)]
t=2.6s  Report generated with both stages
```

---

## Key Points

✅ **Both terminal states detected** — COMPLETE and FAILED stages are now captured

✅ **Faster failure detection** — No waiting for timeout, see failures immediately

✅ **Partial metrics ok** — Report shows what data was before failure

✅ **User can debug** — See which stage failed, check Spark UI for why

⚠️ **Read error details in Spark UI** — SparkMonitor shows stage failed, HS has details

---

## Next Time You See a Failed Stage

1. **Check the SparkMonitor report** — It will tell you which stage failed
2. **Open Spark History Server** — http://localhost:18080
3. **Find your application** — Look at the failed stage details
4. **Read the error message** — It will tell you exactly what went wrong
5. **Apply the fix** — Memory? Path? Format? All fixable!

**Result:** Faster debugging, clearer insights, better understanding of failures! 🎯

---

## Summary

| Item | Before | Now |
|------|--------|-----|
| COMPLETE stages | ✅ Captured | ✅ Captured |
| FAILED stages | ❌ Ignored | ✅ Captured |
| Timeout for failures | 3 minutes ⏳ | Immediate 🚀 |
| Report quality | Incomplete | Complete |
| Debugging speed | Slow | Fast |

**Result: Better visibility into what happens during your Spark job execution!** 🎯
