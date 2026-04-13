# Metrics Aggregation: Are We Summing Correctly?

Critical question: When we sum metrics across stages, are we accurately representing what happened?

---

## The Question

```python
# Current code in _render_report():
t = {
    "shuffleWriteBytes": sum(s.get("shuffleWriteBytes", 0) for s in stages),
    "diskBytesSpilled": sum(s.get("diskBytesSpilled", 0) for s in stages),
    ...
}

# But shuffle happens BETWEEN stages, not WITHIN a stage
# So summing across stages... is that right?
```

---

## Answer: YES, Summing is Correct (With Caveats)

### Understanding Shuffle

**Shuffle** = Network data transfer between stages

```
Stage 0: Read + Filter
├─ Input: 100GB from S3
├─ Output: 50GB filtered
└─ shuffleWriteBytes: 0 (no shuffle in this stage)

↓ SHUFFLE BOUNDARY ← Network transfer here!

Stage 1: GroupBy + Shuffle
├─ Input: 50GB (from previous stage)
├─ Shuffle operation: Group data by key
├─ Output: Grouped data
└─ shuffleWriteBytes: 50GB (data written for next stage)

↓ SHUFFLE BOUNDARY ← More network transfer!

Stage 2: Final Aggregation
├─ Input: 50GB (from stage 1's shuffle)
├─ Output: 1 result
└─ shuffleWriteBytes: 0
```

### Why Summing is Correct

```
Total network traffic = Stage 0's shuffle + Stage 1's shuffle + Stage 2's shuffle
                     = 0GB + 50GB + 0GB
                     = 50GB total

This represents: Total data moved across the network
```

---

## Real Example: Multiple Shuffles

### Code
```python
df = spark.read.parquet("s3://data/")  # 100GB
df = df.filter(df.year == 2024)         # Filter
df = df.groupBy("region").agg(...)      # First shuffle
df = df.join(other_df, "key")           # Second shuffle
result = df.groupBy("category").count() # Third shuffle
result.show()
```

### Stages & Shuffles

```
Stage 0: Read + Filter
├─ Input: 100GB from S3
├─ Output: 50GB
└─ shuffleWriteBytes: 0

↓ (no shuffle)

Stage 1: First GroupBy
├─ Input: 50GB
├─ Output: Grouped by region (20GB)
├─ shuffleWriteBytes: 50GB ← First shuffle
└─ shuffleReadBytes: 50GB (from stage 0)

↓ (SHUFFLE #1: 50GB moved)

Stage 2: Join
├─ Input: 20GB from stage 1 + other_df
├─ Output: Joined data (30GB)
├─ shuffleWriteBytes: 30GB ← Second shuffle
└─ shuffleReadBytes: 30GB + other_df size

↓ (SHUFFLE #2: 30GB moved)

Stage 3: Final GroupBy
├─ Input: 30GB from stage 2
├─ Output: 1 final result
├─ shuffleWriteBytes: 30GB ← Third shuffle
└─ shuffleReadBytes: 30GB

↓ (SHUFFLE #3: 30GB moved)

Stage 4: Collect
├─ Return result to driver
```

### Summed Metrics

```python
Total shuffleWriteBytes = 0 + 50 + 30 + 30 = 110GB

What does this mean?
├─ Stage 1 shuffle: 50GB network traffic
├─ Stage 2 shuffle: 30GB network traffic
├─ Stage 3 shuffle: 30GB network traffic
└─ Total: 110GB moved across network

Is this correct? YES!
Each shuffle is expensive network work.
110GB total network traffic is the real cost.
```

---

## When Summing Can Be Misleading

### Case 1: Same Data Recycled Through Multiple Shuffles

```
Stage 0: Read 100GB

Stage 1: GroupBy
├─ Read: 100GB
├─ Write: 100GB (shuffle for groupby)
└─ shuffleWriteBytes: 100GB

Stage 2: GroupBy different key
├─ Read: 100GB (from stage 1's output)
├─ Write: 100GB (shuffle for new groupby)
└─ shuffleWriteBytes: 100GB

Total shuffleWriteBytes = 100 + 100 = 200GB

Reality: Only 100GB of data, but passed through 2 shuffles
Cost: 200GB network traffic (correct!)
```

**Verdict:** Sum is CORRECT - each shuffle is real network work.

### Case 2: Shuffle with Filtering

```
Stage 0: Read + Filter
├─ Input: 100GB
├─ Output: 10GB (filtered)
└─ shuffleWriteBytes: 0

Stage 1: GroupBy
├─ Input: 10GB
├─ Output: 5GB
├─ shuffleWriteBytes: 10GB ← Write for shuffle
└─ shuffleReadBytes: 10GB ← Read from previous

Total shuffleWriteBytes = 0 + 10 = 10GB

Reality: 10GB moved across network
Cost: 10GB (correct!)
```

**Verdict:** Sum is CORRECT.

---

## When Summing CAN Mislead Users

### Case: Showing Total vs Per-Stage

**What we show:**
```
🟡 Large shuffle — 100GB of data moved
   This is almost certainly your bottleneck
```

**Reality:**
```
Stage 1 shuffle: 50GB
Stage 2 shuffle: 30GB
Stage 3 shuffle: 20GB
Total: 100GB

But what if the user thinks:
"100GB in a single shuffle" (WRONG)
vs
"100GB across 3 separate shuffles" (RIGHT)
```

**Is this misleading?** Slightly, but the sum is still correct.

---

## Other Metrics: Are They Correct to Sum?

### diskBytesSpilled

```
Stage 0: Process data
├─ Runs out of memory
├─ Spills 5GB to disk
└─ diskBytesSpilled: 5GB

Stage 1: Different data
├─ Runs out of memory
├─ Spills 3GB to disk
└─ diskBytesSpilled: 3GB

Total diskBytesSpilled = 5 + 3 = 8GB

Meaning: 8GB total written to disk (across two stages)
Correct? YES - both are real I/O operations
```

### memoryBytesSpilled

```
Stage 0: Shuffle
├─ Internal data moved between memory pools
├─ memoryBytesSpilled: 10GB
└─ (Data stayed in memory, just reorganized)

Stage 1: Shuffle
├─ memoryBytesSpilled: 5GB
└─ (Data stayed in memory, just reorganized)

Total memoryBytesSpilled = 10 + 5 = 15GB

Meaning: 15GB moved between memory pools
Correct? YES - represents memory pressure
```

### executorRunTime

```
Stage 0: 100 tasks
├─ Each takes 1 second
├─ executorRunTime: 100s total (sum of all task times)
└─ But with 8 slots, actual time: 100 ÷ 8 = 12.5 seconds

Stage 1: 50 tasks
├─ executorRunTime: 50s total
└─ With 8 slots, actual time: 50 ÷ 8 = 6.25 seconds

Total executorRunTime = 100 + 50 = 150s

Meaning: 150 seconds of executor work across all stages
Actual wall-clock time: ~18.75 seconds (parallelized)

Is summing correct? YES for showing total work
But it's NOT the same as elapsed time!
```

---

## Current Implementation: Is It Correct?

```python
def _render_report(stages, elapsed_s, ml_libs=None):
    # Aggregate totals across stages
    t = {
        "numTasks":           sum(s.get("numTasks", 0) for s in stages),
        "executorRunTime":    sum(s.get("executorRunTime", 0) for s in stages),
        "shuffleWriteBytes":  sum(s.get("shuffleWriteBytes", 0) for s in stages),
        "diskBytesSpilled":   sum(s.get("diskBytesSpilled", 0) for s in stages),
        ...
    }
```

### Analysis

| Metric | Summing | Correct? | Notes |
|--------|---------|----------|-------|
| **numTasks** | ✓ | YES | Total parallelization |
| **executorRunTime** | ✓ | YES | Total work, not elapsed |
| **shuffleWriteBytes** | ✓ | YES | Total network traffic |
| **diskBytesSpilled** | ✓ | YES | Total disk I/O |
| **memoryBytesSpilled** | ✓ | YES | Total memory pressure |
| **jvmGcTime** | ✓ | YES | Total GC time |
| **inputBytes** | ✓ | YES | Total data read |
| **outputBytes** | ✓ | YES | Total data written |

**Verdict: All correct!** ✅

---

## Potential Issues with Current Presentation

### Issue 1: executorRunTime vs elapsed_s

```python
# We report both:
Total time: 150s (elapsed_s)         ← Wall-clock time
Executor time: 150s (sum of executor runs)  ← Confusing!

# User might think: "Why is executor time = wall-clock time?"
# Answer: Because in this example, only 1 stage/task was running
```

**Fix:** Clarify in report:
```
Total execution time: 150s (wall-clock)
Total executor work: 150s (sum of all tasks, parallelized)
```

### Issue 2: Shuffle Metric Clarity

```python
# Current report:
"Network shuffle (join / groupBy cost): 100GB"

# User might think: "Is this one shuffle or multiple?"
# Answer: Could be multiple shuffles summed together
```

**Fix:** Add context:
```
"Network shuffle: 100GB total across all operations"
```

### Issue 3: Per-Stage vs Aggregated

```python
# We only show aggregated metrics
# But user might benefit from seeing:
├─ Stage 0: 0GB shuffle
├─ Stage 1: 50GB shuffle
└─ Stage 2: 50GB shuffle
```

**Fix:** Add per-stage option (advanced).

---

## Recommended Improvements

### Change 1: Clarify Metrics Labels

```python
# Current:
"Total time": _fmt_elapsed(elapsed_s)

# Proposed:
"Wall-clock time": _fmt_elapsed(elapsed_s)  # Clear it's real time
"Total executor work": _fmt_ms(t["executorRunTime"])  # Clarify it's sum
```

### Change 2: Add Parallelization Info

```python
# Current:
"Parallel tasks used": str(t["numTasks"])

# Proposed:
parallelization_ratio = t["executorRunTime"] / (elapsed_s * 1000)
"Parallelization efficiency": f"{parallelization_ratio:.1f}x"
# Shows how well we used the cluster
```

### Change 3: Better Shuffle Context

```python
# Current advice:
if t["shuffleWriteBytes"] > 1 * 1024**3:
    fired.append("Very large shuffle")

# Proposed:
avg_shuffle_per_stage = t["shuffleWriteBytes"] / max(len(stages), 1)
if avg_shuffle_per_stage > 1 * 1024**3:
    fired.append("Very large shuffle per stage")
```

---

## Conclusion: Are We Sharing Wrong Info?

### Current State: ✅ CORRECT but COULD BE CLEARER

**Correct:**
- ✅ Summing shuffleWriteBytes = total network traffic (accurate)
- ✅ Summing diskBytesSpilled = total disk spill (accurate)
- ✅ Summing all metrics represents actual work done (accurate)

**Could Be Clearer:**
- ⚠️ executorRunTime label could clarify it's "total work, not elapsed"
- ⚠️ Shuffle metric could clarify "across all operations"
- ⚠️ Missing context about parallelization efficiency

---

## Suggested Report Improvement

### Current Output
```
Total time: 150s
Parallel tasks: 1200
Network shuffle: 100GB
```

### Improved Output
```
Wall-clock time: 150s (actual elapsed)
Executor work: 1200s (sum of all tasks)
Parallelization: 8.0x (1200s ÷ 150s)

Network shuffle: 100GB total
├─ Stage 1 groupBy: 50GB
├─ Stage 2 shuffle: 50GB
└─ Stage 3 shuffle: 0GB
```

---

## Final Answer

**Question:** "Are we summing wrong and sharing false info?"

**Answer:** No, we're summing correctly. Each metric sum represents real work:
- shuffleWriteBytes sum = total network traffic (accurate)
- diskBytesSpilled sum = total disk spill (accurate)
- All sums represent actual cost to the cluster

**However:** We could be clearer about what the sums mean. The current presentation is correct but could benefit from:
1. Clarifying executorRunTime vs elapsed_s
2. Explaining shuffle is "total across operations"
3. Adding parallelization ratio
4. Optionally showing per-stage breakdown

**User is not being misled, just not given full context.** The metrics are accurate and the advice is sound.
