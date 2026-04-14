# SparkMonitor Design & Architecture

## High-Level Overview

SparkMonitor is a **zero-dependency Jupyter cell magic** that monitors Spark job performance and provides **plain-English recommendations** for optimization.

```
┌─────────────────────────────────────────────────────────────┐
│ USER WRITES JUPYTER CELL WITH %%measure                     │
└────────────────────────────┬────────────────────────────────┘
                             │
                ┌────────────▼───────────┐
                │ 1. CONNECT TO SPARK    │
                │   (History Server /    │
                │    YARN / Direct UI)   │
                └────────────┬───────────┘
                             │
                ┌────────────▼──────────────────┐
                │ 2. CAPTURE "BEFORE" STATE     │
                │   (Which stages completed)    │
                └────────────┬──────────────────┘
                             │
                ┌────────────▼──────────────────┐
                │ 3. EXECUTE USER CELL CODE     │
                │   (User's Spark SQL, etc.)    │
                └────────────┬──────────────────┘
                             │
                ┌────────────▼──────────────────┐
                │ 4. MEASURE ELAPSED TIME       │
                │   (Wall-clock seconds)        │
                └────────────┬──────────────────┘
                             │
                ┌────────────▼──────────────────┐
                │ 5. RETRY WAIT FOR HISTORY     │
                │   SERVER (Smart retry with    │
                │   adaptive timeouts)          │
                └────────────┬──────────────────┘
                             │
                ┌────────────▼──────────────────────┐
                │ 6. CAPTURE NEW STAGES             │
                │   (Stages that completed         │
                │    after user cell ran)          │
                └────────────┬──────────────────────┘
                             │
                ┌────────────▼──────────────────────┐
                │ 7. ANALYZE METRICS                │
                │   ├─ Aggregate across stages      │
                │   ├─ Find problem stages          │
                │   └─ Check advice rules           │
                └────────────┬──────────────────────┘
                             │
                ┌────────────▼──────────────────────┐
                │ 8. RENDER REPORT                  │
                │   ├─ Metrics table                │
                │   ├─ Per-stage breakdown          │
                │   └─ Advice cards with fixes      │
                └────────────┬──────────────────────┘
                             │
                ┌────────────▼──────────────────────┐
                │ USER SEES RESULT IN JUPYTER      │
                │ • Green (OK) / Yellow (Warn)     │
                │ • Red (Error)                    │
                │ • Copy-paste fixes included      │
                └──────────────────────────────────┘
```

---

## Module Structure

### 1. Connection Discovery (Lines 29-221)

**Purpose:** Find where Spark UI metrics are available

**Functions:**
- `_get_spark_context_info()` — Extract app ID and YARN host from SparkContext
- `_get_history_server_info()` — Check if Spark History Server is accessible (port 18080)
- `_get_yarn_spark_ui()` — Query YARN Resource Manager (port 8088) for driver location
- `_find_spark_port_and_host()` — Try all connection methods in priority order

**Priority Order (Most Reliable First):**
```
1. History Server (port 18080) - stable, works for live & completed jobs
   └─ Reads from event logs in real-time

2. YARN RM (port 8088) - good for EMR, gives tracking URL
   └─ Queries cluster resource manager

3. Direct Spark UI (port 4040-4045) - works if port not blocked
   └─ Connect directly to driver
```

**Why this order?** History Server is most reliable because:
- It always works (doesn't require driver to be accessible)
- It has latency but that's predictable and retryable
- On EMR with blocked ports, it's often the only option

---

### 2. REST API & Stage Capture (Lines 224-368)

**Purpose:** Fetch stage metrics from Spark

**Simple Functions:**
- `_get(path, host, port)` — Generic HTTP JSON fetch

**Stage Functions:**
- `_stage_ids_done()` — Get COMPLETE stage IDs before cell runs
- `_new_stages()` — Get NEW COMPLETE stages after cell runs
- `_get_all_stages()` — Get all stages (for debugging)
- `_capture_stages_with_retry()` — Smart wait loop for History Server latency

**Smart Retry Logic:**
```
History Server latency varies by job duration:

Very quick jobs (< 0.5s):
  └─ HS hasn't indexed yet, wait longer
  └─ Initial: 3.0s, max retries: 8, interval: 1.5s

Quick jobs (0.5-2.0s):
  └─ Initial: 2.0s, max retries: 6, interval: 1.0s

Normal jobs (2.0-5.0s):
  └─ Initial: 1.0s, max retries: 5, interval: 0.8s

Long jobs (> 5.0s):
  └─ Latency negligible, wait short
  └─ Initial: 0.8s, max retries: 2, interval: 0.6s

Global timeout: Never wait more than 15.0 seconds total
```

---

### 3. Formatters (Lines 375-395)

**Purpose:** Convert raw metrics to human-readable strings

- `_fmt_bytes()` — 1048576 → "1.0 MB"
- `_fmt_ms()` — 5000 → "5.0 s"
- `_fmt_elapsed()` — 125.3 → "2 min 5 s"

---

### 4. Advice Engine (Lines 398-593)

**Purpose:** Rules that detect performance issues and suggest fixes

**Structure:** List of rules, each with:
```python
{
    "trigger": lambda t, e: CONDITION,  # When to fire
    "level": "warn" or "error",         # Severity
    "title": "...",                      # Heading
    "what": "...",                       # What happened (with {subs})
    "why": "...",                        # Why it matters
    "fix": "...",                        # Solution (plain English)
    "code": "..."                        # Copy-paste code example
}
```

**Rules Included:**
1. **Disk Spill** (ERROR) — Data written to disk (10-100x slower than RAM)
2. **Memory Pressure** (WARN) — Close to disk spill
3. **Large Shuffle 500MB-1GB** (WARN) — Network transfer is expensive
4. **Very Large Shuffle >1GB** (ERROR) — Shuffle dominates runtime
5. **Few Tasks** (WARN) — Cluster underutilized
6. **High GC Time** (WARN) — Python UDFs causing overhead
7. **Slow Cell with No Data** (WARN) — Uncached lineage replay

---

### 5. HTML Building Blocks (Lines 596-756)

**Purpose:** Generate the visual report shown in Jupyter

**Components:**
- `_MONO` — Monospace font constant
- `_THEME` — Color schemes (error=red, warn=yellow, ok=green)
- `_advice_card()` — HTML for a single advice card
- `_metric_row()` — HTML for a single metrics table row
- `_per_stage_breakdown()` — HTML table with per-stage metrics

**Example Advice Card:**
```
🟡 Large shuffle — join moved 50GB
   What happened: Spark moved 50GB across the network
   Why it matters: Network transfers are expensive
   What to do: Use broadcast() if one table < 100MB
   [Code snippet]
```

---

### 6. Analysis & Report Rendering (Lines 763-1034)

**Purpose:** Analyze metrics and generate the final HTML report

**Functions:**
- `_find_problem_stages()` — Identify which stages have issues
- `_render_report()` — Main report generator (orchestrates everything)

**Report Sections:**
1. **Header** — App ID, number of stages, task count, elapsed time
2. **Health Banner** — Green (OK), Yellow (WARN), Red (ERROR)
3. **Full Metrics** — Collapsible table with aggregated metrics
4. **Per-Stage Breakdown** — Collapsible table showing each stage's metrics
5. **Advice Cards** — Actionable recommendations
6. **Footer** — Credit line

---

### 7. Cell Magic (Lines 1037-1127)

**Purpose:** Jupyter integration (the `%%measure` command)

**Flow:**
```python
@register_cell_magic
def measure(line, cell):
    # 1. Get user namespace (so exec() can access 'spark', 'df', etc.)
    user_ns = _get_jupyter_namespace()

    # 2. Check Spark connection
    if not connected:
        show error
        execute cell anyway (don't break their code)
        return

    # 3. Capture "before" state
    before = _stage_ids_done(...)

    # 4. Time and execute cell code
    t0 = time.monotonic()
    exec(cell, user_ns)  # Execute in USER's namespace
    elapsed = time.monotonic() - t0

    # 5. Capture "after" state with smart retry
    new = _capture_stages_with_retry(...)

    # 6. Detect ML libraries
    ml_libs = _detect_ml_patterns(cell)

    # 7. Render report
    _render_report(new, elapsed, ml_libs)
```

---

### 8. Initialization & Startup (Lines 1037-1201)

**Purpose:** Connect to Spark when module is loaded, show status

**What happens when `%run sparkmonitor.py` is executed:**

```python
# 1. Find Spark connection
_HOST, _PORT = _find_spark_port_and_host()
_APP_ID = _get_app_id(_HOST, _PORT)

# 2. Show appropriate status message
if connected and app_id found:
    ✓ "SparkMonitor is ready"
elif connected but no app_id:
    ⚠️ "Could not get app ID, re-run this cell"
elif not connected:
    ⚠️ "Could not connect to Spark, check configuration"
```

---

## Key Design Decisions

### 1. Why History Server, Not Direct Spark UI?

| Aspect | History Server | Direct UI |
|--------|----------------|-----------|
| Startup | Not needed | Must wait for driver |
| Port blocking | Doesn't matter | Blocked on EMR by default |
| After job ends | Still accessible | Driver gone, no metrics |
| Latency | 1-5 seconds | Immediate |
| Reliability | Very high | Depends on network/firewall |

**Decision:** Use History Server first, fall back to others if needed.

---

### 2. Why Aggregated + Per-Stage Metrics?

Users need to understand:
1. **Total work** — "Was this job slow?" (aggregated)
2. **Which stage** — "Which stage caused it?" (per-stage)
3. **How much** — "How much network traffic?" (per-stage amounts)

Example:
```
Total shuffle: 100GB
But which stage?
  Stage 0: 0GB (read - no shuffle)
  Stage 1: 50GB (groupBy - THIS ONE!)
  Stage 2: 50GB (join - AND THIS ONE!)
```

---

### 3. Why Smart Retry Logic?

History Server has variable latency:
- Quick jobs (< 0.5s): HS hasn't started indexing yet → wait 3.0s
- Normal jobs: HS catching up → wait 1.0-2.0s
- Long jobs: HS keeping up → wait 0.8s

Without this, quick jobs would timeout and show "No Spark work".

---

### 4. Why ML Library Detection?

When users pass a Spark DataFrame to sklearn/xgboost, they often don't realize Spark will recompute the entire lineage unless they cache it first.

Example:
```python
# SLOW: Spark recomputes df for every ML operation
df = spark.read.parquet(...)
df = df.filter(df.year == 2024)
# ... more transformations ...
df_pd = df.toPandas()  # Entire lineage recomputed here!
model.fit(df_pd, y)

# FAST: Cache the result first
df = df.cache()
df.count()  # Force cache load
df_pd = df.toPandas()  # Uses cached data
```

SparkMonitor detects this pattern and recommends caching.

---

## Common Code Paths

### Path 1: User Runs Cell with %%measure

```
Cell starts → measure() called
  ↓
Connect to Spark (History Server / YARN / Direct)
  ↓
Record current stage IDs
  ↓
Execute user's Python code (df.show(), etc.)
  ↓
Measure elapsed time
  ↓
Wait for History Server to index (smart retry)
  ↓
Get new stages that completed
  ↓
Analyze metrics
  ↓
Check advice rules against metrics
  ↓
Render HTML report with metrics, stages, and advice
```

### Path 2: Event Logging Not Enabled

```
Cell starts → measure() called
  ↓
Try to connect to History Server
  ↓
No event logs, stages never appear
  ↓
After timeout, new stages = []
  ↓
_render_report([])
  ↓
Shows "No Spark work ran in this cell"
```

---

## Performance Considerations

| Operation | Latency | Why |
|-----------|---------|-----|
| Connection check | < 100ms | Local network, port check |
| REST API call | 100-500ms | HTTP over network |
| Stage fetch | 200-1000ms | HS indexing may be slow |
| Report rendering | < 100ms | HTML generation |
| **Total for quick job** | 3-5 seconds | Mostly waiting for HS |

---

## Example: Full Flow for Simple Job

```python
# Cell runs: %%measure

# 1. START
before = {0, 1}  # Stages 0 and 1 already complete

# 2. EXECUTE CODE
t0 = monotonic()
spark.range(100).count()  # Your code
t1 = monotonic()
elapsed = 2.5 seconds

# 3. WAIT FOR HISTORY SERVER
# History Server is indexing the new stage
# elapsed < 5, so try: initial_wait=1.0, max_retries=5

# Try 1: sleep(1.0), ask HS → no new stages yet, keep trying
# Try 2: sleep(0.8), ask HS → new stages: [2]
# Found it!

# 4. ANALYZE
stages = [2]  # Only new stage
metrics = {
    "shuffleWriteBytes": 500 * 1024**2,  # 500 MB
    "diskBytesSpilled": 0,
    ...
}

# 5. CHECK ADVICE
Rule: 500MB < shuffle <= 1GB?
→ YES, trigger warning: "Large shuffle"

# 6. RENDER
Display:
  ✓ Green banner (not critical, just a warning)
  📊 Metrics table
  📋 Per-stage breakdown
  💡 Advice card: "Try broadcast() if one table < 100MB"
```

---

## Summary

SparkMonitor simplifies Spark performance debugging by:

1. **Finding metrics** — Auto-detects Spark UI location (3-tier fallback)
2. **Measuring work** — Captures stages before/after cell execution
3. **Smart waiting** — Adapts retry logic based on job duration
4. **Analysis** — Identifies performance issues automatically
5. **Clear advice** — Explains what happened, why it matters, and how to fix it
6. **Per-stage context** — Shows which specific stage is the bottleneck

**No Spark expertise required.** Users just add `%%measure` and read the plain-English report.
