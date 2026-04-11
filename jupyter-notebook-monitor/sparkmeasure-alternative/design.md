# Spark Monitor Architecture

## Overview

This tool collection monitors Spark jobs on EMR/YARN in three scenarios:
1. **Live jobs** — use `%%measure` during execution
2. **Completed jobs** — use `analyze_job()` after completion
3. **Any job** — use YARN + History Server when direct access is blocked

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Spark Driver (running on cluster)                           │
│  • Executes SQL/DataFrame operations                        │
│  • Writes event log (to HDFS/S3)                            │
│  • Exposes Spark UI on port 4040 (driver-local)             │
└────┬────────────────────────────────────────────────────────┘
     │
     ├─→ Event Log (HDFS/S3)
     │    └─→ Spark History Server (port 18080)
     │         └─→ REST API: /api/v1/applications/{appId}/*
     │
     ├─→ YARN Resource Manager (port 8088)
     │    └─→ REST API: /ws/v1/cluster/apps/{appId}
     │         • Tracks app location (driver host:port)
     │         • Provides tracking URL (→ Spark UI)
     │
     └─→ Spark UI (port 4040 on driver)
          └─→ REST API: /api/v1/applications/{appId}/*
               • Live metrics during execution
               • Network access may be blocked
```

---

## Connection Priority

### For `%%measure` (Live Jobs)

```
1. Spark History Server (localhost:18080)
   ✓ Reads event logs in real-time
   ✓ Works even if driver UI blocked
   ✓ Stable endpoint
   ✓ Same metrics as live Spark UI

2. YARN RM → Spark UI (localhost:8088 → driver:4040)
   ✓ Get driver location from YARN
   ✓ Direct to driver UI if accessible
   ✗ Requires network access to driver

3. sparkContext.getWebUI()
   ✓ Ask driver directly where its UI is
   ✗ Requires direct JVM access

4. localhost:4040-4045
   ✓ Local driver detection
   ✗ Only works on master node
```

**Why History Server first?**
- History Server **reads event logs in real-time** — it has live metrics even during execution
- No network blocking issues (separate stable service)
- Works in all network topologies
- Doesn't require direct driver access

### For `analyze_job()` (Completed Jobs)

```
1. Spark History Server (localhost:18080)
   ✓ Permanent data storage
   ✓ Always available
   ✓ Can analyze jobs from days/weeks ago
```

---

## What Each Service Provides

### YARN Resource Manager (`localhost:8088`)

**Purpose:** Cluster resource management & app tracking

**Available endpoints:**
- `/ws/v1/cluster/apps` — list all applications
- `/ws/v1/cluster/apps/{appId}` — app status, location, queue
- `/ws/v1/cluster/apps/{appId}/statistics` — resource usage (limited)

**Does NOT provide:**
- ❌ Stage metrics (number of tasks, shuffle bytes, etc.)
- ❌ Task details
- ❌ Memory spill metrics
- ❌ GC time
- ❌ Executor details

**Conclusion:** YARN can't be a replacement for Spark UI, but it can tell us where the driver is running.

---

### Spark History Server (`localhost:18080`)

**Purpose:** Persistent historical data + real-time event streaming

**Available endpoints:**
- `/api/v1/applications` — list all (running & completed)
- `/api/v1/applications/{appId}/stages` — detailed stage metrics
- `/api/v1/applications/{appId}/executors` — executor info
- `/api/v1/applications/{appId}/tasks` — task details

**Data source:**
- Reads Spark event logs (`spark-events/` directory or S3)
- Updates in real-time as events are written by driver
- Persists after driver exits

**Advantages:**
- ✓ Works for LIVE jobs (reads events as they stream)
- ✓ Works for completed jobs
- ✓ Stable endpoint (separate service)
- ✓ Survives driver restarts
- ✓ No network blocking issues

---

### Spark UI (`driver-host:4040`)

**Purpose:** Live metrics during job execution

**Available endpoints:**
- Same REST API as History Server
- `/api/v1/applications/{appId}/stages`
- `/api/v1/applications/{appId}/executors`

**Data source:**
- Runs on Spark driver process
- Real-time metrics from running executors

**Limitations:**
- ❌ Only accessible if driver host reachable
- ❌ Only works while driver is running
- ❌ May be blocked by network/firewall

---

## Why This Matters for EMR

**Problem:** Jupyter on EMR is often on a different machine (or remote laptop) than the Spark driver, so:
- `localhost:4040` doesn't reach the driver
- Spark driver host may not be accessible from Jupyter's network

**Solution:** Use Spark History Server instead
- Runs on master node (like YARN RM)
- Has the same detailed metrics as live Spark UI
- Reads events in real-time from event logs
- No network access to driver needed

---

## Configuration

### Spark Event Logging

For History Server to work, Spark must log events:

```python
# In EMR bootstrap or Jupyter:
spark = SparkSession.builder \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "s3://your-bucket/spark-events/") \
    .config("spark.eventLog.compression.codec", "snappy") \
    .getOrCreate()
```

Or in `spark-defaults.conf`:
```
spark.eventLog.enabled           true
spark.eventLog.dir               s3://your-bucket/spark-events/
spark.eventLog.compression.codec snappy
```

### History Server Location

For this tool, edit the host/port in code:

```python
# sparkmonitor.py
_HOST, _PORT = _find_spark_port_and_host()

# analyze_job.py
analyze_job("app-123", host="localhost", port=18080)
```

---

## Troubleshooting

### History Server metrics are delayed
- History Server reads events with a lag (~1-5 seconds)
- Normal behavior, events eventually appear
- Solution: Spark UI has instant metrics if accessible

### History Server has no data
- Check: Is `spark.eventLog.enabled = true`?
- Check: Is event log directory readable (HDFS/S3)?
- Check: Is History Server pointed to correct directory?

### Can't reach localhost:18080
- If Jupyter is remote: set up SSH tunnel
  ```bash
  ssh -L 18080:localhost:18080 emr-master
  ```
- Or update code to use actual History Server host:
  ```python
  analyze_job("app-123", host="emr-master", port=18080)
  ```

### Spark UI metrics more recent than History Server
- History Server lags by ~1-5 seconds
- If you need instant metrics, use Spark UI if accessible
- For most analysis, this lag doesn't matter

---

## Summary

| Feature | YARN RM | History Server | Spark UI |
|---------|---------|---|----------|
| **Live job metrics** | ❌ Limited | ✅ Yes (real-time) | ✅ Yes |
| **Completed job data** | ❌ No | ✅ Yes | ❌ No |
| **Always accessible** | ✅ Yes | ✅ Yes | ❌ Network-dependent |
| **Spark detail metrics** | ❌ No | ✅ Yes | ✅ Yes |
| **Location** | Cluster | Cluster | Driver node |

**Best approach:** Use History Server as primary (stable, always works), fall back to Spark UI if available (for instant metrics), use YARN to find driver if needed.
