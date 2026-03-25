# Real-Time Monitoring Enhancements

## New Features Added

### 1️⃣ **Real-Time Metrics Tab** ⚡
- **Live current metrics** displayed in a dedicated tab
- Updates every 30 seconds automatically
- Shows current values + status indicators (🟢 Good, 🟠 Warning, 🔴 Critical)
- Per-node view with quick node switching

**Metrics displayed:**
- CPU Usage (%)
- Memory Usage (%)
- Disk Usage (%)
- System Load (1min, 5min)
- YARN Pending Applications
- Network I/O (bytes in/out)

### 2️⃣ **System Load Metrics** 📊
Now monitoring system load averages:
- **1-minute load**: Current system load
- **5-minute load**: Medium-term trend
- **15-minute load**: Long-term trend

Useful for detecting:
- CPU contention
- I/O bottlenecks
- Capacity planning

### 3️⃣ **Network I/O Metrics** 🌐
Tracking network traffic:
- **Bytes In**: Incoming traffic
- **Bytes Out**: Outgoing traffic

Useful for detecting:
- Network-heavy operations
- Data transfer bottlenecks
- Unusual traffic patterns

### 4️⃣ **Configurable S3 Upload Frequency** ⏱️
Instead of just daily uploads, S3 now receives updates every **15 minutes** (configurable):

**Before:**
```
5 mins → Collect metrics (local only)
5 AM   → Save to S3 (once daily)
```

**After:**
```
5 mins    → Collect metrics (local)
15 mins   → Upload snapshot to S3 (pseudo real-time)
5 AM      → Daily peak report to S3
```

### 5️⃣ **Dual S3 Storage Format**

Two files per node in S3:

**`cluster_history.csv`** (Daily peaks)
```
Date,MaxCPU,MaxMem,MaxDisk,MaxPending,MaxYarnMem,MaxYarnCPU,MaxLoad1m,MaxLoad5m,MaxLoad15m,MaxNetBytesIn,MaxNetBytesOut
2024-03-25,85.5,72.3,65.1,5,8192,64,2.5,2.3,2.1,1048576,2097152
```

**`realtime_snapshot.json`** (Current metrics, updated every 15 min)
```json
{
  "timestamp": "2024-03-25T14:30:00.123456",
  "ip": "10.0.0.1",
  "cpu_percent": 45.2,
  "memory_percent": 62.1,
  "disk_percent": 55.3,
  "load_1m": 2.3,
  "load_5m": 2.1,
  "load_15m": 1.9,
  "net_bytes_in": 1048576,
  "net_bytes_out": 2097152,
  "yarn_pending": 3,
  "yarn_mem_allocated": 8192,
  "yarn_vcores_allocated": 64
}
```

---

## Configuration

### Change S3 Upload Frequency

Edit `cluster_monitor_and_alert.py`:

```python
# Default: 15 minutes
S3_UPLOAD_INTERVAL_MINUTES = 15  # Change to 5, 10, 20, 30, etc.
```

Examples:
- `5` - Every 5 minutes (more frequent, slightly higher S3 cost)
- `10` - Every 10 minutes (balanced)
- `15` - Every 15 minutes (default, recommended)
- `30` - Every 30 minutes (less frequent, lower cost)

### Change System Load Threshold

Edit `cluster_monitor_and_alert.py`:

```python
# Alert if load exceeds number of CPUs
if system_load["load_1m"] > psutil.cpu_count():
    alert_msg += f"- HIGH SYSTEM LOAD: {system_load['load_1m']} (CPUs: {psutil.cpu_count()})\n"
```

---

## Dashboard Changes

### New Real-Time Tab
- **Location**: Second tab after Overview
- **Auto-refresh**: Every 30 seconds when tab is active
- **Data source**: `realtime_snapshot.json` from S3
- **Status colors**:
  - 🟢 **Green** (Normal)
  - 🟠 **Orange** (Warning - approaching threshold)
  - 🔴 **Red** (Critical - threshold exceeded)

### Thresholds for Status Indicators

| Metric | Warning | Critical |
|--------|---------|----------|
| CPU (%) | > 60 | > 80 |
| Memory (%) | > 60 | > 80 |
| Disk (%) | > 60 | > 85 |
| System Load (1m) | > CPU count | > 2× CPU count |
| YARN Pending | > 5 | > 10 |

---

## API Endpoints

### New Real-Time Endpoints

**Get real-time data for all nodes:**
```bash
GET /api/realtime
```

Response:
```json
{
  "10.0.0.1": {
    "timestamp": "2024-03-25T14:30:00.123456",
    "cpu_percent": 45.2,
    ...
  },
  "10.0.0.2": {
    "timestamp": "2024-03-25T14:30:00.123456",
    "cpu_percent": 52.1,
    ...
  }
}
```

**Get real-time data for specific node:**
```bash
GET /api/realtime/10.0.0.1
```

Response:
```json
{
  "timestamp": "2024-03-25T14:30:00.123456",
  "ip": "10.0.0.1",
  "cpu_percent": 45.2,
  "memory_percent": 62.1,
  ...
}
```

---

## Performance & Costs

### S3 API Calls
With 5 nodes and 15-minute update interval:

```
Per node per day:
- cluster_history.csv: 1 PUT (daily at 5 AM)
- realtime_snapshot.json: 96 PUTs (every 15 min × 24h)

Total: 97 PUTs per node per day
5 nodes × 97 = 485 PUTs/day

Monthly cost: ~485 × 30 × $0.000005 = ~$0.07
```

Very minimal cost!

### Local Storage
- `cluster_history.csv`: Grows ~1 KB per day
- Total for 10 days × 5 nodes: ~50 KB

### Network Impact
- Each snapshot: ~500 bytes
- Every 15 min: ~32 KB/day per node
- Very minimal bandwidth

---

## Monitoring Workflow

### During 10-Day Test Run

**Hour 1-24:**
- Metrics collecting (every 5 min)
- Real-time snapshots uploading (every 15 min)
- Dashboard shows "Loading..." until first real-time snapshot arrives

**Day 1 at 5 AM:**
- First daily report generated
- `cluster_history.csv` updated in S3
- Dashboard Overview tab now shows data

**Day 1 onwards:**
- Real-time tab shows live metrics (auto-refreshing)
- Can check current system state in real-time
- Historical tabs show trends

**Day 10:**
- 10 days of peak data in `cluster_history.csv`
- Hundreds of real-time snapshots for detailed analysis
- Export data for further analysis

---

## Troubleshooting

### Real-Time Tab Shows "Loading..."
- **Cause**: No real-time snapshot uploaded yet
- **Solution**: Wait 15 minutes for first snapshot, then refresh

### Real-Time Data Disappears
- **Cause**: Monitor script stopped or S3 access lost
- **Solution**: Check monitor logs: `journalctl -u emr-monitor -f`

### Snapshots Not Uploading to S3
- **Cause**: S3 credentials or permissions issue
- **Solution**: Test S3 upload manually:
  ```bash
  aws s3 cp test.txt s3://sparksql-emr-monitoring/
  ```

### Dashboard Not Refreshing Real-Time
- **Cause**: Dashboard cached outdated data
- **Solution**: Hard refresh browser (Ctrl+Shift+R or Cmd+Shift+R)

---

## Files Modified

### cluster_monitor_and_alert.py
- ✅ Added `get_system_load()` function
- ✅ Added `get_network_metrics()` function
- ✅ Added `save_realtime_snapshot_to_s3()` function
- ✅ Added `upload_realtime_snapshot()` function
- ✅ Updated metrics tracking for new fields
- ✅ Added configurable `S3_UPLOAD_INTERVAL_MINUTES`
- ✅ Added scheduled task for 15-min S3 uploads

### emr_dashboard.py
- ✅ Added `get_realtime_snapshot_from_s3()` function
- ✅ Added `/api/realtime` endpoint
- ✅ Added `/api/realtime/<ip>` endpoint

### templates/dashboard.html
- ✅ Added Real-Time tab with auto-refresh
- ✅ Added real-time status display
- ✅ Added CSS for status indicators
- ✅ Added JavaScript for auto-refresh (30 sec)
- ✅ Added per-node real-time navigation

---

## Next Steps

1. **Deploy updated scripts** to EMR master nodes
2. **Verify** real-time snapshots appear in S3 after 15 minutes
3. **Check** Real-Time tab auto-refreshes every 30 seconds
4. **Monitor** daily trends in historical tabs
5. **Analyze** network & load metrics for capacity planning

---

## Summary

✨ **Now you have:**
- ⚡ Real-time metrics view (updates every 30 sec)
- 📊 System load monitoring (1m, 5m, 15m)
- 🌐 Network I/O tracking
- 📈 S3 snapshots every 15 minutes
- 🎯 Color-coded status indicators
- 🔄 Auto-refreshing dashboard

All with **minimal S3 costs** (~$0.07/month)!
