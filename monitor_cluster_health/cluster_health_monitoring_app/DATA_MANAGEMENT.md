# 10-Day Rolling Data Management Guide

## Overview

Data is stored in **two locations** with different retention strategies:

```
┌─────────────────────────────────────────────────────────────┐
│                    MONITORING DATA FLOW                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Every 5 minutes:                                           │
│  ┌─────────────────────────────────────────┐               │
│  │  Collect metrics                        │               │
│  │  (CPU, Memory, Disk, Load, Network)     │               │
│  └──────────────┬──────────────────────────┘               │
│                 │                                            │
│         ┌───────┴───────┐                                   │
│         ▼               ▼                                    │
│    LOCAL CSV      IN MEMORY                                │
│  (Rolling Daily)   (Peak Tracking)                         │
│                                                              │
│         Every 15 minutes:                                   │
│         ┌──────────────┐                                   │
│         ▼              ▼                                    │
│      S3 JSON       S3 CSV                                  │
│  (Snapshots)   (Daily Peaks)                               │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Storage Locations & Retention

### 1. **Local CSV** (On EMR Master Node)

**File**: `cluster_history.csv`
**Location**: Working directory where script runs
**Content**: Daily peak statistics
**Update**: Once daily at 5:00 AM
**Retention**: 28 days (then archives & resets)

**Format**:
```
Date,MaxCPU,MaxMem,MaxDisk,MaxPending,MaxYarnMem,MaxYarnCPU,MaxLoad1m,MaxLoad5m,MaxLoad15m,MaxNetBytesIn,MaxNetBytesOut
2024-03-25,85.5,72.3,65.1,5,8192,64,2.5,2.3,2.1,1048576,2097152
2024-03-26,88.2,75.1,66.3,8,8192,64,2.8,2.5,2.3,1572864,3145728
...
```

**What it tracks**: Peak values only (maximum during the day)

---

### 2. **S3 Storage** (Cloud Backup + Real-time)

#### A. Daily Peaks CSV
**Location**: `s3://sparksql-emr-monitoring/{IP}/cluster_history.csv`
**Content**: Same as local CSV (daily peaks)
**Update**: Once daily at 5:00 AM
**Retention**: Permanent (you control via lifecycle policies)
**Backup**: YES - This is your permanent historical record

**Example path**:
```
s3://sparksql-emr-monitoring/10.0.0.1/cluster_history.csv
s3://sparksql-emr-monitoring/10.0.0.2/cluster_history.csv
... (one per EMR node)
```

#### B. Real-Time Snapshots JSON
**Location**: `s3://sparksql-emr-monitoring/{IP}/realtime_snapshot.json`
**Content**: Current metrics snapshot
**Update**: Every 15 minutes (configurable)
**Retention**: Permanent (overwrites previous snapshot)
**Purpose**: Live dashboard data (not historical)

**Example**:
```json
{
  "timestamp": "2024-03-25T14:30:00.123456",
  "cpu_percent": 45.2,
  "memory_percent": 62.1,
  "disk_percent": 55.3,
  ...
}
```

---

## 10-Day Test Run Timeline

### **Day 1**
```
Start → Monitor script begins
        ├─ Local: cluster_history.csv created (empty)
        ├─ Memory: Tracking peak values
        └─ S3: Waiting for first 5 AM report
```

### **Day 1 at 5:00 AM**
```
First Daily Report
        ├─ Local: 1 row added (Day 1 peaks)
        ├─ S3: cluster_history.csv uploaded (1 row)
        ├─ Email: Daily report sent
        └─ Memory: Peak counters reset
```

### **Days 2-9**
```
Each day at 5:00 AM:
        ├─ Local: 1 new row added (Day N peaks)
        ├─ S3: cluster_history.csv updated (N rows)
        ├─ Email: Daily report sent
        └─ Memory: Peak counters reset

After 28 days (not applicable for 10-day run):
        ├─ Local: CSV archived as archive_stats_YYYYMMDD.csv
        ├─ Local: cluster_history.csv reset (empty)
        └─ S3: unaffected (keeps full history)
```

### **Day 10 (End of Test)**
```
Final State:
        ├─ Local: cluster_history.csv (10 rows, 10 days of data)
        ├─ S3: cluster_history.csv (10 rows, permanent backup)
        ├─ S3: realtime_snapshot.json (latest snapshot)
        └─ Real-time snapshots: ~960 per node (96/day × 10 days)
```

---

## Data Storage Breakdown (10-day test)

### Per Node Storage

**Local CSV** (on EMR master):
- 10 rows × ~200 bytes = ~2 KB per node
- 5 nodes × 2 KB = **10 KB total**

**S3 - CSV**:
- 10 rows × ~200 bytes = ~2 KB per node
- 5 nodes × 2 KB = **10 KB total**

**S3 - Real-time snapshots**:
- 1 snapshot per 15 min × 96 per day × 10 days = 960 snapshots
- 960 × ~500 bytes = ~480 KB per node
- 5 nodes × 480 KB = **2.4 MB total** (highly compressible)

**Total storage**: ~2.4 MB (minimal!)

---

## Configuration: How to Manage Retention

### Change Local CSV Rollover Period

Edit `cluster_monitor_and_alert.py`:

```python
# Currently: Archives after 28 days
if len(history) >= 28:  # ← Change this number
    last_4_weeks = history[-28:]  # ← And this
    ...
    os.rename(STATS_FILE, f"archive_stats_{datetime.now().strftime('%Y%m%d')}.csv")
    init_stats_file()
```

**For 10-day test run** (keep all data):
```python
if len(history) >= 10:  # Store for 10 days
    # Or change to a larger number like 90 for 3 months
```

### Change S3 Upload Frequency

Edit `cluster_monitor_and_alert.py`:

```python
S3_UPLOAD_INTERVAL_MINUTES = 15  # Change to 5, 10, 20, 30, etc.
```

**Impact on snapshots**:
- 5 min interval: 288 snapshots/day × 10 = 2,880 total
- 15 min interval: 96 snapshots/day × 10 = 960 total (default)
- 30 min interval: 48 snapshots/day × 10 = 480 total

---

## Accessing the Data

### View Local CSV on EMR Master

```bash
# See all data
cat cluster_history.csv

# See last 5 days
tail -5 cluster_history.csv

# See specific day
grep "2024-03-25" cluster_history.csv
```

### Download from S3

```bash
# Single node
aws s3 cp s3://sparksql-emr-monitoring/10.0.0.1/cluster_history.csv ./

# All nodes
aws s3 sync s3://sparksql-emr-monitoring/ ./backups/

# Specific date range (requires filtering after download)
aws s3 cp s3://sparksql-emr-monitoring/10.0.0.1/ ./ --recursive
```

### View in Dashboard

```
http://localhost:5000
├─ Overview tab     → Daily peaks (from S3 CSV)
├─ Real-Time tab    → Latest snapshot (from S3 JSON)
├─ Health Status    → Current status (from S3 JSON)
├─ CPU/Mem/Disk     → 60-day trends (from S3 CSV)
└─ Details          → Last 10 days table (from S3 CSV)
```

---

## Post-10-Day Analysis

### Export Final Data

```bash
# Backup everything
aws s3 sync s3://sparksql-emr-monitoring/ ./final_backup/

# Archive locally
mkdir -p ~/emr_monitoring_results/2024-03-25_to_04-04
cp final_backup/* ~/emr_monitoring_results/2024-03-25_to_04-04/
tar -czf emr_monitoring_results.tar.gz ~/emr_monitoring_results/
```

### Analyze Data

**Option 1: Use Dashboard**
- Check all trend charts (60-day view will show your 10 days)
- Export images/screenshots for reports

**Option 2: Process CSV Files**
```python
import pandas as pd

# Load 10-day data
df = pd.read_csv('cluster_history.csv')

# Statistics
print(f"Average CPU: {df['MaxCPU'].mean():.2f}%")
print(f"Peak CPU: {df['MaxCPU'].max():.2f}%")
print(f"Days above 80% CPU: {(df['MaxCPU'] > 80).sum()}")

# Per-node breakdown
df.groupby('IP')[['MaxCPU', 'MaxMem', 'MaxDisk']].describe()
```

---

## Disaster Recovery

### If Monitor Script Stops

**Data is safe because**:
- Local CSV has all data up to last 5 AM
- S3 has permanent backup
- Real-time snapshots stop updating but historical data remains

**To resume**:
```bash
# On EMR master
python cluster_monitor_and_alert.py

# Data collection resumes
# No data loss (picks up from next 5 AM)
```

### If Local CSV Gets Corrupted

**Recover from S3**:
```bash
aws s3 cp s3://sparksql-emr-monitoring/10.0.0.1/cluster_history.csv ./cluster_history.csv
```

### If You Need More Than 28 Days of Local Storage

**Change retention in script**:
```python
# Current: 28 days before archiving
if len(history) >= 28:

# Change to: 365 days (1 year)
if len(history) >= 365:
```

---

## Daily Operations

### Monitor 5 AM Upload

```bash
# Check logs
journalctl -u emr-monitor --since "today" | grep "Stats saved"

# Verify S3 upload
aws s3 ls s3://sparksql-emr-monitoring/10.0.0.1/ --human-readable
```

### Check Data Growth

```bash
# Local CSV size
ls -lh cluster_history.csv

# S3 bucket size
aws s3 ls s3://sparksql-emr-monitoring/ --summarize --human-readable
```

### Monitor Dashboard

```bash
# Check if data is visible
curl http://localhost:5000/api/stats | jq

# Check health status
curl http://localhost:5000/api/health-overview | jq

# Real-time snapshot
curl http://localhost:5000/api/realtime/10.0.0.1 | jq
```

---

## Best Practices for 10-Day Test

1. **Backup before stopping**:
   ```bash
   aws s3 sync s3://sparksql-emr-monitoring/ ~/backup_day10/
   ```

2. **Keep local CSVs**:
   - Don't delete `cluster_history.csv` during test
   - Useful for comparing local vs. S3 data

3. **Monitor S3 costs**:
   - 10 days × 5 nodes × 97 PUTs = ~4,850 requests
   - Cost: ~$0.02

4. **Document findings**:
   - Screenshot dashboard tabs
   - Export CSV files with timestamps
   - Note any unusual patterns

5. **Export at end**:
   ```bash
   aws s3 sync s3://sparksql-emr-monitoring/ ./final_data_10day/
   ```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    10-DAY DATA LIFECYCLE                        │
└─────────────────────────────────────────────────────────────────┘

EMR Master Node 1 (10.0.0.1)
├─ Every 5 min
│  ├─ Collect metrics
│  └─ Update in-memory peaks
│
├─ Every 15 min (S3_UPLOAD_INTERVAL_MINUTES)
│  └─ Upload realtime_snapshot.json to S3
│
├─ Daily at 5 AM
│  ├─ Save row to local cluster_history.csv
│  └─ Upload cluster_history.csv to S3
│
└─ After 28 days (not applicable for 10-day test)
   ├─ Archive local CSV
   └─ Reset for next 28 days

S3 Bucket (sparksql-emr-monitoring)
├─ 10.0.0.1/
│  ├─ cluster_history.csv (daily peaks - permanent)
│  └─ realtime_snapshot.json (latest - overwrites)
├─ 10.0.0.2/
│  ├─ cluster_history.csv
│  └─ realtime_snapshot.json
└─ ... (3 more nodes)

Dashboard (http://localhost:5000)
├─ Reads from S3 every 30 sec
├─ Shows trends (CSV data)
└─ Shows real-time (JSON data)
```

---

## Summary: 10-Day Test Data Management

| Aspect | Details |
|--------|---------|
| **Local Storage** | cluster_history.csv (10 rows after 10 days) |
| **S3 Backup** | cluster_history.csv (permanent) |
| **Real-time Data** | ~960 snapshots per node |
| **Total Size** | ~2.4 MB (highly compressible) |
| **Cost** | ~$0.02 for 10-day test |
| **Retention** | 28 days local, permanent on S3 |
| **Recovery** | Full backup always on S3 |
| **Access** | Dashboard + CSV export |

✅ **Everything is automatically managed!** Just run the scripts and let them collect data.

---

## Quick Reference: File Locations

```
LOCAL (EMR Master Node):
  /path/to/cluster_history.csv          # Daily peaks (10 rows after 10 days)

S3:
  s3://sparksql-emr-monitoring/{IP}/cluster_history.csv      # Permanent backup
  s3://sparksql-emr-monitoring/{IP}/realtime_snapshot.json    # Latest snapshot

DASHBOARD:
  http://localhost:5000                 # All data accessible here
```
