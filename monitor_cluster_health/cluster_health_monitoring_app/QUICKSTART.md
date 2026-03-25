# Quick Start Guide - EMR Cluster Health Monitoring

## 30-Second Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Deploy on EMR Master Nodes (Run once on each of 5 masters)

**Option A: One-liner with auto-detect IP**
```bash
python cluster_monitor_and_alert.py
```

**Option B: Explicit IP**
```bash
python cluster_monitor_and_alert.py --ip 10.0.0.1
```

The script will:
- ✅ Auto-detect local IP (if not provided)
- ✅ Save metrics every 5 minutes
- ✅ Save daily peak stats to local CSV
- ✅ Upload daily stats to S3 at 5:00 AM
- ✅ Send daily/weekly email reports

### 3. Start Dashboard (On central monitoring server)

```bash
python emr_dashboard.py
```

Then open: **http://localhost:5000**

---

## What Happens Every 5 Minutes

```
┌─────────────────────────────────┐
│  Collect Metrics                │
│  - CPU %                         │
│  - Memory %                      │
│  - Disk %                        │
│  - YARN Pending Apps             │
│  - YARN Allocated Resources      │
└─────────────────────────────────┘
           ↓ Every 5 min
┌─────────────────────────────────┐
│  Track Daily Peaks               │
│  (Keep max values for the day)   │
└─────────────────────────────────┘
           ↓ At 5:00 AM
┌─────────────────────────────────┐
│  Save Daily Report               │
│  ├─ Local: cluster_history.csv   │
│  ├─ S3: {IP}/cluster_history.csv │
│  └─ Email: Daily + 4-week report │
└─────────────────────────────────┘
```

---

## File Structure

```
monitor_cluster_health/
├── cluster_monitor_and_alert.py    # Deploy on each EMR master
├── emr_dashboard.py                # Run on central monitoring server
├── requirements.txt                # Python dependencies
├── templates/
│   └── dashboard.html              # Web UI (auto-served by Flask)
├── DEPLOYMENT.md                   # Detailed setup guide
└── QUICKSTART.md                   # This file
```

---

## S3 Bucket Structure

After running for a few days:

```
sparksql-emr-monitoring/
├── 10.0.0.1/
│   └── cluster_history.csv         # EMR 1 stats
├── 10.0.0.2/
│   └── cluster_history.csv         # EMR 2 stats
├── 10.0.0.3/
│   └── cluster_history.csv         # EMR 3 stats
├── 10.0.0.4/
│   └── cluster_history.csv         # EMR 4 stats
└── 10.0.0.5/
    └── cluster_history.csv         # EMR 5 stats
```

---

## Dashboard Features

| Tab | What You See |
|-----|--------------|
| **Overview** | Current status cards - CPU, Memory, Disk for all 5 nodes |
| **CPU Usage** | Line chart of CPU trends over 60 days |
| **Memory Usage** | Line chart of memory trends over 60 days |
| **Disk Usage** | Line chart of disk trends over 60 days |
| **YARN Resources** | Pending apps & allocated memory/vCores |
| **Details** | Table view - drill into specific node data |

---

## Configuration Files

### Update IPs in Dashboard

Edit `emr_dashboard.py`:
```python
MASTER_NODE_IPS = [
    "10.0.0.1",    # EMR 1 - CHANGE TO YOUR IPs
    "10.0.0.2",    # EMR 2
    "10.0.0.3",    # EMR 3
    "10.0.0.4",    # EMR 4
    "10.0.0.5"     # EMR 5
]
```

### Update S3 Bucket Name

Edit both files:
```python
S3_BUCKET = "sparksql-emr-monitoring"  # Change to your bucket name
```

### Adjust Thresholds

Edit `cluster_monitor_and_alert.py`:
```python
CPU_THRESHOLD = 90.0       # Alert if CPU > 90%
MEM_THRESHOLD = 90.0       # Alert if Memory > 90%
DISK_THRESHOLD = 85.0      # Alert if Disk > 85%
PENDING_THRESHOLD = 10     # Alert if pending apps > 10
```

---

## Common Commands

### Check Monitoring Status
```bash
# View last 10 lines of stats file
tail -10 cluster_history.csv

# Check S3 bucket
aws s3 ls sparksql-emr-monitoring/

# List stats for specific node
aws s3 ls sparksql-emr-monitoring/10.0.0.1/
```

### Download Stats Locally
```bash
aws s3 cp sparksql-emr-monitoring/10.0.0.1/cluster_history.csv ./
```

### View Logs (if running as systemd service)
```bash
journalctl -u emr-monitor -f          # Follow logs
journalctl -u emr-monitor --lines 100 # Last 100 lines
```

---

## Testing the Setup

### 1. Test Monitor Script (5-minute collection)
```bash
# Start in foreground
python cluster_monitor_and_alert.py --ip 10.0.0.1

# Wait 5 minutes, then Ctrl+C
# Check: cluster_history.csv should have 1 row
cat cluster_history.csv
```

### 2. Test Dashboard
```bash
# In a separate terminal, start dashboard
python emr_dashboard.py

# Open browser: http://localhost:5000
# You should see "Loading..." placeholders
```

### 3. Wait for First Daily Report
- Monitoring must run past 5:00 AM to trigger daily save
- After that, S3 will have the CSV file
- Dashboard will populate once data is in S3

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| **Script won't start** | `pip install -r requirements.txt` first |
| **No IP detected** | Use `--ip 10.0.0.1` to set manually |
| **S3 upload fails** | Check AWS credentials & IAM permissions |
| **Dashboard shows no data** | Wait 24 hours for first daily report, then refresh |
| **YARN metrics missing** | Check: `curl http://localhost:8088/ws/v1/cluster/metrics` |

---

## Next Steps

1. **Deploy monitors** on all 5 EMR master nodes
2. **Start dashboard** on central server
3. **Wait 24 hours** for first data collection
4. **Access dashboard** and monitor trends
5. **Adjust thresholds** based on your workload
6. **Configure email alerts** (SMTP server in script)

See **DEPLOYMENT.md** for advanced setup (systemd services, production deployment, etc.)
