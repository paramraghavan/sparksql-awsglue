# EMR Multi-Cluster Health Monitoring Solution

## Overview

Complete monitoring solution for **5 EMR clusters** collecting stats every 5 minutes, saving to both local CSV and S3, with a web-based dashboard for visualization.

### What's Included

```
📦 monitor_cluster_health/
├── 🔧 cluster_monitor_and_alert.py    # Monitoring agent (deploy on each EMR master)
├── 🌐 emr_dashboard.py                # Web dashboard (deploy on central server)
├── 📄 templates/dashboard.html        # Interactive UI with Chart.js
├── 📋 requirements.txt                # Python dependencies
├── 📖 QUICKSTART.md                   # Fast setup (5 min read)
├── 📖 DEPLOYMENT.md                   # Detailed guide (production-ready)
├── ✅ SETUP_CHECKLIST.md              # Step-by-step checklist
└── 📖 README_MONITORING.md            # This file
```

---

## Features

### Monitoring Script (`cluster_monitor_and_alert.py`)

✅ **Metrics Collected** (every 5 minutes):
- CPU usage (%)
- Memory usage (%)
- Disk usage (%)
- YARN pending applications
- YARN allocated memory & vCores

✅ **Data Storage**:
- **Local**: Daily peak stats in `cluster_history.csv`
- **S3**: Automated daily uploads with IP-based prefixes
- **Daily rotation**: Archives old files automatically

✅ **Reporting**:
- Daily report at 5:00 AM (email)
- 4-week trend summary (email)
- Peak resource tracking

✅ **Smart Features**:
- Auto-detects local IP (or accepts --ip argument)
- Single-instance locking (prevents duplicate runs)
- Graceful cleanup on shutdown
- YARN metrics integration
- Process identification for alerts

### Dashboard (`emr_dashboard.py`)

✅ **Real-Time Visualization**:
- Overview cards showing current status for all 5 nodes
- 60-day trend charts (CPU, Memory, Disk)
- YARN resource metrics
- Detailed per-node tables

✅ **Color-Coded Alerts**:
- 🟢 Green (Normal): < 60%
- 🟠 Orange (Warning): 60-80%
- 🔴 Red (Danger): > 80%

✅ **API Endpoints**:
- `/api/stats` - Summary for all nodes
- `/api/chart/{metric}` - Chart data
- `/api/node/{ip}` - Node-specific details

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    EMR Cluster 1-5                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ Master Node  │  │ Master Node  │  │ Master Node  │  ...     │
│  │ 10.0.0.1    │  │ 10.0.0.2    │  │ 10.0.0.3    │           │
│  │              │  │              │  │              │           │
│  │ monitor.py ├──┼──┤ monitor.py ├──┼──┤ monitor.py ├──┐       │
│  └──────────────┘  └──────────────┘  └──────────────┘  │       │
└─────────────────────────────────────────────────────────┼───────┘
                                                           │
                                   ┌───────────────────────┘
                                   │
                                   ↓
                        ┌──────────────────────┐
                        │   S3 Bucket          │
                        │  (JSON/CSV Storage)  │
                        │  - 10.0.0.1/csv      │
                        │  - 10.0.0.2/csv      │
                        │  - 10.0.0.3/csv      │
                        │  - 10.0.0.4/csv      │
                        │  - 10.0.0.5/csv      │
                        └──────────────────────┘
                                   ↑
                                   │
                        ┌──────────────────────┐
                        │  Dashboard Server    │
                        │  (Flask App)         │
                        │  :5000               │
                        └──────────────────────┘
                                   ↑
                                   │
                        ┌──────────────────────┐
                        │   Web Browser        │
                        │  http://localhost:5000
                        └──────────────────────┘
```

---

## Quick Start (5 minutes)

### 1. **Install**
```bash
pip install -r requirements.txt
```

### 2. **Deploy Monitors** (On each of 5 EMR master nodes)
```bash
# Auto-detect IP
python cluster_monitor_and_alert.py

# Or specify IP
python cluster_monitor_and_alert.py --ip 10.0.0.1
```

### 3. **Start Dashboard** (On central monitoring server)
```bash
python emr_dashboard.py
```
Open: **http://localhost:5000**

### 4. **Wait for Data**
- Monitors start collecting immediately (every 5 minutes)
- First daily report at 5:00 AM
- Data appears in S3 and dashboard after ~24 hours

---

## Data Flow

```
┌─────────────────────────┐
│  Every 5 minutes        │
│  Collect Metrics        │
└────────────┬────────────┘
             │
             ↓
┌─────────────────────────┐
│  Track Daily Peaks      │
│  (Max values)           │
└────────────┬────────────┘
             │
      ┌──────┴──────┐
      │             │
      ↓             ↓
  Local CSV      In Memory
      │
      └──────┬──────┐
             │      │
         Every     At 5 AM
         usage    Daily
         check    Report
                      │
                      ↓
            ┌─────────────────┐
            │ Save + Upload   │
            │ to S3           │
            └────────┬────────┘
                     │
                     ↓
            ┌─────────────────┐
            │  Dashboard      │
            │  Reads from S3  │
            │  Displays UI    │
            └─────────────────┘
```

---

## Configuration

### Before Deploying

**Edit both files:**
```python
S3_BUCKET = "sparksql-emr-monitoring"  # Your bucket name
```

**Dashboard only:**
```python
MASTER_NODE_IPS = [
    "10.0.0.1",  # Your actual IPs
    "10.0.0.2",
    "10.0.0.3",
    "10.0.0.4",
    "10.0.0.5"
]
```

**Monitor script only:**
```python
CPU_THRESHOLD = 90.0
MEM_THRESHOLD = 90.0
DISK_THRESHOLD = 85.0
PENDING_THRESHOLD = 10
AUTO_KILL = False  # Alert without killing

EMAIL_SENDER = "monitor@yourdomain.com"
EMAIL_RECEIVER = "admin@yourdomain.com"
SMTP_SERVER = "localhost"
```

---

## Deployment Options

### 1. **Development** (Quick testing)
```bash
python cluster_monitor_and_alert.py
python emr_dashboard.py
```

### 2. **Systemd Service** (Recommended for 10-day run)
```bash
# Create service file
sudo nano /etc/systemd/system/emr-monitor.service

# Start
sudo systemctl enable emr-monitor
sudo systemctl start emr-monitor
sudo systemctl status emr-monitor

# View logs
journalctl -u emr-monitor -f
```

### 3. **Production** (High availability)
```bash
# Dashboard with Gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 emr_dashboard:app

# Behind Nginx proxy
# (See DEPLOYMENT.md for config)
```

---

## Dashboard Tabs

| Tab | Description |
|-----|-------------|
| **Overview** | Current status cards for all 5 nodes |
| **CPU Usage** | 60-day CPU trend chart |
| **Memory Usage** | 60-day memory trend chart |
| **Disk Usage** | 60-day disk trend chart |
| **YARN Resources** | Pending apps & allocated resources |
| **Details** | Table view - last 10 days per node |

---

## S3 Storage Structure

After running for several days:

```
s3://sparksql-emr-monitoring/
├── 10.0.0.1/cluster_history.csv      # EMR 1 (5-min to daily rollup)
├── 10.0.0.2/cluster_history.csv      # EMR 2
├── 10.0.0.3/cluster_history.csv      # EMR 3
├── 10.0.0.4/cluster_history.csv      # EMR 4
└── 10.0.0.5/cluster_history.csv      # EMR 5
```

Each CSV contains:
```
Date,MaxCPU,MaxMem,MaxDisk,MaxPending,MaxYarnMem,MaxYarnCPU
2024-03-25,85.5,72.3,65.1,5,8192,64
2024-03-26,88.2,75.1,66.3,8,8192,64
```

---

## AWS Permissions Required

### For Monitoring Scripts (On EMR Masters)
```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:PutObject"],
        "Resource": "arn:aws:s3:::sparksql-emr-monitoring/*"
    }]
}
```

### For Dashboard (Central Server)
```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:GetObject"],
        "Resource": "arn:aws:s3:::sparksql-emr-monitoring/*"
    }]
}
```

---

## API Examples

```bash
# Get summary for all nodes
curl http://localhost:5000/api/stats | jq

# Get CPU trend data
curl http://localhost:5000/api/chart/MaxCPU | jq

# Get node-specific data (last 10 days)
curl http://localhost:5000/api/node/10.0.0.1 | jq
```

---

## Troubleshooting

### Monitor won't start
```bash
# Check dependencies
pip install -r requirements.txt

# Test with explicit IP
python cluster_monitor_and_alert.py --ip 10.0.0.1

# Check logs
tail -20 /var/log/syslog  # or journalctl
```

### S3 upload fails
```bash
# Test AWS credentials
aws s3 ls sparksql-emr-monitoring/

# Check IAM permissions
aws iam get-user

# Manual S3 test
aws s3 cp cluster_history.csv s3://sparksql-emr-monitoring/test.csv
```

### Dashboard shows no data
```bash
# Wait 24 hours for first data
# Then verify S3 files exist
aws s3 ls sparksql-emr-monitoring/ --recursive

# Check dashboard logs
tail -50 /tmp/flask.log
```

---

## Getting Started

1. **Read**: QUICKSTART.md (5 minutes)
2. **Configure**: Update S3 bucket and IP addresses
3. **Deploy**: Follow SETUP_CHECKLIST.md
4. **Monitor**: Access dashboard at http://localhost:5000

For advanced setup, see **DEPLOYMENT.md**.

---

## Files Summary

| File | Purpose | Deploy On |
|------|---------|-----------|
| `cluster_monitor_and_alert.py` | Metrics collection & S3 upload | Each EMR master |
| `emr_dashboard.py` | Web dashboard server | Central monitoring server |
| `templates/dashboard.html` | Web UI (auto-served) | Central monitoring server |
| `requirements.txt` | Python dependencies | All servers |
| `QUICKSTART.md` | Fast setup guide | - |
| `DEPLOYMENT.md` | Detailed documentation | - |
| `SETUP_CHECKLIST.md` | Step-by-step setup | - |

---

## Performance Notes

- **Collection**: 5-minute intervals = ~288 datapoints/day/node
- **Storage**: ~1 KB per daily row × 5 nodes × 10 days = 50 KB total
- **S3 Costs**: Negligible (1 PUT/day/node = $0.25/month)
- **Network**: Minimal impact (<1% bandwidth)
- **CPU**: <1% on master nodes

---

## Monitoring Checklist (10-Day Run)

- [ ] Deploy monitors on 5 EMR master nodes
- [ ] Start dashboard on central server
- [ ] Verify monitors are running: `systemctl status emr-monitor`
- [ ] Wait 24 hours for first daily report
- [ ] Access dashboard: http://localhost:5000
- [ ] Verify data appears in all tabs
- [ ] Monitor for 10 days continuously
- [ ] Export final data for analysis
- [ ] Stop monitors and shutdown

---

## Support

See documentation files for detailed setup:
- **QUICKSTART.md** - 5-minute fast start
- **DEPLOYMENT.md** - Production-ready guide with systemd, Gunicorn, troubleshooting
- **SETUP_CHECKLIST.md** - Step-by-step verification checklist

---

## Next Steps

👉 Start with **QUICKSTART.md** for immediate setup!
