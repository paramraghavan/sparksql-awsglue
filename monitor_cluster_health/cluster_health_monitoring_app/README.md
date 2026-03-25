# 🚀 EMR Cluster Health Monitoring App

Complete monitoring solution for **5 EMR clusters** with web dashboard.

## Directory Structure

```
cluster_health_monitoring_app/
├── 📄 README.md                      ← This file
├── 📄 README_MONITORING.md           ← Overview & architecture
├── 📄 QUICKSTART.md                  ← 5-minute setup guide
├── 📄 DEPLOYMENT.md                  ← Production setup & troubleshooting
├── 📄 SETUP_CHECKLIST.md             ← Step-by-step checklist
│
├── 🔧 cluster_monitor_and_alert.py   ← Deploy on each EMR master (5 copies)
├── 🌐 emr_dashboard.py               ← Deploy on central server (1 copy)
├── 📋 requirements.txt               ← Python dependencies
│
└── 📁 templates/
    └── dashboard.html                ← Web UI (interactive charts)
```

---

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Deploy Monitoring Script (on each EMR master - 5 copies)
```bash
# Auto-detect local IP
python cluster_monitor_and_alert.py

# Or specify IP explicitly
python cluster_monitor_and_alert.py --ip 10.0.0.1
```

### 3. Start Dashboard (on central monitoring server - 1 copy)
```bash
python emr_dashboard.py
```

Access dashboard: **http://localhost:5000**

---

## What It Does

✅ **Monitors** every 5 minutes:
- CPU usage (%)
- Memory usage (%)
- Disk usage (%)
- YARN pending applications
- YARN allocated resources

✅ **Saves** to both:
- **Local CSV**: `cluster_history.csv` (daily rollover)
- **S3**: Automatic daily uploads with IP-based prefixes

✅ **Displays**:
- Real-time status cards
- 60-day trend charts
- Per-node details table
- Color-coded alerts

---

## Configuration

Update these variables **before deploying**:

### Both Scripts
```python
S3_BUCKET = "sparksql-emr-monitoring"  # Change to your bucket name
```

### Dashboard Only (emr_dashboard.py)
```python
MASTER_NODE_IPS = [
    "10.0.0.1",    # Change to your actual IPs
    "10.0.0.2",
    "10.0.0.3",
    "10.0.0.4",
    "10.0.0.5"
]
```

### Monitor Script Only (cluster_monitor_and_alert.py)
```python
CPU_THRESHOLD = 90.0
MEM_THRESHOLD = 90.0
DISK_THRESHOLD = 85.0
PENDING_THRESHOLD = 10

EMAIL_SENDER = "monitor@yourdomain.com"
EMAIL_RECEIVER = "admin@yourdomain.com"
SMTP_SERVER = "localhost"
```

---

## Documentation

| File | Purpose |
|------|---------|
| **README_MONITORING.md** | Complete overview, architecture diagram |
| **QUICKSTART.md** | Fast 5-minute setup guide |
| **DEPLOYMENT.md** | Production deployment, systemd, Gunicorn, troubleshooting |
| **SETUP_CHECKLIST.md** | Phase-by-phase verification checklist |

**👉 Start with README_MONITORING.md!**

---

## Dashboard Features

### Tabs
1. **Overview** - Current status cards for all 5 EMR nodes
2. **CPU Usage** - 60-day CPU trend
3. **Memory Usage** - 60-day memory trend
4. **Disk Usage** - 60-day disk trend
5. **YARN Resources** - Pending apps & allocated resources
6. **Details** - Detailed per-node table

### Color Coding
- 🟢 **Green** (< 60%) - Normal
- 🟠 **Orange** (60-80%) - Warning
- 🔴 **Red** (> 80%) - Danger

---

## Files Deployed

### On Each EMR Master Node (5 copies)
- `cluster_monitor_and_alert.py`
- `requirements.txt`

### On Central Monitoring Server (1 copy)
- `emr_dashboard.py`
- `requirements.txt`
- `templates/` folder

---

## S3 Bucket Structure

After running:
```
sparksql-emr-monitoring/
├── 10.0.0.1/cluster_history.csv
├── 10.0.0.2/cluster_history.csv
├── 10.0.0.3/cluster_history.csv
├── 10.0.0.4/cluster_history.csv
└── 10.0.0.5/cluster_history.csv
```

---

## Deployment Options

### Development
```bash
python cluster_monitor_and_alert.py
python emr_dashboard.py
```

### Systemd Service (Recommended)
```bash
sudo systemctl enable emr-monitor
sudo systemctl start emr-monitor
journalctl -u emr-monitor -f
```

### Production (Gunicorn)
```bash
gunicorn -w 4 -b 0.0.0.0:5000 emr_dashboard:app
```

---

## Timeline

| Day | Action |
|-----|--------|
| 1 | Deploy monitors on 5 EMR masters |
| 1 | Start dashboard on central server |
| 2 | First daily report & S3 upload |
| 2+ | Dashboard populates with data |
| 10 | Stop monitors, export final data |

---

## Troubleshooting

**Won't start?**
```bash
pip install -r requirements.txt
```

**No IP detected?**
```bash
python cluster_monitor_and_alert.py --ip 10.0.0.1
```

**S3 upload fails?**
- Check AWS credentials: `aws s3 ls sparksql-emr-monitoring/`
- Check IAM permissions (see DEPLOYMENT.md)

**Dashboard empty?**
- Wait 24 hours for first data
- Then refresh browser
- Check S3: `aws s3 ls sparksql-emr-monitoring/ --recursive`

---

## Support

See documentation files for detailed setup and troubleshooting:
- **README_MONITORING.md** - Overview + architecture
- **QUICKSTART.md** - Fast setup
- **DEPLOYMENT.md** - Production setup + troubleshooting
- **SETUP_CHECKLIST.md** - Verification checklist

---

**👉 Start with QUICKSTART.md for immediate setup!**
