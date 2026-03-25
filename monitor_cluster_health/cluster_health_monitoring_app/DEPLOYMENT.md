# EMR Cluster Health Monitoring - Deployment Guide

## Overview
This solution provides:
1. **cluster_monitor_and_alert.py** - Monitoring script deployed on each EMR master node
2. **emr_dashboard.py** - Flask dashboard to visualize stats from all 5 EMR clusters

## Architecture

```
EMR Master Node 1 → cluster_monitor_and_alert.py → S3: sparksql-emr-monitoring/10.0.0.1/
EMR Master Node 2 → cluster_monitor_and_alert.py → S3: sparksql-emr-monitoring/10.0.0.2/
EMR Master Node 3 → cluster_monitor_and_alert.py → S3: sparksql-emr-monitoring/10.0.0.3/
EMR Master Node 4 → cluster_monitor_and_alert.py → S3: sparksql-emr-monitoring/10.0.0.4/
EMR Master Node 5 → cluster_monitor_and_alert.py → S3: sparksql-emr-monitoring/10.0.0.5/
                          ↓
                    Shared S3 Bucket
                          ↑
            Dashboard Server (Flask App)
                 Displays via Web UI
```

---

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Part A: Monitoring Script Setup (On Each EMR Master Node)

### Configuration

Update these variables in `cluster_monitor_and_alert.py`:

```python
# S3 Configuration
S3_BUCKET = "sparksql-emr-monitoring"  # Change to your bucket name
S3_ENABLED = True                       # Set to False to use local CSV only

# Thresholds
CPU_THRESHOLD = 90.0
MEM_THRESHOLD = 90.0
DISK_THRESHOLD = 85.0
PENDING_THRESHOLD = 10

# Email Settings
EMAIL_SENDER = "monitor@yourdomain.com"
EMAIL_RECEIVER = "admin@yourdomain.com"
SMTP_SERVER = "localhost"
```

### AWS IAM Permissions

The IAM role/credentials used must have S3 permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::sparksql-emr-monitoring/*"
        }
    ]
}
```

### Running the Monitor

**Option 1: Manual Start**
```bash
# Auto-detect local IP
python cluster_monitor_and_alert.py

# Or specify IP explicitly
python cluster_monitor_and_alert.py --ip 10.0.0.1
```

**Option 2: Systemd Service (Recommended for 10-day continuous run)**

Create `/etc/systemd/system/emr-monitor.service`:

```ini
[Unit]
Description=EMR Cluster Health Monitor
After=network.target

[Service]
Type=simple
User=hadoop
WorkingDirectory=/path/to/monitor_cluster_health
ExecStart=/usr/bin/python3 cluster_monitor_and_alert.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable emr-monitor
sudo systemctl start emr-monitor
sudo systemctl status emr-monitor
```

View logs:
```bash
journalctl -u emr-monitor -f
```

**Option 3: Cron Job (For daily rolling restarts)**

```bash
# Add to crontab
*/5 * * * * /usr/bin/python3 /path/to/cluster_monitor_and_alert.py
```

### What Gets Saved

#### Local File (cluster_history.csv)
Saves daily peak stats with rollover:
```
Date,MaxCPU,MaxMem,MaxDisk,MaxPending,MaxYarnMem,MaxYarnCPU
2024-03-25,85.5,72.3,65.1,5,8192,64
2024-03-26,88.2,75.1,66.3,8,8192,64
```

Rolls over every 28 days with archive.

#### S3 Storage
- **Location**: `s3://sparksql-emr-monitoring/{IP}/cluster_history.csv`
- **Format**: Same CSV format, updated daily at 5:00 AM
- **Retention**: You control via S3 lifecycle policies

---

## Part B: Dashboard Setup (Central Monitoring Server)

### Configuration

Update `emr_dashboard.py` with your master node IPs:

```python
MASTER_NODE_IPS = [
    "10.0.0.1",    # EMR 1
    "10.0.0.2",    # EMR 2
    "10.0.0.3",    # EMR 3
    "10.0.0.4",    # EMR 4
    "10.0.0.5"     # EMR 5
]

S3_BUCKET = "sparksql-emr-monitoring"
```

### AWS Credentials

The dashboard server needs S3 read permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::sparksql-emr-monitoring/*"
        }
    ]
}
```

Configure credentials via:
- AWS IAM Role (if running on EC2/EMR)
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `~/.aws/credentials` file

### Running the Dashboard

```bash
python emr_dashboard.py
```

Access the dashboard at: **http://localhost:5000**

### Production Deployment

Use **Gunicorn** for production:

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 emr_dashboard:app
```

Or with **Nginx**:

```nginx
server {
    listen 80;
    server_name monitoring.example.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

---

## Dashboard Features

### Tabs

1. **Overview** - Current status cards for all 5 EMR nodes
   - Latest CPU, Memory, Disk usage
   - 30-day average & max values

2. **CPU Usage** - Historical CPU trend (last 60 days)
   - Line chart comparing all 5 nodes

3. **Memory Usage** - Historical memory trend (last 60 days)

4. **Disk Usage** - Historical disk trend (last 60 days)

5. **YARN Resources**
   - Pending applications
   - Allocated memory & vCores

6. **Details** - Detailed table view per node
   - Last 10 days of data
   - All metrics in table format

### Color Coding

Status cards show:
- **Green** - Normal (< 60%)
- **Orange** - Warning (60-80%)
- **Red** - Danger (> 80%)

---

## API Endpoints

```
GET /api/stats                    - Summary stats for all nodes
GET /api/chart/{metric}           - Chart data (MaxCPU, MaxMem, MaxDisk, etc.)
GET /api/node/{ip}                - Last 10 days for specific node
```

Example:
```bash
curl http://localhost:5000/api/stats | jq
curl http://localhost:5000/api/chart/MaxCPU | jq
curl http://localhost:5000/api/node/10.0.0.1 | jq
```

---

## Troubleshooting

### Monitor Script Issues

**S3 Upload Fails**
- Check IAM permissions
- Verify bucket exists
- Check AWS credentials: `aws s3 ls sparksql-emr-monitoring/`

**No IP Address Detected**
- Manually pass: `python cluster_monitor_and_alert.py --ip 10.0.0.1`
- Check network connectivity: `curl 8.8.8.8`

**YARN Metrics Unavailable**
- Verify Resource Manager is running: `curl http://localhost:8088/ws/v1/cluster/metrics`
- Check firewall: `sudo netstat -tlnp | grep 8088`

### Dashboard Issues

**Data Not Loading**
- Check S3 connectivity: `aws s3 ls sparksql-emr-monitoring/`
- Verify IPs match: Update `MASTER_NODE_IPS` in `emr_dashboard.py`
- Check CloudWatch logs for S3 errors

**Charts Empty**
- Monitoring scripts must run for at least 1 day to collect data
- Verify CSV files exist in S3: `aws s3 ls sparksql-emr-monitoring/10.0.0.1/`

---

## 10-Day Test Run

For a 10-day continuous monitoring run:

1. **Start monitors** on all 5 EMR master nodes (systemd services)
2. **Wait 24 hours** for first daily report and S3 upload
3. **Start dashboard** server after first data arrives
4. **Let run for 10 days** with automatic daily rollover
5. **Stop monitors** after 10 days
6. **Analyze data** via dashboard

Data will be automatically rolled over each day and archived locally. S3 maintains full history.

---

## Performance Tuning

### Reduce Polling Load
Change in `cluster_monitor_and_alert.py`:
```python
schedule.every(5).minutes.do(monitor_job, ip=ip)  # Change 5 to 10 or 15
```

### S3 Costs
- 5 nodes × 1 upload/day × 10 days = 50 PUT requests (~$0.25)
- Implement S3 lifecycle policies to move old data to Glacier

---

## S3 Lifecycle Policy Example

```json
{
    "Rules": [
        {
            "Id": "Archive old stats",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "GLACIER"
                }
            ]
        }
    ]
}
```

---

## Next Steps

1. Update bucket name and IPs in both scripts
2. Create S3 bucket with appropriate lifecycle policies
3. Deploy monitoring script to all 5 EMR master nodes
4. Start dashboard server on central monitoring instance
5. Access dashboard and verify data collection
6. Configure alerts/emails as needed
