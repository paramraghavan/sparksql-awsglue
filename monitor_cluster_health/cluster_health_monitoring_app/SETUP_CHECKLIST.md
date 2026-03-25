# EMR Cluster Monitoring - Setup Checklist

## Pre-Flight Checks

- [ ] AWS S3 bucket created: `sparksql-emr-monitoring`
- [ ] 5 EMR clusters running and accessible
- [ ] IAM roles have S3 permissions (see DEPLOYMENT.md)
- [ ] Central server for dashboard identified
- [ ] Python 3.7+ installed on all servers

---

## Phase 1: Prepare Code

- [ ] Clone/copy monitor_cluster_health directory
- [ ] Review and update S3_BUCKET variable in both files
- [ ] Review and update MASTER_NODE_IPS in emr_dashboard.py
- [ ] Install dependencies: `pip install -r requirements.txt`

---

## Phase 2: Deploy Monitors (On Each of 5 EMR Master Nodes)

For each EMR master (EMR 1, 2, 3, 4, 5):

- [ ] Copy `cluster_monitor_and_alert.py` to master node
- [ ] SSH into master node
- [ ] Install deps: `pip install -r requirements.txt`
- [ ] Test with: `python cluster_monitor_and_alert.py`
  - Wait 5 minutes
  - Verify `cluster_history.csv` created
  - Verify S3 upload attempt (check AWS credentials)
  - Press Ctrl+C to stop
- [ ] Setup systemd service (see DEPLOYMENT.md) OR cron job
- [ ] Start monitoring: `systemctl start emr-monitor`
- [ ] Verify running: `systemctl status emr-monitor`

**Repeat above for all 5 EMR nodes**

---

## Phase 3: Deploy Dashboard

On central monitoring server:

- [ ] Copy `emr_dashboard.py` to server
- [ ] Copy `templates/` directory
- [ ] Install deps: `pip install -r requirements.txt`
- [ ] Update MASTER_NODE_IPS with actual IPs
- [ ] Configure AWS credentials (IAM role, env vars, or ~/.aws/credentials)
- [ ] Test: `python emr_dashboard.py`
- [ ] Open browser: http://localhost:5000
- [ ] Verify page loads (even if "Loading..." on data)
- [ ] Setup production deployment (Gunicorn, Nginx) if needed
- [ ] Configure firewall rules to allow port 5000

---

## Phase 4: Wait for Data

- [ ] Monitor scripts should be running continuously
- [ ] Wait for 5:00 AM (first daily report)
- [ ] Check S3: `aws s3 ls sparksql-emr-monitoring/`
- [ ] Verify CSVs exist: `aws s3 ls sparksql-emr-monitoring/10.0.0.1/`
- [ ] Refresh dashboard (browser refresh)
- [ ] Verify charts populate with data

---

## Phase 5: Validation & Tuning

- [ ] Overview tab shows all 5 nodes with current stats
- [ ] CPU/Memory/Disk charts display 60-day trends
- [ ] Details tab shows node-specific data
- [ ] Refresh button works
- [ ] Email alerts working (check SMTP config)
- [ ] Adjust thresholds if needed
- [ ] Configure S3 lifecycle policies (optional)

---

## Phase 6: 10-Day Production Run

- [ ] All monitors running continuously
- [ ] Dashboard accessible 24/7
- [ ] Monitor logs for errors: `journalctl -u emr-monitor -f`
- [ ] Check daily S3 uploads at 5:01 AM
- [ ] Let run for full 10 days
- [ ] Export/backup data for analysis

---

## Post-Run Analysis

- [ ] Download S3 data: `aws s3 sync sparksql-emr-monitoring/ ./backups/`
- [ ] Analyze trends from dashboard
- [ ] Review peak resource usage
- [ ] Identify bottlenecks or patterns
- [ ] Use insights to optimize cluster

---

## Rollback/Stop

If issues occur:

```bash
# Stop monitor on EMR master
systemctl stop emr-monitor

# Stop dashboard
kill %1  # or Ctrl+C

# Restore from backup
aws s3 cp s3://sparksql-emr-monitoring/10.0.0.1/ ./backup/
```

---

## Variables to Customize

| Variable | File | Current | Your Value |
|----------|------|---------|-----------|
| S3_BUCKET | cluster_monitor_and_alert.py | sparksql-emr-monitoring | [ ] |
| S3_BUCKET | emr_dashboard.py | sparksql-emr-monitoring | [ ] |
| MASTER_NODE_IPS | emr_dashboard.py | 10.0.0.1-5 | [ ] |
| CPU_THRESHOLD | cluster_monitor_and_alert.py | 90.0 | [ ] |
| MEM_THRESHOLD | cluster_monitor_and_alert.py | 90.0 | [ ] |
| DISK_THRESHOLD | cluster_monitor_and_alert.py | 85.0 | [ ] |
| EMAIL_SENDER | cluster_monitor_and_alert.py | monitor@yourdomain.com | [ ] |
| EMAIL_RECEIVER | cluster_monitor_and_alert.py | admin@yourdomain.com | [ ] |
| SMTP_SERVER | cluster_monitor_and_alert.py | localhost | [ ] |

---

## Quick Commands Reference

```bash
# Install dependencies
pip install -r requirements.txt

# Test monitor on EMR master (auto-detect IP)
python cluster_monitor_and_alert.py

# Test monitor with explicit IP
python cluster_monitor_and_alert.py --ip 10.0.0.1

# Start dashboard
python emr_dashboard.py

# Check monitor status (systemd)
systemctl status emr-monitor
journalctl -u emr-monitor -f

# List S3 data
aws s3 ls sparksql-emr-monitoring/ --recursive

# Download from S3
aws s3 cp s3://sparksql-emr-monitoring/10.0.0.1/cluster_history.csv .

# View last stats
tail -5 cluster_history.csv
```

---

## Support & Debugging

**Files to check:**
- Monitor logs: `journalctl -u emr-monitor` or script output
- Dashboard logs: Terminal where `python emr_dashboard.py` runs
- Local CSV: `cluster_history.csv` on each EMR master
- S3 files: Check via AWS Console or `aws s3 ls`

**Common issues:**
1. See DEPLOYMENT.md troubleshooting section
2. Check AWS IAM permissions
3. Verify S3 bucket exists and is accessible
4. Ensure monitor scripts reach 5:00 AM for first upload

---

## Timeline

| Day | Expected Status |
|-----|-----------------|
| **Day 0** | Deploy code, start monitors |
| **Day 1** | First daily report, S3 upload, 1 data point in dashboard |
| **Day 2-10** | Continuous collection, trends visible in charts |
| **Day 10** | Stop monitors, export final data |

---

Done! Ready to proceed? Start with Phase 1.
