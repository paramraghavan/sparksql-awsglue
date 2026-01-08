# EMR Continuous Monitoring - Sustained High Usage Detection

**The Right Way to Monitor:** Detects **SUSTAINED** high resource usage, not transient spikes.

## Why Continuous Monitoring?

### ❌ Problem with Snapshot Monitoring (Cron):
```
08:00 - Check: 60% - OK
08:05 - Check: 95% - ALERT! (but might be temporary)
08:10 - Check: 65% - OK (was just node allocation)
```
**False positive!** Killed a job during normal operation.

### ✅ Solution: Continuous Monitoring:
```
Continuous checks every 60s:
08:00 - 60% - OK
08:01 - 75% - OK (spike)
08:02 - 88% - Monitoring... (above threshold but not sustained)
08:03 - 91% - Monitoring... (2 min sustained)
08:04 - 89% - Monitoring... (3 min sustained)
08:05 - 90% - Monitoring... (4 min sustained)
08:06 - 92% - ALERT! (5+ min sustained = real problem)
```
**True positive!** Only alerts on sustained issues.

---

## How It Works

1. **Checks every N seconds** (default: 60s)
2. **Tracks history** of utilization over time
3. **Only alerts if** threshold exceeded for X minutes (default: 5 min)
4. **Ignores transient spikes** (node allocation, job finishing, etc.)
5. **10-minute cooldown** between alerts (no spam)

---

## Quick Start

### Basic Usage (5-min sustained, 60s checks)

```bash
python monitor_continuous.py
```

**Output:**
```
Starting EMR Continuous Monitor
YARN URL: http://localhost:8088
Thresholds: Memory 85%, vCPU 85%
Sustained Period: 300s (5.0 min)
Check Interval: 60s
Auto-Kill: DISABLED
======================================================================

[14:23:45] Check #1: Memory 72.3%, vCPU 68.5% - OK
[14:24:45] Check #2: Memory 74.1%, vCPU 70.2% - OK
[14:25:45] Check #3: Memory 88.5%, vCPU 85.3% - OK (transient spike, monitoring...)
[14:26:45] Check #4: Memory 89.2%, vCPU 87.1% - OK (transient spike, monitoring...)
[14:27:45] Check #5: Memory 90.1%, vCPU 88.5% - OK (transient spike, monitoring...)
[14:28:45] Check #6: Memory 91.3%, vCPU 89.2% - OK (transient spike, monitoring...)
[14:29:45] Check #7: Memory 92.1%, vCPU 90.5% - BREACH (sustained 5.0 min)

======================================================================
SUSTAINED HIGH USAGE ALERT - 2025-01-07 14:29:45
======================================================================

CRITICAL: Cluster has been above threshold for 5.0 minutes

CURRENT METRICS:
  Memory: 92.1% (442.0/480.0 GB)
  vCPUs:  90.5% (72/80 cores)
  Active Nodes: 5
  Running Apps: 8
  Pending Apps: 3

======================================================================
```

---

## Configuration Options

### Adjust Sustained Period

```bash
# Conservative: Require 10 minutes sustained
python monitor_continuous.py --sustained-period 600

# Aggressive: Alert after 3 minutes sustained
python monitor_continuous.py --sustained-period 180

# Very conservative: 15 minutes sustained
python monitor_continuous.py --sustained-period 900
```

### Adjust Check Interval

```bash
# Check every 30 seconds (more responsive)
python monitor_continuous.py --check-interval 30

# Check every 2 minutes (less overhead)
python monitor_continuous.py --check-interval 120
```

### Adjust Thresholds

```bash
# Higher threshold (90%)
python monitor_continuous.py --memory-threshold 90 --vcpu-threshold 90

# Different thresholds for memory vs vCPU
python monitor_continuous.py --memory-threshold 85 --vcpu-threshold 90
```

### Enable Auto-Kill

```bash
# Kill top resource hog after 5 min sustained breach
python monitor_continuous.py --auto-kill --sustained-period 300

# Kill top 2 apps after 10 min sustained breach
python monitor_continuous.py --auto-kill --sustained-period 600 --kill-count 2
```

---

## Running as Background Daemon

### Option 1: Using nohup (Simple)

```bash
# Start in background
nohup python monitor_continuous.py > /tmp/emr-monitor.log 2>&1 &

# Check if running
ps aux | grep monitor_continuous

# View logs
tail -f /tmp/emr-monitor.log

# Stop
pkill -f monitor_continuous.py
```

### Option 2: Using systemd (Production)

**Install:**
```bash
# Copy service file
sudo cp emr-monitor.service /etc/systemd/system/

# Edit if needed
sudo nano /etc/systemd/system/emr-monitor.service

# Reload systemd
sudo systemctl daemon-reload

# Start service
sudo systemctl start emr-monitor

# Enable on boot
sudo systemctl enable emr-monitor
```

**Manage:**
```bash
# Check status
sudo systemctl status emr-monitor

# View logs
sudo journalctl -u emr-monitor -f

# Restart
sudo systemctl restart emr-monitor

# Stop
sudo systemctl stop emr-monitor
```

### Option 3: Using screen (Development)

```bash
# Start in screen session
screen -S emr-monitor
python monitor_continuous.py

# Detach: Ctrl+A, then D

# Reattach
screen -r emr-monitor

# Kill
screen -X -S emr-monitor quit
```

---

## Recommended Configurations

### Conservative (Production)
```bash
python monitor_continuous.py \
  --sustained-period 600 \
  --memory-threshold 90 \
  --check-interval 60
```
- 10 minutes sustained
- 90% threshold
- Check every minute

### Balanced (Default)
```bash
python monitor_continuous.py \
  --sustained-period 300 \
  --memory-threshold 85 \
  --check-interval 60
```
- 5 minutes sustained
- 85% threshold
- Check every minute

### Aggressive (With Auto-Kill)
```bash
python monitor_continuous.py \
  --sustained-period 180 \
  --memory-threshold 85 \
  --check-interval 30 \
  --auto-kill
```
- 3 minutes sustained
- 85% threshold
- Check every 30 seconds
- Auto-kill enabled

---

## Integration with Alerts

### Email on Sustained Alert

```bash
# Wrapper script
#!/bin/bash
python monitor_continuous.py 2>&1 | while read line; do
    echo "$line"
    if echo "$line" | grep -q "SUSTAINED HIGH USAGE ALERT"; then
        echo "$line" | mail -s "EMR Sustained Alert" you@email.com
    fi
done
```

### SNS Topic

```bash
# Modify script to send SNS when alert fires
# Add to handle_sustained_breach():
aws sns publish \
  --topic-arn arn:aws:sns:us-east-1:123:emr-alerts \
  --subject "EMR Sustained High Usage" \
  --message "$alert_msg"
```

### Slack Webhook

```bash
# Add to handle_sustained_breach():
curl -X POST YOUR_WEBHOOK_URL \
  -H 'Content-Type: application/json' \
  -d "{\"text\": \"EMR Alert: Sustained high usage detected\"}"
```

---

## Logs and Monitoring

### Application Logs
- **Stdout:** Real-time check results
- **File:** `/tmp/emr_continuous_monitor.log` - All actions logged

### Log Format
```
2025-01-07 14:29:45|ALERT|Memory:92.1%|vCPU:90.5%|Duration:300s|Killed:0
2025-01-07 14:30:15|KILLED|application_123|Sustained 5.0min above 92.1%
2025-01-07 15:00:00|ALERT|Memory:88.5%|vCPU:87.2%|Duration:360s|Killed:1
```

### View Logs
```bash
# Real-time stdout
tail -f /var/log/emr-monitor.log

# Action log
cat /tmp/emr_continuous_monitor.log

# Filter for alerts only
grep ALERT /tmp/emr_continuous_monitor.log
```

---

## Transient vs Sustained Examples

### Transient (No Alert)
```
Scenario: New nodes being added
14:00 - 70% - OK
14:01 - 88% - Monitoring (spike from rebalancing)
14:02 - 89% - Monitoring
14:03 - 72% - OK (nodes added, back to normal)
Result: No alert (not sustained 5+ min)
```

### Sustained (Alert!)
```
Scenario: Runaway job consuming resources
14:00 - 70% - OK
14:01 - 88% - Monitoring
14:02 - 89% - Monitoring
14:03 - 90% - Monitoring
14:04 - 91% - Monitoring
14:05 - 92% - Monitoring
14:06 - 93% - ALERT! (5+ min sustained)
Result: Alert + optional auto-kill
```

---

## Comparison: Cron vs Continuous

| Aspect | Cron (Snapshot) | Continuous (Sustained) |
|--------|----------------|------------------------|
| **False Positives** | High | Low |
| **Response Time** | 5-15 min | 5-10 min |
| **Transient Spikes** | Triggers alert | Ignores |
| **Resource Usage** | Minimal | ~10MB RAM |
| **Detection** | Point-in-time | Time-series |
| **Best For** | Simple alerts | Production |

---

## Troubleshooting

### "Monitor keeps alerting"
This means sustained high usage! Actions:
1. Check if auto-scaling is enabled
2. Review running applications: `yarn application -list`
3. Consider scaling up cluster
4. Enable auto-kill to free resources

### "No alerts but cluster is slow"
Check your thresholds:
- Lower threshold: 85% → 80%
- Shorten sustained period: 300s → 180s

### "Getting false positives"
Increase sustained period:
- 300s → 600s (5 min → 10 min)

### High CPU usage from monitor
Increase check interval:
- 60s → 120s

---

## Best Practices

1. **Start conservative:** 10-min sustained, 90% threshold
2. **Monitor for 1 week** without auto-kill
3. **Review logs** to understand usage patterns
4. **Tune thresholds** based on observed behavior
5. **Enable auto-kill** only after understanding patterns
6. **Set up alerts** (email/SNS/Slack)
7. **Run as systemd service** for reliability

---

## When to Use Each Script

| Script | Use Case |
|--------|----------|
| `monitor_continuous.py` | **Production - Sustained monitoring** |
| `monitor_with_autokill.py` | One-time checks, cron jobs |
| `monitor_minimal.py` | Quick status checks |

**Recommendation:** Use `monitor_continuous.py` for production monitoring to avoid false positives from transient spikes.

---

## Production Setup Example

```bash
# 1. Install script
sudo cp monitor_continuous.py /usr/local/bin/

# 2. Install service
sudo cp emr-monitor.service /etc/systemd/system/

# 3. Edit service for your YARN URL
sudo nano /etc/systemd/system/emr-monitor.service

# 4. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable emr-monitor
sudo systemctl start emr-monitor

# 5. Verify
sudo systemctl status emr-monitor
sudo journalctl -u emr-monitor -f
```

Now you have **sustained high usage detection** running 24/7!
