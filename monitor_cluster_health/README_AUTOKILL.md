# EMR Cluster Monitor with Auto-Kill

Monitor cluster health and **automatically terminate resource-hogging jobs** to prevent cluster failure.

## ⚠️ WARNING: Use Auto-Kill Carefully!

Auto-killing jobs can:
- ✅ Prevent cluster death
- ✅ Free up resources quickly
- ❌ Kill legitimate long-running jobs
- ❌ Cause data loss if jobs aren't idempotent

**Recommendation:** Start with `--dry-run` to see what would be killed!

---

## Quick Start

### 1. Monitor Only (Safe)

```bash
python monitor_with_autokill.py
```

### 2. Dry-Run (See What Would Be Killed)

```bash
python monitor_with_autokill.py --auto-kill --dry-run
```

Output:
```
[DRY-RUN] Would kill: application_1234567890_0123 (My Spark Job) - 64.0GB, 16 cores
```

### 3. Auto-Kill Enabled (Dangerous!)

```bash
python monitor_with_autokill.py --auto-kill --kill-count 1
```

---

## How It Works

1. **Checks cluster utilization** (memory/vCPU %)
2. **If threshold exceeded:**
   - Identifies top N resource-consuming apps
   - Kills them via YARN REST API
   - Logs to `/tmp/emr_auto_kill.log`
3. **Sends alert** with kill details

### Which Jobs Get Killed?

Apps are ranked by **allocated memory (GB)** and top N are killed:

```
Running Apps:
1. app_123 - 120 GB  ← KILLED (top consumer)
2. app_456 - 80 GB   ← KILLED if kill-count=2
3. app_789 - 40 GB
4. app_012 - 20 GB
```

---

## Usage Examples

### Kill Top Resource Hog at 85% Utilization

```bash
python monitor_with_autokill.py \
  --auto-kill \
  --memory-threshold 85 \
  --kill-count 1
```

### Kill Top 2 Apps at 90% Utilization

```bash
python monitor_with_autokill.py \
  --auto-kill \
  --memory-threshold 90 \
  --vcpu-threshold 90 \
  --kill-count 2
```

### Test Without Actually Killing

```bash
python monitor_with_autokill.py --auto-kill --dry-run
```

---

## Minimal Version

Edit `monitor_minimal_autokill.py`:

```python
YARN_URL = "http://localhost:8088"
THRESHOLD = 85
AUTO_KILL = True   # Set to False to disable
KILL_COUNT = 1     # How many to kill
```

Run:
```bash
python monitor_minimal_autokill.py
```

---

## Scheduling with Cron

### Conservative: Alert + Manual Kill

```cron
*/5 * * * * python monitor_with_autokill.py --quiet
```

### Aggressive: Auto-Kill Enabled

```cron
*/5 * * * * python monitor_with_autokill.py --auto-kill --kill-count 1 --quiet
```

### Safest: Dry-Run + Email

```cron
*/5 * * * * python monitor_with_autokill.py --dry-run 2>&1 | mail -s "EMR Would Kill" you@email.com
```

---

## Example Alert Output

### With Auto-Kill

```
EMR CLUSTER ALERT - http://master-node:8088
======================================================================
CRITICAL THRESHOLDS EXCEEDED:

  - MEMORY CRITICAL: 89.5% used (430.0/480.0 GB)
  - VCPU CRITICAL: 91.2% used (73/80 cores)

======================================================================
AUTO-KILLED 1 APPLICATION(S):

  • application_1704723456789_0234
    Name: my-etl-job
    Resources: 64.0 GB, 16 vCores

Killed apps logged to: /tmp/emr_auto_kill.log

======================================================================
CURRENT STATUS:
  Memory: 89.5% (430.0/480.0 GB)
  vCPUs:  91.2% (73/80 cores)
  Active Nodes: 5
  Running Apps: 11  (was 12)
  Pending Apps: 2

======================================================================
```

### Kill Log (`/tmp/emr_auto_kill.log`)

```
2025-01-07 14:23:45 | KILLED | application_1704723456789_0234 | Cluster at 89.5% capacity
2025-01-07 14:35:12 | KILLED | application_1704723456789_0567 | Cluster at 91.2% capacity
```

---

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--yarn-url` | `http://localhost:8088` | YARN ResourceManager URL |
| `--memory-threshold` | 85 | Memory % to trigger alert |
| `--vcpu-threshold` | 85 | vCPU % to trigger alert |
| `--auto-kill` | Off | Enable auto-kill |
| `--kill-count` | 1 | Number of top apps to kill |
| `--dry-run` | Off | Show what would be killed |
| `--quiet` | Off | Only output on alert |

---

## Safety Best Practices

### 1. Start with Dry-Run

```bash
# Run for a week in dry-run mode
python monitor_with_autokill.py --auto-kill --dry-run >> /tmp/dry-run.log
```

Review logs to see what would have been killed.

### 2. Use High Thresholds Initially

```bash
# Start at 95%, then lower if needed
python monitor_with_autokill.py --auto-kill --memory-threshold 95
```

### 3. Kill Only 1 App at a Time

```bash
# Kill-count=1 gives apps a chance to finish
python monitor_with_autokill.py --auto-kill --kill-count 1
```

### 4. Whitelist Critical Jobs

Modify the script to skip certain apps:

```python
# In identify_offenders function, add:
PROTECTED_APPS = ['critical-etl', 'payment-processor']
filtered_apps = [app for app in apps 
                 if not any(name in app['name'] for name in PROTECTED_APPS)]
```

### 5. Alert + Manual Kill (Safest)

```bash
# Alert only, you decide what to kill
python monitor_with_autokill.py
# Then manually: yarn application -kill <app-id>
```

---

## When to Use Auto-Kill

### ✅ Good Use Cases:

- **Ad-hoc queries** eating all memory
- **Test jobs** running on production cluster
- **Runaway jobs** that should have finished
- **Off-hours monitoring** (no one to respond)
- **Transient workloads** (can be restarted easily)

### ❌ Bad Use Cases:

- **Critical ETL pipelines**
- **Payment processing jobs**
- **Jobs with side effects** (database writes)
- **Long-running ML training** (hours of progress lost)
- **Jobs without checkpointing**

---

## Manual Kill Commands

If you prefer manual intervention:

### List Running Apps

```bash
yarn application -list
```

### Kill Specific App

```bash
yarn application -kill application_1234567890_0001
```

### Kill All Apps (Nuclear Option!)

```bash
yarn application -list | grep application_ | awk '{print $1}' | xargs -n1 yarn application -kill
```

---

## Troubleshooting

### "Failed to kill app: HTTP 403"

YARN ResourceManager has security enabled. You need proper permissions.

### Apps Not Being Killed

Check:
1. YARN URL is correct
2. Port 8088 is accessible
3. No firewall blocking PUT requests
4. Check logs: `cat /tmp/emr_auto_kill.log`

### Killed Wrong Job

1. Review `/tmp/emr_auto_kill.log`
2. Check why that job was using most resources
3. Consider using `--kill-count 0` (alert only) temporarily
4. Add job to whitelist

---

## Alternative: YARN Preemption

Instead of killing, enable YARN preemption to gracefully reclaim resources:

Edit `/etc/hadoop/conf/yarn-site.xml`:
```xml
<property>
  <name>yarn.resourcemanager.scheduler.monitor.enable</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.scheduler.monitor.policies</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy</value>
</property>
```

Restart YARN ResourceManager.

---

## Scripts Summary

| Script | Auto-Kill | Lines | Use Case |
|--------|-----------|-------|----------|
| `monitor_with_autokill.py` | Optional | 200 | Production-ready with options |
| `monitor_minimal_autokill.py` | Always on | 50 | Simple auto-kill |
| `monitor_cluster_health.py` | No | 115 | Alert only (safest) |

---

## Recommendation

**Start here:**
1. Use `monitor_cluster_health.py` (no auto-kill) for 1 week
2. Review alerts to understand patterns
3. Try `--dry-run` for 1 week to see what would be killed
4. Enable auto-kill with `--kill-count 1` and threshold 90%
5. Lower threshold to 85% after observing behavior

**Production setup:**
```cron
# Every 5 min: Alert + auto-kill top hog at 90%
*/5 * * * * python monitor_with_autokill.py --auto-kill --memory-threshold 90 --quiet || mail -s "EMR Alert" you@email.com
```

This balances cluster stability with job safety.
