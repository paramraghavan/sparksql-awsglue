# EMR Cluster Health Monitoring

Simple scripts to monitor EMR cluster capacity and alert before failures occur.

## Quick Start

### Option 1: Full Featured Monitor

```bash
python monitor_cluster_health.py --yarn-url http://master-node:8088
```

**Features:**
- Configurable thresholds (default 85%)
- Checks memory, vCPUs, unhealthy nodes, pending apps
- Detailed alert messages
- Exit code 1 on alert, 0 if healthy

### Option 2: Ultra-Minimal Monitor

```bash
python monitor_minimal.py
```

**15 lines of code** - just checks memory/vCPU against threshold.

## Exit Codes

- `0` = Healthy
- `1` = Alert condition (threshold exceeded)
- `2` = Error (can't connect to YARN)

## Usage Examples

### Check with custom thresholds

```bash
python monitor_cluster_health.py \
  --yarn-url http://master-node:8088 \
  --memory-threshold 80 \
  --vcpu-threshold 90
```

### Quiet mode (only output on alert)

```bash
python monitor_cluster_health.py --quiet
```

## Scheduling with Cron

### Every 5 minutes

```bash
crontab -e
```

Add:
```cron
*/5 * * * * /home/hadoop/monitor_and_email.sh >> /var/log/cluster-monitor.log 2>&1
```

### Every 15 minutes with email

```cron
*/15 * * * * python3 /home/hadoop/monitor_cluster_health.py --quiet || mail -s "EMR Alert" you@example.com
```

## Email Integration

### Method 1: Direct with mail command

```bash
python monitor_cluster_health.py --quiet || \
  python monitor_cluster_health.py | mail -s "EMR Alert" you@example.com
```

### Method 2: Using wrapper script

Edit `monitor_and_email.sh`:
```bash
EMAIL="your-email@example.com"
```

Run:
```bash
./monitor_and_email.sh
```

### Method 3: AWS SNS

```bash
if ! python monitor_cluster_health.py --quiet; then
    OUTPUT=$(python monitor_cluster_health.py)
    aws sns publish \
        --topic-arn arn:aws:sns:us-east-1:123456789:emr-alerts \
        --message "$OUTPUT"
fi
```

## Alert Example

```
EMR CLUSTER ALERT
============================================================
CRITICAL THRESHOLDS EXCEEDED:
  - MEMORY CRITICAL: 87.5% used (420.0/480.0 GB)
  - VCPU CRITICAL: 91.2% used (73/80 cores)

ACTION REQUIRED: Review cluster capacity or scale up!
============================================================
```

## Recommended Thresholds

| Threshold | Use Case |
|-----------|----------|
| 80% | Conservative, early warning |
| 85% | **Default**, industry standard |
| 90% | Aggressive, auto-scaling needed |

## Installation

```bash
# 1. Copy to edge node
scp monitor_cluster_health.py hadoop@edge-node:/home/hadoop/

# 2. Install requests
pip install requests

# 3. Test
python monitor_cluster_health.py

# 4. Set up cron
crontab -e
```
