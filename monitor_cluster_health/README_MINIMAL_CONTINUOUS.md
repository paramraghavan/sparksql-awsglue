# Minimal Continuous Monitor

**65 lines of code** - Detects sustained high usage, ignores transient spikes.

## Configuration

Edit these lines in `monitor_continuous_minimal.py`:

```python
YARN_URL = "http://localhost:8088"
THRESHOLD = 85              # Alert at 85%
SUSTAINED_SECONDS = 300     # Must be high for 5 minutes
CHECK_INTERVAL = 60         # Check every 60 seconds
AUTO_KILL = False           # Set True to auto-kill
```

## Usage

### Run in Foreground
```bash
python monitor_continuous_minimal.py
```

### Run in Background
```bash
nohup python monitor_continuous_minimal.py > /tmp/monitor.log 2>&1 &
```

### Stop Background Process
```bash
pkill -f monitor_continuous_minimal.py
```

## Example Output

```
Continuous Monitor: Threshold 85%, Sustained 300s, Check every 60s
Auto-Kill: DISABLED

[14:23:45] Mem 72% | CPU 68% | OK
[14:24:45] Mem 88% | CPU 85% | HIGH (monitoring 0s)
[14:25:45] Mem 89% | CPU 87% | HIGH (monitoring 60s)
[14:26:45] Mem 90% | CPU 88% | HIGH (monitoring 120s)
[14:27:45] Mem 91% | CPU 89% | HIGH (monitoring 180s)
[14:28:45] Mem 92% | CPU 90% | HIGH (monitoring 240s)
[14:29:45] Mem 93% | CPU 91% | HIGH (monitoring 300s) → SUSTAINED 5.0min!

============================================================
ALERT: Above 85% for 5.0 minutes
Memory: 93.0% | vCPU: 91.0%
============================================================

[14:30:45] Mem 94% | CPU 92% | HIGH (monitoring 360s)
[14:31:45] Mem 75% | CPU 70% | OK
```

## How It Works

1. Checks every 60 seconds
2. If above threshold, starts tracking duration
3. If below threshold, resets duration to 0
4. Alerts only after 5+ minutes sustained
5. Won't re-alert for 10 minutes (cooldown)

## Transient vs Sustained

**Transient (No Alert):**
```
88% → 89% → 90% → 72% (back to normal after 3 min)
```

**Sustained (Alert!):**
```
88% → 89% → 90% → 91% → 92% → 93% (5+ min = ALERT)
```

## Common Settings

### Conservative (10 min sustained)
```python
SUSTAINED_SECONDS = 600
THRESHOLD = 90
```

### Aggressive (3 min sustained)
```python
SUSTAINED_SECONDS = 180
THRESHOLD = 85
```

### With Auto-Kill
```python
AUTO_KILL = True
SUSTAINED_SECONDS = 300
```

## Quick Comparison

| Feature | Minimal Continuous | Full Continuous |
|---------|-------------------|-----------------|
| **Lines of code** | 65 | 250 |
| **Sustained detection** | ✅ | ✅ |
| **Auto-kill** | ✅ | ✅ |
| **Alert cooldown** | ✅ | ✅ |
| **Detailed logs** | ❌ | ✅ |
| **Per-app details** | ❌ | ✅ |
| **CLI options** | ❌ | ✅ |
| **Use case** | Simple setups | Production |

## Integration with Email

```bash
#!/bin/bash
python monitor_continuous_minimal.py 2>&1 | while read line; do
    echo "$line"
    if echo "$line" | grep -q "ALERT:"; then
        echo "$line" | mail -s "EMR Alert" you@email.com
    fi
done
```

## Installation

```bash
# 1. Copy to server
scp monitor_continuous_minimal.py hadoop@edge-node:/home/hadoop/

# 2. Edit configuration
nano monitor_continuous_minimal.py

# 3. Run
python monitor_continuous_minimal.py

# Or run in background
nohup python monitor_continuous_minimal.py > /tmp/monitor.log 2>&1 &
```

Perfect for simple deployments where you just need sustained monitoring without all the bells and whistles!
