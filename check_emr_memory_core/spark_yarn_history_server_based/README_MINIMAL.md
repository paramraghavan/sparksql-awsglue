# Minimal EMR Cluster Resource Scripts

Three versions of minimal scripts to quickly check cluster resources.

## 1. Ultra-Minimal One-Liner (`cluster_resources_oneliner.py`)

**Single line output - perfect for scripts and monitoring**

```bash
python cluster_resources_oneliner.py
```

Output:
```
MEM: 256/480GB | CORES: 48/80
```

Use when: Quick status checks, scripts, dashboards

---

## 2. Basic Minimal (`cluster_resources_minimal.py`)

**Two lines with percentages**

```bash
python cluster_resources_minimal.py
```

Output:
```
Memory:  256.0 GB / 480.0 GB (53.3%)
Cores:   48 / 80 (60.0%)
```

Use when: Quick manual checks, cron job outputs

---

## 3. Simple with Details (`cluster_resources_simple.py`)

**Formatted output with available resources**

```bash
python cluster_resources_simple.py
```

Output:
```
MEMORY
  Total:      480.0 GB
  Allocated:  256.0 GB (53.3%)
  Available:  224.0 GB

CORES
  Total:      80
  Allocated:  48 (60.0%)
  Available:  32
```

**With custom YARN URL:**
```bash
python cluster_resources_simple.py --yarn-url http://master-node:8088
```

Use when: Need available resources too, slightly more detail

---

## Quick Comparison

| Script | Lines | Size | Use Case |
|--------|-------|------|----------|
| oneliner | 1 | 5 lines | Scripts, monitoring |
| minimal | 2 | 23 lines | Quick checks |
| simple | 10 | 41 lines | Readable output |

## Configuration

All scripts default to `http://localhost:8088` for YARN.

To change:

**oneliner.py and minimal.py:**
```python
YARN_URL = "http://your-master-node:8088"  # Edit line 6
```

**simple.py:**
```bash
python cluster_resources_simple.py --yarn-url http://your-master-node:8088
```

## Integration Examples

### Bash Script
```bash
#!/bin/bash
RESOURCES=$(python cluster_resources_oneliner.py)
echo "Cluster Status: $RESOURCES"
```

### Cron Job (every 5 minutes)
```bash
*/5 * * * * /path/to/cluster_resources_minimal.py >> /var/log/cluster_resources.log
```

### Shell Alias
```bash
alias emr-status='python /path/to/cluster_resources_minimal.py'
```

### Pre-job Check
```bash
# Check before submitting Spark job
python cluster_resources_simple.py
if [ $? -eq 0 ]; then
    spark-submit my_job.py
fi
```

## Requirements

```bash
pip install requests
```

## Troubleshooting

If you get connection errors:
1. Check YARN is running: `curl http://localhost:8088/ws/v1/cluster/metrics`
2. Update YARN_URL if not on master node
3. Ensure port 8088 is accessible
