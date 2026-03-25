# Health Status Overview Tab

## Overview

New **🏥 Health Status** tab provides a **single unified view** of all 5 EMR clusters with color-coded status.

## Features

### 1. **Cluster Summary**
Quick status indicators for the entire cluster:
- **Cluster Status**: Overall health (🟢 Healthy | 🟠 Warning | 🔴 Critical)
- **Healthy Nodes**: Count of nodes with no issues
- **Warning Nodes**: Count of nodes with warnings
- **Critical Nodes**: Count of nodes with critical issues

### 2. **Per-Node Health Cards**
Individual card for each EMR node showing:

**Card Color** (Border + Background):
- 🟢 **Green** - All metrics OK
- 🟠 **Orange** - Some metrics in warning range
- 🔴 **Red** - At least one metric in critical range
- ⚪ **Gray** - No real-time data available

**Card Contents**:
- EMR number (1-5)
- Overall status badge
- 4 key metrics:
  - CPU (%)
  - Memory (%)
  - Disk (%)
  - System Load (1m)
- Issues list (if any) with warnings

**Status Determination**:
```
IF any metric >= CRITICAL threshold
  → Status = CRITICAL (Red 🔴)
ELSE IF any metric >= WARNING threshold
  → Status = WARNING (Orange 🟠)
ELSE
  → Status = HEALTHY (Green 🟢)
```

## Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| CPU (%) | > 60 | > 80 |
| Memory (%) | > 60 | > 80 |
| Disk (%) | > 60 | > 85 |
| System Load (1m) | > CPU count | > 2× CPU count |
| YARN Pending | > 5 | > 10 |

## Visual Layout

```
┌─────────────────────────────────────────────────────┐
│         🏥 Cluster Health Overview                  │
├─────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ Cluster  │  │ Healthy  │  │ Warning  │ Critical│
│  │ Status   │  │ Nodes: 3 │  │ Nodes: 1 │ Nodes:1│
│  │    🟢    │  │  / 5     │  │  / 5     │  / 5   │
│  │ HEALTHY  │  │          │  │          │        │
│  └──────────┘  └──────────┘  └──────────┘────────┘
├─────────────────────────────────────────────────────┤
│                    NODE CARDS                       │
├─────────────────────────────────────────────────────┤
│ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│ │ EMR 1       │  │ EMR 2       │  │ EMR 3       │ │
│ │ 🟢 HEALTHY  │  │ 🟢 HEALTHY  │  │ 🟠 WARNING  │ │
│ │             │  │             │  │             │ │
│ │ CPU: 45.2%  │  │ CPU: 52.1%  │  │ CPU: 62.3%  │ │
│ │ MEM: 60.1%  │  │ MEM: 61.5%  │  │ MEM: 65.2%  │ │
│ │ DISK: 50.3% │  │ DISK: 48.2% │  │ DISK: 52.1% │ │
│ │ LOAD: 1.5   │  │ LOAD: 2.1   │  │ LOAD: 3.2   │ │
│ │             │  │             │  │             │ │
│ │ ✓ No issues │  │ ✓ No issues │  │ ⚠️ Memory    │ │
│ │             │  │             │  │    warning  │ │
│ └─────────────┘  └─────────────┘  └─────────────┘ │
│ ┌─────────────┐  ┌─────────────┐                   │
│ │ EMR 4       │  │ EMR 5       │                   │
│ │ 🟢 HEALTHY  │  │ 🔴 CRITICAL │                   │
│ │             │  │             │                   │
│ │ CPU: 48.5%  │  │ CPU: 85.2%  │                   │
│ │ MEM: 55.3%  │  │ MEM: 78.1%  │                   │
│ │ DISK: 51.2% │  │ DISK: 82.5% │                   │
│ │ LOAD: 1.8   │  │ LOAD: 6.2   │                   │
│ │             │  │             │                   │
│ │ ✓ No issues │  │ ⚠️ CPU:      │                   │
│ │             │  │    critical │                   │
│ │             │  │ ⚠️ Disk:     │                   │
│ │             │  │    warning  │                   │
│ └─────────────┘  └─────────────┘                   │
└─────────────────────────────────────────────────────┘
```

## Auto-Refresh

- **Updates**: Every 30 seconds when tab is active
- **Data Source**: Real-time snapshots (`realtime_snapshot.json`) from S3
- **Stops**: When switching to different tab

## API Endpoint

**GET /api/health-overview**

Response structure:
```json
{
  "cluster_status": "healthy|warning|critical|unknown",
  "critical_nodes": 0,
  "warning_nodes": 1,
  "healthy_nodes": 4,
  "nodes": {
    "10.0.0.1": {
      "emr_number": 1,
      "status": "healthy",
      "severity": 0,
      "issues": [],
      "metrics": {
        "cpu_percent": 45.2,
        "memory_percent": 60.1,
        "disk_percent": 50.3,
        "load_1m": 1.5,
        "yarn_pending": 2
      },
      "timestamp": "2024-03-25T14:30:00.123456"
    },
    ...
  }
}
```

## Use Cases

### At-a-Glance Monitoring
Quickly see which clusters need attention without drilling into details.

### Incident Response
Identify which nodes are having issues and what the problems are.

### Capacity Planning
Track which nodes consistently hit warning/critical thresholds.

### Triage
Determine which node to investigate first based on severity.

## Status Transitions

### Example 1: CPU Increases
```
Time: 10:00  →  Status: 🟢 HEALTHY (CPU: 45%)
Time: 10:15  →  Status: 🟠 WARNING  (CPU: 65% - above warning)
Time: 10:30  →  Status: 🔴 CRITICAL (CPU: 82% - above critical)
Time: 10:45  →  Status: 🟠 WARNING  (CPU: 70% - back below critical)
Time: 11:00  →  Status: 🟢 HEALTHY  (CPU: 50% - back below warning)
```

### Example 2: Multiple Issues
If a node has:
- CPU: 75% (warning)
- Memory: 88% (critical)
- Disk: 52% (healthy)

Overall Status: 🔴 CRITICAL (because at least one metric is critical)

## Color Scheme

### CSS Classes
- `.healthy` - Green (#4CAF50)
- `.warning` - Orange (#FF9800)
- `.critical` - Red (#F44336)
- `.unknown` - Gray (#9E9E9E)

### Icon Reference
- 🟢 Healthy
- 🟠 Warning
- 🔴 Critical
- ⚪ Unknown/No Data

## Troubleshooting

### All Nodes Show "Unknown"
- **Cause**: No real-time snapshots available yet
- **Solution**: Wait 15 minutes for first snapshot upload

### Status Not Updating
- **Cause**: Auto-refresh stopped
- **Solution**: Switch to different tab and back to Health Status tab

### Metrics Show as 0
- **Cause**: Old/incomplete snapshot data
- **Solution**: Monitor script may be stopped - check logs

## Configuration

No configuration needed! The thresholds are built into the health calculation logic.

To customize thresholds, edit the `calculate_node_health()` function in `emr_dashboard.py`:

```python
def calculate_node_health(snapshot):
    thresholds = {
        'cpu_percent': {'warning': 60, 'critical': 80},
        'memory_percent': {'warning': 60, 'critical': 80},
        'disk_percent': {'warning': 60, 'critical': 85},
        'load_1m': {'warning': 4, 'critical': 8},
        'yarn_pending': {'warning': 5, 'critical': 10}
    }
    # Adjust values as needed
```

## Performance

- **API Call**: Single `/api/health-overview` request
- **Data Size**: ~5-10 KB (all 5 nodes)
- **Refresh Rate**: 30 seconds
- **S3 Requests**: None (uses cached real-time snapshots)

## Benefits Over Other Tabs

| Feature | Overview | Health Status | Real-Time | Details |
|---------|----------|---------------|-----------|---------|
| Single unified view | ❌ | ✅ | ❌ | ❌ |
| All nodes at once | ❌ | ✅ | Per-node | ❌ |
| Color-coded status | ❌ | ✅ | ❌ | ❌ |
| Issue highlights | ❌ | ✅ | ✅ | ❌ |
| Historical trends | ✅ | ❌ | ❌ | ✅ |
| Real-time data | ❌ | ✅ | ✅ | ❌ |

Perfect for: **Quick cluster health check** ✅

## Screenshots

### Healthy Cluster
All cards 🟢 green, summary shows 5 healthy nodes

### Mixed Status
Some cards 🟢, some 🟠, summary reflects the distribution

### Problem Cluster
One or more 🔴 red cards alert to critical issues

## Next Steps

1. Access **🏥 Health Status** tab
2. Review cluster summary at top
3. Check any orange (🟠) or red (🔴) cards for issues
4. Click on detailed tabs for trend analysis
5. Implement fixes based on findings

---

**Perfect for:** Quick health checks, incident triage, capacity planning!
