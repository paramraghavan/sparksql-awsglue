#!/usr/bin/env python3
"""Ultra-minimal cluster monitor - exits 1 if threshold exceeded"""
import requests, sys

YARN_URL = "http://localhost:8088"  # Change as needed
THRESHOLD = 85  # Alert at 85% usage

try:
    m = requests.get(f"{YARN_URL}/ws/v1/cluster/metrics", timeout=5).json()['clusterMetrics']
    mem_pct = (m['allocatedMB'] / m['totalMB'] * 100)
    cpu_pct = (m['allocatedVirtualCores'] / m['totalVirtualCores'] * 100)
    
    if mem_pct >= THRESHOLD or cpu_pct >= THRESHOLD:
        print(f"ALERT: Memory {mem_pct:.0f}% | vCPU {cpu_pct:.0f}% | Threshold {THRESHOLD}%")
        sys.exit(1)
    else:
        print(f"OK: Memory {mem_pct:.0f}% | vCPU {cpu_pct:.0f}%")
        sys.exit(0)
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(2)
