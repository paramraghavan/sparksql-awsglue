#!/usr/bin/env python3
"""Ultra-minimal: Single line output"""
import requests, sys
try:
    m = requests.get("http://localhost:8088/ws/v1/cluster/metrics", timeout=5).json()['clusterMetrics']
    print(f"MEM: {m['allocatedMB']/1024:.0f}/{m['totalMB']/1024:.0f}GB | CORES: {m['allocatedVirtualCores']}/{m['totalVirtualCores']}")
except Exception as e:
    print(f"Error: {e}"); sys.exit(1)
