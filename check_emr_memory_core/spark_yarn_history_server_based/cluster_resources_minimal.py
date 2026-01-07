#!/usr/bin/env python3
"""
Minimal EMR Cluster Resources - Just the essentials
"""
import requests
import sys

# Configuration
YARN_URL = "http://localhost:8088"  # Change if needed

def get_resources():
    try:
        response = requests.get(f"{YARN_URL}/ws/v1/cluster/metrics", timeout=5)
        metrics = response.json()['clusterMetrics']
        
        # Memory in GB
        total_memory = metrics['totalMB'] / 1024
        allocated_memory = metrics['allocatedMB'] / 1024
        
        # vCores
        total_cores = metrics['totalVirtualCores']
        allocated_cores = metrics['allocatedVirtualCores']
        
        print(f"Memory:  {allocated_memory:.1f} GB / {total_memory:.1f} GB ({allocated_memory/total_memory*100:.1f}%)")
        print(f"Cores:   {allocated_cores} / {total_cores} ({allocated_cores/total_cores*100:.1f}%)")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    get_resources()
