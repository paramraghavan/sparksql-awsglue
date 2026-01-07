#!/usr/bin/env python3
"""
Minimal EMR Cluster Resources with CLI support
Usage: python cluster_resources_simple.py [--yarn-url URL]
"""
import requests
import sys
import argparse

def get_resources(yarn_url):
    try:
        response = requests.get(f"{yarn_url}/ws/v1/cluster/metrics", timeout=5)
        metrics = response.json()['clusterMetrics']
        
        # Memory in GB
        total_memory = metrics['totalMB'] / 1024
        allocated_memory = metrics['allocatedMB'] / 1024
        available_memory = metrics['availableMB'] / 1024
        
        # vCores
        total_cores = metrics['totalVirtualCores']
        allocated_cores = metrics['allocatedVirtualCores']
        available_cores = metrics['availableVirtualCores']
        
        print(f"MEMORY")
        print(f"  Total:      {total_memory:.1f} GB")
        print(f"  Allocated:  {allocated_memory:.1f} GB ({allocated_memory/total_memory*100:.1f}%)")
        print(f"  Available:  {available_memory:.1f} GB")
        print(f"\nCORES")
        print(f"  Total:      {total_cores}")
        print(f"  Allocated:  {allocated_cores} ({allocated_cores/total_cores*100:.1f}%)")
        print(f"  Available:  {available_cores}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to YARN: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get EMR cluster resources')
    parser.add_argument('--yarn-url', default='http://localhost:8088', 
                       help='YARN ResourceManager URL (default: http://localhost:8088)')
    args = parser.parse_args()
    
    get_resources(args.yarn_url)
