#!/usr/bin/env python3
"""Minimal cluster monitor with auto-kill option"""
import requests, sys, json

YARN_URL = "http://localhost:8088"
THRESHOLD = 85
AUTO_KILL = True  # Set to False to only alert
KILL_COUNT = 1    # Number of top apps to kill

def kill_app(app_id):
    """Kill a YARN application"""
    try:
        url = f"{YARN_URL}/ws/v1/cluster/apps/{app_id}/state"
        requests.put(url, json={"state": "KILLED"}, timeout=10)
        return True
    except:
        return False

try:
    # Check cluster metrics
    m = requests.get(f"{YARN_URL}/ws/v1/cluster/metrics", timeout=5).json()['clusterMetrics']
    mem_pct = (m['allocatedMB'] / m['totalMB'] * 100)
    cpu_pct = (m['allocatedVirtualCores'] / m['totalVirtualCores'] * 100)
    
    if mem_pct >= THRESHOLD or cpu_pct >= THRESHOLD:
        print(f"ALERT: Memory {mem_pct:.0f}% | vCPU {cpu_pct:.0f}% | Threshold {THRESHOLD}%")
        
        # Auto-kill if enabled
        if AUTO_KILL:
            apps = requests.get(f"{YARN_URL}/ws/v1/cluster/apps?state=RUNNING", timeout=5).json()
            apps_list = apps.get('apps', {}).get('app', [])
            
            if apps_list:
                # Sort by memory usage, kill top N
                top_apps = sorted(apps_list, key=lambda x: x.get('allocatedMB', 0), reverse=True)[:KILL_COUNT]
                
                for app in top_apps:
                    app_id = app['id']
                    app_name = app['name']
                    mem_gb = app.get('allocatedMB', 0) / 1024
                    
                    if kill_app(app_id):
                        print(f"KILLED: {app_id} ({app_name}) - {mem_gb:.1f}GB")
                        with open("/tmp/emr_kills.log", "a") as f:
                            f.write(f"{app_id}|{app_name}|{mem_gb:.1f}GB\n")
        
        sys.exit(1)
    else:
        print(f"OK: Memory {mem_pct:.0f}% | vCPU {cpu_pct:.0f}%")
        sys.exit(0)
        
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(2)
