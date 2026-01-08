#!/usr/bin/env python3
"""Minimal continuous monitor - detects sustained high usage"""
import requests, time, sys
from datetime import datetime

# Configuration
YARN_URL = "http://localhost:8088"
THRESHOLD = 85              # Alert threshold %
SUSTAINED_SECONDS = 300     # Must be high for this long (5 min)
CHECK_INTERVAL = 60         # Check every N seconds
AUTO_KILL = False           # Set True to auto-kill

# State tracking
breach_start = None
last_alert = 0
ALERT_COOLDOWN = 600        # Don't re-alert within 10 min

def get_metrics():
    """Get current memory and vCPU usage %"""
    try:
        r = requests.get(f"{YARN_URL}/ws/v1/cluster/metrics", timeout=5)
        m = r.json()['clusterMetrics']
        mem = (m['allocatedMB'] / m['totalMB'] * 100)
        cpu = (m['allocatedVirtualCores'] / m['totalVirtualCores'] * 100)
        return mem, cpu, m
    except:
        return None, None, None

def kill_top_app():
    """Kill top memory consumer"""
    try:
        r = requests.get(f"{YARN_URL}/ws/v1/cluster/apps?state=RUNNING", timeout=5)
        apps = r.json().get('apps', {}).get('app', [])
        if apps:
            top = max(apps, key=lambda x: x.get('allocatedMB', 0))
            app_id = top['id']
            requests.put(f"{YARN_URL}/ws/v1/cluster/apps/{app_id}/state", 
                        json={"state": "KILLED"}, timeout=10)
            return app_id, top['name'], top.get('allocatedMB', 0) / 1024
    except:
        pass
    return None, None, 0

print(f"Continuous Monitor: Threshold {THRESHOLD}%, Sustained {SUSTAINED_SECONDS}s, Check every {CHECK_INTERVAL}s")
print(f"Auto-Kill: {'ENABLED' if AUTO_KILL else 'DISABLED'}\n")

try:
    while True:
        mem, cpu, metrics = get_metrics()
        
        if mem is not None:
            now = time.time()
            is_high = mem >= THRESHOLD or cpu >= THRESHOLD
            
            # Track breach duration
            if is_high:
                if breach_start is None:
                    breach_start = now
                breach_duration = now - breach_start
            else:
                breach_start = None
                breach_duration = 0
            
            # Status
            ts = datetime.now().strftime("%H:%M:%S")
            status = "HIGH" if is_high else "OK"
            print(f"[{ts}] Mem {mem:.0f}% | CPU {cpu:.0f}% | {status}", end="")
            
            # Alert on sustained breach
            if breach_duration >= SUSTAINED_SECONDS and (now - last_alert) > ALERT_COOLDOWN:
                print(f" → SUSTAINED {breach_duration/60:.1f}min!")
                print(f"\n{'='*60}")
                print(f"ALERT: Above {THRESHOLD}% for {breach_duration/60:.1f} minutes")
                print(f"Memory: {mem:.1f}% | vCPU: {cpu:.1f}%")
                
                if AUTO_KILL:
                    app_id, name, gb = kill_top_app()
                    if app_id:
                        print(f"KILLED: {app_id} ({name}) - {gb:.1f}GB")
                
                print(f"{'='*60}\n")
                last_alert = now
            elif is_high:
                print(f" (monitoring {breach_duration:.0f}s)")
            else:
                print()
        
        time.sleep(CHECK_INTERVAL)
        
except KeyboardInterrupt:
    print("\nStopped")
    sys.exit(0)
