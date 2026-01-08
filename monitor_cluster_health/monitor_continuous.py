#!/usr/bin/env python3
"""
EMR Cluster Continuous Monitor - Detects SUSTAINED high resource usage
Runs as daemon, only alerts/kills on sustained threshold breaches
"""
import requests
import sys
import argparse
import time
import json
from datetime import datetime
from collections import deque
from typing import List, Dict, Any

class ClusterMonitor:
    def __init__(self, yarn_url, memory_threshold=85, vcpu_threshold=85,
                 sustained_period=300, check_interval=60, auto_kill=False, kill_count=1):
        """
        Args:
            yarn_url: YARN ResourceManager URL
            memory_threshold: Memory % threshold
            vcpu_threshold: vCPU % threshold
            sustained_period: Seconds threshold must be exceeded (default: 300 = 5 min)
            check_interval: Seconds between checks (default: 60)
            auto_kill: Auto-kill offending jobs
            kill_count: Number of apps to kill
        """
        self.yarn_url = yarn_url
        self.memory_threshold = memory_threshold
        self.vcpu_threshold = vcpu_threshold
        self.sustained_period = sustained_period
        self.check_interval = check_interval
        self.auto_kill = auto_kill
        self.kill_count = kill_count
        
        # Track history: (timestamp, memory_pct, vcpu_pct)
        max_history = int(sustained_period / check_interval) + 5
        self.history = deque(maxlen=max_history)
        self.last_alert_time = 0
        self.alert_cooldown = 600  # Don't re-alert within 10 minutes
        
    def get_cluster_metrics(self):
        """Get current cluster metrics"""
        try:
            response = requests.get(f"{self.yarn_url}/ws/v1/cluster/metrics", timeout=10)
            metrics = response.json()['clusterMetrics']
            
            total_memory = metrics['totalMB']
            allocated_memory = metrics['allocatedMB']
            memory_pct = (allocated_memory / total_memory * 100) if total_memory > 0 else 0
            
            total_vcpus = metrics['totalVirtualCores']
            allocated_vcpus = metrics['allocatedVirtualCores']
            vcpu_pct = (allocated_vcpus / total_vcpus * 100) if total_vcpus > 0 else 0
            
            return {
                'timestamp': time.time(),
                'memory_pct': memory_pct,
                'vcpu_pct': vcpu_pct,
                'memory_gb': allocated_memory / 1024,
                'total_memory_gb': total_memory / 1024,
                'vcpus': allocated_vcpus,
                'total_vcpus': total_vcpus,
                'unhealthy_nodes': metrics.get('unhealthyNodes', 0),
                'pending_apps': metrics.get('appsPending', 0),
                'running_apps': metrics.get('appsRunning', 0),
                'active_nodes': metrics.get('activeNodes', 0)
            }
        except Exception as e:
            print(f"[{datetime.now()}] ERROR getting metrics: {e}")
            return None
    
    def is_sustained_breach(self):
        """Check if threshold has been exceeded for sustained period"""
        if len(self.history) < 2:
            return False, 0
        
        # Calculate how long threshold has been breached
        breach_start = None
        current_time = time.time()
        
        for entry in self.history:
            timestamp, mem_pct, vcpu_pct = entry
            
            if mem_pct >= self.memory_threshold or vcpu_pct >= self.vcpu_threshold:
                if breach_start is None:
                    breach_start = timestamp
            else:
                # Threshold not breached, reset
                breach_start = None
        
        if breach_start is not None:
            breach_duration = current_time - breach_start
            if breach_duration >= self.sustained_period:
                return True, breach_duration
        
        return False, 0
    
    def get_running_applications(self):
        """Get all running applications"""
        try:
            response = requests.get(f"{self.yarn_url}/ws/v1/cluster/apps?state=RUNNING", timeout=10)
            apps_data = response.json().get('apps', {})
            if not apps_data:
                return []
            return apps_data.get('app', [])
        except:
            return []
    
    def kill_application(self, app_id: str, reason: str) -> bool:
        """Kill a YARN application"""
        try:
            kill_url = f"{self.yarn_url}/ws/v1/cluster/apps/{app_id}/state"
            payload = {"state": "KILLED"}
            response = requests.put(kill_url, json=payload, timeout=10)
            
            if response.status_code == 200:
                self.log_action(f"KILLED|{app_id}|{reason}")
                return True
            return False
        except:
            return False
    
    def log_action(self, message: str):
        """Log actions to file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp}|{message}\n"
        
        try:
            with open("/tmp/emr_continuous_monitor.log", "a") as f:
                f.write(log_entry)
        except:
            pass
    
    def handle_sustained_breach(self, current_metrics, breach_duration):
        """Handle sustained threshold breach"""
        # Check alert cooldown
        if time.time() - self.last_alert_time < self.alert_cooldown:
            return
        
        alert_msg = f"""
{'='*70}
SUSTAINED HIGH USAGE ALERT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*70}

CRITICAL: Cluster has been above threshold for {breach_duration/60:.1f} minutes

CURRENT METRICS:
  Memory: {current_metrics['memory_pct']:.1f}% ({current_metrics['memory_gb']:.1f}/{current_metrics['total_memory_gb']:.1f} GB)
  vCPUs:  {current_metrics['vcpu_pct']:.1f}% ({current_metrics['vcpus']}/{current_metrics['total_vcpus']} cores)
  Active Nodes: {current_metrics['active_nodes']}
  Unhealthy Nodes: {current_metrics['unhealthy_nodes']}
  Running Apps: {current_metrics['running_apps']}
  Pending Apps: {current_metrics['pending_apps']}

THRESHOLD: Memory {self.memory_threshold}%, vCPU {self.vcpu_threshold}%
SUSTAINED PERIOD: {self.sustained_period/60:.1f} minutes
"""
        
        killed_apps = []
        
        # Auto-kill if enabled
        if self.auto_kill:
            apps = self.get_running_applications()
            if apps:
                # Sort by memory, kill top N
                top_apps = sorted(apps, key=lambda x: x.get('allocatedMB', 0), reverse=True)[:self.kill_count]
                
                alert_msg += f"\nAUTO-KILLING {len(top_apps)} APPLICATION(S):\n"
                
                for app in top_apps:
                    app_id = app['id']
                    app_name = app['name']
                    app_memory = app.get('allocatedMB', 0) / 1024
                    app_vcores = app.get('allocatedVCores', 0)
                    
                    if self.kill_application(app_id, f"Sustained {breach_duration/60:.1f}min above {max(current_metrics['memory_pct'], current_metrics['vcpu_pct']):.1f}%"):
                        alert_msg += f"  • KILLED: {app_id} ({app_name})\n"
                        alert_msg += f"    Resources: {app_memory:.1f} GB, {app_vcores} vCores\n"
                        killed_apps.append(app_id)
        
        alert_msg += f"\n{'='*70}\n"
        
        print(alert_msg)
        self.log_action(f"ALERT|Memory:{current_metrics['memory_pct']:.1f}%|vCPU:{current_metrics['vcpu_pct']:.1f}%|Duration:{breach_duration:.0f}s|Killed:{len(killed_apps)}")
        
        self.last_alert_time = time.time()
    
    def run(self):
        """Main monitoring loop"""
        print(f"Starting EMR Continuous Monitor")
        print(f"YARN URL: {self.yarn_url}")
        print(f"Thresholds: Memory {self.memory_threshold}%, vCPU {self.vcpu_threshold}%")
        print(f"Sustained Period: {self.sustained_period}s ({self.sustained_period/60:.1f} min)")
        print(f"Check Interval: {self.check_interval}s")
        print(f"Auto-Kill: {'ENABLED' if self.auto_kill else 'DISABLED'}")
        if self.auto_kill:
            print(f"Kill Count: {self.kill_count}")
        print(f"{'='*70}\n")
        
        self.log_action(f"STARTED|Threshold:{self.memory_threshold}%|Sustained:{self.sustained_period}s|AutoKill:{self.auto_kill}")
        
        check_count = 0
        
        try:
            while True:
                check_count += 1
                current_metrics = self.get_cluster_metrics()
                
                if current_metrics:
                    # Add to history
                    self.history.append((
                        current_metrics['timestamp'],
                        current_metrics['memory_pct'],
                        current_metrics['vcpu_pct']
                    ))
                    
                    # Check for sustained breach
                    is_breach, breach_duration = self.is_sustained_breach()
                    
                    # Print status
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    status = "BREACH" if is_breach else "OK"
                    print(f"[{timestamp}] Check #{check_count}: Memory {current_metrics['memory_pct']:.1f}%, "
                          f"vCPU {current_metrics['vcpu_pct']:.1f}% - {status}", end="")
                    
                    if is_breach:
                        print(f" (sustained {breach_duration/60:.1f} min)")
                        self.handle_sustained_breach(current_metrics, breach_duration)
                    else:
                        # Show if currently above threshold but not sustained
                        if current_metrics['memory_pct'] >= self.memory_threshold or current_metrics['vcpu_pct'] >= self.vcpu_threshold:
                            print(f" (transient spike, monitoring...)")
                        else:
                            print()
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            print("\n\nMonitor stopped by user")
            self.log_action("STOPPED|User interrupt")
            sys.exit(0)

def main():
    parser = argparse.ArgumentParser(
        description='Continuous EMR cluster monitor - detects SUSTAINED high usage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor with 5-minute sustained period (default)
  python monitor_continuous.py
  
  # Require 10 minutes of sustained high usage before alert
  python monitor_continuous.py --sustained-period 600
  
  # Check every 30 seconds (more responsive)
  python monitor_continuous.py --check-interval 30
  
  # Enable auto-kill after 5 min sustained breach
  python monitor_continuous.py --auto-kill --sustained-period 300
  
  # Conservative: 10 min sustained, high threshold
  python monitor_continuous.py --sustained-period 600 --memory-threshold 90
  
  # Run as background daemon
  nohup python monitor_continuous.py > /tmp/monitor.log 2>&1 &
"""
    )
    parser.add_argument('--yarn-url', default='http://localhost:8088',
                       help='YARN ResourceManager URL')
    parser.add_argument('--memory-threshold', type=int, default=85,
                       help='Memory threshold %% (default: 85)')
    parser.add_argument('--vcpu-threshold', type=int, default=85,
                       help='vCPU threshold %% (default: 85)')
    parser.add_argument('--sustained-period', type=int, default=300,
                       help='Seconds threshold must be exceeded (default: 300 = 5 min)')
    parser.add_argument('--check-interval', type=int, default=60,
                       help='Seconds between checks (default: 60)')
    parser.add_argument('--auto-kill', action='store_true',
                       help='Auto-kill top resource hogs on sustained breach')
    parser.add_argument('--kill-count', type=int, default=1,
                       help='Number of apps to kill (default: 1)')
    
    args = parser.parse_args()
    
    monitor = ClusterMonitor(
        yarn_url=args.yarn_url,
        memory_threshold=args.memory_threshold,
        vcpu_threshold=args.vcpu_threshold,
        sustained_period=args.sustained_period,
        check_interval=args.check_interval,
        auto_kill=args.auto_kill,
        kill_count=args.kill_count
    )
    
    monitor.run()

if __name__ == '__main__':
    main()
