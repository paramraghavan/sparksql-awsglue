#!/usr/bin/env python3
"""
EMR Cluster Health Monitor with Auto-Kill
Monitors cluster health and optionally terminates resource-hogging applications
"""
import requests
import sys
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any

def get_running_applications(yarn_url: str) -> List[Dict[str, Any]]:
    """Get all running applications with resource usage"""
    try:
        response = requests.get(f"{yarn_url}/ws/v1/cluster/apps?state=RUNNING", timeout=10)
        apps_data = response.json().get('apps', {})
        if not apps_data:
            return []
        return apps_data.get('app', [])
    except Exception as e:
        print(f"Warning: Could not get running applications - {e}")
        return []

def kill_application(yarn_url: str, app_id: str, reason: str = "Resource limit exceeded") -> bool:
    """Kill a YARN application"""
    try:
        kill_url = f"{yarn_url}/ws/v1/cluster/apps/{app_id}/state"
        payload = {"state": "KILLED"}
        response = requests.put(kill_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            log_kill(app_id, reason)
            return True
        else:
            print(f"Failed to kill {app_id}: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"Error killing {app_id}: {e}")
        return False

def log_kill(app_id: str, reason: str):
    """Log killed application to file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} | KILLED | {app_id} | {reason}\n"
    
    try:
        with open("/tmp/emr_auto_kill.log", "a") as f:
            f.write(log_entry)
    except:
        pass  # Don't fail if logging fails

def identify_offenders(apps: List[Dict[str, Any]], top_n: int = 3) -> List[Dict[str, Any]]:
    """Identify top resource-consuming applications"""
    # Sort by memory allocated (descending)
    sorted_apps = sorted(apps, key=lambda x: x.get('allocatedMB', 0), reverse=True)
    return sorted_apps[:top_n]

def check_cluster_health(yarn_url, memory_threshold=85, vcpu_threshold=85, 
                        auto_kill=False, dry_run=False, kill_count=1):
    """
    Check cluster health and optionally kill offending jobs
    """
    try:
        # Get cluster metrics
        response = requests.get(f"{yarn_url}/ws/v1/cluster/metrics", timeout=10)
        metrics = response.json()['clusterMetrics']
        
        # Calculate utilization
        total_memory = metrics['totalMB']
        allocated_memory = metrics['allocatedMB']
        memory_util = (allocated_memory / total_memory * 100) if total_memory > 0 else 0
        
        total_vcpus = metrics['totalVirtualCores']
        allocated_vcpus = metrics['allocatedVirtualCores']
        vcpu_util = (allocated_vcpus / total_vcpus * 100) if total_vcpus > 0 else 0
        
        alerts = []
        threshold_exceeded = False
        
        # Check thresholds
        if memory_util >= memory_threshold:
            alerts.append(f"MEMORY CRITICAL: {memory_util:.1f}% used ({allocated_memory/1024:.1f}/{total_memory/1024:.1f} GB)")
            threshold_exceeded = True
        
        if vcpu_util >= vcpu_threshold:
            alerts.append(f"VCPU CRITICAL: {vcpu_util:.1f}% used ({allocated_vcpus}/{total_vcpus} cores)")
            threshold_exceeded = True
        
        unhealthy_nodes = metrics.get('unhealthyNodes', 0)
        if unhealthy_nodes > 0:
            alerts.append(f"UNHEALTHY NODES: {unhealthy_nodes} node(s) unhealthy")
        
        pending_apps = metrics.get('appsPending', 0)
        if pending_apps > 10:
            alerts.append(f"QUEUE BACKUP: {pending_apps} applications pending")
        
        # If threshold exceeded and auto-kill enabled, find and kill offenders
        killed_apps = []
        if threshold_exceeded and auto_kill:
            running_apps = get_running_applications(yarn_url)
            
            if running_apps:
                offenders = identify_offenders(running_apps, top_n=kill_count)
                
                for app in offenders:
                    app_id = app['id']
                    app_name = app['name']
                    app_memory = app.get('allocatedMB', 0) / 1024
                    app_vcores = app.get('allocatedVCores', 0)
                    
                    kill_reason = f"Cluster at {max(memory_util, vcpu_util):.1f}% capacity"
                    
                    if dry_run:
                        print(f"[DRY-RUN] Would kill: {app_id} ({app_name}) - {app_memory:.1f}GB, {app_vcores} cores")
                    else:
                        print(f"Killing: {app_id} ({app_name}) - {app_memory:.1f}GB, {app_vcores} cores")
                        if kill_application(yarn_url, app_id, kill_reason):
                            killed_apps.append({
                                'id': app_id,
                                'name': app_name,
                                'memory_gb': app_memory,
                                'vcores': app_vcores
                            })
        
        # Build alert message
        if alerts or killed_apps:
            message = f"""
EMR CLUSTER ALERT - {yarn_url}
{'='*70}
"""
            if alerts:
                message += "CRITICAL THRESHOLDS EXCEEDED:\n\n"
                message += '\n'.join('  - ' + alert for alert in alerts)
                message += "\n"
            
            if killed_apps:
                message += f"\n{'='*70}\n"
                message += f"AUTO-KILLED {len(killed_apps)} APPLICATION(S):\n\n"
                for app in killed_apps:
                    message += f"  • {app['id']}\n"
                    message += f"    Name: {app['name']}\n"
                    message += f"    Resources: {app['memory_gb']:.1f} GB, {app['vcores']} vCores\n"
                message += f"\nKilled apps logged to: /tmp/emr_auto_kill.log\n"
            
            message += f"\n{'='*70}\n"
            message += "CURRENT STATUS:\n"
            message += f"  Memory: {memory_util:.1f}% ({allocated_memory/1024:.1f}/{total_memory/1024:.1f} GB)\n"
            message += f"  vCPUs:  {vcpu_util:.1f}% ({allocated_vcpus}/{total_vcpus} cores)\n"
            message += f"  Active Nodes: {metrics.get('activeNodes', 0)}\n"
            message += f"  Unhealthy Nodes: {unhealthy_nodes}\n"
            message += f"  Running Apps: {metrics.get('appsRunning', 0)}\n"
            message += f"  Pending Apps: {pending_apps}\n"
            message += f"\n{'='*70}\n"
            
            return False, message
        else:
            return True, f"Cluster healthy - Memory: {memory_util:.1f}%, vCPUs: {vcpu_util:.1f}%"
            
    except Exception as e:
        return False, f"ERROR: Unable to check cluster health - {e}"

def main():
    parser = argparse.ArgumentParser(
        description='Monitor EMR cluster health and optionally kill offending jobs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor only (no auto-kill)
  python monitor_cluster_health.py
  
  # Auto-kill top resource hog if threshold exceeded
  python monitor_cluster_health.py --auto-kill --kill-count 1
  
  # Dry-run to see what would be killed
  python monitor_cluster_health.py --auto-kill --dry-run
  
  # Kill top 2 apps with custom threshold
  python monitor_cluster_health.py --auto-kill --kill-count 2 --memory-threshold 90
"""
    )
    parser.add_argument('--yarn-url', default='http://localhost:8088',
                       help='YARN ResourceManager URL (default: http://localhost:8088)')
    parser.add_argument('--memory-threshold', type=int, default=85,
                       help='Memory threshold %% (default: 85)')
    parser.add_argument('--vcpu-threshold', type=int, default=85,
                       help='vCPU threshold %% (default: 85)')
    parser.add_argument('--auto-kill', action='store_true',
                       help='Automatically kill top resource-consuming apps when threshold exceeded')
    parser.add_argument('--kill-count', type=int, default=1,
                       help='Number of top apps to kill (default: 1)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be killed without actually killing')
    parser.add_argument('--quiet', action='store_true',
                       help='Only output on alert')
    
    args = parser.parse_args()
    
    if args.dry_run:
        args.auto_kill = True  # dry-run implies auto-kill is enabled
    
    is_healthy, message = check_cluster_health(
        args.yarn_url,
        args.memory_threshold,
        args.vcpu_threshold,
        args.auto_kill,
        args.dry_run,
        args.kill_count
    )
    
    if not is_healthy:
        print(message)
        sys.exit(1)
    else:
        if not args.quiet:
            print(message)
        sys.exit(0)

if __name__ == '__main__':
    main()
