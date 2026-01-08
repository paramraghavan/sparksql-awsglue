#!/usr/bin/env python3
"""
EMR Cluster Health Monitor - Alert on high resource usage
Exit code 1 if threshold exceeded, 0 if healthy
"""
import requests
import sys
import argparse

def check_cluster_health(yarn_url, memory_threshold=85, vcpu_threshold=85):
    """
    Check if cluster is approaching capacity limits
    Returns: (is_healthy, alert_message)
    """
    try:
        response = requests.get(f"{yarn_url}/ws/v1/cluster/metrics", timeout=10)
        metrics = response.json()['clusterMetrics']
        
        # Calculate utilization percentages
        total_memory = metrics['totalMB']
        allocated_memory = metrics['allocatedMB']
        memory_util = (allocated_memory / total_memory * 100) if total_memory > 0 else 0
        
        total_vcpus = metrics['totalVirtualCores']
        allocated_vcpus = metrics['allocatedVirtualCores']
        vcpu_util = (allocated_vcpus / total_vcpus * 100) if total_vcpus > 0 else 0
        
        alerts = []
        
        # Check memory threshold
        if memory_util >= memory_threshold:
            alerts.append(f"MEMORY CRITICAL: {memory_util:.1f}% used ({allocated_memory/1024:.1f}/{total_memory/1024:.1f} GB)")
        
        # Check vCPU threshold
        if vcpu_util >= vcpu_threshold:
            alerts.append(f"VCPU CRITICAL: {vcpu_util:.1f}% used ({allocated_vcpus}/{total_vcpus} cores)")
        
        # Check for unhealthy nodes
        unhealthy_nodes = metrics.get('unhealthyNodes', 0)
        if unhealthy_nodes > 0:
            alerts.append(f"UNHEALTHY NODES: {unhealthy_nodes} node(s) unhealthy")
        
        # Check pending apps (queue backup)
        pending_apps = metrics.get('appsPending', 0)
        if pending_apps > 10:
            alerts.append(f"QUEUE BACKUP: {pending_apps} applications pending")
        
        if alerts:
            alert_message = f"""
EMR CLUSTER ALERT - {yarn_url}
{'='*60}
CRITICAL THRESHOLDS EXCEEDED:

{chr(10).join('  - ' + alert for alert in alerts)}

CURRENT STATUS:
  Memory: {memory_util:.1f}% ({allocated_memory/1024:.1f}/{total_memory/1024:.1f} GB)
  vCPUs:  {vcpu_util:.1f}% ({allocated_vcpus}/{total_vcpus} cores)
  Active Nodes: {metrics.get('activeNodes', 0)}
  Unhealthy Nodes: {unhealthy_nodes}
  Running Apps: {metrics.get('appsRunning', 0)}
  Pending Apps: {pending_apps}

ACTION REQUIRED: Review cluster capacity or scale up!
{'='*60}
"""
            return False, alert_message
        else:
            return True, f"Cluster healthy - Memory: {memory_util:.1f}%, vCPUs: {vcpu_util:.1f}%"
            
    except Exception as e:
        return False, f"ERROR: Unable to check cluster health - {e}"

def main():
    parser = argparse.ArgumentParser(description='Monitor EMR cluster health')
    parser.add_argument('--yarn-url', default='http://localhost:8088',
                       help='YARN ResourceManager URL')
    parser.add_argument('--memory-threshold', type=int, default=85,
                       help='Memory utilization threshold %% (default: 85)')
    parser.add_argument('--vcpu-threshold', type=int, default=85,
                       help='vCPU utilization threshold %% (default: 85)')
    parser.add_argument('--quiet', action='store_true',
                       help='Only output on alert')
    
    args = parser.parse_args()
    
    is_healthy, message = check_cluster_health(
        args.yarn_url,
        args.memory_threshold,
        args.vcpu_threshold
    )
    
    if not is_healthy:
        print(message)
        sys.exit(1)  # Alert condition
    else:
        if not args.quiet:
            print(message)
        sys.exit(0)  # Healthy

if __name__ == '__main__':
    main()
