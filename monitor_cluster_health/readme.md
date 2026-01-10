# monitor clsuer health

To monitor an EMR cluster's health and prevent failures, focusing on the **YARN Resource Manager API** is more effective
than the Spark History Server. The History Server tells you about past jobs, but the Resource Manager tells you how
much "room" is left in the cluster right now.

### Why you see 100% memory but no failure

In YARN, **Memory Usage** usually refers to **Reserved/Allocated Memory**, not necessarily **Physical RAM** being used
by the OS.

* If YARN says 100% memory usage, it means all "slots" are taken by containers.
* New jobs will simply wait in a `PENDING` state rather than crashing the cluster.
* A "Cluster Failure" usually occurs when the **Master Node's** physical memory hits 100% (causing an OOM) or if local
  disks fill up.

---

### Python Monitoring Script

This script queries the YARN Resource Manager API to calculate the percentage of VCPUs and Memory currently allocated.

```python
import requests
import sys

# Configuration
# Replace with your Master Node DNS or IP
YARN_RM_URL = "http://<EMR_MASTER_IP>:8088/ws/v1/cluster/metrics"
THRESHOLD = 90.0  # Alert threshold in percentage


def check_cluster_health():
    try:
        response = requests.get(YARN_RM_URL, timeout=10)
        response.raise_for_status()
        metrics = response.json()['clusterMetrics']

        # Memory Calculation
        total_mem = metrics['totalMB']
        allocated_mem = metrics['allocatedMB']
        mem_utilization = (allocated_mem / total_mem) * 100 if total_mem > 0 else 0

        # VCPU Calculation
        total_vcores = metrics['totalVirtualCores']
        allocated_vcores = metrics['allocatedVirtualCores']
        vcpu_utilization = (allocated_vcores / total_vcores) * 100 if total_vcores > 0 else 0

        # Pending Apps (Crucial for detecting "stuck" clusters)
        pending_apps = metrics.get('appsPending', 0)

        print(f"Memory Utilization: {mem_utilization:.2f}% ({allocated_mem}/{total_mem} MB)")
        print(f"vCPU Utilization: {vcpu_utilization:.2f}% ({allocated_vcores}/{total_vcores})")
        print(f"Pending Applications: {pending_apps}")

        # Alert Logic
        if mem_utilization > THRESHOLD or vcpu_utilization > THRESHOLD:
            print("⚠️ ALERT: Cluster is nearing maximum capacity!")
            # Add logic here to send an SNS notification or Slack webhook

        if pending_apps > 5:
            print("⚠️ WARNING: Multiple applications are pending. Resources are exhausted.")

    except Exception as e:
        print(f"Error connecting to YARN: {e}")
        sys.exit(1)


if __name__ == "__main__":
    check_cluster_health()

```

---

### Key Metrics for Detecting Cluster Failure

To detect an actual crash rather than just a full cluster, look for these specific indicators in the YARN API response:

| Metric           | Why it matters                                                                                                                      |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `unhealthyNodes` | If this is > 0, nodes are failing (usually due to full disks or high hardware load).                                                |
| `lostNodes`      | If nodes drop off, your capacity shrinks, which often leads to a chain reaction of failures.                                        |
| `appsPending`    | If this number is growing while CPU is low, your memory is likely fragmented or exhausted, preventing new containers from starting. |

### How to use this effectively

1. **Deployment:** Run this script from a cron job on an external instance or a "bastion" host that has network access
   to the EMR Master node on port 8088.
2. **OS Level Monitoring:** Since YARN only tracks *allocated* resources, you should also monitor the **Master Node's**
   physical health. If the Master node's CPU/Memory hits 100%, the `ResourceManager` process will hang, and the cluster
   will become unresponsive.
3. **CloudWatch Alternative:** For AWS EMR, the most robust way to do this is via CloudWatch Alarms on the
   `MemoryAvailableMB` and `RunningAndPendingJobs` metrics.
