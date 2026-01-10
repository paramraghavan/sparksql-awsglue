import requests
import psutil
import socket

# Configuration
THRESHOLD_PERCENT = 90.0
# If running on the Master node, keep localhost. Otherwise, use the Master Private IP.
RM_PORT = "8088"
YARN_URL = f"http://localhost:{RM_PORT}/ws/v1/cluster"


def get_physical_metrics():
    """Checks the actual OS-level memory and CPU of the Master Node."""
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    return {
        "phys_mem_util": mem.percent,
        "phys_cpu_util": cpu,
        "phys_mem_available_gb": mem.available / (1024 ** 3)
    }


def get_yarn_metrics():
    """Checks YARN logical allocation and node health."""
    try:
        r = requests.get(f"{YARN_URL}/metrics", timeout=5)
        m = r.json()['clusterMetrics']

        # Calculate YARN logic utilization
        yarn_mem_util = (m['allocatedMB'] / m['totalMB']) * 100 if m['totalMB'] > 0 else 0

        return {
            "yarn_mem_util": yarn_mem_util,
            "unhealthy_nodes": m['unhealthyNodes'],
            "lost_nodes": m['lostNodes'],
            "pending_apps": m['appsPending']
        }
    except Exception as e:
        return {"error": f"YARN API Unreachable: {e}"}


def audit_cluster():
    hostname = socket.gethostname()
    phys = get_physical_metrics()
    yarn = get_yarn_metrics()

    print(f"--- Cluster Health Report ({hostname}) ---")
    print(f"Physical RAM Usage: {phys['phys_mem_util']}% ({phys['phys_mem_available_gb']:.2f} GB free)")
    print(f"Physical CPU Usage: {phys['phys_cpu_util']}%")

    if "error" in yarn:
        print(f"⚠️ CRITICAL: {yarn['error']}")
    else:
        print(f"YARN Allocated Mem: {yarn['yarn_mem_util']:.1f}%")
        print(f"Unhealthy/Lost Nodes: {yarn['unhealthy_nodes']} / {yarn['lost_nodes']}")
        print(f"Apps Pending: {yarn['pending_apps']}")

        # ALERT LOGIC
        if phys['phys_mem_util'] > THRESHOLD_PERCENT:
            print("🚨 ALERT: Master Node Physical RAM is nearly full! Risk of OOM crash.")

        if yarn['unhealthy_nodes'] > 0:
            print("🚨 ALERT: Workers are reporting unhealthy (likely full disks).")

        if yarn['yarn_mem_util'] > 98 and yarn['pending_apps'] > 0:
            print("⚠️ NOTICE: Cluster is at capacity. New jobs are queuing.")


if __name__ == "__main__":
    audit_cluster()
