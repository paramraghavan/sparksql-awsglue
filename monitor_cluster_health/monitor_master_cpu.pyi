import psutil
import time

# Configuration
CPU_THRESHOLD = 90.0  # Percentage
CPU_CHECK_INTERVAL = 2  # Seconds to average CPU usage


def monitor_master_cpu():
    # interval=1 provides a much more accurate reading than a snapshot
    cpu_usage = psutil.cpu_percent(interval=CPU_CHECK_INTERVAL)

    # Check Load Average (Unix specific)
    # This tells you how many processes are waiting for CPU.
    # If this is much higher than the number of cores, the cluster is 'lagging'.
    load1, load5, load15 = psutil.getloadavg()
    num_cores = psutil.cpu_count()

    print(f"Current CPU Usage: {cpu_usage}%")
    print(f"Load Average (1m): {load1} (Cores: {num_cores})")

    if cpu_usage > CPU_THRESHOLD:
        print("🚨 ALERT: Master CPU is saturated! Risk of lost heartbeats and cluster termination.")

    if load1 > (num_cores * 1.5):
        print("⚠️ WARNING: CPU Task Queue is overloaded. Services will be unresponsive.")


if __name__ == "__main__":
    monitor_master_cpu()