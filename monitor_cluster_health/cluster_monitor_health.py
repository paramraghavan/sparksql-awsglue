"""
pip install --> pip install psutil requests schedule

This script monitors local system resources and YARN queue health. If thresholds are exceeded,
it identifies the heaviest process, sends an email, and optionally terminates the process.

System Monitoring:
Uses psutil to check the actual hardware load of the Master Node. This is crucial because if the Master Node's CPU or RAM is exhausted, the entire cluster becomes unresponsive.

YARN Integration:
It queries the Resource Manager REST API. It specifically looks for Pending Applications. A high number of pending apps usually indicates a resource deadlock or a queue bottleneck.

Process Identification:
If thresholds are hit, the script scans all running processes and sorts them by CPU usage to find the "offender."

The "Kill" Option:
If AUTO_KILL is set to True, it sends a SIGTERM signal to the identified PID.

File Locking (fcntl): * We use /tmp/cluster_monitor.lock.
The fcntl.LOCK_EX | fcntl.LOCK_NB flag ensures that if the lock is already held by another process, the new script instance fails to get the lock and exits gracefully.
The lock is automatically released by the Operating System if the script crashes or is stopped.

How the Cleanup Works
atexit.register(cleanup): This tells Python, "No matter how you close (unless it's a hard kill -9), run the cleanup function first."
fcntl.lockf(..., fcntl.LOCK_UN): This manually releases the kernel-level lock.
os.remove(LOCK_FILE): This deletes the file from /tmp/, leaving the system clean.

"""

import psutil
import requests
import smtplib
import schedule
import time
import os
import signal
import fcntl
import sys
import atexit
from email.message import EmailMessage

# --- CONFIGURATION ---
MASTER_IP = "127.0.0.1"  # Set the IP address of your master node
YARN_RM_PORT = "8088"
CPU_THRESHOLD = 90.0
MEM_THRESHOLD = 90.0
PENDING_THRESHOLD = 10
AUTO_KILL = False  # CAUTION: Set to True to terminate offending processes
LOCK_FILE = "/tmp/cluster_monitor.lock"

# Email Settings
EMAIL_SENDER = "monitor@example.com"
EMAIL_RECEIVER = "admin@example.com"
SMTP_SERVER = "localhost"

# Global variable to hold file handle so it doesn't get garbage collected
lock_file_handle = None


def cleanup():
    """Removes the lock file on script exit."""
    global lock_file_handle
    if lock_file_handle:
        try:
            # Release lock and close
            fcntl.lockf(lock_file_handle, fcntl.LOCK_UN)
            lock_file_handle.close()
            # Remove the physical file
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
                print(f"[{time.ctime()}] Lock file removed. Clean exit.")
        except Exception as e:
            print(f"Error during cleanup: {e}")


def ensure_single_instance():
    """Ensures only one instance of the script runs using a file lock."""
    global lock_file_handle
    # Open file in 'a+' to create if it doesn't exist without truncating
    lock_file_handle = open(LOCK_FILE, 'a+')
    try:
        # LOCK_EX: Exclusive lock, LOCK_NB: Non-blocking (fail if already locked)
        fcntl.lockf(lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Register the cleanup function to run when Python exits
        atexit.register(cleanup)
    except IOError:
        print(f"[{time.ctime()}] Another instance is already running. Exiting.")
        sys.exit(0)


def get_yarn_metrics():
    url = f"http://{MASTER_IP}:{YARN_RM_PORT}/ws/v1/cluster/metrics"
    try:
        response = requests.get(url, timeout=5)
        return response.json().get('clusterMetrics', {})
    except Exception as e:
        print(f"Error connecting to YARN RM: {e}")
        return None


def find_offending_process():
    """Identifies the process using the most resources."""
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            processes.append(proc.info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if not processes:
        return None
    # Prioritize CPU usage to find the offender
    return sorted(processes, key=lambda x: x['cpu_percent'], reverse=True)[0]


def send_alert(subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    try:
        with smtplib.SMTP(SMTP_SERVER) as s:
            s.send_message(msg)
    except Exception as e:
        print(f"Alert failed to send: {e}")


def monitor_job():
    print(f"[{time.ctime()}] Checking cluster health...")

    cpu_usage = psutil.cpu_percent(interval=1)
    mem_usage = psutil.virtual_memory().percent

    yarn_data = get_yarn_metrics()
    pending_apps = yarn_data.get('appsPending', 0) if yarn_data else 0

    issue_detected = False
    if cpu_usage > CPU_THRESHOLD or mem_usage > MEM_THRESHOLD or pending_apps > PENDING_THRESHOLD:
        issue_detected = True
        offender = find_offending_process()

        status_report = (
            f"ALERT: Potential Cluster Failure on {MASTER_IP}\n"
            f"System CPU: {cpu_usage}% | System Mem: {mem_usage}%\n"
            f"YARN Pending Apps: {pending_apps}\n"
        )

        if offender:
            status_report += f"\nOffending Process:\nName: {offender['name']}\nPID: {offender['pid']}\nCPU Usage: {offender['cpu_percent']}%"

        if AUTO_KILL and offender:
            try:
                os.kill(offender['pid'], signal.SIGTERM)
                status_report += f"\n\nACTION: Process {offender['pid']} was terminated."
            except Exception as e:
                status_report += f"\n\nACTION FAILED: Could not kill {offender['pid']}: {e}"

        send_alert("CRITICAL: Cluster Health Alert", status_report)
        print("Issue detected! Alert sent.")
    else:
        print("System status: OK")


# --- MAIN ---
if __name__ == "__main__":
    ensure_single_instance()

    N = 5  # Run every 5 minutes
    schedule.every(N).minutes.do(monitor_job)

    print(f"Monitor active for {MASTER_IP}. Frequency: {N} min.")

    # Run first check immediately
    monitor_job()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        # atexit.register(cleanup) will handle the lock file removal here
        sys.exit(0)
