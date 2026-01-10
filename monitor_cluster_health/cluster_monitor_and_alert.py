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

"""

import psutil
import requests
import smtplib
import schedule
import time
import os
import signal
from email.message import EmailMessage

# --- CONFIGURATION ---
MASTER_IP = "127.0.0.1"  # Or the specific Cluster IP
YARN_RM_PORT = "8088"
CPU_THRESHOLD = 90.0  # Percent
MEM_THRESHOLD = 90.0  # Percent
PENDING_THRESHOLD = 10  # Number of pending apps
AUTO_KILL = False  # Set to True to enable killing offending processes

# Email Settings
EMAIL_SENDER = "monitor@example.com"
EMAIL_RECEIVER = "admin@example.com"
SMTP_SERVER = "localhost"


def get_yarn_metrics():
    """Fetch metrics from YARN Resource Manager."""
    url = f"http://{MASTER_IP}:{YARN_RM_PORT}/ws/v1/cluster/metrics"
    try:
        response = requests.get(url, timeout=5)
        return response.json().get('clusterMetrics', {})
    except Exception as e:
        print(f"Error connecting to YARN: {e}")
        return None


def find_offending_process():
    """Identify the process using the most CPU."""
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        processes.append(proc.info)

    # Sort by CPU usage descending
    top_proc = sorted(processes, key=lambda x: x['cpu_percent'], reverse=True)[0]
    return top_proc


def send_alert(subject, body):
    """Send an email alert."""
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    try:
        with smtplib.SMTP(SMTP_SERVER) as s:
            s.send_message(msg)
        print("Alert email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


def monitor_job():
    print(f"[{time.ctime()}] Running health check...")

    # 1. System Metrics
    cpu_usage = psutil.cpu_percent(interval=1)
    mem_usage = psutil.virtual_memory().percent

    # 2. YARN Metrics
    yarn_data = get_yarn_metrics()
    pending_apps = yarn_data.get('appsPending', 0) if yarn_data else 0

    issue_detected = False
    alert_msg = ""

    if cpu_usage > CPU_THRESHOLD or mem_usage > MEM_THRESHOLD or pending_apps > PENDING_THRESHOLD:
        issue_detected = True
        offender = find_offending_process()

        alert_msg = (
            f"CRITICAL: Cluster Health Issue on {MASTER_IP}\n"
            f"CPU: {cpu_usage}% | Mem: {mem_usage}%\n"
            f"YARN Pending Apps: {pending_apps}\n\n"
            f"Offending Process: {offender['name']} (PID: {offender['pid']})\n"
            f"Process CPU: {offender['cpu_percent']}%\n"
        )

        if AUTO_KILL:
            try:
                os.kill(offender['pid'], signal.SIGTERM)
                alert_msg += f"ACTION TAKEN: Process {offender['pid']} has been terminated."
            except Exception as e:
                alert_msg += f"ACTION FAILED: Could not kill process {offender['pid']}: {e}"

    if issue_detected:
        print(alert_msg)
        send_alert("Cluster Health Alert", alert_msg)
    else:
        print("Cluster is healthy.")


# --- SCHEDULER ---
N = 5  # Run every 5 minutes
schedule.every(N).minutes.do(monitor_job)

if __name__ == "__main__":
    print(f"Starting monitor on {MASTER_IP} every {N} minutes...")
    # Run once immediately on start
    monitor_job()
    while True:
        schedule.run_pending()
        time.sleep(1)
