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


# Create daily report
with max master cpu/meemory
max vcore, vmem , pending queue from yarn server
send daily  report at 5.00 am
accurate statistics for 4 weeks (28 days),
add low disk space
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
import argparse
import csv
from datetime import datetime
from email.message import EmailMessage

# --- CONFIGURATION ---
YARN_RM_PORT = "8088"
CPU_THRESHOLD = 90.0
MEM_THRESHOLD = 90.0
DISK_THRESHOLD = 85.0  # Alert if disk usage > 85%
PENDING_THRESHOLD = 10
AUTO_KILL = False
LOCK_FILE = "/tmp/cluster_monitor.lock"
STATS_FILE = "cluster_history.csv"

# Email Settings
EMAIL_SENDER = "monitor@yourdomain.com"
EMAIL_RECEIVER = "admin@yourdomain.com"
SMTP_SERVER = "localhost"

# --- GLOBAL TRACKING ---
# Added 'disk' to tracking
daily_stats = {"cpu": 0, "mem": 0, "disk": 0, "pending": 0, "yarn_mem": 0, "yarn_cpu": 0}
lock_file_handle = None


def ensure_single_instance():
    global lock_file_handle
    lock_file_handle = open(LOCK_FILE, 'a+')
    try:
        fcntl.lockf(lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        atexit.register(cleanup)
    except IOError:
        sys.exit(1)


def cleanup():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)


def init_stats_file():
    if not os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            # Added MaxDisk to header
            writer.writerow(["Date", "MaxCPU", "MaxMem", "MaxDisk", "MaxPending", "MaxYarnMem", "MaxYarnCPU"])


def save_daily_to_csv():
    today = datetime.now().strftime("%Y-%m-%d")
    with open(STATS_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            today, daily_stats["cpu"], daily_stats["mem"], daily_stats["disk"],
            daily_stats["pending"], daily_stats["yarn_mem"], daily_stats["yarn_cpu"]
        ])


def get_yarn_metrics(ip):
    try:
        r = requests.get(f"http://{ip}:{YARN_RM_PORT}/ws/v1/cluster/metrics", timeout=5)
        return r.json().get('clusterMetrics', {})
    except:
        return None


def find_offending_process():
    procs = []
    for p in psutil.process_iter(['pid', 'name', 'cpu_percent']):
        try:
            procs.append(p.info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return sorted(procs, key=lambda x: x['cpu_percent'], reverse=True)[0] if procs else None


def monitor_job(ip):
    global daily_stats
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent  # Monitoring Root Partition
    yarn = get_yarn_metrics(ip) or {}

    # Update Daily Peaks
    daily_stats["cpu"] = max(daily_stats["cpu"], cpu)
    daily_stats["mem"] = max(daily_stats["mem"], mem)
    daily_stats["disk"] = max(daily_stats["disk"], disk)
    daily_stats["pending"] = max(daily_stats["pending"], yarn.get('appsPending', 0))
    daily_stats["yarn_mem"] = max(daily_stats["yarn_mem"], yarn.get('allocatedMB', 0))
    daily_stats["yarn_cpu"] = max(daily_stats["yarn_cpu"], yarn.get('allocatedVirtualCores', 0))

    # Real-time Alerts
    alert_msg = ""
    if cpu > CPU_THRESHOLD: alert_msg += f"- CPU is high: {cpu}%\n"
    if mem > MEM_THRESHOLD: alert_msg += f"- Memory is high: {mem}%\n"
    if disk > DISK_THRESHOLD: alert_msg += f"- DISK SPACE LOW: {disk}% used on /\n"
    if (
    yarn.get('appsPending', 0)) > PENDING_THRESHOLD: alert_msg += f"- YARN Pending Apps: {yarn.get('appsPending')}\n"

    if alert_msg:
        offender = find_offending_process()
        full_report = f"CRITICAL SYSTEM ALERT ({ip})\n{alert_msg}"
        if offender:
            full_report += f"\nTop Process: {offender['name']} (PID: {offender['pid']})\n"
            if AUTO_KILL and cpu > CPU_THRESHOLD:
                os.kill(offender['pid'], signal.SIGTERM)
                full_report += "Action: High-CPU process terminated.\n"
        send_email(f"ALERT: Cluster Risk {ip}", full_report)


def send_reports(ip):
    save_daily_to_csv()

    # Read history
    with open(STATS_FILE, 'r') as f:
        history = list(csv.DictReader(f))

    # 1. Daily Report (5 AM)
    daily_msg = (
        f"Daily Peak Report - {ip}\n"
        f"CPU: {daily_stats['cpu']}% | Mem: {daily_stats['mem']}% | Disk: {daily_stats['disk']}%"
    )
    send_email(f"Daily Cluster Report: {ip}", daily_msg)

    # 2. 4-Week Summary (Check every 28 days)
    if len(history) >= 28:
        last_4_weeks = history[-28:]
        peak_disk = max(float(x['MaxDisk']) for x in last_4_weeks)
        avg_cpu = sum(float(x['MaxCPU']) for x in last_4_weeks) / 28

        four_week_msg = (
            f"--- 4-WEEK TREND SUMMARY ({ip}) ---\n"
            f"Period: {last_4_weeks[0]['Date']} to {last_4_weeks[-1]['Date']}\n"
            f"Average Daily Peak CPU: {avg_cpu:.2f}%\n"
            f"Absolute Peak Disk Usage: {peak_disk}%\n"
            f"Peak YARN Pending: {max(int(x['MaxPending']) for x in last_4_weeks)}\n"
        )
        send_email(f"4-WEEK SUMMARY: {ip}", four_week_msg)

        # Archive and reset CSV
        os.rename(STATS_FILE, f"archive_stats_{datetime.now().strftime('%Y%m%d')}.csv")
        init_stats_file()

    # Reset daily counters
    for k in daily_stats: daily_stats[k] = 0


def send_email(subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    try:
        with smtplib.SMTP(SMTP_SERVER) as s:
            s.send_message(msg)
    except Exception as e:
        print(f"Mail error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ip")
    args = parser.parse_args()

    ensure_single_instance()
    init_stats_file()

    schedule.every(5).minutes.do(monitor_job, ip=args.ip)
    schedule.every().day.at("05:00").do(send_reports, ip=args.ip)

    print(f"Monitor started for {args.ip}. Reports: Daily (5AM) & 4-Week Trend.")
    while True:
        schedule.run_pending()
        time.sleep(1)