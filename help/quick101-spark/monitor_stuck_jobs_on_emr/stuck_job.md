# Detect stuck jobs

Simple and effective Python Flask-based monitoring tool for your EMR Spark jobs that can list running jobs, check their
status, and detect where they are stuck (stage, executor, SQL), the approach involves:
Using the Yarn ResourceManager REST API on the EMR master node (port 8088) to list running applications/jobs and their
statuses. Using the Spark History Server REST API on the EMR master node (if available) to fetch detailed Spark job, stage, and
task information. Analyzing stages, executors, and SQL queries from the Spark job details and logs to detect where a job is stuck.

- /api/v1/applications – list all applications (completed and running)
- /api/v1/applications/{appId}/stages – list all stages for an application
- /api/v1/applications/{appId}/stages/{stageId} – details of a single stage including tasks info

```python
from flask import Flask, jsonify, render_template_string, request
import requests

app = Flask(__name__)

# Config - list of EMR master nodes IPs
EMR_MASTERS = [
    "master-ip-1",
    "master-ip-2"
]

YARN_RUNNING_APPS_PATH = "/ws/v1/cluster/apps?state=RUNNING"

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>EMR Cluster Job Monitor</title>
<style>
  body { font-family: Arial, sans-serif; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
  th { background-color: #f2f2f2; }
  tr:hover { background-color: #ddd; cursor: pointer; }
</style>
<script>
async function fetchJobDetails(masterIp, appId) {
  const response = await fetch(`/jobdetails?master=${masterIp}&appid=${appId}`);
  const data = await response.json();
  if(data.error){
    document.getElementById('job-details').innerHTML = `<p>${data.error}</p>`;
    return;
  }
  let details = `<h2>Job ${appId} Details</h2>`;
  details += `<p><b>Name:</b> ${data.name}</p>`;
  details += `<p><b>User:</b> ${data.user}</p>`;
  details += `<p><b>State:</b> ${data.state}</p>`;
  details += `<p><b>Start Time:</b> ${new Date(data.startTime).toLocaleString()}</p>`;
  details += `<p><b>Elapsed Time:</b> ${Math.floor((Date.now() - data.startTime)/60000)} mins</p>`;
  details += `<h3>Stages</h3><ul>`;
  data.stages.forEach(stage => {
    details += `<li>Stage ${stage.stageId}: ${stage.status}, tasks: ${stage.numTasks}, completed: ${stage.numCompletedTasks}</li>`;
  });
  details += `</ul>`;
  document.getElementById('job-details').innerHTML = details;
}
</script>
</head>
<body>
<h1>EMR Spark Running Jobs Monitor</h1>
<table>
  <thead>
    <tr>
      <th>Master IP</th>
      <th>Application ID</th>
      <th>Name</th>
      <th>User</th>
      <th>State</th>
      <th>Start Time</th>
    </tr>
  </thead>
  <tbody>
    {% for job in jobs %}
    <tr onclick="fetchJobDetails('{{ job.master }}', '{{ job.appId }}')">
      <td>{{ job.master }}</td>
      <td>{{ job.appId }}</td>
      <td>{{ job.name }}</td>
      <td>{{ job.user }}</td>
      <td>{{ job.state }}</td>
      <td>{{ job.startTime }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
<div id="job-details" style="margin-top:20px;"></div>
</body>
</html>
"""


def get_running_apps(master_ip):
    url = f"http://{master_ip}:8088" + YARN_RUNNING_APPS_PATH
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        apps = data.get('apps', {}).get('app', [])
        # Convert start time from epoch millis to readable timestamp for display
        for app in apps:
            if 'startedTime' in app:
                try:
                    app['startedTime'] = int(app['startedTime'])
                except:
                    app['startedTime'] = 0
        return apps if apps else []
    except Exception as e:
        print(f"Error querying {url}: {e}")
        return []


def get_spark_stages(master_ip, app_id):
    url = f"http://{master_ip}:18080/api/v1/applications/{app_id}/stages"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        stages = resp.json()
        #detect_stuck_stages(stages)
        return stages
    except Exception as e:
        print(f"Error querying Spark stages at {url}: {e}")
        return []


def get_job_details(master_ip, app_id):
    apps = get_running_apps(master_ip)
    for app in apps:
        if app['id'] == app_id:
            stages = get_spark_stages(master_ip, app_id)
            # Simplify stage info for UI display
            stage_info = []
            for stage in stages:
                stage_info.append({
                    "stageId": stage.get("stageId"),
                    "status": stage.get("status"),
                    "numTasks": stage.get("numTasks"),
                    "numCompletedTasks": stage.get("numCompleteTasks", 0)
                })
            return {
                "appId": app_id,
                "name": app.get('name', 'Unknown'),
                "user": app.get('user', 'Unknown'),
                "state": app.get('state', 'UNKNOWN'),
                "startTime": app.get('startedTime'),
                "stages": stage_info
            }
    return {"error": "Job not found"}


@app.route('/')
def index():
    jobs = []
    for master in EMR_MASTERS:
        apps = get_running_apps(master)
        for app in apps:
            jobs.append({
                "master": master,
                "appId": app['id'],
                "name": app.get('name', 'Unknown'),
                "user": app.get('user', 'Unknown'),
                "state": app.get('state', 'UNKNOWN'),
                "startTime": app.get('startedTime')
            })
    return render_template_string(HTML_TEMPLATE, jobs=jobs)


@app.route('/jobdetails')
def jobdetails():
    master = request.args.get('master')
    app_id = request.args.get('appid')
    details = get_job_details(master, app_id)
    return jsonify(details)

import time
from datetime import datetime, timezone

def detect_stuck_stages(stages):
    stuck_stages = []
    now = datetime.now(timezone.utc).timestamp() * 1000  # current epoch ms
    LONG_RUNNING_THRESHOLD_MS = 60 * 60 * 1000  # 1 hour, configurable

    for stage in stages:
        status = stage.get("status")
        num_tasks = stage.get("numTasks", 0)
        num_completed = stage.get("numCompletedTasks", 0)
        submission_time_str = stage.get("submissionTime")
        completion_time_str = stage.get("completionTime")

        # Convert submissionTime to epoch ms if possible
        if submission_time_str:
            try:
                submission_time = datetime.fromisoformat(submission_time_str.replace('Z', '+00:00')).timestamp() * 1000
            except Exception:
                submission_time = None
        else:
            submission_time = None

        duration_ms = None
        if submission_time:
            if completion_time_str:
                try:
                    completion_time = datetime.fromisoformat(completion_time_str.replace('Z', '+00:00')).timestamp() * 1000
                except Exception:
                    completion_time = None
                if completion_time:
                    duration_ms = completion_time - submission_time
            else:
                # Still running - use current time
                duration_ms = now - submission_time

        # Detect if stage is stuck based on heuristic:
        # - RUNNING or PENDING for very long time
        # - Few or no completed tasks appearing stalled
        if status in ("RUNNING", "PENDING"):
            if duration_ms and duration_ms > LONG_RUNNING_THRESHOLD_MS:
                # If very few completed tasks or none after long time, suspect stuck
                if num_completed == 0 or num_completed < num_tasks * 0.1:
                    stuck_stages.append({
                        "stageId": stage.get("stageId"),
                        "reason": f"Stage running for >1h with only {num_completed}/{num_tasks} tasks completed",
                        "status": status,
                        "duration_ms": duration_ms,
                        "numTasks": num_tasks,
                        "numCompletedTasks": num_completed
                    })

    return stuck_stages



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
```

### Usage:

- Replace `"master-ip-1"` etc. with your real EMR master node IP addresses.
- Ensure Spark History Server is running and reachable on port 18080 on those masters.
- Install Flask and Requests (`pip install flask requests`).
- Run this script, then browse to `http://<your-flask-server>:5000` to see running jobs and drill into their Spark
  stages.
