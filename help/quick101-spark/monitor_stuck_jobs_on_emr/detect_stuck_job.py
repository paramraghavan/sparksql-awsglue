from flask import Flask, jsonify, render_template_string, request
import requests
from datetime import datetime, timezone

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
  .stuck { color: red; font-weight: bold; }
  .advisory { color: blue; font-style: italic; margin-top: 10px; }
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
    let stuckInfo = stage.reasons ? `<br/><span class="stuck">Stuck: ${stage.reasons.join("; ")}</span>` : "";
    let sqlInfo = stage.sql_text ? `<br/><b>SQL:</b> <pre>${stage.sql_text}</pre>` : "";
    details += `<li>Stage ${stage.stageId}: ${stage.status}, tasks: ${stage.numTasks}, completed: ${stage.numCompletedTasks}${stuckInfo}${sqlInfo}</li>`;
  });
  details += `</ul>`;
  if (data.resource_advisory && data.resource_advisory.length > 0) {
    details += `<div class="advisory"><b>Resource Advisory:</b><ul>`;
    data.resource_advisory.forEach(note => {
      details += `<li>${note}</li>`;
    });
    details += `</ul></div>`;
  }
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
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        stages = resp.json()
        return stages
    except Exception as e:
        print(f"Error querying Spark stages at {url}: {e}")
        return []


def fetch_stage_tasks(master_ip, app_id, stage_id):
    url = f"http://{master_ip}:18080/api/v1/applications/{app_id}/stages/{stage_id}/tasks"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching tasks for stage {stage_id}: {e}")
        return []


def fetch_stage_sql_info(master_ip, app_id, stage_id):
    url = f"http://{master_ip}:18080/api/v1/applications/{app_id}/stages/{stage_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        stage_info = resp.json()
        sql_text = stage_info.get('description', '') or stage_info.get('details', '') or ""
        return sql_text
    except Exception as e:
        print(f"Error fetching SQL info for stage {stage_id}: {e}")
        return ""


def detect_stuck_stages(master_ip, app_id, stages):
    stuck_stages = []
    now = datetime.now(timezone.utc).timestamp() * 1000
    LONG_RUNNING_THRESHOLD_MS = 60 * 60 * 1000
    SKEWED_TASK_THRESHOLD_RATIO = 5.0
    SLOW_TASK_PERCENT_THRESHOLD = 0.3
    COSTLY_OPS_KEYWORDS = ["shuffle", "join", "aggregate", "group by", "window", "sort"]

    for stage in stages:
        status = stage.get("status")
        num_tasks = stage.get("numTasks", 0)
        num_completed = stage.get("numCompletedTasks", 0)
        submission_time_str = stage.get("submissionTime")
        completion_time_str = stage.get("completionTime")

        submission_time = None
        if submission_time_str:
            try:
                submission_time = datetime.fromisoformat(submission_time_str.replace('Z', '+00:00')).timestamp() * 1000
            except Exception:
                submission_time = None

        duration_ms = None
        if submission_time:
            if completion_time_str:
                try:
                    completion_time = datetime.fromisoformat(
                        completion_time_str.replace('Z', '+00:00')).timestamp() * 1000
                except Exception:
                    completion_time = None
                if completion_time:
                    duration_ms = completion_time - submission_time
            else:
                duration_ms = now - submission_time

        reason_msgs = []

        # Basic long-running with few tasks completed
        if status in ("RUNNING", "PENDING") and duration_ms and duration_ms > LONG_RUNNING_THRESHOLD_MS:
            if num_completed == 0 or num_completed < num_tasks * 0.1:
                reason_msgs.append(f"Stage running >1h with only {num_completed}/{num_tasks} tasks completed")

        # Task duration skew analysis
        if status == "RUNNING" and num_tasks > 0:
            tasks = fetch_stage_tasks(master_ip, app_id, stage.get("stageId"))
            if tasks:
                durations = []
                for task in tasks:
                    metrics = task.get("taskMetrics")
                    if metrics and metrics.get("executorRunTime") is not None:
                        durations.append(metrics.get("executorRunTime"))
                if durations:
                    median_duration = sorted(durations)[len(durations) // 2]
                    slow_tasks = [d for d in durations if d > median_duration * SKEWED_TASK_THRESHOLD_RATIO]
                    if len(slow_tasks) / len(durations) > SLOW_TASK_PERCENT_THRESHOLD:
                        reason_msgs.append(
                            f"{len(slow_tasks)} tasks >{SKEWED_TASK_THRESHOLD_RATIO}x median duration (data skew)")

        # SQL detection
        sql_text = fetch_stage_sql_info(master_ip, app_id, stage.get("stageId"))
        if sql_text and any(op in sql_text.lower() for op in COSTLY_OPS_KEYWORDS):
            if status in ("RUNNING", "PENDING"):
                reason_msgs.append("Contains expensive SQL operations likely causing bottleneck")

        stuck_stages.append({
            "stageId": stage.get("stageId"),
            "status": status,
            "numTasks": num_tasks,
            "numCompletedTasks": num_completed,
            "reasons": reason_msgs if reason_msgs else None,
            "sql_text": sql_text if sql_text else None
        })

    return stuck_stages


def analyze_resource_efficiency(stages):
    notes = []
    total_tasks = 0
    total_fast_tasks = 0
    all_task_durations = []

    for stage in stages:
        # Need to fetch tasks for this stage to analyze durations
        tasks = stage.get("tasks", [])
        # If tasks not included, assume empty, to avoid extra fetches here
        total_tasks += len(tasks)
        for task in tasks:
            metrics = task.get("taskMetrics")
            if metrics and metrics.get("executorRunTime") is not None:
                dur = metrics.get("executorRunTime")
                all_task_durations.append(dur)
                if dur < 1000:  # tasks under 1 second
                    total_fast_tasks += 1

    if total_tasks == 0:
        return []

    fast_task_ratio = total_fast_tasks / total_tasks
    avg_duration = sum(all_task_durations) / len(all_task_durations) if all_task_durations else 0

    if fast_task_ratio > 0.7:
        notes.append(
            "High ratio of very fast tasks suggests possible over-provisioned executors (idle/wasted resources).")

    if avg_duration > 60000:
        notes.append("Average task duration is high, consider increasing cluster resources or optimizing job.")

    return notes


def get_job_details(master_ip, app_id):
    apps = get_running_apps(master_ip)
    for app in apps:
        if app['id'] == app_id:
            stages = get_spark_stages(master_ip, app_id)
            # Fetch detailed tasks for each stage to pass to resource analysis
            for stage in stages:
                tasks = fetch_stage_tasks(master_ip, app_id, stage["stageId"])
                stage["tasks"] = tasks

            stage_info = detect_stuck_stages(master_ip, app_id, stages)
            resource_advisory = analyze_resource_efficiency(stages)

            return {
                "appId": app_id,
                "name": app.get('name', 'Unknown'),
                "user": app.get('user', 'Unknown'),
                "state": app.get('state', 'UNKNOWN'),
                "startTime": app.get('startedTime'),
                "stages": stage_info,
                "resource_advisory": resource_advisory
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
