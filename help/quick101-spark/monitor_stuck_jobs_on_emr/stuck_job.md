# Detect stuck jobs

Simple and effective Python Flask-based monitoring tool for your EMR Spark jobs that can list running jobs, check their
status, and detect where they are stuck (stage, executor, SQL), the approach involves:
Using the Yarn ResourceManager REST API on the EMR master node (port 8088) to list running applications/jobs and their
statuses. Using the Spark History Server REST API on the EMR master node (if available) to fetch detailed Spark job, stage, and
task information. Analyzing stages, executors, and SQL queries from the Spark job details and logs to detect where a job is stuck.

- /api/v1/applications – list all applications (completed and running)
- /api/v1/applications/{appId}/stages – list all stages for an application
- /api/v1/applications/{appId}/stages/{stageId} – details of a single stage including tasks info
- [detect_stuck_job.py](detect_stuck_job.py)

### Usage:

- Replace `"master-ip-1"` etc. with your real EMR master node IP addresses.
- Ensure Spark History Server is running and reachable on port 18080 on those masters.
- Install Flask and Requests (`pip install flask requests`).
- Run this script, then browse to `http://<your-flask-server>:5000` to see running jobs and drill into their Spark
  stages.
