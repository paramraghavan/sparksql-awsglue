To detect where a Spark job is stuck using the stages information from the Spark History Server API, you typically
analyze the following:

### Key Indicators for Stuck Stages

1. **Long-running stages**: Stages that have been running for a very long time without completion.
2. **Tasks not completing**: Stages with many tasks still pending or running but not progressing.
3. **Task failure or retries**: High number of failed tasks or retries in a stage.
4. **Skewed task durations**: Individual tasks taking abnormally long time compared to others, indicating data skew or
   resource issues.
5. **SQL plan stages stuck**: For SQL queries, stages corresponding to bottleneck steps like joins, shuffles, or
   aggregations.

### How to Implement This in Code

From the stage JSON objects you get from the API, useful fields include:

- `status`: RUNNING, PENDING, COMPLETED, FAILED
- `numTasks`: total tasks in stage
- `numCompletedTasks`: completed tasks
- `submissionTime` and `completionTime`: timestamps to calculate duration
- `tasks`: sometimes available via detailed stage endpoint for per-task info

Example heuristic logic:

import time
from datetime import datetime, timezone

def detect_stuck_stages(master_ip, app_id, stages):
stuck_stages = []
now = datetime.now(timezone.utc).timestamp() * 1000 # current epoch ms
LONG_RUNNING_THRESHOLD_MS = 60 * 60 * 1000 # 1 hour threshold
SKEWED_TASK_THRESHOLD_RATIO = 5.0 # task duration is 5x median considered skewed
SLOW_TASK_PERCENT_THRESHOLD = 0.3 # 30% tasks slower than median triggers

    def fetch_stage_tasks(stage_id):
        url = f"http://{master_ip}:18080/api/v1/applications/{app_id}/stages/{stage_id}/tasks"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error fetching tasks for stage {stage_id}: {e}")
            return []

    for stage in stages:
        status = stage.get("status")
        num_tasks = stage.get("numTasks", 0)
        num_completed = stage.get("numCompletedTasks", 0)
        submission_time_str = stage.get("submissionTime")
        completion_time_str = stage.get("completionTime")

        # Parse submission time
        submission_time = None
        if submission_time_str:
            try:
                submission_time = datetime.fromisoformat(submission_time_str.replace('Z', '+00:00')).timestamp() * 1000
            except Exception:
                submission_time = None

        # Compute duration
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
                duration_ms = now - submission_time

        reason_msgs = []

        # Basic long-running check with few completed tasks
        if status in ("RUNNING", "PENDING") and duration_ms and duration_ms > LONG_RUNNING_THRESHOLD_MS:
            if num_completed == 0 or num_completed < num_tasks * 0.1:
                reason_msgs.append(f"Stage running >1h with only {num_completed}/{num_tasks} tasks completed")

        # Task duration skew check
        if status == "RUNNING" and num_tasks > 0:
            tasks = fetch_stage_tasks(stage.get("stageId"))
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
                        reason_msgs.append(f"{len(slow_tasks)} tasks are >{SKEWED_TASK_THRESHOLD_RATIO}x median duration (data skew)")

        if reason_msgs:
            stuck_stages.append({
                "stageId": stage.get("stageId"),
                "status": status,
                "duration_ms": duration_ms,
                "numTasks": num_tasks,
                "numCompletedTasks": num_completed,
                "reasons": reason_msgs
            })

    return stuck_stages

### Integration

- Call this function after fetching stages from the Spark History Server API.
- This inspects stages for long-running states with few completed tasks and also detects skew based on task executor run
  times.
- The returned list includes stuck stages with detailed reasons to highlight where jobs are stuck in terms of code
  stages or skewed task execution.
- You can tune thresholds and add more advanced checks like task-level duration skew by querying stage task endpoints.

This approach points you to stages in the job where progress is stalled, highlighting possible code bottlenecks (e.g.,
heavy shuffles, unbalanced data) to investigate further.

### How to add SQL query detection for stuck stages:

1. **Fetch SQL execution details** for each stage via Spark History Server API:
    - Use `/api/v1/applications/{appId}/stages/{stageId}` endpoint, which often includes the SQL description or SQL plan
      info in the `details` or `description` fields.
    - Look for certain keywords or structure indicating expensive operations, such as large shuffles, joins, or
      aggregations.

2. **Detect stuck stages caused by SQL operations** if:
    - The stage includes heavy shuffle operations.
    - The SQL query plan shows expensive operations that align with slow/stalled stages.
    - Stages corresponding to complex SQL operations have long durations or high skew observed by previous heuristic.

3. **Example heuristic additions for SQL detection**:

```python
def fetch_stage_sql_info(master_ip, app_id, stage_id):
    url = f"http://{master_ip}:18080/api/v1/applications/{app_id}/stages/{stage_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        stage_info = resp.json()
        # SQL query text is usually in description or details
        sql_text = stage_info.get('description', '') or stage_info.get('details', '')
        return sql_text
    except Exception as e:
        print(f"Error fetching SQL info for stage {stage_id}: {e}")
        return ""


def detect_stuck_stages_with_sql(master_ip, app_id, stages):
    stuck_stages = []
    # previous detection omitted here for brevity, add combined logic

    for stage in stages:
        # previous checks ...

        # Fetch SQL query text
        sql_text = fetch_stage_sql_info(master_ip, app_id, stage.get('stageId'))
        # Simple check for costly operations keywords
        costly_ops = ["shuffle", "join", "aggregate", "group by", "window", "sort"]
        if any(op in sql_text.lower() for op in costly_ops):
            # If stage is long-running and involves expensive SQL ops consider stuck
            # Combine with previous duration/skew checks as necessary
            if stage.get('status') in ("RUNNING", "PENDING"):
                # Example: if stage running long, flag it with SQL info
                stuck_stages.append({
                    "stageId": stage.get("stageId"),
                    "status": stage.get("status"),
                    "sql_text": sql_text,
                    "reason": "Contains expensive SQL operations likely causing bottleneck"
                })

    return stuck_stages
```

## resource utilization adivsiory
- Executor runtime utilization vs. task parallelism
- Number of active vs. failed tasks
- Stage and job durations vs. number of tasks
- Signs of over-provisioning (many idle executors) or under-provisioning (many slow tasks, long queues)
  
### How to add simple resource advisory logic:

1. Use task and executor metrics from Spark History Server REST API:
   - Total tasks, completed tasks, failed tasks per stage
   - Task run times, executor run times
   - Possibly number of executors (if available from API or logs)

2. Define heuristics, e.g.:
   - If most tasks complete very quickly and many executors remain idle → over-provisioned resources
   - If tasks have large skew or long runtimes relative to total resources → under-provisioned or imbalanced
   - If many task failures or retries → possibly inefficient resource usage or configuration issues

### Example  returning advisory notes per job:

```python
def analyze_resource_efficiency(stages):
    notes = []
    total_tasks = 0
    total_fast_tasks = 0
    max_executor_run = 0
    all_task_durations = []

    for stage in stages:
        tasks = stage.get("tasks", [])
        total_tasks += len(tasks)
        for task in tasks:
            metrics = task.get("taskMetrics")
            if metrics and metrics.get("executorRunTime") is not None:
                dur = metrics.get("executorRunTime")
                all_task_durations.append(dur)
                if dur < 1000:  # tasks under 1 second
                    total_fast_tasks += 1
                if dur > max_executor_run:
                    max_executor_run = dur

    if total_tasks == 0:
        return notes

    fast_task_ratio = total_fast_tasks / total_tasks
    avg_duration = sum(all_task_durations) / len(all_task_durations) if all_task_durations else 0

    if fast_task_ratio > 0.7:
        notes.append("High ratio of very fast tasks suggests possible over-provisioned executors (idle/wasted resources).")

    if avg_duration > 60000:  # avg task > 1 min
        notes.append("Average task duration is high, consider increasing cluster resources or optimizing job.")

    # Add more heuristics as needed...

    return notes
```
