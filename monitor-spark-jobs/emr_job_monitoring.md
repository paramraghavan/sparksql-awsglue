# How to use Sparkand uarn history server api exposed by emr
How YARN and Spark History Server work together in EMR, and how this monitoring code leverages both
services to collect comprehensive job data.

## EMR Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    EMR Cluster                              │
├─────────────────────────────────────────────────────────────┤
│  Master Node                                                │
│  ├── YARN Resource Manager (Port 8088) ─ Real-time data    │
│  ├── Spark History Server (Port 18080) ─ Historical data   │
│  └── Hadoop NameNode, Spark Master, etc.                   │
├─────────────────────────────────────────────────────────────┤
│  Core/Task Nodes                                            │
│  ├── YARN Node Managers                                     │
│  ├── Spark Executors                                        │
│  └── HDFS Data Nodes                                        │
└─────────────────────────────────────────────────────────────┘
```

## 1. YARN Resource Manager - The Cluster Orchestrator

### What YARN Does:

```text
# YARN manages ALL cluster resources
- CPU cores (vCores)
- Memory (RAM)
- Containers (execution units)
- Job scheduling and queuing
- Resource allocation across applications
```

### Why Our Code Uses YARN API:

```python
url = f"{self.yarn_rm_url}/ws/v1/cluster/apps"
params = {'states': 'RUNNING'}
```

**YARN gives us:**

- **Real-time data**: What's happening RIGHT NOW
- **All application types**: Spark, MapReduce, Hive, etc.
- **Resource allocation**: How much memory/CPU each job is using
- **Queue information**: Which queue jobs are running in
- **Container details**: Number of containers per application

### YARN API Response Example:

```json
{
  "apps": {
    "app": [
      {
        "id": "application_1640995200000_0001",
        "name": "daily_etl_pipeline",
        "user": "hadoop",
        "queue": "default",
        "state": "RUNNING",
        "applicationType": "SPARK",
        "allocatedMB": 8192,
        // Memory in MB
        "allocatedVCores": 4,
        // CPU cores
        "runningContainers": 3,
        // Active containers
        "progress": 45.5,
        // Job progress %
        "startedTime": 1640995210000,
        "elapsedTime": 120000
        // Runtime in ms
      }
    ]
  }
}
```

## 2. Spark History Server - The Detailed Historian

### What Spark History Server Does:

```text
# Spark History Server stores detailed execution data
- Application metadata (name, user, duration)
- Executor details (cores, memory per executor)
- Stage information (tasks, shuffle data)
- Job DAG and execution plan
- Detailed performance metrics
```

### Why Our Code Uses Spark History Server:

```python
# First call - get basic application info
url = f"{self.spark_history_url}/api/v1/applications"

# Second call - get detailed executor info per application
executors_url = f"{self.spark_history_url}/api/v1/applications/{app_id}/executors"
```

**Spark History Server gives us:**

- **Detailed resource breakdown**: Per-executor memory and cores
- **Historical data**: Completed jobs with full metrics
- **Performance data**: Task durations, shuffle sizes, GC time
- **Spark-specific metrics**: Not available in YARN

### Spark History Server API Response Example:

```json
// Basic application info
{
  "id": "application_1640995200000_0001",
  "name": "daily_etl_pipeline",
  "attempts": [
    {
      "startTime": "2024-01-01T10:00:00.000GMT",
      "endTime": "2024-01-01T10:30:00.000GMT",
      "duration": 1800000,
      "completed": true
    }
  ]
}

// Executor details
[
  {
    "id": "driver",
    "hostPort": "ip-10-0-1-100:35000",
    "totalCores": 2,
    "maxMemory": 2147483648,
    // 2GB in bytes
    "totalDuration": 45000,
    "totalGCTime": 2000
  },
  {
    "id": "1",
    "hostPort": "ip-10-0-1-101:35000",
    "totalCores": 4,
    "maxMemory": 4294967296,
    // 4GB in bytes
    "totalDuration": 120000,
    "totalGCTime": 5000
  }
]
```

## 3. How Our Code Combines Both Services

### For Running Jobs - YARN Only:

```python
def get_running_jobs_detailed(self):
    # YARN has real-time data for active jobs
    url = f"{self.yarn_rm_url}/ws/v1/cluster/apps"
    params = {'states': 'RUNNING'}

    # Extract what YARN provides
    job_info = {
        'Memory (GB)': round(app.get('allocatedMB', 0) / 1024, 2),
        'vCores': app.get('allocatedVCores', 0),
        'Containers': app.get('runningContainers', 0),
        'Progress (%)': round(app.get('progress', 0), 1),
        'Elapsed Time (mins)': round(elapsed_seconds / 60, 1)
    }
```

**Why not Spark History Server for running jobs?**

- Spark History Server only has data AFTER jobs complete
- YARN provides real-time resource allocation
- We need current progress and container status

### For Completed Jobs - Both Services:

```python
def get_completed_jobs_detailed(self, limit=100):
    # Step 1: Get basic job info from Spark History Server
    url = f"{self.spark_history_url}/api/v1/applications"
    apps = response.json()

    for app in apps:
        # Step 2: Get detailed executor info for each job
        executors_url = f"{self.spark_history_url}/api/v1/applications/{app_id}/executors"
        executors = executors_response.json()

        # Step 3: Calculate total resources across all executors
        total_cores = sum(ex.get('totalCores', 0) for ex in executors)
        max_memory_mb = sum(ex.get('maxMemory', 0) for ex in executors) // (1024 * 1024)
```

**Why Spark History Server for completed jobs?**

- More detailed resource breakdown
- Actual resource usage (not just allocation)
- Historical performance metrics
- Spark-specific execution details

## 4. Resource Calculation Logic

### Memory Calculation:

```python
# YARN gives allocated memory (what was requested)
yarn_memory = app.get('allocatedMB', 0) / 1024  # Convert MB to GB

# Spark gives actual memory usage per executor
spark_memory = sum(executor.get('maxMemory', 0) for executor in executors) / (1024 * 1024 * 1024)
```

### vCore Calculation:

```python
# YARN gives allocated vCores (what was requested)
yarn_vcores = app.get('allocatedVCores', 0)

# Spark gives actual cores per executor
spark_vcores = sum(executor.get('totalCores', 0) for executor in executors)
```

### Resource-Hours Calculation:

```python
# Cost-like metric: Resource × Time
duration_hours = duration_ms / (1000 * 60 * 60)
memory_hours = (memory_gb) * duration_hours
vcore_hours = vcores * duration_hours
```

## 5. Data Collection Flow in Our Code

### Step-by-Step Process:

```python
# 1. User clicks "Refresh Data"
if st.button("Refresh Data"):
    # 2. Initialize monitor with both URLs
    monitor = JobResourceMonitor(
        "http://master-node:18080",  # Spark History Server
        "http://master-node:8088"  # YARN Resource Manager
    )

    # 3. Get running jobs from YARN (real-time)
    running_jobs = monitor.get_running_jobs_detailed()
    # API call: GET /ws/v1/cluster/apps?states=RUNNING

    # 4. Get completed jobs from Spark History (historical)
    completed_jobs = monitor.get_completed_jobs_detailed(limit=200)
    # API calls: 
    # - GET /api/v1/applications (basic info)
    # - GET /api/v1/applications/{id}/executors (detailed info per job)

    # 5. Store in session state
    st.session_state['running_jobs'] = running_jobs
    st.session_state['completed_jobs'] = completed_jobs
```

## 6. Why We Need Both Services

### YARN Strengths:

-  Real-time cluster state
-  All application types (not just Spark)
-  Resource allocation and queuing
-  Container management
-  Limited detail on Spark internals     X
-  No historical data after job completion X

### Spark History Server Strengths:

-  Detailed Spark execution metrics
-  Historical data persistence
-  Per-executor resource breakdown
-  Stage and task-level details
-  Only Spark applications       X
-  No real-time data for running jobs X

### Combined Power:

```python
# Real-time monitoring
running_data = yarn_api()  # What's happening now
completed_data = spark_api()  # What happened before

# Complete picture
total_picture = running_data + completed_data
```

## 7. Job Lifecycle in Our Monitoring

### Job Submission to Completion:

```
1. Job Submitted → YARN receives application
   ├── YARN allocates containers
   ├── Our code sees it in "Running Jobs" tab
   └── Real-time progress tracking

2. Job Executing → Spark executors running
   ├── YARN tracks resource usage
   ├── Spark tracks internal metrics
   └── Our code shows progress, containers, memory

3. Job Completes → Spark writes to History Server
   ├── YARN removes from active applications
   ├── Spark History Server stores detailed data
   └── Our code shows it in "Completed Jobs" tab
```

## 8. API Endpoint Reference

### YARN Resource Manager APIs:

```python
# Get all applications
GET / ws / v1 / cluster / apps

# Get running applications only
GET / ws / v1 / cluster / apps?states = RUNNING

# Get cluster metrics
GET / ws / v1 / cluster / metrics

# Get application details
GET / ws / v1 / cluster / apps / {application_id}
```

### Spark History Server APIs:

```python
# Get all applications
GET / api / v1 / applications

# Get application details
GET / api / v1 / applications / {application_id}

# Get executors for an application
GET / api / v1 / applications / {application_id} / executors

# Get stages for an application
GET / api / v1 / applications / {application_id} / stages
```

## 9. Error Handling Strategy

### Why Errors Happen:

```text
# Common scenarios requiring error handling:
1. Network timeouts to APIs
2. Jobs with incomplete data in Spark History
3. Applications that failed during startup
4. API rate limiting under heavy load
5. Jobs still writing data to History Server
```

### Our Error Handling:

```python
try:
    # Try to get detailed executor information
    executors_response = requests.get(executors_url)
    executors = executors_response.json()
    # Calculate detailed metrics...

except Exception as e:
    # Graceful degradation - show job with minimal info
    job_info = {
        'Job ID': app_id,
        'Job Name': app.get('name', 'Unknown'),
        'Status': 'ERROR',
        'Memory (GB)': 0,
        'vCores': 0
    }
    continue  # Don't crash, just skip to next job
```

This architecture allows  monitoring tool to provide both real-time visibility into current cluster usage (via YARN)
and detailed historical analysis of job performance (via Spark History Server), giving users a complete picture of their
EMR cluster resource utilization.