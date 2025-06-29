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

- Real-time cluster state
- All application types (not just Spark)
- Resource allocation and queuing
- Container management
- Limited detail on Spark internals X
- No historical data after job completion X

### Spark History Server Strengths:

- Detailed Spark execution metrics
- Historical data persistence
- Per-executor resource breakdown
- Stage and task-level details
- Only Spark applications X
- No real-time data for running jobs X

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

This architecture allows monitoring tool to provide both real-time visibility into current cluster usage (via YARN)
and detailed historical analysis of job performance (via Spark History Server), giving users a complete picture of their
EMR cluster resource utilization.
==============================================================================================================

# EMR Job Monitor - DataFrame Processing Flow

## 1. Initial Data Collection Phase

### From YARN Resource Manager (Running Jobs)

```python
def get_running_jobs_detailed(self):
    # API Call to YARN RM
    url = f"{self.yarn_rm_url}/ws/v1/cluster/apps"
    params = {'states': 'RUNNING'}
    response = requests.get(url, params=params)
    apps = response.json().get('apps', {}).get('app', [])
```

**What happens:**

- Makes REST API call to YARN Resource Manager
- Gets JSON response with running applications
- Extracts the 'app' array containing job details

### DataFrame Creation for Running Jobs

```python
# For each app, creates a dictionary with processed data
job_info = {
    'Job ID': app.get('id'),
    'Job Name': app.get('name', 'Unknown')[:50],
    'User': app.get('user'),
    'Queue': app.get('queue'),
    'App Type': app.get('applicationType'),
    'Submit Time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
    'Elapsed Time (mins)': round(elapsed_seconds / 60, 1),
    'Memory (GB)': round(allocated_mb / 1024, 2),
    'vCores': allocated_vcores,
    'Containers': running_containers,
    'Progress (%)': round(progress, 1),
    'Status': 'RUNNING',
    'Memory-Hours': round((allocated_mb / 1024) * (elapsed_seconds / 3600), 2),
    'vCore-Hours': round(allocated_vcores * (elapsed_seconds / 3600), 2)
}

# Creates pandas DataFrame
return pd.DataFrame(running_jobs)
```

**DataFrame Structure for Running Jobs:**
| Column | Type | Description |
|--------|------|-------------|
| Job ID | string | YARN application ID |
| Job Name | string | Application name (truncated) |
| User | string | Username who submitted |
| Memory (GB) | float | Current memory allocation |
| vCores | int | Current vCore allocation |
| Elapsed Time (mins) | float | Time since job started |
| Progress (%) | float | Job completion percentage |

---

## 2. Spark History Server Data (Completed Jobs)

### API Calls and DataFrame Creation

```python
def get_completed_jobs_detailed(self, limit=100):
    # Main applications API
    url = f"{self.spark_history_url}/api/v1/applications"
    response = requests.get(url, params={'limit': limit})
    apps = response.json()

    # For each app, get executor details
    for app in apps:
        app_id = app['id']
        executors_url = f"{self.spark_history_url}/api/v1/applications/{app_id}/executors"
        executors_response = requests.get(executors_url)
        executors = executors_response.json()
```

**DataFrame Structure for Completed Jobs:**
| Column | Type | Description |
|--------|------|-------------|
| Job ID | string | Spark application ID |
| Job Name | string | Application name |
| User | string | Spark user |
| Submit Time | string | When job was submitted |
| End Time | string | When job finished |
| Duration (mins) | float | Total execution time |
| Memory (GB) | float | Total memory used |
| vCores | int | Total vCores used |
| Status | string | COMPLETED/FAILED/ERROR |

---

## 3. Streamlit Data Storage and Session Management

### Session State Storage

```python
if st.button("Refresh Data") or 'data_loaded' not in st.session_state:
    with st.spinner("Loading job data..."):
        monitor = JobResourceMonitor(spark_url, yarn_url)

        # Load data into DataFrames
        running_jobs = monitor.get_running_jobs_detailed()
        completed_jobs = monitor.get_completed_jobs_detailed(limit=200)

        # Store in Streamlit session state
        st.session_state['running_jobs'] = running_jobs
        st.session_state['completed_jobs'] = completed_jobs
        st.session_state['data_loaded'] = True
```

**Purpose:**

- Avoids re-fetching data on every Streamlit interaction
- Maintains DataFrames across tab switches
- Enables refresh functionality

---

## 4. Tab 1: Running Jobs DataFrame Operations

### Filtering Operations

```python
# Create working copy
filtered_df = running_jobs.copy()

# Apply filters using pandas operations
if selected_job != 'All':
    filtered_df = filtered_df[filtered_df['Job Name'].str.contains(selected_job, case=False, na=False)]

if selected_user != 'All':
    filtered_df = filtered_df[filtered_df['User'] == selected_user]

if min_memory > 0:
    filtered_df = filtered_df[filtered_df['Memory (GB)'] >= min_memory]
```

### Sorting and Display

```python
# Sort DataFrame
filtered_df = filtered_df.sort_values(sort_by, ascending=sort_ascending)

# Display in Streamlit
st.dataframe(filtered_df, use_container_width=True, height=400)
```

**DataFrame Operations Used:**

- `.copy()` - Creates independent copy for filtering
- `.str.contains()` - String pattern matching for job names
- Boolean indexing - Filters rows based on conditions
- `.sort_values()` - Sorts by selected column

---

## 5. Tab 2: Completed Jobs DataFrame Operations

### Time-based Filtering

```python
# Create datetime column for filtering
def safe_parse_submit_time(time_str):
    try:
        return pd.to_datetime(time_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except:
        return pd.NaT


filtered_df['Submit DateTime'] = filtered_df['Submit Time'].apply(safe_parse_submit_time)

# Time filter
cutoff_time = datetime.now() - timedelta(hours=hours_back)
filtered_df = filtered_df[filtered_df['Submit DateTime'] >= cutoff_time]
```

### DataFrame Styling

```python
def style_status(val):
    if val == 'COMPLETED':
        return 'background-color: #d4edda; color: #155724'  # Green
    elif val == 'FAILED':
        return 'background-color: #f8d7da; color: #721c24'  # Red
    else:
        return ''


styled_df = display_df.style.applymap(style_status, subset=['Status'])
st.dataframe(styled_df, use_container_width=True, height=500)
```

**DataFrame Operations Used:**

- `.apply()` - Applies function to parse datetime strings
- `.dropna()` - Removes rows with invalid dates
- Time-based boolean indexing
- `.style.applymap()` - Applies conditional formatting

---

## 6. Tab 3: Hourly Aggregation DataFrame Operations

### Time Window Creation

```python
def get_aggregated_by_job_submission(self, df, time_window_hours=24):
    # Convert to datetime
    df['Submit DateTime'] = df['Submit Time'].apply(safe_datetime_convert)

    # Create hourly time windows
    df['Submit Hour'] = df['Submit DateTime'].dt.floor('H')
```

### Groupby Aggregation

```python
# Aggregate by hour
hourly_agg = df.groupby('Submit Hour').agg({
    'Job ID': 'count',
    'Memory (GB)': ['sum', 'mean', 'max'],
    'vCores': ['sum', 'mean', 'max'],
    'Duration (mins)': ['mean', 'max'],
    'Memory-Hours': 'sum',
    'vCore-Hours': 'sum',
    'User': lambda x: len(x.unique())
}).round(2)
```

### Column Flattening

```python
# Flatten multi-level column names
hourly_agg.columns = [
    'Job Count', 'Total Memory (GB)', 'Avg Memory (GB)', 'Max Memory (GB)',
    'Total vCores', 'Avg vCores', 'Max vCores',
    'Avg Duration (mins)', 'Max Duration (mins)',
    'Total Memory-Hours', 'Total vCore-Hours', 'Unique Users'
]
```

**DataFrame Operations Used:**

- `.dt.floor('H')` - Rounds datetime to nearest hour
- `.groupby()` - Groups data by time windows
- `.agg()` - Applies multiple aggregation functions
- Multi-level column flattening
- `.round()` - Rounds numeric values

---

## 7. Tab 4: Job Summary DataFrame Operations

### DataFrame Combination

```python
# Combine running and completed jobs
all_jobs = []

if not running_jobs.empty:
    running_summary = running_jobs.copy()
    running_summary['Job Type'] = 'RUNNING'
    all_jobs.append(running_summary)

if not completed_jobs.empty:
    completed_summary = completed_jobs.copy()
    completed_summary['Job Type'] = 'COMPLETED'
    all_jobs.append(completed_summary)

# Concatenate DataFrames
combined_df = pd.concat(all_jobs, ignore_index=True, sort=False)
```

### Data Type Cleaning

```python
# Clean numeric columns
numeric_columns = ['Memory (GB)', 'vCores', 'Memory-Hours', 'vCore-Hours']
for col in numeric_columns:
    if col in combined_df.columns:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0)
```

### Complex Aggregation

```python
# Aggregate by job name
job_summary = combined_df.groupby('Job Name').agg({
    'Job ID': 'count',
    'Memory (GB)': ['sum', 'mean', 'max'],
    'vCores': ['sum', 'mean', 'max'],
    'Memory-Hours': 'sum',
    'vCore-Hours': 'sum',
    'Effective Duration (mins)': 'mean',
    'User': lambda x: ', '.join(x.dropna().unique()[:5])
}).round(2)
```

### Cross-tabulation

```python
# Job type breakdown
job_types = combined_df.groupby(['Job Name', 'Job Type']).size().unstack(fill_value=0)
job_summary = job_summary.join(job_types, how='left').fillna(0)
```

**DataFrame Operations Used:**

- `.concat()` - Combines multiple DataFrames vertically
- `.pd.to_numeric()` - Converts columns to numeric with error handling
- Complex `.groupby().agg()` - Multiple aggregation functions per column
- `.unstack()` - Pivots grouped data
- `.join()` - Merges DataFrames on index

---

## 8. Common DataFrame Patterns Throughout

### Error Handling Pattern

```python
try:
    # DataFrame operation
    result = df.some_operation()
except Exception as e:
    # Fallback or error display
    st.error(f"Error: {e}")
```

### Safe Data Access Pattern

```python
# Safe numeric conversion
value = pd.to_numeric(raw_value, errors='coerce')

# Safe string operations
filtered = df[df['column'].str.contains(pattern, case=False, na=False)]

# Safe datetime conversion
df['datetime_col'] = pd.to_datetime(df['string_col'], errors='coerce')
```

### Export Pattern

```python
# Convert DataFrame to CSV
csv_data = filtered_df.to_csv(index=False)
st.download_button("Download CSV", csv_data, "filename.csv", "text/csv")
```

## Summary of DataFrame Flow

1. **Raw API Data** → **Structured Dictionaries** → **Initial DataFrames**
2. **Session Storage** → **Working Copies** for each tab
3. **Filtering & Sorting** → **Display DataFrames**
4. **Aggregation & Analysis** → **Summary DataFrames**
5. **Export & Visualization** → **Final Output**

========================================================================================

# Resource Metrics: Running vs Completed Jobs

## Data Sources & What They Represent

### Running Jobs (from YARN Resource Manager)

**Source:** YARN ResourceManager REST API (`/ws/v1/cluster/apps`)
**Nature:** Real-time, current allocations

### Completed Jobs (from Spark History Server)

**Source:** Spark History Server REST API (`/api/v1/applications`)
**Nature:** Historical, actual usage data

---

## Memory Metrics Comparison

### Running Jobs - Memory (GB)

```python
# From YARN API
allocated_mb = float(app.get('allocatedMB', 0))
'Memory (GB)': round(allocated_mb / 1024, 2)
```

**What it means:**

- **Currently allocated memory** to the job RIGHT NOW
- Memory that YARN has **reserved** for this job
- **Not necessarily being used** - just reserved
- Can change during job execution as containers are added/removed
- Includes memory for driver + all executors currently running

**Example:** Job shows 50 GB - means YARN has allocated 50 GB of cluster memory to this job currently

### Completed Jobs - Memory (GB)

```python
# From Spark History Server
for ex in executors:
    memory = int(ex.get('maxMemory', 0))
    max_memory_mb += memory
max_memory_mb = max_memory_mb // (1024 * 1024)  # Convert bytes to MB
```

**What it means:**

- **Peak memory usage** across all executors during the job's lifetime
- Sum of `maxMemory` from all executors that ran during the job
- Represents **actual memory capacity** that was available to the job
- Fixed number - won't change since job is complete
- Historical maximum, not average usage

**Example:** Job shows 50 GB - means the job had access to 50 GB total memory capacity across all executors

---

## vCores Metrics Comparison

### Running Jobs - vCores

```python
# From YARN API
allocated_vcores = int(app.get('allocatedVCores', 0))
```

**What it means:**

- **Currently allocated virtual cores** to the job
- Number of CPU cores YARN has **reserved** for this job right now
- Active allocation that can be used for parallel processing
- Can fluctuate as containers start/stop

**Example:** Job shows 20 vCores - means 20 CPU cores are currently reserved for this job

### Completed Jobs - vCores

```python
# From Spark History Server
for ex in executors:
    cores = int(ex.get('totalCores', 0))
    total_cores += cores
```

**What it means:**

- **Total cores** that were available across all executors
- Sum of all cores from all executors that participated in the job
- Represents the job's **parallelism capacity**
- Historical maximum core count

**Example:** Job shows 20 vCores - means the job had 20 cores available for parallel processing across all executors

---

## Time Metrics: Key Differences

### Running Jobs

```python
'Elapsed Time (mins)': round(elapsed_seconds / 60, 1)
'Memory-Hours': round((allocated_mb / 1024) * (elapsed_seconds / 3600), 2)
'vCore-Hours': round(allocated_vcores * (elapsed_seconds / 3600), 2)
```

**Meaning:**

- **Elapsed Time:** How long the job has been running so far
- **Memory-Hours:** Allocated memory × elapsed time (cost metric)
- **vCore-Hours:** Allocated cores × elapsed time (cost metric)
- **Still accumulating** - numbers grow until job completes

### Completed Jobs

```python
'Duration (mins)': round(duration_ms / (1000 * 60), 1)
'Memory-Hours': round((max_memory_mb / 1024) * duration_hours, 2)
'vCore-Hours': round(total_cores * duration_hours, 2)
```

**Meaning:**

- **Duration:** Total time the job took to complete
- **Memory-Hours:** Peak memory × total duration (final cost)
- **vCore-Hours:** Total cores × total duration (final cost)
- **Fixed values** - final resource consumption

---

## Practical Implications

### For Cost Analysis

| Metric           | Running Jobs      | Completed Jobs      |
|------------------|-------------------|---------------------|
| **Memory-Hours** | Current burn rate | Final cost          |
| **vCore-Hours**  | Ongoing cost      | Total cost          |
| **Purpose**      | Monitor spending  | Historical analysis |

### For Capacity Planning

| Aspect       | Running Jobs            | Completed Jobs         |
|--------------|-------------------------|------------------------|
| **Memory**   | Current cluster load    | Historical peak usage  |
| **vCores**   | Current CPU utilization | Historical parallelism |
| **Use Case** | Real-time monitoring    | Pattern analysis       |

### For Performance Optimization

| Metric            | Running Jobs          | Completed Jobs       |
|-------------------|-----------------------|----------------------|
| **Progress %**    | Current completion    | Always 100%          |
| **Resource/Time** | Efficiency monitoring | Post-mortem analysis |
| **Containers**    | Active parallelism    | Not available        |

---

## Important Considerations

### Running Jobs Caveats

1. **Allocation ≠ Usage:** Job might be allocated 50 GB but only using 30 GB
2. **Dynamic Allocation:** Resources can change during execution
3. **Overprovisioning:** YARN might allocate more than actually needed
4. **Container Overhead:** Includes YARN container overhead

### Completed Jobs Caveats

1. **Peak vs Average:** Shows maximum capacity, not average usage
2. **Executor Churn:** Multiple executors over time are summed
3. **Failed Executors:** May include resources from failed/restarted executors
4. **Memory Overhead:** Includes Spark's memory overhead calculations

### Code Example: Different Data Sources

```python
# Running Job (YARN API response)
{
    "allocatedMB": 51200,  # Currently allocated
    "allocatedVCores": 20,  # Currently allocated
    "runningContainers": 5,  # Active now
    "progress": 45.5  # Current progress
}

# Completed Job (Spark History API + Executors API)
{
    "maxMemory": 53687091200,  # Peak capacity (bytes)
    "totalCores": 20,  # Total cores used
    "duration": 1800000,  # Total time (ms)
    "completed": true  # Final status
}
```

## Summary

- **Running Jobs:** Show **current allocations** and **ongoing consumption**
- **Completed Jobs:** Show **peak capacity** and **total consumption**
- **Both are valuable** but answer different questions about resource usage
- **Use running jobs** for real-time monitoring and intervention
- **Use completed jobs** for historical analysis and optimization