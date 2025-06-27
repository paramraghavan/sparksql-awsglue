Great question! Let me explain what these monitors do and show you how to get resource usage by individual jobs instead
of by user.

## What the Monitors Do

### Spark Job Monitor

- **Connects to Spark History Server** (port 18080) which stores completed Spark application data
- **Retrieves application metadata**: job names, duration, user, status
- **Gets executor information**: number of cores, memory allocated per executor
- **Accesses stage details**: task-level performance metrics
- **Provides historical data**: jobs that have completed (not real-time running jobs)

### YARN Resource Monitor

- **Connects to YARN Resource Manager** (port 8088) which manages cluster resources
- **Shows real-time cluster state**: total/available memory, vCores, containers
- **Lists currently running applications**: active jobs consuming resources right now
- **Tracks resource allocation**: how much memory/CPU each application is using
- **Provides queue information**: which queues jobs are running in

## Resource Usage by Job (Instead of User)

Code to track resources by individual jobs:

```python
import requests
import pandas as pd
from datetime import datetime
import json


class JobResourceMonitor:
    def __init__(self, spark_history_url, yarn_rm_url):
        self.spark_history_url = spark_history_url
        self.yarn_rm_url = yarn_rm_url

    def get_running_jobs_resources(self):
        """Get real-time resource usage for currently running jobs"""
        url = f"{self.yarn_rm_url}/ws/v1/cluster/apps"
        params = {'states': 'RUNNING'}
        response = requests.get(url, params=params)
        apps = response.json().get('apps', {}).get('app', [])

        running_jobs = []
        for app in apps:
            job_info = {
                'job_id': app.get('id'),
                'job_name': app.get('name'),
                'user': app.get('user'),
                'queue': app.get('queue'),
                'application_type': app.get('applicationType'),
                'start_time': app.get('startedTime'),
                'elapsed_time_ms': app.get('elapsedTime'),
                'allocated_memory_mb': app.get('allocatedMB', 0),
                'allocated_vcores': app.get('allocatedVCores', 0),
                'running_containers': app.get('runningContainers', 0),
                'memory_seconds': app.get('memorySeconds', 0),
                'vcore_seconds': app.get('vcoreSeconds', 0),
                'progress': app.get('progress', 0),
                'final_status': app.get('finalStatus', 'UNDEFINED')
            }
            running_jobs.append(job_info)

        return pd.DataFrame(running_jobs)

    def get_completed_jobs_resources(self, limit=50):
        """Get resource usage for recently completed Spark jobs"""
        # Get applications from Spark History Server
        url = f"{self.spark_history_url}/api/v1/applications"
        params = {'limit': limit}
        response = requests.get(url, params=params)
        apps = response.json()

        completed_jobs = []
        for app in apps:
            app_id = app['id']

            try:
                # Get detailed executor information
                executors_url = f"{self.spark_history_url}/api/v1/applications/{app_id}/executors"
                executors_response = requests.get(executors_url)
                executors = executors_response.json()

                # Calculate resource usage
                total_cores = sum(ex.get('totalCores', 0) for ex in executors)
                max_memory_mb = sum(ex.get('maxMemory', 0) for ex in executors) // (1024 * 1024)
                total_task_time = sum(ex.get('totalDuration', 0) for ex in executors)
                total_gc_time = sum(ex.get('totalGCTime', 0) for ex in executors)

                # Get stage information for more details
                stages_url = f"{self.spark_history_url}/api/v1/applications/{app_id}/stages"
                stages_response = requests.get(stages_url)
                stages = stages_response.json()

                total_tasks = sum(stage.get('numTasks', 0) for stage in stages)
                failed_tasks = sum(stage.get('numFailedTasks', 0) for stage in stages)

                job_info = {
                    'job_id': app_id,
                    'job_name': app.get('name'),
                    'user': app.get('sparkUser'),
                    'start_time': app.get('attempts', [{}])[0].get('startTime'),
                    'end_time': app.get('attempts', [{}])[0].get('endTime'),
                    'duration_ms': app.get('attempts', [{}])[0].get('duration', 0),
                    'duration_minutes': app.get('attempts', [{}])[0].get('duration', 0) / (1000 * 60),
                    'total_cores': total_cores,
                    'max_memory_mb': max_memory_mb,
                    'total_task_time_ms': total_task_time,
                    'total_gc_time_ms': total_gc_time,
                    'gc_time_ratio': (total_gc_time / total_task_time * 100) if total_task_time > 0 else 0,
                    'total_tasks': total_tasks,
                    'failed_tasks': failed_tasks,
                    'success_rate': ((total_tasks - failed_tasks) / total_tasks * 100) if total_tasks > 0 else 0,
                    'status': 'COMPLETED' if app.get('attempts', [{}])[0].get('completed', False) else 'FAILED'
                }
                completed_jobs.append(job_info)

            except Exception as e:
                print(f"Error processing app {app_id}: {e}")
                continue

        return pd.DataFrame(completed_jobs)

    def get_job_efficiency_metrics(self, job_id):
        """Get detailed efficiency metrics for a specific job"""
        try:
            # Get executor details
            executors_url = f"{self.spark_history_url}/api/v1/applications/{job_id}/executors"
            executors_response = requests.get(executors_url)
            executors = executors_response.json()

            # Get stage details
            stages_url = f"{self.spark_history_url}/api/v1/applications/{job_id}/stages"
            stages_response = requests.get(stages_url)
            stages = stages_response.json()

            efficiency_metrics = {
                'job_id': job_id,
                'executor_count': len([ex for ex in executors if ex['id'] != 'driver']),
                'avg_cpu_utilization': sum(ex.get('totalDuration', 0) for ex in executors) / sum(
                    ex.get('totalCores', 1) for ex in executors) if executors else 0,
                'memory_spill_disk': sum(ex.get('diskBytesSpilled', 0) for ex in executors),
                'memory_spill_memory': sum(ex.get('memoryBytesSpilled', 0) for ex in executors),
                'shuffle_read_mb': sum(ex.get('totalShuffleRead', 0) for ex in executors) / (1024 * 1024),
                'shuffle_write_mb': sum(ex.get('totalShuffleWrite', 0) for ex in executors) / (1024 * 1024),
                'input_size_mb': sum(ex.get('totalInputSize', 0) for ex in executors) / (1024 * 1024),
                'output_size_mb': sum(ex.get('totalOutputSize', 0) for ex in executors) / (1024 * 1024),
                'stage_count': len(stages),
                'longest_stage_duration': max((stage.get('executorRunTime', 0) for stage in stages), default=0)
            }

            return efficiency_metrics

        except Exception as e:
            print(f"Error getting efficiency metrics for {job_id}: {e}")
            return None


# Usage Examples
monitor = JobResourceMonitor("http://your-master-node:18080", "http://your-master-node:8088")

# Get currently running jobs and their resource usage
print("=== CURRENTLY RUNNING JOBS ===")
running_jobs = monitor.get_running_jobs_resources()
if not running_jobs.empty:
    # Sort by resource usage
    running_jobs_sorted = running_jobs.sort_values('allocated_memory_mb', ascending=False)
    print(running_jobs_sorted[
              ['job_name', 'user', 'allocated_memory_mb', 'allocated_vcores', 'elapsed_time_ms', 'progress']])
else:
    print("No jobs currently running")

print("\n=== RECENTLY COMPLETED JOBS ===")
completed_jobs = monitor.get_completed_jobs_resources()
if not completed_jobs.empty:
    # Sort by duration or resource usage
    completed_jobs_sorted = completed_jobs.sort_values('max_memory_mb', ascending=False)
    print(completed_jobs_sorted[
              ['job_name', 'user', 'duration_minutes', 'max_memory_mb', 'total_cores', 'success_rate', 'status']])
else:
    print("No completed jobs found")

# Get efficiency metrics for a specific job
if not completed_jobs.empty:
    sample_job_id = completed_jobs.iloc[0]['job_id']
    print(f"\n=== EFFICIENCY METRICS FOR JOB: {sample_job_id} ===")
    efficiency = monitor.get_job_efficiency_metrics(sample_job_id)
    if efficiency:
        for key, value in efficiency.items():
            print(f"{key}: {value}")
```

## Enhanced Job Analysis Dashboard

```python
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


def create_job_analysis_dashboard():
    st.title("EMR Job Resource Analysis")

    monitor = JobResourceMonitor("http://your-master-node:18080", "http://your-master-node:8088")

    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["Running Jobs", "Completed Jobs", "Job Efficiency"])

    with tab1:
        st.header("Currently Running Jobs")
        running_jobs = monitor.get_running_jobs_resources()

        if not running_jobs.empty:
            # Real-time resource usage
            fig = px.bar(running_jobs, x='job_name', y='allocated_memory_mb',
                         color='user', title="Memory Usage by Running Job")
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig)

            # Resource utilization table
            st.subheader("Resource Details")
            display_cols = ['job_name', 'user', 'allocated_memory_mb', 'allocated_vcores',
                            'running_containers', 'progress', 'elapsed_time_ms']
            st.dataframe(running_jobs[display_cols])
        else:
            st.info("No jobs currently running")

    with tab2:
        st.header("Completed Jobs Analysis")
        completed_jobs = monitor.get_completed_jobs_resources()

        if not completed_jobs.empty:
            # Job duration vs resource usage
            fig = px.scatter(completed_jobs, x='max_memory_mb', y='duration_minutes',
                             color='user', size='total_cores',
                             hover_data=['job_name', 'success_rate'],
                             title="Job Duration vs Memory Usage")
            st.plotly_chart(fig)

            # Top resource consumers
            top_jobs = completed_jobs.nlargest(10, 'max_memory_mb')
            st.subheader("Top 10 Memory Consumers")
            st.dataframe(
                top_jobs[['job_name', 'user', 'max_memory_mb', 'total_cores', 'duration_minutes', 'success_rate']])

            # Job success rate
            success_rate_fig = px.histogram(completed_jobs, x='success_rate', nbins=20,
                                            title="Job Success Rate Distribution")
            st.plotly_chart(success_rate_fig)

    with tab3:
        st.header("Job Efficiency Analysis")

        if not completed_jobs.empty:
            # Select a job for detailed analysis
            job_names = completed_jobs['job_name'].tolist()
            selected_job = st.selectbox("Select a job for detailed analysis:", job_names)

            if selected_job:
                job_row = completed_jobs[completed_jobs['job_name'] == selected_job].iloc[0]
                job_id = job_row['job_id']

                # Display basic metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Duration", f"{job_row['duration_minutes']:.1f} min")
                with col2:
                    st.metric("Max Memory", f"{job_row['max_memory_mb']:.0f} MB")
                with col3:
                    st.metric("Total Cores", f"{job_row['total_cores']}")
                with col4:
                    st.metric("Success Rate", f"{job_row['success_rate']:.1f}%")

                # Get detailed efficiency metrics
                efficiency = monitor.get_job_efficiency_metrics(job_id)
                if efficiency:
                    st.subheader("Efficiency Metrics")

                    # Create efficiency visualization
                    metrics_df = pd.DataFrame([
                        {"Metric": "Shuffle Read (MB)", "Value": efficiency['shuffle_read_mb']},
                        {"Metric": "Shuffle Write (MB)", "Value": efficiency['shuffle_write_mb']},
                        {"Metric": "Input Size (MB)", "Value": efficiency['input_size_mb']},
                        {"Metric": "Output Size (MB)", "Value": efficiency['output_size_mb']},
                        {"Metric": "Memory Spill (MB)", "Value": efficiency['memory_spill_memory'] / (1024 * 1024)},
                        {"Metric": "Disk Spill (MB)", "Value": efficiency['memory_spill_disk'] / (1024 * 1024)}
                    ])

                    fig = px.bar(metrics_df, x='Metric', y='Value',
                                 title="Job I/O and Spill Metrics")
                    fig.update_xaxis(tickangle=45)
                    st.plotly_chart(fig)

                    # Display raw metrics
                    st.json(efficiency)


if __name__ == "__main__":
    create_job_analysis_dashboard()
```

## Key Differences: Job-Based vs User-Based Monitoring

### Job-Based Monitoring Shows:

- **Individual job performance**: Each job's resource consumption
- **Job efficiency**: CPU utilization, memory spills, I/O patterns
- **Resource allocation per job**: Exact cores/memory per application
- **Job duration and success rates**: Performance metrics per job
- **Detailed execution metrics**: Stage-level performance data

### User-Based Monitoring Shows:

- **Aggregate usage**: Total resources consumed by each user
- **User behavior patterns**: Who uses the cluster most
- **Resource quotas**: How much each user is consuming
- **Multi-job analysis**: All jobs from a user combined

The job-based approach gives you much more granular visibility into individual job performance and efficiency, which is
crucial for optimizing Spark applications and identifying problematic jobs that may be consuming excessive resources.