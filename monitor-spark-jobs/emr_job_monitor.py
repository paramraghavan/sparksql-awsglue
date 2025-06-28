import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import time


class JobResourceMonitor:
    def __init__(self, spark_history_url, yarn_rm_url):
        self.spark_history_url = spark_history_url
        self.yarn_rm_url = yarn_rm_url

    def get_running_jobs_detailed(self):
        """Get detailed information for currently running jobs"""
        url = f"{self.yarn_rm_url}/ws/v1/cluster/apps"
        params = {'states': 'RUNNING'}
        response = requests.get(url, params=params)
        apps = response.json().get('apps', {}).get('app', [])

        running_jobs = []
        for app in apps:
            started_time = app.get('startedTime', 0)
            elapsed_time = app.get('elapsedTime', 0)

            # Handle string timestamps and convert to int
            try:
                started_time = int(started_time) if started_time else 0
                elapsed_time = int(elapsed_time) if elapsed_time else 0
            except (ValueError, TypeError):
                started_time = 0
                elapsed_time = 0

            start_time = datetime.fromtimestamp(started_time / 1000) if started_time > 0 else datetime.now()
            elapsed_seconds = elapsed_time / 1000

            # Safe conversion of numeric fields
            try:
                allocated_mb = float(app.get('allocatedMB', 0))
                allocated_vcores = int(app.get('allocatedVCores', 0))
                running_containers = int(app.get('runningContainers', 0))
                progress = float(app.get('progress', 0))
            except (ValueError, TypeError):
                allocated_mb = 0
                allocated_vcores = 0
                running_containers = 0
                progress = 0

            job_info = {
                'Job ID': app.get('id'),
                'Job Name': app.get('name', 'Unknown')[:50],  # Truncate long names
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
            running_jobs.append(job_info)

        return pd.DataFrame(running_jobs)

    def get_completed_jobs_detailed(self, limit=100):
        """Get detailed information for completed Spark jobs"""
        url = f"{self.spark_history_url}/api/v1/applications"
        params = {'limit': limit}
        response = requests.get(url, params=params)
        apps = response.json()

        completed_jobs = []
        for app in apps:
            app_id = app['id']

            try:
                # Get executor information
                executors_url = f"{self.spark_history_url}/api/v1/applications/{app_id}/executors"
                executors_response = requests.get(executors_url)
                executors = executors_response.json()

                # Calculate metrics with safe type conversion
                total_cores = 0
                max_memory_mb = 0

                for ex in executors:
                    try:
                        cores = int(ex.get('totalCores', 0))
                        memory = int(ex.get('maxMemory', 0))
                        total_cores += cores
                        max_memory_mb += memory
                    except (ValueError, TypeError):
                        continue

                max_memory_mb = max_memory_mb // (1024 * 1024)  # Convert bytes to MB

                # Time calculations with safe conversion
                start_time_ms = app.get('attempts', [{}])[0].get('startTime', 0)
                end_time_ms = app.get('attempts', [{}])[0].get('endTime', 0)
                duration_ms = app.get('attempts', [{}])[0].get('duration', 0)

                try:
                    start_time_ms = int(start_time_ms) if start_time_ms else 0
                    end_time_ms = int(end_time_ms) if end_time_ms else 0
                    duration_ms = int(duration_ms) if duration_ms else 0
                except (ValueError, TypeError):
                    start_time_ms = 0
                    end_time_ms = 0
                    duration_ms = 0

                start_time = datetime.fromtimestamp(start_time_ms / 1000) if start_time_ms > 0 else None
                end_time = datetime.fromtimestamp(end_time_ms / 1000) if end_time_ms > 0 else None
                duration_hours = duration_ms / (1000 * 60 * 60) if duration_ms > 0 else 0

                job_info = {
                    'Job ID': app_id,
                    'Job Name': app.get('name', 'Unknown')[:50],
                    'User': app.get('sparkUser'),
                    'Submit Time': start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else 'Unknown',
                    'End Time': end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'Unknown',
                    'Duration (mins)': round(duration_ms / (1000 * 60), 1) if duration_ms else 0,
                    'Memory (GB)': round(max_memory_mb / 1024, 2),
                    'vCores': total_cores,
                    'Status': 'COMPLETED' if app.get('attempts', [{}])[0].get('completed', False) else 'FAILED',
                    'Memory-Hours': round((max_memory_mb / 1024) * duration_hours, 2),
                    'vCore-Hours': round(total_cores * duration_hours, 2)
                }
                completed_jobs.append(job_info)

            except Exception as e:
                # For failed requests, add minimal info with safe conversions
                start_time_ms = app.get('attempts', [{}])[0].get('startTime', 0)
                try:
                    start_time_ms = int(start_time_ms) if start_time_ms else 0
                    start_time = datetime.fromtimestamp(start_time_ms / 1000) if start_time_ms > 0 else None
                except (ValueError, TypeError):
                    start_time = None

                job_info = {
                    'Job ID': app_id,
                    'Job Name': app.get('name', 'Unknown')[:50],
                    'User': app.get('sparkUser', 'Unknown'),
                    'Submit Time': start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else 'Unknown',
                    'End Time': 'Error',
                    'Duration (mins)': 0,
                    'Memory (GB)': 0,
                    'vCores': 0,
                    'Status': 'ERROR',
                    'Memory-Hours': 0,
                    'vCore-Hours': 0
                }
                completed_jobs.append(job_info)
                continue

        return pd.DataFrame(completed_jobs)

    def get_aggregated_by_job_submission(self, df, time_window_hours=24):
        """Aggregate jobs by submission time windows"""
        if df.empty:
            return pd.DataFrame()

        # Safe datetime conversion
        def safe_datetime_convert(time_str):
            if pd.isna(time_str) or time_str == 'Unknown' or time_str == 'Error':
                return pd.NaT

            try:
                # Try different datetime formats
                if isinstance(time_str, str):
                    # Format: "2024-01-01 10:30:00"
                    if len(time_str) >= 19:
                        return pd.to_datetime(time_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
                    else:
                        return pd.to_datetime(time_str, errors='coerce')
                else:
                    return pd.to_datetime(time_str, errors='coerce')
            except:
                return pd.NaT

        # Convert submit time to datetime with error handling
        df['Submit DateTime'] = df['Submit Time'].apply(safe_datetime_convert)

        # Remove rows with invalid dates
        df = df.dropna(subset=['Submit DateTime'])

        if df.empty:
            return pd.DataFrame()

        # Create time windows (hourly aggregation)
        df['Submit Hour'] = df['Submit DateTime'].dt.floor('H')

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

        # Flatten column names
        hourly_agg.columns = [
            'Job Count', 'Total Memory (GB)', 'Avg Memory (GB)', 'Max Memory (GB)',
            'Total vCores', 'Avg vCores', 'Max vCores',
            'Avg Duration (mins)', 'Max Duration (mins)',
            'Total Memory-Hours', 'Total vCore-Hours', 'Unique Users'
        ]

        # Reset index to make Submit Hour a column
        hourly_agg = hourly_agg.reset_index()
        hourly_agg['Submit Hour'] = hourly_agg['Submit Hour'].dt.strftime('%Y-%m-%d %H:00')

        return hourly_agg.sort_values('Submit Hour', ascending=False)


def main():
    st.set_page_config(page_title="EMR Job Resource Monitor", layout="wide")
    st.title("EMR Job Resource Monitor - Tabular View")

    # Configuration inputs
    col1, col2 = st.columns(2)
    with col1:
        spark_url = st.text_input("Spark History Server URL", "http://your-master-node:18080")
    with col2:
        yarn_url = st.text_input("YARN Resource Manager URL", "http://your-master-node:8088")

    if st.button("Refresh Data") or 'data_loaded' not in st.session_state:
        with st.spinner("Loading job data..."):
            monitor = JobResourceMonitor(spark_url, yarn_url)

            try:
                # Load data
                running_jobs = monitor.get_running_jobs_detailed()
                completed_jobs = monitor.get_completed_jobs_detailed(limit=200)

                # Store in session state
                st.session_state['running_jobs'] = running_jobs
                st.session_state['completed_jobs'] = completed_jobs
                st.session_state['data_loaded'] = True
                st.success("Data loaded successfully!")

            except Exception as e:
                st.error(f"Error loading data: {e}")
                return

    if 'data_loaded' in st.session_state:
        running_jobs = st.session_state['running_jobs']
        completed_jobs = st.session_state['completed_jobs']
        monitor = JobResourceMonitor(spark_url, yarn_url)

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["Running Jobs", "Completed Jobs", "Hourly Aggregation", "User Summary"])

        with tab1:
            st.header("Currently Running Jobs")

            if not running_jobs.empty:
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Running Jobs", len(running_jobs))
                with col2:
                    st.metric("Total Memory (GB)", f"{running_jobs['Memory (GB)'].sum():.1f}")
                with col3:
                    st.metric("Total vCores", running_jobs['vCores'].sum())
                with col4:
                    st.metric("Total Containers", running_jobs['Containers'].sum())

                # Detailed table
                st.subheader("Job Details")

                # Add filters
                col1, col2, col3 = st.columns(3)
                with col1:
                    job_names = ['All'] + sorted(running_jobs['Job Name'].unique().tolist())
                    selected_job = st.selectbox("Filter by Job Name", job_names, key="running_job_filter")
                with col2:
                    users = ['All'] + sorted(running_jobs['User'].unique().tolist())
                    selected_user = st.selectbox("Filter by User", users, key="running_user_filter")
                with col3:
                    min_memory = st.number_input("Min Memory (GB)", min_value=0.0, value=0.0,
                                                 key="running_memory_filter")

                # Apply filters
                filtered_df = running_jobs.copy()
                if selected_job != 'All':
                    filtered_df = filtered_df[filtered_df['Job Name'].str.contains(selected_job, case=False, na=False)]
                if selected_user != 'All':
                    filtered_df = filtered_df[filtered_df['User'] == selected_user]
                if min_memory > 0:
                    filtered_df = filtered_df[filtered_df['Memory (GB)'] >= min_memory]

                # Sort options
                sort_by = st.selectbox("Sort by",
                                       ['Submit Time', 'Memory (GB)', 'vCores', 'Elapsed Time (mins)', 'Progress (%)'],
                                       key="running_sort")
                sort_ascending = st.checkbox("Ascending", value=False, key="running_ascending")

                filtered_df = filtered_df.sort_values(sort_by, ascending=sort_ascending)

                st.dataframe(filtered_df, use_container_width=True, height=400)

                # Export option
                csv_running = filtered_df.to_csv(index=False)
                st.download_button("Download Running Jobs CSV", csv_running, "running_jobs.csv", "text/csv")

            else:
                st.info("No jobs currently running")

        with tab2:
            st.header("Completed Jobs")

            if not completed_jobs.empty:
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Completed Jobs", len(completed_jobs))
                with col2:
                    total_memory_hours = completed_jobs['Memory-Hours'].sum()
                    st.metric("Total Memory-Hours", f"{total_memory_hours:.1f}")
                with col3:
                    total_vcore_hours = completed_jobs['vCore-Hours'].sum()
                    st.metric("Total vCore-Hours", f"{total_vcore_hours:.1f}")
                with col4:
                    success_rate = (completed_jobs['Status'] == 'COMPLETED').mean() * 100
                    st.metric("Success Rate", f"{success_rate:.1f}%")

                # Filters
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    job_names = ['All'] + sorted(
                        [name for name in completed_jobs['Job Name'].unique() if pd.notna(name)])
                    selected_job = st.selectbox("Filter by Job Name", job_names, key="completed_job_filter")
                with col2:
                    users = ['All'] + sorted(completed_jobs['User'].unique().tolist())
                    selected_user = st.selectbox("Filter by User", users, key="completed_user_filter")
                with col3:
                    status_options = ['All'] + sorted(completed_jobs['Status'].unique().tolist())
                    selected_status = st.selectbox("Filter by Status", status_options, key="completed_status_filter")
                with col4:
                    hours_back = st.number_input("Show jobs from last N hours", min_value=1, value=24,
                                                 key="completed_hours_filter")

                # Apply filters
                filtered_df = completed_jobs.copy()

                # Time filter with safe datetime conversion
                cutoff_time = datetime.now() - timedelta(hours=hours_back)

                def safe_parse_submit_time(time_str):
                    if pd.isna(time_str) or time_str == 'Unknown' or time_str == 'Error':
                        return pd.NaT
                    try:
                        if isinstance(time_str, str):
                            return pd.to_datetime(time_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
                        else:
                            return pd.to_datetime(time_str, errors='coerce')
                    except:
                        return pd.NaT

                filtered_df['Submit DateTime'] = filtered_df['Submit Time'].apply(safe_parse_submit_time)

                # Filter out invalid dates and apply time filter
                filtered_df = filtered_df.dropna(subset=['Submit DateTime'])
                if not filtered_df.empty:
                    filtered_df = filtered_df[filtered_df['Submit DateTime'] >= cutoff_time]

                if selected_job != 'All':
                    filtered_df = filtered_df[filtered_df['Job Name'].str.contains(selected_job, case=False, na=False)]
                if selected_user != 'All':
                    filtered_df = filtered_df[filtered_df['User'] == selected_user]
                if selected_status != 'All':
                    filtered_df = filtered_df[filtered_df['Status'] == selected_status]

                # Sort options
                sort_by = st.selectbox("Sort by",
                                       ['Submit Time', 'Duration (mins)', 'Memory (GB)', 'vCores', 'Memory-Hours',
                                        'vCore-Hours'],
                                       key="completed_sort")
                sort_ascending = st.checkbox("Ascending", value=False, key="completed_ascending")

                filtered_df = filtered_df.sort_values(sort_by, ascending=sort_ascending)

                # Remove helper column before display
                display_df = filtered_df.drop('Submit DateTime', axis=1, errors='ignore')

                # Style the dataframe with color coding for status
                def style_status(val):
                    if val == 'COMPLETED':
                        return 'background-color: #d4edda; color: #155724'  # Green
                    elif val == 'FAILED':
                        return 'background-color: #f8d7da; color: #721c24'  # Red
                    elif val == 'ERROR':
                        return 'background-color: #fff3cd; color: #856404'  # Yellow
                    else:
                        return ''

                styled_df = display_df.style.applymap(style_status, subset=['Status'])
                st.dataframe(styled_df, use_container_width=True, height=500)

                # Export option
                csv_completed = display_df.to_csv(index=False)
                st.download_button("Download Completed Jobs CSV", csv_completed, "completed_jobs.csv", "text/csv")

            else:
                st.info("No completed jobs found")

        with tab3:
            st.header("Hourly Job Submission Aggregation")

            if not completed_jobs.empty:
                # Generate hourly aggregation
                hourly_agg = monitor.get_aggregated_by_job_submission(completed_jobs)

                if not hourly_agg.empty:
                    st.subheader("Resource Usage by Submit Hour")

                    # Show last N hours
                    hours_to_show = st.slider("Show last N hours", min_value=6, max_value=168, value=48,
                                              key="agg_hours")
                    display_agg = hourly_agg.head(hours_to_show)

                    st.dataframe(display_agg, use_container_width=True, height=600)

                    # Summary stats
                    st.subheader("Summary Statistics")
                    summary_stats = {
                        'Peak Hour (Job Count)': display_agg.loc[display_agg['Job Count'].idxmax(), 'Submit Hour'],
                        'Peak Hour (Memory Usage)': display_agg.loc[
                            display_agg['Total Memory (GB)'].idxmax(), 'Submit Hour'],
                        'Peak Hour (vCore Usage)': display_agg.loc[display_agg['Total vCores'].idxmax(), 'Submit Hour'],
                        'Avg Jobs per Hour': display_agg['Job Count'].mean(),
                        'Avg Memory per Hour (GB)': display_agg['Total Memory (GB)'].mean(),
                        'Avg vCores per Hour': display_agg['Total vCores'].mean()
                    }

                    summary_df = pd.DataFrame(list(summary_stats.items()), columns=['Metric', 'Value'])
                    st.dataframe(summary_df, use_container_width=True)

                    # Export
                    csv_hourly = display_agg.to_csv(index=False)
                    st.download_button("Download Hourly Aggregation CSV", csv_hourly, "hourly_aggregation.csv",
                                       "text/csv")

                else:
                    st.info("No data available for aggregation")
            else:
                st.info("No completed jobs available for aggregation")

        with tab4:
            st.header("Job Summary")

            # Combine running and completed jobs for job summary
            all_jobs = []

            if not running_jobs.empty:
                running_summary = running_jobs.copy()
                running_summary['Job Type'] = 'RUNNING'
                all_jobs.append(running_summary)

            if not completed_jobs.empty:
                completed_summary = completed_jobs.copy()
                completed_summary['Job Type'] = 'COMPLETED'
                all_jobs.append(completed_summary)

            if all_jobs:
                combined_df = pd.concat(all_jobs, ignore_index=True)

                # Job aggregation instead of user aggregation
                job_summary = combined_df.groupby('Job Name').agg({
                    'Job ID': 'count',
                    'Memory (GB)': ['sum', 'mean', 'max'],
                    'vCores': ['sum', 'mean', 'max'],
                    'Memory-Hours': 'sum',
                    'vCore-Hours': 'sum',
                    'Duration (mins)': 'mean',
                    'User': lambda x: ', '.join(x.unique())  # Show all users who ran this job
                }).round(2)

                # Flatten column names
                job_summary.columns = [
                    'Total Runs', 'Total Memory (GB)', 'Avg Memory (GB)', 'Max Memory (GB)',
                    'Total vCores', 'Avg vCores', 'Max vCores',
                    'Total Memory-Hours', 'Total vCore-Hours', 'Avg Duration (mins)', 'Users'
                ]

                # Add job type breakdown (running vs completed)
                job_types = combined_df.groupby(['Job Name', 'Job Type']).size().unstack(fill_value=0)
                job_summary = job_summary.join(job_types, how='left').fillna(0)

                # Sort by total resource usage
                job_summary = job_summary.sort_values('Total Memory-Hours', ascending=False)

                # Add filters for job summary
                col1, col2 = st.columns(2)
                with col1:
                    min_runs = st.number_input("Min number of runs", min_value=1, value=1, key="job_summary_min_runs")
                with col2:
                    min_memory_hours = st.number_input("Min Memory-Hours", min_value=0.0, value=0.0,
                                                       key="job_summary_min_memory")

                # Apply filters
                filtered_job_summary = job_summary[
                    (job_summary['Total Runs'] >= min_runs) &
                    (job_summary['Total Memory-Hours'] >= min_memory_hours)
                    ]

                st.subheader("Job Resource Usage Summary")
                st.dataframe(filtered_job_summary, use_container_width=True)

                # Show top resource consuming jobs
                st.subheader("Top 10 Resource Consuming Jobs")
                top_jobs = filtered_job_summary.head(10)[
                    ['Total Runs', 'Total Memory-Hours', 'Total vCore-Hours', 'Avg Duration (mins)', 'Users']]
                st.dataframe(top_jobs, use_container_width=True)

                # Export
                csv_jobs = job_summary.to_csv()
                st.download_button("Download Job Summary CSV", csv_jobs, "job_summary.csv", "text/csv")

            else:
                st.info("No job data available for job summary")

    # Auto-refresh option
    st.sidebar.header("Settings")
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 30 seconds")

    if auto_refresh:
        time.sleep(30)
        st.experimental_rerun()


if __name__ == "__main__":
    main()