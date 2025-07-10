import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np


class JobResourceMonitor:
    def __init__(self, spark_history_url, yarn_rm_url, emr_pricing=None):
        self.spark_history_url = spark_history_url
        self.yarn_rm_url = yarn_rm_url

        # Default EMR pricing (you should update these based on your region and instance types)
        self.emr_pricing = emr_pricing or {
            'emr_charge_per_hour': 0.096,  # EMR service charge per vCore-hour
            'ec2_cost_per_vcpu_hour': 0.0464,  # Average EC2 cost per vCPU-hour
            'memory_cost_per_gb_hour': 0.0050,  # Approximate memory cost per GB-hour
        }

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

            memory_hours = (allocated_mb / 1024) * (elapsed_seconds / 3600)
            vcore_hours = allocated_vcores * (elapsed_seconds / 3600)

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
                'Memory-Hours': round(memory_hours, 2),
                'vCore-Hours': round(vcore_hours, 2)
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

                memory_hours = (max_memory_mb / 1024) * duration_hours
                vcore_hours = total_cores * duration_hours

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
                    'Memory-Hours': round(memory_hours, 2),
                    'vCore-Hours': round(vcore_hours, 2)
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

    def calculate_job_costs(self, df):
        """Calculate costs for each job based on resource usage"""
        if df.empty:
            return df

        df_cost = df.copy()

        # Calculate individual cost components
        df_cost['EMR Service Cost'] = df_cost['vCore-Hours'] * self.emr_pricing['emr_charge_per_hour']
        df_cost['EC2 Compute Cost'] = df_cost['vCore-Hours'] * self.emr_pricing['ec2_cost_per_vcpu_hour']
        df_cost['Memory Cost'] = df_cost['Memory-Hours'] * self.emr_pricing['memory_cost_per_gb_hour']

        # Total cost per job
        df_cost['Total Cost'] = (df_cost['EMR Service Cost'] +
                                 df_cost['EC2 Compute Cost'] +
                                 df_cost['Memory Cost'])

        # Round cost columns
        cost_columns = ['EMR Service Cost', 'EC2 Compute Cost', 'Memory Cost', 'Total Cost']
        df_cost[cost_columns] = df_cost[cost_columns].round(4)

        return df_cost

    def get_jobs_by_date_range(self, start_date, end_date, include_running=True):
        """Get all jobs within a specified date range"""
        # Convert string dates to datetime objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Add time to end_date to include the full day
        end_date = end_date.replace(hour=23, minute=59, second=59)

        all_jobs = []

        # Get completed jobs (increase limit to get more historical data)
        completed_df = self.get_completed_jobs_detailed(limit=1000)

        if not completed_df.empty:
            # Filter completed jobs by date range
            completed_df['Submit DateTime'] = pd.to_datetime(completed_df['Submit Time'], errors='coerce')
            completed_df = completed_df.dropna(subset=['Submit DateTime'])

            # Filter by date range
            mask = (completed_df['Submit DateTime'] >= start_date) & (completed_df['Submit DateTime'] <= end_date)
            completed_filtered = completed_df[mask]

            if not completed_filtered.empty:
                all_jobs.append(completed_filtered)

        # Get running jobs if requested
        if include_running:
            running_df = self.get_running_jobs_detailed()
            if not running_df.empty:
                running_df['Submit DateTime'] = pd.to_datetime(running_df['Submit Time'], errors='coerce')
                running_df = running_df.dropna(subset=['Submit DateTime'])

                # Filter running jobs by date range
                mask = (running_df['Submit DateTime'] >= start_date) & (running_df['Submit DateTime'] <= end_date)
                running_filtered = running_df[mask]

                if not running_filtered.empty:
                    all_jobs.append(running_filtered)

        # Combine all jobs
        if all_jobs:
            combined_df = pd.concat(all_jobs, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()

    def calculate_emr_costs_for_date_range(self, start_date, end_date, include_running=True):
        """Calculate EMR costs for jobs within a date range"""
        jobs_df = self.get_jobs_by_date_range(start_date, end_date, include_running)

        if jobs_df.empty:
            return {
                'total_cost': 0,
                'job_count': 0,
                'cost_breakdown': {},
                'jobs_with_costs': pd.DataFrame()
            }

        # Calculate costs for each job
        jobs_with_costs = self.calculate_job_costs(jobs_df)

        # Calculate summary statistics
        total_cost = jobs_with_costs['Total Cost'].sum()
        job_count = len(jobs_with_costs)

        # Cost breakdown
        cost_breakdown = {
            'EMR Service Cost': jobs_with_costs['EMR Service Cost'].sum(),
            'EC2 Compute Cost': jobs_with_costs['EC2 Compute Cost'].sum(),
            'Memory Cost': jobs_with_costs['Memory Cost'].sum(),
            'Total vCore-Hours': jobs_with_costs['vCore-Hours'].sum(),
            'Total Memory-Hours': jobs_with_costs['Memory-Hours'].sum(),
            'Average Cost per Job': total_cost / job_count if job_count > 0 else 0
        }

        # Round breakdown values
        for key, value in cost_breakdown.items():
            if isinstance(value, (int, float)):
                cost_breakdown[key] = round(value, 4)

        return {
            'total_cost': round(total_cost, 4),
            'job_count': job_count,
            'cost_breakdown': cost_breakdown,
            'jobs_with_costs': jobs_with_costs
        }

    def get_daily_cost_summary(self, start_date, end_date):
        """Get daily cost breakdown for the date range"""
        jobs_df = self.get_jobs_by_date_range(start_date, end_date)

        if jobs_df.empty:
            return pd.DataFrame()

        jobs_with_costs = self.calculate_job_costs(jobs_df)

        # Extract date from Submit Time
        jobs_with_costs['Submit Date'] = pd.to_datetime(jobs_with_costs['Submit Time']).dt.date

        # Group by date
        daily_summary = jobs_with_costs.groupby('Submit Date').agg({
            'Job ID': 'count',
            'Total Cost': 'sum',
            'EMR Service Cost': 'sum',
            'EC2 Compute Cost': 'sum',
            'Memory Cost': 'sum',
            'vCore-Hours': 'sum',
            'Memory-Hours': 'sum',
            'User': lambda x: len(x.unique())
        }).round(4)

        # Rename columns
        daily_summary.columns = [
            'Job Count', 'Total Cost', 'EMR Service Cost', 'EC2 Compute Cost',
            'Memory Cost', 'vCore-Hours', 'Memory-Hours', 'Unique Users'
        ]

        return daily_summary.reset_index()


# Usage example
if __name__ == "__main__":
    # Initialize the monitor with your EMR endpoints
    monitor = JobResourceMonitor(
        spark_history_url="http://your-spark-history-server:18080",
        yarn_rm_url="http://your-yarn-rm:8088",
        emr_pricing={
            'emr_charge_per_hour': 0.096,  # Update based on your region
            'ec2_cost_per_vcpu_hour': 0.0464,  # Update based on instance types
            'memory_cost_per_gb_hour': 0.0050,  # Approximate memory cost
        }
    )

    # Calculate costs for a date range
    start_date = "2024-01-01"
    end_date = "2024-01-07"

    cost_analysis = monitor.calculate_emr_costs_for_date_range(start_date, end_date)

    print(f"EMR Cost Analysis for {start_date} to {end_date}")
    print(f"Total Cost: ${cost_analysis['total_cost']:.2f}")
    print(f"Number of Jobs: {cost_analysis['job_count']}")
    print("\nCost Breakdown:")
    for key, value in cost_analysis['cost_breakdown'].items():
        print(f"  {key}: ${value:.2f}" if isinstance(value, (int, float)) else f"  {key}: {value}")

    # Get daily cost summary
    daily_costs = monitor.get_daily_cost_summary(start_date, end_date)
    print("\nDaily Cost Summary:")
    print(daily_costs.to_string(index=False))

    # Get detailed jobs with costs
    jobs_with_costs = cost_analysis['jobs_with_costs']
    print(f"\nTop 5 Most Expensive Jobs:")
    if not jobs_with_costs.empty:
        top_jobs = jobs_with_costs.nlargest(5, 'Total Cost')[
            ['Job Name', 'User', 'Total Cost', 'vCore-Hours', 'Memory-Hours']]
        print(top_jobs.to_string(index=False))