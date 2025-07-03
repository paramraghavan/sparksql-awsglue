import streamlit as st
import boto3
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import json
from urllib.parse import urljoin

# Configure page
st.set_page_config(
    page_title="EMR Spark Resource Monitor",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ EMR Spark Job Resource Monitor")
st.markdown("Monitor Spark job resources on EMR clusters - tabulated view")

# Sidebar configuration
st.sidebar.header("Configuration")

# AWS Configuration
aws_region = st.sidebar.text_input("AWS Region", value="us-east-1")

# Cluster ID input methods
st.sidebar.subheader("Cluster Identification")
input_method = st.sidebar.radio(
    "How to identify cluster:",
    ["Manual Entry", "From Spark History URL", "From YARN URL"]
)

cluster_id = None
spark_history_server = None
yarn_rm_url = None

if input_method == "Manual Entry":
    cluster_id = st.sidebar.text_input("EMR Cluster ID", placeholder="j-XXXXXXXXXX")
    # Optional URLs
    spark_history_server = st.sidebar.text_input(
        "Spark History Server URL (optional)",
        placeholder="http://your-cluster-master:18080"
    )
    yarn_rm_url = st.sidebar.text_input(
        "YARN ResourceManager URL (optional)",
        placeholder="http://your-cluster-master:8088"
    )

elif input_method == "From Spark History URL":
    spark_history_server = st.sidebar.text_input(
        "Spark History Server URL",
        placeholder="http://your-cluster-master:18080"
    )
    if spark_history_server:
        extracted_cluster_id = extract_cluster_id_from_url(spark_history_server)
        if extracted_cluster_id:
            cluster_id = extracted_cluster_id
            st.sidebar.success(f"✅ Detected Cluster ID: {cluster_id}")
        else:
            st.sidebar.error("❌ Could not extract cluster ID from URL")

    # Optional YARN URL
    yarn_rm_url = st.sidebar.text_input(
        "YARN ResourceManager URL (optional)",
        placeholder="http://your-cluster-master:8088"
    )

else:  # From YARN URL
    yarn_rm_url = st.sidebar.text_input(
        "YARN ResourceManager URL",
        placeholder="http://your-cluster-master:8088"
    )
    if yarn_rm_url:
        extracted_cluster_id = extract_cluster_id_from_url(yarn_rm_url)
        if extracted_cluster_id:
            cluster_id = extracted_cluster_id
            st.sidebar.success(f"✅ Detected Cluster ID: {cluster_id}")
        else:
            st.sidebar.error("❌ Could not extract cluster ID from URL")

    # Optional Spark History URL
    spark_history_server = st.sidebar.text_input(
        "Spark History Server URL (optional)",
        placeholder="http://your-cluster-master:18080"
    )

# Filters
st.sidebar.header("Filters")
job_name_filter = st.sidebar.text_input("Job Name Filter")
user_filter = st.sidebar.text_input("User Filter")
job_status = st.sidebar.selectbox(
    "Job Status",
    ["All", "RUNNING", "SUCCEEDED", "FAILED", "KILLED", "COMPLETED"]
)

# Time range
time_range = st.sidebar.selectbox(
    "Time Range",
    ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"]
)

# Display options
st.sidebar.header("Display Options")
show_completed = st.sidebar.checkbox("Show Completed Jobs", value=True)
show_failed = st.sidebar.checkbox("Show Failed Jobs", value=True)
max_jobs = st.sidebar.slider("Max Jobs to Display", 10, 100, 50)

# Auto refresh
auto_refresh = st.sidebar.checkbox("Auto Refresh", value=False)
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 10, 300, 30)


# Initialize AWS clients
@st.cache_resource
def get_aws_clients(region):
    try:
        emr_client = boto3.client('emr', region_name=region)
        cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        return emr_client, cloudwatch_client
    except Exception as e:
        st.error(f"Error connecting to AWS: {str(e)}")
        return None, None


def extract_cluster_id_from_url(url):
    """Extract EMR cluster ID from Spark History Server or YARN ResourceManager URL"""
    try:
        # Method 1: Try to get cluster info from Spark History Server
        if ':18080' in url:  # Spark History Server
            try:
                # Get environment info from Spark History Server
                env_url = urljoin(url, "/api/v1/applications")
                response = requests.get(env_url, timeout=10)

                if response.status_code == 200:
                    apps = response.json()
                    if apps:
                        # Get the first application's environment info
                        app_id = apps[0]['id']
                        env_detail_url = urljoin(url, f"/api/v1/applications/{app_id}/environment")
                        env_response = requests.get(env_detail_url, timeout=5)

                        if env_response.status_code == 200:
                            env_data = env_response.json()
                            # Look for EMR cluster ID in environment variables
                            for category in env_data:
                                if category.get('name') == 'System Properties':
                                    for prop in category.get('values', []):
                                        if 'aws.emr.job.flow.id' in prop[0]:
                                            return prop[1]
                                        elif 'CLUSTER_ID' in prop[0]:
                                            return prop[1]
            except Exception as e:
                st.warning(f"Could not extract from Spark API: {str(e)}")

        # Method 2: Try to get cluster info from YARN ResourceManager
        elif ':8088' in url:  # YARN ResourceManager
            try:
                cluster_info_url = urljoin(url, "/ws/v1/cluster/info")
                response = requests.get(cluster_info_url, timeout=10)

                if response.status_code == 200:
                    cluster_data = response.json()
                    cluster_info = cluster_data.get('clusterInfo', {})

                    # Look for EMR cluster ID in various fields
                    cluster_id_candidates = [
                        cluster_info.get('haZooKeeperConnectionString', ''),
                        cluster_info.get('resourceManagerVersion', ''),
                        cluster_info.get('hadoopVersion', '')
                    ]

                    for candidate in cluster_id_candidates:
                        if candidate and 'j-' in candidate:
                            # Extract j-XXXXXXXXXX pattern
                            import re
                            match = re.search(r'j-[A-Z0-9]{8,}', candidate)
                            if match:
                                return match.group(0)
            except Exception as e:
                st.warning(f"Could not extract from YARN API: {str(e)}")

        # Method 3: Try to extract from hostname pattern
        try:
            from urllib.parse import urlparse
            parsed_url = urlparse(url)
            hostname = parsed_url.hostname

            if hostname:
                # EMR hostnames often contain cluster ID
                # Pattern: ip-172-31-xx-xx.region.compute.internal or ec2-xx-xx-xx-xx.region.compute.amazonaws.com

                # Try to get instance metadata if it's an EC2 instance
                if 'compute.internal' in hostname or 'compute.amazonaws.com' in hostname:
                    # This would require running from within the cluster
                    # We'll try a different approach - check EMR tags
                    pass
        except Exception as e:
            pass

        # Method 4: Manual extraction attempt from URL structure
        # Some EMR setups include cluster ID in the URL path or subdomain
        try:
            import re
            # Look for j-XXXXXXXXXX pattern anywhere in the URL
            match = re.search(r'j-[A-Z0-9]{8,}', url)
            if match:
                return match.group(0)
        except Exception as e:
            pass

        return None

    except Exception as e:
        st.error(f"Error extracting cluster ID: {str(e)}")
        return None


def get_cluster_id_from_instance_metadata(url):
    """Try to get cluster ID by connecting to the instance and querying metadata"""
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        # Try to access instance metadata through a proxy endpoint (if available)
        # This is a fallback method that might work in some setups
        metadata_endpoints = [
            "/proxy/application_metadata",
            "/metadata/cluster",
            "/emr/cluster-id"
        ]

        for endpoint in metadata_endpoints:
            try:
                metadata_url = urljoin(base_url, endpoint)
                response = requests.get(metadata_url, timeout=5)
                if response.status_code == 200:
                    data = response.text
                    import re
                    match = re.search(r'j-[A-Z0-9]{8,}', data)
                    if match:
                        return match.group(0)
            except:
                continue

        return None
    except Exception as e:
        return None


def discover_cluster_id_interactive(url):
    """Interactive method to help user find cluster ID"""
    st.sidebar.markdown("### 🔍 Cluster ID Discovery")

    # Show what we tried
    st.sidebar.info("""
    **Automatic detection methods tried:**
    1. ✅ Spark History Server API
    2. ✅ YARN ResourceManager API  
    3. ✅ URL pattern matching
    """)

    # Manual input as fallback
    manual_cluster_id = st.sidebar.text_input(
        "Enter Cluster ID manually",
        placeholder="j-XXXXXXXXXX",
        help="If auto-detection failed, please enter your EMR cluster ID"
    )

    if manual_cluster_id:
        return manual_cluster_id

    # Show instructions for finding cluster ID
    with st.sidebar.expander("📝 How to find your Cluster ID"):
        st.markdown("""
        **Method 1: AWS Console**
        1. Go to EMR console
        2. Find your cluster name
        3. Copy the Cluster ID (j-XXXXXXXXXX)

        **Method 2: AWS CLI**
        ```bash
        aws emr list-clusters --active
        ```

        **Method 3: From EMR Master Node**
        ```bash
        cat /mnt/var/lib/info/job-flow.json | grep jobFlowId
        ```

        **Method 4: Check instance tags**
        Look for 'aws:elasticmapreduce:job-flow-id' tag
        """)

    return None
    """Convert time range selection to datetime objects"""
    end_time = datetime.utcnow()
    if time_range == "Last 1 Hour":
        start_time = end_time - timedelta(hours=1)
    elif time_range == "Last 6 Hours":
        start_time = end_time - timedelta(hours=6)
    elif time_range == "Last 24 Hours":
        start_time = end_time - timedelta(hours=24)
    else:  # Last 7 Days
        start_time = end_time - timedelta(days=7)

    return start_time, end_time


def get_cluster_info(emr_client, cluster_id):
    """Get cluster information"""
    try:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster = response['Cluster']

        # Get instance groups for total instance count
        instance_groups = emr_client.list_instance_groups(ClusterId=cluster_id)
        total_instances = sum([ig['RequestedInstanceCount'] for ig in instance_groups['InstanceGroups']])

        return {
            'Cluster Name': cluster['Name'],
            'Status': cluster['Status']['State'],
            'Master Instance Type': cluster['Ec2InstanceAttributes']['Ec2InstanceType'],
            'Total Instances': total_instances,
            'Created': cluster['Status']['Timeline']['CreationDateTime'],
            'Region': cluster['Ec2InstanceAttributes']['Ec2AvailabilityZone']
        }
    except Exception as e:
        st.error(f"Error fetching cluster info: {str(e)}")
        return None


def get_spark_applications_from_history_server(history_server_url):
    """Fetch Spark applications from History Server API"""
    try:
        apps_url = urljoin(history_server_url, "/api/v1/applications")
        response = requests.get(apps_url, timeout=10)

        if response.status_code == 200:
            applications = response.json()

            spark_jobs = []
            for app in applications:
                app_id = app['id']
                app_detail_url = urljoin(history_server_url, f"/api/v1/applications/{app_id}")

                try:
                    detail_response = requests.get(app_detail_url, timeout=5)
                    if detail_response.status_code == 200:
                        app_detail = detail_response.json()[0]

                        # Get executors info
                        executors_url = urljoin(history_server_url, f"/api/v1/applications/{app_id}/executors")
                        exec_response = requests.get(executors_url, timeout=5)
                        executors = exec_response.json() if exec_response.status_code == 200 else []

                        # Calculate resource usage
                        total_cores = sum([exec.get('totalCores', 0) for exec in executors])
                        total_memory_bytes = sum([exec.get('maxMemory', 0) for exec in executors])
                        total_memory_mb = total_memory_bytes / (1024 * 1024)

                        # Calculate duration
                        start_time = pd.to_datetime(app_detail.get('startTime', ''), format='%Y-%m-%dT%H:%M:%S.%fZ',
                                                    errors='coerce')
                        end_time = pd.to_datetime(app_detail.get('endTime', ''), format='%Y-%m-%dT%H:%M:%S.%fZ',
                                                  errors='coerce') if app_detail.get('endTime') else None

                        duration_ms = app_detail.get('duration', 0)
                        duration_min = duration_ms / (1000 * 60) if duration_ms else 0

                        job_info = {
                            'Source': 'Spark History',
                            'Application ID': app_id,
                            'Job Name': app_detail.get('name', 'Unknown'),
                            'User': app_detail.get('sparkUser', 'Unknown'),
                            'Status': app_detail.get('status', 'Unknown'),
                            'Start Time': start_time.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(start_time) else 'N/A',
                            'End Time': end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time and pd.notna(
                                end_time) else 'Running' if app_detail.get('status') == 'RUNNING' else 'N/A',
                            'Duration (min)': f"{duration_min:.1f}",
                            'Total Cores': total_cores,
                            'Total Memory (MB)': f"{total_memory_mb:.0f}",
                            'Executors': len([e for e in executors if e.get('id') != 'driver']),
                            'Tasks Total': sum([exec.get('totalTasks', 0) for exec in executors]),
                            'Tasks Failed': sum([exec.get('failedTasks', 0) for exec in executors]),
                            'Core-Hours': f"{(duration_min / 60 * total_cores):.2f}" if duration_min > 0 and total_cores > 0 else "0.00"
                        }
                        spark_jobs.append(job_info)
                except Exception as e:
                    continue

            return spark_jobs
        else:
            st.error(f"Failed to fetch from Spark History Server: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Error connecting to Spark History Server: {str(e)}")
        return []


def get_yarn_applications(yarn_url):
    """Fetch applications from YARN ResourceManager"""
    try:
        apps_url = urljoin(yarn_url, "/ws/v1/cluster/apps")
        response = requests.get(apps_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            applications = data.get('apps', {}).get('app', [])

            yarn_apps = []
            for app in applications:
                if app.get('applicationType') == 'SPARK':
                    start_time = pd.to_datetime(app.get('startedTime', 0), unit='ms', errors='coerce')
                    finish_time = pd.to_datetime(app.get('finishedTime', 0), unit='ms', errors='coerce') if app.get(
                        'finishedTime', 0) > 0 else None

                    elapsed_ms = app.get('elapsedTime', 0)
                    elapsed_min = elapsed_ms / (1000 * 60)

                    allocated_mb = app.get('allocatedMB', 0)
                    allocated_cores = app.get('allocatedVCores', 0)

                    app_info = {
                        'Source': 'YARN',
                        'Application ID': app.get('id', ''),
                        'Job Name': app.get('name', ''),
                        'User': app.get('user', ''),
                        'Status': app.get('state', ''),
                        'Final Status': app.get('finalStatus', ''),
                        'Start Time': start_time.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(start_time) else 'N/A',
                        'End Time': finish_time.strftime('%Y-%m-%d %H:%M:%S') if finish_time and pd.notna(
                            finish_time) else 'Running' if app.get('state') == 'RUNNING' else 'N/A',
                        'Duration (min)': f"{elapsed_min:.1f}",
                        'Allocated Memory (MB)': allocated_mb,
                        'Allocated VCores': allocated_cores,
                        'Running Containers': app.get('runningContainers', 0),
                        'Queue': app.get('queue', ''),
                        'Progress (%)': f"{app.get('progress', 0):.1f}",
                        'Core-Hours': f"{(elapsed_min / 60 * allocated_cores):.2f}" if elapsed_min > 0 and allocated_cores > 0 else "0.00"
                    }
                    yarn_apps.append(app_info)

            return yarn_apps
        else:
            st.error(f"Failed to fetch from YARN: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Error connecting to YARN ResourceManager: {str(e)}")
        return []


def get_emr_steps_spark_jobs(emr_client, cluster_id):
    """Get Spark jobs from EMR steps"""
    try:
        response = emr_client.list_steps(
            ClusterId=cluster_id,
            StepStates=['PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED']
        )

        spark_steps = []
        for step in response['Steps']:
            step_detail = emr_client.describe_step(
                ClusterId=cluster_id,
                StepId=step['Id']
            )

            step_info = step_detail['Step']
            hadoop_jar_step = step_info.get('HadoopJarStep', {})

            # Check if this is a Spark step
            jar = hadoop_jar_step.get('Jar', '')
            args = hadoop_jar_step.get('Args', [])

            if 'spark' in jar.lower() or any('spark' in str(arg).lower() for arg in args):
                created = step['Status']['Timeline'].get('CreationDateTime')
                started = step['Status']['Timeline'].get('StartDateTime')
                ended = step['Status']['Timeline'].get('EndDateTime')

                # Calculate duration
                duration_min = 0
                if started and ended:
                    duration = ended - started
                    duration_min = duration.total_seconds() / 60
                elif started:
                    duration = datetime.utcnow().replace(tzinfo=started.tzinfo) - started
                    duration_min = duration.total_seconds() / 60

                spark_job = {
                    'Source': 'EMR Steps',
                    'Step ID': step['Id'],
                    'Job Name': step['Name'],
                    'Status': step['Status']['State'],
                    'Created': created.strftime('%Y-%m-%d %H:%M:%S') if created else 'N/A',
                    'Started': started.strftime('%Y-%m-%d %H:%M:%S') if started else 'N/A',
                    'Ended': ended.strftime('%Y-%m-%d %H:%M:%S') if ended else 'Running' if step['Status'][
                                                                                                'State'] == 'RUNNING' else 'N/A',
                    'Duration (min)': f"{duration_min:.1f}",
                    'JAR': jar,
                    'Action On Failure': step_info.get('ActionOnFailure', 'N/A')
                }

                spark_steps.append(spark_job)

        return spark_steps
    except Exception as e:
        st.error(f"Error fetching EMR steps: {str(e)}")
        return []


def get_cluster_resource_summary(cloudwatch_client, cluster_id, start_time, end_time):
    """Get current cluster resource summary"""
    try:
        metrics_summary = {}

        metric_queries = [
            ('CPUUtilization', 'CPU Utilization (%)'),
            ('MemoryPercentage', 'Memory Usage (%)'),
            ('AppsRunning', 'Running Applications'),
            ('AppsPending', 'Pending Applications'),
            ('ContainersAllocated', 'Allocated Containers'),
            ('ContainersPending', 'Pending Containers')
        ]

        for metric_name, display_name in metric_queries:
            try:
                response = cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/ElasticMapReduce',
                    MetricName=metric_name,
                    Dimensions=[{'Name': 'JobFlowId', 'Value': cluster_id}],
                    StartTime=end_time - timedelta(minutes=15),  # Last 15 minutes
                    EndTime=end_time,
                    Period=300,
                    Statistics=['Average']
                )

                if response['Datapoints']:
                    latest_value = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])[-1]['Average']
                    if metric_name in ['CPUUtilization', 'MemoryPercentage']:
                        metrics_summary[display_name] = f"{latest_value:.1f}%"
                    else:
                        metrics_summary[display_name] = f"{int(latest_value)}"
                else:
                    metrics_summary[display_name] = "N/A"
            except:
                metrics_summary[display_name] = "N/A"

        return metrics_summary
    except Exception as e:
        st.error(f"Error fetching cluster metrics: {str(e)}")
        return {}


def apply_filters(jobs, job_name_filter, user_filter, job_status, show_completed, show_failed):
    """Apply filters to job list"""
    filtered_jobs = jobs.copy()

    # Status filter
    if not show_completed:
        filtered_jobs = [job for job in filtered_jobs
                         if not any(status in job.get('Status', job.get('State', '')).upper()
                                    for status in ['COMPLETED', 'SUCCEEDED', 'FINISHED'])]

    if not show_failed:
        filtered_jobs = [job for job in filtered_jobs
                         if not any(status in job.get('Status', job.get('State', '')).upper()
                                    for status in ['FAILED', 'KILLED', 'ERROR'])]

    # Name filter
    if job_name_filter:
        filtered_jobs = [job for job in filtered_jobs
                         if job_name_filter.lower() in job.get('Job Name', job.get('Name', '')).lower()]

    # User filter
    if user_filter:
        filtered_jobs = [job for job in filtered_jobs
                         if user_filter.lower() in job.get('User', '').lower()]

    # Status filter
    if job_status != "All":
        filtered_jobs = [job for job in filtered_jobs
                         if job_status.upper() in job.get('Status', job.get('State', '')).upper()]

    return filtered_jobs


# Main application
if not cluster_id:
    st.warning("⚠️ Please provide cluster identification information")

    # Show current input method and provide guidance
    if input_method == "From Spark History URL" and spark_history_server:
        st.info("🔍 Attempting to auto-detect cluster ID from Spark History Server...")
        if st.button("🔄 Retry Auto-Detection"):
            st.rerun()

        # Show discovery helper
        manual_id = discover_cluster_id_interactive(spark_history_server)
        if manual_id:
            cluster_id = manual_id

    elif input_method == "From YARN URL" and yarn_rm_url:
        st.info("🔍 Attempting to auto-detect cluster ID from YARN ResourceManager...")
        if st.button("🔄 Retry Auto-Detection"):
            st.rerun()

        # Show discovery helper
        manual_id = discover_cluster_id_interactive(yarn_rm_url)
        if manual_id:
            cluster_id = manual_id

    if not cluster_id:
        st.markdown("""
        ## 📋 Setup Instructions

        ### **Option 1: Manual Entry**
        - Enter your EMR Cluster ID directly (format: j-XXXXXXXXXX)
        - Find it in AWS EMR Console or use: `aws emr list-clusters --active`

        ### **Option 2: Auto-detect from Spark History Server**
        - Enter Spark History Server URL (usually port 18080)
        - Format: `http://ip-172-31-xx-xx.region.compute.internal:18080`
        - We'll try to extract cluster ID automatically

        ### **Option 3: Auto-detect from YARN ResourceManager**
        - Enter YARN ResourceManager URL (usually port 8088)
        - Format: `http://ip-172-31-xx-xx.region.compute.internal:8088`
        - We'll try to extract cluster ID automatically

        ### 🔧 **Required AWS Permissions:**
        ```json
        {
            "emr:DescribeCluster",
            "emr:ListSteps", 
            "emr:DescribeStep",
            "cloudwatch:GetMetricStatistics"
        }
        ```

        ### 📊 **Features:**
        - **Real-time monitoring** of Spark jobs on EMR
        - **Resource tracking**: cores, memory, executors, tasks
        - **Multiple data sources**: EMR Steps, Spark History, YARN
        - **Advanced filtering** by job name, user, status
        - **Tabulated view** for easy data analysis
        - **CSV export** for further analysis
        """)

else:
    # Initialize AWS clients
    emr_client, cloudwatch_client = get_aws_clients(aws_region)

    if emr_client and cloudwatch_client:
        # Get time range
        start_time, end_time = get_time_range_dates(time_range)

        # Header with cluster info
        st.subheader("📊 Cluster Overview")

        cluster_info = get_cluster_info(emr_client, cluster_id)
        if cluster_info:
            # Display cluster info in columns
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Cluster Status", cluster_info['Status'])
            with col2:
                st.metric("Total Instances", cluster_info['Total Instances'])
            with col3:
                st.metric("Master Type", cluster_info['Master Instance Type'])
            with col4:
                st.metric("Region", cluster_info['Region'])

            # Get current resource metrics
            if cloudwatch_client:
                resource_summary = get_cluster_resource_summary(cloudwatch_client, cluster_id, start_time, end_time)
                if resource_summary:
                    st.subheader("🔧 Current Resource Usage")

                    # Display resource metrics in a table
                    resource_df = pd.DataFrame(list(resource_summary.items()),
                                               columns=['Metric', 'Current Value'])

                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.dataframe(resource_df, hide_index=True, use_container_width=True)

        # Collect all Spark jobs
        st.subheader("⚡ Spark Jobs")

        all_jobs = []
        data_sources = []

        # EMR Steps
        with st.spinner("Fetching EMR Steps..."):
            emr_jobs = get_emr_steps_spark_jobs(emr_client, cluster_id)
            all_jobs.extend(emr_jobs)
            if emr_jobs:
                data_sources.append(f"EMR Steps ({len(emr_jobs)} jobs)")

        # Spark History Server
        if spark_history_server:
            with st.spinner("Fetching Spark History..."):
                spark_jobs = get_spark_applications_from_history_server(spark_history_server)
                all_jobs.extend(spark_jobs)
                if spark_jobs:
                    data_sources.append(f"Spark History ({len(spark_jobs)} jobs)")

        # YARN Applications
        if yarn_rm_url:
            with st.spinner("Fetching YARN Applications..."):
                yarn_jobs = get_yarn_applications(yarn_rm_url)
                all_jobs.extend(yarn_jobs)
                if yarn_jobs:
                    data_sources.append(f"YARN ({len(yarn_jobs)} jobs)")

        # Display data sources
        if data_sources:
            st.info(f"📊 **Data Sources**: {' | '.join(data_sources)}")

        # Apply filters
        filtered_jobs = apply_filters(all_jobs, job_name_filter, user_filter, job_status, show_completed, show_failed)

        # Limit number of jobs displayed
        if len(filtered_jobs) > max_jobs:
            filtered_jobs = filtered_jobs[:max_jobs]
            st.warning(f"Showing first {max_jobs} jobs. Adjust 'Max Jobs to Display' in sidebar to see more.")

        if filtered_jobs:
            # Summary statistics
            st.subheader("📈 Summary Statistics")

            total_jobs = len(filtered_jobs)
            running_jobs = len([j for j in filtered_jobs
                                if 'RUNNING' in j.get('Status', j.get('State', '')).upper()])

            # Calculate totals where available
            total_cores = 0
            total_memory = 0
            total_core_hours = 0

            for job in filtered_jobs:
                cores = job.get('Total Cores', job.get('Allocated VCores', 0))
                memory = job.get('Total Memory (MB)', job.get('Allocated Memory (MB)', 0))
                core_hours = job.get('Core-Hours', '0.00')

                if isinstance(cores, (int, float)):
                    total_cores += cores
                if isinstance(memory, (int, float)):
                    total_memory += memory
                try:
                    total_core_hours += float(str(core_hours).replace(',', ''))
                except:
                    pass

            # Display summary in columns
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                st.metric("Total Jobs", total_jobs)
            with col2:
                st.metric("Running Jobs", running_jobs)
            with col3:
                st.metric("Total Cores", f"{total_cores:,}")
            with col4:
                st.metric("Total Memory (GB)", f"{total_memory / 1024:,.1f}")
            with col5:
                st.metric("Total Core-Hours", f"{total_core_hours:,.2f}")

            # Jobs table
            st.subheader("📋 Job Details")

            df_jobs = pd.DataFrame(filtered_jobs)

            # Configure column display based on available data
            priority_columns = ['Source', 'Job Name', 'User', 'Status', 'Start Time', 'Duration (min)',
                                'Total Cores', 'Allocated VCores', 'Total Memory (MB)', 'Allocated Memory (MB)',
                                'Core-Hours']

            display_columns = [col for col in priority_columns if col in df_jobs.columns]

            # Add any remaining columns not in priority list
            remaining_columns = [col for col in df_jobs.columns if col not in display_columns]
            display_columns.extend(remaining_columns)

            # Display the table
            st.dataframe(df_jobs[display_columns], use_container_width=True, hide_index=True)

            # Resource usage by user
            if len(filtered_jobs) > 1:
                st.subheader("👥 Resource Usage by User")

                user_stats = {}
                for job in filtered_jobs:
                    user = job.get('User', 'Unknown')
                    if user not in user_stats:
                        user_stats[user] = {
                            'Jobs': 0,
                            'Total Cores': 0,
                            'Total Memory (MB)': 0,
                            'Running Jobs': 0,
                            'Core-Hours': 0.0
                        }

                    user_stats[user]['Jobs'] += 1

                    # Add cores
                    cores = job.get('Total Cores', job.get('Allocated VCores', 0))
                    if isinstance(cores, (int, float)):
                        user_stats[user]['Total Cores'] += cores

                    # Add memory
                    memory = job.get('Total Memory (MB)', job.get('Allocated Memory (MB)', 0))
                    if isinstance(memory, (int, float)):
                        user_stats[user]['Total Memory (MB)'] += memory

                    # Count running jobs
                    if 'RUNNING' in job.get('Status', job.get('State', '')).upper():
                        user_stats[user]['Running Jobs'] += 1

                    # Add core-hours
                    try:
                        core_hours = float(str(job.get('Core-Hours', '0.00')).replace(',', ''))
                        user_stats[user]['Core-Hours'] += core_hours
                    except:
                        pass

                user_df = pd.DataFrame.from_dict(user_stats, orient='index')
                user_df.index.name = 'User'
                user_df = user_df.reset_index()

                # Format memory as GB
                user_df['Total Memory (GB)'] = (user_df['Total Memory (MB)'] / 1024).round(1)
                user_df = user_df.drop('Total Memory (MB)', axis=1)

                # Round core-hours
                user_df['Core-Hours'] = user_df['Core-Hours'].round(2)

                st.dataframe(user_df, use_container_width=True, hide_index=True)

            # Export option
            st.subheader("💾 Export Data")

            if st.button("📄 Download Job Data as CSV"):
                csv = df_jobs.to_csv(index=False)
                st.download_button(
                    label="💾 Download CSV",
                    data=csv,
                    file_name=f"emr_spark_jobs_{cluster_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

        else:
            st.info("ℹ️ No Spark jobs found matching the current filters")

            # Show what filters are active
            active_filters = []
            if job_name_filter:
                active_filters.append(f"Job Name: '{job_name_filter}'")
            if user_filter:
                active_filters.append(f"User: '{user_filter}'")
            if job_status != "All":
                active_filters.append(f"Status: {job_status}")
            if not show_completed:
                active_filters.append("Hiding completed jobs")
            if not show_failed:
                active_filters.append("Hiding failed jobs")

            if active_filters:
                st.write("**Active filters:**")
                for filter_item in active_filters:
                    st.write(f"- {filter_item}")

# Auto refresh
if auto_refresh and cluster_id:
    time.sleep(refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
st.markdown("""
### 💡 **Tips:**
- **Core-Hours** = Duration × Cores (useful for cost estimation)
- Use **filters** to focus on specific users or job types
- **Export CSV** for further analysis in Excel/other tools
- Enable **auto-refresh** for real-time monitoring
- Connect **Spark History Server** for detailed executor metrics
""")