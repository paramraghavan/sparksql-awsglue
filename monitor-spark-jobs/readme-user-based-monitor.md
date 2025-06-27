Yes, there are several excellent tools and approaches to monitor Spark job resource usage on your shared EMR cluster.
Here are the best options:

## 1. Spark History Server + Custom Monitoring

### Access Built-in Spark UI

Your EMR cluster already has these UIs running:

- **Spark History Server**: `http://master-node:18080`
- **YARN Resource Manager**: `http://master-node:8088`
- **Ganglia**: `http://master-node/ganglia`

### Python Library to Query Spark History Server

```python
import requests
import pandas as pd
from datetime import datetime
import json


class SparkJobMonitor:
    def __init__(self, spark_history_server_url):
        self.base_url = spark_history_server_url

    def get_all_applications(self):
        """Get all Spark applications"""
        url = f"{self.base_url}/api/v1/applications"
        response = requests.get(url)
        return response.json()

    def get_application_details(self, app_id):
        """Get detailed metrics for a specific application"""
        url = f"{self.base_url}/api/v1/applications/{app_id}"
        response = requests.get(url)
        return response.json()

    def get_executors(self, app_id):
        """Get executor information"""
        url = f"{self.base_url}/api/v1/applications/{app_id}/executors"
        response = requests.get(url)
        return response.json()

    def get_stages(self, app_id):
        """Get stage information"""
        url = f"{self.base_url}/api/v1/applications/{app_id}/stages"
        response = requests.get(url)
        return response.json()

    def get_resource_usage_summary(self):
        """Get resource usage summary for all jobs"""
        apps = self.get_all_applications()

        summary = []
        for app in apps:
            app_id = app['id']
            try:
                executors = self.get_executors(app_id)

                total_cores = sum(ex.get('totalCores', 0) for ex in executors)
                total_memory = sum(ex.get('maxMemory', 0) for ex in executors)

                summary.append({
                    'app_id': app_id,
                    'app_name': app.get('name', 'Unknown'),
                    'user': app.get('sparkUser', 'Unknown'),
                    'start_time': app.get('attempts', [{}])[0].get('startTime', ''),
                    'duration': app.get('attempts', [{}])[0].get('duration', 0),
                    'total_cores': total_cores,
                    'total_memory_mb': total_memory // (1024 * 1024),
                    'status': app.get('attempts', [{}])[0].get('completed', False)
                })
            except:
                continue

        return pd.DataFrame(summary)


# Usage
monitor = SparkJobMonitor("http://your-master-node:18080")
df = monitor.get_resource_usage_summary()
print(df.head())
```

## 2. YARN Resource Manager API Monitoring

```python
import requests
import time
from datetime import datetime


class YARNResourceMonitor:
    def __init__(self, yarn_rm_url):
        self.base_url = yarn_rm_url

    def get_cluster_metrics(self):
        """Get overall cluster resource metrics"""
        url = f"{self.base_url}/ws/v1/cluster/metrics"
        response = requests.get(url)
        return response.json()['clusterMetrics']

    def get_running_applications(self):
        """Get currently running applications"""
        url = f"{self.base_url}/ws/v1/cluster/apps"
        params = {'states': 'RUNNING'}
        response = requests.get(url, params=params)
        return response.json().get('apps', {}).get('app', [])

    def get_resource_usage_by_user(self):
        """Get resource usage breakdown by user"""
        apps = self.get_running_applications()
        user_usage = {}

        for app in apps:
            user = app.get('user', 'unknown')
            if user not in user_usage:
                user_usage[user] = {
                    'allocated_mb': 0,
                    'allocated_vcores': 0,
                    'running_containers': 0,
                    'applications': []
                }

            user_usage[user]['allocated_mb'] += app.get('allocatedMB', 0)
            user_usage[user]['allocated_vcores'] += app.get('allocatedVCores', 0)
            user_usage[user]['running_containers'] += app.get('runningContainers', 0)
            user_usage[user]['applications'].append({
                'id': app.get('id'),
                'name': app.get('name'),
                'queue': app.get('queue')
            })

        return user_usage


# Usage
yarn_monitor = YARNResourceMonitor("http://your-master-node:8088")
cluster_metrics = yarn_monitor.get_cluster_metrics()
user_usage = yarn_monitor.get_resource_usage_by_user()

print(f"Cluster Memory: {cluster_metrics['totalMB']} MB")
print(f"Available Memory: {cluster_metrics['availableMB']} MB")
print(f"Memory Utilization: {(1 - cluster_metrics['availableMB'] / cluster_metrics['totalMB']) * 100:.1f}%")
```

## 3. Comprehensive Monitoring Dashboard

```python
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pandas as pd


class EMRClusterDashboard:
    def __init__(self, spark_history_url, yarn_rm_url):
        self.spark_monitor = SparkJobMonitor(spark_history_url)
        self.yarn_monitor = YARNResourceMonitor(yarn_rm_url)

    def create_dashboard(self):
        st.title("EMR Cluster Resource Monitor")

        # Real-time cluster metrics
        col1, col2, col3, col4 = st.columns(4)

        cluster_metrics = self.yarn_monitor.get_cluster_metrics()

        with col1:
            memory_util = (1 - cluster_metrics['availableMB'] / cluster_metrics['totalMB']) * 100
            st.metric("Memory Utilization", f"{memory_util:.1f}%")

        with col2:
            vcores_util = (1 - cluster_metrics['availableVirtualCores'] / cluster_metrics['totalVirtualCores']) * 100
            st.metric("vCores Utilization", f"{vcores_util:.1f}%")

        with col3:
            st.metric("Running Apps", cluster_metrics['appsRunning'])

        with col4:
            st.metric("Pending Apps", cluster_metrics['appsPending'])

        # User resource usage
        st.subheader("Resource Usage by User")
        user_usage = self.yarn_monitor.get_resource_usage_by_user()

        if user_usage:
            user_df = pd.DataFrame([
                {
                    'User': user,
                    'Memory (GB)': data['allocated_mb'] / 1024,
                    'vCores': data['allocated_vcores'],
                    'Containers': data['running_containers'],
                    'Applications': len(data['applications'])
                }
                for user, data in user_usage.items()
            ])

            fig = px.bar(user_df, x='User', y='Memory (GB)',
                         title="Memory Usage by User")
            st.plotly_chart(fig)

            fig2 = px.bar(user_df, x='User', y='vCores',
                          title="vCore Usage by User")
            st.plotly_chart(fig2)

            st.dataframe(user_df)

        # Job history and performance
        st.subheader("Recent Job Performance")
        job_summary = self.spark_monitor.get_resource_usage_summary()

        if not job_summary.empty:
            # Convert duration to minutes
            job_summary['duration_minutes'] = job_summary['duration'] / (1000 * 60)

            fig3 = px.scatter(job_summary, x='total_cores', y='duration_minutes',
                              color='user', size='total_memory_mb',
                              hover_data=['app_name'],
                              title="Job Duration vs Resources Used")
            st.plotly_chart(fig3)

            # Top resource consumers
            top_jobs = job_summary.nlargest(10, 'total_memory_mb')
            st.subheader("Top 10 Memory Consumers")
            st.dataframe(top_jobs[['app_name', 'user', 'total_cores', 'total_memory_mb', 'duration_minutes']])


# Run the dashboard
if __name__ == "__main__":
    dashboard = EMRClusterDashboard(
        "http://your-master-node:18080",
        "http://your-master-node:8088"
    )
    dashboard.create_dashboard()
```

## 4. Automated Alerting System

```python
import smtplib
from email.mime.text import MIMEText
import time
import logging


class ResourceAlertSystem:
    def __init__(self, yarn_monitor, thresholds):
        self.yarn_monitor = yarn_monitor
        self.thresholds = thresholds
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('cluster_alerts.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def check_resource_usage(self):
        """Check if resource usage exceeds thresholds"""
        metrics = self.yarn_monitor.get_cluster_metrics()

        memory_util = (1 - metrics['availableMB'] / metrics['totalMB']) * 100
        vcores_util = (1 - metrics['availableVirtualCores'] / metrics['totalVirtualCores']) * 100

        alerts = []

        if memory_util > self.thresholds['memory']:
            alerts.append(f"High memory usage: {memory_util:.1f}%")

        if vcores_util > self.thresholds['vcores']:
            alerts.append(f"High vCore usage: {vcores_util:.1f}%")

        if metrics['appsPending'] > self.thresholds['pending_apps']:
            alerts.append(f"High pending applications: {metrics['appsPending']}")

        return alerts

    def send_alert(self, alerts):
        """Send alert notifications"""
        if alerts:
            message = "EMR Cluster Resource Alert:\n\n" + "\n".join(alerts)
            self.logger.warning(message)
            # Add email/Slack notification logic here

    def monitor_continuously(self, interval_minutes=5):
        """Continuously monitor and alert"""
        while True:
            try:
                alerts = self.check_resource_usage()
                if alerts:
                    self.send_alert(alerts)
                time.sleep(interval_minutes * 60)
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                time.sleep(60)


# Usage
thresholds = {
    'memory': 85,  # Alert if memory > 85%
    'vcores': 90,  # Alert if vCores > 90%
    'pending_apps': 10  # Alert if > 10 pending apps
}

alert_system = ResourceAlertSystem(yarn_monitor, thresholds)
alert_system.monitor_continuously()
```

## 5. Quick Setup Script

```bash
#!/bin/bash
# setup_monitoring.sh

# Install required packages
pip install requests pandas plotly streamlit

# Get EMR master node details
CLUSTER_NAME="your-cluster-name"
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[?Name=='$CLUSTER_NAME'].Id" --output text)
MASTER_DNS=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PublicDnsName' --output text)

echo "Spark History Server: http://$MASTER_DNS:18080"
echo "YARN Resource Manager: http://$MASTER_DNS:8088"
echo "Ganglia: http://$MASTER_DNS/ganglia"

# Run the monitoring dashboard
streamlit run emr_dashboard.py
```

## 6. Integration with Existing Tools

For enterprise environments, consider integrating with:

- **Grafana + Prometheus**: For advanced dashboards
- **Datadog/New Relic**: For comprehensive monitoring
- **AWS CloudWatch**: For AWS-native monitoring
- **Spark Plugins**: Like Spark History Server extensions

## Key Benefits:

- **Resource Attribution**: Track which users/jobs consume the most resources
- **Performance Optimization**: Identify inefficient jobs
- **Capacity Planning**: Understand usage patterns
- **Cost Management**: Monitor resource waste
- **Real-time Alerts**: Prevent cluster overload

This monitoring setup will give you comprehensive visibility into your shared EMR cluster's resource usage and help
optimize performance for all users.