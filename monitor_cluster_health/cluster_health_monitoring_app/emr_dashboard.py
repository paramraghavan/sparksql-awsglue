"""
EMR Cluster Health Dashboard
Reads monitoring stats from S3 prefixes and displays interactive charts
"""

from flask import Flask, render_template, jsonify
import boto3
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import json

app = Flask(__name__)

# Configuration
S3_BUCKET = "sparksql-emr-monitoring"
MASTER_NODE_IPS = [
    "10.0.0.1",    # EMR 1
    "10.0.0.2",    # EMR 2
    "10.0.0.3",    # EMR 3
    "10.0.0.4",    # EMR 4
    "10.0.0.5"     # EMR 5
]

s3_client = boto3.client('s3')


def get_stats_from_s3(ip):
    """Fetch cluster_history.csv from S3 for a given IP."""
    try:
        s3_key = f"{ip}/cluster_history.csv"
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        df['IP'] = ip
        return df
    except Exception as e:
        print(f"Error fetching stats for {ip}: {e}")
        return pd.DataFrame()


def get_realtime_snapshot_from_s3(ip):
    """Fetch real-time snapshot JSON from S3 for a given IP."""
    try:
        s3_key = f"{ip}/realtime_snapshot.json"
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        snapshot_content = response['Body'].read().decode('utf-8')
        return json.loads(snapshot_content)
    except Exception as e:
        print(f"Error fetching real-time snapshot for {ip}: {e}")
        return {}


def load_all_stats():
    """Load stats from all master nodes."""
    all_data = []
    for ip in MASTER_NODE_IPS:
        df = get_stats_from_s3(ip)
        if not df.empty:
            all_data.append(df)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def calculate_node_health(snapshot):
    """
    Calculate overall health status for a node based on real-time metrics.
    Returns: {'status': 'critical'|'warning'|'healthy', 'severity': 0-2, 'issues': [...]}
    """
    issues = []
    severity = 0  # 0=healthy, 1=warning, 2=critical

    # Define thresholds
    thresholds = {
        'cpu_percent': {'warning': 60, 'critical': 80},
        'memory_percent': {'warning': 60, 'critical': 80},
        'disk_percent': {'warning': 60, 'critical': 85},
        'load_1m': {'warning': 4, 'critical': 8},
        'yarn_pending': {'warning': 5, 'critical': 10}
    }

    # Check each metric
    for metric, threshold in thresholds.items():
        if metric not in snapshot:
            continue

        value = snapshot[metric]

        if threshold['critical'] > 0 and value >= threshold['critical']:
            severity = max(severity, 2)
            metric_name = metric.replace('_', ' ').title()
            issues.append(f"{metric_name}: {value} (critical)")
        elif threshold['warning'] > 0 and value >= threshold['warning']:
            severity = max(severity, 1)
            metric_name = metric.replace('_', ' ').title()
            issues.append(f"{metric_name}: {value} (warning)")

    status_map = {0: 'healthy', 1: 'warning', 2: 'critical'}
    return {
        'status': status_map[severity],
        'severity': severity,
        'issues': issues
    }


def prepare_chart_data(df, metric, top_days=60):
    """Prepare data for Chart.js."""
    if df.empty:
        return {"labels": [], "datasets": []}

    # Get last N days
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').tail(top_days)

    datasets = []
    colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF']

    for idx, ip in enumerate(MASTER_NODE_IPS):
        ip_data = df[df['IP'] == ip]
        if not ip_data.empty:
            datasets.append({
                "label": f"EMR {idx+1} ({ip})",
                "data": ip_data[metric].fillna(0).tolist(),
                "borderColor": colors[idx],
                "backgroundColor": colors[idx] + "20",
                "tension": 0.1
            })

    labels = df['Date'].dt.strftime('%Y-%m-%d').unique().tolist()

    return {
        "labels": labels,
        "datasets": datasets
    }


@app.route('/')
def index():
    """Main dashboard page."""
    return render_template('dashboard.html')


@app.route('/api/stats')
def api_stats():
    """API endpoint to fetch all stats."""
    try:
        df = load_all_stats()
        if df.empty:
            return jsonify({"error": "No data available"}), 404

        # Get summary stats
        summary = {}
        for ip in MASTER_NODE_IPS:
            ip_data = df[df['IP'] == ip]
            if not ip_data.empty:
                summary[ip] = {
                    "last_cpu": ip_data['MaxCPU'].iloc[-1],
                    "last_mem": ip_data['MaxMem'].iloc[-1],
                    "last_disk": ip_data['MaxDisk'].iloc[-1],
                    "last_pending": ip_data['MaxPending'].iloc[-1],
                    "avg_cpu_30d": ip_data['MaxCPU'].tail(30).mean(),
                    "max_cpu_30d": ip_data['MaxCPU'].tail(30).max(),
                    "max_mem_30d": ip_data['MaxMem'].tail(30).max(),
                    "max_disk_30d": ip_data['MaxDisk'].tail(30).max(),
                }

        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/chart/<metric>')
def api_chart(metric):
    """API endpoint for chart data."""
    try:
        valid_metrics = ['MaxCPU', 'MaxMem', 'MaxDisk', 'MaxPending', 'MaxYarnMem', 'MaxYarnCPU']

        if metric not in valid_metrics:
            return jsonify({"error": f"Invalid metric. Valid: {valid_metrics}"}), 400

        df = load_all_stats()
        if df.empty:
            return jsonify({"error": "No data available"}), 404

        chart_data = prepare_chart_data(df, metric)
        return jsonify(chart_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/node/<ip>')
def api_node(ip):
    """Get detailed stats for a specific node."""
    try:
        df = get_stats_from_s3(ip)
        if df.empty:
            return jsonify({"error": f"No data for IP {ip}"}), 404

        # Return last 10 days
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').tail(10)

        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/realtime')
def api_realtime():
    """Get real-time snapshots from all nodes."""
    try:
        realtime = {}
        for ip in MASTER_NODE_IPS:
            snapshot = get_realtime_snapshot_from_s3(ip)
            if snapshot:
                realtime[ip] = snapshot

        if not realtime:
            return jsonify({"error": "No real-time data available"}), 404

        return jsonify(realtime)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/realtime/<ip>')
def api_realtime_node(ip):
    """Get real-time snapshot for a specific node."""
    try:
        snapshot = get_realtime_snapshot_from_s3(ip)
        if not snapshot:
            return jsonify({"error": f"No real-time data for IP {ip}"}), 404

        return jsonify(snapshot)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/health-overview')
def api_health_overview():
    """Get overall health status for all nodes."""
    try:
        health = {}
        cluster_status = 'healthy'  # Will update based on nodes
        critical_count = 0
        warning_count = 0

        for idx, ip in enumerate(MASTER_NODE_IPS):
            snapshot = get_realtime_snapshot_from_s3(ip)

            if not snapshot:
                # No real-time data yet
                health[ip] = {
                    'emr_number': idx + 1,
                    'status': 'unknown',
                    'severity': -1,
                    'issues': ['No real-time data available'],
                    'metrics': {}
                }
            else:
                node_health = calculate_node_health(snapshot)

                # Track cluster overall status
                if node_health['severity'] == 2:
                    critical_count += 1
                    cluster_status = 'critical'
                elif node_health['severity'] == 1 and cluster_status != 'critical':
                    warning_count += 1
                    cluster_status = 'warning'

                health[ip] = {
                    'emr_number': idx + 1,
                    'status': node_health['status'],
                    'severity': node_health['severity'],
                    'issues': node_health['issues'],
                    'metrics': {
                        'cpu_percent': round(snapshot.get('cpu_percent', 0), 1),
                        'memory_percent': round(snapshot.get('memory_percent', 0), 1),
                        'disk_percent': round(snapshot.get('disk_percent', 0), 1),
                        'load_1m': round(snapshot.get('load_1m', 0), 2),
                        'yarn_pending': snapshot.get('yarn_pending', 0)
                    },
                    'timestamp': snapshot.get('timestamp', '')
                }

        # Determine overall cluster health
        if critical_count > 0:
            cluster_status = 'critical'
        elif warning_count > 0:
            cluster_status = 'warning'

        return jsonify({
            'cluster_status': cluster_status,
            'critical_nodes': critical_count,
            'warning_nodes': warning_count,
            'healthy_nodes': len(MASTER_NODE_IPS) - critical_count - warning_count,
            'nodes': health
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    print(f"Starting EMR Dashboard on port 5000")
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Monitoring nodes: {', '.join(MASTER_NODE_IPS)}")
    app.run(debug=True, host='0.0.0.0', port=5000)
