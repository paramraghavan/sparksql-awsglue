#!/usr/bin/env python3
"""
Flask Web Application for Spark Job Analysis
Provides a web interface to analyze Spark jobs via History Server
"""

from flask import Flask, render_template_string, request, jsonify
import requests
from datetime import datetime
import statistics
from typing import Dict, List

app = Flask(__name__)


class SparkJobAnalyzer:
    def __init__(self, history_server_url: str):
        self.base_url = history_server_url.rstrip('/')
        self.api_base = f"{self.base_url}/api/v1"
    
    def get_application_info(self, app_id: str) -> Dict:
        url = f"{self.api_base}/applications/{app_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def get_jobs(self, app_id: str) -> List[Dict]:
        url = f"{self.api_base}/applications/{app_id}/jobs"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def get_stages(self, app_id: str) -> List[Dict]:
        url = f"{self.api_base}/applications/{app_id}/stages"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def get_stage_details(self, app_id: str, stage_id: int, attempt_id: int = 0) -> Dict:
        url = f"{self.api_base}/applications/{app_id}/stages/{stage_id}/{attempt_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def get_executors(self, app_id: str) -> List[Dict]:
        url = f"{self.api_base}/applications/{app_id}/executors"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def analyze_data_skew(self, stage_details: Dict) -> Dict:
        tasks = stage_details.get('tasks', {}).values() if isinstance(stage_details.get('tasks'), dict) else []
        
        if not tasks:
            return {"skew_detected": False, "message": "No task data available"}
        
        durations = [task.get('taskMetrics', {}).get('executorRunTime', 0) for task in tasks if task.get('taskMetrics')]
        
        if len(durations) < 2:
            return {"skew_detected": False, "message": "Insufficient task data"}
        
        avg_duration = statistics.mean(durations)
        max_duration = max(durations)
        min_duration = min(durations)
        median_duration = statistics.median(durations)
        stdev = statistics.stdev(durations) if len(durations) > 1 else 0
        
        skew_ratio = max_duration / avg_duration if avg_duration > 0 else 0
        coefficient_of_variation = (stdev / avg_duration * 100) if avg_duration > 0 else 0
        
        skew_detected = skew_ratio > 3 or coefficient_of_variation > 50
        
        return {
            "skew_detected": skew_detected,
            "skew_ratio": round(skew_ratio, 2),
            "coefficient_of_variation": round(coefficient_of_variation, 2),
            "avg_duration_ms": round(avg_duration, 2),
            "max_duration_ms": round(max_duration, 2),
            "min_duration_ms": round(min_duration, 2),
            "median_duration_ms": round(median_duration, 2),
            "total_tasks": len(durations)
        }
    
    def analyze_job(self, app_id: str) -> Dict:
        analysis = {
            "app_id": app_id,
            "timestamp": datetime.now().isoformat(),
            "issues": [],
            "recommendations": [],
            "summary": {},
            "stages": []
        }
        
        try:
            # Get application info
            app_info = self.get_application_info(app_id)
            analysis['app_name'] = app_info.get('name', 'Unknown')
            analysis['app_status'] = app_info.get('attempts', [{}])[0].get('completed', False)
            
            # Get jobs
            jobs = self.get_jobs(app_id)
            active_jobs = [j for j in jobs if j['status'] == 'RUNNING']
            failed_jobs = [j for j in jobs if j['status'] == 'FAILED']
            
            if active_jobs:
                analysis['issues'].append({
                    'type': 'warning',
                    'message': f"{len(active_jobs)} job(s) currently running"
                })
            
            if failed_jobs:
                analysis['issues'].append({
                    'type': 'error',
                    'message': f"{len(failed_jobs)} job(s) failed"
                })
            
            # Get stages
            stages = self.get_stages(app_id)
            active_stages = [s for s in stages if s['status'] == 'ACTIVE']
            failed_stages = [s for s in stages if s['status'] == 'FAILED']
            completed_stages = [s for s in stages if s['status'] == 'COMPLETE']
            
            # Analyze active stages
            bottleneck_stages = []
            for stage in active_stages:
                stage_id = stage['stageId']
                attempt_id = stage['attemptId']
                
                stage_info = {
                    'stage_id': stage_id,
                    'name': stage['name'],
                    'status': stage['status'],
                    'num_tasks': stage.get('numTasks', 0),
                    'num_active_tasks': stage.get('numActiveTasks', 0),
                    'num_complete_tasks': stage.get('numCompleteTasks', 0),
                    'num_failed_tasks': stage.get('numFailedTasks', 0),
                    'issues': []
                }
                
                try:
                    stage_details = self.get_stage_details(app_id, stage_id, attempt_id)
                    skew_analysis = self.analyze_data_skew(stage_details)
                    stage_info['skew_analysis'] = skew_analysis
                    
                    if skew_analysis['skew_detected']:
                        stage_info['issues'].append(
                            f"Data skew: max task {skew_analysis['skew_ratio']}x slower than average"
                        )
                        analysis['issues'].append({
                            'type': 'warning',
                            'message': f"Stage {stage_id}: Data skew detected (ratio: {skew_analysis['skew_ratio']}x)"
                        })
                    
                    if stage.get('numFailedTasks', 0) > 0:
                        stage_info['issues'].append(f"{stage['numFailedTasks']} tasks failed")
                        analysis['issues'].append({
                            'type': 'error',
                            'message': f"Stage {stage_id}: {stage['numFailedTasks']} tasks failed"
                        })
                
                except Exception as e:
                    stage_info['issues'].append(f"Could not get detailed info: {str(e)}")
                
                analysis['stages'].append(stage_info)
                if stage_info['issues']:
                    bottleneck_stages.append(stage_info)
            
            # Analyze completed stages
            slow_stages = []
            for stage in sorted(completed_stages, key=lambda x: x.get('executorRunTime', 0), reverse=True)[:5]:
                duration_sec = stage.get('executorRunTime', 0) / 1000
                if duration_sec > 60:
                    slow_stages.append({
                        'stage_id': stage['stageId'],
                        'name': stage['name'],
                        'duration_sec': round(duration_sec, 2),
                        'num_tasks': stage.get('numTasks', 0)
                    })
            
            # Get executor info
            try:
                executors = self.get_executors(app_id)
                executor_issues = []
                for executor in executors:
                    if executor['id'] == 'driver':
                        continue
                    failed_tasks = executor.get('failedTasks', 0)
                    if failed_tasks > 0:
                        executor_issues.append({
                            'executor_id': executor['id'],
                            'failed_tasks': failed_tasks,
                            'total_tasks': executor.get('totalTasks', 0)
                        })
                        analysis['issues'].append({
                            'type': 'error',
                            'message': f"Executor {executor['id']}: {failed_tasks} failed tasks"
                        })
                analysis['executor_issues'] = executor_issues
            except Exception as e:
                analysis['executor_issues'] = []
            
            # Generate recommendations
            if bottleneck_stages:
                analysis['recommendations'].append(
                    "Enable Adaptive Query Execution (AQE) to handle data skew automatically"
                )
                analysis['recommendations'].append(
                    "Consider repartitioning data with salting for skewed keys"
                )
            
            if failed_stages or failed_jobs:
                analysis['recommendations'].append(
                    "Check executor logs for OutOfMemory errors"
                )
                analysis['recommendations'].append(
                    "Increase spark.executor.memory and spark.executor.memoryOverhead"
                )
            
            if slow_stages:
                analysis['recommendations'].append(
                    "Review and optimize SQL queries in slow stages"
                )
                analysis['recommendations'].append(
                    "Consider increasing parallelism with spark.sql.shuffle.partitions"
                )
            
            analysis['summary'] = {
                'total_jobs': len(jobs),
                'active_jobs': len(active_jobs),
                'failed_jobs': len(failed_jobs),
                'total_stages': len(stages),
                'active_stages': len(active_stages),
                'failed_stages': len(failed_stages),
                'completed_stages': len(completed_stages),
                'bottleneck_stages_count': len(bottleneck_stages),
                'slow_stages': slow_stages
            }
            
            return analysis
        
        except Exception as e:
            analysis['error'] = str(e)
            return analysis


# HTML Template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Spark Job Analyzer</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        h1 {
            color: #667eea;
            margin-bottom: 20px;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: 600;
            color: #333;
        }
        input {
            width: 100%;
            padding: 10px;
            border: 2px solid #e0e0e0;
            border-radius: 5px;
            font-size: 14px;
        }
        input:focus {
            outline: none;
            border-color: #667eea;
        }
        button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 5px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s;
        }
        button:hover {
            transform: translateY(-2px);
        }
        button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
        .results {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            display: none;
        }
        .results.show {
            display: block;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .summary-card {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }
        .summary-card h3 {
            font-size: 32px;
            color: #667eea;
            margin-bottom: 5px;
        }
        .summary-card p {
            color: #666;
            font-size: 14px;
        }
        .issue {
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
            border-left: 4px solid;
        }
        .issue.warning {
            background: #fff3cd;
            border-color: #ffc107;
        }
        .issue.error {
            background: #f8d7da;
            border-color: #dc3545;
        }
        .stage-card {
            background: #f8f9fa;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
            border-left: 4px solid #667eea;
        }
        .stage-card h4 {
            color: #333;
            margin-bottom: 10px;
        }
        .metric {
            display: inline-block;
            margin-right: 20px;
            color: #666;
        }
        .recommendation {
            background: #d1ecf1;
            padding: 12px;
            margin: 8px 0;
            border-radius: 5px;
            border-left: 4px solid #17a2b8;
        }
        .loading {
            text-align: center;
            padding: 40px;
            display: none;
        }
        .loading.show {
            display: block;
        }
        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .error-message {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Spark Job Analyzer</h1>
            <p style="color: #666; margin-bottom: 20px;">Analyze EMR Spark jobs to identify stuck stages, data skew, and bottlenecks</p>
            
            <form id="analyzeForm">
                <div class="form-group">
                    <label>Spark History Server URL:</label>
                    <input type="text" id="historyServer" placeholder="http://your-emr-master:18080" required>
                </div>
                <div class="form-group">
                    <label>Application ID:</label>
                    <input type="text" id="appId" placeholder="application_1234567890123_0001" required>
                </div>
                <button type="submit" id="analyzeBtn">Analyze Job</button>
            </form>
        </div>
        
        <div class="loading" id="loading">
            <div class="spinner"></div>
            <p style="margin-top: 20px; color: white; font-weight: 600;">Analyzing job...</p>
        </div>
        
        <div class="results" id="results"></div>
    </div>
    
    <script>
        document.getElementById('analyzeForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const historyServer = document.getElementById('historyServer').value;
            const appId = document.getElementById('appId').value;
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            const analyzeBtn = document.getElementById('analyzeBtn');
            
            loading.classList.add('show');
            results.classList.remove('show');
            analyzeBtn.disabled = true;
            
            try {
                const response = await fetch('/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ history_server: historyServer, app_id: appId })
                });
                
                const data = await response.json();
                
                if (data.error) {
                    results.innerHTML = `
                        <div class="error-message">
                            <strong>Error:</strong> ${data.error}
                        </div>
                    `;
                } else {
                    displayResults(data);
                }
                
                results.classList.add('show');
            } catch (error) {
                results.innerHTML = `
                    <div class="error-message">
                        <strong>Error:</strong> ${error.message}
                    </div>
                `;
                results.classList.add('show');
            } finally {
                loading.classList.remove('show');
                analyzeBtn.disabled = false;
            }
        });
        
        function displayResults(data) {
            const results = document.getElementById('results');
            
            let html = `
                <h2>Analysis Results for ${data.app_id}</h2>
                <p style="color: #666; margin: 10px 0;">Application: ${data.app_name || 'Unknown'}</p>
                <p style="color: #666; margin-bottom: 20px;">Analyzed at: ${new Date(data.timestamp).toLocaleString()}</p>
                
                <h3>Summary</h3>
                <div class="summary-grid">
                    <div class="summary-card">
                        <h3>${data.summary.total_jobs || 0}</h3>
                        <p>Total Jobs</p>
                    </div>
                    <div class="summary-card">
                        <h3>${data.summary.active_jobs || 0}</h3>
                        <p>Active Jobs</p>
                    </div>
                    <div class="summary-card">
                        <h3>${data.summary.total_stages || 0}</h3>
                        <p>Total Stages</p>
                    </div>
                    <div class="summary-card">
                        <h3>${data.summary.active_stages || 0}</h3>
                        <p>Active Stages</p>
                    </div>
                </div>
            `;
            
            if (data.issues && data.issues.length > 0) {
                html += '<h3 style="margin-top: 30px;">Issues Detected</h3>';
                data.issues.forEach(issue => {
                    html += `<div class="issue ${issue.type}">${issue.message}</div>`;
                });
            }
            
            if (data.stages && data.stages.length > 0) {
                html += '<h3 style="margin-top: 30px;">Active Stages</h3>';
                data.stages.forEach(stage => {
                    html += `
                        <div class="stage-card">
                            <h4>Stage ${stage.stage_id}: ${stage.name}</h4>
                            <div class="metric">Tasks: ${stage.num_complete_tasks}/${stage.num_tasks}</div>
                            <div class="metric">Active: ${stage.num_active_tasks}</div>
                            <div class="metric">Failed: ${stage.num_failed_tasks}</div>
                    `;
                    
                    if (stage.skew_analysis && stage.skew_analysis.skew_detected) {
                        html += `
                            <div style="margin-top: 10px; color: #856404; background: #fff3cd; padding: 10px; border-radius: 5px;">
                                ⚠️ Data Skew: Max task ${stage.skew_analysis.skew_ratio}x slower than average<br>
                                Avg: ${(stage.skew_analysis.avg_duration_ms/1000).toFixed(2)}s | 
                                Max: ${(stage.skew_analysis.max_duration_ms/1000).toFixed(2)}s
                            </div>
                        `;
                    }
                    
                    html += '</div>';
                });
            }
            
            if (data.summary.slow_stages && data.summary.slow_stages.length > 0) {
                html += '<h3 style="margin-top: 30px;">Slowest Completed Stages</h3>';
                data.summary.slow_stages.forEach(stage => {
                    html += `
                        <div class="stage-card">
                            <h4>Stage ${stage.stage_id}: ${stage.name}</h4>
                            <div class="metric">Duration: ${stage.duration_sec}s</div>
                            <div class="metric">Tasks: ${stage.num_tasks}</div>
                        </div>
                    `;
                });
            }
            
            if (data.recommendations && data.recommendations.length > 0) {
                html += '<h3 style="margin-top: 30px;">Recommendations</h3>';
                data.recommendations.forEach(rec => {
                    html += `<div class="recommendation">✓ ${rec}</div>`;
                });
            }
            
            if (!data.issues || data.issues.length === 0) {
                html += '<div class="recommendation" style="margin-top: 20px;">✓ No major issues detected. Job appears healthy.</div>';
            }
            
            results.innerHTML = html;
        }
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        history_server = data.get('history_server')
        app_id = data.get('app_id')
        
        if not history_server or not app_id:
            return jsonify({'error': 'Missing history_server or app_id'}), 400
        
        analyzer = SparkJobAnalyzer(history_server)
        result = analyzer.analyze_job(app_id)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Run on all interfaces so it's accessible from EMR
    app.run(host='0.0.0.0', port=5000, debug=True)
