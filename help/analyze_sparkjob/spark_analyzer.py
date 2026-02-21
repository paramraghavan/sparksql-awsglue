#!/usr/bin/env python3
"""
Spark Application Analyzer
==========================
Analyzes Spark applications using the History Server REST API to determine
resource usage and provide optimization recommendations.

Usage:
    python spark_analyzer.py --url http://history-server:18080 --app-id application_1234567890123_0001
    python spark_analyzer.py --url http://history-server:18080 --app-id application_1234567890123_0001 --output report.json
"""

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
import requests


# =============================================================================
# Data Classes for Structured Results
# =============================================================================

@dataclass
class ExecutorMetrics:
    total_executors: int = 0
    active_executors: int = 0
    total_cores: int = 0
    total_memory_mb: int = 0
    total_task_time_ms: int = 0
    total_gc_time_ms: int = 0
    gc_time_ratio: float = 0.0
    total_input_bytes: int = 0
    total_output_bytes: int = 0
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    total_disk_spill_bytes: int = 0
    total_memory_spill_bytes: int = 0
    max_memory_used_bytes: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    executor_details: list = field(default_factory=list)


@dataclass
class StageMetrics:
    stage_id: int = 0
    stage_name: str = ""
    status: str = ""
    num_tasks: int = 0
    duration_ms: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    disk_spill_bytes: int = 0
    memory_spill_bytes: int = 0
    task_time_min_ms: int = 0
    task_time_max_ms: int = 0
    task_time_median_ms: int = 0
    task_time_p75_ms: int = 0
    task_time_p25_ms: int = 0
    skew_ratio: float = 0.0
    gc_time_ms: int = 0
    task_metrics: dict = field(default_factory=dict)


@dataclass
class JobMetrics:
    job_id: int = 0
    name: str = ""
    status: str = ""
    num_stages: int = 0
    num_tasks: int = 0
    num_completed_stages: int = 0
    num_failed_stages: int = 0
    duration_ms: int = 0


@dataclass
class SQLQueryMetrics:
    query_id: int = 0
    description: str = ""
    duration_ms: int = 0
    status: str = ""
    running_jobs: int = 0
    succeeded_jobs: int = 0
    failed_jobs: int = 0


@dataclass
class ConfigurationAnalysis:
    executor_memory: str = ""
    executor_cores: str = ""
    executor_instances: str = ""
    executor_memory_overhead: str = ""
    driver_memory: str = ""
    shuffle_partitions: str = ""
    default_parallelism: str = ""
    dynamic_allocation: str = ""
    adaptive_enabled: str = ""
    memory_fraction: str = ""
    storage_fraction: str = ""
    all_configs: dict = field(default_factory=dict)


@dataclass
class Issue:
    severity: str  # HIGH, MEDIUM, LOW
    category: str  # MEMORY, PARALLELISM, SHUFFLE, GC, SKEW
    description: str
    current_value: str
    recommendation: str
    config_key: Optional[str] = None
    suggested_value: Optional[str] = None


@dataclass
class AnalysisReport:
    application_id: str = ""
    application_name: str = ""
    start_time: str = ""
    end_time: str = ""
    duration_ms: int = 0
    duration_human: str = ""
    spark_version: str = ""
    user: str = ""

    executor_metrics: ExecutorMetrics = field(default_factory=ExecutorMetrics)
    stage_metrics: list = field(default_factory=list)
    job_metrics: list = field(default_factory=list)
    sql_metrics: list = field(default_factory=list)
    configuration: ConfigurationAnalysis = field(default_factory=ConfigurationAnalysis)

    issues: list = field(default_factory=list)
    recommendations: dict = field(default_factory=dict)

    analysis_timestamp: str = ""


# =============================================================================
# Spark History Server API Client
# =============================================================================

class SparkHistoryServerClient:
    """Client for Spark History Server REST API"""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_base = f"{self.base_url}/api/v1"
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, endpoint: str) -> dict:
        """Make GET request to API endpoint"""
        url = f"{self.api_base}{endpoint}"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            raise

    def list_applications(self) -> list:
        """List all applications"""
        return self._get("/applications")

    def get_application(self, app_id: str) -> dict:
        """Get application details"""
        return self._get(f"/applications/{app_id}")

    def get_jobs(self, app_id: str) -> list:
        """Get all jobs for an application"""
        return self._get(f"/applications/{app_id}/jobs")

    def get_stages(self, app_id: str) -> list:
        """Get all stages for an application"""
        return self._get(f"/applications/{app_id}/stages")

    def get_stage_detail(self, app_id: str, stage_id: int, attempt_id: int = 0) -> dict:
        """Get detailed stage information including task metrics"""
        return self._get(f"/applications/{app_id}/stages/{stage_id}/{attempt_id}")

    def get_executors(self, app_id: str) -> list:
        """Get executor information"""
        return self._get(f"/applications/{app_id}/allexecutors")

    def get_environment(self, app_id: str) -> dict:
        """Get environment and configuration"""
        return self._get(f"/applications/{app_id}/environment")

    def get_sql_queries(self, app_id: str) -> list:
        """Get SQL query information (Spark 2.3+)"""
        try:
            return self._get(f"/applications/{app_id}/sql")
        except:
            return []  # SQL endpoint may not exist in older versions


# =============================================================================
# Analyzer
# =============================================================================

class SparkApplicationAnalyzer:
    """Analyzes Spark application metrics and provides recommendations"""

    def __init__(self, client: SparkHistoryServerClient, app_id: str):
        self.client = client
        self.app_id = app_id
        self.report = AnalysisReport()

    def analyze(self) -> AnalysisReport:
        """Run complete analysis"""
        print(f"Analyzing application: {self.app_id}")

        self._analyze_application_info()
        self._analyze_environment()
        self._analyze_executors()
        self._analyze_jobs()
        self._analyze_stages()
        self._analyze_sql()
        self._identify_issues()
        self._generate_recommendations()

        self.report.analysis_timestamp = datetime.now().isoformat()
        return self.report

    def _analyze_application_info(self):
        """Analyze basic application information"""
        print("  - Fetching application info...")
        app_info = self.client.get_application(self.app_id)

        # Handle both single app and list response
        if isinstance(app_info, list):
            app_info = app_info[0] if app_info else {}

        attempts = app_info.get('attempts', [{}])
        latest_attempt = attempts[0] if attempts else {}

        self.report.application_id = self.app_id
        self.report.application_name = app_info.get('name', 'Unknown')
        self.report.start_time = latest_attempt.get('startTime', '')
        self.report.end_time = latest_attempt.get('endTime', '')
        self.report.duration_ms = latest_attempt.get('duration', 0)
        self.report.duration_human = self._format_duration(self.report.duration_ms)
        self.report.spark_version = latest_attempt.get('sparkVersion', 'Unknown')
        self.report.user = latest_attempt.get('sparkUser', 'Unknown')

    def _analyze_environment(self):
        """Analyze Spark configuration"""
        print("  - Fetching environment/configuration...")
        env = self.client.get_environment(self.app_id)

        spark_props = {}
        for prop in env.get('sparkProperties', []):
            if len(prop) >= 2:
                spark_props[prop[0]] = prop[1]

        config = self.report.configuration
        config.executor_memory = spark_props.get('spark.executor.memory', 'Not Set')
        config.executor_cores = spark_props.get('spark.executor.cores', 'Not Set')
        config.executor_instances = spark_props.get('spark.executor.instances', 'Dynamic')
        config.executor_memory_overhead = spark_props.get('spark.executor.memoryOverhead',
                                                          spark_props.get('spark.yarn.executor.memoryOverhead', 'Auto'))
        config.driver_memory = spark_props.get('spark.driver.memory', 'Not Set')
        config.shuffle_partitions = spark_props.get('spark.sql.shuffle.partitions', '200')
        config.default_parallelism = spark_props.get('spark.default.parallelism', 'Auto')
        config.dynamic_allocation = spark_props.get('spark.dynamicAllocation.enabled', 'false')
        config.adaptive_enabled = spark_props.get('spark.sql.adaptive.enabled', 'false')
        config.memory_fraction = spark_props.get('spark.memory.fraction', '0.6')
        config.storage_fraction = spark_props.get('spark.memory.storageFraction', '0.5')
        config.all_configs = spark_props

    def _analyze_executors(self):
        """Analyze executor metrics"""
        print("  - Fetching executor metrics...")
        executors = self.client.get_executors(self.app_id)

        metrics = self.report.executor_metrics

        for exec_info in executors:
            exec_id = exec_info.get('id', '')
            is_driver = exec_id == 'driver'

            if not is_driver:
                metrics.total_executors += 1
                if exec_info.get('isActive', False):
                    metrics.active_executors += 1

                metrics.total_cores += exec_info.get('totalCores', 0)

                # Memory (maxMemory is in bytes)
                max_mem = exec_info.get('maxMemory', 0)
                metrics.total_memory_mb += max_mem // (1024 * 1024)

                # Track peak memory usage
                mem_used = exec_info.get('memoryUsed', 0)
                if mem_used > metrics.max_memory_used_bytes:
                    metrics.max_memory_used_bytes = mem_used

            # Aggregate metrics (include driver for totals)
            metrics.total_task_time_ms += exec_info.get('totalDuration', 0)
            metrics.total_gc_time_ms += exec_info.get('totalGCTime', 0)
            metrics.total_input_bytes += exec_info.get('totalInputBytes', 0)
            metrics.total_output_bytes += exec_info.get('totalOutputBytes', 0)
            metrics.total_shuffle_read_bytes += exec_info.get('totalShuffleRead', 0)
            metrics.total_shuffle_write_bytes += exec_info.get('totalShuffleWrite', 0)
            metrics.total_tasks_completed += exec_info.get('totalTasks', 0)
            metrics.total_tasks_failed += exec_info.get('failedTasks', 0)

            # Memory/disk spill from memory metrics
            mem_metrics = exec_info.get('memoryMetrics', {})
            # Note: spill metrics are at task level, aggregated from stages

            # Store executor details
            metrics.executor_details.append({
                'id': exec_id,
                'host': exec_info.get('hostPort', ''),
                'is_active': exec_info.get('isActive', False),
                'cores': exec_info.get('totalCores', 0),
                'max_memory_mb': max_mem // (1024 * 1024),
                'memory_used_mb': mem_used // (1024 * 1024),
                'task_time_ms': exec_info.get('totalDuration', 0),
                'gc_time_ms': exec_info.get('totalGCTime', 0),
                'tasks_completed': exec_info.get('totalTasks', 0),
                'tasks_failed': exec_info.get('failedTasks', 0),
            })

        # Calculate GC ratio
        if metrics.total_task_time_ms > 0:
            metrics.gc_time_ratio = (metrics.total_gc_time_ms / metrics.total_task_time_ms) * 100

    def _analyze_jobs(self):
        """Analyze job metrics"""
        print("  - Fetching job metrics...")
        jobs = self.client.get_jobs(self.app_id)

        for job in jobs:
            submission_time = job.get('submissionTime', '')
            completion_time = job.get('completionTime', '')

            # Calculate duration
            duration = 0
            if submission_time and completion_time:
                try:
                    start = datetime.fromisoformat(submission_time.replace('GMT', '+00:00').replace('Z', '+00:00'))
                    end = datetime.fromisoformat(completion_time.replace('GMT', '+00:00').replace('Z', '+00:00'))
                    duration = int((end - start).total_seconds() * 1000)
                except:
                    pass

            job_metrics = JobMetrics(
                job_id=job.get('jobId', 0),
                name=job.get('name', ''),
                status=job.get('status', ''),
                num_stages=job.get('numStages', 0),
                num_tasks=job.get('numTasks', 0),
                num_completed_stages=job.get('numCompletedStages', 0),
                num_failed_stages=job.get('numFailedStages', 0),
                duration_ms=duration
            )
            self.report.job_metrics.append(job_metrics)

    def _analyze_stages(self):
        """Analyze stage metrics with task-level details"""
        print("  - Fetching stage metrics...")
        stages = self.client.get_stages(self.app_id)

        total_disk_spill = 0
        total_memory_spill = 0

        for stage in stages:
            stage_id = stage.get('stageId', 0)
            attempt_id = stage.get('attemptId', 0)

            # Get detailed stage info for task metrics
            try:
                stage_detail = self.client.get_stage_detail(self.app_id, stage_id, attempt_id)
            except:
                stage_detail = stage

            # Extract task metrics quantiles if available
            task_quantiles = stage_detail.get('taskMetricsDistributions', {})
            executor_run_time = task_quantiles.get('executorRunTime', [0, 0, 0, 0, 0])

            # quantiles are typically [min, 25th, median, 75th, max]
            task_time_min = int(executor_run_time[0]) if len(executor_run_time) > 0 else 0
            task_time_p25 = int(executor_run_time[1]) if len(executor_run_time) > 1 else 0
            task_time_median = int(executor_run_time[2]) if len(executor_run_time) > 2 else 0
            task_time_p75 = int(executor_run_time[3]) if len(executor_run_time) > 3 else 0
            task_time_max = int(executor_run_time[4]) if len(executor_run_time) > 4 else 0

            # Calculate skew ratio
            skew_ratio = 0.0
            if task_time_median > 0:
                skew_ratio = task_time_max / task_time_median

            disk_spill = stage.get('diskBytesSpilled', 0)
            memory_spill = stage.get('memoryBytesSpilled', 0)
            total_disk_spill += disk_spill
            total_memory_spill += memory_spill

            stage_metrics = StageMetrics(
                stage_id=stage_id,
                stage_name=stage.get('name', ''),
                status=stage.get('status', ''),
                num_tasks=stage.get('numTasks', 0),
                duration_ms=stage.get('executorRunTime', 0),
                input_bytes=stage.get('inputBytes', 0),
                output_bytes=stage.get('outputBytes', 0),
                shuffle_read_bytes=stage.get('shuffleReadBytes', 0),
                shuffle_write_bytes=stage.get('shuffleWriteBytes', 0),
                disk_spill_bytes=disk_spill,
                memory_spill_bytes=memory_spill,
                task_time_min_ms=task_time_min,
                task_time_max_ms=task_time_max,
                task_time_median_ms=task_time_median,
                task_time_p75_ms=task_time_p75,
                task_time_p25_ms=task_time_p25,
                skew_ratio=skew_ratio,
                gc_time_ms=stage.get('jvmGcTime', 0),
                task_metrics=task_quantiles
            )
            self.report.stage_metrics.append(stage_metrics)

        # Update executor metrics with spill totals
        self.report.executor_metrics.total_disk_spill_bytes = total_disk_spill
        self.report.executor_metrics.total_memory_spill_bytes = total_memory_spill

    def _analyze_sql(self):
        """Analyze SQL query metrics"""
        print("  - Fetching SQL metrics...")
        sql_queries = self.client.get_sql_queries(self.app_id)

        for query in sql_queries:
            sql_metrics = SQLQueryMetrics(
                query_id=query.get('id', 0),
                description=query.get('description', '')[:200],  # Truncate long descriptions
                duration_ms=query.get('duration', 0),
                status=query.get('status', ''),
                running_jobs=query.get('runningJobs', 0),
                succeeded_jobs=query.get('successedJobs', query.get('succeededJobs', 0)),
                failed_jobs=query.get('failedJobs', 0)
            )
            self.report.sql_metrics.append(sql_metrics)

    def _identify_issues(self):
        """Identifies performance bottlenecks and automated stuck-job patterns"""
        print("  - Identifying issues...")
        issues = []
        exec_metrics = self.report.executor_metrics
        config = self.report.configuration

        # --- [1. STRAGGLER / STUCK TASK DETECTION] ---
        # Automates Step 12 & 13 of the manual guide: checks if a single task hangs the stage
        for stage in self.report.stage_metrics:
            # Only analyze stages with meaningful work (> 5 seconds median)
            if stage.task_time_median_ms > 5000:
                # If the slowest task is > 5x the median, it's a straggler
                if stage.task_time_max_ms > (stage.task_time_median_ms * 5):
                    issues.append(Issue(
                        severity="HIGH",
                        category="SKEW",
                        description=f"Straggler Task in Stage {stage.stage_id}: One task is significantly slower than others.",
                        current_value=f"Max: {stage.task_time_max_ms / 1000}s vs Median: {stage.task_time_median_ms / 1000}s",
                        recommendation="Check for data skew. Enable Adaptive Query Execution (AQE) or manually salt skewed keys.",
                        config_key="spark.sql.adaptive.skewJoin.enabled",
                        suggested_value="true"
                    ))

        # --- [2. RESOURCE STARVATION / IDLE EXECUTORS] ---
        # Automates the "Active Tasks = 0" check. Flags if job runs long but cores are idle.
        if self.report.duration_ms > 600000:  # If job has run > 10 minutes
            total_potential_time = exec_metrics.total_cores * self.report.duration_ms
            utilization = (
                                  exec_metrics.total_task_time_ms / total_potential_time) * 100 if total_potential_time > 0 else 0

            if utilization < 10:
                issues.append(Issue(
                    severity="MEDIUM",
                    category="PARALLELISM",
                    description="Resource Starvation: Job is active but executors are mostly idle.",
                    current_value=f"{utilization:.1f}% Core Utilization",
                    recommendation="Check YARN RM Scheduler or Driver thread dumps for blocks (Step 13 in README).",
                    config_key="spark.dynamicAllocation.enabled",
                    suggested_value="true"
                ))

        # --- [3. EXISTING MEMORY & GC CHECKS] ---
        if exec_metrics.gc_time_ratio > 10:
            severity = "HIGH" if exec_metrics.gc_time_ratio > 20 else "MEDIUM"
            issues.append(Issue(
                severity=severity,
                category="GC",
                description=f"High GC time: {exec_metrics.gc_time_ratio:.1f}% of task time spent in GC",
                current_value=f"{exec_metrics.gc_time_ratio:.1f}%",
                recommendation="Increase executor memory to reduce pressure",
                config_key="spark.executor.memory",
                suggested_value=self._suggest_memory_increase(config.executor_memory)
            ))

        if exec_metrics.total_disk_spill_bytes > 0:
            spill_gb = exec_metrics.total_disk_spill_bytes / (1024 ** 3)
            issues.append(Issue(
                severity="HIGH" if spill_gb > 1 else "MEDIUM",
                category="MEMORY",
                description=f"Disk spill detected: {spill_gb:.2f} GB spilled to disk",
                current_value=f"{spill_gb:.2f} GB",
                recommendation="Increase executor memory to avoid slow disk I/O",
                config_key="spark.executor.memory",
                suggested_value=self._suggest_memory_increase(config.executor_memory)
            ))

        # Sort issues by severity
        severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        issues.sort(key=lambda x: severity_order.get(x.severity, 3))
        self.report.issues = issues

    def _generate_recommendations(self):
        """Generate configuration recommendations"""
        print("  - Generating recommendations...")
        recommendations = {}

        # Collect all suggested config changes
        for issue in self.report.issues:
            if issue.config_key and issue.suggested_value:
                # Keep highest severity recommendation for each config
                if issue.config_key not in recommendations:
                    recommendations[issue.config_key] = {
                        'current': self._get_current_config(issue.config_key),
                        'suggested': issue.suggested_value,
                        'reason': issue.description
                    }

        # Add general recommendations based on analysis
        config = self.report.configuration
        exec_metrics = self.report.executor_metrics

        # Memory recommendation
        if 'spark.executor.memory' not in recommendations:
            current_mem = config.executor_memory
            if exec_metrics.total_disk_spill_bytes > 0 or exec_metrics.gc_time_ratio > 10:
                recommendations['spark.executor.memory'] = {
                    'current': current_mem,
                    'suggested': self._suggest_memory_increase(current_mem),
                    'reason': 'Increase memory to reduce GC pressure and disk spill'
                }

        self.report.recommendations = recommendations

    def _get_current_config(self, key: str) -> str:
        """Get current configuration value"""
        config = self.report.configuration
        config_map = {
            'spark.executor.memory': config.executor_memory,
            'spark.executor.cores': config.executor_cores,
            'spark.executor.instances': config.executor_instances,
            'spark.executor.memoryOverhead': config.executor_memory_overhead,
            'spark.sql.shuffle.partitions': config.shuffle_partitions,
            'spark.sql.adaptive.enabled': config.adaptive_enabled,
        }
        return config_map.get(key, config.all_configs.get(key, 'Not Set'))

    def _suggest_memory_increase(self, current: str) -> str:
        """Suggest increased memory value"""
        if not current or current == 'Not Set':
            return '4g'

        # Parse current value
        current = current.lower().strip()
        if current.endswith('g'):
            value = int(current[:-1])
            return f"{int(value * 1.5)}g"
        elif current.endswith('m'):
            value = int(current[:-1])
            return f"{int(value * 1.5)}m"
        return '4g'

    def _suggest_overhead(self, executor_memory: str) -> str:
        """Suggest memory overhead (typically 10-15% of executor memory)"""
        if not executor_memory or executor_memory == 'Not Set':
            return '1g'

        executor_memory = executor_memory.lower().strip()
        if executor_memory.endswith('g'):
            value = int(executor_memory[:-1])
            overhead = max(int(value * 0.15), 1)
            return f"{overhead}g"
        elif executor_memory.endswith('m'):
            value = int(executor_memory[:-1])
            overhead = max(int(value * 0.15), 384)
            return f"{overhead}m"
        return '1g'

    @staticmethod
    def _format_duration(ms: int) -> str:
        """Format milliseconds to human readable duration"""
        if ms < 1000:
            return f"{ms}ms"

        seconds = ms // 1000
        if seconds < 60:
            return f"{seconds}s"

        minutes = seconds // 60
        seconds = seconds % 60
        if minutes < 60:
            return f"{minutes}m {seconds}s"

        hours = minutes // 60
        minutes = minutes % 60
        return f"{hours}h {minutes}m {seconds}s"


# =============================================================================
# Report Generator
# =============================================================================

class ReportGenerator:
    """Generates human-readable reports from analysis"""

    @staticmethod
    def generate_text_report(report: AnalysisReport) -> str:
        """Generate text report"""
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append("SPARK APPLICATION ANALYSIS REPORT")
        lines.append("=" * 80)
        lines.append("")

        # Application Info
        lines.append("APPLICATION INFORMATION")
        lines.append("-" * 40)
        lines.append(f"  Application ID:   {report.application_id}")
        lines.append(f"  Application Name: {report.application_name}")
        lines.append(f"  Spark Version:    {report.spark_version}")
        lines.append(f"  User:             {report.user}")
        lines.append(f"  Duration:         {report.duration_human}")
        lines.append(f"  Start Time:       {report.start_time}")
        lines.append(f"  End Time:         {report.end_time}")
        lines.append("")

        # Current Configuration
        config = report.configuration
        lines.append("CURRENT CONFIGURATION")
        lines.append("-" * 40)
        lines.append(f"  Executor Memory:      {config.executor_memory}")
        lines.append(f"  Executor Cores:       {config.executor_cores}")
        lines.append(f"  Executor Instances:   {config.executor_instances}")
        lines.append(f"  Memory Overhead:      {config.executor_memory_overhead}")
        lines.append(f"  Driver Memory:        {config.driver_memory}")
        lines.append(f"  Shuffle Partitions:   {config.shuffle_partitions}")
        lines.append(f"  Dynamic Allocation:   {config.dynamic_allocation}")
        lines.append(f"  AQE Enabled:          {config.adaptive_enabled}")
        lines.append("")

        # Executor Metrics
        exec_metrics = report.executor_metrics
        lines.append("EXECUTOR METRICS")
        lines.append("-" * 40)
        lines.append(f"  Total Executors:      {exec_metrics.total_executors}")
        lines.append(f"  Total Cores:          {exec_metrics.total_cores}")
        lines.append(f"  Total Memory:         {exec_metrics.total_memory_mb:,} MB")
        lines.append(f"  Tasks Completed:      {exec_metrics.total_tasks_completed:,}")
        lines.append(f"  Tasks Failed:         {exec_metrics.total_tasks_failed:,}")
        lines.append("")
        lines.append(
            f"  Total Task Time:      {ReportGenerator._format_bytes_or_time(exec_metrics.total_task_time_ms, 'time')}")
        lines.append(
            f"  Total GC Time:        {ReportGenerator._format_bytes_or_time(exec_metrics.total_gc_time_ms, 'time')}")
        lines.append(f"  GC Time Ratio:        {exec_metrics.gc_time_ratio:.2f}%")
        lines.append("")
        lines.append(
            f"  Total Input:          {ReportGenerator._format_bytes_or_time(exec_metrics.total_input_bytes, 'bytes')}")
        lines.append(
            f"  Total Output:         {ReportGenerator._format_bytes_or_time(exec_metrics.total_output_bytes, 'bytes')}")
        lines.append(
            f"  Total Shuffle Read:   {ReportGenerator._format_bytes_or_time(exec_metrics.total_shuffle_read_bytes, 'bytes')}")
        lines.append(
            f"  Total Shuffle Write:  {ReportGenerator._format_bytes_or_time(exec_metrics.total_shuffle_write_bytes, 'bytes')}")
        lines.append(
            f"  Total Disk Spill:     {ReportGenerator._format_bytes_or_time(exec_metrics.total_disk_spill_bytes, 'bytes')}")
        lines.append(
            f"  Total Memory Spill:   {ReportGenerator._format_bytes_or_time(exec_metrics.total_memory_spill_bytes, 'bytes')}")
        lines.append("")

        # Stage Summary
        if report.stage_metrics:
            lines.append("STAGE SUMMARY (Top 10 by Duration)")
            lines.append("-" * 40)
            sorted_stages = sorted(report.stage_metrics, key=lambda s: s.duration_ms, reverse=True)[:10]

            lines.append(
                f"  {'ID':<6} {'Tasks':<8} {'Duration':<12} {'Skew':<8} {'Shuffle Read':<15} {'Disk Spill':<15}")
            lines.append(f"  {'-' * 6} {'-' * 8} {'-' * 12} {'-' * 8} {'-' * 15} {'-' * 15}")

            for stage in sorted_stages:
                duration = ReportGenerator._format_bytes_or_time(stage.duration_ms, 'time')
                skew = f"{stage.skew_ratio:.1f}x" if stage.skew_ratio > 0 else "N/A"
                shuffle = ReportGenerator._format_bytes_or_time(stage.shuffle_read_bytes, 'bytes')
                spill = ReportGenerator._format_bytes_or_time(stage.disk_spill_bytes, 'bytes')
                lines.append(
                    f"  {stage.stage_id:<6} {stage.num_tasks:<8} {duration:<12} {skew:<8} {shuffle:<15} {spill:<15}")
            lines.append("")

        # Issues
        if report.issues:
            lines.append("ISSUES IDENTIFIED")
            lines.append("-" * 40)
            for i, issue in enumerate(report.issues, 1):
                lines.append(f"  {i}. [{issue.severity}] {issue.category}")
                lines.append(f"     {issue.description}")
                lines.append(f"     Current: {issue.current_value}")
                lines.append(f"     Action:  {issue.recommendation}")
                if issue.config_key:
                    lines.append(f"     Config:  {issue.config_key} = {issue.suggested_value}")
                lines.append("")
        else:
            lines.append("ISSUES IDENTIFIED")
            lines.append("-" * 40)
            lines.append("  No significant issues detected.")
            lines.append("")

        # Recommendations
        if report.recommendations:
            lines.append("RECOMMENDED CONFIGURATION CHANGES")
            lines.append("-" * 40)
            for config_key, rec in report.recommendations.items():
                lines.append(f"  {config_key}")
                lines.append(f"    Current:   {rec['current']}")
                lines.append(f"    Suggested: {rec['suggested']}")
                lines.append(f"    Reason:    {rec['reason']}")
                lines.append("")

            # Generate spark-submit snippet
            lines.append("SPARK-SUBMIT SNIPPET")
            lines.append("-" * 40)
            lines.append("  Add these to your spark-submit command:")
            lines.append("")
            for config_key, rec in report.recommendations.items():
                lines.append(f"    --conf {config_key}={rec['suggested']} \\")
            lines.append("")

        lines.append("=" * 80)
        lines.append(f"Report generated: {report.analysis_timestamp}")
        lines.append("=" * 80)

        return "\n".join(lines)

    @staticmethod
    def _format_bytes_or_time(value: int, value_type: str) -> str:
        """Format bytes or milliseconds to human readable"""
        if value_type == 'bytes':
            if value == 0:
                return "0 B"
            units = ['B', 'KB', 'MB', 'GB', 'TB']
            unit_index = 0
            float_value = float(value)
            while float_value >= 1024 and unit_index < len(units) - 1:
                float_value /= 1024
                unit_index += 1
            return f"{float_value:.2f} {units[unit_index]}"
        elif value_type == 'time':
            if value < 1000:
                return f"{value}ms"
            seconds = value / 1000
            if seconds < 60:
                return f"{seconds:.1f}s"
            minutes = seconds / 60
            if minutes < 60:
                return f"{minutes:.1f}m"
            hours = minutes / 60
            return f"{hours:.1f}h"
        return str(value)


# =============================================================================
# Custom JSON Encoder for dataclasses
# =============================================================================

class DataclassJSONEncoder(json.JSONEncoder):
    """JSON Encoder that handles dataclasses"""

    def default(self, obj):
        if hasattr(obj, '__dataclass_fields__'):
            return asdict(obj)
        return super().default(obj)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Analyze Spark applications from History Server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --url http://spark-history:18080 --app-id application_1234567890123_0001
  %(prog)s --url http://spark-history:18080 --app-id app-20231201120000-0001 --output report.json
  %(prog)s --url http://spark-history:18080 --list
        """
    )

    parser.add_argument('--url', required=True, help='Spark History Server URL (e.g., http://localhost:18080)')
    parser.add_argument('--app-id', help='Application ID to analyze')
    parser.add_argument('--list', action='store_true', help='List all applications')
    parser.add_argument('--output', help='Output file for JSON report (optional)')
    parser.add_argument('--json', action='store_true', help='Output JSON to stdout instead of text')
    parser.add_argument('--timeout', type=int, default=30, help='API request timeout in seconds')

    args = parser.parse_args()

    # Create client
    client = SparkHistoryServerClient(args.url, timeout=args.timeout)

    # List applications mode
    if args.list:
        print("Fetching applications...")
        apps = client.list_applications()
        print(f"\nFound {len(apps)} applications:\n")
        print(f"{'Application ID':<45} {'Name':<30} {'Status':<12} {'Duration'}")
        print("-" * 110)
        for app in apps[:50]:  # Limit to 50
            app_id = app.get('id', 'Unknown')
            name = app.get('name', 'Unknown')[:28]
            attempts = app.get('attempts', [{}])
            latest = attempts[0] if attempts else {}
            status = 'COMPLETED' if latest.get('completed', False) else 'RUNNING'
            duration = latest.get('duration', 0)
            duration_str = SparkApplicationAnalyzer._format_duration(duration)
            print(f"{app_id:<45} {name:<30} {status:<12} {duration_str}")

        if len(apps) > 50:
            print(f"\n... and {len(apps) - 50} more applications")
        return

    # Analyze application mode
    if not args.app_id:
        parser.error("--app-id is required for analysis (use --list to see available applications)")

    try:
        # Run analysis
        analyzer = SparkApplicationAnalyzer(client, args.app_id)
        report = analyzer.analyze()

        # Generate output
        if args.json:
            print(json.dumps(report, cls=DataclassJSONEncoder, indent=2))
        else:
            text_report = ReportGenerator.generate_text_report(report)
            print(text_report)

        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, cls=DataclassJSONEncoder, indent=2)
            print(f"\nJSON report saved to: {args.output}")

    except requests.exceptions.ConnectionError:
        print(f"Error: Could not connect to Spark History Server at {args.url}")
        print("Please check that the URL is correct and the server is running.")
        sys.exit(1)
    except Exception as e:
        print(f"Error analyzing application: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
