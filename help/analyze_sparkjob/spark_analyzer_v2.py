#!/usr/bin/env python3
"""
Spark Application Analyzer v2
=============================
Enhanced analyzer based on actual Spark History Server UI observations.

New features in v2:
- Executor loss detection (separate from failed tasks)
- Failed stages detection and reporting
- Over-provisioning detection (data per core ratio)
- Storage memory utilization analysis
- Job type detection (MLlib, SQL, Streaming)
- Active vs Dead executor breakdown
- Health score calculation
- More accurate recommendations

Usage:
    python spark_analyzer_v2.py --url http://history-server:18080 --app-id application_1234567890123_0001
    python spark_analyzer_v2.py --url http://history-server:18080 --list
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List
import requests


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ExecutorMetrics:
    # Counts
    total_executors: int = 0
    active_executors: int = 0
    dead_executors: int = 0
    
    # Resources
    total_cores: int = 0
    active_cores: int = 0
    
    # Memory (in MB)
    executor_memory_mb: int = 0
    total_cluster_memory_mb: int = 0
    
    # Storage Memory
    total_storage_memory_mb: int = 0
    storage_memory_used_mb: int = 0
    storage_utilization_pct: float = 0.0
    
    # Time metrics
    total_task_time_ms: int = 0
    total_gc_time_ms: int = 0
    gc_time_ratio: float = 0.0
    
    # I/O metrics
    total_input_bytes: int = 0
    total_output_bytes: int = 0
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    
    # Spill metrics
    total_disk_spill_bytes: int = 0
    total_memory_spill_bytes: int = 0
    
    # Task metrics
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    
    # Executor health
    executor_losses: int = 0
    
    # Efficiency metrics
    data_per_core_mb: float = 0.0
    memory_per_core_mb: float = 0.0
    
    # Details
    executor_details: list = field(default_factory=list)


@dataclass
class StageMetrics:
    stage_id: int = 0
    attempt_id: int = 0
    stage_name: str = ""
    status: str = ""
    num_tasks: int = 0
    num_complete_tasks: int = 0
    num_failed_tasks: int = 0
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
    failure_reason: str = ""


@dataclass
class StagesSummary:
    total_stages: int = 0
    completed_stages: int = 0
    failed_stages: int = 0
    active_stages: int = 0
    pending_stages: int = 0
    skipped_stages: int = 0
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    max_stage_shuffle_bytes: int = 0
    failed_stage_ids: list = field(default_factory=list)
    failed_stage_reasons: list = field(default_factory=list)


@dataclass
class JobMetrics:
    job_id: int = 0
    name: str = ""
    status: str = ""
    num_stages: int = 0
    num_tasks: int = 0
    num_completed_stages: int = 0
    num_failed_stages: int = 0
    num_skipped_stages: int = 0
    duration_ms: int = 0


@dataclass 
class SQLQueryMetrics:
    query_id: int = 0
    description: str = ""
    duration_ms: int = 0
    status: str = ""


@dataclass
class JobTypeAnalysis:
    detected_type: str = ""
    confidence: str = ""
    indicators: list = field(default_factory=list)
    algorithm_detected: str = ""


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
    serializer: str = ""
    all_configs: dict = field(default_factory=dict)


@dataclass
class Issue:
    severity: str
    category: str
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
    stages_summary: StagesSummary = field(default_factory=StagesSummary)
    stage_metrics: list = field(default_factory=list)
    job_metrics: list = field(default_factory=list)
    sql_metrics: list = field(default_factory=list)
    
    job_type: JobTypeAnalysis = field(default_factory=JobTypeAnalysis)
    configuration: ConfigurationAnalysis = field(default_factory=ConfigurationAnalysis)
    
    issues: list = field(default_factory=list)
    recommendations: dict = field(default_factory=dict)
    health_score: int = 100
    analysis_timestamp: str = ""


# =============================================================================
# API Client
# =============================================================================

class SparkHistoryServerClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_base = f"{self.base_url}/api/v1"
        self.timeout = timeout
        self.session = requests.Session()
    
    def _get(self, endpoint: str):
        url = f"{self.api_base}{endpoint}"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            raise
    
    def list_applications(self) -> list:
        return self._get("/applications")
    
    def get_application(self, app_id: str):
        return self._get(f"/applications/{app_id}")
    
    def get_jobs(self, app_id: str) -> list:
        return self._get(f"/applications/{app_id}/jobs")
    
    def get_stages(self, app_id: str) -> list:
        return self._get(f"/applications/{app_id}/stages")
    
    def get_stage_detail(self, app_id: str, stage_id: int, attempt_id: int = 0):
        try:
            return self._get(f"/applications/{app_id}/stages/{stage_id}/{attempt_id}")
        except:
            return {}
    
    def get_executors(self, app_id: str) -> list:
        return self._get(f"/applications/{app_id}/allexecutors")
    
    def get_environment(self, app_id: str):
        return self._get(f"/applications/{app_id}/environment")
    
    def get_sql_queries(self, app_id: str) -> list:
        try:
            return self._get(f"/applications/{app_id}/sql")
        except:
            return []


# =============================================================================
# Job Type Detector
# =============================================================================

class JobTypeDetector:
    MLLIB_PATTERNS = {
        'GeneralizedLinearRegression': r'GeneralizedLinearRegression',
        'LinearRegression': r'LinearRegression(?!.*Generalized)',
        'LogisticRegression': r'LogisticRegression',
        'DecisionTree': r'DecisionTree',
        'RandomForest': r'RandomForest',
        'GradientBoostedTrees': r'GBT|GradientBoosted',
        'KMeans': r'KMeans',
        'ALS': r'\bALS\b',
        'PCA': r'\bPCA\b',
        'WeightedLeastSquares': r'WeightedLeastSquares',
        'TreeAggregate': r'treeAggregate',
    }
    
    SQL_PATTERNS = [r'Exchange', r'BroadcastExchange', r'HashAggregate', 
                   r'SortMergeJoin', r'BroadcastHashJoin', r'Scan parquet', r'FileScan']
    
    STREAMING_PATTERNS = [r'MicroBatch', r'StreamExecution', r'Streaming', r'Kafka']
    
    @classmethod
    def detect(cls, stage_names: List[str]) -> JobTypeAnalysis:
        result = JobTypeAnalysis()
        all_names = ' '.join(stage_names)
        
        # Check MLlib
        mllib_indicators = []
        algorithm = ""
        for algo_name, pattern in cls.MLLIB_PATTERNS.items():
            if re.search(pattern, all_names, re.IGNORECASE):
                mllib_indicators.append(algo_name)
                if not algorithm:
                    algorithm = algo_name
        
        if mllib_indicators:
            result.detected_type = "MLlib"
            result.algorithm_detected = algorithm
            result.indicators = mllib_indicators
            result.confidence = "High" if len(mllib_indicators) >= 2 else "Medium"
            return result
        
        # Check Streaming
        streaming_count = sum(1 for p in cls.STREAMING_PATTERNS if re.search(p, all_names, re.IGNORECASE))
        if streaming_count > 0:
            result.detected_type = "Streaming"
            result.confidence = "High" if streaming_count >= 2 else "Medium"
            result.indicators = [p for p in cls.STREAMING_PATTERNS if re.search(p, all_names, re.IGNORECASE)]
            return result
        
        # Check SQL
        sql_count = sum(1 for p in cls.SQL_PATTERNS if re.search(p, all_names, re.IGNORECASE))
        if sql_count >= 2:
            result.detected_type = "SparkSQL"
            result.confidence = "High" if sql_count >= 4 else "Medium"
            result.indicators = [p for p in cls.SQL_PATTERNS if re.search(p, all_names, re.IGNORECASE)][:5]
            return result
        
        result.detected_type = "ETL/RDD"
        result.confidence = "Low"
        result.indicators = ["No specific patterns detected"]
        return result


# =============================================================================
# Analyzer
# =============================================================================

class SparkApplicationAnalyzer:
    def __init__(self, client: SparkHistoryServerClient, app_id: str):
        self.client = client
        self.app_id = app_id
        self.report = AnalysisReport()
    
    def analyze(self) -> AnalysisReport:
        print(f"Analyzing application: {self.app_id}")
        
        self._analyze_application_info()
        self._analyze_environment()
        self._analyze_executors()
        self._analyze_jobs()
        self._analyze_stages()
        self._analyze_sql()
        self._detect_job_type()
        self._calculate_efficiency_metrics()
        self._identify_issues()
        self._generate_recommendations()
        self._calculate_health_score()
        
        self.report.analysis_timestamp = datetime.now().isoformat()
        return self.report
    
    def _analyze_application_info(self):
        print("  - Fetching application info...")
        app_info = self.client.get_application(self.app_id)
        
        if isinstance(app_info, list):
            app_info = app_info[0] if app_info else {}
        
        attempts = app_info.get('attempts', [{}])
        latest = attempts[0] if attempts else {}
        
        self.report.application_id = self.app_id
        self.report.application_name = app_info.get('name', 'Unknown')
        self.report.start_time = latest.get('startTime', '')
        self.report.end_time = latest.get('endTime', '')
        self.report.duration_ms = latest.get('duration', 0)
        self.report.duration_human = self._format_duration(self.report.duration_ms)
        self.report.spark_version = latest.get('sparkVersion', 'Unknown')
        self.report.user = latest.get('sparkUser', 'Unknown')
    
    def _analyze_environment(self):
        print("  - Fetching configuration...")
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
        config.serializer = spark_props.get('spark.serializer', 'JavaSerializer')
        config.all_configs = spark_props
    
    def _analyze_executors(self):
        print("  - Fetching executor metrics...")
        executors = self.client.get_executors(self.app_id)
        
        metrics = self.report.executor_metrics
        
        for exec_info in executors:
            exec_id = exec_info.get('id', '')
            is_driver = exec_id == 'driver'
            is_active = exec_info.get('isActive', False)
            
            max_mem = exec_info.get('maxMemory', 0)
            mem_used = exec_info.get('memoryUsed', 0)
            
            if not is_driver:
                metrics.total_executors += 1
                cores = exec_info.get('totalCores', 0)
                
                if is_active:
                    metrics.active_executors += 1
                    metrics.active_cores += cores
                else:
                    metrics.dead_executors += 1
                    # Count as loss if died with work done
                    if exec_info.get('totalTasks', 0) > 0:
                        metrics.executor_losses += 1
                
                metrics.total_cores += cores
                
                if max_mem > 0:
                    exec_mem_mb = max_mem // (1024 * 1024)
                    if metrics.executor_memory_mb == 0:
                        metrics.executor_memory_mb = exec_mem_mb
                    metrics.total_cluster_memory_mb += exec_mem_mb
                
                metrics.total_storage_memory_mb += max_mem // (1024 * 1024)
                metrics.storage_memory_used_mb += mem_used // (1024 * 1024)
            
            metrics.total_task_time_ms += exec_info.get('totalDuration', 0)
            metrics.total_gc_time_ms += exec_info.get('totalGCTime', 0)
            metrics.total_input_bytes += exec_info.get('totalInputBytes', 0)
            metrics.total_output_bytes += exec_info.get('totalOutputBytes', 0)
            metrics.total_shuffle_read_bytes += exec_info.get('totalShuffleRead', 0)
            metrics.total_shuffle_write_bytes += exec_info.get('totalShuffleWrite', 0)
            metrics.total_tasks_completed += exec_info.get('totalTasks', 0)
            metrics.total_tasks_failed += exec_info.get('failedTasks', 0)
            
            # Store details
            task_time = exec_info.get('totalDuration', 0)
            gc_time = exec_info.get('totalGCTime', 0)
            gc_ratio = (gc_time / task_time * 100) if task_time > 0 else 0
            
            metrics.executor_details.append({
                'id': exec_id,
                'host': exec_info.get('hostPort', ''),
                'is_active': is_active,
                'cores': exec_info.get('totalCores', 0),
                'max_memory_mb': max_mem // (1024 * 1024),
                'memory_used_mb': mem_used // (1024 * 1024),
                'task_time_ms': task_time,
                'gc_time_ms': gc_time,
                'gc_ratio': gc_ratio,
                'tasks_completed': exec_info.get('totalTasks', 0),
                'tasks_failed': exec_info.get('failedTasks', 0),
            })
        
        if metrics.total_task_time_ms > 0:
            metrics.gc_time_ratio = (metrics.total_gc_time_ms / metrics.total_task_time_ms) * 100
        
        if metrics.total_storage_memory_mb > 0:
            metrics.storage_utilization_pct = (metrics.storage_memory_used_mb / metrics.total_storage_memory_mb) * 100
    
    def _analyze_jobs(self):
        print("  - Fetching job metrics...")
        jobs = self.client.get_jobs(self.app_id)
        
        for job in jobs:
            job_metrics = JobMetrics(
                job_id=job.get('jobId', 0),
                name=job.get('name', ''),
                status=job.get('status', ''),
                num_stages=job.get('numStages', 0),
                num_tasks=job.get('numTasks', 0),
                num_completed_stages=job.get('numCompletedStages', 0),
                num_failed_stages=job.get('numFailedStages', 0),
                num_skipped_stages=job.get('numSkippedStages', 0),
            )
            self.report.job_metrics.append(job_metrics)
    
    def _analyze_stages(self):
        print("  - Fetching stage metrics...")
        stages = self.client.get_stages(self.app_id)
        
        summary = self.report.stages_summary
        total_disk_spill = 0
        total_memory_spill = 0
        
        for stage in stages:
            stage_id = stage.get('stageId', 0)
            attempt_id = stage.get('attemptId', 0)
            status = stage.get('status', 'UNKNOWN')
            
            summary.total_stages += 1
            if status == 'COMPLETE':
                summary.completed_stages += 1
            elif status == 'FAILED':
                summary.failed_stages += 1
                summary.failed_stage_ids.append(stage_id)
                summary.failed_stage_reasons.append(stage.get('failureReason', 'Unknown')[:200])
            elif status == 'ACTIVE':
                summary.active_stages += 1
            elif status == 'PENDING':
                summary.pending_stages += 1
            elif status == 'SKIPPED':
                summary.skipped_stages += 1
            
            # Get detailed metrics
            stage_detail = self.client.get_stage_detail(self.app_id, stage_id, attempt_id)
            if not stage_detail:
                stage_detail = stage
            
            task_quantiles = stage_detail.get('taskMetricsDistributions', {})
            executor_run_time = task_quantiles.get('executorRunTime', [0, 0, 0, 0, 0])
            
            task_time_min = int(executor_run_time[0]) if len(executor_run_time) > 0 else 0
            task_time_p25 = int(executor_run_time[1]) if len(executor_run_time) > 1 else 0
            task_time_median = int(executor_run_time[2]) if len(executor_run_time) > 2 else 0
            task_time_p75 = int(executor_run_time[3]) if len(executor_run_time) > 3 else 0
            task_time_max = int(executor_run_time[4]) if len(executor_run_time) > 4 else 0
            
            skew_ratio = (task_time_max / task_time_median) if task_time_median > 0 else 0.0
            
            disk_spill = stage.get('diskBytesSpilled', 0)
            memory_spill = stage.get('memoryBytesSpilled', 0)
            total_disk_spill += disk_spill
            total_memory_spill += memory_spill
            
            shuffle_read = stage.get('shuffleReadBytes', 0)
            shuffle_write = stage.get('shuffleWriteBytes', 0)
            summary.total_shuffle_read_bytes += shuffle_read
            summary.total_shuffle_write_bytes += shuffle_write
            if shuffle_write > summary.max_stage_shuffle_bytes:
                summary.max_stage_shuffle_bytes = shuffle_write
            
            stage_metrics = StageMetrics(
                stage_id=stage_id,
                attempt_id=attempt_id,
                stage_name=stage.get('name', ''),
                status=status,
                num_tasks=stage.get('numTasks', 0),
                num_complete_tasks=stage.get('numCompleteTasks', 0),
                num_failed_tasks=stage.get('numFailedTasks', 0),
                duration_ms=stage.get('executorRunTime', 0),
                input_bytes=stage.get('inputBytes', 0),
                output_bytes=stage.get('outputBytes', 0),
                shuffle_read_bytes=shuffle_read,
                shuffle_write_bytes=shuffle_write,
                disk_spill_bytes=disk_spill,
                memory_spill_bytes=memory_spill,
                task_time_min_ms=task_time_min,
                task_time_max_ms=task_time_max,
                task_time_median_ms=task_time_median,
                task_time_p75_ms=task_time_p75,
                task_time_p25_ms=task_time_p25,
                skew_ratio=skew_ratio,
                gc_time_ms=stage.get('jvmGcTime', 0),
                failure_reason=stage.get('failureReason', ''),
            )
            self.report.stage_metrics.append(stage_metrics)
        
        self.report.executor_metrics.total_disk_spill_bytes = total_disk_spill
        self.report.executor_metrics.total_memory_spill_bytes = total_memory_spill
    
    def _analyze_sql(self):
        print("  - Fetching SQL metrics...")
        sql_queries = self.client.get_sql_queries(self.app_id)
        
        for query in sql_queries:
            sql_metrics = SQLQueryMetrics(
                query_id=query.get('id', 0),
                description=query.get('description', '')[:200],
                duration_ms=query.get('duration', 0),
                status=query.get('status', ''),
            )
            self.report.sql_metrics.append(sql_metrics)
    
    def _detect_job_type(self):
        print("  - Detecting job type...")
        stage_names = [s.stage_name for s in self.report.stage_metrics]
        self.report.job_type = JobTypeDetector.detect(stage_names)
    
    def _calculate_efficiency_metrics(self):
        print("  - Calculating efficiency metrics...")
        metrics = self.report.executor_metrics
        
        if metrics.total_cores > 0:
            metrics.data_per_core_mb = metrics.total_input_bytes / (1024 * 1024) / metrics.total_cores
        
        if metrics.total_cores > 0 and metrics.total_cluster_memory_mb > 0:
            metrics.memory_per_core_mb = metrics.total_cluster_memory_mb / metrics.total_cores
    
    def _identify_issues(self):
        print("  - Identifying issues...")
        issues = []
        exec_metrics = self.report.executor_metrics
        stages_summary = self.report.stages_summary
        config = self.report.configuration
        
        # 1. EXECUTOR LOSS
        if exec_metrics.executor_losses > 0 or exec_metrics.dead_executors > 5:
            loss_count = max(exec_metrics.executor_losses, exec_metrics.dead_executors)
            severity = "HIGH" if loss_count > 10 else "MEDIUM" if loss_count > 3 else "LOW"
            issues.append(Issue(
                severity=severity,
                category="EXECUTOR_LOSS",
                description=f"Executor losses: {exec_metrics.dead_executors} dead executors ({exec_metrics.executor_losses} likely killed by YARN)",
                current_value=f"{exec_metrics.dead_executors} dead, {exec_metrics.active_executors} active",
                recommendation="Increase spark.executor.memoryOverhead to 15-20% of executor memory",
                config_key="spark.executor.memoryOverhead",
                suggested_value=self._suggest_overhead(config.executor_memory)
            ))
        
        # 2. FAILED STAGES
        if stages_summary.failed_stages > 0:
            severity = "HIGH" if stages_summary.failed_stages > 2 else "MEDIUM"
            reasons = '; '.join(stages_summary.failed_stage_reasons[:3]) if stages_summary.failed_stage_reasons else 'Unknown'
            issues.append(Issue(
                severity=severity,
                category="FAILED_STAGES",
                description=f"{stages_summary.failed_stages} stages failed. Reasons: {reasons[:150]}",
                current_value=f"Failed stage IDs: {stages_summary.failed_stage_ids[:5]}",
                recommendation="Check stage failure details. Common causes: OOM, fetch failures, data corruption",
                config_key=None,
                suggested_value=None
            ))
        
        # 3. OVER-PROVISIONED
        if exec_metrics.data_per_core_mb > 0 and exec_metrics.data_per_core_mb < 100:
            total_data_gb = exec_metrics.total_input_bytes / (1024**3)
            recommended_cores = max(int(total_data_gb * 0.5), 20)
            recommended_executors = max(recommended_cores // 4, 5)
            
            issues.append(Issue(
                severity="MEDIUM",
                category="OVER_PROVISIONED",
                description=f"Cluster over-provisioned: {exec_metrics.data_per_core_mb:.0f} MB/core (ideal: 500MB-2GB)",
                current_value=f"{exec_metrics.total_cores} cores for {total_data_gb:.1f} GiB data",
                recommendation=f"Reduce to ~{recommended_executors} executors ({recommended_cores} cores)",
                config_key="spark.dynamicAllocation.maxExecutors",
                suggested_value=str(recommended_executors)
            ))
        
        # 4. UNUSED STORAGE MEMORY
        if exec_metrics.total_storage_memory_mb > 1000 and exec_metrics.storage_utilization_pct < 1:
            issues.append(Issue(
                severity="LOW",
                category="MEMORY",
                description=f"Storage memory unused: {exec_metrics.total_storage_memory_mb/1024:.1f} GiB allocated, {exec_metrics.storage_utilization_pct:.1f}% used",
                current_value=f"{exec_metrics.storage_memory_used_mb} MB / {exec_metrics.total_storage_memory_mb} MB",
                recommendation="Consider caching DataFrames, or reduce spark.memory.storageFraction",
                config_key="spark.memory.storageFraction",
                suggested_value="0.3"
            ))
        
        # 5. HIGH GC
        if exec_metrics.gc_time_ratio > 10:
            severity = "HIGH" if exec_metrics.gc_time_ratio > 20 else "MEDIUM"
            issues.append(Issue(
                severity=severity,
                category="GC",
                description=f"High GC time: {exec_metrics.gc_time_ratio:.1f}% of task time",
                current_value=f"{exec_metrics.gc_time_ratio:.1f}%",
                recommendation="Increase executor memory or reduce cores per executor",
                config_key="spark.executor.memory",
                suggested_value=self._suggest_memory_increase(config.executor_memory)
            ))
        
        # 6. DISK SPILL
        if exec_metrics.total_disk_spill_bytes > 0:
            spill_gb = exec_metrics.total_disk_spill_bytes / (1024**3)
            severity = "HIGH" if spill_gb > 10 else "MEDIUM" if spill_gb > 1 else "LOW"
            issues.append(Issue(
                severity=severity,
                category="MEMORY",
                description=f"Disk spill: {spill_gb:.2f} GB spilled",
                current_value=f"{spill_gb:.2f} GB",
                recommendation="Increase executor memory",
                config_key="spark.executor.memory",
                suggested_value=self._suggest_memory_increase(config.executor_memory)
            ))
        
        # 7. TASK SKEW
        skewed_stages = [s for s in self.report.stage_metrics if s.skew_ratio > 3 and s.num_tasks > 1]
        if skewed_stages:
            worst_skew = max(s.skew_ratio for s in skewed_stages)
            severity = "HIGH" if worst_skew > 10 else "MEDIUM"
            issues.append(Issue(
                severity=severity,
                category="SKEW",
                description=f"Task skew in {len(skewed_stages)} stages. Worst: {worst_skew:.1f}x",
                current_value=f"{worst_skew:.1f}x (max/median)",
                recommendation="Enable AQE or salt skewed keys",
                config_key="spark.sql.adaptive.enabled",
                suggested_value="true"
            ))
        
        # 8. SHUFFLE PARTITIONS
        shuffle_partitions = int(config.shuffle_partitions) if config.shuffle_partitions.isdigit() else 200
        total_shuffle = exec_metrics.total_shuffle_read_bytes + exec_metrics.total_shuffle_write_bytes
        
        if total_shuffle > 0:
            avg_partition_mb = (total_shuffle / shuffle_partitions) / (1024 * 1024)
            
            if avg_partition_mb > 500:
                optimal = int(total_shuffle / (150 * 1024 * 1024))
                issues.append(Issue(
                    severity="MEDIUM",
                    category="PARALLELISM",
                    description=f"Shuffle partitions too few. Avg: {avg_partition_mb:.0f} MB (target: 128-200 MB)",
                    current_value=f"{shuffle_partitions} partitions",
                    recommendation="Increase shuffle partitions",
                    config_key="spark.sql.shuffle.partitions",
                    suggested_value=str(optimal)
                ))
            elif avg_partition_mb < 10 and shuffle_partitions > 100:
                optimal = max(int(total_shuffle / (128 * 1024 * 1024)), 20)
                issues.append(Issue(
                    severity="LOW",
                    category="PARALLELISM",
                    description=f"Shuffle partitions too many. Avg: {avg_partition_mb:.1f} MB",
                    current_value=f"{shuffle_partitions} partitions",
                    recommendation="Reduce shuffle partitions or enable AQE",
                    config_key="spark.sql.shuffle.partitions",
                    suggested_value=str(optimal)
                ))
        
        # 9. AQE NOT ENABLED
        if config.adaptive_enabled.lower() != 'true' and self.report.spark_version >= '3':
            issues.append(Issue(
                severity="MEDIUM",
                category="CONFIGURATION",
                description="AQE not enabled (recommended for Spark 3+)",
                current_value="false",
                recommendation="Enable for automatic optimization",
                config_key="spark.sql.adaptive.enabled",
                suggested_value="true"
            ))
        
        # 10. FAILED TASKS
        if exec_metrics.total_tasks_failed > 0:
            total = exec_metrics.total_tasks_completed + exec_metrics.total_tasks_failed
            rate = (exec_metrics.total_tasks_failed / total * 100) if total > 0 else 0
            severity = "HIGH" if rate > 5 else "MEDIUM" if rate > 1 else "LOW"
            issues.append(Issue(
                severity=severity,
                category="RELIABILITY",
                description=f"{exec_metrics.total_tasks_failed} tasks failed ({rate:.1f}%)",
                current_value=f"{exec_metrics.total_tasks_failed} failed",
                recommendation="Check executor logs, increase memory overhead",
                config_key="spark.executor.memoryOverhead",
                suggested_value=self._suggest_overhead(config.executor_memory)
            ))
        
        # Sort by severity
        severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        issues.sort(key=lambda x: severity_order.get(x.severity, 3))
        
        self.report.issues = issues
    
    def _generate_recommendations(self):
        print("  - Generating recommendations...")
        recommendations = {}
        
        for issue in self.report.issues:
            if issue.config_key and issue.suggested_value:
                if issue.config_key not in recommendations:
                    recommendations[issue.config_key] = {
                        'current': self._get_current_config(issue.config_key),
                        'suggested': issue.suggested_value,
                        'reason': issue.description
                    }
        
        self.report.recommendations = recommendations
    
    def _calculate_health_score(self):
        score = 100
        for issue in self.report.issues:
            if issue.severity == "HIGH":
                score -= 20
            elif issue.severity == "MEDIUM":
                score -= 10
            else:
                score -= 5
        self.report.health_score = max(0, score)
    
    def _get_current_config(self, key: str) -> str:
        config = self.report.configuration
        config_map = {
            'spark.executor.memory': config.executor_memory,
            'spark.executor.cores': config.executor_cores,
            'spark.executor.instances': config.executor_instances,
            'spark.executor.memoryOverhead': config.executor_memory_overhead,
            'spark.sql.shuffle.partitions': config.shuffle_partitions,
            'spark.sql.adaptive.enabled': config.adaptive_enabled,
            'spark.memory.storageFraction': config.storage_fraction,
            'spark.dynamicAllocation.maxExecutors': config.executor_instances,
        }
        return config_map.get(key, config.all_configs.get(key, 'Not Set'))
    
    def _suggest_memory_increase(self, current: str) -> str:
        if not current or current == 'Not Set':
            return '8g'
        current = current.lower().strip()
        if current.endswith('g'):
            return f"{int(int(current[:-1]) * 1.5)}g"
        elif current.endswith('m'):
            return f"{int(int(current[:-1]) * 1.5)}m"
        return '8g'
    
    def _suggest_overhead(self, executor_memory: str) -> str:
        if not executor_memory or executor_memory == 'Not Set':
            return '3g'
        executor_memory = executor_memory.lower().strip()
        if executor_memory.endswith('g'):
            overhead = max(int(int(executor_memory[:-1]) * 0.20), 2)
            return f"{overhead}g"
        elif executor_memory.endswith('m'):
            overhead = max(int(int(executor_memory[:-1]) * 0.20), 1024)
            return f"{overhead}m"
        return '3g'
    
    @staticmethod
    def _format_duration(ms: int) -> str:
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
    @staticmethod
    def generate_text_report(report: AnalysisReport) -> str:
        lines = []
        
        lines.append("=" * 80)
        lines.append("SPARK APPLICATION ANALYSIS REPORT (v2)")
        lines.append("=" * 80)
        lines.append("")
        
        # Health Score
        score = report.health_score
        indicator = "✓ GOOD" if score >= 80 else "⚠ WARNING" if score >= 60 else "✗ CRITICAL"
        lines.append(f"HEALTH SCORE: {indicator} ({score}/100)")
        lines.append("")
        
        # Application Info
        lines.append("APPLICATION INFORMATION")
        lines.append("-" * 40)
        lines.append(f"  Application ID:   {report.application_id}")
        lines.append(f"  Application Name: {report.application_name}")
        lines.append(f"  Spark Version:    {report.spark_version}")
        lines.append(f"  User:             {report.user}")
        lines.append(f"  Duration:         {report.duration_human}")
        lines.append("")
        
        # Job Type
        job_type = report.job_type
        lines.append("JOB TYPE DETECTION")
        lines.append("-" * 40)
        lines.append(f"  Type:       {job_type.detected_type}")
        lines.append(f"  Confidence: {job_type.confidence}")
        if job_type.algorithm_detected:
            lines.append(f"  Algorithm:  {job_type.algorithm_detected}")
        lines.append(f"  Indicators: {', '.join(job_type.indicators[:5])}")
        lines.append("")
        
        # Configuration
        config = report.configuration
        lines.append("CURRENT CONFIGURATION")
        lines.append("-" * 40)
        lines.append(f"  Executor Memory:      {config.executor_memory}")
        lines.append(f"  Executor Cores:       {config.executor_cores}")
        lines.append(f"  Memory Overhead:      {config.executor_memory_overhead}")
        lines.append(f"  Shuffle Partitions:   {config.shuffle_partitions}")
        lines.append(f"  Dynamic Allocation:   {config.dynamic_allocation}")
        lines.append(f"  AQE Enabled:          {config.adaptive_enabled}")
        lines.append("")
        
        # Executor Metrics
        em = report.executor_metrics
        lines.append("EXECUTOR METRICS")
        lines.append("-" * 40)
        lines.append(f"  Executors:        {em.total_executors} total ({em.active_executors} active, {em.dead_executors} dead)")
        lines.append(f"  Executor Losses:  {em.executor_losses}")
        lines.append(f"  Total Cores:      {em.total_cores} ({em.active_cores} active)")
        lines.append(f"  Memory/Executor:  {em.executor_memory_mb:,} MB")
        lines.append(f"  Cluster Memory:   {em.total_cluster_memory_mb/1024:.1f} GiB")
        lines.append("")
        lines.append(f"  Storage Memory:   {em.storage_memory_used_mb:,} / {em.total_storage_memory_mb:,} MB ({em.storage_utilization_pct:.1f}%)")
        lines.append("")
        lines.append(f"  Tasks:            {em.total_tasks_completed:,} completed, {em.total_tasks_failed:,} failed")
        lines.append(f"  Task Time:        {ReportGenerator._format_time(em.total_task_time_ms)}")
        lines.append(f"  GC Time:          {ReportGenerator._format_time(em.total_gc_time_ms)} ({em.gc_time_ratio:.2f}%)")
        lines.append("")
        lines.append(f"  Input:            {ReportGenerator._format_bytes(em.total_input_bytes)}")
        lines.append(f"  Shuffle Read:     {ReportGenerator._format_bytes(em.total_shuffle_read_bytes)}")
        lines.append(f"  Shuffle Write:    {ReportGenerator._format_bytes(em.total_shuffle_write_bytes)}")
        lines.append(f"  Disk Spill:       {ReportGenerator._format_bytes(em.total_disk_spill_bytes)}")
        lines.append("")
        lines.append(f"  Data/Core:        {em.data_per_core_mb:.1f} MB")
        lines.append(f"  Memory/Core:      {em.memory_per_core_mb:.1f} MB")
        lines.append("")
        
        # Stage Summary
        ss = report.stages_summary
        lines.append("STAGE SUMMARY")
        lines.append("-" * 40)
        lines.append(f"  Completed: {ss.completed_stages}  |  Failed: {ss.failed_stages}  |  Active: {ss.active_stages}  |  Skipped: {ss.skipped_stages}")
        if ss.failed_stages > 0:
            lines.append(f"  Failed IDs: {ss.failed_stage_ids[:10]}")
        lines.append("")
        
        # Top Stages
        if report.stage_metrics:
            lines.append("TOP STAGES BY DURATION")
            lines.append("-" * 40)
            sorted_stages = sorted(report.stage_metrics, key=lambda s: s.duration_ms, reverse=True)[:10]
            lines.append(f"  {'ID':<5} {'Tasks':<8} {'Duration':<10} {'Skew':<8} {'Input':<12} {'Status'}")
            lines.append(f"  {'-'*5} {'-'*8} {'-'*10} {'-'*8} {'-'*12} {'-'*8}")
            for s in sorted_stages:
                dur = ReportGenerator._format_time(s.duration_ms)
                skew = f"{s.skew_ratio:.1f}x" if s.skew_ratio > 0 else "N/A"
                inp = ReportGenerator._format_bytes(s.input_bytes)
                status = "✓" if s.status == "COMPLETE" else "✗" if s.status == "FAILED" else s.status[:6]
                lines.append(f"  {s.stage_id:<5} {s.num_tasks:<8} {dur:<10} {skew:<8} {inp:<12} {status}")
            lines.append("")
        
        # Issues
        if report.issues:
            lines.append("ISSUES IDENTIFIED")
            lines.append("-" * 40)
            for i, issue in enumerate(report.issues, 1):
                sev = "!!!" if issue.severity == "HIGH" else "! " if issue.severity == "MEDIUM" else "  "
                lines.append(f"  {i}. [{sev}] {issue.category}: {issue.description}")
                lines.append(f"       Current: {issue.current_value}")
                lines.append(f"       Fix: {issue.recommendation}")
                if issue.config_key:
                    lines.append(f"       Config: {issue.config_key}={issue.suggested_value}")
                lines.append("")
        else:
            lines.append("ISSUES IDENTIFIED")
            lines.append("-" * 40)
            lines.append("  ✓ No significant issues detected!")
            lines.append("")
        
        # Recommendations
        if report.recommendations:
            lines.append("RECOMMENDED SPARK-SUBMIT")
            lines.append("-" * 40)
            lines.append("  spark-submit \\")
            for key, rec in report.recommendations.items():
                lines.append(f"    --conf {key}={rec['suggested']} \\")
            lines.append("    your_app.py")
            lines.append("")
        
        lines.append("=" * 80)
        lines.append(f"Generated: {report.analysis_timestamp}")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_bytes(value: int) -> str:
        if value == 0:
            return "0 B"
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        idx = 0
        fval = float(value)
        while fval >= 1024 and idx < len(units) - 1:
            fval /= 1024
            idx += 1
        return f"{fval:.1f} {units[idx]}"
    
    @staticmethod
    def _format_time(ms: int) -> str:
        if ms < 1000:
            return f"{ms}ms"
        sec = ms / 1000
        if sec < 60:
            return f"{sec:.1f}s"
        mins = sec / 60
        if mins < 60:
            return f"{mins:.1f}m"
        return f"{mins/60:.1f}h"


# =============================================================================
# JSON Encoder
# =============================================================================

class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, '__dataclass_fields__'):
            return asdict(obj)
        return super().default(obj)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Spark Application Analyzer v2')
    parser.add_argument('--url', required=True, help='Spark History Server URL')
    parser.add_argument('--app-id', help='Application ID')
    parser.add_argument('--list', action='store_true', help='List applications')
    parser.add_argument('--output', help='Output JSON file')
    parser.add_argument('--json', action='store_true', help='Output JSON to stdout')
    parser.add_argument('--timeout', type=int, default=30)
    
    args = parser.parse_args()
    
    client = SparkHistoryServerClient(args.url, timeout=args.timeout)
    
    if args.list:
        print("Fetching applications...")
        apps = client.list_applications()
        print(f"\nFound {len(apps)} applications:\n")
        print(f"{'Application ID':<45} {'Name':<25} {'Status':<10} {'Duration'}")
        print("-" * 100)
        for app in apps[:50]:
            app_id = app.get('id', 'Unknown')
            name = app.get('name', 'Unknown')[:23]
            attempts = app.get('attempts', [{}])
            latest = attempts[0] if attempts else {}
            status = 'DONE' if latest.get('completed', False) else 'RUNNING'
            duration = SparkApplicationAnalyzer._format_duration(latest.get('duration', 0))
            print(f"{app_id:<45} {name:<25} {status:<10} {duration}")
        return
    
    if not args.app_id:
        parser.error("--app-id required (use --list to see apps)")
    
    try:
        analyzer = SparkApplicationAnalyzer(client, args.app_id)
        report = analyzer.analyze()
        
        if args.json:
            print(json.dumps(report, cls=DataclassJSONEncoder, indent=2))
        else:
            print(ReportGenerator.generate_text_report(report))
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, cls=DataclassJSONEncoder, indent=2)
            print(f"\nSaved to: {args.output}")
            
    except requests.exceptions.ConnectionError:
        print(f"Error: Cannot connect to {args.url}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
