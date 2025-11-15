#!/usr/bin/env python3
"""
Spark Event Log Analyzer and Optimizer for AWS EMR
Analyzes Spark history event logs and provides configuration and code recommendations
"""

import json
import gzip
import argparse
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any
from datetime import datetime


class SparkEventLogAnalyzer:
    def __init__(self, log_path: str):
        self.log_path = log_path
        self.events = []
        self.metrics = {
            'total_tasks': 0,
            'failed_tasks': 0,
            'total_stages': 0,
            'shuffle_read_bytes': 0,
            'shuffle_write_bytes': 0,
            'executor_run_time': 0,
            'executor_cpu_time': 0,
            'jvm_gc_time': 0,
            'result_size': 0,
            'memory_bytes_spilled': 0,
            'disk_bytes_spilled': 0,
            'peak_execution_memory': 0,
            'input_bytes': 0,
            'output_bytes': 0,
            'executors': set(),
            'task_durations': [],
            'skewed_tasks': [],
            'stages_info': {},
            'job_duration': 0
        }
        
    def parse_event_log(self):
        """Parse Spark event log file (supports both gzip and plain text)"""
        print(f"Parsing event log: {self.log_path}")
        
        try:
            if self.log_path.endswith('.gz'):
                with gzip.open(self.log_path, 'rt', encoding='utf-8') as f:
                    self._read_events(f)
            else:
                with open(self.log_path, 'r', encoding='utf-8') as f:
                    self._read_events(f)
                    
            print(f"Parsed {len(self.events)} events")
            return True
        except Exception as e:
            print(f"Error parsing log file: {e}")
            return False
    
    def _read_events(self, file_handle):
        """Read events from file handle"""
        for line in file_handle:
            line = line.strip()
            if line:
                try:
                    event = json.loads(line)
                    self.events.append(event)
                except json.JSONDecodeError:
                    continue
    
    def analyze_events(self):
        """Analyze parsed events and extract metrics"""
        print("\nAnalyzing events...")
        
        app_start_time = None
        app_end_time = None
        
        for event in self.events:
            event_type = event.get('Event')
            
            # Application timing
            if event_type == 'SparkListenerApplicationStart':
                app_start_time = event.get('Timestamp')
                
            elif event_type == 'SparkListenerApplicationEnd':
                app_end_time = event.get('Timestamp')
                
            # Executor tracking
            elif event_type == 'SparkListenerExecutorAdded':
                executor_id = event.get('Executor ID')
                self.metrics['executors'].add(executor_id)
                
            # Stage completion
            elif event_type == 'SparkListenerStageCompleted':
                self._analyze_stage(event)
                
            # Task completion
            elif event_type == 'SparkListenerTaskEnd':
                self._analyze_task(event)
        
        # Calculate job duration
        if app_start_time and app_end_time:
            self.metrics['job_duration'] = (app_end_time - app_start_time) / 1000  # Convert to seconds
        
        print(f"Analysis complete. Processed {self.metrics['total_tasks']} tasks across {self.metrics['total_stages']} stages")
    
    def _analyze_stage(self, event):
        """Analyze stage completion event"""
        stage_info = event.get('Stage Info', {})
        stage_id = stage_info.get('Stage ID')
        
        self.metrics['total_stages'] += 1
        self.metrics['stages_info'][stage_id] = {
            'name': stage_info.get('Stage Name', ''),
            'num_tasks': stage_info.get('Number of Tasks', 0),
            'parent_ids': stage_info.get('Parent IDs', [])
        }
    
    def _analyze_task(self, event):
        """Analyze task completion event"""
        task_info = event.get('Task Info', {})
        task_metrics = event.get('Task Metrics', {})
        
        if not task_metrics:
            return
        
        self.metrics['total_tasks'] += 1
        
        # Task failures
        if not task_info.get('Successful', True):
            self.metrics['failed_tasks'] += 1
        
        # Task duration
        launch_time = task_info.get('Launch Time', 0)
        finish_time = task_info.get('Finish Time', 0)
        duration = finish_time - launch_time
        self.metrics['task_durations'].append(duration)
        
        # Executor metrics
        self.metrics['executor_run_time'] += task_metrics.get('Executor Run Time', 0)
        self.metrics['executor_cpu_time'] += task_metrics.get('Executor CPU Time', 0)
        self.metrics['jvm_gc_time'] += task_metrics.get('JVM GC Time', 0)
        self.metrics['result_size'] += task_metrics.get('Result Size', 0)
        
        # Memory and spill metrics
        self.metrics['memory_bytes_spilled'] += task_metrics.get('Memory Bytes Spilled', 0)
        self.metrics['disk_bytes_spilled'] += task_metrics.get('Disk Bytes Spilled', 0)
        self.metrics['peak_execution_memory'] = max(
            self.metrics['peak_execution_memory'],
            task_metrics.get('Peak Execution Memory', 0)
        )
        
        # Shuffle metrics
        shuffle_read = task_metrics.get('Shuffle Read Metrics', {})
        shuffle_write = task_metrics.get('Shuffle Write Metrics', {})
        
        self.metrics['shuffle_read_bytes'] += shuffle_read.get('Total Bytes Read', 0)
        self.metrics['shuffle_write_bytes'] += shuffle_write.get('Shuffle Bytes Written', 0)
        
        # Input/Output metrics
        input_metrics = task_metrics.get('Input Metrics', {})
        output_metrics = task_metrics.get('Output Metrics', {})
        
        self.metrics['input_bytes'] += input_metrics.get('Bytes Read', 0)
        self.metrics['output_bytes'] += output_metrics.get('Bytes Written', 0)
        
        # Detect skewed tasks (tasks taking > 3x median)
        if len(self.metrics['task_durations']) > 10:
            median_duration = sorted(self.metrics['task_durations'])[len(self.metrics['task_durations']) // 2]
            if duration > median_duration * 3:
                self.metrics['skewed_tasks'].append({
                    'task_id': task_info.get('Task ID'),
                    'duration': duration,
                    'stage_id': task_info.get('Stage ID')
                })
    
    def generate_recommendations(self) -> Dict[str, Any]:
        """Generate configuration and code recommendations based on analysis"""
        recommendations = {
            'spark_configs': {},
            'code_suggestions': [],
            'observations': [],
            'severity': 'INFO'  # INFO, WARNING, CRITICAL
        }
        
        # Helper functions
        def bytes_to_mb(bytes_val):
            return bytes_val / (1024 * 1024)
        
        def bytes_to_gb(bytes_val):
            return bytes_val / (1024 * 1024 * 1024)
        
        num_executors = len(self.metrics['executors'])
        total_tasks = self.metrics['total_tasks']
        
        # 1. Memory Spill Analysis
        if self.metrics['memory_bytes_spilled'] > 0 or self.metrics['disk_bytes_spilled'] > 0:
            memory_spill_gb = bytes_to_gb(self.metrics['memory_bytes_spilled'])
            disk_spill_gb = bytes_to_gb(self.metrics['disk_bytes_spilled'])
            
            recommendations['observations'].append(
                f"Memory spilling detected: {memory_spill_gb:.2f} GB to memory, {disk_spill_gb:.2f} GB to disk"
            )
            recommendations['severity'] = 'WARNING'
            
            if disk_spill_gb > 1:
                recommendations['severity'] = 'CRITICAL'
                recommendations['spark_configs']['spark.executor.memory'] = '16g'  # Increase from default
                recommendations['spark_configs']['spark.executor.memoryOverhead'] = '3g'
                recommendations['spark_configs']['spark.memory.fraction'] = '0.8'
                recommendations['code_suggestions'].append(
                    "Consider repartitioning data to reduce memory pressure: df.repartition(200)"
                )
                recommendations['code_suggestions'].append(
                    "Use broadcast joins for small tables: spark.sql.autoBroadcastJoinThreshold=100MB"
                )
        
        # 2. Shuffle Analysis
        shuffle_read_gb = bytes_to_gb(self.metrics['shuffle_read_bytes'])
        shuffle_write_gb = bytes_to_gb(self.metrics['shuffle_write_bytes'])
        
        if shuffle_read_gb > 10 or shuffle_write_gb > 10:
            recommendations['observations'].append(
                f"High shuffle detected: {shuffle_read_gb:.2f} GB read, {shuffle_write_gb:.2f} GB written"
            )
            
            recommendations['spark_configs']['spark.sql.shuffle.partitions'] = '400'  # Increase from default 200
            recommendations['spark_configs']['spark.shuffle.compress'] = 'true'
            recommendations['spark_configs']['spark.shuffle.spill.compress'] = 'true'
            recommendations['spark_configs']['spark.sql.adaptive.enabled'] = 'true'
            recommendations['spark_configs']['spark.sql.adaptive.coalescePartitions.enabled'] = 'true'
            
            recommendations['code_suggestions'].append(
                "Optimize joins: Use broadcast joins where possible or consider bucketing for large tables"
            )
            recommendations['code_suggestions'].append(
                "Reduce shuffle: Use reduceByKey instead of groupByKey, filter data early"
            )
        
        # 3. GC Time Analysis
        if total_tasks > 0:
            gc_ratio = self.metrics['jvm_gc_time'] / self.metrics['executor_run_time']
            if gc_ratio > 0.1:  # More than 10% time in GC
                recommendations['observations'].append(
                    f"High GC time detected: {gc_ratio*100:.1f}% of executor time"
                )
                recommendations['severity'] = 'WARNING'
                
                recommendations['spark_configs']['spark.executor.memory'] = '20g'
                recommendations['spark_configs']['spark.executor.memoryOverhead'] = '4g'
                recommendations['spark_configs']['spark.executor.extraJavaOptions'] = (
                    '-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12'
                )
                
                recommendations['code_suggestions'].append(
                    "Reduce object creation: Use primitive types, reuse objects, avoid UDFs when possible"
                )
                recommendations['code_suggestions'].append(
                    "Cache/persist intermediate results if reused: df.cache() or df.persist(StorageLevel.MEMORY_AND_DISK)"
                )
        
        # 4. Task Skew Analysis
        if len(self.metrics['skewed_tasks']) > total_tasks * 0.05:  # More than 5% skewed
            recommendations['observations'].append(
                f"Data skew detected: {len(self.metrics['skewed_tasks'])} tasks are significantly slower"
            )
            recommendations['severity'] = 'WARNING'
            
            recommendations['spark_configs']['spark.sql.adaptive.skewJoin.enabled'] = 'true'
            recommendations['spark_configs']['spark.sql.adaptive.skewJoin.skewedPartitionFactor'] = '5'
            recommendations['spark_configs']['spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'] = '256MB'
            
            recommendations['code_suggestions'].append(
                "Handle data skew: Add salt keys for skewed joins or use salting technique"
            )
            recommendations['code_suggestions'].append(
                "Example: df.withColumn('salt', (rand() * 10).cast('int')).join(other_df, ['key', 'salt'])"
            )
        
        # 5. Parallelism Analysis
        if num_executors > 0 and total_tasks > 0:
            avg_tasks_per_executor = total_tasks / num_executors
            
            if avg_tasks_per_executor < 10:
                recommendations['observations'].append(
                    f"Low parallelism: Only {avg_tasks_per_executor:.1f} tasks per executor on average"
                )
                recommendations['code_suggestions'].append(
                    f"Increase parallelism: Use df.repartition({num_executors * 4}) to create more tasks"
                )
        
        # 6. Failed Tasks
        if self.metrics['failed_tasks'] > 0:
            failure_rate = self.metrics['failed_tasks'] / total_tasks
            recommendations['observations'].append(
                f"Task failures detected: {self.metrics['failed_tasks']} failed ({failure_rate*100:.1f}%)"
            )
            
            if failure_rate > 0.05:
                recommendations['severity'] = 'CRITICAL'
                recommendations['spark_configs']['spark.task.maxFailures'] = '8'
                recommendations['spark_configs']['spark.speculation'] = 'true'
                recommendations['code_suggestions'].append(
                    "Investigate task failures: Check executor logs for OOM errors or data issues"
                )
        
        # 7. EMR-specific recommendations
        recommendations['spark_configs']['spark.dynamicAllocation.enabled'] = 'true'
        recommendations['spark_configs']['spark.dynamicAllocation.minExecutors'] = '2'
        recommendations['spark_configs']['spark.dynamicAllocation.maxExecutors'] = str(num_executors * 2)
        recommendations['spark_configs']['spark.dynamicAllocation.executorIdleTimeout'] = '60s'
        
        # 8. S3 Optimizations for EMR
        if self.metrics['input_bytes'] > 0 or self.metrics['output_bytes'] > 0:
            recommendations['spark_configs']['spark.hadoop.fs.s3a.connection.maximum'] = '100'
            recommendations['spark_configs']['spark.hadoop.fs.s3a.threads.max'] = '20'
            recommendations['spark_configs']['spark.hadoop.fs.s3a.connection.timeout'] = '60000'
            recommendations['spark_configs']['spark.hadoop.fs.s3a.fast.upload'] = 'true'
        
        return recommendations
    
    def print_report(self):
        """Print analysis report"""
        print("\n" + "="*80)
        print("SPARK EVENT LOG ANALYSIS REPORT")
        print("="*80)
        
        print(f"\nApplication Metrics:")
        print(f"  Total Duration: {self.metrics['job_duration']:.2f} seconds")
        print(f"  Total Stages: {self.metrics['total_stages']}")
        print(f"  Total Tasks: {self.metrics['total_tasks']}")
        print(f"  Failed Tasks: {self.metrics['failed_tasks']}")
        print(f"  Number of Executors: {len(self.metrics['executors'])}")
        
        print(f"\nMemory Metrics:")
        print(f"  Memory Spilled: {self.metrics['memory_bytes_spilled'] / (1024**3):.2f} GB")
        print(f"  Disk Spilled: {self.metrics['disk_bytes_spilled'] / (1024**3):.2f} GB")
        print(f"  Peak Execution Memory: {self.metrics['peak_execution_memory'] / (1024**2):.2f} MB")
        
        print(f"\nShuffle Metrics:")
        print(f"  Shuffle Read: {self.metrics['shuffle_read_bytes'] / (1024**3):.2f} GB")
        print(f"  Shuffle Write: {self.metrics['shuffle_write_bytes'] / (1024**3):.2f} GB")
        
        print(f"\nI/O Metrics:")
        print(f"  Input Bytes: {self.metrics['input_bytes'] / (1024**3):.2f} GB")
        print(f"  Output Bytes: {self.metrics['output_bytes'] / (1024**3):.2f} GB")
        
        print(f"\nPerformance Metrics:")
        print(f"  Executor Run Time: {self.metrics['executor_run_time'] / 1000:.2f} seconds")
        print(f"  JVM GC Time: {self.metrics['jvm_gc_time'] / 1000:.2f} seconds")
        if self.metrics['executor_run_time'] > 0:
            gc_ratio = (self.metrics['jvm_gc_time'] / self.metrics['executor_run_time']) * 100
            print(f"  GC Time Ratio: {gc_ratio:.2f}%")
        
        print(f"\nTask Skew:")
        print(f"  Skewed Tasks: {len(self.metrics['skewed_tasks'])}")
        
        # Generate and print recommendations
        recommendations = self.generate_recommendations()
        
        print("\n" + "="*80)
        print(f"RECOMMENDATIONS (Severity: {recommendations['severity']})")
        print("="*80)
        
        if recommendations['observations']:
            print("\nObservations:")
            for obs in recommendations['observations']:
                print(f"  • {obs}")
        
        if recommendations['spark_configs']:
            print("\nRecommended Spark Configurations:")
            for config, value in recommendations['spark_configs'].items():
                print(f"  {config} = {value}")
        
        if recommendations['code_suggestions']:
            print("\nCode Optimization Suggestions:")
            for i, suggestion in enumerate(recommendations['code_suggestions'], 1):
                print(f"  {i}. {suggestion}")
        
        print("\n" + "="*80)
    
    def export_recommendations(self, output_file: str):
        """Export recommendations to JSON file"""
        recommendations = self.generate_recommendations()
        
        output_data = {
            'timestamp': datetime.now().isoformat(),
            'log_file': self.log_path,
            'metrics': {
                'job_duration': self.metrics['job_duration'],
                'total_stages': self.metrics['total_stages'],
                'total_tasks': self.metrics['total_tasks'],
                'failed_tasks': self.metrics['failed_tasks'],
                'num_executors': len(self.metrics['executors']),
                'memory_spilled_gb': self.metrics['memory_bytes_spilled'] / (1024**3),
                'disk_spilled_gb': self.metrics['disk_bytes_spilled'] / (1024**3),
                'shuffle_read_gb': self.metrics['shuffle_read_bytes'] / (1024**3),
                'shuffle_write_gb': self.metrics['shuffle_write_bytes'] / (1024**3),
            },
            'recommendations': recommendations
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nRecommendations exported to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Spark event logs and generate optimization recommendations for AWS EMR'
    )
    parser.add_argument(
        'log_path',
        help='Path to Spark event log file (can be .gz compressed)'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output file for recommendations (JSON format)',
        default='spark_recommendations.json'
    )
    parser.add_argument(
        '--app-id',
        help='Application ID (for reference only)',
        default=None
    )
    
    args = parser.parse_args()
    
    # Check if log file exists
    if not Path(args.log_path).exists():
        print(f"Error: Log file not found: {args.log_path}")
        sys.exit(1)
    
    print(f"Spark Event Log Analyzer for AWS EMR")
    if args.app_id:
        print(f"Application ID: {args.app_id}")
    
    # Create analyzer and run analysis
    analyzer = SparkEventLogAnalyzer(args.log_path)
    
    if not analyzer.parse_event_log():
        sys.exit(1)
    
    analyzer.analyze_events()
    analyzer.print_report()
    analyzer.export_recommendations(args.output)


if __name__ == '__main__':
    main()
