'''python
#!/usr/bin/env python3
"""
Spark Job Analyzer - Query Spark History Server to detect stuck/slow jobs
Analyzes stages, tasks, and executors to identify bottlenecks
"""

import requests
import json
from typing import Dict, List, Optional
from datetime import datetime
import statistics


class SparkJobAnalyzer:
    def __init__(self, history_server_url: str):
        """
        Initialize the analyzer with Spark History Server URL
        
        Args:
            history_server_url: URL of Spark History Server (e.g., http://your-emr-master:18080)
        """
        self.base_url = history_server_url.rstrip('/')
        self.api_base = f"{self.base_url}/api/v1"
    
    def get_application_info(self, app_id: str) -> Dict:
        """Get basic application information"""
        url = f"{self.api_base}/applications/{app_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_jobs(self, app_id: str) -> List[Dict]:
        """Get all jobs for an application"""
        url = f"{self.api_base}/applications/{app_id}/jobs"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_stages(self, app_id: str) -> List[Dict]:
        """Get all stages for an application"""
        url = f"{self.api_base}/applications/{app_id}/stages"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_stage_details(self, app_id: str, stage_id: int, attempt_id: int = 0) -> Dict:
        """Get detailed information about a specific stage"""
        url = f"{self.api_base}/applications/{app_id}/stages/{stage_id}/{attempt_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_stage_tasks(self, app_id: str, stage_id: int, attempt_id: int = 0) -> List[Dict]:
        """Get all tasks for a specific stage"""
        url = f"{self.api_base}/applications/{app_id}/stages/{stage_id}/{attempt_id}/taskSummary"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_executors(self, app_id: str) -> List[Dict]:
        """Get executor information"""
        url = f"{self.api_base}/applications/{app_id}/executors"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def analyze_data_skew(self, stage_details: Dict) -> Dict:
        """Analyze task execution times to detect data skew"""
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
        
        # Data skew indicators
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
            "total_tasks": len(durations),
            "message": f"Data skew detected! Max task is {skew_ratio:.1f}x slower than average" if skew_detected else "No significant data skew"
        }
    
    def analyze_job(self, app_id: str) -> Dict:
        """
        Comprehensive analysis of a Spark job
        Returns detailed analysis including bottlenecks and recommendations
        """
        print(f"\n{'='*80}")
        print(f"Analyzing Application: {app_id}")
        print(f"{'='*80}\n")
        
        analysis = {
            "app_id": app_id,
            "timestamp": datetime.now().isoformat(),
            "issues": [],
            "recommendations": [],
            "summary": {}
        }
        
        try:
            # Get application info
            app_info = self.get_application_info(app_id)
            print(f"Application Name: {app_info['name']}")
            print(f"Status: {app_info.get('attempts', [{}])[0].get('completed', 'Unknown')}")
            
            # Get jobs
            jobs = self.get_jobs(app_id)
            print(f"\nTotal Jobs: {len(jobs)}")
            
            # Identify stuck/slow jobs
            active_jobs = [j for j in jobs if j['status'] == 'RUNNING']
            failed_jobs = [j for j in jobs if j['status'] == 'FAILED']
            
            if active_jobs:
                print(f"⚠️  Active/Running Jobs: {len(active_jobs)}")
                analysis['issues'].append(f"{len(active_jobs)} job(s) currently running")
            
            if failed_jobs:
                print(f"❌ Failed Jobs: {len(failed_jobs)}")
                analysis['issues'].append(f"{len(failed_jobs)} job(s) failed")
            
            # Get stages
            stages = self.get_stages(app_id)
            print(f"\nTotal Stages: {len(stages)}")
            
            # Analyze stages
            active_stages = [s for s in stages if s['status'] == 'ACTIVE']
            failed_stages = [s for s in stages if s['status'] == 'FAILED']
            completed_stages = [s for s in stages if s['status'] == 'COMPLETE']
            
            print(f"Active Stages: {len(active_stages)}")
            print(f"Completed Stages: {len(completed_stages)}")
            print(f"Failed Stages: {len(failed_stages)}")
            
            # Analyze each active stage in detail
            print(f"\n{'='*80}")
            print("DETAILED STAGE ANALYSIS")
            print(f"{'='*80}\n")
            
            bottleneck_stages = []
            
            for stage in active_stages:
                stage_id = stage['stageId']
                attempt_id = stage['attemptId']
                
                print(f"\n--- Stage {stage_id} (Attempt {attempt_id}) ---")
                print(f"Name: {stage['name']}")
                print(f"Status: {stage['status']}")
                print(f"Tasks: {stage.get('numActiveTasks', 0)} active, "
                      f"{stage.get('numCompleteTasks', 0)} completed, "
                      f"{stage.get('numFailedTasks', 0)} failed")
                
                # Get detailed stage info
                try:
                    stage_details = self.get_stage_details(app_id, stage_id, attempt_id)
                    
                    # Analyze data skew
                    skew_analysis = self.analyze_data_skew(stage_details)
                    print(f"\nData Skew Analysis:")
                    print(f"  - {skew_analysis['message']}")
                    if skew_analysis['skew_detected']:
                        print(f"  - Skew Ratio: {skew_analysis['skew_ratio']}x")
                        print(f"  - Max Task Duration: {skew_analysis['max_duration_ms']/1000:.2f}s")
                        print(f"  - Avg Task Duration: {skew_analysis['avg_duration_ms']/1000:.2f}s")
                        analysis['issues'].append(
                            f"Stage {stage_id}: Data skew detected (ratio: {skew_analysis['skew_ratio']}x)"
                        )
                        bottleneck_stages.append({
                            'stage_id': stage_id,
                            'stage_name': stage['name'],
                            'skew_analysis': skew_analysis
                        })
                    
                    # Check for task failures
                    if stage.get('numFailedTasks', 0) > 0:
                        print(f"  ⚠️  Failed Tasks: {stage['numFailedTasks']}")
                        analysis['issues'].append(
                            f"Stage {stage_id}: {stage['numFailedTasks']} tasks failed"
                        )
                    
                    # Check execution metrics
                    metrics = stage_details.get('executorRunTime', 0)
                    if metrics:
                        print(f"  - Total Executor Run Time: {metrics/1000:.2f}s")
                
                except Exception as e:
                    print(f"  ⚠️  Could not get detailed stage info: {str(e)}")
            
            # Analyze completed stages for historical issues
            print(f"\n{'='*80}")
            print("COMPLETED STAGES - PERFORMANCE REVIEW")
            print(f"{'='*80}\n")
            
            slow_stages = []
            for stage in sorted(completed_stages, key=lambda x: x.get('executorRunTime', 0), reverse=True)[:5]:
                stage_id = stage['stageId']
                duration_sec = stage.get('executorRunTime', 0) / 1000
                
                if duration_sec > 60:  # Stages longer than 60 seconds
                    print(f"\nStage {stage_id}: {stage['name']}")
                    print(f"  - Duration: {duration_sec:.2f}s")
                    print(f"  - Tasks: {stage.get('numTasks', 0)}")
                    
                    slow_stages.append({
                        'stage_id': stage_id,
                        'name': stage['name'],
                        'duration_sec': duration_sec
                    })
            
            # Get executor information
            print(f"\n{'='*80}")
            print("EXECUTOR ANALYSIS")
            print(f"{'='*80}\n")
            
            try:
                executors = self.get_executors(app_id)
                print(f"Total Executors: {len(executors)}")
                
                for executor in executors:
                    if executor['id'] == 'driver':
                        continue
                    
                    total_tasks = executor.get('totalTasks', 0)
                    failed_tasks = executor.get('failedTasks', 0)
                    
                    if failed_tasks > 0:
                        print(f"\n⚠️  Executor {executor['id']}:")
                        print(f"  - Failed Tasks: {failed_tasks}/{total_tasks}")
                        analysis['issues'].append(
                            f"Executor {executor['id']}: {failed_tasks} failed tasks"
                        )
            
            except Exception as e:
                print(f"Could not get executor info: {str(e)}")
            
            # Generate recommendations
            print(f"\n{'='*80}")
            print("RECOMMENDATIONS")
            print(f"{'='*80}\n")
            
            if bottleneck_stages:
                analysis['recommendations'].append(
                    "Data skew detected: Consider repartitioning with salting or using adaptive query execution"
                )
                print("✓ Enable adaptive query execution (AQE) to handle skew automatically")
                print("✓ Consider repartitioning data with salting for skewed keys")
                print("✓ Increase spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
            
            if failed_stages or failed_jobs:
                analysis['recommendations'].append(
                    "Failed stages/jobs detected: Check executor logs and increase executor memory"
                )
                print("✓ Check executor logs for OutOfMemory errors")
                print("✓ Increase spark.executor.memory and spark.executor.memoryOverhead")
                print("✓ Review task failure reasons in Spark UI")
            
            if slow_stages:
                analysis['recommendations'].append(
                    "Slow stages detected: Review SQL queries and optimize joins/aggregations"
                )
                print("✓ Review and optimize SQL queries in slow stages")
                print("✓ Check if broadcast joins can be used for small tables")
                print("✓ Consider increasing parallelism with spark.sql.shuffle.partitions")
            
            if not analysis['issues']:
                print("✓ No major issues detected. Job appears healthy.")
            
            analysis['summary'] = {
                'total_jobs': len(jobs),
                'active_jobs': len(active_jobs),
                'failed_jobs': len(failed_jobs),
                'total_stages': len(stages),
                'active_stages': len(active_stages),
                'bottleneck_stages': bottleneck_stages,
                'slow_stages': slow_stages
            }
            
            return analysis
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Error connecting to Spark History Server: {str(e)}")
            analysis['issues'].append(f"Connection error: {str(e)}")
            return analysis
        except Exception as e:
            print(f"❌ Error during analysis: {str(e)}")
            analysis['issues'].append(f"Analysis error: {str(e)}")
            return analysis


def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze Spark jobs via History Server')
    parser.add_argument('--history-server', required=True,
                       help='Spark History Server URL (e.g., http://your-emr-master:18080)')
    parser.add_argument('--app-id', required=True,
                       help='Spark Application ID (e.g., application_1234567890123_0001)')
    parser.add_argument('--output', help='Output JSON file path (optional)')
    
    args = parser.parse_args()
    
    analyzer = SparkJobAnalyzer(args.history_server)
    analysis = analyzer.analyze_job(args.app_id)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"\n✓ Analysis saved to {args.output}")


if __name__ == '__main__':
    main()
'''