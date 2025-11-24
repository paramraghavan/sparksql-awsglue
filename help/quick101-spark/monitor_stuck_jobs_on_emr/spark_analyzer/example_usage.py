#!/usr/bin/env python3
"""
Example usage patterns for Spark Job Analyzer
Demonstrates various ways to use the analyzer for different scenarios
"""

from spark_job_analyzer import SparkJobAnalyzer
import json
import time
from datetime import datetime


def example_1_basic_analysis():
    """Example 1: Basic job analysis"""
    print("=" * 80)
    print("EXAMPLE 1: Basic Job Analysis")
    print("=" * 80)
    
    # Initialize analyzer
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    
    # Analyze a specific application
    app_id = "application_1234567890123_0001"
    result = analyzer.analyze_job(app_id)
    
    # Print summary
    print(f"\nJob Status: {'RUNNING' if result['summary']['active_jobs'] > 0 else 'COMPLETED'}")
    print(f"Total Stages: {result['summary']['total_stages']}")
    print(f"Issues Found: {len(result['issues'])}")


def example_2_detect_stuck_jobs():
    """Example 2: Detect if job is stuck"""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Detect Stuck Jobs")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    app_id = "application_1234567890123_0001"
    
    result = analyzer.analyze_job(app_id)
    
    # Check if job is stuck
    is_stuck = False
    
    if result['summary']['active_stages'] > 0:
        for stage in result.get('stages', []):
            # If a stage has been running with very few completed tasks for a long time
            completion_rate = stage['num_complete_tasks'] / stage['num_tasks'] if stage['num_tasks'] > 0 else 0
            
            if stage['status'] == 'ACTIVE' and completion_rate < 0.1:
                print(f"\n⚠️  STUCK STAGE DETECTED:")
                print(f"Stage {stage['stage_id']}: {stage['name']}")
                print(f"Progress: {stage['num_complete_tasks']}/{stage['num_tasks']} tasks")
                print(f"Active tasks: {stage['num_active_tasks']}")
                is_stuck = True
    
    if not is_stuck:
        print("\n✓ No stuck stages detected. Job is progressing normally.")


def example_3_data_skew_detection():
    """Example 3: Detailed data skew analysis"""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Data Skew Detection")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    app_id = "application_1234567890123_0001"
    
    # Get stages
    stages = analyzer.get_stages(app_id)
    
    print("\nAnalyzing stages for data skew...\n")
    
    skewed_stages = []
    
    for stage in stages:
        if stage['status'] in ['ACTIVE', 'COMPLETE']:
            stage_id = stage['stageId']
            attempt_id = stage['attemptId']
            
            try:
                stage_details = analyzer.get_stage_details(app_id, stage_id, attempt_id)
                skew_analysis = analyzer.analyze_data_skew(stage_details)
                
                if skew_analysis['skew_detected']:
                    skewed_stages.append({
                        'stage_id': stage_id,
                        'name': stage['name'],
                        'skew_ratio': skew_analysis['skew_ratio'],
                        'max_duration_sec': skew_analysis['max_duration_ms'] / 1000,
                        'avg_duration_sec': skew_analysis['avg_duration_ms'] / 1000
                    })
            except:
                continue
    
    if skewed_stages:
        print("Data Skew Detected in the following stages:\n")
        for stage in sorted(skewed_stages, key=lambda x: x['skew_ratio'], reverse=True):
            print(f"Stage {stage['stage_id']}: {stage['name']}")
            print(f"  Skew Ratio: {stage['skew_ratio']}x")
            print(f"  Max Task: {stage['max_duration_sec']:.2f}s")
            print(f"  Avg Task: {stage['avg_duration_sec']:.2f}s")
            print()
    else:
        print("✓ No significant data skew detected across all stages.")


def example_4_monitor_running_job():
    """Example 4: Monitor a running job continuously"""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Continuous Job Monitoring")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    app_id = "application_1234567890123_0001"
    
    print(f"\nMonitoring application {app_id}...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            result = analyzer.analyze_job(app_id)
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Status Update:")
            print(f"Active Jobs: {result['summary']['active_jobs']}")
            print(f"Active Stages: {result['summary']['active_stages']}")
            
            if result['issues']:
                print(f"⚠️  Issues: {len(result['issues'])}")
                for issue in result['issues'][:3]:  # Show first 3 issues
                    print(f"  - {issue.get('message', 'Unknown issue')}")
            else:
                print("✓ No issues detected")
            
            # Check if job is complete
            if result['summary']['active_jobs'] == 0:
                print("\n✓ Job completed!")
                break
            
            time.sleep(30)  # Check every 30 seconds
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")


def example_5_compare_jobs():
    """Example 5: Compare multiple jobs"""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Compare Multiple Jobs")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    
    # List of application IDs to compare
    app_ids = [
        "application_1234567890123_0001",
        "application_1234567890123_0002",
        "application_1234567890123_0003"
    ]
    
    print("\nComparing job performance:\n")
    
    comparison = []
    
    for app_id in app_ids:
        try:
            result = analyzer.analyze_job(app_id)
            
            comparison.append({
                'app_id': app_id,
                'total_stages': result['summary']['total_stages'],
                'failed_stages': result['summary']['failed_stages'],
                'issues_count': len(result['issues']),
                'has_skew': result['summary']['bottleneck_stages_count'] > 0
            })
        except Exception as e:
            print(f"Could not analyze {app_id}: {str(e)}")
    
    # Print comparison table
    print(f"{'Application ID':<40} {'Stages':<10} {'Failed':<10} {'Issues':<10} {'Skew'}")
    print("-" * 90)
    
    for job in comparison:
        print(f"{job['app_id']:<40} {job['total_stages']:<10} "
              f"{job['failed_stages']:<10} {job['issues_count']:<10} "
              f"{'Yes' if job['has_skew'] else 'No'}")


def example_6_export_to_json():
    """Example 6: Export detailed analysis to JSON"""
    print("\n" + "=" * 80)
    print("EXAMPLE 6: Export Analysis to JSON")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    app_id = "application_1234567890123_0001"
    
    # Perform analysis
    result = analyzer.analyze_job(app_id)
    
    # Save to JSON file
    output_file = f"spark_analysis_{app_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2)
    
    print(f"\n✓ Analysis saved to: {output_file}")
    print(f"File size: {len(json.dumps(result))} bytes")


def example_7_custom_alerts():
    """Example 7: Custom alerting logic"""
    print("\n" + "=" * 80)
    print("EXAMPLE 7: Custom Alerting")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    app_id = "application_1234567890123_0001"
    
    result = analyzer.analyze_job(app_id)
    
    # Define alert thresholds
    SKEW_THRESHOLD = 5.0
    FAILED_TASKS_THRESHOLD = 10
    
    alerts = []
    
    # Check for severe data skew
    for stage in result.get('stages', []):
        if stage.get('skew_analysis', {}).get('skew_ratio', 0) > SKEW_THRESHOLD:
            alerts.append({
                'severity': 'HIGH',
                'type': 'DATA_SKEW',
                'message': f"Stage {stage['stage_id']}: Severe data skew detected "
                          f"(ratio: {stage['skew_analysis']['skew_ratio']}x)"
            })
    
    # Check for high task failure rate
    if result['summary'].get('failed_stages', 0) > 0:
        alerts.append({
            'severity': 'CRITICAL',
            'type': 'FAILED_STAGES',
            'message': f"{result['summary']['failed_stages']} stage(s) failed"
        })
    
    # Print alerts
    if alerts:
        print("\n🚨 ALERTS:\n")
        for alert in alerts:
            print(f"[{alert['severity']}] {alert['type']}: {alert['message']}")
        
        # Here you would integrate with your alerting system
        # send_slack_alert(alerts)
        # send_pagerduty_alert(alerts)
        # send_email_alert(alerts)
    else:
        print("\n✓ No alerts triggered. Job is healthy.")


def example_8_stage_timeline():
    """Example 8: Analyze stage execution timeline"""
    print("\n" + "=" * 80)
    print("EXAMPLE 8: Stage Execution Timeline")
    print("=" * 80)
    
    analyzer = SparkJobAnalyzer("http://your-emr-master:18080")
    app_id = "application_1234567890123_0001"
    
    stages = analyzer.get_stages(app_id)
    
    print("\nStage Execution Timeline:\n")
    
    # Sort stages by completion time
    completed_stages = [s for s in stages if s['status'] == 'COMPLETE']
    completed_stages.sort(key=lambda x: x.get('submissionTime', 0))
    
    for stage in completed_stages[:10]:  # Show first 10 stages
        duration_sec = stage.get('executorRunTime', 0) / 1000
        num_tasks = stage.get('numTasks', 0)
        
        # Create a simple text-based timeline visualization
        bar_length = int(duration_sec / 10)  # Scale to reasonable length
        bar = '█' * min(bar_length, 50)
        
        print(f"Stage {stage['stageId']:>3}: {bar} {duration_sec:>6.1f}s ({num_tasks} tasks)")


if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("SPARK JOB ANALYZER - USAGE EXAMPLES")
    print("=" * 80)
    print("\nNote: Update the history_server URL and app_id in each example")
    print("before running.")
    print("\nAvailable examples:")
    print("  1. Basic job analysis")
    print("  2. Detect stuck jobs")
    print("  3. Data skew detection")
    print("  4. Continuous monitoring")
    print("  5. Compare multiple jobs")
    print("  6. Export to JSON")
    print("  7. Custom alerting")
    print("  8. Stage timeline visualization")
    print("\nUncomment the examples you want to run below:\n")
    
    # Uncomment the examples you want to run:
    # example_1_basic_analysis()
    # example_2_detect_stuck_jobs()
    # example_3_data_skew_detection()
    # example_4_monitor_running_job()
    # example_5_compare_jobs()
    # example_6_export_to_json()
    # example_7_custom_alerts()
    # example_8_stage_timeline()
