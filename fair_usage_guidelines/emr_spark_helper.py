"""
EMR Spark Helper Module
=======================
Import this module to easily set up Spark sessions with proper configurations
and cluster health checks for shared EMR environments.

Usage:
    from emr_spark_helper import create_spark_session, check_and_alert
    
    # Check cluster health
    check_and_alert()
    
    # Create Spark session with recommended settings
    spark = create_spark_session(
        app_name="MyAnalysis",
        max_executors=20,
        job_type="medium"
    )
"""

import os
import sys
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from pyspark.sql import SparkSession


# =============================================================================
# Configuration Constants
# =============================================================================

CLUSTER_CONFIG = {
    "master_nodes": 1,
    "core_nodes": 2,
    "task_nodes": 100,
    "yarn_url": "http://localhost:8088/cluster/cluster",
    "spark_history_url": "http://localhost:18080",
    "alert_email": "data-team@company.com"
}

JOB_TEMPLATES = {
    "small": {
        "max_executors": 10,
        "executor_memory": "4g",
        "executor_cores": 2,
        "driver_memory": "2g"
    },
    "medium": {
        "max_executors": 20,
        "executor_memory": "8g",
        "executor_cores": 4,
        "driver_memory": "4g"
    },
    "large": {
        "max_executors": 40,
        "executor_memory": "12g",
        "executor_cores": 4,
        "driver_memory": "8g"
    }
}


# =============================================================================
# Health Check Functions
# =============================================================================

def check_cluster_health(yarn_url=None, timeout=10):
    """
    Check if EMR cluster is healthy before starting Spark jobs.
    
    Args:
        yarn_url (str): YARN ResourceManager URL
        timeout (int): Request timeout in seconds
        
    Returns:
        tuple: (is_healthy: bool, message: str, details: dict)
    """
    if yarn_url is None:
        yarn_url = CLUSTER_CONFIG["yarn_url"]
    
    details = {
        "timestamp": datetime.now().isoformat(),
        "yarn_url": yarn_url,
        "status_code": None,
        "error": None
    }
    
    try:
        print("🔍 Checking cluster health...")
        response = requests.get(yarn_url, timeout=timeout)
        details["status_code"] = response.status_code
        
        if response.status_code == 200:
            print("✅ Cluster is HEALTHY and accepting jobs")
            return True, "Cluster is operational", details
        else:
            msg = f"Cluster returned unexpected status: {response.status_code}"
            print(f"⚠️ {msg}")
            return False, msg, details
            
    except requests.exceptions.Timeout:
        msg = "Cluster health check timed out - cluster may be overloaded"
        details["error"] = "timeout"
        print(f"❌ {msg}")
        return False, msg, details
        
    except requests.exceptions.ConnectionError:
        msg = "Cannot connect to YARN ResourceManager - cluster may be down"
        details["error"] = "connection_error"
        print(f"❌ {msg}")
        return False, msg, details
        
    except Exception as e:
        msg = f"Unexpected error during health check: {str(e)}"
        details["error"] = str(e)
        print(f"❌ {msg}")
        return False, msg, details


def get_yarn_cluster_metrics():
    """
    Get detailed metrics from YARN cluster.
    
    Returns:
        dict: Cluster metrics or None if unavailable
    """
    try:
        metrics_url = "http://localhost:8088/ws/v1/cluster/metrics"
        response = requests.get(metrics_url, timeout=5)
        
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None


def send_alert_email(subject, message, recipient=None, smtp_config=None):
    """
    Send email alert for cluster issues.
    
    Args:
        subject (str): Email subject
        message (str): Email body
        recipient (str): Recipient email address
        smtp_config (dict): SMTP configuration
    """
    if recipient is None:
        recipient = CLUSTER_CONFIG["alert_email"]
    
    try:
        sender = smtp_config.get("sender", "emr-alerts@company.com") if smtp_config else "emr-alerts@company.com"
        
        msg = MIMEText(message)
        msg['Subject'] = f"[EMR Alert] {subject}"
        msg['From'] = sender
        msg['To'] = recipient
        
        print(f"📧 Alert notification prepared: {subject}")
        print(f"   Recipient: {recipient}")
        
        # Uncomment and configure for actual email sending
        # if smtp_config:
        #     smtp = smtplib.SMTP(smtp_config['server'], smtp_config['port'])
        #     smtp.starttls()
        #     smtp.login(smtp_config['username'], smtp_config['password'])
        #     smtp.sendmail(sender, [recipient], msg.as_string())
        #     smtp.quit()
        #     print("✅ Alert email sent successfully")
        
    except Exception as e:
        print(f"⚠️ Could not send email alert: {e}")


def check_and_alert(raise_on_failure=True):
    """
    Check cluster health and send alert if unhealthy.
    
    Args:
        raise_on_failure (bool): Whether to raise exception if cluster is unhealthy
        
    Returns:
        bool: True if healthy, False otherwise
    """
    is_healthy, message, details = check_cluster_health()
    
    if not is_healthy:
        alert_msg = f"""
EMR Cluster Health Alert
========================

Status: UNHEALTHY
Time: {details['timestamp']}
Message: {message}

Details:
- YARN URL: {details['yarn_url']}
- Status Code: {details.get('status_code', 'N/A')}
- Error: {details.get('error', 'N/A')}

Please check the cluster status before running jobs.
"""
        send_alert_email("Cluster Health Alert", alert_msg)
        
        if raise_on_failure:
            raise RuntimeError(f"Cluster health check failed: {message}")
    
    return is_healthy


# =============================================================================
# Spark Session Creation
# =============================================================================

def create_spark_session(
    app_name=None,
    max_executors=20,
    job_type="medium",
    custom_config=None,
    enable_adaptive_query=True,
    check_cluster=True
):
    """
    Create a Spark session with optimized configurations for shared EMR cluster.
    
    Args:
        app_name (str): Spark application name (will auto-generate if None)
        max_executors (int): Maximum number of executors (overrides job_type)
        job_type (str): Job size template: "small", "medium", or "large"
        custom_config (dict): Additional Spark configurations
        enable_adaptive_query (bool): Enable Spark Adaptive Query Execution
        check_cluster (bool): Check cluster health before creating session
        
    Returns:
        SparkSession: Configured Spark session
        
    Example:
        >>> spark = create_spark_session(
        ...     app_name="MyAnalysis",
        ...     job_type="medium",
        ...     max_executors=25
        ... )
    """
    
    # Check cluster health first
    if check_cluster:
        check_and_alert(raise_on_failure=True)
    
    # Generate app name if not provided
    if app_name is None:
        user = os.environ.get("USER", "unknown")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        app_name = f"{user}_notebook_{timestamp}"
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        app_name = f"{app_name}_{timestamp}"
    
    # Get job template
    if job_type not in JOB_TEMPLATES:
        print(f"⚠️ Unknown job_type '{job_type}', using 'medium'")
        job_type = "medium"
    
    template = JOB_TEMPLATES[job_type].copy()
    
    # Override max_executors if specified
    if max_executors != 20:  # 20 is the default
        template["max_executors"] = max_executors
    
    print(f"🚀 Creating Spark session: {app_name}")
    print(f"📋 Job Template: {job_type}")
    print(f"📊 Max Executors: {template['max_executors']}")
    print("=" * 70)
    
    # Build Spark configuration
    builder = SparkSession.builder.appName(app_name)
    
    # Core dynamic allocation configs
    configs = {
        # Dynamic Allocation (CRITICAL for shared cluster)
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": str(template["max_executors"]),
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        "spark.dynamicAllocation.cachedExecutorIdleTimeout": "300s",
        "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout": "1s",
        
        # Executor configuration
        "spark.executor.memory": template["executor_memory"],
        "spark.executor.cores": str(template["executor_cores"]),
        "spark.executor.memoryOverhead": "2g",
        
        # Driver configuration
        "spark.driver.memory": template["driver_memory"],
        "spark.driver.maxResultSize": "2g",
        "spark.driver.memoryOverhead": "1g",
        
        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",
        
        # Shuffle and parallelism
        "spark.sql.shuffle.partitions": "200",
        "spark.default.parallelism": "200",
        "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
        
        # Performance optimizations
        "spark.speculation": "true",
        "spark.speculation.multiplier": "2",
        "spark.speculation.quantile": "0.75",
    }
    
    # Adaptive Query Execution
    if enable_adaptive_query:
        configs.update({
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB
            "spark.sql.adaptive.skewJoin.enabled": "true",
        })
    
    # Add custom configurations
    if custom_config:
        configs.update(custom_config)
    
    # Apply all configurations
    for key, value in configs.items():
        builder = builder.config(key, value)
    
    # Create session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Print success message
    print("✅ Spark session created successfully!")
    print(f"📱 Application ID: {spark.sparkContext.applicationId}")
    print(f"🔗 Spark UI: http://localhost:4040")
    print(f"📊 Configuration Summary:")
    print(f"   - Executor Memory: {template['executor_memory']}")
    print(f"   - Executor Cores: {template['executor_cores']}")
    print(f"   - Max Executors: {template['max_executors']}")
    print(f"   - Dynamic Allocation: Enabled ✅")
    print("=" * 70)
    
    return spark


def stop_spark_session(spark):
    """
    Gracefully stop Spark session and release resources.
    
    Args:
        spark (SparkSession): Spark session to stop
    """
    try:
        print("🧹 Cleaning up...")
        
        # Clear cache
        spark.catalog.clearCache()
        print("✅ Cache cleared")
        
        # Stop session
        spark.stop()
        print("✅ Spark session stopped")
        print("♻️ Resources released back to cluster")
        
    except Exception as e:
        print(f"⚠️ Error during cleanup: {e}")


# =============================================================================
# Monitoring Functions
# =============================================================================

def get_application_stats(spark):
    """
    Display current Spark application statistics.
    
    Args:
        spark (SparkSession): Active Spark session
    """
    sc = spark.sparkContext
    
    print("📊 Spark Application Statistics")
    print("=" * 70)
    print(f"Application ID:   {sc.applicationId}")
    print(f"Application Name: {sc.appName}")
    print(f"Spark UI:         http://localhost:4040")
    print(f"Parallelism:      {sc.defaultParallelism}")
    
    # Try to get executor information
    try:
        status = sc.statusTracker()
        executor_info = sc._jsc.sc().getExecutorMemoryStatus()
        num_executors = len(executor_info) - 1  # Subtract driver
        print(f"Active Executors: {num_executors}")
        
        # Get job and stage info
        active_jobs = len(status.getActiveJobIds())
        active_stages = len(status.getActiveStageIds())
        print(f"Active Jobs:      {active_jobs}")
        print(f"Active Stages:    {active_stages}")
        
    except Exception as e:
        print(f"Executor info:    Not yet available")
    
    print("=" * 70)


def show_cluster_resources():
    """
    Show available cluster resources from YARN.
    """
    metrics = get_yarn_cluster_metrics()
    
    if metrics and "clusterMetrics" in metrics:
        cm = metrics["clusterMetrics"]
        
        print("🖥️  Cluster Resource Status")
        print("=" * 70)
        print(f"Available Memory:     {cm.get('availableMB', 0)} MB")
        print(f"Allocated Memory:     {cm.get('allocatedMB', 0)} MB")
        print(f"Total Memory:         {cm.get('totalMB', 0)} MB")
        print(f"Available VCores:     {cm.get('availableVirtualCores', 0)}")
        print(f"Allocated VCores:     {cm.get('allocatedVirtualCores', 0)}")
        print(f"Total VCores:         {cm.get('totalVirtualCores', 0)}")
        print(f"Active Nodes:         {cm.get('activeNodes', 0)}")
        print(f"Unhealthy Nodes:      {cm.get('unhealthyNodes', 0)}")
        print(f"Running Applications: {cm.get('appsRunning', 0)}")
        print("=" * 70)
    else:
        print("⚠️ Could not retrieve cluster metrics from YARN")


# =============================================================================
# Convenience Functions
# =============================================================================

def quick_start(app_name=None, job_type="medium", max_executors=20):
    """
    Quick start function - checks cluster and creates Spark session in one call.
    
    Args:
        app_name (str): Application name
        job_type (str): "small", "medium", or "large"
        max_executors (int): Maximum executors
        
    Returns:
        SparkSession: Configured Spark session
    """
    return create_spark_session(
        app_name=app_name,
        job_type=job_type,
        max_executors=max_executors,
        check_cluster=True
    )


if __name__ == "__main__":
    # Test the module
    print("EMR Spark Helper Module")
    print("=" * 70)
    
    # Test health check
    is_healthy = check_and_alert(raise_on_failure=False)
    
    if is_healthy:
        print("\nCluster is ready for use!")
        print("\nExample usage:")
        print("  from emr_spark_helper import quick_start")
        print("  spark = quick_start(app_name='MyJob', job_type='medium')")
