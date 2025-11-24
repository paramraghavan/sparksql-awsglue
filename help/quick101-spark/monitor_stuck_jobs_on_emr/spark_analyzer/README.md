# Spark Job Analyzer for EMR

Comprehensive tool to analyze stuck/slow Spark jobs on EMR by querying the Spark History Server REST API.

## Features

✅ **Detect Stuck Jobs**: Identify jobs and stages that are running longer than expected  
✅ **Data Skew Analysis**: Detect uneven task distribution causing bottlenecks  
✅ **Failed Task Detection**: Find tasks and executors with failures  
✅ **Performance Insights**: Identify slow stages and recommend optimizations  
✅ **Two Interfaces**: Command-line script + Flask web UI  

## What It Detects

1. **Data Skew**: Tasks with significantly different execution times (indicates partition skew)
2. **Stuck Stages**: Active stages that may be blocked or slow
3. **Failed Tasks/Jobs**: Tasks or jobs that have failed
4. **Executor Issues**: Executors with high failure rates
5. **Slow Stages**: Completed stages that took longer than expected
6. **Task Imbalances**: Uneven task distribution across executors

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install manually
pip install requests Flask
```

## Usage

### Option 1: Command-Line Script

Best for automated monitoring and CI/CD pipelines.

```bash
python spark_job_analyzer.py \
  --history-server http://your-emr-master-dns:18080 \
  --app-id application_1234567890123_0001
```

Save results to JSON:
```bash
python spark_job_analyzer.py \
  --history-server http://your-emr-master-dns:18080 \
  --app-id application_1234567890123_0001 \
  --output analysis_results.json
```

### Option 2: Flask Web Application

Best for interactive analysis with a visual interface.

```bash
# Start the Flask server
python spark_analyzer_flask.py

# Access the web interface
# Open browser to: http://localhost:5000
```

Then:
1. Enter your Spark History Server URL (e.g., `http://your-emr-master:18080`)
2. Enter the Application ID (e.g., `application_1234567890123_0001`)
3. Click "Analyze Job"

## Finding Your Spark History Server URL

### For EMR Clusters:

1. **Using EMR Master Node**:
   ```bash
   # SSH to EMR master
   ssh -i your-key.pem hadoop@your-emr-master-dns
   
   # Spark History Server is usually at:
   http://localhost:18080
   ```

2. **From Outside EMR**:
   - SSH tunnel: `ssh -i key.pem -L 18080:localhost:18080 hadoop@emr-master-dns`
   - Then access: `http://localhost:18080`

3. **EMR Console**:
   - Go to EMR Console → Your Cluster → Application history
   - Find the "Spark History Server" link

### Finding Application ID:

1. **From Spark History Server UI**:
   - Go to http://your-emr-master:18080
   - Find your application in the list
   - Application ID format: `application_<timestamp>_<number>`

2. **From EMR Step**:
   - EMR Console → Steps → View logs → Find application ID in logs

3. **From YARN ResourceManager**:
   - Access YARN UI: http://emr-master:8088
   - Find application in the list

## Example Output

```
================================================================================
Analyzing Application: application_1699920000000_0042
================================================================================

Application Name: Data Processing Pipeline
Status: RUNNING

Total Jobs: 3
⚠️  Active/Running Jobs: 1

Total Stages: 15
Active Stages: 2
Completed Stages: 13
Failed Stages: 0

================================================================================
DETAILED STAGE ANALYSIS
================================================================================

--- Stage 14 (Attempt 0) ---
Name: aggregate at MyApp.scala:123
Status: ACTIVE
Tasks: 5 active, 195 completed, 0 failed

Data Skew Analysis:
  - Data skew detected! Max task is 8.5x slower than average
  - Skew Ratio: 8.5x
  - Max Task Duration: 320.45s
  - Avg Task Duration: 37.68s

================================================================================
RECOMMENDATIONS
================================================================================

✓ Enable adaptive query execution (AQE) to handle skew automatically
✓ Consider repartitioning data with salting for skewed keys
✓ Increase spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes
```

## Understanding the Analysis

### Data Skew Metrics

- **Skew Ratio**: Max task duration / Average task duration
  - < 2: Good balance
  - 2-3: Minor skew
  - > 3: **Significant skew** (action needed)

- **Coefficient of Variation**: (Standard Deviation / Mean) × 100
  - < 30%: Good
  - 30-50%: Moderate
  - > 50%: **High variability** (likely skew)

### Common Issues & Solutions

#### 1. Data Skew Detected
**Problem**: Some tasks take much longer than others  
**Solutions**:
```python
# Enable Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Manual repartitioning with salting
df.withColumn("salt", (rand() * 10).cast("int"))
  .repartition(col("skewed_key"), col("salt"))
```

#### 2. Slow Stages
**Problem**: Stages taking longer than expected  
**Solutions**:
```python
# Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Use broadcast joins for small tables
df1.join(broadcast(df2), "key")

# Cache intermediate results
df.cache()
```

#### 3. Failed Tasks
**Problem**: Tasks failing due to OOM or other errors  
**Solutions**:
```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")

# Increase task memory
spark.conf.set("spark.executor.memoryFraction", "0.8")
```

## API Reference

### SparkJobAnalyzer Class

```python
from spark_job_analyzer import SparkJobAnalyzer

# Initialize
analyzer = SparkJobAnalyzer("http://your-history-server:18080")

# Get application info
app_info = analyzer.get_application_info("application_123")

# Get all jobs
jobs = analyzer.get_jobs("application_123")

# Get all stages
stages = analyzer.get_stages("application_123")

# Get stage details with task info
stage_details = analyzer.get_stage_details("application_123", stage_id=5)

# Analyze data skew for a stage
skew_analysis = analyzer.analyze_data_skew(stage_details)

# Full analysis
analysis = analyzer.analyze_job("application_123")
```

## Spark History Server REST API Endpoints

The tool uses these REST API endpoints:

- `GET /api/v1/applications` - List all applications
- `GET /api/v1/applications/{appId}` - Get application info
- `GET /api/v1/applications/{appId}/jobs` - Get all jobs
- `GET /api/v1/applications/{appId}/stages` - Get all stages
- `GET /api/v1/applications/{appId}/stages/{stageId}/{attemptId}` - Get stage details
- `GET /api/v1/applications/{appId}/executors` - Get executor info

Full API documentation: https://spark.apache.org/docs/latest/monitoring.html#rest-api

## Troubleshooting

### Connection Issues

```bash
# Test if History Server is accessible
curl http://your-emr-master:18080/api/v1/applications

# If connection refused, check:
1. History Server is running: yarn application -list -appStates ALL
2. Port 18080 is open in security group
3. Using correct DNS/IP address
```

### Application Not Found

```bash
# List all applications
curl http://your-emr-master:18080/api/v1/applications | jq '.[].id'

# Check if application is still running
yarn application -list
```

### Performance Tips

- For long-running analysis, use the command-line script and save to JSON
- For real-time monitoring, use the Flask web UI
- Cache the Flask app results to avoid repeated API calls
- Set up scheduled analysis jobs for critical pipelines

## Advanced Usage

### Programmatic Integration

```python
from spark_job_analyzer import SparkJobAnalyzer
import json

# Analyze job
analyzer = SparkJobAnalyzer("http://history-server:18080")
result = analyzer.analyze_job("application_123")

# Check for critical issues
if result['summary']['bottleneck_stages_count'] > 0:
    print("WARNING: Data skew detected!")
    for stage in result['summary']['bottleneck_stages']:
        print(f"Stage {stage['stage_id']}: {stage['skew_analysis']['skew_ratio']}x skew")

# Send alerts
if result['summary']['failed_jobs'] > 0:
    send_slack_alert(f"Job failed: {result['app_id']}")
```

### Monitoring Multiple Jobs

```bash
# Create a monitoring script
for app_id in $(cat application_ids.txt); do
  python spark_job_analyzer.py \
    --history-server http://history-server:18080 \
    --app-id $app_id \
    --output "analysis_${app_id}.json"
done
```

## Contributing

Feel free to extend this tool with additional metrics:
- Memory usage analysis
- Network I/O metrics
- Shuffle read/write statistics
- Custom performance thresholds

## License

MIT License - Feel free to use and modify for your needs.
