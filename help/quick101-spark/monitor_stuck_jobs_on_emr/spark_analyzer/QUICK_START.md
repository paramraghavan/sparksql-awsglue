# Quick Start Guide - Spark Job Analyzer

## 🚀 Get Started in 3 Steps

### Step 1: Install Dependencies
```bash
pip install requests Flask
```

### Step 2: Find Your Information

**Spark History Server URL:**
- For EMR: `http://your-emr-master-dns:18080`
- SSH Tunnel: `ssh -i key.pem -L 18080:localhost:18080 hadoop@emr-master`

**Application ID:**
- Open Spark History Server UI: http://your-emr-master:18080
- Find your job in the list
- Copy the Application ID (format: `application_1234567890123_0001`)

### Step 3: Run the Analyzer

**Option A: Web Interface (Recommended for First-Time Use)**
```bash
python spark_analyzer_flask.py
```
Then open: http://localhost:5000

**Option B: Command Line**
```bash
python spark_job_analyzer.py \
  --history-server http://your-emr-master:18080 \
  --app-id application_1234567890123_0001
```

## 🔍 What You'll Get

The analyzer will tell you:

1. ✅ **Is your job stuck?** - Identifies stages that aren't progressing
2. ⚠️ **Data skew issues** - Finds tasks taking 3x+ longer than average
3. ❌ **Failed tasks** - Lists failures and problematic executors
4. 📊 **Performance bottlenecks** - Highlights slow stages
5. 💡 **Recommendations** - Suggests fixes for detected issues

## 🎯 Common Use Cases

### Use Case 1: "My job is stuck for 2 hours"
```bash
python spark_job_analyzer.py \
  --history-server http://emr-master:18080 \
  --app-id application_XXX
```
Look for:
- Active stages with < 10% task completion
- Data skew ratio > 5x
- Failed task counts

### Use Case 2: "Same job runs fast sometimes, slow other times"
```bash
# Analyze both runs and compare
python spark_job_analyzer.py --history-server URL --app-id FAST_RUN_ID --output fast.json
python spark_job_analyzer.py --history-server URL --app-id SLOW_RUN_ID --output slow.json

# Compare the JSONs to find differences in:
# - Data skew ratios
# - Task completion times
# - Number of executors
```

### Use Case 3: "Monitor a long-running job"
```bash
# Use the Flask web interface
python spark_analyzer_flask.py

# Refresh the page periodically to see progress
```

## 📋 Example Output

```
================================================================================
Analyzing Application: application_1699920000000_0042
================================================================================

Application Name: ETL Pipeline
Status: RUNNING

⚠️ Active Jobs: 1
Total Stages: 15

--- Stage 14 (Attempt 0) ---
Name: aggregate at Pipeline.scala:123
Tasks: 5 active, 195 completed, 0 failed

Data Skew Analysis:
  - ⚠️ Data skew detected! Max task is 8.5x slower than average
  - Skew Ratio: 8.5x
  - Max Task Duration: 320.45s
  - Avg Task Duration: 37.68s

================================================================================
RECOMMENDATIONS
================================================================================

✓ Enable adaptive query execution (AQE)
✓ Consider repartitioning with salting
✓ Increase spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes
```

## 🛠️ Quick Fixes

### If Data Skew Detected:
```python
# Enable AQE (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### If Tasks Failing:
```python
# Increase memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
```

### If Stage Slow:
```python
# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Use broadcast for small tables
df.join(broadcast(small_df), "key")
```

## 🔗 Files Included

- `spark_job_analyzer.py` - Command-line analyzer
- `spark_analyzer_flask.py` - Web interface
- `example_usage.py` - Code examples
- `requirements.txt` - Dependencies
- `README.md` - Full documentation

## 💡 Pro Tips

1. **Run analysis while job is still active** to catch issues in real-time
2. **Save results to JSON** for historical comparison
3. **Set up monitoring** for critical production jobs
4. **Check for skew ratio > 3x** - that's usually the culprit for slow jobs
5. **Look at executor failures** - they indicate resource issues

## 🆘 Troubleshooting

**Can't connect to History Server?**
```bash
# Test connection
curl http://your-emr-master:18080/api/v1/applications

# If fails, check:
# 1. History Server is running
# 2. Security group allows port 18080
# 3. SSH tunnel is active (if accessing remotely)
```

**Application not found?**
```bash
# List all applications
curl http://emr-master:18080/api/v1/applications | jq '.[].id'
```

## 📚 Next Steps

1. Read the full README.md for detailed documentation
2. Check example_usage.py for advanced patterns
3. Integrate with your alerting system (Slack, PagerDuty, etc.)
4. Set up automated monitoring for production jobs

## 🎓 Understanding the Metrics

**Skew Ratio**: Max task time ÷ Avg task time
- < 2: Good ✅
- 2-3: Minor skew ⚠️
- > 3: Significant skew ❌ (needs fixing)

**Coefficient of Variation**: Measures task time variability
- < 30%: Consistent ✅
- 30-50%: Some variation ⚠️
- > 50%: High variability ❌ (likely data skew)

---

**Ready to analyze?** Run the Flask app or command-line script now! 🚀
