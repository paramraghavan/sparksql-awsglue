## Installation & Setup

```bash
# Install the only dependency
pip install requests

# Make it executable (optional)
chmod +x spark_analyzer.py
```

---

## Usage Examples

### 1. List All Applications

```bash
python spark_analyzer.py --url http://your-history-server:18080 --list
```

This shows all applications with their IDs, names, status, and duration so you can pick which one to analyze.

### 2. Analyze a Specific Application

```bash
python spark_analyzer.py --url http://your-history-server:18080 --app-id application_1234567890123_0001
```

### 3. Save JSON Report for Further Processing

```bash
python spark_analyzer.py --url http://your-history-server:18080 --app-id application_1234567890123_0001 --output report.json
```

### 4. Output JSON to Stdout (for piping)

```bash
python spark_analyzer.py --url http://your-history-server:18080 --app-id application_1234567890123_0001 --json
```

---

## What the Script Analyzes

| Category          | Metrics Collected                                                        |
|-------------------|--------------------------------------------------------------------------|
| **Executors**     | Count, cores, memory, GC time, task time, shuffle I/O, disk/memory spill |
| **Stages**        | Task count, duration, skew ratio (max/median), shuffle sizes, spill      |
| **Jobs**          | Duration, stage counts, failures                                         |
| **SQL Queries**   | Duration, success/failure counts                                         |
| **Configuration** | All spark.* properties used                                              |

---

## Issues Automatically Detected

The script identifies these problems and suggests fixes:

| Issue                | Detection Criteria              | Suggested Fix                         |
|----------------------|---------------------------------|---------------------------------------|
| High GC              | GC time > 10% of task time      | Increase `spark.executor.memory`      |
| Disk Spill           | Any bytes spilled to disk       | Increase memory                       |
| Task Skew            | Max task time > 3x median       | Enable AQE                            |
| Wrong Partition Size | Avg partition < 10MB or > 500MB | Adjust `spark.sql.shuffle.partitions` |
| Small Tasks          | Median task < 100ms             | Reduce parallelism                    |
| AQE Disabled         | Spark 3+ without AQE            | Enable `spark.sql.adaptive.enabled`   |
| Task Failures        | Any failed tasks                | Increase `memoryOverhead`             |

---

## Sample Output

```
================================================================================
SPARK APPLICATION ANALYSIS REPORT
================================================================================

APPLICATION INFORMATION
----------------------------------------
  Application ID:   application_1702000000000_0001
  Application Name: MySparkJob
  Spark Version:    3.4.1
  Duration:         45m 23s

CURRENT CONFIGURATION
----------------------------------------
  Executor Memory:      4g
  Executor Cores:       4
  Shuffle Partitions:   200
  AQE Enabled:          false

EXECUTOR METRICS
----------------------------------------
  Total Executors:      10
  Total Cores:          40
  GC Time Ratio:        15.32%
  Total Disk Spill:     25.50 GB

ISSUES IDENTIFIED
----------------------------------------
  1. [HIGH] GC
     High GC time: 15.3% of task time spent in garbage collection
     Current: 15.3%
     Action:  Increase executor memory
     Config:  spark.executor.memory = 6g

  2. [HIGH] MEMORY
     Disk spill detected: 25.50 GB spilled to disk
     Config:  spark.executor.memory = 6g

RECOMMENDED CONFIGURATION CHANGES
----------------------------------------
  spark.executor.memory
    Current:   4g
    Suggested: 6g

SPARK-SUBMIT SNIPPET
----------------------------------------
  Add these to your spark-submit command:

    --conf spark.executor.memory=6g \
    --conf spark.sql.adaptive.enabled=true \
```

---

## REST API Endpoints Used

The script uses these Spark History Server API endpoints:

| Endpoint                                          | Data Retrieved        |
|---------------------------------------------------|-----------------------|
| `/api/v1/applications`                            | List all apps         |
| `/api/v1/applications/{id}`                       | App details, duration |
| `/api/v1/applications/{id}/jobs`                  | Job metrics           |
| `/api/v1/applications/{id}/stages`                | Stage metrics         |
| `/api/v1/applications/{id}/stages/{id}/{attempt}` | Task-level metrics    |
| `/api/v1/applications/{id}/allexecutors`          | Executor metrics      |
| `/api/v1/applications/{id}/environment`           | Spark configuration   |
| `/api/v1/applications/{id}/sql`                   | SQL query metrics     |
