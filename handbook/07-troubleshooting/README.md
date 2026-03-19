# Spark Troubleshooting Guide

## Overview

This guide helps you diagnose and fix production Spark issues systematically. It covers 28 real problems from the exercises/ directory, organized by **symptom** (not root cause).

## How to Use This Guide

### Step 1: Identify Your Symptom
- Go to the symptom list below
- Find the one matching your issue
- Follow the diagnosis steps

### Step 2: Diagnose the Root Cause
- Check logs in Spark UI (http://driver:4040)
- Run diagnostic commands
- Identify which of the common causes applies

### Step 3: Apply Solutions
- Solutions are ordered by likelihood
- Each includes code and configuration changes
- Real case studies show actual scenarios

### Step 4: Verify
- Monitor job execution
- Check task duration and resource utilization
- Measure improvement (job duration, memory usage, etc.)

---

## Symptom Quick Reference

| Symptom | Section | Likely Causes |
|---------|---------|-------------|
| Job stuck at 99% | [Job Stuck Issues](01-job-stuck-issues.md) | Data skew, GC, network |
| OutOfMemoryError | [Memory Errors](02-memory-errors.md) | Partitions too large, caching, UDFs |
| Some tasks much slower | [Skew Problems](03-skew-problems.md) | Uneven data distribution, hot keys |
| Shuffle fetch failed | [Shuffle Failures](04-shuffle-failures.md) | Disk full, network, wrong codec |
| Job slower than expected | [Performance Degradation](05-performance-degradation.md) | Config, partitions, joins |
| Job fails immediately | [Configuration Errors](06-configuration-errors.md) | Wrong config, classpath, version |
| Schema doesn't match | [Schema Issues](07-schema-issues.md) | Type mismatch, evolution, null |
| AWS permission error | [AWS-Specific Errors](08-aws-specific-errors.md) | IAM role, S3, Glue, Athena |

---

## Diagnostic Process

### Quick Checks (Do First)

```bash
# 1. Check job status
# Go to: http://driver-ip:4040/jobs/

# 2. Check driver logs
# EMR: tail -f /var/log/spark/driver.log

# 3. Check executor logs
# EMR: yarn logs -applicationId <app-id>

# 4. Check system resources
# free -h              # Memory
# df -h                # Disk space
# top -b -n 1          # CPU
```

### Spark UI Analysis

**Tabs to check**:
- **Jobs**: See active/failed jobs, duration, stages
- **Stages**: Task duration distribution, shuffle read/write, skew indicators
- **Storage**: Cached DataFrames, memory usage
- **Executors**: GC time, shuffle spill, memory used/free
- **SQL**: Query plans, join strategies, scan sizes

---

## Common Patterns

### Pattern 1: One Task Much Slower (Data Skew)
```
Stages tab shows:
- Task 1: 2 seconds
- Task 2: 2 seconds
- Task 3: 450 seconds  ← STRAGGLER

In SQL tab, see SortMergeJoin or BroadcastHashJoin on hot key
```

**Solution**: Use salting or repartition with salt

### Pattern 2: All Tasks Slow (Parallelism Issue)
```
Stages tab shows:
- Total tasks: 50 (should be 200+)
- Task duration: 30 seconds each
- Total runtime: 30 seconds (only 50 parallel tasks)

In Stages: "Default parallelism" showing low partition count
```

**Solution**: Increase shuffle partitions

### Pattern 3: Sporadic Failures (Memory Spill)
```
Executors tab shows:
- GC time: 40% (should be < 10%)
- Shuffle spill: 2GB (should be 0)
- Memory used: 8GB/8GB (max out)

Some tasks fail: "GC overhead limit exceeded"
```

**Solution**: Reduce executor cores or increase memory

### Pattern 4: Immediate Failure (Configuration)
```
Job fails in seconds with:
- "Cannot find main class"
- "ClassNotFoundException"
- "Cannot access s3://..."
```

**Solution**: Check classpath, JAR files, IAM role

---

## Troubleshooting Flowchart

```
Does job fail immediately?
├─ YES → Check Configuration Errors (06)
└─ NO

Is job very slow?
├─ YES → Check Performance Degradation (05)
└─ NO

Does job get OutOfMemoryError?
├─ YES → Check Memory Errors (02)
└─ NO

Are some tasks much slower than others?
├─ YES → Check Skew Problems (03)
└─ NO

Does job get stuck at 99%?
├─ YES → Check Job Stuck Issues (01)
└─ NO

Is there a shuffle error?
├─ YES → Check Shuffle Failures (04)
└─ NO

Is there a schema issue?
├─ YES → Check Schema Issues (07)
└─ NO

Is there an AWS error?
├─ YES → Check AWS-Specific Errors (08)
└─ NO

→ Check Spark logs for detailed error message
```

---

## Key Metrics to Monitor

### Execution Metrics
- **Task duration**: Should be similar across tasks (watch for skew)
- **GC time**: Should be < 5% (> 10% is problem)
- **Shuffle write/read**: Indicates data movement

### Resource Metrics
- **Memory used**: Should not consistently hit max (leave 20% headroom)
- **Disk usage**: Monitor `tmp/` directory during shuffle
- **Network bandwidth**: High during shuffles (normal)

### Job Metrics
- **Parallelism**: Number of partitions/tasks
- **Job duration**: Baseline for comparison
- **Number of stages**: Each wide transformation = new stage

---

## Tools & Commands

### Check Configuration
```python
spark.sparkContext.getConf().getAll()
spark.conf.get("spark.sql.shuffle.partitions")
```

### Check DataFrame Info
```python
df.rdd.getNumPartitions()  # Number of partitions
df.count()                  # Total rows (triggers computation)
df.explain()                # Logical plan
df.explain(True)            # Physical plan
```

### Examine Partitions
```python
from pyspark.sql.functions import spark_partition_id

df.groupBy(spark_partition_id()).count().show(50)  # Size of each partition
df.select(spark_partition_id()).distinct().count()  # Unique partitions
```

### Check Cache Status
```python
spark.sql("SHOW CACHES").show()
spark.sparkContext._jsc.sc().getPersistentRDDs()
```

### YARN Commands (EMR)
```bash
# List running applications
yarn application -list

# Kill application
yarn application -kill <application_id>

# Get application logs
yarn logs -applicationId <application_id>

# Check resource availability
yarn node -list -all
```

---

## Real Case Studies

Each section includes **case studies** from actual problems:
- Problem description
- Investigation steps
- Root cause found
- Solution applied
- Lesson learned

These real scenarios teach more than theory!

---

## Prevention Best Practices

1. **Monitor from the start**: Enable Spark UI and event logs
2. **Use Adaptive Query Execution**: `spark.sql.adaptive.enabled=true`
3. **Set proper shuffle partitions**: Aim for 128MB per partition
4. **Cache reused DataFrames**: Don't recompute from source
5. **Profile before tuning**: Find actual bottlenecks
6. **Test on sample data**: Catch issues before full run
7. **Version control configs**: Document what works

---

## When to Escalate

If you've tried solutions in this guide and still stuck:

1. **Collect information**:
   - Spark UI (all tabs)
   - Driver/executor logs
   - Configuration settings
   - Data sample (if shareable)
   - Recent changes

2. **Check Spark docs**: https://spark.apache.org/docs/latest/
3. **Search Stack Overflow**: Include error message + version
4. **EMR AWS support**: If using EMR specific features

---

## Sections in This Guide

1. [**Job Stuck Issues**](01-job-stuck-issues.md) - Jobs at 99%, hung tasks
2. [**Memory Errors**](02-memory-errors.md) - OOM, container killed
3. [**Skew Problems**](03-skew-problems.md) - Uneven partitions, stragglers
4. [**Shuffle Failures**](04-shuffle-failures.md) - Shuffle fetch failed, disk
5. [**Performance Degradation**](05-performance-degradation.md) - Slow jobs
6. [**Configuration Errors**](06-configuration-errors.md) - Common config mistakes
7. [**Schema Issues**](07-schema-issues.md) - Schema mismatches, type conflicts
8. [**AWS-Specific Errors**](08-aws-specific-errors.md) - EMR, Glue, S3, IAM

---

## Quick Reference: Commands

```python
# Check partitions
df.rdd.getNumPartitions()

# View execution plan
df.explain(True)

# Check partition sizes
df.groupBy(spark_partition_id()).count().show(100)

# Cache DataFrame
df.cache()
df.count()  # Trigger caching

# Clear cache
df.unpersist()

# Set config
spark.conf.set("spark.sql.shuffle.partitions", "500")

# View config
spark.sparkContext.getConf().get("spark.sql.shuffle.partitions")
```

---

## Last Resort: Debugging Code

If issue persists, add logging to find exact problem:

```python
# Log partition sizes
df.groupBy(spark_partition_id()).agg(count("*")).repartition(1).collect()

# Log processing times
import time
start = time.time()
result = df.groupBy("key").count().collect()
print(f"Took {time.time() - start} seconds")

# Log memory usage
import psutil
print(psutil.virtual_memory())

# Log configuration
spark.sparkContext.getConf().getAll()
```

---

**Remember**: Most Spark issues have been seen before. This guide covers the patterns. Start with symptom identification, then follow diagnostic steps.

**Happy debugging! 🔧**
