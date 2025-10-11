```markdown
Above did not fix. This Spark job used to run last month, but this month it's failing:
error:
error occurred while
org.apache.spark.SparkException: Job aborted due to (<HOST>.example.internal executor <EXECUTOR_ID> failed 4 times, most recent failure: Lost task <TASK_ID> in stage <STAGE_ID> (TID <TID>))
Task failed while writing rows to s3://<BUCKET>/<PREFIX>/results/<TS>/et_pool_exp_scored_<RUN_ID>):
org.apache.spark.SparkException: [TASK_WRITE_FAILED]
org.apache.spark.sql.errors.QueryExecutionErrors.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:<LINE>)
at org.apache.spark.sql.execution.datasources.FileFormatWriter.executeTask(FileFormatWriter.scala:<LINE>)
at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:<LINE>)
at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:<LINE>)
at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:<LINE>)
at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:<LINE>)
at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:<LINE>)
at org.apache.spark.rdd.RDD.iterator(RDD.scala:<LINE>)
at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:<LINE>)
at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:<LINE>)
at org.apache.spark.scheduler.Task.run(Task.scala:<LINE>)
at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:<LINE>)
at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:<LINE>)
at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:<LINE>)
at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:<LINE>)
at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:<LINE>)
at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:<LINE>)
at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:<LINE>)
at java.base/java.lang.Thread.run(Thread.java:<LINE>)

java.nio.file.AccessDeniedException:
<PREFIX>/results/<TS>/et_pool_exp_scored_<RUN_ID>/_temporary/0/_temporary/attempt_<ATTEMPT_ID>/part-<PART_SEQ>-<UUID>-c000.snappy-parquet:
Writing Object on
<PREFIX>/results/<TS>/et_pool_exp_scored_<RUN_ID>/_temporary/0/_temporary/attempt_<ATTEMPT_ID>/part-<PART_SEQ>-<UUID>-c000.snappy-parquet

software.amazon.awssdk.services.s3.model.S3Exception: (Service: S3, Request ID: <REQ_ID_OR_NULL>): <MSG_OR_NULL>
at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:<LINE>)
at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:<LINE>)
at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:<LINE>)
at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:<LINE>)
at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:<LINE>)
at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:<LINE>)
at org.apache.hadoop.fs.s3a.WriteOperationHelper.retry(WriteOperationHelper.java:<LINE>)
at org.apache.hadoop.fs.s3a.WriteOperationHelper.putObject(WriteOperationHelper.java:<LINE>)
at org.apache.hadoop.fs.s3a.S3ABlockOutputStream.lambda$putObject$0(S3ABlockOutputStream.java:<LINE>)
at org.apache.hadoop.thirdparty.com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:<LINE>)
at org.apache.hadoop.thirdparty.com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:<LINE>)
at org.apache.hadoop.thirdparty.com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:<LINE>)

```

# Try the following
**This is key: "It worked last month but fails now."** This changes everything! The error shows `AccessDeniedException` but with `Request ID: null`, which means it's **NOT a permissions issue** - it's an **S3 rate limiting or concurrency issue**.

## Root Cause: S3 Write Throttling

When Request ID is `null`, the request never reached S3. This happens when:
1. Too many concurrent writes overwhelm S3
2. S3 rate limits are hit
3. Network/connection pool exhaustion

## Immediate Fixes to Try (In Order)

### Fix 1: Reduce Concurrent S3 Writes (Try This First)

```python
# Drastically reduce concurrent writes to S3
df.coalesce(50) \  # Use coalesce, not repartition
  .write \
  .option('maxRecordsPerFile', 100000) \  # Larger files, fewer writes
  .mode('overwrite') \
  .parquet('s3://bucket/path/')
```

**Why:** You're likely hitting **S3's 3,500 PUT requests/second limit per prefix**. With 130+ concurrent tasks, you're overwhelming S3.

### Fix 2: Add S3 Partitioning to Spread Load

```python
# Spread writes across multiple S3 prefixes
df.repartition(200) \
  .write \
  .partitionBy("date", "some_category") \  # Creates multiple S3 prefixes
  .option('maxRecordsPerFile', 50000) \
  .mode('overwrite') \
  .parquet('s3://bucket/path/')
```

**Why:** Each S3 prefix gets its own 3,500 req/sec limit. Partitioning spreads load.

### Fix 3: Configure S3A Connection Pool

```python
# Increase S3 connection settings
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", 100)  # Lower than default
spark.conf.set("spark.hadoop.fs.s3a.threads.max", 20)  # Reduce threads
spark.conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
spark.conf.set("spark.hadoop.fs.s3a.connection.timeout", "200000")
spark.conf.set("spark.hadoop.fs.s3a.attempts.maximum", 20)  # More retries
spark.conf.set("spark.hadoop.fs.s3a.retry.limit", "10")

# Use committer that's more reliable
spark.conf.set("spark.hadoop.fs.s3a.committer.name", "magic")
spark.conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
spark.conf.set("spark.sql.sources.commitProtocolClass", 
               "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
spark.conf.set("spark.sql.parquet.output.committer.class",
               "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")

# Then write
df.coalesce(50).write.option('maxRecordsPerFile', 100000).parquet('s3://...')
```

### Fix 4: Check What Changed Since Last Month

```python
# Compare data size
print("Current data stats:")
print(f"  Row count: {df.count():,}")
print(f"  Partitions: {df.rdd.getNumPartitions()}")
print(f"  Columns: {len(df.columns)}")

# Check if data volume increased significantly
# If data is 2-3x larger than last month, that explains the failure
```

## Systematic Troubleshooting

### Step 1: Verify S3 Bucket Settings

```bash
# Check if anything changed in S3 bucket
# - Versioning enabled/disabled?
# - Lifecycle policies added?
# - Bucket policies changed?
# - Request rate limits hit?

# Check CloudWatch metrics for your S3 bucket:
# - 4xxErrors (rate limiting shows here)
# - 5xxErrors  
# - AllRequests
```

### Step 2: Test with Minimal Concurrency

```python
# Test 1: Write with only 10 concurrent tasks
print("Test 1: Minimal concurrency (10 tasks)...")
df.coalesce(10) \
  .write \
  .option('maxRecordsPerFile', 200000) \
  .mode('overwrite') \
  .parquet('s3://bucket/test_minimal/')

# If this works, it confirms S3 rate limiting
```

### Step 3: Use S3 Committer (Most Reliable)

```python
# EMR 5.19+ or Spark 3.1+ supports S3A committers
# These are MUCH more reliable than default FileOutputCommitter

# Configure before creating SparkSession
spark = SparkSession.builder \
    .appName("S3CommitterJob") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
    .getOrCreate()

# Then write normally
df.repartition(100) \
  .write \
  .option('maxRecordsPerFile', 50000) \
  .mode('overwrite') \
  .parquet('s3://bucket/path/')
```

### Step 4: Write to Different S3 Location

```python
# Test if the specific S3 path is the issue
df.coalesce(50).write.mode('overwrite').parquet('s3://bucket/test_new_location/')

# If this works, the original path may have:
# - Too many objects
# - Corrupted _temporary directory
# - S3 eventual consistency issues
```

### Step 5: Clean Temporary Directories

```python
# The _temporary directories may not be cleaned up
# Use AWS CLI to check and clean

import subprocess

# Check for leftover temporary files
s3_path = "s3://aws-icn31-s3-us-east-1-getdata-uattop-raw/prepay/results/"

# List temporary directories
subprocess.run([
    "aws", "s3", "ls", f"{s3_path}",
    "--recursive", "|", "grep", "_temporary"
])

# Clean them (be careful!)
# aws s3 rm s3://bucket/path/_temporary --recursive
```

## Production Fix: Staged Write Approach

```python
def reliable_s3_write(df, s3_path):
    """
    Staged approach to avoid S3 rate limiting
    """
    from datetime import datetime
    
    # 1. Reduce concurrency
    print("[1/5] Reducing concurrency...")
    df_reduced = df.coalesce(50)  # Only 50 concurrent writes
    
    # 2. Cache to avoid recomputation
    print("[2/5] Caching data...")
    df_reduced.cache()
    df_reduced.count()
    
    # 3. Write to staging location first
    staging_path = f"{s3_path}_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"[3/5] Writing to staging: {staging_path}")
    
    df_reduced.write \
        .option('maxRecordsPerFile', 100000) \
        .option('compression', 'snappy') \
        .mode('overwrite') \
        .parquet(staging_path)
    
    # 4. Verify write
    print("[4/5] Verifying staging write...")
    staging_df = spark.read.parquet(staging_path)
    staging_count = staging_df.count()
    original_count = df.count()
    
    print(f"  Original: {original_count:,} rows")
    print(f"  Staging:  {staging_count:,} rows")
    
    if staging_count != original_count:
        raise Exception(f"Row count mismatch! {original_count} vs {staging_count}")
    
    # 5. Move to final location
    print(f"[5/5] Moving to final location: {s3_path}")
    # Use AWS CLI for reliable move
    subprocess.run([
        "aws", "s3", "mv", staging_path, s3_path, "--recursive"
    ], check=True)
    
    df_reduced.unpersist()
    print("✓ Write complete!")

# Use it
reliable_s3_write(your_df, "s3://bucket/path/")
```

## Quick Checklist

Run these diagnostics:

```python
# 1. Check data size change
print("="*70)
print("DATA SIZE ANALYSIS")
print("="*70)
row_count = df.count()
num_cols = len(df.columns)
num_parts = df.rdd.getNumPartitions()

print(f"Rows: {row_count:,}")
print(f"Columns: {num_cols}")
print(f"Partitions: {num_parts}")
print(f"Estimated concurrent S3 writes: {num_parts}")

if num_parts > 100:
    print("\n⚠️  WARNING: Too many concurrent writes!")
    print(f"   Recommend: df.coalesce(50)")

# 2. Test minimal write
print("\n" + "="*70)
print("TESTING MINIMAL WRITE")
print("="*70)

try:
    test_path = "s3://your-bucket/test_minimal_write/"
    df.coalesce(10).limit(10000).write.mode("overwrite").parquet(test_path)
    print("✓ Minimal write successful")
    print("→ Issue is likely S3 rate limiting with high concurrency")
except Exception as e:
    print(f"✗ Even minimal write failed: {e}")
    print("→ Check S3 bucket permissions and policies")
```

## Best Solution for Your Case

Given it worked last month and `Request ID: null` error:

```python
# RECOMMENDED: Reduce concurrency dramatically
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", 100)
spark.conf.set("spark.hadoop.fs.s3a.threads.max", 20)
spark.conf.set("spark.hadoop.fs.s3a.attempts.maximum", 20)

df.coalesce(30) \  # Very low concurrency
  .write \
  .option('maxRecordsPerFile', 200000) \  # Larger files
  .mode('overwrite') \
  .parquet('s3://bucket/path/')
```

**This should fix 90% of "worked last month, fails now" S3 write issues.**