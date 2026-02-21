# Spark Performance Troubleshooting Runbook

## End-to-End Manual Analysis Using Spark History Server and EMR Logs

------------------------------------------------------------------------

# 1. Objective

This guide provides a **single continuous workflow** for diagnosing
Spark job performance issues using:

-   Spark History Server UI
-   Executor and Stage metrics
-   YARN logs (EMR environments)

The process helps identify:

-   Slow jobs
-   Jobs stuck near completion
-   Memory pressure
-   Data skew
-   Inefficient partitioning
-   Cluster resource waste
-   S3 throttling
-   Executor failures

------------------------------------------------------------------------

# 2. Investigation Flow (Always Follow This Order)

1.  Establish job context (Application Summary)
2.  Validate cluster health (Executors Tab)
3.  Locate bottlenecks (Stages Tab)
4.  Diagnose stragglers or hangs
5.  Verify runtime configuration
6.  Confirm external causes using YARN logs
7.  Apply configuration corrections

------------------------------------------------------------------------

# 3. Step 1 --- Open the Spark Application

Navigate to the Spark History Server:

    http://<history-server>:18080

Select the application to analyze.

------------------------------------------------------------------------

# 4. Step 2 --- Application Summary (Establish Context)

  Metric             Why It Matters
  ------------------ ------------------------------
  Duration           Overall runtime baseline
  Completed Stages   Execution complexity
  Spark Properties   Actual runtime configuration

------------------------------------------------------------------------

# 5. Step 3 --- Executors Tab (Cluster Health Diagnosis)

## Memory and Garbage Collection

  Metric      Healthy Range    Warning
  ----------- ---------------- ---------------
  GC Time     \<5% task time   \>10%
  Disk Used   0 B              Any value \>0

**Interpretation** - Disk spill \> 0 → Executor memory insufficient. -
High GC time → Heap pressure or too many cores per executor.

------------------------------------------------------------------------

## Resource Balance Across Executors

Large variance in executor runtime indicates **data skew**.

------------------------------------------------------------------------

## Storage Memory Usage

Healthy usage: **40--60%**\
Above 90% indicates cache pressure and possible spills.

------------------------------------------------------------------------

## Shuffle/Input Distribution

Goal: **128MB--200MB processed per task**.

If larger: - Tasks become heavy - GC increases - Disk spill likely

------------------------------------------------------------------------

# 6. Step 4 --- Stages Tab (Locate the Bottleneck)

## Task Duration Analysis

Compare:

    Max Duration vs Median Duration

  Result             Meaning
  ------------------ -----------
  Max \< 2× Median   Healthy
  Max \> 5× Median   Data skew

------------------------------------------------------------------------

## Scheduler Delay

  Value      Meaning
  ---------- ------------------
  \<100 ms   Healthy
  \>1 sec    Cluster overload

------------------------------------------------------------------------

## Locality Level

Desired: `NODE_LOCAL`

If `ANY`: - Network transfer delays dominate runtime.

------------------------------------------------------------------------

## Shuffle Read Size

Healthy: **128MB--200MB per task**\
\>1GB indicates under-partitioning.

------------------------------------------------------------------------

# 7. Step 5 --- Diagnosing Jobs Stuck at 99%

## Identify Stragglers

Sort Tasks by Duration (Descending).\
One task much slower ⇒ skew.

## Executor Thread Dump

Search for:

    BLOCKED

Possible causes: - JDBC waits - External services - Deadlocks

------------------------------------------------------------------------

# 8. Step 6 --- Verify Runtime Configuration

Check Environment Tab:

-   spark.sql.shuffle.partitions
-   executor memory
-   executor cores
-   dynamic allocation

If partitions = 200 → default configuration.

------------------------------------------------------------------------

# 9. Step 7 --- Calculate Optimal Configuration

## Shuffle Partitions

    Total Shuffle Read / 128MB

## Executor Memory

Increase by **50%** if disk spill or high GC detected.

Memory overhead ≈ **10--15%**.

## Executor Cores

Recommended: **4--5 cores per executor**.

## Dynamic Allocation

Enable if idle executors exist:

    spark.dynamicAllocation.enabled=true

------------------------------------------------------------------------

# 10. Step 8 --- Validate External Causes Using YARN Logs (EMR)

## Detect S3 Throttling

    yarn logs -applicationId <app_id> | grep -i "SlowDown"

Indicators: - 503 Service Unavailable - SlowDown errors

## Detect Memory Kills

    yarn logs -applicationId <app_id> | grep -i "beyond physical memory limits"

Fix:

    spark.executor.memoryOverhead ≈ 15%

------------------------------------------------------------------------

# 11. Mapping Observations to Fixes

  Observation        Root Cause          Action
  ------------------ ------------------- --------------------------------
  Disk spill         Memory shortage     Increase executor memory
  GC \>10%           Heap pressure       Reduce cores / increase memory
  Max ≫ Median       Data skew           Enable adaptive skew join
  Tasks \>200MB      Under-partitioned   Increase shuffle partitions
  Idle executors     Resource waste      Enable dynamic allocation
  SlowDown errors    S3 throttling       Reduce request concurrency
  Job stuck at 99%   Straggler task      Skew mitigation

------------------------------------------------------------------------

# 12. Quick Operational Checklist

1.  Check Executors → memory & GC\
2.  Check Stages → skew & shuffle size\
3.  Inspect active stage if stuck\
4.  Verify configs in Environment tab\
5.  Calculate optimal partitions\
6.  Validate YARN logs\
7.  Apply fixes

------------------------------------------------------------------------

## End of Runbook
