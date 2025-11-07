If your Spark jobs are running "forever" on EMR and show as running in the YARN history server—even though task
instances look available—here are the recommended steps to troubleshoot and resolve slow or stuck jobs:

***

### Steps to Diagnose and Resolve Slow/Stuck Spark Jobs on EMR

#### 1. Check Job Progress in Spark UI

- Open the Spark Web UI for your cluster/application.
- Look at the "Stages" and "Executors" tabs:
    - Are stages actually in progress, or stuck with no active tasks?
    - Are executors busy, idle, or lost?
    - If no stages are advancing, the job may be stuck on a resource, shuffle, or data skew issue.[1][2]

#### 2. Review the Yarn Resource Manager UI

- Ensure your job isn't stuck in ACCEPTED state (waiting for resources).[3][4]
- Double-check there are no missing containers, unhealthy nodes, or blocks on resource queue.[4]

#### 3. Dive Into Application Logs

- Look at the stdout/stderr logs, and Spark event logs for signs of exceptions:
    - Common problematic messages: `OutOfMemoryError`, lost task/slave, deadlocks, or serialization issues.[5][6]
- Check error logs from both driver and executor sides:
    - Out-of-memory (OOM) or orphaned stages often cause jobs to hang or crawl.
    - If you use UDFs, inspect their execution steps for slow or non-terminating cases.[7][8]

#### 4. Analyze for Data Skew, Large Shuffles, or Broadcasts

- Large or uneven data partitions can freeze shuffles and sort operations.[9]
- Excessive broadcasting or the use of `.collect()` or `.collectAsMap()` on large sets can lock memory and slow
  progress.[5]
- The Spark Web UI (Tasks and Stages tabs) can show tasks with excessive runtimes or sizes.
- Tune parameters like:
    - `spark.sql.autoBroadcastJoinThreshold`
    - `spark.executor.memory`, `spark.driver.memory`
    - `spark.dynamicAllocation.enabled`
    - Use repartitioning or optimize join keys to reduce skew.

#### 5. Hardware and Cluster Health Checks

- Look for any instance failures, lost nodes/slaves, or resource exhaustion in EMR console.[10][11]
- Are you using enough executors/cores? Sometimes EMR shows resources allocated but network or disk throttling causes
  jobs to freeze.
- Review metrics in CloudWatch and YARN for CPU, memory, disk, and network.[12]

#### 6. Configuration Tuning & Step Isolation

- Disable unnecessary caching/persisting if memory is low.[6]
- Consider running with fewer executor cores (sometimes 1 core per executor helps stability).
- Try isolating job steps and running them individually to identify bottlenecks.

#### 7. Miscellaneous and External Factors

- If you overwrite existing S3 files, ensure the paths are unique or cleaned up early.[6]
- Very slow or lost file reads from S3 can cause stuck tasks.
- If the job involves external API calls or custom Python/R code, log time spent per stage.

#### 8. Restart or Kill and Retry Job

- If jobs are truly stuck with no log updates or progress, kill the application (via YARN Resource Manager or EMR
  console) and retry with gradually increased logging.
- If all else fails, scale cluster resources up, break jobs down, or seek root cause in logs and Spark DAG visibility.

***

### Quick Checklist

| Step              | Description                                   |
|-------------------|-----------------------------------------------|
| Check Spark UI    | Stages & Executors status, stuck tasks        |
| Check Yarn UI     | Job state, missing containers/nodes           |
| Review Logs       | Errors (OOM, deadlock, lost slave, UDF hang)  |
| Data Skew/Shuffle | Partitioning, join types, broadcasting        |
| Cluster Health    | Lost instances, resource exhaustion           |
| Config Tuning     | Executor/memory/background settings           |
| Misc              | S3 I/O, overwrites, custom code bottlenecks   |
| Kill/Retry        | If all else fails, restart and optimize steps |

***

Following this systematic approach ensures the real bottleneck or failure is identified. Making incremental corrections
and improvements based on each finding improves job reliability and performance for future large Spark workloads on
EMR.

## spark.eventLog.dir

The default value for `spark.eventLog.dir` is:

```
file:/tmp/spark-events
```

This means if you do not explicitly set `spark.eventLog.dir`, Spark writes event logs to the local directory
`/tmp/spark-events` on the machine where the Spark driver runs.

### Additional details:

- The directory must exist before the SparkContext is initialized.
- On clusters like AWS EMR, it is common to override this to an HDFS or S3 path for durability across nodes, e.g.
  `s3://my-bucket/spark-event-logs/` or `hdfs://namenode:8021/spark-events`.
- Using the default local directory may cause issues with the Spark History Server or logs being lost after driver
  termination.

This default is documented in the official Apache Spark config and monitoring pages and common knowledge in Spark
deployments.[1][5][8][9]

***

If you want Spark UI history to be reliably accessible on EMR, you should explicitly set `spark.eventLog.dir` to a
shared accessible directory like an S3 bucket or HDFS path with correct permissions. Otherwise, by default, logs go to
local `/tmp/spark-events` on the driver node only.

[1](https://spark.apache.org/docs/latest/configuration.html)
[2](https://stackoverflow.com/questions/31233830/apache-spark-setting-spark-eventlog-enabled-and-spark-eventlog-dir-at-submit-or)
[3](https://www.ibm.com/docs/en/zpas/1.1.0?topic=spark-enabling-history-service)
[4](https://docs.oracle.com/cd/E76382_01/bigData.Doc/data_processing_onPrem/src/rdp_config_spark.html)
[5](https://books.japila.pl/apache-spark-internals/history-server/configuration-properties/)
[6](https://docs.datastax.com/en/dse/6.9/spark/configure-history-server.html)
[7](https://community.cloudera.com/t5/Community-Articles/Change-spark-history-event-log-location/ta-p/248654)
[8](https://spark.apache.org/docs/latest/monitoring.html)
[9](https://ftp.sjtu.edu.cn/sites/www.apache.org/spark/docs/2.2.2/configuration.html)
[10](https://docs.nvidia.com/spark-rapids/user-guide/23.12.1/spark-profiling-tool.html)

## Check disk space on clsuter - master, AM or task

Yes, insufficient disk space on the EMR master, application master (AM), or task nodes can cause Spark jobs to slow
down, hang, or even fail, especially since Spark, YARN, and EMR use local disk extensively for intermediate data, logs,
shuffle files, event logging, and more.

Here’s a detailed step-by-step checklist to check disk space issues on an AWS EMR cluster:

***

### Step 1: Connect to EMR Nodes

- SSH into the **EMR master node**:
  ```bash
  ssh -i <your-key.pem> hadoop@<master-node-public-dns>
  ```
- Optionally, SSH into **core/task nodes** (using private IP or bastion host):
  ```bash
  ssh -i <your-key.pem> hadoop@<core-node-private-ip>
  ```

***

### Step 2: Check Disk Space on Each Node

Run the following command on each node (master, core, task):

```bash
df -h
```

- This shows mounted volumes, usage, and available space in human-readable form.
- Focus on:
    - Root file system: usually `/` or `/mnt/`
    - Mounted storage for YARN local dirs (e.g., `/mnt/yarn`, `/mnt/hadoop/yarn`, `/var/log` sometimes)
    - Any attached EBS volumes or instance store disks
- Example output summary per disk:
  ```
  Filesystem      Size  Used Avail Use% Mounted on
  /dev/xvdb1      100G   80G   20G  80%  /
  /dev/nvme1n1    500G  475G   25G  95% /mnt/data
  ```

If any important volume is >90% full or near 0 disk space available, it is likely contributing to your job issues.

***

### Step 3: Check YARN and Spark Local Directory Usage

Check YARN local directories where intermediate shuffle data and logs reside:

```bash
yarn nodemanager -status
```

Or manually check YARN local dirs (example paths):

```bash
du -sh /mnt/yarn/nm-local-dir/usercache
du -sh /mnt/yarn/nm-local-dir/nmPrivateContainerFiles
```

Also check Spark’s local storage directories (like `/tmp` or configured `spark.local.dir`):

```bash
du -sh /tmp/*
```

Look for unexpectedly large directories consuming disk.

***

### Step 4: Check HDFS Usage (if applicable)

If your EMR cluster uses HDFS (not just S3), check HDFS disk usage:

```bash
hdfs dfsadmin -report
```

This shows used and free space in the HDFS cluster.

***

### Step 5: Monitor Logs for Disk Space Issues

On the master node, check system logs for disk-related warnings/errors:

```bash
sudo dmesg | grep -i 'no space\|disk full\|write error'
sudo tail -100 /var/log/messages
sudo tail -100 /var/log/syslog
```

Spark and YARN logs (under `/var/log/spark/` and `/var/log/hadoop-yarn/`) may also show errors related to disk or file
system issues.

***

### Step 6: Use AWS Console & CloudWatch

- In the AWS console under **EC2 > Instances**, check the volume sizes and attachment for each node.
- Use **CloudWatch Metrics**:
    - Monitor disk utilization and free space metrics if enabled.
    - Set alarms for low disk space.

***

### Step 7: Mitigations & Fixes

- Clean up disk space:
    - Delete old logs: `sudo rm -rf /var/log/spark/* /var/log/hadoop-yarn/*`
    - Clean temporary files `/tmp`, YARN local dirs.
- Increase disk size:
    - Add or resize EBS volumes attached to your instances.
    - Restart cluster with bigger instance types or more nodes.
- Configure Spark/YARN to use multiple disks or larger volumes (`spark.local.dir`, `yarn.nodemanager.local-dirs`).

***

### Summary Table

| Step                         | Command/Action                    | What to Look For                      |
|------------------------------|-----------------------------------|---------------------------------------|
| SSH into nodes               | `ssh -i key.pem hadoop@<node-ip>` | Access master, core, and task nodes   |
| Check disk space             | `df -h`                           | Disk usage %, free space on volumes   |
| Check YARN local dirs        | `du -sh /mnt/yarn/nm-local-dir/*` | Large shuffle or container file usage |
| Check Spark temp dirs        | `du -sh /tmp/*`                   | Large temporary Spark files           |
| Check HDFS usage (if used)   | `hdfs dfsadmin -report`           | HDFS free space                       |
| Check logs for errors        | `dmesg`, `tail /var/log/messages` | Disk space errors/warnings            |
| Use AWS Console & CloudWatch | Disk volume metrics and alarms    | Disk space and IO/slowness indicators |

***

Checking disk space and freeing or increasing it is often a first and crucial step when jobs seem stuck or very slow on
EMR, especially when shuffle, spill, or log writes are heavy.