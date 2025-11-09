### 1. Clear Spark Event Logs

- Spark event logs are typically stored in the configured `spark.eventLog.dir`, often an S3 bucket or HDFS location.
- To clear:
    - If stored in **S3**, use AWS CLI or S3 management tools to delete logs older than a specific date, e.g.:

      ```
      aws s3 rm s3://your-spark-logs-path/ --recursive --exclude "*" --include "application_*" --profile your-profile
      ```

    - To selectively delete older logs, use `--query` or scripting to filter by last modified date.
    - If stored locally on HDFS or EMR nodes, remove files in the directory configured as `spark.eventLog.dir`.

### 2. Clear YARN Application Logs

- YARN logs are aggregated and stored based on YARN log aggregation configuration.
- For logs stored on **HDFS** or **S3**:
    - Remove logs by deleting files in the configured aggregation directory, e.g.,

      ```
      hdfs dfs -rm -r /path/to/yarn/logs/*
      ```
      or use AWS CLI for S3 location.
- For logs on local node disks, clean log directories like `/var/log/hadoop-yarn/container` or
  `/var/log/hadoop-yarn/nm`.

### 3. Use EMR Steps or Scripts for Cleanup

- You can automate log cleanup by running shell scripts on master or core nodes as EMR steps or cron jobs that:
    - Delete Spark event logs older than retention period.
    - Remove YARN aggregated logs older than retention threshold.
    - Use tools like `find` to delete old files by modification time:

      ```bash
      find /path/to/logs -type f -mtime +14 -delete
      ```

### 4. Configure Automatic Log Cleanup

- Enable and configure:
    - Spark History Server log cleaner by setting `spark.history.fs.cleaner.enabled=true` and related properties for
      retention.
    - YARN log aggregation retention with properties like `yarn.log-aggregation.retain-seconds` or cluster-level
      scripts.

***

### Important

- Always backup or archive logs to a place like S3 if you might need them for later analysis before deletion.
- Confirm log storage locations in your EMR cluster configuration before deleting.

## check spark.eventLog.dir location

The `spark.eventLog.dir` configuration specifies where Spark event logs are stored, and on an EMR cluster it is
typically set in the Spark configuration.

### How to check `spark.eventLog.dir` on EMR:

1. **Check Spark Configuration via AWS Console or CLI:**
    - Log into the AWS EMR Console.
    - Go to your cluster's **Configurations** tab.
    - Look for the **spark-defaults** configuration classification.
    - `spark.eventLog.dir` will be listed there if set explicitly.

2. **Check config on EMR master node via CLI:**
    - SSH into the EMR master node.
    - Run this command to see Spark defaults including event log dir:
      ```
      yarn classpath | tr ':' '\n' | grep spark-defaults.conf
      ```
    - Or check if config files are under `/etc/spark/conf/` and examine `spark-defaults.conf`.
    - You can also run:
      ```bash
      spark-submit --version
      ```
      Or start a spark shell and run:
      ```scala
      spark.conf.get("spark.eventLog.dir")
      ```

3. **Default location if not explicitly set:**
    - For EMR, default is often:
      ```
      hdfs:///var/log/spark/apps
      ```
    - Or an S3 bucket if event logs are configured to be stored on S3, e.g.,
      ```
      s3://your-bucket/spark-logs/
      ```

4. **Check cluster bootstrap or config JSON:**
    - If you launched via CLI or SDK, check your cluster configurations JSON file or CLI options for `spark-defaults`
      properties.

### Summary:

- `spark.eventLog.dir` location is viewable via cluster spark-defaults configs in the Console or from the EMR master
  node config files.
- Typical locations: HDFS `/var/log/spark/apps` or S3 bucket if configured.
- You can also query or print it from a spark shell or job runtime.

If you want, I can help you with commands to SSH and locate the configuration files or extract the value
programmatically.

[1](https://docs.aws.amazon.com/emr/latest/ManagementGuide/app-history-spark-UI.html)
[2](https://aws.github.io/aws-emr-best-practices/docs/benchmarks/Analyzing/retrieve_event_logs/)
[3](https://spark.apache.org/docs/latest/monitoring.html)
[4](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks-log-rotation.html)
[5](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-debugging.html)
[6](https://stackoverflow.com/questions/30494905/where-are-the-spark-logs-on-emr)
[7](https://www.youtube.com/watch?v=HlDcmMM8BNo)
[8](https://docs.nvidia.com/spark-rapids/user-guide/23.12.1/spark-profiling-tool.html)