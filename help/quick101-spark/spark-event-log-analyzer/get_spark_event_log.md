Here are the commands to **list and download the Spark event log file** named `application_<application_id>` from HDFS,
S3, or the local path:

***

### **1. From HDFS**

**List logs:**

```bash
hdfs dfs -ls /var/log/spark/apps/
```

**Download specific application log:**

```bash
hdfs dfs -copyToLocal /var/log/spark/apps/application_<application_id> ./application_<application_id>
```

This saves the file to your current directory.

* This file is formatted as a JSON event stream—each line is a JSON object describing a Spark event (such as job start,
  stage completion, task end, configuration info, etc.).
* The content is not a single valid JSON document, but a sequence of JSON objects, one per line.
* This file is directly consumable by the Spark History Server for reconstructing the Spark UI, DAG, and job metrics.
* You can parse it in Python by reading lines and loading each line as a JSON object.

***

### **2. From S3**

**List logs:**

```bash
aws s3 ls s3://your-bucket/spark-logs/
```

**Download specific application log:**

```bash
aws s3 cp s3://your-bucket/spark-logs/application_<application_id> ./application_<application_id>
```

Replace `your-bucket` with your S3 bucket name.

***

### **3. From Local Filesystem**

**List logs:**

```bash
ls /var/log/spark/apps/
```

**Copy application log:**

```bash
cp /var/log/spark/apps/application_<application_id> ./application_<application_id>
```

***

**Note:**

- Check your Spark configuration to confirm the event log directory (`spark.eventLog.dir`).
- On Amazon EMR, logs may be on HDFS or in S3, as set by your cluster config.

