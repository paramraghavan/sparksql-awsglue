# Complete Guide: Submitting Spark Jobs from Edge Node to EMR Cluster

Here's the complete step-by-step process to set up and submit Spark jobs from your edge node to the EMR cluster:

## Step 1: Copy Hadoop Configuration Files

Copy the essential Hadoop configuration files from your EMR master node to the edge node:

```bash
# Copy Hadoop configuration files
scp hadoop@<emr-master-ip>:/etc/hadoop/conf/core-site.xml /etc/hadoop/conf/
scp hadoop@<emr-master-ip>:/etc/hadoop/conf/yarn-site.xml /etc/hadoop/conf/
scp hadoop@<emr-master-ip>:/etc/hadoop/conf/hdfs-site.xml /etc/hadoop/conf/
```

## Step 2: Set Up spark-env.sh

Copy and configure the Spark environment file:

```bash
# Copy spark-env.sh from EMR master node
scp hadoop@<emr-master-ip>:/usr/lib/spark/conf/spark-env.sh /usr/lib/spark/conf/
```

**Or create/modify it manually** at `/usr/lib/spark/conf/spark-env.sh`:

```bash
#!/usr/bin/env bash

# Hadoop Configuration Directory
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# Java Home (ensure this matches your Java installation)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # Adjust as needed

# Spark specific configurations
export SPARK_DIST_CLASSPATH=$(/usr/bin/hadoop classpath)

# Python path (if using PySpark)
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Additional JVM options
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djava.net.preferIPv4Stack=true"
```

Make it executable:

```bash
chmod +x /usr/lib/spark/conf/spark-env.sh
```

## Step 3: Set Environment Variables

Set these environment variables on your edge node (add to `~/.bashrc` for persistence):

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/usr/lib/spark  # Adjust path as needed
```

Source the environment:

```bash
source ~/.bashrc
source /usr/lib/spark/conf/spark-env.sh
```

## Step 4: Verify Configuration

Test connectivity to ensure everything is working:

```bash
# Test Hadoop connectivity
hadoop fs -ls /

# Test YARN connectivity
yarn application -list

# Verify Spark can find Hadoop classpath
echo $SPARK_DIST_CLASSPATH
```

## Step 5: Navigate to Your Code Directory

```bash
cd /data/uat/run_model
```

## Step 6: Submit Your Spark Job

### Basic Submission (your original command enhanced):

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  model_run.py
```

### Enhanced Submission with Resource Configuration:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --files /data/uat/run_model/config.conf \  # if you have config files
  model_run.py
```

## Step 7: Monitor Job Progress

```bash
# List running applications
yarn application -list -appStates RUNNING

# Get application logs (replace with actual application ID)
yarn logs -applicationId <application_id>

# Monitor job in real-time
yarn application -status <application_id>
```

## Complete Configuration Checklist

Ensure these files are properly configured on your edge node:

### Required Files:

- ✅ `/etc/hadoop/conf/core-site.xml`
- ✅ `/etc/hadoop/conf/yarn-site.xml`
- ✅ `/etc/hadoop/conf/hdfs-site.xml`
- ✅ `/usr/lib/spark/conf/spark-env.sh`

### Environment Variables:

- ✅ `HADOOP_CONF_DIR=/etc/hadoop/conf`
- ✅ `YARN_CONF_DIR=/etc/hadoop/conf`
- ✅ `SPARK_HOME=/usr/lib/spark`
- ✅ `SPARK_DIST_CLASSPATH` (set in spark-env.sh)

## Troubleshooting

If job submission fails:

1. **Check network connectivity**:
   ```bash
   telnet <emr-master-ip> 8032  # YARN ResourceManager port
   ```

2. **Verify Hadoop configuration**:
   ```bash
   hadoop fs -ls /
   yarn node -list
   ```

3. **Check Spark environment**:
   ```bash
   spark-submit --version
   echo $SPARK_DIST_CLASSPATH
   ```

4. **Review application logs**:
   ```bash
   yarn logs -applicationId <application_id>
   ```

Since your shared storage `/data/uat/run_model` is accessible from both the edge node and EMR cluster, the cluster will
be able to access your Python file and any dependencies directly without additional file transfers.

## How does spark-submit on the edge node figure out the cluster id to submit the job

Great question! Spark-submit on the edge node doesn't use a "cluster ID" but rather discovers and connects to the EMR
cluster through the Hadoop/YARN configuration files. Here's how it works:

## How Spark-Submit Discovers the EMR Cluster

### 1. YARN ResourceManager Discovery

When you specify `--master yarn`, Spark looks for the YARN ResourceManager address in these configuration files:

**In `yarn-site.xml`:**

```xml

<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>ip-10-0-1-100.ec2.internal</value>  <!-- EMR master node hostname -->
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>ip-10-0-1-100.ec2.internal:8032</value>  <!-- ResourceManager port -->
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>ip-10-0-1-100.ec2.internal:8088</value>  <!-- Web UI port -->
    </property>
</configuration>
```

### 2. HDFS NameNode Discovery

**In `core-site.xml`:**

```xml

<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://ip-10-0-1-100.ec2.internal:8020</value>  <!-- HDFS NameNode -->
    </property>
</configuration>
```

### 3. The Connection Flow
The edge node does NOT automatically SSH to the master node when you run spark-submit
Here's what happens when you run `spark-submit --master yarn`:

```
Edge Node → Reads yarn-site.xml → Finds ResourceManager at ip-10-0-1-100.ec2.internal:8032
         ↓
Edge Node → Contacts ResourceManager → "I want to submit a Spark application"
         ↓
ResourceManager → "OK, here are available NodeManagers" → Returns cluster topology
         ↓
Spark → Schedules containers on NodeManagers → Launches Application Master
         ↓
Application Master → Manages Spark executors across the cluster nodes
```

## Key Configuration Files and Their Role

### 1. **yarn-site.xml** - Tells Spark WHERE the cluster is:

```xml
<!-- This is how Spark finds your EMR cluster -->
<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>ip-10-0-1-100.ec2.internal</value>
</property>
```

### 2. **core-site.xml** - Tells Spark WHERE the shared storage is:

```xml
<!-- This is how Spark finds HDFS and your files -->
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://ip-10-0-1-100.ec2.internal:8020</value>
</property>
```

### 3. **spark-env.sh** - Tells Spark HOW to find these configs:

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf  # "Look here for cluster info"
export YARN_CONF_DIR=/etc/hadoop/conf
```

## What Happens Behind the Scenes

When you run your command:

```bash
spark-submit --master yarn --deploy-mode cluster model_run.py
```

1. **Configuration Loading**: Spark reads `$HADOOP_CONF_DIR/yarn-site.xml` and finds:
    - ResourceManager address: `ip-10-0-1-100.ec2.internal:8032`

2. **Cluster Connection**: Spark contacts the ResourceManager at that address

3. **Resource Negotiation**: ResourceManager provides available NodeManager endpoints

4. **Job Submission**: Spark submits the application to run across those nodes

## Verification Commands

You can verify the cluster discovery is working:

```bash
# Check what ResourceManager Spark will connect to
grep -A 1 "yarn.resourcemanager.address" /etc/hadoop/conf/yarn-site.xml

# Test direct connection to ResourceManager
yarn application -list

# See cluster nodes
yarn node -list

# Check HDFS connection
hadoop fs -ls /
```

## Alternative: Manual Cluster Specification

If needed, you can also specify the ResourceManager directly in spark-submit:

```bash
spark-submit \
  --master yarn \
  --conf spark.yarn.resourcemanager.address=ip-10-0-1-100.ec2.internal:8032 \
  --deploy-mode cluster \
  model_run.py
```

But this is usually unnecessary since the configuration files handle this automatically.

The beauty of this approach is that your edge node acts like just another client to the EMR cluster - it uses the same
discovery mechanism that any Hadoop/Spark client would use to find and connect to the cluster services.