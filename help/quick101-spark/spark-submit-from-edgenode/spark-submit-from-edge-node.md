When using `spark-submit` with `--deploy-mode cluster` from an AWS edge node without explicitly passing a cluster ID,
Spark identifies the target cluster through several configuration mechanisms:

## How Spark Identifies the Target Cluster

**1. Cluster Manager Configuration**
The cluster manager URL is typically specified via:

- `--master` parameter in spark-submit command
- `spark.master` property in spark-defaults.conf
- Environment variables

Common patterns:

```bash
# For EMR clusters
spark-submit --master yarn --deploy-mode cluster

# For EKS/Kubernetes
spark-submit --master k8s://https://kubernetes-api-server-url

# For Spark Standalone
spark-submit --master spark://master-host:7077
```

**2. YARN Configuration (Most Common for EMR)**
When using `--master yarn`, Spark reads Hadoop configuration files to locate the YARN ResourceManager:

- `yarn-site.xml` - Contains ResourceManager address
- `core-site.xml` - Contains filesystem configurations
- `hdfs-site.xml` - Contains HDFS configurations

## Required Setup on Edge Node

**For EMR Clusters:**

1. **Hadoop/YARN Configuration Files**
   ```
   /etc/hadoop/conf/
   ├── core-site.xml
   ├── yarn-site.xml
   ├── hdfs-site.xml
   └── mapred-site.xml
   ```

2. **Spark Installation**
    - Spark binaries (usually in `/usr/lib/spark/` on EMR edge nodes)
    - Spark configuration in `/etc/spark/conf/`

3. **Environment Variables**
   ```bash
   export SPARK_HOME=/usr/lib/spark
   export HADOOP_CONF_DIR=/etc/hadoop/conf
   export YARN_CONF_DIR=/etc/hadoop/conf
   ```

**For Kubernetes/EKS:**

1. **kubectl configuration**
   ```
   ~/.kube/config - Contains cluster connection details
   ```

2. **Spark Kubernetes dependencies**
    - Kubernetes service account configurations
    - RBAC permissions

**For Standalone Clusters:**

1. **Spark configuration**
   ```
   spark-defaults.conf with spark.master=spark://master:7077
   ```

## Configuration Files Location Priority

Spark searches for configuration in this order:

1. Command-line parameters (`--master`, `--conf`)
2. `spark-defaults.conf`
3. Environment variables
4. System properties

## Example Edge Node Setup Commands

```bash
# Set environment variables
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/usr/lib/spark

# Verify cluster connectivity
yarn cluster --list-node-managers

# Submit job
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class MyMainClass \
  my-application.jar
```

The key is that the edge node must have the appropriate client configurations that point to your specific cluster's
management services (YARN ResourceManager, Kubernetes API server, etc.).

## Configuration on  edge node to point to the rigth cluster

Here are the key files to check in `/etc/hadoop/conf/` and what cluster-related information to look for:

## 1. yarn-site.xml

**Most Critical - Contains YARN ResourceManager details**

```xml
<?xml version="1.0"?>
<configuration>
    <!-- ResourceManager address - THIS IS KEY -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>ip-10-0-1-100.ec2.internal</value>
    </property>

    <property>
        <name>yarn.resourcemanager.address</name>
        <value>ip-10-0-1-100.ec2.internal:8032</value>
    </property>

    <!-- ResourceManager webapp address -->
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>ip-10-0-1-100.ec2.internal:8088</value>
    </property>

    <!-- High Availability setup (if applicable) -->
    <property>
        <name>yarn.resourcemanager.ha.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>yarn.resourcemanager.ha.rm-ids</name>
        <value>rm1,rm2</value>
    </property>
</configuration>
```

## 2. core-site.xml

**Contains filesystem and cluster core settings**

```xml
<?xml version="1.0"?>
<configuration>
    <!-- Default filesystem - points to HDFS NameNode -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://ip-10-0-1-101.ec2.internal:8020</value>
    </property>

    <!-- NameNode address (alternative format) -->
    <property>
        <name>fs.default.name</name>
        <value>hdfs://namenode-cluster</value>
    </property>

    <!-- High Availability NameService ID -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://mycluster</value>
    </property>
</configuration>
```

## 3. hdfs-site.xml

**Contains HDFS-specific cluster information**

```xml
<?xml version="1.0"?>
<configuration>
    <!-- NameNode addresses for HA setup -->
    <property>
        <name>dfs.nameservices</name>
        <value>mycluster</value>
    </property>

    <property>
        <name>dfs.ha.namenodes.mycluster</name>
        <value>nn1,nn2</value>
    </property>

    <property>
        <name>dfs.namenode.rpc-address.mycluster.nn1</name>
        <value>ip-10-0-1-101.ec2.internal:8020</value>
    </property>

    <property>
        <name>dfs.namenode.rpc-address.mycluster.nn2</name>
        <value>ip-10-0-1-102.ec2.internal:8020</value>
    </property>

    <!-- NameNode HTTP addresses -->
    <property>
        <name>dfs.namenode.http-address.mycluster.nn1</name>
        <value>ip-10-0-1-101.ec2.internal:50070</value>
    </property>
</configuration>
```

## Quick Commands to Verify Cluster Connectivity

```bash
# 1. Check if YARN cluster is reachable
yarn cluster --list-node-managers

# 2. Check HDFS connectivity
hdfs dfsadmin -report

# 3. List YARN applications
yarn application -list

# 4. Check ResourceManager web UI accessibility
curl -I http://$(grep -A1 "yarn.resourcemanager.webapp.address" /etc/hadoop/conf/yarn-site.xml | grep -oP '(?<=<value>)[^<]*')

# 5. Verify NameNode connectivity
hdfs fsck /
```

## Key Things to Look For:

**In yarn-site.xml:**

- `yarn.resourcemanager.address` - Must point to your cluster's ResourceManager
- `yarn.resourcemanager.hostname` - Cluster's master node hostname
- Check if HA is enabled and multiple ResourceManagers are configured

**In core-site.xml:**

- `fs.defaultFS` - Must point to your cluster's NameNode
- Should match the cluster you want to submit jobs to

**In hdfs-site.xml:**

- NameNode addresses should be reachable from your edge node
- If using NameNode HA, multiple namenodes should be configured

## Troubleshooting Commands:

```bash
# Test ResourceManager connectivity
telnet <resourcemanager-hostname> 8032

# Test NameNode connectivity  
telnet <namenode-hostname> 8020

# Check Hadoop configuration
hadoop classpath
echo $HADOOP_CONF_DIR
```

If any of these hostnames/IPs are unreachable from your edge node, that's likely why spark-submit can't identify or
connect to your cluster.