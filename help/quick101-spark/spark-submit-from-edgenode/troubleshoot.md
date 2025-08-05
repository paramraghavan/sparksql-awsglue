## Common Issues and Solutions

### 1. **Network Connectivity & Security Groups**

The copied keys might not work because your edge node may not have proper network access to the EMR cluster:

```bash
# Test connectivity to EMR master node
telnet <emr-master-ip> 8032  # YARN ResourceManager
telnet <emr-master-ip> 9000  # HDFS NameNode (if using port 9000)
telnet <emr-master-ip> 8020  # HDFS NameNode (default port)
```

**Fix**: Ensure your edge node's security group allows outbound traffic to the EMR cluster's security groups on the
required ports.

### 2. **Configuration File Issues**

Even though you copied the files, they might contain internal IP addresses that aren't accessible from your edge node.

**Check your configuration files**:

```bash
# Check what's in your core-site.xml
cat /etc/hadoop/conf/core-site.xml | grep -A 2 -B 2 "fs.defaultFS"

# Check yarn-site.xml
cat /etc/hadoop/conf/yarn-site.xml | grep -A 2 -B 2 "yarn.resourcemanager"
```

**Common fix needed** - Update the configuration files to use the EMR master's **public IP** or **public DNS** instead
of private IPs:

```xml
<!-- In core-site.xml -->
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://<EMR-MASTER-PUBLIC-IP>:8020
    </value>
</property>

        <!-- In yarn-site.xml -->
<property>
<name>yarn.resourcemanager.hostname</name>
<value>
    <EMR-MASTER-PUBLIC-IP>
</value>
</property>
```

### 3. **Missing Dependencies**

Your edge node might be missing required libraries:

```bash
# Install missing dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install openjdk-8-jdk hadoop-client

# Or on Amazon Linux/CentOS
sudo yum install java-1.8.0-openjdk hadoop-client
```

### 4. **Spark Configuration Issues**

Create a proper `spark-defaults.conf`:

```bash
# Create /usr/lib/spark/conf/spark-defaults.conf
cat > /usr/lib/spark/conf/spark-defaults.conf << EOF
spark.master                     yarn
spark.submit.deployMode          client
spark.yarn.am.memory             1g
spark.executor.memory            1g
spark.executor.cores             1
spark.sql.adaptive.enabled       true
spark.hadoop.fs.defaultFS        hdfs://<EMR-MASTER-PUBLIC-IP>:8020
EOF
```

### 5. **Test Your Setup**

Try these diagnostic commands:

```bash
# Test Hadoop connectivity
hadoop fs -ls /

# Test YARN connectivity  
yarn node -list

# Simple spark-submit test
spark-submit --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode client \
  $SPARK_HOME/examples/jars/spark-examples*.jar 10
```

### 6. **Authentication Issues**

If you're still having authentication problems:

```bash
# Make sure you're using the correct user
sudo -u hadoop spark-submit --version

# Or set up proper Kerberos if your cluster uses it
# Check if Kerberos is enabled in your configs
grep -r "kerberos\|krb5" /etc/hadoop/conf/
```

## Quick Debugging Steps

1. **Check what error you're getting**:

```bash
spark-submit --master yarn --deploy-mode client --class org.apache.spark.examples.SparkPi $SPARK_HOME/examples/jars/spark-examples*.jar 10 2>&1 | tee spark-debug.log
```

2. **Verify environment variables**:

```bash
echo $HADOOP_CONF_DIR
echo $YARN_CONF_DIR
echo $SPARK_HOME
```

3. **Check if Spark can find Hadoop**:

```bash
$SPARK_HOME/bin/spark-shell --master local
```

Can you share the specific error message you're getting when running spark-submit? That would help me provide more
targeted troubleshooting steps.