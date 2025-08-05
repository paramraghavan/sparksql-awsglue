# Troubleshoot II
>Telnet does not work on aws edge node
## Alternative Connectivity Tests

### 1. **Using netcat (nc) - Usually Available**

```bash
# Test YARN ResourceManager
nc -zv <emr-master-ip> 8032

# Test HDFS NameNode
nc -zv <emr-master-ip> 8020
nc -zv <emr-master-ip> 9000

# Test multiple ports at once
for port in 8020 8032 8088 19888; do
  echo "Testing port $port..."
  nc -zv <emr-master-ip> $port
done
```

### 2. **Using curl for HTTP endpoints**

```bash
# Test YARN ResourceManager Web UI
curl -I http://<emr-master-ip>:8088

# Test HDFS NameNode Web UI  
curl -I http://<emr-master-ip>:9870

# Test Spark History Server
curl -I http://<emr-master-ip>:18080
```

### 3. **Using timeout command**

```bash
timeout 5 bash -c "</dev/tcp/<emr-master-ip>/8032" && echo "Port 8032 open" || echo "Port 8032 closed"
timeout 5 bash -c "</dev/tcp/<emr-master-ip>/8020" && echo "Port 8020 open" || echo "Port 8020 closed"
```

## SSH Key Issues - Alternative Solutions

### 1. **Use Session Manager Instead of SSH**

If you have AWS Systems Manager access:

```bash
# Connect to EMR master via Session Manager (no SSH key needed)
aws ssm start-session --target <emr-master-instance-id>
```

### 2. **Copy Files via S3 (Recommended)**

Instead of SCP, use S3 as intermediate storage:

**On EMR Master Node:**

```bash
# Create config bundle and upload to S3
sudo tar -czf /tmp/hadoop-spark-configs.tar.gz \
  /etc/hadoop/conf/core-site.xml \
  /etc/hadoop/conf/yarn-site.xml \
  /etc/hadoop/conf/hdfs-site.xml \
  /usr/lib/spark/conf/spark-env.sh \
  /usr/lib/spark/conf/spark-defaults.conf

aws s3 cp /tmp/hadoop-spark-configs.tar.gz s3://your-bucket/configs/
```

**On Edge Node:**

```bash
# Download and extract configs
aws s3 cp s3://your-bucket/configs/hadoop-spark-configs.tar.gz /tmp/
cd /
sudo tar -xzf /tmp/hadoop-spark-configs.tar.gz
```

### 3. **Get Configs via EMR API/CLI**

```bash
# Get cluster details
CLUSTER_ID="j-xxxxxxxxxxxxx"
aws emr describe-cluster --cluster-id $CLUSTER_ID

# Get bootstrap actions and configuration
aws emr list-bootstrap-actions --cluster-id $CLUSTER_ID
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Configurations'
```

## Manual Configuration (When File Copy Fails)

### 1. **Create core-site.xml manually**

```bash
sudo mkdir -p /etc/hadoop/conf
cat > /tmp/core-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://EMR-MASTER-PRIVATE-IP:8020</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
</configuration>
EOF
sudo mv /tmp/core-site.xml /etc/hadoop/conf/
```

### 2. **Create yarn-site.xml manually**

```bash
cat > /tmp/yarn-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>EMR-MASTER-PRIVATE-IP</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>EMR-MASTER-PRIVATE-IP:8032</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>EMR-MASTER-PRIVATE-IP:8088</value>
  </property>
</configuration>
EOF
sudo mv /tmp/yarn-site.xml /etc/hadoop/conf/
```

## Troubleshooting Network Issues

### 1. **Check Security Groups**

```bash
# Get your edge node's security groups
aws ec2 describe-instances --instance-ids <your-edge-instance-id> \
  --query 'Reservations[0].Instances[0].SecurityGroups[*].GroupId'

# Check EMR cluster security groups
aws emr describe-cluster --cluster-id <cluster-id> \
  --query 'Cluster.Ec2InstanceAttributes'
```

### 2. **Test with Hadoop/Spark commands directly**

```bash
# Test HDFS connectivity
export HADOOP_CONF_DIR=/etc/hadoop/conf
hadoop fs -ls / 2>&1 | head -20

# Test YARN connectivity
yarn node -list 2>&1 | head -10

# Check what Spark sees
spark-submit --version
```

### 3. **Check if services are accessible via public endpoints**

Sometimes EMR clusters expose public endpoints:

```bash
# Try public DNS names instead of IP
curl -I http://<emr-master-public-dns>:8088
```

## Quick Diagnostic Script

Create this script to check your setup:

```bash
#!/bin/bash
echo "=== Connectivity Tests ==="
nc -zv <emr-master-ip> 8032 2>&1 | grep -E "(open|succeeded|Connected)"
nc -zv <emr-master-ip> 8020 2>&1 | grep -E "(open|succeeded|Connected)"

echo "=== Configuration Files ==="
ls -la /etc/hadoop/conf/
ls -la /usr/lib/spark/conf/

echo "=== Environment Variables ==="
echo "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
echo "YARN_CONF_DIR: $YARN_CONF_DIR"
echo "SPARK_HOME: $SPARK_HOME"

echo "=== Basic Tests ==="
hadoop version 2>/dev/null | head -1
spark-submit --version 2>&1 | head -1
```
