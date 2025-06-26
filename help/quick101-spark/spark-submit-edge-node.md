# Spark submit from edge node

```text
Working with aws on us-east-1. I have EMR and  aws edge nodes both configured on us-east-1.
I am able perform spark-submit from EMR master node.
I want to perform spark-submit from edge node as well - spark-submit -master "yarn" \ -deploy-mode "client" ./my_job.py
What do i need to configure to be able to perfom spark-submit from aws edge node
```

## get the EMR_MASTER_PRIVATE_IP

There are several ways to get the EMR master node's private IP address. Here are the most common methods:

## 1. AWS CLI (Recommended)

**Get Cluster Details:**

```bash
# List all clusters to find your cluster ID
aws emr list-clusters --active

# Get master node private IP using cluster ID
aws emr describe-cluster --cluster-id j-XXXXXXXXXX --query 'Cluster.MasterPublicDnsName'

# For private IP specifically
aws emr list-instances --cluster-id j-XXXXXXXXXX --instance-group-types MASTER --query 'Instances[0].PrivateIpAddress'
```

**One-liner to get private IP:**

```bash
aws emr list-instances --cluster-id j-XXXXXXXXXX --instance-group-types MASTER --query 'Instances[0].PrivateIpAddress' --output text
```

## 2. AWS Console

1. Go to **EMR Console** → **Clusters**
2. Click on your cluster name
3. Go to **Instances** tab
4. Look for the **Master** instance
5. The private IP will be listed in the **Private IP address** column

## 3. From Within EMR Master Node

If you're already logged into the EMR master node:

```bash
# Get private IP
hostname -I | awk '{print $1}'

# Or
curl http://169.254.169.254/latest/meta-data/local-ipv4

# Or check network interface
ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d/ -f1
```

## 4. Using EC2 API

Since EMR master is an EC2 instance:

```bash
# First get the instance ID from EMR
INSTANCE_ID=$(aws emr list-instances --cluster-id j-XXXXXXXXXX --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)

# Then get private IP from EC2
aws ec2 describe-instances --instance-ids $INSTANCE_ID --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text
```

## 5. Using EMR Notebooks/Zeppelin

If you have EMR notebooks or Zeppelin running:

```python
# In a notebook cell
import socket

print(socket.gethostbyname(socket.gethostname()))
```

## 6. Script to Get All EMR Details

Here's a useful script to get all the information you need:

```bash
#!/bin/bash
CLUSTER_ID="j-XXXXXXXXXX"  # Replace with your cluster ID

echo "EMR Cluster Details:"
echo "==================="
echo "Cluster ID: $CLUSTER_ID"

MASTER_IP=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateIpAddress' --output text)
MASTER_DNS=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateDnsName' --output text)

echo "Master Private IP: $MASTER_IP"
echo "Master Private DNS: $MASTER_DNS"
```

## Quick Command for Your Use Case

For your specific configuration files, you can directly substitute:

```bash
# Get the IP
MASTER_IP=$(aws emr list-instances --cluster-id j-XXXXXXXXXX --instance-group-types MASTER --query 'Instances[0].PrivateIpAddress' --output text)

# Use it in your config
echo "Master IP is: $MASTER_IP"
```

Replace `j-XXXXXXXXXX` with your actual EMR cluster ID. If you don't know your cluster ID, run:

```bash
aws emr list-clusters --active
```

The private IP will typically be in the format `10.x.x.x` or `172.x.x.x` depending on your VPC CIDR block.

## spark-submit from edge node
To perform `spark-submit` from your AWS edge node to an EMR cluster, you need to configure several components to establish proper connectivity and authentication. Here's what you need to set up:

### 1. Network Connectivity

**Security Groups:**
- Ensure your edge node's security group allows outbound traffic to the EMR cluster
- EMR master node security group should allow inbound connections from your edge node on ports:
  - 8032 (ResourceManager)
  - 8030, 8031, 8033 (ResourceManager additional ports)
  - 10020 (JobHistory Server)
  - 8020 (HDFS NameNode)

**VPC Configuration:**
- Both EMR cluster and edge node should be in the same VPC or have proper VPC peering/transit gateway setup
- Route tables should allow communication between subnets

### 2. Spark and Hadoop Configuration

**Install Required Software on Edge Node:**
```bash
# Install Spark (same version as EMR cluster)
wget https://archive.apache.org/dist/spark/spark-3.x.x/spark-3.x.x-bin-hadoop3.2.tgz
tar -xzf spark-3.x.x-bin-hadoop3.2.tgz

# Install Hadoop client libraries
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.x.x/hadoop-3.x.x.tar.gz
```

**Configure Hadoop on Edge Node:**
Create `/etc/hadoop/conf/core-site.xml`:
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://EMR_MASTER_PRIVATE_IP:8020</value>
    </property>
</configuration>
```

Create `/etc/hadoop/conf/yarn-site.xml`:
```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>EMR_MASTER_PRIVATE_IP</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>EMR_MASTER_PRIVATE_IP:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>EMR_MASTER_PRIVATE_IP:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>EMR_MASTER_PRIVATE_IP:8031</value>
    </property>
</configuration>
```

### 3. Authentication Setup

**Copy EMR Configuration Files:**
From your EMR master node, copy these files to your edge node:
```bash
# On EMR master, copy configs
scp /etc/hadoop/conf/core-site.xml user@edge-node:/etc/hadoop/conf/
scp /etc/hadoop/conf/yarn-site.xml user@edge-node:/etc/hadoop/conf/
scp /etc/hadoop/conf/hdfs-site.xml user@edge-node:/etc/hadoop/conf/
```

**Set up Kerberos (if EMR has Kerberos enabled):**
- Install Kerberos client on edge node
- Copy krb5.conf from EMR master
- Set up keytab or kinit authentication

### 4. Environment Configuration

**Set Environment Variables on Edge Node:**
```bash
export HADOOP_HOME=/path/to/hadoop
export SPARK_HOME=/path/to/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin
```

### 5. Test Connectivity

**Verify YARN connectivity:**
```bash
yarn node -list
hadoop fs -ls /
```

**Test Spark submit:**
```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 2g \
  --num-executors 2 \
  ./my_job.py
```

### 6. Alternative Approach: EMR Steps API

If direct spark-submit proves complex, consider using EMR Steps API from your edge node:

```bash
aws emr add-steps \
  --cluster-id j-XXXXXXXXXX \
  --steps Type=Spark,Name="My Spark Job",ActionOnFailure=CONTINUE,Args=[--py-files,s3://my-bucket/my_job.py]
```

