# Complete Guide: EMR Key Pair Setup and Spark Job Submission from Edge Node

## Overview

This guide walks you through creating EC2 key pairs, configuring EMR clusters for SSH access, and submitting Spark jobs
from an edge node to the EMR cluster.

---

## Part 1: Create EC2 Key Pair

### Method 1: Using AWS Console

#### Step 1: Navigate to EC2 Key Pairs

1. Log into **AWS Console**
2. Navigate to **EC2 Dashboard**
3. In the left sidebar, click **Key Pairs** (under Network & Security)
4. Click **Create key pair**

#### Step 2: Configure Key Pair

1. **Name**: Enter a descriptive name (e.g., `emr-master-keypair`)
2. **Key pair type**: Select **RSA** (recommended)
3. **Private key file format**:
    - Select **.pem** for Linux/Mac
    - Select **.ppk** for Windows (PuTTY)
4. **Tags** (optional): Add tags for organization
5. Click **Create key pair**

#### Step 3: Download and Secure Private Key

1. Browser will automatically download the private key file
2. **Important**: This is the only time you can download this file
3. Move the file to a secure location (e.g., `~/.ssh/`)
4. Set proper permissions:
   ```bash
   chmod 400 ~/.ssh/emr-master-keypair.pem
   ```

### Method 2: Using AWS CLI

```bash
# Create key pair and save private key
aws ec2 create-key-pair \
    --key-name emr-master-keypair \
    --key-type rsa \
    --key-format pem \
    --query 'KeyMaterial' \
    --output text > ~/.ssh/emr-master-keypair.pem

# Set proper permissions
chmod 400 ~/.ssh/emr-master-keypair.pem
```

### Method 3: Import Existing Public Key

If you already have an SSH key pair:

```bash
# Generate public key from existing private key (if needed)
ssh-keygen -y -f ~/.ssh/existing-key.pem > ~/.ssh/existing-key.pub

# Import to AWS
aws ec2 import-key-pair \
    --key-name emr-existing-keypair \
    --public-key-material fileb://~/.ssh/existing-key.pub
```

---

## Part 2: Create EMR Cluster with Key Pair

### Method 1: Using AWS Console

#### Step 1: Launch EMR Cluster

1. Navigate to **EMR Dashboard**
2. Click **Create cluster**
3. Choose **Go to advanced options**

#### Step 2: Configure Software and Steps

1. **Release**: Select EMR version (e.g., emr-6.15.0)
2. **Applications**: Select required applications:
    - ✅ Spark
    - ✅ Hadoop
    - ✅ Livy (for REST API access)
    - ✅ Zeppelin (optional, for notebooks)
3. Click **Next**

#### Step 3: Configure Hardware

1. **Network**: Select your VPC and subnet
2. **Master node**:
    - Instance type: m5.xlarge (or as needed)
    - Instance count: 1
3. **Core nodes**:
    - Instance type: m5.large (or as needed)
    - Instance count: 2+ (as needed)
4. **Task nodes**: Optional
5. Click **Next**

#### Step 4: Configure Security

1. **Cluster name**: Enter descriptive name
2. **Logging**: Enable and specify S3 bucket
3. **Debugging**: Enable if needed
4. **Termination protection**: Enable if desired
5. **Scale down behavior**: Configure as needed
6. Click **Next**

#### Step 5: Configure Security Settings

1. **EC2 key pair**: Select your created key pair (`emr-master-keypair`)
2. **Permissions**:
    - EMR role: `EMR_DefaultRole`
    - EC2 instance profile: `EMR_EC2_DefaultRole`
    - Auto Scaling role: `EMR_AutoScaling_DefaultRole`
3. **Security groups**: Configure or use defaults
4. Click **Create cluster**

### Method 2: Using AWS CLI

```bash
# Create EMR cluster with key pair
aws emr create-cluster \
    --name "MyEMRCluster" \
    --release-label emr-6.15.0 \
    --applications Name=Spark Name=Hadoop Name=Livy \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --ec2-attributes KeyName=emr-master-keypair,SubnetId=subnet-12345678 \
    --service-role EMR_DefaultRole \
    --log-uri s3://my-emr-logs/ \
    --enable-debugging
```

---

## Part 3: Configure Security Groups for SSH Access

### Step 1: Find EMR Security Groups

1. Go to **EMR Dashboard** → **Clusters**
2. Click on your cluster
3. Click **Security and access** tab
4. Note the **Security groups for Master** ID

### Step 2: Modify Master Security Group

1. Navigate to **EC2 Dashboard** → **Security Groups**
2. Find and select the EMR master security group
3. Click **Inbound rules** tab
4. Click **Edit inbound rules**

### Step 3: Add SSH Rule

1. Click **Add rule**
2. **Type**: SSH
3. **Protocol**: TCP
4. **Port range**: 22
5. **Source**:
    - For specific IP: Your edge node IP/32
    - For IP range: Your office network CIDR
    - **⚠️ Security Warning**: Avoid 0.0.0.0/0 (open to world)
6. **Description**: "SSH access from edge node"
7. Click **Save rules**

### Step 4: Verify Livy Access (Optional)

If using Livy for REST API access:

1. Add another rule:
    - **Type**: Custom TCP
    - **Port**: 8998
    - **Source**: Same as SSH rule
    - **Description**: "Livy REST API access"

---

## Part 4: Set Up Edge Node

### Step 1: Install Required Software on Edge Node

#### Install Java (if not present)

```bash
# Amazon Linux/CentOS/RHEL
sudo yum install java-1.8.0-openjdk java-1.8.0-openjdk-devel

# Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-8-jdk

# Verify installation
java -version
```

#### Install Spark Client

```bash
# Download Spark (match EMR version)
cd /opt
sudo wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
sudo tar -xzf spark-3.4.1-bin-hadoop3.tgz
sudo mv spark-3.4.1-bin-hadoop3 spark
sudo chown -R $USER:$USER /opt/spark

# Add to PATH
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc
```

#### Install AWS CLI (if not present)

```bash
# Using pip
pip3 install awscli

# Or using package manager
sudo yum install awscli  # Amazon Linux
sudo apt-get install awscli  # Ubuntu
```

### Step 2: Configure SSH for EMR Access

#### Copy Key Pair to Edge Node

```bash
# If copying from local machine to edge node
scp -i ~/.ssh/edge-node-key.pem ~/.ssh/emr-master-keypair.pem ec2-user@edge-node-ip:~/.ssh/

# Set permissions on edge node
chmod 400 ~/.ssh/emr-master-keypair.pem
```

#### Test SSH Connection

```bash
# Get EMR master public IP from AWS Console
ssh -i ~/.ssh/emr-master-keypair.pem hadoop@<EMR-MASTER-PUBLIC-IP>
```

---

## Part 5: Submit Spark Jobs from Edge Node

### Method 1: Using spark-submit with SSH Tunnel

#### Step 1: Create SSH Tunnel

```bash
# Create tunnel for Spark driver communication
ssh -i ~/.ssh/emr-master-keypair.pem -N -L 8080:localhost:8080 -L 4040:localhost:4040 hadoop@<EMR-MASTER-PUBLIC-IP> &
```

#### Step 2: Configure Spark Client

Create `spark-defaults.conf`:

```bash
# Create Spark config directory
mkdir -p ~/.spark/conf

# Create configuration file
cat > ~/.spark/conf/spark-defaults.conf << 'EOF'
spark.master                     yarn
spark.submit.deployMode          client
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://EMR-MASTER-PRIVATE-IP:8020/spark-logs
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.sql.hive.metastore.version 2.3.9
spark.sql.hive.metastore.jars    builtin
EOF
```

#### Step 3: Submit Spark Job

```bash
# Example: Submit Python Spark job
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    /path/to/your/spark-job.py

# Example: Submit Scala/Java Spark job
spark-submit \
    --master yarn \
    --deploy-mode client \
    --class com.example.SparkApp \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    /path/to/your/spark-app.jar
```

### Method 2: Using Livy REST API

#### Step 1: Install Python Requests (if using Python)

```bash
pip3 install requests
```

#### Step 2: Submit Job via Livy

```python
#!/usr/bin/env python3
import requests
import json
import time

# EMR Master public IP
EMR_MASTER_IP = "YOUR-EMR-MASTER-PUBLIC-IP"
LIVY_URL = f"http://{EMR_MASTER_IP}:8998"


# Create Spark session
def create_spark_session():
    session_data = {
        "kind": "pyspark",
        "driverMemory": "2g",
        "executorMemory": "2g",
        "executorCores": 2,
        "numExecutors": 2
    }

    response = requests.post(f"{LIVY_URL}/sessions",
                             data=json.dumps(session_data),
                             headers={'Content-Type': 'application/json'})
    return response.json()['id']


# Submit code to session
def submit_code(session_id, code):
    statement_data = {"code": code}
    response = requests.post(f"{LIVY_URL}/sessions/{session_id}/statements",
                             data=json.dumps(statement_data),
                             headers={'Content-Type': 'application/json'})
    return response.json()['id']


# Check statement status
def get_statement_status(session_id, statement_id):
    response = requests.get(f"{LIVY_URL}/sessions/{session_id}/statements/{statement_id}")
    return response.json()


# Example usage
if __name__ == "__main__":
    session_id = create_spark_session()
    print(f"Created session: {session_id}")

    # Wait for session to be ready
    time.sleep(30)

    # Submit Spark code
    spark_code = """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("EdgeNodeJob").getOrCreate()
    df = spark.range(1000).toDF("number")
    result = df.count()
    print(f"Count: {result}")
    """

    statement_id = submit_code(session_id, spark_code)
    print(f"Submitted statement: {statement_id}")

    # Check results
    while True:
        status = get_statement_status(session_id, statement_id)
        if status['state'] == 'available':
            print("Output:", status['output'])
            break
        time.sleep(5)
```

### Method 3: Direct SSH and Submit

```bash
#!/bin/bash
# Script to SSH to EMR and submit job

EMR_MASTER_IP="YOUR-EMR-MASTER-PUBLIC-IP"
KEY_PATH="~/.ssh/emr-master-keypair.pem"

# Copy job file to EMR
scp -i $KEY_PATH /local/path/to/spark-job.py hadoop@$EMR_MASTER_IP:/tmp/

# SSH and submit job
ssh -i $KEY_PATH hadoop@$EMR_MASTER_IP << 'EOF'
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    /tmp/spark-job.py
EOF
```

---

## Part 6: Troubleshooting Common Issues

### SSH Connection Issues

#### Problem: Permission denied (publickey)

```bash
# Check key permissions
ls -la ~/.ssh/emr-master-keypair.pem
# Should show: -r-------- (400 permissions)

# Fix permissions if wrong
chmod 400 ~/.ssh/emr-master-keypair.pem
```

#### Problem: Connection timeout

1. **Check security group**: Ensure SSH rule allows your IP
2. **Check subnet**: Ensure EMR is in public subnet with internet gateway
3. **Check NACL**: Verify Network ACL allows SSH traffic

#### Problem: Wrong username

- Use `hadoop` as username for EMR master node
- Not `ec2-user` or `root`

### Spark Submission Issues

#### Problem: Cannot connect to YARN

```bash
# Check if you can reach EMR master
telnet EMR-MASTER-PUBLIC-IP 8032

# Verify YARN is running on EMR
ssh -i ~/.ssh/emr-master-keypair.pem hadoop@EMR-MASTER-IP "yarn node -list"
```

#### Problem: ClassNotFoundException

```bash
# Ensure Spark version compatibility
spark-submit --version

# Check available Spark applications on EMR
ssh -i ~/.ssh/emr-master-keypair.pem hadoop@EMR-MASTER-IP "ls /usr/lib/spark/"
```

#### Problem: Livy not accessible

1. **Check Livy status** on EMR:
   ```bash
   ssh -i ~/.ssh/emr-master-keypair.pem hadoop@EMR-MASTER-IP "sudo status livy-server"
   ```
2. **Check security group**: Ensure port 8998 is open
3. **Check Livy logs**:
   ```bash
   ssh -i ~/.ssh/emr-master-keypair.pem hadoop@EMR-MASTER-IP "sudo tail -f /var/log/livy/livy-server.log"
   ```

---

## Part 7: Security Best Practices

### Key Management

1. **Rotate keys regularly**: Create new key pairs periodically
2. **Limit key distribution**: Only give access to authorized users
3. **Use different keys**: Separate keys for different environments (dev/staging/prod)

### Network Security

1. **Restrict SSH access**: Use specific IP ranges, not 0.0.0.0/0
2. **Use VPN**: Connect through corporate VPN when possible
3. **Enable VPC Flow Logs**: Monitor network traffic
4. **Use private subnets**: Place EMR in private subnets with NAT gateway

### Access Control

1. **Use IAM roles**: Avoid hardcoding AWS credentials
2. **Principle of least privilege**: Grant minimum required permissions
3. **Enable CloudTrail**: Monitor API calls and access patterns
4. **Use EMR managed security groups**: Let EMR manage security group rules

### Example IAM Policy for Edge Node

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "emr:DescribeCluster",
        "emr:ListSteps",
        "emr:AddJobFlowSteps",
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:emr:*:*:cluster/*",
        "arn:aws:s3:::your-data-bucket/*"
      ]
    }
  ]
}
```

---

## Summary

This guide covers the complete process from creating EC2 key pairs to submitting Spark jobs from an edge node to EMR
clusters. Key takeaways:

1. **Security**: Always use proper key permissions (400) and restrict network access
2. **Multiple methods**: Choose between direct spark-submit, Livy REST API, or SSH-based submission
3. **Version compatibility**: Ensure Spark client version matches EMR Spark version
4. **Monitoring**: Use EMR console and Spark UI to monitor job execution
5. **Troubleshooting**: Check connectivity, security groups, and application logs for issues
