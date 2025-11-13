# Detailed Steps for Option B: Terminate & Recreate EMR Cluster

## Prerequisites (From Your Edge Node)

```bash
# Ensure AWS CLI is installed and configured
aws --version
aws sts get-caller-identity  # Verify your credentials work

# Set your region
export AWS_REGION=us-east-1  # Change to your region
```

---

## Step 1: Document Current Cluster Configuration

**Get your cluster ID:**

```bash
# List running clusters
aws emr list-clusters --active --region $AWS_REGION

# Set your cluster ID
export CLUSTER_ID=j-XXXXXXXXXXXXX  # Replace with your cluster ID
```

**Capture complete cluster configuration:**

```bash
# Get full cluster details
aws emr describe-cluster --cluster-id $CLUSTER_ID > cluster_config_backup_$(date +%Y%m%d).json

# Get instance groups configuration
aws emr list-instance-groups --cluster-id $CLUSTER_ID > instance_groups_backup_$(date +%Y%m%d).json

# Get bootstrap actions
aws emr list-bootstrap-actions --cluster-id $CLUSTER_ID > bootstrap_actions_backup_$(date +%Y%m%d).json

# Get cluster configurations (Spark, YARN, etc.)
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Configurations' > configurations_backup_$(date +%Y%m%d).json

# Get security configuration if used
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.SecurityConfiguration' > security_config_backup_$(date +%Y%m%d).json

# Get tags
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Tags' > tags_backup_$(date +%Y%m%d).json
```

**Extract key information you'll need:**

```bash
# Get instance types and counts
aws emr list-instance-groups --cluster-id $CLUSTER_ID --query 'InstanceGroups[*].[InstanceGroupType,InstanceType,RequestedInstanceCount]' --output table

# Get EMR release version
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.ReleaseLabel' --output text

# Get subnet ID
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.Ec2SubnetId' --output text

# Get key pair name
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.Ec2KeyName' --output text

# Get service role and job flow role
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.ServiceRole' --output text
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.IamInstanceProfile' --output text
```

---

## Step 2: Stop/Pause Running Jobs (if applicable)

```bash
# Check for running applications
aws emr list-steps --cluster-id $CLUSTER_ID --step-states RUNNING

# If you have running jobs, either:
# 1. Wait for them to complete, or
# 2. Cancel them (they'll need to be rerun)
aws emr cancel-steps --cluster-id $CLUSTER_ID --step-ids s-XXXXXXXXXXXXX
```

---

## Step 3: Create Recreation Script

Create a file called `recreate_emr_cluster.sh`:

```bash
#!/bin/bash

# Variables - CUSTOMIZE THESE
CLUSTER_NAME="your-cluster-name"
RELEASE_LABEL="emr-6.15.0"  # Use your version
SUBNET_ID="subnet-xxxxx"
KEY_NAME="your-keypair"
SERVICE_ROLE="EMR_DefaultRole"
EC2_ROLE="EMR_EC2_DefaultRole"
LOG_URI="s3://your-log-bucket/emr-logs/"
REGION="us-east-1"

# Instance configuration
MASTER_INSTANCE_TYPE="m5.xlarge"
CORE_INSTANCE_TYPE="m5.2xlarge"
CORE_INSTANCE_COUNT=5
TASK_INSTANCE_TYPE="m5.2xlarge"  # If you use task nodes
TASK_INSTANCE_COUNT=0

# Security groups (if specific ones)
MASTER_SG="sg-xxxxx"  # Optional
SLAVE_SG="sg-xxxxx"   # Optional

# Applications
APPLICATIONS="Name=Hadoop Name=Spark Name=Hive Name=Livy"  # Adjust as needed

# Configuration file (if you have custom configs)
CONFIGURATIONS_FILE="configurations_backup_$(date +%Y%m%d).json"

echo "Creating new EMR cluster..."

# Basic create cluster command
aws emr create-cluster \
  --name "$CLUSTER_NAME" \
  --release-label $RELEASE_LABEL \
  --applications $APPLICATIONS \
  --ec2-attributes "KeyName=$KEY_NAME,SubnetId=$SUBNET_ID,InstanceProfile=$EC2_ROLE" \
  --instance-groups \
    "InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE_TYPE,InstanceCount=1" \
    "InstanceGroupType=CORE,InstanceType=$CORE_INSTANCE_TYPE,InstanceCount=$CORE_INSTANCE_COUNT" \
  --service-role $SERVICE_ROLE \
  --log-uri $LOG_URI \
  --region $REGION \
  --enable-debugging \
  --tags "Environment=Production" "ManagedBy=DataTeam" \
  --configurations file://$CONFIGURATIONS_FILE \
  --output text

# If using task nodes, add this to instance-groups:
#   "InstanceGroupType=TASK,InstanceType=$TASK_INSTANCE_TYPE,InstanceCount=$TASK_INSTANCE_COUNT" \

# If using bootstrap actions, add:
#   --bootstrap-actions Path=s3://your-bucket/bootstrap.sh,Name=CustomBootstrap \

# If using security configuration:
#   --security-configuration your-security-config-name \

# If using auto-scaling:
#   --auto-scaling-role EMR_AutoScaling_DefaultRole \
```

**Make it executable:**

```bash
chmod +x recreate_emr_cluster.sh
```

---

## Step 4: Terminate Old Cluster

**Check one more time that jobs are done:**

```bash
aws emr list-steps --cluster-id $CLUSTER_ID --step-states RUNNING PENDING
```

**Terminate the cluster:**

```bash
# Terminate with protection disabled
aws emr terminate-clusters --cluster-ids $CLUSTER_ID

# If you get an error about termination protection:
aws emr modify-cluster-attributes --cluster-id $CLUSTER_ID --no-termination-protected
aws emr terminate-clusters --cluster-ids $CLUSTER_ID
```

**Monitor termination:**

```bash
# Watch cluster status
watch -n 10 'aws emr describe-cluster --cluster-id $CLUSTER_ID --query "Cluster.Status.State" --output text'

# Or check periodically
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Status.State' --output text
```

Wait until status shows `TERMINATED` (usually 5-10 minutes)

---

## Step 5: Create New Cluster

```bash
# Run your recreation script
./recreate_emr_cluster.sh

# Save the new cluster ID
NEW_CLUSTER_ID=$(aws emr list-clusters --active --query 'Clusters[0].Id' --output text)
echo "New Cluster ID: $NEW_CLUSTER_ID"
export NEW_CLUSTER_ID
```

**Monitor cluster creation:**

```bash
# Watch cluster startup
watch -n 10 'aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query "Cluster.Status.State" --output text'

# Check bootstrap progress
aws emr list-bootstrap-actions --cluster-id $NEW_CLUSTER_ID

# Check for any errors
aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query 'Cluster.Status.StateChangeReason'
```

Wait for status to change from `STARTING` → `BOOTSTRAPPING` → `RUNNING` (typically 10-15 minutes)

---

## Step 6: Validation & Testing

**Get master node DNS:**

```bash
MASTER_DNS=$(aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query 'Cluster.MasterPublicDnsName' --output text)
echo "Master Node: $MASTER_DNS"
```

**Verify cluster health:**

```bash
# SSH to master node
ssh -i /path/to/your-key.pem hadoop@$MASTER_DNS

# Once on master, check services
sudo systemctl status hadoop-yarn-resourcemanager
sudo systemctl status spark-history-server

# Check YARN
yarn node -list

# Check HDFS
hdfs dfsadmin -report

# Exit master node
exit
```

**Check cluster metrics:**

```bash
# View resource manager UI
echo "YARN RM UI: http://$MASTER_DNS:8088"

# View Spark History Server
echo "Spark History: http://$MASTER_DNS:18080"

# Check CloudWatch metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/ElasticMapReduce \
  --metric-name IsIdle \
  --dimensions Name=JobFlowId,Value=$NEW_CLUSTER_ID \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average
```

**Run a test job:**

```bash
# Submit a simple test job
aws emr add-steps \
  --cluster-id $NEW_CLUSTER_ID \
  --steps Type=Spark,Name="Test-Job",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://your-bucket/test_job.py]

# Monitor the step
STEP_ID=$(aws emr list-steps --cluster-id $NEW_CLUSTER_ID --query 'Steps[0].Id' --output text)
aws emr describe-step --cluster-id $NEW_CLUSTER_ID --step-id $STEP_ID --query 'Step.Status'
```

---

## Step 7: Compare Performance

**Run your actual job and compare:**

```bash
# Submit your real workload
aws emr add-steps \
  --cluster-id $NEW_CLUSTER_ID \
  --steps Type=Spark,Name="Production-Job",ActionOnFailure=CONTINUE,Args=[your-spark-args]

# Monitor execution time
start_time=$(date +%s)
# ... wait for job to complete ...
end_time=$(date +%s)
duration=$((end_time - start_time))
echo "Job completed in $duration seconds"
```

**Check Spark History Server for metrics:**

- Navigate to `http://$MASTER_DNS:18080`
- Compare task times, shuffle metrics, GC time with historical data

---

## Step 8: Update DNS/Connection Strings (if applicable)

If you have applications pointing to the cluster:

```bash
# Update any configuration files or environment variables
echo "OLD_CLUSTER_ID=$CLUSTER_ID"
echo "NEW_CLUSTER_ID=$NEW_CLUSTER_ID"
echo "NEW_MASTER_DNS=$MASTER_DNS"

# Update your connection strings in:
# - Application configs
# - Jenkins/Airflow jobs
# - Documentation
```

---

## Troubleshooting

**If cluster creation fails:**

```bash
# Check reason
aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query 'Cluster.Status.StateChangeReason'

# Common issues:
# - Insufficient capacity: Try different instance type or AZ
# - IAM role issues: Verify roles exist and have correct permissions
# - Subnet issues: Check subnet has available IPs
# - Security group issues: Verify SG rules
```

**If performance isn't improved:**

```bash
# Check if data skew developed
# Check S3 request metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name AllRequests \
  --dimensions Name=BucketName,Value=your-bucket \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

---

## Quick Reference Commands

```bash
# Save these for future use
alias emr-list='aws emr list-clusters --active'
alias emr-status='aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query "Cluster.Status"'
alias emr-master='aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query "Cluster.MasterPublicDnsName" --output text'
```
