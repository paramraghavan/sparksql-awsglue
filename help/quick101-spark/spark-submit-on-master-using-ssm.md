# How to use AWS Systems Manager (SSM) to execute `spark-submit` commands on your EMR master node remotely. 

## Prerequisites

First, ensure your EMR cluster has SSM enabled. You can do this during cluster creation:

```bash
aws emr create-cluster \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
  --applications Name=Spark \
  --bootstrap-actions Path=s3://your-bucket/install-ssm-agent.sh
```

## Method 1: Direct SSM Command

```bash
#!/bin/bash
CLUSTER_NAME="my-emr-cluster"

# Get cluster ID and master instance ID
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[?Name=='$CLUSTER_NAME'].Id" --output text)
MASTER_INSTANCE_ID=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)

# Execute spark-submit via SSM
aws ssm send-command \
  --instance-ids $MASTER_INSTANCE_ID \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
    "sudo su - hadoop",
    "cd /home/hadoop/process_model",
    "spark-submit --master yarn --deploy-mode cluster my_job.py"
  ]' \
  --output text
```

## Method 2: With Command ID Tracking

```bash
#!/bin/bash
CLUSTER_NAME="my-emr-cluster"
SCRIPT_PATH="/home/hadoop/process_model/my_job.py"

# Get instance details
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[?Name=='$CLUSTER_NAME'].Id" --output text)
MASTER_INSTANCE_ID=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)

echo "Submitting Spark job via SSM..."
echo "Cluster ID: $CLUSTER_ID"
echo "Master Instance: $MASTER_INSTANCE_ID"

# Submit command and get command ID
COMMAND_ID=$(aws ssm send-command \
  --instance-ids $MASTER_INSTANCE_ID \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[
    'sudo su - hadoop -c \"cd /home/hadoop/process_model && spark-submit --master yarn --deploy-mode client --driver-memory 2g --executor-memory 2g --num-executors 2 $SCRIPT_PATH\"'
  ]" \
  --query 'Command.CommandId' \
  --output text)

echo "Command ID: $COMMAND_ID"

# Wait for completion and get output
echo "Waiting for command completion..."
aws ssm wait command-executed --command-id $COMMAND_ID --instance-id $MASTER_INSTANCE_ID

# Get command output
echo "Command output:"
aws ssm get-command-invocation --command-id $COMMAND_ID --instance-id $MASTER_INSTANCE_ID --query 'StandardOutputContent' --output text
```

## Method 3: Advanced with Error Handling

```bash
#!/bin/bash

submit_spark_job_via_ssm() {
    local cluster_name=$1
    local python_script=$2
    local spark_args=$3

    # Get cluster and instance details
    local cluster_id=$(aws emr list-clusters --active --query "Clusters[?Name=='$cluster_name'].Id" --output text)
    
    if [ -z "$cluster_id" ]; then
        echo "Error: Cluster '$cluster_name' not found"
        return 1
    fi

    local master_instance_id=$(aws emr list-instances --cluster-id $cluster_id --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)

    # Prepare spark-submit command
    local spark_command="cd /home/hadoop/process_model && spark-submit --master yarn $spark_args $python_script"

    echo "Executing: $spark_command"

    # Execute via SSM
    local command_id=$(aws ssm send-command \
        --instance-ids $master_instance_id \
        --document-name "AWS-RunShellScript" \
        --parameters "commands=[
            'sudo su - hadoop -c \"$spark_command\"'
        ]" \
        --query 'Command.CommandId' \
        --output text)

    # Monitor execution
    echo "Command ID: $command_id"
    aws ssm wait command-executed --command-id $command_id --instance-id $master_instance_id

    # Get results
    local status=$(aws ssm get-command-invocation --command-id $command_id --instance-id $master_instance_id --query 'Status' --output text)
    
    if [ "$status" = "Success" ]; then
        echo "✅ Spark job completed successfully"
        aws ssm get-command-invocation --command-id $command_id --instance-id $master_instance_id --query 'StandardOutputContent' --output text
    else
        echo "❌ Spark job failed"
        aws ssm get-command-invocation --command-id $command_id --instance-id $master_instance_id --query 'StandardErrorContent' --output text
    fi
}

# Usage
submit_spark_job_via_ssm "my-emr-cluster" "my_job.py" "--deploy-mode client --driver-memory 2g --executor-memory 2g"
```

## Method 4: With Supporting Files

If your job needs additional Python files:

```bash
SPARK_COMMAND="spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files /home/hadoop/process_model/utils.py,/home/hadoop/process_model/config.py \
  --driver-memory 2g \
  --executor-memory 2g \
  --num-executors 3 \
  /home/hadoop/process_model/my_job.py"

aws ssm send-command \
  --instance-ids $MASTER_INSTANCE_ID \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[
    'sudo su - hadoop -c \"cd /home/hadoop/process_model && $SPARK_COMMAND\"'
  ]"
```

## Key Points:

- **SSM Agent** must be installed and running on EMR nodes
- Use `sudo su - hadoop` to switch to the hadoop user context
- **Deploy mode**: Use `client` mode to see logs in SSM output, `cluster` mode for production
- **File paths**: Ensure all Python files are accessible to the hadoop user
- **Monitoring**: Use command IDs to track execution and retrieve logs
