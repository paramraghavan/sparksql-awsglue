Script that reboots EMR nodes sequentially by node type (task, core, then master) to
minimize cluster disruption. It includes warnings and confirms before each reboot step:

```bash
#!/bin/bash

CLUSTER_ID=$1

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: ./emr_soft_reboot.sh j-XXXXXXXXXXXXX"
    exit 1
fi

# Function to reboot instances by type sequentially
reboot_instances_by_type() {
    NODE_TYPE=$1
    echo ""
    echo "Preparing to reboot $NODE_TYPE nodes..."
    
    INSTANCE_IDS=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types $NODE_TYPE --query 'Instances[*].Ec2InstanceId' --output text)
    
    if [ -z "$INSTANCE_IDS" ]; then
        echo "No $NODE_TYPE nodes found."
        return
    fi
    
    echo "Found $NODE_TYPE instances: $INSTANCE_IDS"
    echo "WARNING: Rebooting $NODE_TYPE nodes. This may cause temporary disruption."
    echo "Proceed to reboot $NODE_TYPE nodes in 5 seconds... (Ctrl+C to cancel)"
    sleep 5
    
    aws ec2 reboot-instances --instance-ids $INSTANCE_IDS
    echo "$NODE_TYPE nodes reboot initiated. Waiting for cluster to stabilize..."
    
    for i in {1..30}; do
        STATUS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Status.State' --output text)
        echo "[$i/30] Cluster status: $STATUS"
        if [ "$STATUS" == "RUNNING" ]; then
            echo "Cluster is back to RUNNING state after $NODE_TYPE reboot."
            break
        fi
        sleep 20
    done
}

echo "Starting sequential reboot of EMR cluster nodes: Task -> Core -> Master."

reboot_instances_by_type TASK
reboot_instances_by_type CORE

# Master reboot has the highest impact, ask for confirmation
read -p "Master node reboot can cause downtime. Reboot master nodes? (yes/no): " CONFIRM
if [[ "$CONFIRM" == "yes" ]]; then
    reboot_instances_by_type MASTER
else
    echo "Skipping master node reboot."
fi

echo ""
echo "Sequential reboot process completed. Test your jobs now."
```

This approach reduces risk by rebooting tasks and cores first while allowing you to decide if and when to reboot the
master node. It waits for the cluster to stabilize between steps.
