It is generally possible to reboot core nodes of an EMR cluster in addition to task nodes, but caution is advised. Core
nodes store HDFS data and run critical processes; rebooting them can cause temporary disruption or increased risk of
data unavailability if not handled carefully. Master nodes (including the application master) should be rebooted only if
you can tolerate some downtime and understand the impact on scheduling and cluster management.

Core nodes are stateful in that they store HDFS data, so rebooting them can affect cluster health if the cluster cannot
tolerate temporary node unavailability or if enough nodes do not remain running to maintain HDFS replication. However,
EMR can wait for nodes to reboot and rejoin the cluster, avoiding replacements unless nodes remain unhealthy for
extended periods.

Given that your current script reboots all instances for the cluster without discriminating node type, but previously
only rebooted task nodes (which are stateless), adding core nodes to the reboot operation is allowable if you want a
full reboot of the cluster nodes, but you should do so with:

- Understanding that core node reboot might temporarily impact running jobs or HDFS availability.
- Monitoring cluster health closely.
- Ensuring that you have data backups and enough replication.

For a graceful approach, rebooting core nodes sequentially or during maintenance windows is recommended instead of all
at once.

***
Script to include core nodes (in addition to task nodes) for reboot, while noting the above cautions, and keeping the
logic simple by rebooting all instances (task + core). The master node will still be excluded, as rebooting it generally
leads to cluster downtime and should be done cautiously with high availability or planned maintenance.

```bash
#!/bin/bash

CLUSTER_ID=$1

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: ./emr_soft_reboot.sh j-XXXXXXXXXXXXX"
    exit 1
fi

echo "Rebooting task and core instances in cluster $CLUSTER_ID..."

# Get instance IDs for both task and core nodes (exclude master)
INSTANCE_IDS=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types TASK CORE --query 'Instances[*].Ec2InstanceId' --output text)

echo "Found instances: $INSTANCE_IDS"

if [ -z "$INSTANCE_IDS" ]; then
    echo "No task or core nodes found to reboot."
    exit 0
fi

echo "Rebooting in 5 seconds... (Ctrl+C to cancel)"
sleep 5

# Reboot instances
aws ec2 reboot-instances --instance-ids $INSTANCE_IDS

echo "Reboot initiated. Monitoring cluster status..."
echo ""

# Monitor cluster status until RUNNING or timeout
for i in {1..30}; do
    STATUS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Status.State' --output text)
    echo "[$i/30] Cluster status: $STATUS"
    
    if [ "$STATUS" == "RUNNING" ]; then
        echo "Cluster is back to RUNNING state!"
        break
    fi
    
    sleep 20
done

echo ""
echo "Done! Test your jobs now."
```

This updated script will reboot all task and core nodes but leave the master nodes intact. Rebooting master nodes
requires caution and usually more controlled processes. If you want to reboot master nodes as well, it would need a
separate workflow because of the possible downtime and impact. Let me know if you want that included with appropriate
warnings. 

