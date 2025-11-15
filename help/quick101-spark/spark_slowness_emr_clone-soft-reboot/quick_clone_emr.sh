# On your edge node, create the file
cat > quick_clone_emr.sh << 'EOF'
#!/bin/bash

# Usage: ./quick_clone_emr.sh j-XXXXXXXXXXXXX

CLUSTER_ID=$1

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: ./quick_clone_emr.sh j-XXXXXXXXXXXXX"
    exit 1
fi

echo "Cloning cluster: $CLUSTER_ID"

# Extract configuration
CLUSTER_JSON=$(aws emr describe-cluster --cluster-id $CLUSTER_ID)
INSTANCE_GROUPS=$(aws emr list-instance-groups --cluster-id $CLUSTER_ID)

# Get basic info
CLUSTER_NAME=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.Name')
RELEASE_LABEL=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.ReleaseLabel')
SUBNET_ID=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.Ec2InstanceAttributes.Ec2SubnetId')
KEY_NAME=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.Ec2InstanceAttributes.Ec2KeyName')
SERVICE_ROLE=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.ServiceRole')
EC2_ROLE=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.Ec2InstanceAttributes.IamInstanceProfile')
LOG_URI=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.LogUri')

# Get applications
APPLICATIONS=$(echo "$CLUSTER_JSON" | jq -r '.Cluster.Applications[].Name' | xargs -I {} echo "Name={}" | xargs)

# Get instance types and counts
MASTER_TYPE=$(echo "$INSTANCE_GROUPS" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="MASTER") | .InstanceType')
CORE_TYPE=$(echo "$INSTANCE_GROUPS" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="CORE") | .InstanceType')
CORE_COUNT=$(echo "$INSTANCE_GROUPS" | jq -r '.InstanceGroups[] | select(.InstanceGroupType=="CORE") | .RequestedInstanceCount')

echo ""
echo "Creating new cluster with configuration:"
echo "  Name: $CLUSTER_NAME-clone"
echo "  Version: $RELEASE_LABEL"
echo "  Master: $MASTER_TYPE"
echo "  Core: $CORE_COUNT x $CORE_TYPE"
echo ""
echo "Proceed? (yes/no)"
read CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi

# Create new cluster
NEW_CLUSTER_ID=$(aws emr create-cluster \
  --name "$CLUSTER_NAME-clone" \
  --release-label $RELEASE_LABEL \
  --applications $APPLICATIONS \
  --ec2-attributes KeyName=$KEY_NAME,SubnetId=$SUBNET_ID,InstanceProfile=$EC2_ROLE \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=$MASTER_TYPE,InstanceCount=1 \
    InstanceGroupType=CORE,InstanceType=$CORE_TYPE,InstanceCount=$CORE_COUNT \
  --service-role $SERVICE_ROLE \
  --log-uri $LOG_URI \
  --enable-debugging \
  --output text)

if [ $? -eq 0 ]; then
    echo ""
    echo "SUCCESS!"
    echo "New cluster created: $NEW_CLUSTER_ID"
    echo ""
    echo "Monitor status:"
    echo "  watch -n 10 'aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query \"Cluster.Status.State\" --output text'"
else
    echo "ERROR: Failed to create cluster"
    exit 1
fi
EOF

# Make it executable
chmod +x quick_clone_emr.sh

echo "Script created successfully!"