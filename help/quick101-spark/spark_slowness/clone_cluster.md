# Yes! That's Exactly What the Script Does

The `generate_recreate_script.sh` I shared above **does exactly this** - it reads your existing running cluster and generates a script to create a fresh one with identical configuration.

---

## Simple 3-Step Process

**Step 1: Get your cluster ID**
```bash
# Find your running cluster
aws emr list-clusters --active

# Copy the cluster ID (looks like: j-2AXXXXXXXXXX)
```

**Step 2: Generate the recreation script**
```bash
# Run the generator with your cluster ID
./generate_recreate_script.sh j-2AXXXXXXXXXX

# This will create:
# - recreate_emr_cluster.sh (ready to run)
# - cluster_configurations_j-2AXXXXXXXXXX.json (if you have custom configs)
```

**Step 3: Create the fresh cluster**
```bash
# Run the generated script
./recreate_emr_cluster.sh

# It will ask for confirmation, then create the new cluster
# You don't need to terminate the old one first!
```

---

## Even Simpler: One-Liner Version

If you just want a quick script without all the automation, here's a simplified version:

**Save as `quick_clone_emr.sh`:**

```bash
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

echo "New cluster created: $NEW_CLUSTER_ID"
echo ""
echo "Monitor status:"
echo "  aws emr describe-cluster --cluster-id $NEW_CLUSTER_ID --query 'Cluster.Status.State'"
```

**Usage:**
```bash
chmod +x quick_clone_emr.sh
./quick_clone_emr.sh j-2AXXXXXXXXXX
```

---

## Ultra-Simple: AWS CLI One-Liner

If you just want to clone it quickly without any script:

```bash
# Set your cluster ID
OLD_CLUSTER=j-2AXXXXXXXXXX

# Clone it (creates new cluster immediately)
aws emr create-cluster \
  --name "$(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.Name' --output text)-fresh" \
  --release-label $(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.ReleaseLabel' --output text) \
  --applications $(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.Applications[].Name' --output text | xargs -I {} echo "Name={}" | xargs) \
  --ec2-attributes KeyName=$(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.Ec2InstanceAttributes.Ec2KeyName' --output text),SubnetId=$(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.Ec2InstanceAttributes.Ec2SubnetId' --output text),InstanceProfile=$(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.Ec2InstanceAttributes.IamInstanceProfile' --output text) \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=$(aws emr list-instance-groups --cluster-id $OLD_CLUSTER --query 'InstanceGroups[?InstanceGroupType==`MASTER`].InstanceType' --output text),InstanceCount=1 \
    InstanceGroupType=CORE,InstanceType=$(aws emr list-instance-groups --cluster-id $OLD_CLUSTER --query 'InstanceGroups[?InstanceGroupType==`CORE`].InstanceType' --output text),InstanceCount=$(aws emr list-instance-groups --cluster-id $OLD_CLUSTER --query 'InstanceGroups[?InstanceGroupType==`CORE`].RequestedInstanceCount' --output text) \
  --service-role $(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.ServiceRole' --output text) \
  --log-uri $(aws emr describe-cluster --cluster-id $OLD_CLUSTER --query 'Cluster.LogUri' --output text) \
  --enable-debugging
```

---

## What You DON'T Need to Do

❌ **Don't** need to terminate the old cluster first  
❌ **Don't** need to manually copy configurations  
❌ **Don't** need to know all the settings  
❌ **Don't** need to stop your jobs first  

✅ **Just** provide the cluster ID, and the script clones everything!

---

## Quick Decision Guide

**Use the full `generate_recreate_script.sh` if:**
- You want to save the configuration for future use
- You want to review before creating
- You need to modify some settings

**Use the `quick_clone_emr.sh` if:**
- You just want a fast clone
- You trust the existing configuration
- You want minimal steps


## Try This Right Now

```bash
# Quick test - just tell me your cluster ID
aws emr list-clusters --active

# Then run this (replace with your actual cluster ID)
./generate_recreate_script.sh j-XXXXXXXXXXXXX

# Review what it generated
cat recreate_emr_cluster.sh

# Create fresh cluster
./recreate_emr_cluster.sh
```
