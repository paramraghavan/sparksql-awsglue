# Complete Guide: Mounting EFS on EMR Cluster Nodes

## The Problem

Your EMR master node can see the EFS mount, but task/worker nodes cannot. This happens because the mount configuration
only ran on the master node, not on all nodes in the cluster.

---

## Part 1: For NEW Clusters (Bootstrap Action)

The best solution for new clusters is a **bootstrap action** that runs on all nodes when they launch.

### Mount Script (`mount-efs.sh`)

```bash
#!/bin/bash
set -x

# Variables - customize these
EFS_ID="fs-xxxxxxxx"
MOUNT_POINT="/mnt/your-efs-mount"
REGION="us-east-1"

# Log output for debugging
exec > /tmp/efs-mount.log 2>&1

echo "Starting EFS mount script on $(hostname)"

# Install EFS utilities (amazon-efs-utils)
if ! command -v mount.efs &> /dev/null; then
    echo "Installing amazon-efs-utils..."
    sudo yum install -y amazon-efs-utils
fi

# Create the mount point if it doesn't exist
if [ ! -d "$MOUNT_POINT" ]; then
    echo "Creating mount point: $MOUNT_POINT"
    sudo mkdir -p "$MOUNT_POINT"
fi

# Check if already mounted
if mountpoint -q "$MOUNT_POINT"; then
    echo "$MOUNT_POINT is already mounted"
    exit 0
fi

# Mount EFS with TLS encryption
echo "Mounting EFS $EFS_ID to $MOUNT_POINT"
sudo mount -t efs -o tls,iam "$EFS_ID":/ "$MOUNT_POINT"

# Verify mount succeeded
if mountpoint -q "$MOUNT_POINT"; then
    echo "EFS mounted successfully"
    
    # Add to fstab for persistence
    if ! grep -q "$EFS_ID" /etc/fstab; then
        echo "$EFS_ID:/ $MOUNT_POINT efs _netdev,tls,iam 0 0" | sudo tee -a /etc/fstab
    fi
else
    echo "ERROR: Failed to mount EFS"
    exit 1
fi

# Set permissions if needed
sudo chmod 755 "$MOUNT_POINT"

echo "EFS mount script completed"
```

### Using the Bootstrap Action

**Option A: AWS CLI**

```bash
aws emr create-cluster \
    --name "My Spark Cluster" \
    --release-label emr-6.10.0 \
    --applications Name=Spark \
    --bootstrap-actions Path=s3://your-bucket/scripts/mount-efs.sh,Name="Mount EFS" \
    --ec2-attributes KeyName=your-key,SubnetId=subnet-xxx \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole
```

**Option B: With Arguments (more flexible)**

```bash
#!/bin/bash
set -x
EFS_ID="$1"
MOUNT_POINT="$2"
# ... rest of script uses these variables
```

Then call with:

```bash
--bootstrap-actions Path=s3://your-bucket/scripts/mount-efs.sh,Name="Mount EFS",Args=[fs-xxxxxxxx,/mnt/your-efs-mount]
```

**Option C: EMR Console**

1. Upload `mount-efs.sh` to S3
2. In EMR console → Create Cluster → Go to advanced options
3. Under **Bootstrap Actions**, add:
    - Script location: `s3://your-bucket/scripts/mount-efs.sh`

---

## Part 2: For ALREADY RUNNING Clusters

### Option 1: SSM Run Command (Best for multiple nodes)

If your EMR nodes have SSM agent installed:

```bash
# Get instance IDs of your EMR cluster nodes
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:elasticmapreduce:job-flow-id,Values=j-XXXXXXXXXXXXX" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text)

# Run mount command on all nodes
aws ssm send-command \
  --document-name "AWS-RunShellScript" \
  --instance-ids $INSTANCE_IDS \
  --parameters 'commands=[
    "sudo yum install -y amazon-efs-utils",
    "sudo mkdir -p /mnt/your-efs-mount",
    "sudo mount -t efs -o tls fs-xxxxxxxx:/ /mnt/your-efs-mount",
    "echo fs-xxxxxxxx:/ /mnt/your-efs-mount efs _netdev,tls 0 0 | sudo tee -a /etc/fstab"
  ]'

# Check the command status
aws ssm list-command-invocations --command-id <command-id-from-above> --details
```

### Option 2: EMR Step with command-runner.jar

```bash
aws emr add-steps --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=CUSTOM_JAR,Name="Mount EFS on all nodes",Jar="command-runner.jar",ActionOnFailure=CONTINUE,Args=["bash","-c","
    # Mount on master
    sudo yum install -y amazon-efs-utils
    sudo mkdir -p /mnt/your-efs-mount
    sudo mount -t efs -o tls fs-xxxxxxxx:/ /mnt/your-efs-mount
    
    # Get all node IPs and mount on each
    for ip in \$(yarn node -list 2>/dev/null | grep -oE '([0-9]{1,3}\.){3}[0-9]{1,3}'); do
      ssh -o StrictHostKeyChecking=no \$ip 'sudo yum install -y amazon-efs-utils; sudo mkdir -p /mnt/your-efs-mount; sudo mount -t efs -o tls fs-xxxxxxxx:/ /mnt/your-efs-mount'
    done
  "]
```

### Option 3: Script from Master Node

SSH to master node and run this script:

```bash
#!/bin/bash

EFS_ID="fs-xxxxxxxx"
MOUNT_POINT="/mnt/your-efs-mount"

# Mount locally on master
sudo yum install -y amazon-efs-utils
sudo mkdir -p $MOUNT_POINT
sudo mount -t efs -o tls $EFS_ID:/ $MOUNT_POINT

# Get all worker node IPs
WORKER_IPS=$(yarn node -list 2>/dev/null | grep -oE '([0-9]{1,3}\.){3}[0-9]{1,3}')

# Mount on each worker
for ip in $WORKER_IPS; do
    echo "Mounting on $ip..."
    ssh -o StrictHostKeyChecking=no $ip "
        sudo yum install -y amazon-efs-utils
        sudo mkdir -p $MOUNT_POINT
        sudo mount -t efs -o tls $EFS_ID:/ $MOUNT_POINT
    "
done

echo "Done! Verifying mounts..."
for ip in $WORKER_IPS; do
    echo "$ip: $(ssh -o StrictHostKeyChecking=no $ip 'mountpoint $MOUNT_POINT')"
done
```

### Option 4: SSH to Each Node Manually

```bash
# SSH to master node first
ssh -i your-key.pem hadoop@<master-public-ip>

# List all nodes
yarn node -list 2>/dev/null | grep -oE '([0-9]{1,3}\.){3}[0-9]{1,3}'
# Or: hdfs dfsadmin -report | grep "^Name"

# SSH to each node and run:
sudo yum install -y amazon-efs-utils
sudo mkdir -p /mnt/your-efs-mount
sudo mount -t efs -o tls fs-xxxxxxxx:/ /mnt/your-efs-mount
```

---

## Part 3: Debugging

### Compare with Working Mount

```bash
# On master node - compare working vs broken mount
cat /etc/fstab
mount | grep efs
```

### Check Logs

```bash
# Check the mount script log
cat /tmp/efs-mount.log

# Check if bootstrap ran
ls -la /emr/instance-controller/lib/bootstrap-actions/

# Check system logs
sudo journalctl -u amazon-efs-mount-watchdog
dmesg | grep -i efs
dmesg | tail -50
```

### Manual Mount Test

```bash
# Check if EFS utils are installed
which mount.efs

# Check if mount point exists
ls -la /mnt/your-efs-mount

# Try manual mount
sudo mount -t efs -o tls fs-xxxxxxxx:/ /mnt/your-efs-mount
```

---

## Part 4: Prerequisites Checklist

| Item                   | Description                                                              | Check |
|------------------------|--------------------------------------------------------------------------|-------|
| **Security Groups**    | EFS security group allows inbound port 2049 from EMR node security group | ☐     |
| **Mount Targets**      | EFS mount target exists in the same subnet/AZ as task nodes              | ☐     |
| **IAM Permissions**    | EMR nodes have IAM permissions for EFS (if using IAM auth)               | ☐     |
| **Script Permissions** | Bootstrap script is executable (`chmod +x`)                              | ☐     |
| **S3 Access**          | Script uploaded to S3 bucket EMR can access                              | ☐     |

### Check EFS Mount Targets

```bash
aws efs describe-mount-targets --file-system-id fs-xxxxxxxx
```

### Security Group Rule Needed

- **Source:** EMR node security group
- **Destination:** EFS security group
- **Port:** 2049 (NFS)
- **Protocol:** TCP

---

## ⚠️ Important: Auto-Scaling Caveat

If your cluster auto-scales and adds **new task nodes after you manually mounted**, those new nodes **won't have the EFS
mount**. Solutions:

1. Recreate the cluster with a bootstrap action (recommended)
2. Set up a cron job or systemd service that checks/mounts on boot
3. Use a custom AMI with the mount pre-configured

---

## Quick Reference

| Scenario                    | Best Solution                                  |
|-----------------------------|------------------------------------------------|
| New cluster                 | Bootstrap action                               |
| Running cluster with SSM    | SSM Run Command                                |
| Running cluster without SSM | Script from master node                        |
| Auto-scaling cluster        | Bootstrap action (requires cluster recreation) |