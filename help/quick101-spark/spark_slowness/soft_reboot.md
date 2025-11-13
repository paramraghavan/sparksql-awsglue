# EMR "Soft Reboot" Options

**EMR doesn't have a true "soft reboot" feature** like Windows, but here are your best alternatives from
least to most disruptive:

---

## Option 1: Restart Services Only (Least Disruptive)

**SSH to master node and restart key services:**

```bash
# Get master node DNS
CLUSTER_ID=j-XXXXXXXXXXXXX
MASTER_DNS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.MasterPublicDnsName' --output text)

# SSH to master
ssh -i /path/to/your-key.pem hadoop@$MASTER_DNS

# Restart YARN ResourceManager
sudo systemctl restart hadoop-yarn-resourcemanager

# Restart Spark History Server
sudo systemctl restart spark-history-server

# Restart HDFS NameNode
sudo systemctl restart hadoop-hdfs-namenode

# Check status
sudo systemctl status hadoop-yarn-resourcemanager
sudo systemctl status spark-history-server
```

**Clean up temp files while you're there:**

```bash
# Clear YARN local directories
sudo rm -rf /mnt/yarn/local/*
sudo rm -rf /mnt1/yarn/local/*

# Clear Spark temp directories
sudo rm -rf /mnt/spark/temp/*
sudo rm -rf /mnt1/spark/temp/*

# Clear old logs (older than 7 days)
sudo find /var/log/hadoop-yarn -type f -mtime +7 -delete
sudo find /var/log/spark -type f -mtime +7 -delete
```

**Restart on all core/task nodes:**

```bash
# Get all node IPs
aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types CORE TASK --query 'Instances[*].PrivateIpAddress' --output text

# SSH to each core node and run:
sudo systemctl restart hadoop-yarn-nodemanager
sudo rm -rf /mnt/yarn/local/*
sudo rm -rf /mnt1/yarn/local/*
```

**⚠️ Limitation:** This doesn't clear memory fragmentation or deeply stuck processes.

---

## Option 2: Restart EC2 Instances (Medium Disruption)

This is the closest to a "soft reboot":

```bash
# Get all instance IDs in the cluster
INSTANCE_IDS=$(aws emr list-instances --cluster-id $CLUSTER_ID --query 'Instances[*].Ec2InstanceId' --output text)

# Reboot all instances (EMR will handle it gracefully)
aws ec2 reboot-instances --instance-ids $INSTANCE_IDS

# Monitor cluster status
watch -n 10 'aws emr describe-cluster --cluster-id $CLUSTER_ID --query "Cluster.Status.State" --output text'
```

**What happens:**

- EC2 instances reboot (like Windows reboot)
- EMR services auto-restart
- Cluster stays in `RUNNING` state
- Takes 5-10 minutes
- Clears memory, temp files, process space

**⚠️ Important:**

- Running jobs will fail
- YARN state is preserved (which might be your problem)
- Better than full recreation but not as clean

---

## Option 3: Rolling Restart via Resize (Low Risk)

Trick: Force EMR to replace nodes without cluster downtime:

```bash
# Get current core instance count
CURRENT_COUNT=$(aws emr list-instance-groups --cluster-id $CLUSTER_ID --query 'InstanceGroups[?InstanceGroupType==`CORE`].RequestedInstanceCount' --output text)

# Scale down by 1
aws emr modify-instance-groups --cluster-id $CLUSTER_ID --instance-groups InstanceGroupId=ig-XXXXX,InstanceCount=$((CURRENT_COUNT-1))

# Wait 2 minutes, then scale back up
sleep 120
aws emr modify-instance-groups --cluster-id $CLUSTER_ID --instance-groups InstanceGroupId=ig-XXXXX,InstanceCount=$CURRENT_COUNT
```

This gets you fresh nodes, but only works for CORE/TASK groups, not MASTER.

---

## Option 4: Clone Configuration + Quick Recreation (Cleanest)

This is really your best option - it's faster than you think:

```bash
# 1. Capture config (2 minutes)
aws emr describe-cluster --cluster-id $CLUSTER_ID > cluster_backup.json

# 2. Create recreation script (already done from previous steps)

# 3. Terminate old cluster (1 minute to submit)
aws emr terminate-clusters --cluster-ids $CLUSTER_ID

# 4. Start new cluster immediately (don't wait for termination)
./recreate_emr_cluster.sh

# Total downtime: ~10-15 minutes for new cluster to be RUNNING
```

---

## Comparison Table

| Method          | Downtime  | Memory Clear | YARN State Clear | Disk Cleanup | Effectiveness |
|-----------------|-----------|--------------|------------------|--------------|---------------|
| Service Restart | None      | ❌ No         | ❌ No             | ⚠️ Partial   | 30%           |
| EC2 Reboot      | 5-10 min  | ✅ Yes        | ❌ No             | ⚠️ Partial   | 60%           |
| Rolling Resize  | Minimal   | ✅ Yes        | ✅ Yes            | ✅ Yes        | 75%           |
| Full Recreation | 10-15 min | ✅ Yes        | ✅ Yes            | ✅ Yes        | 100%          |

---

## My Recommendation

**For your situation (3x slowdown):**

Try in this order:

1. **Start with EC2 Reboot** (Option 2) - Quick test if it helps
   ```bash
   aws ec2 reboot-instances --instance-ids $(aws emr list-instances --cluster-id $CLUSTER_ID --query 'Instances[*].Ec2InstanceId' --output text)
   ```

2. **If still slow** → Do full recreation (Option 4)
    - It's only 15 minutes and guarantees fresh start
    - Much cleaner than debugging accumulated issues

3. **Going forward** → Schedule weekly recreation or enable auto-termination

---

## Quick Script for EC2 Reboot

Save as `emr_soft_reboot.sh`:

```bash
#!/bin/bash

CLUSTER_ID=$1

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: ./emr_soft_reboot.sh j-XXXXXXXXXXXXX"
    exit 1
fi

echo "Rebooting all instances in cluster $CLUSTER_ID..."

# Get all instance IDs
INSTANCE_IDS=$(aws emr list-instances --cluster-id $CLUSTER_ID --query 'Instances[*].Ec2InstanceId' --output text)

echo "Found instances: $INSTANCE_IDS"
echo "Rebooting in 5 seconds... (Ctrl+C to cancel)"
sleep 5

# Reboot
aws ec2 reboot-instances --instance-ids $INSTANCE_IDS

echo "Reboot initiated. Monitoring cluster status..."
echo ""

# Monitor
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

**Use it:**

```bash
chmod +x emr_soft_reboot.sh
./emr_soft_reboot.sh j-XXXXXXXXXXXXX
```