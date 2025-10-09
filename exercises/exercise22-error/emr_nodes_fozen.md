```markdown
my emr has no jobs running. Taskin running it shows as 33. When i submit the  a job it goes inot accept state whats going on
```

It sounds like you have zombie tasks consuming your cluster resources! Here's what's likely happening:
The Problem:

**The Problem:**
- Those 33 running tasks are holding onto cluster resources (memory/vCores)
- Your new job is **ACCEPTED** by YARN but can't get resources to actually **RUN**
- Even though you see "no jobs running," those orphaned tasks are blocking everything

## Quick Diagnosis Steps:

1. **Check YARN ResourceManager UI:**
   - Go to `http://<master-node>:8088`
   - Look at "Cluster Metrics" - check Available vs Used Memory/VCores
   - Check the "Running Applications" section

2. **Find the stuck tasks via SSH:**
   ```bash
   yarn application -list
   # or
   yarn application -list -appStates RUNNING
   ```

## Solutions:

**Option 1: Kill stuck applications**
```bash
yarn application -kill <application_id>
```

**Option 2: Kill all running applications**
```bash
for app in $(yarn application -list 2>/dev/null | grep "application_" | awk '{print $1}'); do
  yarn application -kill $app
done
```

**Option 3: Restart YARN (if killing doesn't work)**
```bash
# On the master node
sudo systemctl stop hadoop-yarn-resourcemanager
sudo systemctl start hadoop-yarn-resourcemanager
```

After clearing those zombie tasks, your cluster resources should free up and new jobs should start running immediately.

## Still a problem 
Are The unhealthy and decommissioned nodes are likely blocking resource allocation or reducing your available capacity below what your jobs need.

## Here's how to fix this:

### 1. **Remove the unhealthy node from YARN:**

```bash
# Get the node hostname
yarn node -list -all

# Decommission the unhealthy node
yarn rmadmin -refreshNodes
```

### 2. **For both unhealthy/decommissioned nodes - terminate them from EMR:**

**Option A: Via AWS CLI**
```bash
aws emr list-instances --cluster-id <your-cluster-id> --instance-states RUNNING

# Find the instance IDs of unhealthy nodes, then:
aws emr modify-instance-groups --cluster-id <your-cluster-id> \
  --instance-groups InstanceGroupId=<ig-id>,InstanceCount=<new-count>
```

**Option B: Via EMR Console:**
- Go to EMR → Your Cluster → Hardware
- Reduce the instance count to remove unhealthy nodes

>> Option B: helped
