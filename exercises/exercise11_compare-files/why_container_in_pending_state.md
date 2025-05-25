The issue you're experiencing - **only 5 executors running with lots of containers pending** - indicates **resource
contention** in your EMR cluster. The containers are pending because there aren't enough resources (CPU/memory)
available on your task nodes to start all 72 executors.

## Root Cause Analysis

### Problem: Resource Over-Allocation

```bash
Your configuration:
--num-executors 72
--executor-cores 4  
--executor-memory 16g

Required per executor: 4 cores + 16GB memory
Total cluster requirement: 72 × (4 cores + 16GB) = 288 cores + 1,152GB memory
```

**Your EMR cluster likely doesn't have enough total resources to support this configuration.**

## Diagnostic Steps

### 1. Check Your EMR Cluster Resources

```bash
# Check actual cluster capacity
yarn node -list -all

# Check available resources
yarn cluster -info

# Check current resource usage
yarn application -list
```

### 2. Calculate Realistic Resource Allocation

**Typical EMR instance types and their usable resources:**

| Instance Type | vCPUs | Memory | Usable for Spark |
|---------------|-------|--------|------------------|
| m5.xlarge     | 4     | 16GB   | 3 cores, 12GB    |
| m5.2xlarge    | 8     | 32GB   | 7 cores, 26GB    |
| m5.4xlarge    | 16    | 64GB   | 15 cores, 56GB   |
| m5.8xlarge    | 32    | 128GB  | 31 cores, 115GB  |

## Solutions

### Solution 1: Right-Size Your Executor Configuration

**For a typical EMR cluster with m5.2xlarge instances (8 vCPU, 32GB each):**

```bash
# Calculate realistic settings for m5.2xlarge × 20 nodes cluster
# Per node: 7 usable cores, 26GB usable memory
# Total cluster: 140 cores, 520GB memory

spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 8g \
  --driver-cores 2 \
  --executor-memory 8g \      # Reduced from 16g
  --executor-cores 2 \        # Reduced from 4
  --num-executors 60 \        # Reduced from 72 (60 × 2 = 120 cores)
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=120 \     # Match total cores
  --conf spark.default.parallelism=120 \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB \
  myjob.py
```

### Solution 2: Enable Dynamic Allocation (Recommended)

```bash
# Let Spark automatically scale based on available resources
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 8g \
  --driver-cores 2 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=60 \
  --conf spark.dynamicAllocation.initialExecutors=10 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=120 \
  myjob.py
```

### Solution 3: Progressive Resource Testing

Start small and scale up to find your cluster's limits:

```python
# Test script to find optimal configuration
def test_executor_scaling():
    configs_to_test = [
        {"executors": 10, "cores": 2, "memory": "8g"},
        {"executors": 20, "cores": 2, "memory": "8g"},
        {"executors": 30, "cores": 2, "memory": "8g"},
        {"executors": 40, "cores": 2, "memory": "8g"},
        {"executors": 50, "cores": 2, "memory": "8g"},
    ]

    for config in configs_to_test:
        print(f"Testing: {config}")

        # Check if all executors start successfully
        sc = spark.sparkContext
        executor_count = len(sc.statusTracker().getExecutorInfos())

        print(f"Requested: {config['executors']}, Started: {executor_count}")

        if executor_count == config['executors']:
            print(f"✅ Success with {config}")
            return config
        else:
            print(f"❌ Failed - only {executor_count} executors started")

    return None


# Run the test
optimal_config = test_executor_scaling()
```

## Common EMR Resource Issues

### Issue 1: YARN Resource Manager Limits

```bash
# Check YARN configuration
# These settings might be limiting your resources:
yarn.scheduler.maximum-allocation-mb=14336  # Max memory per container
yarn.scheduler.maximum-allocation-vcores=4  # Max cores per container
yarn.nodemanager.resource.memory-mb=14336   # Total memory per node
yarn.nodemanager.resource.cpu-vcores=7     # Total cores per node
```

### Issue 2: Instance Type Mismatch

```bash
# If you have smaller instances than expected:
# Example: m5.large (2 vCPU, 8GB) instead of m5.2xlarge

# Your config needs: 72 × (4 cores + 16GB) 
# But cluster has: 20 × (1 core + 6GB usable)
# Result: Only 5 executors can fit!
```

### Issue 3: Other Applications Using Resources

```bash
# Check what else is running
yarn application -list

# Kill competing applications if necessary
yarn application -kill <application_id>
```

## Recommended Fix for Your Situation

**Start with this conservative configuration:**

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --driver-cores 1 \
  --executor-memory 6g \        # Much smaller
  --executor-cores 2 \          # Fewer cores
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=30 \  # Start with 30 max
  --conf spark.dynamicAllocation.initialExecutors=10 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=60 \
  --conf spark.default.parallelism=60 \
  myjob.py
```

### Monitor Resource Usage

```python
def monitor_cluster_resources():
    """Monitor actual resource usage"""
    import subprocess
    import json

    # Get YARN cluster info
    result = subprocess.run(['yarn', 'cluster', '-info'],
                            capture_output=True, text=True)
    print("=== YARN Cluster Info ===")
    print(result.stdout)

    # Get node information
    result = subprocess.run(['yarn', 'node', '-list'],
                            capture_output=True, text=True)
    print("=== YARN Nodes ===")
    print(result.stdout)

    # Get current Spark executors
    sc = spark.sparkContext
    executors = sc.statusTracker().getExecutorInfos()
    print(f"=== Spark Executors ===")
    print(f"Total executors: {len(executors)}")
    for executor in executors:
        print(f"Executor {executor.executorId}: {executor.host} "
              f"(Cores: {executor.totalCores}, MaxMemory: {executor.maxMemory})")


# Run this to diagnose your cluster
monitor_cluster_resources()
```

## Expected Results

After applying the conservative configuration:

```
Before: 5 executors started, containers pending
After:  10-30 executors started successfully, no pending containers
Performance: Still good due to proper resource utilization
```

**Key takeaway**: The "containers pending" issue means you're requesting more resources than your EMR cluster can
provide. Scale down your executor requirements to match your actual cluster capacity, then gradually increase to find
the optimal configuration.