A custom EMR cluster scaling configuration file that includes all the elements you mentioned. Here's a breakdown of the
key components:

### Core Configuration:

- Basic EMR cluster setup with Spark and Hadoop applications
- Standard master and core node configuration

### Custom Scaling Features:

1. **Number of servers to scale to**:
    - Each application has its own `MaxCapacity` and `MinCapacity` settings
    - For example, `data-processing-app` can scale from 2 to 20 nodes

2. **Task node mix (spot vs on-demand)**:
    - Each app defines its preferred instance mix
    - For example, `data-processing-app` uses 80% spot instances and 20% on-demand
    - `ml-training-app` uses a higher percentage of on-demand (40%) for more stability

3. **Instance count and type assignment by app**:
    - Each application has specific instance types allocated
    - For example, `data-processing-app` uses memory-optimized instances (r5 family)
    - `ml-training-app` uses GPU instances (p3 and g4dn families)

### Additional Features:

- Scaling triggers based on YARN memory availability and container pending ratio
- Timeout settings that switch to on-demand if spot instances aren't available
- Tagging for cost allocation

This configuration gives you a flexible setup that will automatically adjust resources based on the specific Spark
application (appName) that's running. You can easily modify the instance types, counts, and spot/on-demand ratios for
each application type.

## Losing Spot intances

**Spark job is still running** but the **Provisioned Spot Capacity is dropping rapidly (e.g., from 49 → 22 → 15 → 4 → 0)
**, this likely indicates **Spot instance revocation or scaling down**, which can severely affect your job performance
or even cause failures. Here's a breakdown of what might be happening and how to address it:

---

## 🔍 What’s Likely Happening

### 1. **Spot Instance Interruptions**

* **AWS Spot Instances** can be revoked **at any time** when AWS needs capacity or if the market price exceeds your
  bid/threshold.
* When they are interrupted, your cluster **loses executors**, and this results in reduced capacity (which you’re
  seeing).

### 2. **Auto-Scaling Behavior**

* If your cluster is set to **auto-scale**, and Spark thinks it's underutilized (e.g., executors idle), the cluster may
  **scale down automatically**, even though tasks are still pending due to long stages or skew.

### 3. **YARN/MESOS/Standalone Resource Mismatch**

* If you're using YARN or another cluster manager, and there’s pressure on shared resources, Spark may be **preempted**
  or **starved** of resources temporarily.

### 4. **Misconfigured Timeout or Idle Executor Policy**

* If idle executors are set to time out quickly, they may be removed, even though the job will still need them later (
  e.g., in next stages).

---

## ✅ What You Can Do to Fix or Mitigate It

### Immediate Checks:

1. **Check the Spark UI**:

    * Are tasks stuck in a stage or retrying?
    * Are there lost executors? (check “Executors” tab)
    * Are stages pending with no available executors?

2. **Cloud Provider Console (e.g., AWS EMR, Databricks)**:

    * Look for messages like “Spot instance interruption” or “Capacity not available.”
    * Check if the **On-Demand fallback** is configured.

3. **Review Spark Logs**:

    * Search for messages like: `Lost executor`, `Container killed`, `Node lost`, `Executor decommissioned`.

---

### Mitigation Strategies:

#### 🔁 Fallback to On-Demand

If you are using only Spot instances:

* Enable **On-Demand fallback** in your cluster config.

    * **EMR**: Use `InstanceFleet` with `TargetOnDemandCapacity` as backup.
    * **Databricks**: Use a **Spot/On-Demand mix** and set “Spot fallbacks” in autoscaling settings.

#### 🛠️ Modify Spark Configs

In your job configuration, consider adjusting:

```bash
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.minExecutors=10
--conf spark.dynamicAllocation.executorIdleTimeout=300s
--conf spark.dynamicAllocation.schedulerBacklogTimeout=1s
```

* Increase `executorIdleTimeout` to avoid quick executor termination.
* Tune `minExecutors` to prevent scale-down to 0.

#### 📉 Use Fewer or Smaller Spot Nodes

If losing big capacity chunks (e.g., 49 to 0), switch to **smaller instance types** (e.g., `m5.xlarge` instead
of `m5.4xlarge`) to reduce the blast radius of a revoked instance.

---

## 🧠 Recommendation

If you're on **EMR**, configure your cluster with an `InstanceFleet` or `InstanceGroup` with:

* **Spot Core nodes**, but include some **On-Demand or Reserved instances**.
* Enable **termination protection** if available.

If you're on **Databricks**, ensure:

* `Cluster autoscaling` is enabled with **On-Demand fallback**.
* You’re not hitting the **instance quota** or **region capacity limits**.

---

