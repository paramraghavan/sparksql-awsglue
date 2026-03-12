Actually, **no**—when you explicitly call `input_df.repartition(2000, f.rand())`, that specific operation will use *
*2000** partitions regardless of what your global `spark.sql.shuffle.partitions` is set to.

However, you should **still set the global config to 2000**, and here is why:

### 1. The "Hidden" Shuffles

The `repartition` command only controls the data distribution *going into* the `process` function. Once the data is
inside that "black box" function, any subsequent `join`, `groupBy`, or `distinct` will trigger a **new shuffle**.

If you haven't set `spark.sql.shuffle.partitions=2000`, Spark will immediately collapse your 2,000 carefully balanced
partitions back down to the default **200**. This would undo all your hard work, leading right back to the 295 GB
bottleneck and disk spilling.

### 2. The Relationship Between the Two

Think of it this way:

* **`repartition(2000, f.rand())`**: This is your "Entry Strategy" to break up initial data skew.
* **`spark.sql.shuffle.partitions=2000`**: This is your "Safety Net" that keeps the data spread out for every internal
  step within the `process` function.

---

### How to check your EMR Disk Space

You asked about checking the disk to see if it can handle that 295 GB shuffle. Since your shuffle write is 4.1 GB but
the **read** (the data in flight) is 295 GB, you need to ensure your "Task" nodes have enough **Instance Store (SSD)**
or **EBS volume** capacity.

**To check disk usage during the job:**

1. **Ganglia/CloudWatch:** Look at the `Filtered: Local Storage Used` or `DiskUtil` metrics.
2. **SSH into a Core/Task Node:** Run the command `df -h`.

* Look for the mount point `/mnt`, `/mnt1`, etc. This is where Spark stores its shuffle blocks.
* If any node shows **>90% usage**, that is why your EMR cluster is "flapping" (scaling up to get more disk, then
  crashing when a disk fills up).

### Recommended EMR Disk Setup

If you see your disks filling up, you don't necessarily need more nodes; you might just need **larger EBS volumes** per
node.

* **Recommendation:** Ensure each node has at least **100 GB - 200 GB** of EBS storage (gp3) attached. For a 295 GB
  shuffle, you want your total cluster disk capacity to be at least **1 TB** to account for overhead and replication.

---