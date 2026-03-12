> Input_df has 1000 files for total of 28 gb , that means each file is 28 mb, when the read happens how many partitons
> will EMR read this shewn reading into dataframe

By default, Spark usually assigns **one partition per file** when reading from S3, provided the files are not "
splittable" or excessively large.

Since you have 1,000 files, your `input_df` likely starts with **1,000 partitions**.

### The Math of Your Spark Read

When Spark reads those 28 GB across 1,000 files:

* **Data per partition:** $28 \text{ GB} / 1000 = 28 \text{ MB}$.
* **Partition Count:** 1,000.

While 28 MB per partition is a bit small (the "sweet spot" for Spark is usually 128 MB to 200 MB), having 1,000
partitions is actually a good starting point for parallelism.

### Why the 295 GB Shuffle still happens

Even though you start with 1,000 partitions, the "explosion" happens the moment you enter the `process` function. Here
is why the initial 1,000 partitions aren't enough to save you:

1. **The Shuffle Reset:** Most heavy operations (Joins/GroupBys) inside your `process` function will trigger a shuffle.
   By default, Spark resets the partition count to `spark.sql.shuffle.partitions`, which is **200**.
2. **The Bottleneck:** You go from 1,000 parallel streams of data down to **200**. Those 200 tasks are now trying to
   process 295 GB of shuffled data. Each task is trying to juggle ~1.5 GB of data in memory.
3. **The Crash:** This is why your EMR cluster starts scaling up to 42 nodes—it's trying to find enough RAM and Disk to
   hold those 200 massive, bloated tasks.

### Why `repartition(2000, rand())` is the right move

By running that command before the function, you are overriding that 200-partition bottleneck.

* You take your 1,000 initial partitions (28 MB each).
* You force them into **2,000 partitions**.
* This makes each task roughly **14 MB** of input data.
* When that 10x data expansion happens inside your function, each task only grows to **150 MB** (295 GB / 2000) instead
  of **1.5 GB**.

**150 MB fits perfectly in the RAM of almost any EMR instance type**, which will stop the disk spilling and the 64-hour
hang.

---

### How to verify your initial partition count

If you want to be 100% sure how many partitions your EMR started with, run this right after you define `input_df`:

```python
print(f"Initial Partitions: {input_df.rdd.getNumPartitions()}")

```
