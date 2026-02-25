```python
        # Read CSV file
readAsDF = spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv(sys.argv[1])

# Repartition the DataFrame
partitionedDF = readAsDF.repartition(2)

# Apply transformations and aggregation
countDF = partitionedDF
.where(col("Age") < 40)
.select("Age", "Gender", "Country", "state")
.groupBy("Country")
.count()

# Collect results and log
results = countDF.collect()
```

# Spark Execution Analysis: Stages, Shuffles, and Jobs

Below is the detailed breakdown of your Spark code execution on an EMR cluster with **8 executors** and **5 cores per
executor** (40 total slots).

---

## 1. Action vs. Transformation Breakdown

| Code Snippet                     | Category                  | Spark Behavior                                                                 |
|----------------------------------|---------------------------|--------------------------------------------------------------------------------|
| `spark.read.csv(...)`            | **Transformation**        | Creates the logical plan to read the file.                                     |
| `.option("inferSchema", "true")` | **Action (Hidden)**       | Triggers an immediate **Job** to scan the data and determine types.            |
| `.repartition(2)`                | **Wide Transformation**   | Triggers a **Shuffle**. Data is redistributed across the network.              |
| `.where(...)` / `.select(...)`   | **Narrow Transformation** | Executed in-memory; no data moves between executors.                           |
| `.groupBy(...).count()`          | **Wide Transformation**   | Triggers a **Shuffle**. Data with the same "Country" is sent to the same task. |
| `.collect()`                     | **Action**                | Triggers the main **Job** and pulls results to the Driver.                     |

---

## 2. The Execution Plan: Stages and Tasks

Spark divides your code into **Stages** at every *Shuffle Boundary* (Wide Transformation).

### Stage 1: The Read (Narrow)

- **Trigger:** `inferSchema` and the initial read.
- **Task Count:** Based on input file size. If your file is 5GB, Spark divides it by the default block size (128MB),
  resulting in **~40 tasks**.
- **Cluster Utilization:** High. 40 tasks fit perfectly into your 40 available slots.

### Stage 2: The Repartition (Wide Shuffle)

- **Trigger:** `.repartition(2)`.
- **Task Count:** **Exactly 2 tasks**.
- **The Bottleneck:** Even though you have 40 cores, **38 cores will be idle**. All your data is squeezed into just 2
  partitions. This is the most inefficient part of your script.

### Stage 3: Filter & Map (Narrow)

- **Trigger:** `.where(col("Age") < 40)` and `.select(...)`.
- **Task Count:** **2 tasks** (inherited from the previous stage).
- **Logic:** Spark performs the filter and selection on the 2 partitions created in Stage 2.

### Stage 4: GroupBy Aggregation (Wide Shuffle)

- **Trigger:** `.groupBy("Country").count()`.
- **Task Count:** **200 tasks** (Default Shuffle Partition Size).
- **The Shuffle:** Spark takes the data from the 2 partitions and hashes it across 200 new partitions to ensure all "
  USA" rows are in one place, all "UK" rows in another, etc.
- **Cluster Utilization:** Good. 200 tasks will run in 5 waves of 40 tasks.

---

## 3. Summary of Key Concepts

### Narrow vs. Wide Dependencies

- **Narrow:** `where` and `select`. Each input partition contributes to exactly one output partition. These stay within
  the same stage.
- **Wide:** `repartition` and `groupBy`. One input partition contributes to many output partitions. These create a *
  *Stage Boundary** and require a **Shuffle**.

### The Role of Default Shuffle Size

The `spark.sql.shuffle.partitions` (default = 200) determines the number of tasks **after** a shuffle (Stage 4).

- Stage 1 (Read) ignores this number.
- Stage 2 (Repartition) overrides it with `2`.
- Stage 4 (GroupBy) finally uses the default `200`.

---

## Optimization Recommendation

Your code currently forces the cluster to work on only **2 cores** in the middle of the job.

### Current Code

```python
partitionedDF = readAsDF.repartition(2)
````

### Recommended Change (Total Slots = 40)

```python
partitionedDF = readAsDF.repartition(40)
```
This allows Spark to fully utilize all executor cores.



# 100 small files to read Spark Small File Problem: Execution Analysis

When you have **100 small files (10KB each)**, you encounter the classic **"Small File Problem"** in Spark. Even though
the total data volume is tiny, the administrative overhead for Spark and the EMR cluster is significant.

Here is exactly what happens during the `read` stage:

---

## 1. Task Creation: The 1:1 Mapping

By default, Spark creates at least **one task per file** when reading formats like CSV, JSON, or Parquet if they aren't
combined.

- **Your Read Stage:** Spark will launch **100 tasks**.
- **The Irony:** You have 40 slots. Spark will run:
    - 40 tasks
    - another 40 tasks
    - then the final 20 tasks

---

## 2. Overhead vs. Computation

For a 10KB file, the actual computation takes milliseconds. However, the **orchestration overhead** takes much longer.

### Sources of Overhead

- **Listing files:** The Driver requests a list of all 100 files from S3.
- **Scheduling:** The Driver sends 100 serialized task packets to Executors.
- **Connection setup:** Each task opens a separate HTTP connection to S3 to read only 10KB.

### Result

Your Spark job spends **most of its time managing tasks** rather than actually reading data.

---

## 3. How Your Specific Code Reacts

Two parts of your code amplify the problem:

### 1. `inferSchema`

Because you have 100 files, Spark may open **all 100 files** just to determine the schema before starting the real job.

➡️ This effectively **doubles the overhead**.

### 2. `repartition(2)`

After reading 100 tiny files into 100 partitions:

- Spark performs a **Shuffle**
- Moves ~1MB of total data across the network
- Compresses everything into only **2 partitions**

This is massive overkill for such a small dataset.

---

## 4. How to Fix It (The EMR Way)

If you cannot change how data arrives, configure Spark to **combine small files during read**.

### Use `maxPartitionBytes` and `openCostInBytes`

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4MB
````

### What This Does

* Spark treats each file as if opening it costs **4MB**.
* Spark groups multiple small files into a single task.
* Tasks are filled up to **128MB** each.

### Result

Instead of **100 tasks for ~1MB of data**, Spark realizes all files can be processed in **one single task**.

---

## Summary Table: 100 Small Files

| Metric                      | Without Optimization | With File Combining        |
|-----------------------------|----------------------|----------------------------|
| **Total Tasks**             | 100                  | 1                          |
| **Waves on EMR (40 slots)** | 3 Waves              | 1 Wave (using only 1 core) |
| **S3 Connections**          | 100                  | 1 (Sequential or pooled)   |
| **Performance**             | Slow (High overhead) | Fast (Low overhead)        |

---

