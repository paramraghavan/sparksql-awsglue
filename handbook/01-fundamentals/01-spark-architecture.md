# Spark Architecture & Cluster Components

## What is Apache Spark?

Apache Spark is a **distributed computing engine** for large-scale data processing. It abstracts the complexity of Hadoop and allows you to work with data as if it were on a single machine, while Spark handles the distributed execution across a cluster.

### Key Differences from MapReduce

| Feature | MapReduce | Spark |
|---------|-----------|-------|
| **Processing** | Disk-based between stages | In-memory (100x+ faster) |
| **Programming** | Map → Reduce only | Map, Reduce, Join, Window, ML, Graph, Streaming |
| **Iterative jobs** | Writes to disk each iteration | Keeps data in memory across iterations |
| **Speed** | Slower (I/O bound) | Much faster (memory bound) |
| **Ease of use** | Low-level programming | High-level APIs (SQL, DataFrame, Python) |

---

## Spark Cluster Architecture

```
┌────────────────────────────────────────────────────────┐
│                   Driver Program                        │
│              (Your main() script runs here)             │
│         Runs SparkSession / SparkContext               │
└──────────────────────┬─────────────────────────────────┘
                       │
              ┌────────┴────────┐
              │ Cluster Manager │
              │  (YARN/Mesos/   │
              │  Kubernetes)    │
              └────────┬────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
    ┌───▼────┐    ┌───▼────┐    ┌───▼────┐
    │Executor │    │Executor │    │Executor │
    │  Node1  │    │  Node2  │    │  Node3  │
    │         │    │         │    │         │
    │ ┌─────┐ │    │ ┌─────┐ │    │ ┌─────┐ │
    │ │Task1│ │    │ │Task3│ │    │ │Task5│ │
    │ │     │ │    │ │     │ │    │ │     │ │
    │ ├─────┤ │    │ ├─────┤ │    │ ├─────┤ │
    │ │Task2│ │    │ │Task4│ │    │ │Task6│ │
    │ │Cache│ │    │ │Cache│ │    │ │Cache│ │
    │ └─────┘ │    │ └─────┘ │    │ └─────┘ │
    └─────────┘    └─────────┘    └─────────┘
```

### Components Explained

#### Driver Program
- **Responsible for**: Your application code, DAG creation, task scheduling
- **Runs**: SparkContext/SparkSession
- **Memory**: `spark.driver.memory` (default 1GB)
- **Collects results**: All final results returned to driver
- **Sends tasks**: Pushes tasks to executors for parallel processing
- **In YARN**: Runs as "ApplicationMaster" on a cluster node

#### Executors
- **Responsible for**: Running tasks assigned by driver
- **Runs**: Task execution, data caching/persistence
- **Memory**: `spark.executor.memory` per executor (default 1GB)
- **Cores**: `spark.executor.cores` (default 1)
- **Quantity**: `--num-executors` parameter
- **Failure**: If executor dies, tasks are rescheduled on other executors

#### Cluster Manager
- **Role**: Allocates resources to Spark applications
- **Options**: YARN (EMR), Mesos, Kubernetes, Standalone
- **Negotiation**: Driver requests containers from cluster manager

---

## Data Distribution: RDDs & Partitions

### RDD (Resilient Distributed Dataset)
- **Immutable**: Once created, can't be modified
- **Distributed**: Data split across multiple partitions
- **Resilient**: Can recover from node failures by recomputing
- **Lazy**: Transformations recorded but not executed until action is called
- **In modern Spark**: DataFrames (built on RDDs) are preferred

### DataFrame
- **Structured RDD**: RDD with named columns and schema (like a SQL table)
- **Optimization**: Catalyst optimizer can optimize queries
- **Performance**: Much faster than RDD operations
- **Used by**: Modern PySpark code

### Partitions
- **Unit of parallelism**: Each partition processed by one task
- **Task assignment**: One task per partition per stage
- **Distribution**: Data is split and distributed across partitions
- **Example**: 1000 partitions = up to 1000 parallel tasks

```python
# Check partition count
df.rdd.getNumPartitions()  # Returns number of partitions

# Examine partition sizes
from pyspark.sql.functions import spark_partition_id
df.groupBy(spark_partition_id()).count().show()
```

---

## Execution Model: DAG, Stages, Tasks

### DAG (Directed Acyclic Graph)
- **Purpose**: Represents logical flow of transformations
- **Created by**: SparkContext/SparkSession when you define transformations
- **Lazy**: DAG built incrementally as you chain transformations
- **Optimization**: Catalyst optimizes the logical DAG before physical execution

```python
# Build a DAG
df = spark.read.csv("data.csv")          # Logical node 1
df_filtered = df.filter(df.age > 30)     # Logical node 2
df_agg = df_filtered.groupBy("city").agg(...)  # Logical node 3
df_agg.show()  # ACTION - Spark creates physical plan and executes DAG
```

### Stages
- **What is it**: Group of tasks that can run in parallel without shuffling
- **Boundary**: Created whenever a wide transformation (shuffle) is needed
- **Inside stage**: Narrow transformations are pipelined
- **Dependency**: Later stages depend on completion of earlier stages

```
Stage 1: Read → Filter → Select     [No shuffle]
         ↓
Stage 2: GroupBy shuffle            [Shuffle boundary]
         ↓
Stage 3: Aggregation                [No shuffle within stage]
```

### Tasks
- **Smallest unit**: One partition × one stage = one task
- **Execution**: Tasks run in parallel across executors
- **Example**: 1000 partitions in a stage = 1000 tasks in that stage

```python
# Number of tasks in a stage = number of partitions
df.rdd.getNumPartitions()  # If 200, then each stage has 200 tasks
```

---

## Transformations vs Actions

### Transformations (Lazy)
**Don't trigger execution. Build the DAG.**

```python
# Narrow transformations (no shuffle)
df.select("col1", "col2")
df.filter(df.age > 30)
df.withColumn("new_col", df.col * 2)
df.drop("col1")
df.map(lambda x: x * 2)

# Wide transformations (cause shuffle)
df.groupBy("city").count()
df.join(other_df, on="key")
df.orderBy("col")
df.distinct()
df.repartition(200)
```

### Actions (Eager)
**Trigger execution and return results.**

```python
df.show()              # Display first 20 rows
df.count()            # Return row count
df.collect()          # Return all rows to driver (dangerous for large data!)
df.take(10)           # Return first 10 rows
df.write.parquet(...) # Write to storage
df.foreach(...)       # Apply function to each row
df.reduce(...)        # Aggregate all rows
```

**Rule**: Every action triggers the entire DAG execution.

---

## Narrow vs Wide Transformations

### Narrow Transformations
```
Input partition 1 ──┐
Input partition 2 ──┼──→ Output partition 1
Input partition 3 ──┘

Each input partition contributes to ONE output partition
```

**Examples**:
- `filter()`, `map()`, `select()`, `withColumn()`, `drop()`, `union()`

**Characteristics**:
- No shuffle needed
- Can be pipelined within a single stage
- Fast and efficient
- One output partition per input partition

### Wide Transformations
```
Input partition 1 ┐
Input partition 2 ├──→ Output partition 1
Input partition 3 ┘
      AND
Input partition 1 ┐
Input partition 2 ├──→ Output partition 2
Input partition 3 ┘
```

**Examples**:
- `groupBy()`, `join()`, `orderBy()`, `repartition()`, `distinct()`, `pivot()`

**Characteristics**:
- **Requires shuffle** (expensive!)
- Creates stage boundary
- Data redistributed over network
- Can cause bottlenecks if data is skewed

---

## Lazy Evaluation

### Why Lazy?
1. **Optimization**: Spark can see entire DAG and optimize together (predicate pushdown, column pruning)
2. **Efficiency**: Avoids unnecessary intermediate results
3. **Pipelining**: Multiple transformations combined into single pass

### Example
```python
# Stage 1: None of this executes yet
df = spark.read.csv("data.csv")              # Logical node
df_filtered = df.filter(df.age > 30)         # Logical node
df_selected = df_filtered.select("name", "age", "salary")  # Logical node
df_ordered = df_selected.orderBy(df.salary)  # Logical node

# Stage 2: Action triggers execution
df_ordered.show()  # NOW Spark executes entire DAG

# Stage 3: Action triggers re-execution
df_filtered.count()  # Entire DAG re-executes from the start!
```

**Note**: Each action re-executes from the source unless you `.cache()`.

---

## Caching & Persistence

### When to Cache
```python
# Scenario: You reuse df_large in multiple transformations
df_large = spark.read.csv("large_file.csv")

df_large.cache()  # Flag it for caching

# First action - computed and cached
count1 = df_large.count()

# Second action - uses cache (much faster!)
count2 = df_large.filter(...).count()

# Always unpersist when done
df_large.unpersist()
```

### Cache Levels
```python
from pyspark import StorageLevel

df.cache()                                    # MEMORY_ONLY
df.persist(StorageLevel.MEMORY_ONLY)          # In memory only
df.persist(StorageLevel.MEMORY_AND_DISK)      # Memory first, spill to disk
df.persist(StorageLevel.DISK_ONLY)            # Disk only
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized in memory (slower access, more data)
```

---

## SparkSession vs SparkContext

### SparkSession (Modern, 2.0+)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Use for DataFrames and SQL
df = spark.read.csv(...)
spark.sql("SELECT * FROM table")
```

### SparkContext (Legacy, rarely used now)
```python
from pyspark import SparkContext

sc = SparkContext("local", "MyApp")

# Use for RDD operations
rdd = sc.parallelize([1, 2, 3, 4, 5])
```

**Best Practice**: Use SparkSession. It includes SparkContext (`spark.sparkContext`).

---

## Cluster Modes

### Local Mode
```bash
spark.master("local")      # Single thread
spark.master("local[2]")   # 2 threads
spark.master("local[*]")   # All cores
```
- **Use**: Development, testing, small data
- **Driver + Executors**: Run in same JVM

### Standalone Cluster Mode
```bash
spark-submit --master spark://master:7077 my_job.py
```
- **Use**: Simple clusters, learning
- **Issues**: Limited scheduling, not recommended for production

### YARN Cluster Mode (EMR)
```bash
spark-submit --master yarn --deploy-mode cluster my_job.py
```
- **Use**: Production on AWS EMR
- **Driver**: Runs on cluster (ApplicationMaster)
- **Scheduling**: YARN manages resources fairly

### YARN Client Mode
```bash
spark-submit --master yarn --deploy-mode client my_job.py
```
- **Use**: Interactive (Jupyter, spark-shell)
- **Driver**: Runs on local machine
- **Risk**: Local machine must stay alive

---

## Job Execution Flow

```
1. User submits application with spark-submit
   ↓
2. Spark creates SparkSession/SparkContext
   ↓
3. User builds transformations (DAG built, but not executed)
   ↓
4. User calls action (e.g., .show(), .count(), .write())
   ↓
5. Driver submits job to cluster manager
   ↓
6. Cluster manager allocates executors
   ↓
7. Driver creates stages from DAG
   ↓
8. For each stage:
   - Driver sends tasks to executors
   - Executors process partitions in parallel
   - Results written to disk (for next stage) or collected
   ↓
9. Driver collects final results
   ↓
10. Application returns results to user
```

---

## Interview Questions

### Q: Explain Spark architecture - driver, executors, tasks

**Answer**:
- **Driver**: The JVM process running your application code. Creates the DAG, schedules tasks, collects results. One per application.
- **Executors**: JVM processes on worker nodes that execute tasks. Have memory and cores. Multiple per application.
- **Tasks**: Smallest unit of work. One partition × one stage = one task. Tasks run in parallel across executors.
- **Data**: Split into partitions (e.g., 200 partitions for 1000-task parallelism). Each task processes one partition.

```
Driver ──→ creates DAG ──→ breaks into Stages ──→ sends Tasks ──→ Executors process Partitions
```

### Q: What happens when you call an action like count()?

**Answer**:
1. Spark examines the DAG built from all previous transformations
2. Catalyst optimizer optimizes the logical plan
3. Spark creates physical execution plan with stages
4. For each stage, driver sends tasks (one per partition) to executors
5. Executors process their partitions in parallel
6. Results sent back to driver
7. Driver returns count to user

Each task processes one partition. 1000 partitions = ~1000 parallel tasks.

### Q: Why is Spark faster than Hadoop MapReduce?

**Answer**:
1. **In-memory processing**: Spark keeps data in memory between stages. MapReduce writes to disk.
2. **Optimized DAG**: Catalyst optimizer removes unnecessary steps
3. **Pipelining**: Narrow transformations executed in single pass
4. **Less I/O**: Fewer disk reads/writes

Result: 10-100x faster for typical jobs.

### Q: Explain lazy evaluation and why it's important

**Answer**:
- Spark doesn't execute transformations immediately
- Builds a DAG and optimizes the whole thing
- Only executes when you call an action
- Benefits: Whole-plan optimization, eliminates unnecessary computations
- **Gotcha**: Each action re-executes from source (use cache() to avoid)

---

## Key Takeaways

1. **Spark abstracts distributed processing**: Think of data as one big collection; Spark handles distribution
2. **Partitions are key to parallelism**: More partitions = more parallel tasks (up to executor limits)
3. **Lazy evaluation enables optimization**: Spark sees entire DAG before execution
4. **Transformations vs Actions**: Transformations build DAG, actions execute it
5. **Wide transformations are expensive**: They cause shuffles; minimize them for performance
6. **Cache reused DataFrames**: Each action re-executes unless you cache

---

## See Also
- [Transformations & Actions](03-transformations-actions.md) - Deep dive into specific operations
- [Performance Optimization](../04-performance-optimization/) - Tune for speed
- [Troubleshooting](../07-troubleshooting/) - Debug production issues
