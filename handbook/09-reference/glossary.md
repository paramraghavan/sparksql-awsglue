# PySpark & Spark SQL Glossary

## Core Concepts

**Action**: An operation that triggers execution and returns results to the driver (e.g., `count()`, `collect()`, `write()`). Contrasts with transformations.

**Application Master (AM)**: In YARN, the process that negotiates resources from the Resource Manager and schedules tasks on containers. One per Spark application.

**Broadcast Variable**: A read-only variable sent to all executors efficiently. Used for lookups in UDFs.

**Catalyst Optimizer**: Spark SQL's query optimization engine that transforms logical plans into optimized physical plans using rules like predicate pushdown and column pruning.

**Checkpoint**: Writing a DataFrame to disk/HDFS and truncating its DAG lineage. Used to break long dependency chains in iterative algorithms.

**Coalesce**: Reduces number of partitions without full shuffle. Combines adjacent partitions locally.

**Column Pruning**: Catalyst optimization that reads only columns actually used in the query (not entire source).

**DAG (Directed Acyclic Graph)**: Spark's internal representation of a job's transformations and dependencies. Visualized in Spark UI.

**DataFrame**: Distributed collection of data organized into named columns with inferred/specified schema. The primary data structure in PySpark.

**Driver**: The process running the SparkSession that coordinates job execution, builds the DAG, and collects results. Runs your main script.

**Executor**: JVM process on worker nodes that runs tasks and caches data. Each executor has memory and cores.

**Exchange**: A wide transformation causing shuffle (redistribution of data across partitions).

**Executor Cores**: Number of threads per executor. Each task runs in one thread.

**Executor Memory**: JVM heap memory allocated to each executor (`spark.executor.memory`).

**Executor Memory Overhead**: Off-heap memory for Python processes, thread stacks, NIO buffers (`spark.executor.memoryOverhead`).

**Explode**: Transformation that converts arrays/maps into individual rows. One row becomes multiple.

**Lazy Evaluation**: Spark doesn't execute transformations immediately. Instead, builds a DAG and executes only when an action is called.

**Job**: A unit of work triggered by an action. Contains one or more stages.

**Narrow Transformation**: Transformation where each input partition contributes to at most one output partition. Examples: `filter()`, `map()`, `select()`. No shuffle needed.

**Offset**: In RDDs/DataFrames, the position of a record (0-based). Used in partitioning.

**Partition**: A chunk of data distributed across the cluster. Spark parallelizes by operating on partitions in parallel.

**Predicate Pushdown**: Catalyst optimization that pushes filters down to the data source to reduce data read from disk/S3.

**RDD (Resilient Distributed Dataset)**: Spark's lower-level abstraction. Immutable, lazily-evaluated collection of objects. DataFrames are built on RDDs.

**Repartition**: Reshuffles data to create a new number of partitions. Full shuffle operation.

**Resilience**: Ability to recover from node failures by recomputing lost partitions from original data and transformations.

**Shuffle**: Redistribution of data across network between executors. Expensive operation triggered by wide transformations.

**Shuffle Fetch**: Reading shuffled data from other executors during reduce phase.

**Shuffle Spill**: When partition data exceeds memory, Spark spills to disk. Causes slowdown.

**Shuffle Write**: Writing intermediate results to disk during map phase before sending to reducers.

**Stage**: A set of tasks that can run in parallel without requiring a shuffle. Narrow transformations are pipelined within a stage.

**Task**: Smallest unit of work. Represents one partition being processed by one executor. One task per partition per stage.

**Transformation**: Lazy operation that creates a new DataFrame from an existing one. Examples: `filter()`, `select()`, `join()`. Doesn't trigger execution.

**Tungsten**: Spark's memory management and code generation engine. Features: off-heap memory, cache-aware computation, whole-stage code generation.

**Unified Memory**: Spark's memory model combining storage (cache) and execution (shuffle) memory that can borrow from each other.

**Vectorized UDF**: Python UDF that operates on batches (Pandas Series) instead of single rows. Much faster than row-at-a-time UDFs.

**Wide Transformation**: Transformation where input partitions can contribute to multiple output partitions. Requires shuffle. Examples: `groupBy()`, `join()`, `repartition()`.

---

## AWS & EMR Specific

**Application**: A Spark job/application running on a cluster. Has one driver and multiple executors.

**Availability Zone**: Physical data center region within AWS. Resources in same AZ have lower latency.

**Bootstrap Action**: Script that runs on each node when EMR cluster starts. Used to install software/configure.

**Container**: In YARN, a unit of computation resources (memory + cores). Executors run in containers.

**Core Node**: EMR node that runs tasks and stores HDFS data. Has TaskTracker and YARN NodeManager. Can't be scaled down while cluster running.

**EMR (Elastic MapReduce)**: AWS service for running distributed computing frameworks like Spark. Manages cluster lifecycle.

**EC2 Instance**: Virtual machine in AWS. EMR nodes are EC2 instances.

**Glue Catalog**: AWS metadata repository for tables and schemas. Allows Spark to discover and read S3 data.

**HDFS (Hadoop Distributed File System)**: Distributed file system used by Hadoop/Spark. On EMR, data stored redundantly across nodes.

**IAM Role**: AWS Identity & Access Management role. Controls permissions for EC2/EMR to access other AWS services (S3, Glue, etc.).

**Instance Fleet**: EMR scaling option to mix on-demand and spot instances with instance type diversity.

**Instance Group**: EMR scaling option for single instance type per group.

**Job Flow**: EMR cluster and its executing jobs (legacy term; now called "Cluster").

**Master Node**: EMR node that runs YARN ResourceManager and HDFS NameNode. Coordinates job execution and cluster resources.

**Node Label**: YARN feature to assign labels to nodes (e.g., "CORE") and constrain application placement. Used to ensure ApplicationMaster runs on core nodes.

**Region**: Geographic area in AWS (us-east-1, eu-west-1, etc.). Impacts latency and compliance.

**ResourceManager (RM)**: YARN daemon on master node that allocates cluster resources to applications.

**S3 (Simple Storage Service)**: AWS object storage. Spark reads/writes Parquet, CSV, JSON files from S3.

**S3DistCp**: Hadoop tool for copying data between S3 buckets efficiently (parallel, with retries).

**Spot Instance**: AWS instance with up to 90% discount but can be interrupted. Good for fault-tolerant jobs, risky for interactive/low-latency.

**Task Node**: EMR node that runs only tasks, doesn't store HDFS data. Can be scaled up/down while cluster running.

**Yarn (Yet Another Resource Negotiator)**: Hadoop resource management framework used by EMR. Allocates CPU/memory to Spark applications.

---

## Join & Shuffle

**Anti Join**: Returns rows from left table that have NO matching rows in right table (opposite of inner join).

**Broadcast Join**: Both small table copied to all executors and joined in memory. Much faster than shuffle join for small tables.

**Cartesian Join**: Produces all combinations of rows from both tables (Spark warns about this). Usually accidental and expensive.

**Co-partitioned Join**: Both tables partitioned on join key. Spark avoids shuffle if both are pre-partitioned the same way.

**Cross Join**: Same as Cartesian join - all combinations.

**Equi Join**: Join on equality condition (most common). Spark optimizes these.

**Hash Join**: Builds hash table of smaller table in memory and probes with larger table. Used in broadcast joins.

**Inner Join**: Returns rows that match in both tables.

**Left Anti Join**: Same as anti join - left rows with no right match.

**Left Join (Left Outer)**: All rows from left table plus matches from right (nulls for unmatched rights).

**Left Semi Join**: Returns rows from left table that have at least one match in right table (but no right columns). Like SQL IN clause.

**Merge Join (Sort-Merge Join)**: Both tables sorted by join key, then merged. Good for large tables.

**Outer Join (Full Outer)**: All rows from both tables, nulls for unmatched.

**Right Join (Right Outer)**: All rows from right table plus matches from left (nulls for unmatched lefts).

**Shuffle Hash Join**: Repartitions both tables on join key, then performs hash join. Default for large tables.

---

## Aggregation & Analytics

**Cube**: Aggregation over all combinations of dimensions. Generates all possible sub-aggregates.

**Distinct**: Removes duplicate rows (opposite of duplicates).

**Explode**: Transforms each array element into a separate row.

**Grouping Set**: Aggregation over specified combinations of dimensions (subset of cube).

**Rollup**: Aggregation creating hierarchy of sub-aggregates. Each column builds up (col1; col1+col2; col1+col2+col3).

**Sample**: Randomly selects subset of data (useful for testing on large datasets).

**Skew**: Data distribution imbalance where some partitions have much more data than others.

**Stratified Sampling**: Sampling that maintains distribution within groups (ensures each group represented proportionally).

---

## Performance & Monitoring

**Adaptive Query Execution (AQE)**: Runtime optimization adjusting execution plan based on statistics collected during shuffle.

**Bottleneck**: Stage or task that limits overall performance (usually shuffle, GC, or network).

**Critical Path**: Longest chain of sequential operations in DAG. Determines minimum job runtime.

**Executor ID**: Unique identifier for each executor (e.g., "executor-1").

**Event Log**: Spark stores execution events (task start, task end, shuffle stats) for UI visualization and analysis.

**GC Overhead**: Time spent in garbage collection. >10% is problematic.

**Latency**: Time between request and response. Important for interactive queries.

**Locality**: Data locality levels (PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY).

**Memory Pressure**: Insufficient memory causing spill to disk or OOM.

**Scheduler Delay**: Time a task waits before being assigned to executor. Indicates resource contention.

**Shuffle Read**: Data pulled from other executors' shuffle write files.

**Shuffle Spill**: Data overflowing to disk due to memory constraints.

**Scheduling**: YARN process of assigning containers to jobs and tasks to containers.

**Skew Mitigation**: Techniques to handle data skew (salting, range partitioning, AQE).

**Stragglers**: Tasks running much longer than others (often due to skew or slow nodes).

**Throughput**: Volume of data processed per unit time.

---

## SQL & Schema

**Avro**: Row-based serialization format. Schema stored with data.

**Catalog**: Spark's metadata store for tables, databases, functions, and schemas.

**Column Pruning**: Reading only needed columns, not entire source.

**Columnar Format**: File format storing data column-by-column (vs row-by-row). Better for analytics. Examples: Parquet, ORC.

**Database**: Collection of tables in Spark SQL (default is "default").

**DDL (Data Definition Language)**: SQL for creating/modifying schemas (CREATE, ALTER, DROP).

**DML (Data Manipulation Language)**: SQL for reading/modifying data (SELECT, INSERT, UPDATE, DELETE).

**External Table**: Table whose data is stored outside warehouse directory (in S3, HDFS, etc.). Metadata in Glue/Hive.

**Hive MetaStore**: Metadata repository for tables (legacy; being replaced by Glue Catalog on AWS).

**JSON Lines**: Format with one JSON object per line (newline-delimited JSON).

**Managed Table**: Table whose data is stored in warehouse directory. Spark manages location.

**Metadata**: Information about data (schema, column names, types, statistics, partitions).

**ORC**: Columnar format with built-in compression and statistics.

**Parquet**: Columnar format optimized for compression and analytical queries. Spark default.

**Partition Column**: Column used for logical data organization. Spark creates directory per value (e.g., year=2024/month=03/).

**Schema**: Definition of table structure (column names, types, nullability).

**Schema Evolution**: Adding/modifying columns over time. Parquet handles gracefully.

**Schema on Read**: No schema enforcement at ingestion. Schema applied when reading. Flexible but slower initially. (Used by Glue)

**Schema on Write**: Schema enforcement at ingestion. Strict but fast. (Traditional RDBMS approach)

**Statistics**: Information about data (row count, min/max, null count). Used by Catalyst for optimization.

**Temp View**: Temporary table view of DataFrame. Exists only for session (session-scoped) or query (local-scoped).

**Table**: Permanent named collection of data (metadata in catalog, data on disk).

---

## Optimization Techniques

**Bucketing**: Pre-partitioning data into fixed number of buckets by hash. Useful for frequently joined columns.

**Caching**: Storing DataFrame in memory for reuse. Transparent to user code.

**Compression**: Reducing file size using algorithms (Snappy, Gzip, Bzip2, LZO).

**Column Statistics**: Min, max, count distinct per column. Used by Catalyst.

**Denormalization**: Combining data from multiple tables to avoid joins (trades space for speed).

**Bucketing**: Organizing into buckets by hash of column.

**Indexing**: Not directly supported in Spark (unlike databases). Partitioning/bucketing serve similar purpose.

**Materialization**: Computing and storing results (opposite of lazy evaluation).

**Normalization**: Organizing data to reduce redundancy (opposite of denormalization).

**Partition Pruning**: Skipping entire partition directories based on filters.

**Persistence**: Explicitly storing DataFrame in memory/disk for reuse (like caching but with control).

**Vectorization**: Processing batches of data efficiently (e.g., Pandas UDF).

---

## Error & Troubleshooting

**Executor Crash**: Executor process dies (memory, segfault, etc.). Tasks re-run on different executor.

**GC Overhead Limit**: JVM spent >95% time in garbage collection. Indicates memory pressure.

**Heap Space**: All JVM memory used. Indicates OOM.

**Memory Leak**: Process using more memory over time. Task data not released.

**Off-Heap Memory**: Memory outside JVM heap used by native code and buffers.

**Serialization Error**: Object can't be serialized (sent to executor). Check for non-serializable members.

**Skewed Partition**: Partition with disproportionately more data than others.

**Speculative Execution**: Spark launching backup copy of slow task. Winner finishes first.

**Straggler**: Task running much longer than others (often due to skew).

**Task Failure**: Single task fails (retried up to 4 times). Job continues if enough tasks succeed.

**Task Lost**: Task's executor lost. Data recomputed from previous stage (if not checkpointed).

**Timeout**: Operation exceeds time limit. Check network, resource constraints.

---

## Development

**REPL (Read-Eval-Print Loop)**: Interactive shell (pyspark, spark-shell). Good for exploration.

**Notebook**: Interactive document (Jupyter, Databricks). Code + visualization + markdown.

**Spark Submit**: CLI for submitting Spark jobs to cluster.

**Standalone Cluster**: Spark's built-in cluster manager (not recommended for production).

**Unit Test**: Testing individual components (UDFs, functions) before cluster run.

**Sandbox**: Local development environment mirroring cluster setup.

---

**Last Updated**: March 2026
**Apache Spark Version**: 3.5+
