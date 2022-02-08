Spark Partitioning
-----------------------
Data partitioning is critical to data processing performance especially for large volume of data processing in Spark.
Partitions in Spark won’t span across nodes though one node can contains more than one partitions. When processing,
Spark assigns one task for each partition and each worker threads can only process one task at a time. Thus, with
too few partitions, the application won’t utilize all the cores available in the cluster and it can cause data
skewing problem; with too many partitions, it will bring overhead for Spark to manage too many small tasks.


# Bad partitioning
A good partitioning strategy knows about data and its structure, and cluster configuration. Bad partitioning can lead to
bad performance, mostly in 3 fields :
- Too many partitions corresponding to your cluster size and you won’t use efficiently your cluster. For example,
 it will produce intense task scheduling.
- Not enough partitions corresponding to your cluster size, and you will have to deal with memory and CPU issues.
Memory because your executor nodes will have to put high volume of data in memory (possibly causing OOM Exception),
and CPU because compute across the cluster will be unequal: A subset of you CPU will do the work and the others ones will
look their neighbors work.
- Skewed data in your partitions. When a Spark task will be executed on these partitions, they will be distributed across
executor slots and CPUs. If your partitions are unbalanced in terms of data volume, some tasks will run longer compared to
others and will slow down the global execution time of the tasks (and a node will probably burn more CPU that others)


# What is Spark partitioning ?
Partitioning is nothing but dividing data structure into parts. In a distributed system like Apache Spark, it can be
defined as a division of a dataset stored as multiple parts across the cluster.

Spark uses 3 main data structures : RDDs (Resilient Distributed Datasets), Dataframes and Datasets. Each of this structures
are in memory structures and can be split into chunks of data, each chunk sit on an physical node (executor)

Spark recommends 2-3 tasks per CPU core in your cluster. For example, if you have 1000 CPU core in your cluster, the
recommended partition number is 2000 to 3000. Sometimes, depends on the distribution and skewness of your source data,
you need to tune around to find out the appropriate partitioning strategy.





Reference
---------
- https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough
- https://medium.com/datalex/on-spark-performance-and-partitioning-strategies-72992bbbf150
