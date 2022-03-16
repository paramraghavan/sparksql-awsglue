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

![image](https://user-images.githubusercontent.com/52529498/152910275-db1dcde1-d292-4ec1-981a-614e0ff0777a.png)

if we consider a RDD or a DataFrame of 10 millions rows. It can be divided into 60 partitions across 4 executors (15 partitions per executor).
With 16 CPU core per executor, each task will process one partition.

# How data is distributed across partitions ?
The way data will be distributed across partitions depends on a object called Partitioner. In Apache Spark, there are two 
main Partitioners :

- HashPartitioner is the default partitioner used by Spark. HashPartitioner will distribute evenly data across all the partitions. 
If you don’t provide a specific partition key (a column in case of a dataframe), data will be associated with a key. That will
produce a (K,V) pair and the destination partition will be attributed by the following algorithm:
<pre>
partitionId = hash(Key) % NumberOfPartition
</pre>
Even with hash partitioning, data is not necessarily evenly distributed (specially when the number of partitions is not very high)

- RangePartitioner will distribute data across partitions based on a specific range. The RangePartitioner will use a column 
(for a dataframe) that will be used as partition key. This key will will be sampled (for performance issues) and based on the 
 number of values and the target number of partitions, data will be distributed based on this key.
  
**Note** Once the dataframe is read in, it  does not have information on the partition.  If the dataframe read in are already in sorted
order for the processing that needs to be performed - in this case the shuffle is reduced to minimum. 

# Dataframe repartition
 If you call Dataframe.repartition() without specifying a number of partitions, or during a shuffle, you have to know that Spark will
 produce a new dataframe with X partitions (X equals the value of “spark.sql.shuffle.partitions” parameter which is 200 by default).
 This can result in a dataframe with lots of empty partitions, specially if you are dealing with small data (or not big enough!),
 and scheduling issues.

 repartition() shuffles the data between the executors and divides the data into number of partitions. But this might be an expensive
 operation since it shuffles the data between executors and involves network traffic. Ideal place to partition is at the data source,
 while fetching the data. Things can speed up greatly when data is partitioned the right way but can dramatically slow down when
 done wrong, especially due the Shuffle operation.

Reference
---------
- https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough
- https://medium.com/datalex/on-spark-performance-and-partitioning-strategies-72992bbbf150
- [Does spark dataframe know the partition key of the dataframe](https://stackoverflow.com/questions/48459208/does-spark-know-the-partitioning-key-of-a-dataframe#:~:text=You%20don't.,can%20check%20queryExecution%20for%20Partitioner%20.)

Coalesce vs repartition
-----------------------------
<pre>
Difference between coalesce and repartition

coalesce uses existing partitions to minimize the amount of data that's shuffled.
repartition creates new partitions and does a full shuffle. coalesce results in
partitions with different amounts of data (sometimes partitions that have much different sizes)
and repartition results in roughly equal sized partitions.

Is coalesce or repartition faster?

coalesce may run faster than repartition, but unequal sized partitions are
generally slower to work with than equal sized partitions. You'll usually need to repartition datasets
after filtering a large data set. I've found repartition to be faster overall because Spark is
built to work with equal sized partitions.
ref: https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce
</pre>

- https://stackoverflow.com/questions/46386505/pyspark-difference-between-pyspark-sql-functions-col-and-pyspark-sql-functions-l
