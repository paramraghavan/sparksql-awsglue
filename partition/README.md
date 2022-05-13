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


# Determining the Number of Dataset Partitions on Read

When spark reads from th data source, as a general rule of thumb the number of partitions created 
depends on total number of core available(n - executor X m - cores per executor). Now lets see in details
how this works

**Config parameters which affects the number of partitions in the Dataset are:**

1. spark.default.parallelism (default: Total No. of CPU cores)
2. spark.sql.files.maxPartitionBytes (default: 128 MB)
3. spark.sql.files.openCostInBytes (default: 4 MB), this incurred per partition
4. maxSplitBytes = Minimum(maxPartitionBytes, bytesPerCore)
5. bytesPerCore = (Sum of sizes of all data files + No. of files * openCostInBytes) / default.parallelism

**Calculating file chunk size**

Each of the data files (to be read) is split if it is splittable. 
- If a file is splittable, with a size more than ‘maxSplitBytes’,then the file is split in multiple chunks 
  of ‘maxSplitBytes’, the last chunk being less than or equal to ‘maxSplitBytes’.
- If the file is not splittable or the size is less than ‘maxSplitBytes’, there is only one file chunk 
  of size equal file size.

After file chunks are calculated for all the data files, one or more file chunks are packed in a partition. The packing 
process starts with initializing an empty partition followed by iteration over file chunks, for each iterated file chunk:
- If there is no current partition being packed, initialize a new partition to be packed and assign the iterated file
  chunk to it.The partition size becomes the sum of chunk size and the additional overhead of ‘openCostInBytes’. 
- If the addition of chunk size does not exceed the size of current partition (being packed) by more than ‘maxSplitBytes’,
  then the file chunk becomes the part of the current partition. The partition size is incremented by the sum of the 
  chunk size and the additional overhead of ‘openCostInBytes’.
- If the addition of chunk size exceeds the size of current partition being packed by more than ‘maxSplitBytes’, then
  the current partition is declared as complete and a new partition is initiated. The iterated file chunk becomes the
  part of the newer partition being initiated, and the newer partition size becomes the sum of chunk size and the 
  additional overhead of ‘openCostInBytes’.

Based on the above steps it comes out with the number of partitions for the dataset, corresponding to the data
files being read.


![image](https://user-images.githubusercontent.com/52529498/159253384-0702cd3a-fdb5-41e4-8fbe-fec24f4f183c.png)


**Examples:**

- 54 parquet files, 65 MB each, all 3 config parameters at default, No. of core equal to 10: 
  - The number of partitions for this comes out to be 54. Each file has only one chunk here. It is obvious here
  that two files cannot be packed in one partition (as the size would exceed ‘maxSplitBytes’, 128 MB after adding
  the second file) in this example.

- 54 parquet files, 63 MB each, all 3 config parameters at default, No. of core equal to 10: 
  - The number of partitions comes out to be again 54. It seems that two files can be packed here, but since,
  there is an overhead of ‘openCostInBytes’ (4 MB) after packing the first file, therefore, after adding
  the second file, the limit of 128 MB gets crossed, hence, two files cannot be packed in one partition
  in this example.  

- 54 parquet files, 40 MB each, all 3 config parameters at default, No. of core equal to 10: 
  - The number of partitions comes out to be 18 this time. According to the packing process explained above, even 
  after adding two files of 40 MB and overhead of 4 MB each, the total size comes out to be 88 MB, therefore the
  third file of 40 MB can also be packed since the size come out to be just 128 MB. Hence, the number of partitions
  comes out to be 18.


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
- https://sparkbyexamples.com/spark/spark-partitioning-understanding/

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


## Partition Discovery

Starting from Spark 1.6.0, partition discovery only finds partitions under the given paths by default. For the above
example, if users pass path/to/table/gender=male to either SparkSession.read.parquet or SparkSession.read.load, gender
will not be considered as a partitioning column. If users need to specify the base path that partition discovery should
start with, they can set basePath in the data source options. For example, when path/to/table/gender=male is the path of
the data and users set basePath to path/to/table/, gender will be a partitioning column.
```python
 spark.read \
            .option('basePath', 'path/to/table' ) \
            .load(path='path/to/table/gender=male', format='json')
```


ref: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html