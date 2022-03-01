# Broadcast Variables
In PySpark Broadcast variables are read-only shared variables that are cached and available on all nodes in
a cluster to be used by the tasks. **Instead of sending this data along with every task, pySpark caches the broadcast
variable - this lookup info, on each node/machine**. The tasks use this cached info while executing the transformations. Each
node/executor depending on number of cores could be running lots of tasks(spark recommends 2-3 tasks per CPU core).


When you run a PySpark RDD, DataFrame applications that have the Broadcast variables defined and used,
PySpark does the following:

- PySpark breaks the job into stages.
- Later Stages are also broken into tasks
- Spark broadcasts the common data (reusable) needed by tasks within each stage, one instance broadcast 
variable/table is available on the executor node.

# Scenario
I have a big table which is 10 gb's and smaller table about 11/12 mb's. Do i need to set my spark.sql.autoBroadcastJoinThreshold = 12 mb
in order for sending the whole table / Dataset to all worker nodes?. Note default value for spark.sql.autoBroadcastJoinThreshold is **10 mb**.

First of all spark.sql.autoBroadcastJoinThreshold and broadcast hint are separate mechanisms. Even if autoBroadcastJoinThreshold is 
disabled setting broadcast hint will take precedence. With default settings:

print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")) --> 10485760

Setting the broadcast hint on the smaller table, now the smaller table is broadcasted to the executor nodes. The job was taking an hour or so,
once we used the broadcast function, the total time reduced to 20 minutes. Driver node 60 gb, 5 executor nodes 60 gb  and it can scale upto
10 executor nodes.

# Note
Furthermore broadcasting large objects is unlikely provide any performance boost, and in practice will often degrade
performance and result in stability issue. Remember that broadcasted object has to be first fetch to driver, then 
send to each executor/worker, and finally loaded into memory.

Broadcast Join conditions are the following:
· Table needs to be broadcast less than  spark.sql.autoBroadcastJoinThreshold the configured value, default 10M (or add a broadcast join the hint)
· Base table can not be broadcast, such as the left outer join, only broadcast the right table


# Sort Merge Join -SMJ
When the two tables are very large, Spark SQL uses a new algorithm to join the table, that is, Sort Merge Join. This method does not have to
 load all the data and then into the start hash join, but need to sort the data before the join, as shown below:

You can see that the first two tables in accordance with the join keys were re-shuffle, to ensure that the same value of the join keys will be divided
in the corresponding partition. After partitioning the data in each partition, sorting and then the corresponding partition within the record to
 connect, as shown below:

![image](https://user-images.githubusercontent.com/52529498/155909404-7bfcbbc7-7ce2-478d-825d-b749dad3bc46.png)

https://www.waitingforcode.com/apache-spark-sql/sort-merge-join-spark-sql/read


# Shuffle Join
Shuffle Hash Join, as the name indicates works by shuffling both datasets. So the same keys from both sides end up in the same partition 
or task. Once the data is shuffled, the smallest of the two will be hashed into buckets and a hash join is performed within the partition.

Shuffle Hash Join is different from Broadcast Hash Join because the entire dataset is not broadcasted instead both datasets are shuffled 
and then the smallest side data is hashed and bucketed and hash joined with the bigger side in all the partitions.

Ref: 
-------------------
- **[Spark Join Stratergies](https://towardsdatascience.com/strategies-of-spark-join-c0e7b4572bcf)** 
- https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/
- https://stackoverflow.com/questions/43984068/does-spark-sql-autobroadcastjointhreshold-work-for-joins-using-datasets-join-op
- [pySpark dataframe join](https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples)
- https://stackoverflow.com/questions/48771517/best-way-to-join-multiples-small-tables-with-a-big-table-in-spark-sql
