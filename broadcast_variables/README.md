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



Ref: 
-------------------
- https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/
- https://stackoverflow.com/questions/43984068/does-spark-sql-autobroadcastjointhreshold-work-for-joins-using-datasets-join-op
- [pySpark dataframe join](https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples)
- https://stackoverflow.com/questions/48771517/best-way-to-join-multiples-small-tables-with-a-big-table-in-spark-sql
