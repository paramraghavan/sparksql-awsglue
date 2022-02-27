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




Ref: 
-------------------
- https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/
- https://stackoverflow.com/questions/43984068/does-spark-sql-autobroadcastjointhreshold-work-for-joins-using-datasets-join-op
