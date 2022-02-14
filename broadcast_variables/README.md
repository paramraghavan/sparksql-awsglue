# Broadcast Variables
In PySpark Broadcast variables are read-only shared variables that are cached and available on all nodes in
a cluster to be used by the tasks. **Instead of sending this data along with every task, pySpark caches the broadcast
variable - this lookup info, on each machine. The tasks use this cached info while executing the transformations. Each
node/executor depending on number of cores could be running lots of tasks(spark recommends 2-3 tasks per CPU core).


ref: https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/