```text
1. Data Locality Issues (Why it happens)
Forced Remote Reads: When data is heavily skewed (one partition holds most of the data), that specific executor must process a vast amount of data. If the data required by that executor is not on its local disk (node-local), it must fetch it from other nodes, resulting in RACK_LOCAL or ANY data locality, which is much slower.
Shuffle Inefficiency: In skewed scenarios, particularly during wide transformations like groupBy or join, data is often redistributed poorly. An overloaded executor might not be located close to the intermediate shuffle files it needs, forcing high-latency network transfers.
Breaking Data Locality: Data locality is designed to move computation to where the data resides. When one executor is handling 10x more tasks, the scheduler often has to place tasks on that executor even if the data isn't there, simply because that's where the key data has been shuffled, or to avoid leaving other nodes totally idle, violating the principle of data locality. 
Apache Spark
Apache Spark
 +4
2. Performance Consequences
Straggler Tasks: The entire job stage must wait for the "straggler" tasks (the ones with 10x more data) to finish, causing the "long-tail" effect in the Spark UI.
Executor OOM/Spill to Disk: If the skewed partition cannot fit in the executor's memory, Spark will spill the data to disk. Disk I/O is significantly slower than memory, causing massive performance drops.
Idle Resources: While a few executors are running at 100% load, others sit idle, wasting cluster resources. 
OneUptime
OneUptime
 +4
3. How to Fix It
Enable Adaptive Query Execution (AQE): In Spark 3.0+, enabling spark.sql.adaptive.enabled can automatically detect and split skewed partitions at runtime.
Salting: To handle skewed keys in joins, add a random salt value to the key to break up the hot partition into smaller, more manageable pieces.
Broadcast Hash Join: If one side of a join is small enough, broadcasting it removes the need to shuffle the large skewed dataset entirely, eliminating the data locality issue for that operation.
Repartition/Coalesce: Use .repartition() on the DataFrame to break up skewed partitions before a costly transformation. 
Medium
Medium
 +2
```
ref: https://share.google/aimode/KElYOfdpwrZDlO72W