# Error when reading alrge parquet file from s3

```text
File "/mnt1/yarn/usercache/hadoop/appcache/application_1763051804940_0006/container_1763
count1 = df1.count()
^^^^^^^^^^^^^^^^^^^^

File "/mnt1/yarn/usercache/hadoop/appcache/application_1763051804940_0006/container_1763
return int(self._jdf.count())
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

File "/mnt1/yarn/usercache/hadoop/appcache/application_1763051804940_0006/container_1763
return_value = get_return_value(
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

File "/mnt1/yarn/usercache/hadoop/appcache/application_1763051804940_0006/container_1763
return f(*a, **kw)
^^^^^^^^^^^^^^^^^^

File "/mnt1/yarn/usercache/hadoop/appcache/application_1763051804940_0006/container_1763
raise Py4JJavaError(
^^^^^^^^^^^^^^^^^^^^
py4j.protocol.Py4JJavaError: An error occurred while calling o553.count.
: org.apache.spark.SparkException: Job aborted due to stage failure: Exception while gettin
: org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler
    at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala
```