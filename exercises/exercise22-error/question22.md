I am getting the following error:
Error occurred: An error occurred while calling o3048.parquet.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 107 in stage 33.0 failed 4 times, most recent
failure:
Lost task 107.3 in stage 33.0 (TID 2904) (ip-xx-xxx-xx-xxx.us-east-2.compute internal executor 29):
org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to s3:
//bucket/sss/sss/ss.
at org.apache.spark.sql.errors.QueryExecutionErrorsS.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:776)
at org.apache.spark.sql.execution.datasources.FileFormatWriterS.executeTask(FileFormatWriter.scala:421)
at org.apache.spark.sql.execution.datasources.WriteFilesExec.Sanonfun$doExecuteWrite$1 (WriteFiles scala: 100)
at org.apache.spark.rdd.RDD.Sanonfun$mapPartitionsInternal$2(RDD.scala:909)
at org.apache.spark.rdd.RDD.Sanonfun$mapPartitionsInternal$2Sadapted(RDD.scala:909)
at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:61)
at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:378)
at org.apache.spark.rdd.RDD.iterator(RDD.scala:333)
at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:174)
at org.apache.spark.scheduler.Task.run(Task.scala:152)
at org.apache.spark.executor.Executor$TaskRunner. Sanonfun$run$4(Executor.scala:632)
at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
at org.apache.spark.util.UtilsS.tryWithSafeFinally(Utils.scala:96)

------------------------------------------------------------------------
Error
s3://bucket4/score/
occurred: An error occurred while calling 05256 parquet.
: org.apache.spark. SparkException: Job aborted due to stage failure: Task 194 in stage 44.0 failed 4 times,
most recent failure: Lost task 194.3 in stage
44.0
(TID 3398) (ip-xx-xxx-xx-xxx.us-east-2.compute.internal executor 68): org.apache.spark.SparkException: [TASK WRITE_FAILED] Task failed while writing
rows to s3://bucket4/score/.
at
org.apache.spark.sql.errors.QueryExecutionErrors.taskFailedwhilewritingRowsError (QueryExecutionErrors.scala:776)
org.apache.spark.sql.execution.datasources.FileFormatwriterS.executeTask(FileFormatWriter.scala:421)
at org.apache.spark.sql.execution.datasources.WriteFilesExec.SanonfundoExecuteWriteS1(WriteFiles.scala:100)
Caused by: java.nio.file.AccessDeniedException:
score/results/20251008
012404/scored_13/_temporary/0/_temporary/attempt_202510082006496572569713110240433_0044_m_000194_3398/part-00194-5ee75ba
e-f318-4de4-a131-fe774f480392-c000. snappy •parquet: Writing object on score/results/20251008
012404/score13/_temporary/0/_temporary/attempt
202510082006496572569713110240433_0044 m_000194_3398/part-00194-575ba
e-f318-4de4-a131-fe774f480392-C000. snappy-parquet: software.amazon.awssdk.services.s3.model.3Exception: (Service: S3, status Code: 403, Request ID:
null): null
at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:276)||


-----------------------------------------------------------------------
Here we the modellers do a very complex set o operations and create a dataframe and we get error when writing the
dataframe as parquet to aws 3 bucket

I have written other simple dataframes as parquet to the same s3 bucket and no issues