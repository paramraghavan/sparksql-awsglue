# enable spark logging
enable this so that you can view the submitted job in Spark History Server and Yarn Timeline Server

To enable Spark logging features in your SparkSession code snippet, you need to configure Spark to enable event logging and specify the event log directory (typically an S3 path when running on EMR). Modify your SparkSession builder as follows:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ParquetCompare") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "s3://your-s3-bucket/spark-logs") \
    .getOrCreate()
```

What you need to change/add:
- Add `.config("spark.eventLog.enabled", "true")` to turn on event logging.
- Add `.config("spark.eventLog.dir", "s3://your-s3-bucket/spark-logs")` to specify where event logs will be stored. Replace `"s3://your-s3-bucket/spark-logs"` with your actual S3 bucket path for logs.

This will enable Spark's event logging, capturing stages, tasks, and executor info, which can then be viewed with Spark History Server or used for troubleshooting stuck jobs.

Additionally, ensure:
- Your EMR cluster has yarn log aggregation enabled and configured to send logs to S3.
- Your Spark History Server is configured to read from the eventLog directory.