![image](https://user-images.githubusercontent.com/52529498/200199041-c5913dfc-3694-4778-a0fb-53011506a4a0.png)
```

### Create a Spark Object
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .master('yarn') \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

print("Spark Object id created ...")
spark.stop()
```
- [spark performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [spark with yarn](https://spark.apache.org/docs/latest/running-on-yarn.html#confiquration)


## spark subnit

- jars: Dependency .jar files.
  - Example: --jars/devl/src/main/python/lib/ojdbc7.jar,fil2.jar,file3.jar
- packages: Pass the dependency packages.
  - Example: --packages org.apache.spark:spark-avro2.11:2.4.4
- py-files: Use -py-files to add .py and .zip files. File specified with -py-files are uploaded to the cluster before it
run the application.
  - Example: --py-files file1.py, file2.py,file3.zip
- Example
```python
spark-submit
-master "yarn" \
-deploy-mode "cluster" # default is "client"
-conf spark.sql.shuffle.partitions = 300 \
-conf spark.yarn.appMasterEnv.HDFS_PATH="path1/subpath/event"
-driver-memory 1024M \
-executor-memory 1024M
--num-executors 2
-jars -jars/devl/src/main/python/lib/ojdbc7.jar, fil2.jar, file3.jar \
-packages org.apache.spark:spark-avro2.11:2.4.4|
--py-files file1.py, file2.py, file3.zip
/dev/example1/src/main/python/bin/basic.py arg1 arg2 arg3
```
