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

## Shared variables
Shared variables are the variables that are required to be used by functions and methods in parallel.
Shared variables can be used in parallel operations.
- Spark provides two types of shared variables
  - Broadcast
  - Accumulator

### Broadcast Varaibles
- Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than
shipping a copy of it with tasks.
- Immutable and cached on each worker nodes only once.
- Efficient manner to give a copy of a dataset to each node, provided the dataset is not too big to fit in memory.
### When to use Broadcast Variable:
- For processing, the executors need information regarding variables or methods. This information is serialized by Spark and
sent to each executor and is known as CLOSURE.
- If we have a huge array that is accessed from spark CLOSURES, for example - if we have 5 nodes cluster with 100 partitions
(20 partitions per node), this Array will be distributed at least 100 times (20 times to each node). If you we broadcast
it will be distributed once per node using efficient p2p protocol.

### What not to do:
Once we broadcasted the value to the nodes, we shouldn't make changes to its value to make sure each node have
exact same copy of data. The modified value might be sent to another node later that would give unexpected results.

Example:
![image](https://user-images.githubusercontent.com/52529498/200213323-60dbd85e-4eba-414a-a306-6d112c1db369.png)



