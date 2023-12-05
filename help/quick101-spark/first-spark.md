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


## spark submit

- jars: Dependency .jar files.
  - Example: --jars/devl/src/main/python/lib/ojdbc7.jar,fil2.jar,file3.jar
- packages: Pass the dependency packages like maven
  - Example: --packages org.apache.spark:spark-avro2.11:2.4.4
  - Above refers to the java maven repository, [see here for more](https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.11/2.4.4)
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


### Accumulator Variables
Accumulator is a shared variable to perform sum and counter operations.
These variables are shared by all executors to update and add information through associative or commutative
operations.

- Commutative -> f(x, y) = fl(y, x)
  - Example: sum(5,7 ) = sum(7,5) --> good
  
- Associative -> flf(x, y), 2) = flf(x, 2), y) = flf(y, 2), x)
  - Example : sum(multiply(5,6),7) = sum(multiply(6,7),5) --> not associative;
    sum(sum(5,6),7) = sum(sum(6,7),5) --> good
  - Example:
```python

counter =0

def f1(x):
    global counter
    counter += 1
    
f1 (10)
f1 (10)
# counter  value is 2
print(f'counter: {counter}')

rdd = spark.sparkContext.parallelize([1,2,3])
rdd.foreach (f1)
# don't expect counter to be 5, it is only 3 as 
# if we use counter as accumulator  variable, this counter value will be sent to driver and will add up to 5
print(f'counter: {counter}')

# Using accumulator variable
counter1 = spark.sparkContext.accumulator (0)

def f2 (x):
    global counter1
    counter1.add(1)
    
rdd.foreach(f2)
# the acculiualtor varaible shold be same as the number of row in rdd, which is 3
# accumulator value is sent to driver and addtion happens in the driver
print(f'counter1.value: {counter1.value}')
# counter1.value: 3

```

- Spark natively supports accumulators of numeric types (int, float) and programmers can add support for **new custom types using
AccumulatorParam class of PySpark**.
- Accumulators do not change the lazy evaluation model of Spark. If they are being updated within an operation on an RDD, their value
is only updated once that RDD is computed as part of an action.
- Computations inside transformations are evaluated lazily, so unless an action happens on an RDD the transformations are not
executed. As a result of this, accumulators used inside functions like map() or filter() wont get executed unless some action applied
on the RDD. Spark guarantees to update accumulators inside actions only once. **Always use accumulators inside actions ONLY (ex - foreach)**.

### spark, closures, and how closure can be used with dataframe 
* **What is a Closure in Spark?**: A closure is the entire environment needed to execute a function on a worker node. This includes the function itself and any variables it uses from outside its own scope.
* **Why are Closures Important?**: They allow you to apply complex operations on distributed data. Spark automatically detects and sends the necessary parts of your code and data to the workers.
* **Challenges with Closures**: You need to ensure that all external variables used in your function are serializable. Also, any changes made to these variables within the function will not be reflected back in the driver program, except for special types like accumulators.
* Example below where you have a DataFrame and you want to filter out records based on some external criteria:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("closure_example").getOrCreate()

# Sample DataFrame
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# External variable used in the closure
age_threshold = 30

# Define a function to use as a closure
def is_above_threshold(age):
    return age > age_threshold

# Apply the closure in a DataFrame operation
filtered_df = df.filter((col("Age") > age_threshold))

# Show the result
filtered_df.show()
```
* In the  example above:
  * age_threshold is an external variable used inside the closure.
  * The is_above_threshold function acts as a closure, which is applied to the DataFrame to filter out records.
  * df.filter is the DataFrame operation where the closure is used. The condition inside filter uses age_threshold, which is serialized and sent to each worker node.
  * Example demonstrates how Spark manages the complexity of distributing both the data (the DataFrame) and the computations (the closure) across the cluster. The closure (is_above_threshold function and 
   age_threshold variable) is automatically serialized and distributed to the worker nodes by Spark.



