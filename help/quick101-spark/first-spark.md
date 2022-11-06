```
![image](https://user-images.githubusercontent.com/52529498/200199041-c5913dfc-3694-4778-a0fb-53011506a4a0.png)


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
