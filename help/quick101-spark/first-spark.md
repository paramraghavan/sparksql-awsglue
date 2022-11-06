```
![image](https://user-images.githubusercontent.com/52529498/200198987-1682cefb-a42f-4148-af69-6c7c2f775b6a.png)

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
