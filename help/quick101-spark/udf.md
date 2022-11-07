## udf

- UDFs are the User Defined Functions. Spark UDFs are similar to RDBMS User DefinedFunctions.
- If there is a need of a function and pyspark build-in features don't have this function, then
you can create a udf and use it is DataFrames and Spark SQLs.
- UDFs are error-prune and so should be designed carefully. First check if similar function is
available in pyspark functions library(pyspark.sq.functions). If not designed properly, we
would come across optimization and performance issues.
- We can use UDFs both in DataFrame and Spark SQL.
  - For Spark SQL, create a python function/udf and register it using spark.udf.register method.
  - For DataFrame, create a udf by wrapping under @udf or udf() function.

```python
df = spark.createDataFrame([['robert'], ['raina']], ["Name"])
# By default return type is StringType()
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col
@udf(returnType=StringType())
def initCap(input):
    finalStr=''
    ar = input.split(" ")
    for word in ar:
        finalStr= finalStr + word[0:1].upper() + word[1:len(word)] + " "
    return str.strip(finalStr)

# DataFrame:
df.select(df.Name, initCap(df.Name)).show()

# Spark Sql:
spark.udf.register("initcap", initCap,StringType())
spark.sql("""select Name, initcap(Name) from default.emp """)

+------+-------------+
|  Name|initCap(Name)|
+------+-------------+
|robert|       Robert|
| raina|        Raina|
+------+-------------+

```
