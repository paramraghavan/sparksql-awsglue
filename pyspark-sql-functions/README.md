# pyspark sql functions with examples

- F.isnull() Use this function to check if a column contains null values.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull

# Initialize Spark session
spark = SparkSession.builder.appName("NullCheck").getOrCreate()

# Sample DataFrame
data = [("James", None), ("Anna", 30), ("Robert", 50)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Check for null values in the 'Age' column
df_with_null_check = df.withColumn("IsNull", isnull(col("Age")))

df_with_null_check.show()

# Close the Spark session
spark.stop()

# Result:
# +------+----+------+
# |  Name| Age|IsNull|
# +------+----+------+
# | James|NULL|  true|
# |  Anna|  30| false|
# |Robert|  50| false|
# +------+----+------+
```

- F.coalesce() Use coalesce to return the first non-null value from a list of columns.
```python
from pyspark.sql.functions import coalesce, lit

# Assuming 'df' is your DataFrame
df_with_coalesce = df.withColumn("AgeNonNull", coalesce(col("Age"), lit(0)))
df_with_coalesce.show()

# Result:
# +------+----+----------+
# |  Name| Age|AgeNonNull|
# +------+----+----------+
# | James|NULL|         0|
# |  Anna|  30|        30|
# |Robert|  50|        50|
# +------+----+----------+

```
- F.when().otherwise() This is useful for conditional replacement of null values.
```python
from pyspark.sql.functions import when

# Replace null with a specific value
df_with_replacement = df.withColumn("Age", when(col("Age").isNull(), 0).otherwise(col("Age")))
df_with_replacement.show()

# Result:
# +------+---+
# |  Name|Age|
# +------+---+
# | James|  0|
# |  Anna| 30|
# |Robert| 50|
# +------+---+
```

- DataFrame.dropna() Use this method to drop rows that contain null values.
```python
# Drop rows where any of the column is null
df_dropped = df.dropna()
df_dropped.show()

# Result:
# +------+---+
# |  Name|Age|
# +------+---+
# |  Anna| 30|
# |Robert| 50|
# +------+---+

```
- DataFrame.fillna(), Use this method to fill null values with a default value.
```python
# Fill null values in 'Age' with a default value, say 0
df_filled = df.fillna({'Age': 0})
df_filled.show()

# Result:
# +------+---+
# |  Name|Age|
# +------+---+
# | James|  0|
# |  Anna| 30|
# |Robert| 50|
# +------+---+

```

- F.when is used for expressing conditional logic similar to SQL's CASE WHEN. It is often combined with .otherwise() for handling else cases. In this example, the when function is used to categorize each person as either an 'Adult' or a 'Minor' based on their age.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("ConditionalLogic").getOrCreate()

# Sample DataFrame
data = [("James", 34), ("Anna", 20), ("Robert", 50)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Categorize age into 'Adult' or 'Minor'
df = df.withColumn("Category", when(col("Age") >= 18, "Adult").otherwise("Minor"))

df.show()

# Close the Spark session
spark.stop()
```

- F.col is used for referencing a DataFrame column either for filtering, selecting, or applying functions. In this example, F.col is used to reference the 'Salary' column. We're applying a 10% increase to the salaries less than 4000.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("ColumnReference").getOrCreate()

# Sample DataFrame
data = [("James", 3000), ("Anna", 4500), ("Robert", 4000)]
columns = ["Name", "Salary"]
df = spark.createDataFrame(data, columns)

# Increase salary by 10% for those earning less than 4000
df = df.withColumn("New Salary", when(col("Salary") < 4000, col("Salary") * 1.10).otherwise(col("Salary")))

df.show()

# Close the Spark session
spark.stop()

```

- Combining F.when with multiple conditions. In this code snippet, multiple when conditions are chained together to apply different salary increases based on the age of the individuals.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("MultipleConditions").getOrCreate()

# Sample DataFrame
data = [("James", 34, 3000), ("Anna", 20, 4500), ("Robert", 50, 4000)]
columns = ["Name", "Age", "Salary"]
df = spark.createDataFrame(data, columns)

# Apply different salary increases based on age
df = df.withColumn("New Salary",
                   when(col("Age") < 30, col("Salary") * 1.10)
                   .when(col("Age") < 50, col("Salary") * 1.05)
                   .otherwise(col("Salary")))

df.show()

# Close the Spark session
spark.stop()

```

- `F.expr` allows you to execute SQL-like expressions, which can be particularly useful for complex aggregations. Example: Calculating Weighted Average
Suppose you have a DataFrame of products with their prices and quantities, and you want to calculate the weighted average price.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder.appName("WeightedAverage").getOrCreate()

# Sample DataFrame
data = [("Apple", 2, 30), ("Banana", 1, 50), ("Orange", 3, 20)]
columns = ["Product", "Quantity", "Price"]
df = spark.createDataFrame(data, columns)

# Calculate weighted average
weighted_avg = df.withColumn("Total", expr("Quantity * Price")) \
                 .groupBy() \
                 .agg(expr("sum(Total) / sum(Quantity)").alias("WeightedAveragePrice"))

weighted_avg.show()

# Close the Spark session
spark.stop()

# Result:
# +--------------------+
# |WeightedAveragePrice|
# +--------------------+
# |  28.333333333333332|
# +--------------------+

```
- PySpark SQL window functions, used with F.over(), allow for sophisticated data analysis tasks such as running totals, moving averages, and ranking.
```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum as Fsum

# Initialize Spark session
spark = SparkSession.builder.appName("RunningTotal").getOrCreate()

# Sample DataFrame
data = [("A", 10), ("B", 15), ("C", 20), ("D", 25)]
columns = ["Item", "Value"]
df = spark.createDataFrame(data, columns)

# Define a window specification
windowSpec = Window.orderBy("Item").rowsBetween(Window.unboundedPreceding, 0)

# Calculate running total
df = df.withColumn("RunningTotal", Fsum("Value").over(windowSpec))

df.show()

# Close the Spark session
spark.stop()

# Result:
# +----+-----+------------+
# |Item|Value|RunningTotal|
# +----+-----+------------+
# |   A|   10|          10|
# |   B|   15|          25|
# |   C|   20|          45|
# |   D|   25|          70|
# +----+-----+------------+

```

- PySpark SQL functions also provide powerful string manipulation capabilities. In this example, regexp_extract is used to perform regular expression based extraction to split an email into username and domain parts.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("StringManipulation").getOrCreate()

# Sample DataFrame
data = [("john.doe@company.com",), ("jane.smith@university.edu",)]
columns = ["Email"]
df = spark.createDataFrame(data, columns)

# Extract username and domain from email
df = df.withColumn("Username", regexp_extract("Email", "^(.*?)@", 1)) \
       .withColumn("Domain", regexp_extract("Email", "@(.*)", 1))

df.show()

# Close the Spark session
spark.stop()

# Result
# --------------------+----------+--------------+
# |               Email|  Username|        Domain|
# +--------------------+----------+--------------+
# |john.doe@company.com|  john.doe|   company.com|
# |jane.smith@univer...|jane.smith|university.edu|
# +--------------------+----------+--------------+

```

- PySpark functions are very useful for dealing with complex data types like arrays and maps. In this example, explode is used to transform each element of an array into a separate row.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, explode

# Initialize Spark session
spark = SparkSession.builder.appName("ArrayHandling").getOrCreate()

# Sample DataFrame
data = [(1, ["Apple", "Banana", "Orange"]), (2, ["Carrot", "Beans"])]
columns = ["Id", "Fruits"]
df = spark.createDataFrame(data, columns)

# Explode the fruits array into separate rows
df = df.withColumn("Fruit", explode("Fruits"))

df.show()

# Close the Spark session
spark.stop()

# Result:
# +---+--------------------+------+
# | Id|              Fruits| Fruit|
# +---+--------------------+------+
# |  1|[Apple, Banana, O...| Apple|
# |  1|[Apple, Banana, O...|Banana|
# |  1|[Apple, Banana, O...|Orange|
# |  2|     [Carrot, Beans]|Carrot|
# |  2|     [Carrot, Beans]| Beans|
# +---+--------------------+------+

```
