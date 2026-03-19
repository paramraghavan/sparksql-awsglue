# PySpark Cheatsheet - One Page Reference

## Initialize Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

## Read/Write Data

```python
# Read
df = spark.read.option("header", "true").csv("file.csv")
df = spark.read.parquet("file.parquet")
df = spark.read.json("file.json")

# Write
df.write.mode("overwrite").parquet("output/")  # overwrite, append, ignore, error
df.write.mode("overwrite").option("header", "true").csv("output/")
df.write.partitionBy("year").parquet("output/")
```

## DataFrame Basics

```python
df.show(10, truncate=False)      # Display rows
df.printSchema()                 # Show schema
df.count()                       # Row count
df.columns                       # Column names
df.dtypes                        # Column types
df.explain(True)                 # Show execution plan
```

## Select & Filter

```python
df.select("col1", "col2")
df.select(F.col("name"), (F.col("age") * 2).alias("age2"))
df.filter(F.col("age") > 30)
df.filter("age > 30")             # SQL expression
df.where(F.col("city").isin(["NYC", "LA"]))
df.dropDuplicates(["email"])
```

## Transformations

```python
df.withColumn("new_col", F.lit("constant"))
df.withColumn("age_group", F.when(F.col("age") < 40, "<40").otherwise("40+"))
df.withColumnRenamed("old", "new")
df.drop("col1", "col2")
df.select(F.col("*"), F.col("col1").alias("renamed"))
```

## Aggregations

```python
df.groupBy("city").count()
df.groupBy("city").agg(
    F.count("*").alias("total"),
    F.avg("age").alias("avg_age"),
    F.max("salary").alias("max_salary"),
    F.sum("amount").alias("total_amount")
)
df.groupBy("city", "department").agg(...)
df.agg(F.avg("age"), F.count("*"))  # Entire DataFrame
```

## Joins

```python
# Inner (default)
df1.join(df2, on="id", how="inner")

# Left, right, outer, semi (non-matching rows from right), anti (no match in right)
df1.join(df2, on="id", how="left")
df1.join(df2, on="id", how="outer")
df1.join(df2, on="id", how="left_anti")  # Rows in df1 not in df2

# Broadcast (for small tables < 100MB)
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), on="id")
```

## Window Functions

```python
window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))

df.withColumn("rank", F.rank().over(window_spec))
df.withColumn("row_num", F.row_number().over(window_spec))
df.withColumn("prev_salary", F.lag("salary", 1).over(window_spec))
df.withColumn("next_salary", F.lead("salary", 1).over(window_spec))
```

## String Functions

```python
F.upper("col"), F.lower("col"), F.initcap("col")
F.trim("col"), F.ltrim("col"), F.rtrim("col")
F.substring("col", 1, 5)                  # Start position 1, length 5
F.length("col")
F.concat(F.col("first"), F.lit(" "), F.col("last"))
F.split("col", ",")                       # Returns array
F.explode(F.split("tags", ","))          # Array → rows
F.regexp_extract("col", r"pattern", 1)   # Regex group 1
F.regexp_replace("col", r"[^0-9]", "")   # Remove non-digits
```

## Date Functions

```python
F.current_date()
F.current_timestamp()
F.to_date("col", "yyyy-MM-dd")
F.to_timestamp("col")
F.year("date_col"), F.month(), F.dayofmonth(), F.hour(), F.minute()
F.date_add("date_col", 7)                 # Add 7 days
F.date_sub("date_col", 30)                # Subtract 30 days
F.date_format("date_col", "MMM dd, yyyy")
```

## Null Handling

```python
df.na.fill(0)                          # Fill nulls
df.na.fill({"age": 0, "name": "N/A"})
df.na.drop()                           # Drop any null
df.na.drop("all")                      # Drop if all nulls
df.filter(F.col("name").isNotNull())
F.coalesce("col1", "col2")             # Return first non-null
```

## Partitions & Performance

```python
df.rdd.getNumPartitions()              # Check partition count
df.repartition(200)                    # Shuffle to N partitions
df.repartition(200, "key")             # Shuffle by key
df.coalesce(10)                        # Reduce partitions (no shuffle)

df.cache()                             # Cache in memory
df.persist(StorageLevel.MEMORY_AND_DISK)
df.unpersist()
```

## Configuration Tuning

```python
spark.conf.set("spark.sql.shuffle.partitions", "500")     # Default 200
spark.conf.set("spark.sql.adaptive.enabled", "true")       # Auto optimization
spark.conf.set("spark.memory.fraction", "0.8")             # Default 0.6
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

## Common Patterns

```python
# Deduplication (keep first by order)
window = Window.partitionBy("id").orderBy(F.desc("created_at"))
df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

# Find top N per group
window = Window.partitionBy("dept").orderBy(F.desc("salary"))
df.withColumn("rank", F.rank().over(window)).filter(F.col("rank") <= 3)

# Running total
running_window = Window.partitionBy("account").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(running_window))

# Pivot (rows to columns)
df.groupBy("product").pivot("quarter").agg(F.sum("amount"))

# Explode arrays
df.select("id", F.explode("tags").alias("tag"))
```

## Spark SQL

```python
df.createOrReplaceTempView("my_table")
spark.sql("SELECT * FROM my_table WHERE age > 30").show()

# Cache SQL table
spark.sql("CACHE TABLE my_table")
spark.sql("SELECT * FROM my_table").show()
spark.sql("UNCACHE TABLE my_table")
```

## Debugging

```python
df.explain()                           # Logical plan
df.explain(True)                       # Extended (physical) plan
spark.sparkContext.getConf().getAll()  # All configs
df.rdd.getNumPartitions()
df.selectExpr("*", "spark_partition_id() as partition")  # Show partition IDs
```

## UDFs (Avoid if Possible!)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Slow: Python UDF
@udf(IntegerType())
def add_one(x):
    return x + 1

df.withColumn("new_col", add_one(F.col("age")))

# Better: Pandas UDF (vectorized)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("integer")
def add_one_vectorized(s: pd.Series) -> pd.Series:
    return s + 1

df.withColumn("new_col", add_one_vectorized(F.col("age")))

# Best: Use Spark functions
df.withColumn("new_col", F.col("age") + 1)  # 10-100x faster!
```

## EMR Spark Submit

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-memory 8g \
    --executor-cores 5 \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.memory.fraction=0.8 \
    my_job.py
```

## Common Issues Quick Fix

| Issue | Cause | Fix |
|-------|-------|-----|
| Job stuck at 99% | Data skew | Salt + repartition, or enable AQE |
| Out of memory | Partitions too large | Increase shuffle.partitions |
| Slow job | Low parallelism | Increase executors or cores |
| Shuffle disk spill | Memory pressure | Filter early, use broadcast |
| Small files problem | Too many partitions | Coalesce before write |

---

**Print this page. You'll reference it constantly!**
