![Hadoop yarn Cluster](img.png)
![Hadoop yarn Cluter continued](img_1.png)
![Spark-submit](img_2.png)
![Spark submit continued](img_3.png)
![Transformation and Actions](img_4.png)

## Jobs, Stages , Read/Write Excahnge buffer, Tasks

**start Job 0** 
```python
#code Block 0
readPopulationDF = spark.read
.option("header", "true")
.option("inferSchema", "true")
. csv(args(0)) # <<---- action
```
> csv data 
> https://www.statsamerica.org/downloads/default.aspx --> Population by Age and Sex
> https://population.un.org/wpp/downloads?folder=Standard%20Projections&group=CSV%20format

**end Job 0**

**start Job 1**
```python
#code Block 1
partitionedDF = readPopulationDF .repartition( numPartitions = 2) # wide dependency transformation
countDF = partitionedDF.where ( conditionExpr = Age ‹ 40" ) # narrow transformation
select( col = "Age", cols = "Gender", "Country", "state")  # narrow transformation
.groupBy( col1 = "Country") # wide dependency transformation
.count() # this is count on groupby, Still lazy! result is a DataFrame with columns: [department, count],
         # so it is a transform here and not an action
logger.info(countDF.collect()) # <<---- action
```
**end Job 1**

> Spark will run each code block **as one Spark Job**. 
> Note Job 0 is a read, so it's an action

### Job 1 in detail






## Explain why count on a groupby() is not action but a transformation

```python
# This is an ACTION - triggers execution
df.count()  # Returns a number immediately

# This is TRANSFORMATION + ACTION combined
df.groupBy("column").count()  # Returns a new DataFrame
```

### **What Actually Happens:**

**1. `groupBy()` is a Transformation:**

```python
grouped = df.groupBy("department")  # Lazy - nothing executed yet
# grouped is a GroupedData object, not a DataFrame
```

**2. `.count()` on GroupedData creates a new DataFrame:**

```python
result = df.groupBy("department").count()  # Still lazy!
# result is a DataFrame with columns: [department, count]
```

**3. To actually execute, you need an action:**

```python
# NOW it executes - this is the action
result.show()
result.collect()
result.write.parquet("output/")
```

### Here with an Example:**

```python
# Sample data
df = spark.createDataFrame([
    ("Sales", "John"),
    ("Sales", "Jane"),
    ("IT", "Bob"),
    ("IT", "Alice")
], ["department", "name"])

# This is lazy - no execution yet
grouped_counts = df.groupBy("department").count()
print("No execution happened yet!")

# THIS triggers execution
grouped_counts.show()
# Output:
# +----------+-----+
# |department|count|
# +----------+-----+
# |     Sales|    2|
# |        IT|    2|
# +----------+-----+
```
