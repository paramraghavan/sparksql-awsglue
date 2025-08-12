# PySpark Quick Cheat Sheet

## Critical Don'ts
- **Never forget `spark.stop()`** - Always clean up!
- **Don't use `.collect()` on large data** - Crashes driver
- **Don't chain multiple `.count()` or `.show()`** - Each triggers full computation
- **Avoid `unionByName()` with empty DataFrames** - Creates empty partitions
- **Don't convert to Pandas**: `.toPandas()` brings all data to driver

## Essential Do's
- **Always cache DataFrames used multiple times**: `df.cache()`
- **Check data first**: `df.show(5)`, `df.printSchema()`, `df.count()`
- **Use `.sample()` for development**: `df.sample(0.01)`
- **Monitor partitions**: `df.rdd.getNumPartitions()`
- **Avoid converting to Pandas**: Stay in Spark ecosystem

## 📋 Quick Setup
```python
# Safe session creation
spark = SparkSession.builder.appName("MyApp").getOrCreate()
try:
    # Your code here
    pass
finally:
    spark.stop()
```

## 🔧 Data Operations
```python
# Reading
df = spark.read.csv("file.csv", header=True, inferSchema=True)

# Basic checks
df.show(5)              # Show 5 rows
df.printSchema()        # Check structure  
df.count()              # Row count (expensive!)
df.describe().show()    # Summary stats

# Transformations (lazy)
df.filter(col("age") > 21)
df.select("name", "age")
df.groupBy("category").sum("value")

# Actions (trigger computation)
df.collect()    # Dangerous on large data
df.toPandas()   # Also dangerous - brings all to driver
df.show()       # Safe
df.write.csv("output/")  # Write to storage
```

⚡ Performance Tips
```python
# Cache before multiple uses
df.cache()
result1 = df.filter(...)
result2 = df.groupBy(...)
df.unpersist()  # Clean up

# Optimize partitions
df.coalesce(10)         # Reduce partitions
df.repartition(20)      # Increase partitions

# Safe union
if df2.count() > 0:
    result = df1.unionByName(df2)
else:
    result = df1
    
# When u use unionbyname
# call .count() on dataframe. When you do this spark trigers a full computation(action) on Df
# This forces the spark to materialize the DF and resolve any lazy tansformatios and potentially eliemiante empty or problamatic partitions
# this way the Df is in a "clean" state for writing.

# Broadcast small tables
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")
```

## 🔍 Debugging
```python
df.explain()                    # See execution plan
df.rdd.getNumPartitions()      # Check partitions
spark.catalog.clearCache()     # Clear all cached data
```

## 📝 Jupyter Notebook Checklist
- Start with small data samples
- Always include `spark.stop()` 
- Clear cache between experiments
- Use `df.show(n)` not `df.collect()` or `df.toPandas()`
- Check partition count before processing
- Keep data in Spark - avoid Pandas conversions