# Data Skew Problems

## Symptom

- Some tasks run 10-100x longer than others
- One executor busy while others idle
- In Spark UI, task duration varies wildly

## Common Causes

1. **Join on high-cardinality column with skewed distribution**
   - Grouping by user_id where some users have 1M events
   - Joining on product_id where some products are very popular

2. **Filtering creating imbalance**
   - After filter, 90% of data in one partition

3. **Key distribution naturally skewed**
   - Real-world data often has power law distribution

## Solutions

### Solution 1: Salting (Proven)
```python
from pyspark.sql.functions import rand, floor

df_salted = df \
    .withColumn("_salt", floor(rand() * 10).cast("int")) \
    .repartition(200, "_salt") \
    .drop("_salt")
```

### Solution 2: Enable AQE
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Solution 3: Repartition on Join Key
```python
# Pre-partition before join
df1 = df1.repartition(500, "join_key")
df2 = df2.repartition(500, "join_key")
result = df1.join(df2, "join_key")
```

---

**See Also**: [Job Stuck Issues](01-job-stuck-issues.md), [Data Skew Deep Dive](../03-joins-partitioning/05-data-skew.md)
