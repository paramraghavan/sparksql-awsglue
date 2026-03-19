# Performance Degradation (Slow Jobs)

## Symptom

- Job runs but is slower than expected
- Used to run in 10 minutes, now takes 50 minutes
- Cluster seems underutilized

## Investigation

1. Check Spark UI for:
   - Number of tasks/partitions (should be many)
   - Task duration (should be similar across tasks)
   - Shuffle read/write sizes (should be reasonable)

2. Check configuration:
   ```python
   spark.sparkContext.getConf().getAll()
   ```

3. Profile the job:
   ```python
   df.explain(True)  # See execution plan
   ```

## Solutions

### Solution 1: Increase Shuffle Partitions
```python
spark.conf.set("spark.sql.shuffle.partitions", "500")
```

### Solution 2: Enable AQE
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Solution 3: Use Broadcast Joins
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

### Solution 4: Filter Early
```python
# Filter before joins/aggregations
df_filtered = df.filter(df.date > "2024-01-01")
result = df_filtered.join(...)  # Join smaller data
```

---

**See Also**: [Performance Optimization](../04-performance-optimization/)
