# Memory Errors (OutOfMemoryError)

## Symptom

- Job fails with "java.lang.OutOfMemoryError"
- "Container killed by YARN for exceeding memory limits"
- "GC overhead limit exceeded"
- Tasks fail randomly with memory errors

## Quick Fixes

### 1. Increase Executor Memory
```bash
spark-submit \
    --executor-memory 16g \
    --conf spark.executor.memoryOverhead=2g \
    my_job.py
```

### 2. Increase Shuffle Partitions
```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

Smaller partitions = less memory per executor

### 3. Reduce Executor Cores
```bash
spark-submit --executor-cores 4  # Instead of 5-8
```

Fewer tasks per executor = lower memory pressure

### 4. Filter Data Early
```python
# BAD: Load all, then filter
df = spark.read.parquet("huge_file")
result = df.filter(df.value > 100)

# GOOD: Filter at source if possible
result = spark.read.parquet("huge_file").filter(...)
```

## Root Causes

1. **Partitions too large** - Each partition processing >> data
2. **Caching too much** - Multiple cached DataFrames exhausting memory
3. **Collect() on large data** - Brings all data to driver
4. **UDFs with state** - Accumulating data in memory
5. **Shuffle spill** - Temporary data exceeding memory

## Solutions

See full troubleshooting guides:
- [Partitioning Strategy](../03-joins-partitioning/03-partitioning-strategies.md)
- [Caching Best Practices](../04-performance-optimization/01-caching-persistence.md)
- [UDF Optimization](../04-performance-optimization/05-udfs-optimization.md)

---

**See Also**: [Job Stuck Issues](01-job-stuck-issues.md), [Performance Degradation](05-performance-degradation.md)
