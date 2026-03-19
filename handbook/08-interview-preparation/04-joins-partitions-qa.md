# Joins & Partitions Q&A

40+ questions on join strategies, partitioning, and data skew.

## Join Strategies

### Q1: Explain broadcast join
**Answer**:
- Small table copied to all executors
- Large table scanned and matched
- No shuffle needed
- Fastest for small dimension tables

### Q2: When does Spark use each join strategy?
**Answer**:
- Broadcast: If table < threshold (10MB)
- Sort-Merge: Large tables, default
- Hash: In-memory joins on medium tables

## Data Skew

### Q3: What is data skew?
**Answer**: Uneven data distribution where one partition has 90%+ of data.

### Q4: How to detect skew?
**Answer**:
```python
df.groupBy(spark_partition_id()).count().show(100)
```

### Q5: How to fix skew?
**Answer**: Salting - add random salt and repartition.

## Partitioning Strategy

### Q6: What is optimal partition size?
**Answer**: ~128 MB per partition. calculation: data_size_GB * 8.

### Q7: How to repartition on key?
**Answer**:
```python
df.repartition(200, "key")
```

---

See [05-troubleshooting-qa.md](05-troubleshooting-qa.md) for debugging joins and partitions.
