# Performance & Optimization Q&A

35+ questions on Catalyst, Tungsten, configuration, and optimization techniques.

## Catalyst Optimizer

### Q1: What optimizations does Catalyst do?
**Answer**: Predicate pushdown, column pruning, constant folding, join reordering.

### Q2: When can't Catalyst optimize?
**Answer**: When you use Python UDFs (black box to Catalyst).

## Caching

### Q3: When should you cache?
**Answer**: When reused 2+ times. Not for one-off operations.

### Q4: Cache() vs persist()?
**Answer**: `cache()` is `persist(MEMORY_ONLY)`. Same for MEMORY_ONLY level.

## Configuration

### Q5: What is shuffle.partitions?
**Answer**: Default partitions after shuffle (default 200). Tune to data_size_GB * 4.

### Q6: How to set configuration?
**Answer**:
```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "500") \
    .getOrCreate()
```

## Joins

### Q7: When to use broadcast join?
**Answer**: When small table < broadcast threshold (10MB default).

### Q8: Join strategy tradeoffs?
**Answer**:
- Broadcast: Fastest if small table fits memory
- Sort-Merge: For large tables, shuffle required
- Hash: In-memory hash table, good for medium tables

## Partitioning

### Q9: Repartition() vs coalesce()?
**Answer**:
- `repartition()`: Full shuffle, can increase/decrease partitions
- `coalesce()`: No shuffle, can only decrease partitions

### Q10: When to repartition?
**Answer**: Increasing partitions for parallelism, decreasing before write to reduce files.

---

See [04-joins-partitions-qa.md](04-joins-partitions-qa.md) for joins and partitions.
