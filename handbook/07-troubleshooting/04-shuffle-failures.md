# Shuffle Failures

## Symptom

- "Shuffle fetch failed"
- "Executor lost"
- "Block manager lost"
- Disk space errors during shuffle

## Common Causes

1. **Disk full during shuffle**
   - `/tmp/` directory space exhausted
   - Shuffle spill exceeds available disk

2. **Network timeout**
   - Shuffle fetch timeout (default 120s)
   - Slow/unreliable network

3. **Compression codec issue**
   - Parquet compression codec not available
   - Codec mismatch between writer/reader

## Solutions

### Solution 1: Clear Disk Space
```bash
# On EMR core nodes
sudo df -h
sudo rm -rf /tmp/spark-*
```

### Solution 2: Increase Shuffle Partitions
Smaller partitions = less temp disk needed
```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

### Solution 3: Increase Network Timeout
```python
spark.conf.set("spark.network.timeout", "600s")  # From default 120s
```

### Solution 4: Check Compression Codec
```python
# In Parquet
df.write \
    .option("compression", "snappy") \
    .parquet("output/")
```

---

**See Also**: [Shuffle Operations](../03-joins-partitioning/04-shuffle-operations.md)
