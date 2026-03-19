# Troubleshooting Q&A

30+ debugging scenarios and solutions.

## Common Issues

### Q1: Job stuck at 99%?
**Answer**: Check for data skew. One task processing most data. Solution: Salting.

### Q2: OutOfMemoryError?
**Answer**: Partitions too large. Solution: Increase shuffle.partitions or add memory.

### Q3: GC overhead limit?
**Answer**: Memory pressure from large partitions. Solutions: More memory, fewer cores.

### Q4: Shuffle fetch failed?
**Answer**: Disk full, network timeout, or codec issue.

### Q5: Job slower than expected?
**Answer**: Too few partitions, no caching, inefficient joins. Check shuffle.partitions setting.

---

See [06-coding-patterns.md](06-coding-patterns.md) for solving coding challenges.
