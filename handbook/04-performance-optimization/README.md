# Performance Optimization

Techniques and patterns for optimizing Spark job performance, from basic caching to advanced tuning.

## Files in This Section

### [01. Caching Strategies](01-caching-strategies.md)
How to effectively use caching to speed up your Spark jobs, with special focus on ML workloads.

**Topics:**
- When to cache and when not to
- Cache storage levels (MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER)
- Caching for ML workloads (Generalized Linear Regression, etc.)
- Best practices and common pitfalls
- Real-world performance improvements (10-20x speedup)
- Troubleshooting cache-related OOM errors

**Key Insight**: Caching ML workloads can reduce training time from 5 minutes to 30 seconds by avoiding repeated S3 reads.

---

## Quick Navigation

**Need to speed up ML models?**
- See [Caching Strategies](01-caching-strategies.md) → Focus on "Caching for ML Workloads" section

**Experiencing OOM errors?**
- See [Caching Strategies](01-caching-strategies.md) → "Cache Storage Levels" and "When NOT to Cache"

**General job performance issues?**
- See [Troubleshooting - Performance Degradation](../07-troubleshooting/05-performance-degradation.md)

---

## Related Sections

- **[Data Skew Problems](../07-troubleshooting/03-skew-problems.md)** - Caching can help mitigate skew issues
- **[Memory Errors](../07-troubleshooting/02-memory-errors.md)** - OOM prevention when caching
- **[Production Patterns](../06-production-patterns/)** - How to structure large jobs for optimal performance

---

## Key Principles

1. **Profile before optimizing** - Use Spark UI to identify bottlenecks
2. **Cache only what you reuse** - Single-use DataFrames don't need caching
3. **Materialize after caching** - Always call `.count()` or another action after `.cache()`
4. **Monitor memory** - Check `spark.catalog.listCachedTables()` to avoid OOM
5. **Unpersist when done** - Free memory explicitly with `.unpersist()`

---

## Coming Soon

- **02. Shuffle Optimization** - Reduce shuffle size with partitioning strategies
- **03. Join Optimization** - When to use broadcast, sort-merge, and hash joins
- **04. Executor Configuration** - Tuning executor memory, cores, and overhead
- **05. Query Planning** - Understanding Spark's physical plans and optimization rules

---

Last updated: March 2026
