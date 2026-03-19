# Core Concepts Q&A (50+ Questions)

Master the fundamentals that underpin all Spark knowledge.

---

## Architecture & Cluster

### Q1: Explain Spark's cluster architecture

**Answer**:

Spark uses a **driver-executor model**:

```
Driver Program (SparkContext/SparkSession)
    ├─ Creates DAG from your code
    ├─ Schedules tasks
    └─ Collects results

Cluster Manager (YARN/Mesos/K8s)
    └─ Allocates resources

Worker Nodes
    ├─ Executor 1 (runs tasks in parallel)
    ├─ Executor 2
    └─ Executor 3
```

- **Driver**: Your main program. Creates DAG, schedules tasks, collects results.
- **Executors**: Worker JVMs that run tasks. Cache data locally.
- **Cluster Manager**: Allocates resources (containers, memory, cores).
- **Task**: One partition × one stage = one unit of work.

**Why it matters**: Understanding parallelism and resource allocation

**Follow-up**: "How many executors should I use?" → Depends on cluster size and task size. Typically 20-50 for medium jobs.

---

### Q2: What's the difference between RDD, DataFrame, and Dataset?

**Answer**:

| Aspect | RDD | DataFrame | Dataset |
|--------|-----|-----------|---------|
| **Schema** | No schema | Named columns, typed | Schema + type safety |
| **Optimization** | None | Catalyst optimizer | Catalyst optimizer |
| **Performance** | Slow | Fast (100x RDD) | Fast |
| **API** | Functional (map, filter) | SQL-like (select, where) | Both |
| **Use in PySpark** | Rarely | All the time | N/A (Scala/Java only) |

**When to use**:
- **RDD**: Unstructured data, complex transformations (rare in modern Spark)
- **DataFrame**: 99% of use cases (structured/semi-structured data)
- **Dataset**: Type-safe Scala/Java code (not in Python)

**Code comparison**:
```python
# RDD - slow
rdd = sc.textFile("data.txt").map(lambda x: int(x)).filter(lambda x: x > 30).collect()

# DataFrame - fast (same result, 10-100x faster)
df = spark.read.text("data.txt").withColumn("value", col("value").cast("int")) \
    .filter(col("value") > 30)
```

**Interview tip**: Always choose DataFrame over RDD unless you have specific reason.

**Follow-up**: "When would you use RDD?" → Graph processing, unstructured text, complex custom transformations

---

### Q3: What is a Partition?

**Answer**:

A **partition** is a chunk of data distributed across the cluster.

- One partition = one task during execution
- If 1000 partitions, up to 1000 tasks run in parallel
- Each partition processed independently

**Example**:
```
DataFrame with 1000 rows, 100 partitions:
Partition 0: rows 1-10
Partition 1: rows 11-20
...
Partition 99: rows 991-1000
```

**Why it matters**:
- Parallelism depends on partition count
- More partitions = more parallelism (but scheduling overhead)
- Uneven partitions = data skew = slow jobs

**Check partition count**:
```python
df.rdd.getNumPartitions()  # Returns 100
```

**Follow-up**: "How many partitions should I have?" → Data size / 128MB. Example: 100GB data → 781 partitions, round to 1000.

---

### Q4: Explain the DAG (Directed Acyclic Graph)

**Answer**:

The **DAG** is Spark's internal representation of your job:

```
Step 1: Read CSV  ─┐
                   ├──→ Filter ─────┐
Step 2: Read Parquet ─┘               ├──→ Aggregate ─────┐
                                      │                   └──→ Write
Other transformations ────────────────┘
```

**How it works**:
1. You chain transformations (filter, select, groupBy, etc.)
2. Spark doesn't execute immediately (lazy evaluation)
3. Creates a logical DAG
4. Catalyst optimizes the DAG
5. Creates physical execution plan with stages
6. Action triggers execution

**Why it matters**:
- Catalyst can see entire job and optimize
- Example: Filter before join vs after join (big difference!)
- Predicate pushdown, column pruning, join reordering

**View DAG**:
```python
df.explain()  # See logical plan
df.explain(True)  # See physical plan with stage info

# Also in Spark UI → SQL tab
```

**Follow-up**: "How does Catalyst optimize?" → Predicate pushdown (push filters to source), column pruning (read only needed columns), join reordering (smaller table first)

---

### Q5: What's lazy evaluation and why is it important?

**Answer**:

**Lazy evaluation** means Spark doesn't execute transformations immediately:

```python
# None of this actually executes yet
df = spark.read.csv("data.csv")              # Step 1: Logical node
df_filtered = df.filter(df.age > 30)         # Step 2: Logical node
df_selected = df_filtered.select("name", "age")  # Step 3: Logical node

# NOW execution happens (action called)
df_selected.show()  # Action - triggers execution
```

**Why it matters**:

1. **Whole-job optimization**: Catalyst sees entire DAG
   - Eliminates unnecessary steps
   - Reorders operations for efficiency
   - Pushes filters down (read less data)

2. **Pipeline operations**: Multiple transformations executed together
   - Filter + select executed in one pass
   - Not written to disk in between

3. **Conditional execution**:
   ```python
   df_big = spark.read.parquet("huge_file")  # Not actually read yet!
   if condition:
       result = df_big.count()  # Only now does reading happen
   ```

**Gotcha**: Each action re-executes from source!

```python
df = spark.read.csv("data.csv")
count1 = df.count()  # Re-reads entire CSV
count2 = df.count()  # Re-reads entire CSV again (2x work!)

# Fix:
df.cache()
count1 = df.count()  # Read + cache
count2 = df.count()  # Uses cache
```

**Follow-up**: "When is lazy evaluation a problem?" → Each action retriggers computation. Solution: cache() reused DataFrames.

---

## Transformations vs Actions

### Q6: Explain transformations and actions

**Answer**:

**Transformation**: Returns new DataFrame, doesn't execute
- `filter()`, `select()`, `map()`, `groupBy()`, `join()`
- Builds DAG but doesn't compute

**Action**: Returns result to driver, triggers execution
- `show()`, `count()`, `collect()`, `take()`, `write()`
- Executes entire DAG

**Key insight**: Every action re-executes DAG from source!

```python
# Transformations (no execution)
df_f = df.filter(df.age > 30)          # DAG recorded
df_s = df_f.select("name")             # DAG recorded
df_o = df_s.orderBy("name")            # DAG recorded

# Actions (execute DAG)
df_o.show()                            # Spark executes entire chain
df_o.count()                           # Re-executes from start!
df_o.collect()                         # Re-executes again!
```

**Performance implication**:
```python
# Bad: Re-reads data 3 times
df = spark.read.csv("large_file")
count = df.count()
sum_val = df.agg(F.sum("amount")).collect()
df.write.parquet("output/")

# Good: Read once, use multiple times
df = spark.read.csv("large_file")
df.cache()
count = df.count()        # Read + cache
sum_val = df.agg(F.sum("amount")).collect()  # Use cache
df.write.parquet("output/")
```

**Follow-up**: "List 5 transformations and 5 actions"
- **Transformations**: filter, select, map, groupBy, join, orderBy, withColumn, drop, distinct, union
- **Actions**: count, collect, take, show, write, foreach, reduce, first, saveAsTextFile

---

### Q7: What are narrow vs wide transformations?

**Answer**:

**Narrow**: Each input partition → at most one output partition
- No shuffle needed
- Can be pipelined within a stage
- Fast

Examples: `filter()`, `select()`, `map()`, `withColumn()`, `drop()`

```
Input Partition 0 → Output Partition 0
Input Partition 1 → Output Partition 1
(direct mapping, no network)
```

**Wide**: Input partitions → multiple output partitions
- Requires shuffle (network data transfer)
- Creates stage boundary
- Slow

Examples: `groupBy()`, `join()`, `orderBy()`, `repartition()`, `distinct()`

```
Input Partitions 0,1,2 → Shuffle over network → Output Partitions A,B,C
(data redistributed, network intensive)
```

**Performance implication**:
```python
# 1 stage (only narrow)
df.filter(df.age > 30).select("name").withColumn("senior", F.lit(True))

# 2 stages (wide operation creates boundary)
df.filter(...).select(...).groupBy("department").count()
#                          ^^^^^^ - stage boundary here
```

**In Spark UI**:
- Each wide transformation = new stage
- Same stage = pipelined operations

**Follow-up**: "How do you minimize shuffles?" → Use broadcast joins, filter early, pre-partition on join keys

---

## Catalyst Optimizer

### Q8: What is the Catalyst Optimizer?

**Answer**:

**Catalyst** is Spark SQL's query optimization engine. It transforms your logical query into optimized physical plan:

```
Your Code
  ↓
Logical Plan (unoptimized)
  ↓
Catalyst Optimizer
  ├─ Predicate pushdown
  ├─ Column pruning
  ├─ Constant folding
  └─ Join reordering
  ↓
Physical Plan (optimized)
  ↓
Code Generation (Tungsten)
  ↓
Execution
```

**Key optimizations**:

1. **Predicate Pushdown**: Push filters down to data source
   ```python
   # Written code
   df.select("*").filter(df.age > 30)

   # Catalyst optimizes to
   df.filter(df.age > 30).select("*")  # Filter first, read less data!
   ```

2. **Column Pruning**: Read only needed columns
   ```python
   # You select 3 columns from 50
   df.select("name", "age", "salary")

   # Catalyst reads only 3 columns, not all 50!
   ```

3. **Constant Folding**: Pre-compute constants
   ```python
   # Catalyst sees (5 * 3) and computes to 15 at plan time
   df.withColumn("constant", F.lit(5 * 3))
   ```

4. **Join Reordering**: Put smaller table first
   ```python
   # You: df1.join(df2, "key")
   # Catalyst: If df2 is smaller, reorders to df2.join(df1, "key")
   ```

**Benefits**:
- Same code, 10-100x faster
- Transparent (no code changes needed)
- Works for both DataFrame API and SQL

**View optimization**:
```python
df.explain()  # Logical plan
df.explain(True)  # Extended (physical) plan

# Also Spark UI → SQL tab
```

**Follow-up**: "Why is Catalyst faster?" → UDFs are black boxes (Catalyst can't optimize), native functions are transparent

---

### Q9: What is Tungsten?

**Answer**:

**Tungsten** is Spark's execution engine for:
1. Memory management (off-heap)
2. Code generation
3. Cache-aware computation

**Key features**:

1. **Off-Heap Memory**: Bypasses JVM garbage collection
   - Java objects have overhead (24+ bytes)
   - Spark stores raw bytes in off-heap memory
   - Results: Less GC pauses, faster processing

2. **Whole-Stage Code Generation**: Fuses operators into single Java function
   - Instead of calling operator1, operator2, operator3
   - Generates one function: operator123
   - Results: Less function call overhead, better CPU cache usage

3. **Binary Encoding**: Compact data representation
   - Reduces memory footprint
   - Faster serialization

**Performance impact**:
```
Without Tungsten (RDD): 100 seconds
With Tungsten (DataFrame): 1-10 seconds (10-100x faster!)
```

**Enabled by default**: DataFrame and SQL automatically use Tungsten

**When you don't get Tungsten**:
- Using RDD operations
- Using Python UDFs (must go through Python interpreter)

**Code generation you can see**:
```python
df.explain()  # Look for "GeneratedClass$SpecificOrdering"
```

**Follow-up**: "How do I get Tungsten benefits?" → Use DataFrame API, avoid UDFs, use native Spark functions

---

## Shuffle & Partitioning

### Q10: What is a shuffle and why is it expensive?

**Answer**:

A **shuffle** is redistribution of data across network between executors.

**When shuffle happens**:
- `groupBy()`, `join()`, `orderBy()`, `distinct()`, `repartition()`
- Basically any wide transformation

**Cost breakdown**:
1. **Shuffle Write**: Executor writes intermediate data to disk
2. **Network Transfer**: Data sent over network to other executors
3. **Shuffle Read**: Receiving executor reads data

```
Stage 1 (Map)          Shuffle          Stage 2 (Reduce)
┌──────────────┐   ┌─────────┐    ┌──────────────┐
│ Partition A  │───┤Network &├───→│ Partition A' │
│ (contains    │   │  Disk   │    │ (all key=A)  │
│  keys A,B,C) │───┤Transfer ├───→│              │
│              │   │         │    │ Partition B' │
│ Partition B  │───┤         ├───→│ (all key=B)  │
│ (contains    │   │         │    │              │
│  keys B,C,D) │───┤         ├───→│ Partition C' │
│              │   │         │    │ (all key=C) │
└──────────────┘   └─────────┘    │              │
                                   └──────────────┘
```

**Cost factors**:
- **Amount of data**: 100GB shuffle = 100GB I/O + network
- **Number of partitions**: More partitions = more scheduling overhead
- **Data imbalance (skew)**: One large partition blocks entire stage

**Minimize shuffles**:
```python
# Bad: Join then filter
result = df1.join(df2, "key").filter(df1.value > 100)  # Shuffles all data

# Good: Filter before join
result = df1.filter(df1.value > 100).join(df2, "key")  # Shuffles less data
```

**Use broadcast for small tables**:
```python
from pyspark.sql.functions import broadcast

# Instead of shuffling both tables
result = df1.join(df2, "key")  # Shuffle if both large

# Small table to all executors (no shuffle for small table)
result = df1.join(broadcast(df2), "key")  # Only df1 shuffled
```

**Follow-up**: "How do you measure shuffle size?" → Spark UI → SQL tab → "Shuffle Read Size" and "Shuffle Write Size"

---

## Caching & Persistence

### Q11: When should you cache a DataFrame?

**Answer**:

**Cache** when a DataFrame is used **multiple times**:

```python
# BAD: Recomputes entire pipeline each time
df = spark.read.csv("data.csv")
count1 = df.filter(df.age > 30).count()  # Reads CSV
sum1 = df.filter(df.age > 30).agg(F.sum("amount")).collect()  # Reads CSV again

# GOOD: Cache, then reuse
df = spark.read.csv("data.csv")
df.cache()
count1 = df.filter(df.age > 30).count()  # Reads CSV, caches it
sum1 = df.filter(df.age > 30).agg(F.sum("amount")).collect()  # Uses cache
df.unpersist()
```

**Cache levels**:
```python
from pyspark import StorageLevel

df.cache()                                     # MEMORY_ONLY
df.persist(StorageLevel.MEMORY_ONLY)          # Memory only (fast, limited)
df.persist(StorageLevel.MEMORY_AND_DISK)      # Memory + disk (if spill)
df.persist(StorageLevel.DISK_ONLY)            # Disk only (slow)
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized (compact, slower access)
```

**Rules of thumb**:
- **Do cache**: Reused 2+ times, intermediate aggregations
- **Don't cache**: One-off transformations, if memory is tight

**Cache vs Persist**:
- `cache()` ≈ `persist(StorageLevel.MEMORY_ONLY)`
- `persist()` offers more control

**When to unpersist**:
```python
df.cache()
# ... use it ...
df.unpersist()  # Free memory when done
```

**Check what's cached**:
```python
spark.sql("SHOW CACHES").show()
```

**Follow-up**: "What if you cache too much?" → Spill to disk, memory pressure, OOM errors

---

## Real Interview Closing Tips

- **Know fundamentals cold**: If asked a follow-up, you should have answer
- **Use Spark UI references**: "I would check the Spark UI Executors tab to see..."
- **Show code**: Have working examples for each concept
- **Discuss tradeoffs**: "Broadcast join is faster but only works for small tables"

---

## Quick Self-Check

Can you answer without looking?
1. "Explain DAG" → ✓
2. "Narrow vs wide" → ✓
3. "Why cache?" → ✓
4. "Catalyst does what?" → ✓
5. "Shuffle cost factors?" → ✓

If not, review that section!

---

## See Also
- [DataFrame Basics](../01-fundamentals/02-dataframes-basics.md)
- [Transformations & Actions](../01-fundamentals/03-transformations-actions.md)
- [Catalyst Optimizer](../04-performance-optimization/03-catalyst-optimizer.md)
- [Data Skew](../03-joins-partitioning/05-data-skew.md)

---

**Next**: [02-dataframes-sql-qa.md](02-dataframes-sql-qa.md) - DataFrame operations and SQL queries
