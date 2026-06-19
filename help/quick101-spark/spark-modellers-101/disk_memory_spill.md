# Memory Spill vs Disk Spill - Quick Guide

## What is Memory & Disk Spill?

**Memory Spill**: Data exceeded executor RAM limit → Spark evicts to disk
**Disk Spill**: Actually wrote that evicted data to EBS disk
**Result**: Disk is 100x slower than RAM = your job runs slow

---

## The Simple Analogy

```
Executor Memory = 4GB (small cutting board)
Data to process = 100GB (pile of vegetables)

1. Chop on board (4GB fits) → FULL
2. Can't fit more, brush excess to floor (disk)
3. Pick up from floor when needed → SLOW!
```

---

## Reading vs Shuffle - The Key Distinction

**❌ WRONG ASSUMPTION**: "If file is 100GB split into 128MB chunks, why does 100GB fit in 4GB memory?"

**✓ CORRECT ANSWER**: The problem is NOT during reading, it's during SHUFFLE operations!

### Stage 1: Reading from S3 (128MB Chunks) ✓ NO SPILL

```
100GB file → 128MB partitions (780 total)

Each executor reads 1 partition at a time:
├─ Read partition 1 (128MB) → Process → Output 1MB
├─ Read partition 2 (128MB) → Process → Output 1MB
├─ Read partition 3 (128MB) → Process → Output 1MB
└─ ... continue for 780 partitions

Memory used: Only 128MB at a time
Result: NO SPILL during read ✓
```

### Stage 2: groupBy() Shuffle Operation ✗ CAUSES SPILL

```
After reading all data, you run:
  df.groupBy("user_id").agg(sum("amount"))

This creates SHUFFLE:
├─ Spark collects ALL outputs from 780 partitions
├─ Combines by user_id (50M unique users)
├─ Creates intermediate hash map in memory:
│  {user_1: [50MB of values],
│   user_2: [60MB of values],
│   user_3: [45MB of values],
│   ...50 million more users...}
├─ Total intermediate data: 50-60GB
├─ Executor memory available: 4GB ← FULL!
└─ MEMORY SPILL TRIGGERED! ✗

Memory needed: 50-60GB
Memory available: 4GB
Result: 46-56GB SPILL!
```

### Timeline

```
t=0-60 sec:   Read 100GB in 128MB chunks (no spill) ✓
t=60-70 sec:  Start groupBy shuffle
t=75 sec:     Memory FULL (4GB) → SPILL begins ✗
t=75-420 sec: Disk I/O bottleneck (reading/writing 50GB)
t=420 sec:    Done (6 minutes total!)
```

**Key insight**: The 128MB partition reading is fine. The problem is the SHUFFLE creates intermediate data larger than executor memory.

---

## Real AWS Glue Scenario

```
Setup:
├─ 5 worker nodes (m5.xlarge, 16GB each)
├─ Each executor: 4GB memory
├─ Data: 100GB on S3
├─ Task: groupBy("user_id").agg(sum("amount"))

What happens:
├─ Start: 40GB total executor memory available
├─ Incoming: 100GB data
├─ Memory fills: 4GB FULL!
├─ Spill triggered: Send excess to EBS disk (~60GB)
├─ Problem: Reading from disk = 100-200 MB/s
├─ RAM speed = 10GB/s (50x faster)
└─ Result: Stage takes 5-10 MINUTES instead of 1 minute
```

---

## Why Executor Memory is Only 4GB

```
16GB Node Memory
├─ OS/System: 2GB (reserved)
├─ Spark overhead: 2GB (reserved)
├─ Available: 12GB
└─ Per Executor: 4GB × 2 executors per node
   └─ Formula: 16GB × 0.5 (config) ÷ 2 = 4GB
```

AWS Glue uses 4GB default because it's conservative (prevents crashes).

---

## Quick Comparison & How to Get 8GB ("Just Right")

| Scenario | Memory | Duration | Spill | How to Get |
|----------|--------|----------|-------|-----------|
| **Too Small** | 4GB | 5-10 min | 60GB ✗ | G.2X (default) |
| **Just Right** | 8GB | 1-2 min | 0GB ✓ | **G.4X (upgrade)** |
| **Plenty** | 16GB | 30 sec | 0GB ✓ | G.8X (expensive) |

---

## Detailed: How to Get 8GB Executor Memory

### Current Setup (4GB - Too Small)

```
AWS Glue Configuration:
├─ Worker type: G.2X (m5.xlarge)
├─ Node memory: 16GB
├─ Formula: 16GB × 0.5 / 2 executors = 4GB per executor
├─ Total: 4GB × 10 executors = 40GB
└─ Problem: 100GB data > 40GB memory = SPILL!
```

### Upgraded Setup (8GB - Just Right)

```
AWS Glue Configuration (CHANGE THIS):
├─ Worker type: G.4X (m5.2xlarge) ← UPGRADE from G.2X
├─ Node memory: 32GB (double the G.2X)
├─ Formula: 32GB × 0.5 / 2 executors = 8GB per executor
├─ Total: 8GB × 10 executors = 80GB
└─ Result: 100GB data WITH overhead fits in 80GB! ✓
```

### Step-by-Step in AWS Glue Console

```
1. Open AWS Glue Console
2. Edit your job
3. Find "Worker type" dropdown
   ├─ BEFORE: G.2X
   └─ AFTER: G.4X
4. Keep other settings same:
   ├─ Number of workers: 5 (same)
   ├─ Number of executors: 10 (automatic)
   └─ Spark parameters: (leave as is)
5. Save and run

Done! Now you have 8GB per executor.
```

### Cost-Benefit Analysis

```
COST:
Before: G.2X × 5 nodes × $0.25/hour = $1.25/hour
After:  G.4X × 5 nodes × $0.50/hour = $2.50/hour
Difference: +$1.25/hour

TIME SAVED:
Before: Job runs 5-10 minutes
After:  Job runs 1-2 minutes
Saved: 3-8 minutes per job

COST PER JOB:
Before: $1.25/hour × (8.5 min / 60) = $0.18 per job
After:  $2.50/hour × (1.5 min / 60) = $0.06 per job
SAVINGS: $0.12 per job (33% cheaper!)

If you run 20 jobs/day:
Before: $0.18 × 20 = $3.60/day
After:  $0.06 × 20 = $1.20/day
Daily savings: $2.40/day ✓
```

---

## Why 8GB is "Just Right" - The Math

```
Problem Setup:
├─ Data on S3: 100GB
├─ Number of executors: 10
├─ Data per executor: 100GB ÷ 10 = 10GB
└─ Task: groupBy("user_id").agg(sum("amount"))
   └─ Creates 50-60GB intermediate shuffle data

WITH 4GB EXECUTOR (G.2X):
├─ Memory available: 4GB
├─ Data to handle: 10GB
├─ Deficit: 10GB - 4GB = 6GB SHORT
├─ Solution: Spill 6GB to disk
├─ Speed: Disk I/O (100 MB/s) = SLOW
└─ Result: 5-10 minutes ✗

WITH 8GB EXECUTOR (G.4X):
├─ Memory available: 8GB
├─ Data to handle: 10GB
├─ Deficit: 10GB - 8GB = 2GB SHORT
├─ Solution: Spill only 2GB to disk (minimal)
├─ Speed: Mostly in RAM (10GB/s) with little disk I/O
└─ Result: 1-2 minutes ✓

WITH 16GB EXECUTOR (G.8X):
├─ Memory available: 16GB
├─ Data to handle: 10GB
├─ Extra buffer: 16GB - 10GB = 6GB FREE
├─ Solution: ZERO spill, all in memory
├─ Speed: RAM only (10GB/s) = FAST
└─ Result: 30 seconds (but costs 4x more!)
```

---

## Visual Memory Breakdown

```
G.2X (4GB per executor) - TOO SMALL:
┌─────────────────┐
│ Executor: 4GB   │
│ ├─ Data: 4GB ✓  │  = ALL FULL
│ ├─ Shuffle: 0GB │
│ └─ Spill: 6GB→  → EBS DISK (slow!)
└─────────────────┘

G.4X (8GB per executor) - JUST RIGHT:
┌─────────────────┐
│ Executor: 8GB   │
│ ├─ Data: 6GB ✓  │
│ ├─ Shuffle: 2GB │  = MOSTLY FULL
│ ├─ Free: 0GB    │
│ └─ Spill: ~0GB  → Minimal disk
└─────────────────┘

G.8X (16GB per executor) - PLENTY:
┌──────────────────────┐
│ Executor: 16GB       │
│ ├─ Data: 6GB ✓       │
│ ├─ Shuffle: 4GB      │
│ ├─ Free: 6GB ✓✓      │  = ROOM TO SPARE
│ └─ Spill: 0GB        → No disk!
└──────────────────────┘
```

---

## How to Keep Shuffle Size Small (Prevent Spill)

The shuffle stage creates large intermediate data. Here's how to keep it from being too big:

### Guideline 1: Reduce Unique Keys in groupBy()

```python
# ❌ BAD: 50 million unique user_ids
df.groupBy("user_id").agg(sum("amount"))
# Creates: 50M × average_data_per_user = 50-60GB

# ✅ GOOD: Group by category (only 100 categories)
df.groupBy("category").agg(sum("amount"))
# Creates: 100 × average_data_per_category = 100MB
```

**Rule**: Fewer unique keys = smaller shuffle = no spill

### Guideline 2: Pre-Filter Before Shuffle

```python
# ❌ BAD: Shuffle all 100GB then filter
df.groupBy("user_id").agg(sum("amount")).filter(col("amount") > 1000)

# ✅ GOOD: Filter first, then shuffle smaller data
df.filter(col("amount") > 1000) \
  .groupBy("user_id").agg(sum("amount"))
# Reduces data from 100GB → 50GB before shuffle
```

**Rule**: Push filters down before expensive operations

### Guideline 3: Increase Shuffle Partitions

```python
from pyspark.sql.functions import col, sum

# Default: 200 shuffle partitions
# Result: Each partition = 100GB ÷ 200 = 500MB (might fit in 4GB)

# Increase partitions to distribute work better
spark.conf.set("spark.sql.shuffle.partitions", "400")

result = df.groupBy("user_id").agg(sum("amount"))
# Now: Each partition = 100GB ÷ 400 = 250MB
# Each executor handles less data = less spill
```

**Rule**: More shuffle partitions = smaller intermediate chunks = less memory per executor

### Guideline 4: Use Map-Side Aggregation

```python
# ❌ BAD: Creates full shuffle
df.groupBy("user_id").agg(sum("amount"))

# ✅ GOOD: Pre-aggregate within partitions (map-side agg)
df.groupBy("user_id").agg(sum("amount")) \
  .groupBy("user_id").agg(sum("sum(amount)"))  # Second agg is smaller

# Or better: Use reduceByKey (more efficient)
result = df.rdd \
  .map(lambda x: (x.user_id, x.amount)) \
  .reduceByKey(lambda a, b: a + b) \
  .toDF(["user_id", "total"])
```

**Rule**: Aggregate before shuffle to reduce data size

### Guideline 5: Estimate Shuffle Size Before Running

```
Shuffle Size Estimate:
├─ Number of unique keys × Average value size per key
├─ Example: 50M users × 1KB per user = 50GB
│
├─ With 4GB executor memory:
│  └─ Can safely handle: 4GB × 0.7 = 2.8GB shuffle data
│
└─ If shuffle > 2.8GB per executor:
   ├─ Need to increase executor memory
   ├─ OR reduce unique keys
   ├─ OR increase shuffle partitions
   └─ OR pre-filter data
```

### Guideline 6: Monitor Shuffle Metrics

In Spark UI, check the **Shuffle** tab:
```
Shuffle Metrics:
├─ Shuffle Spill (Memory): Should be 0
├─ Shuffle Spill (Disk): Should be 0
├─ Shuffle Records Written: Total data shuffled
└─ Shuffle Write Time: Should be < 1 minute
```

If Shuffle Spill > 0, apply guidelines 1-5 above.

---

## Quick Decision Tree

```
Is there Spill in groupBy()/join()?
│
├─ YES → Check: Do I have many unique keys?
│  │
│  ├─ YES (50M+ users) → Increase executor memory (Guideline 1)
│  │
│  └─ NO (10 categories) → Already optimized!
│
└─ NO → All good! ✓
```

---

## How to Fix It (3 Options)

### 1️⃣ Upgrade Worker Type (Best)
```
BEFORE: G.2X (16GB node) → 4GB per executor
AFTER:  G.4X (32GB node) → 8GB per executor

Result: Double memory = No spill, 5x faster
Cost: +50% but job runs faster (breaks even)
```

### 2️⃣ Add More Worker Nodes
```
BEFORE: 5 nodes × 4GB = 40GB total
AFTER:  10 nodes × 4GB = 80GB total

Result: More memory = No spill
Cost: 2x more expensive
```

### 3️⃣ Optimize Code (Using Guidelines)
```python
# Apply guidelines to reduce shuffle size

# BAD - creates 50-60GB shuffle
df.groupBy("user_id").agg(sum("amount"))

# GOOD - filters first (Guideline 2)
df.filter(col("amount") > 0) \
  .groupBy("user_id").agg(sum("amount"))

# BETTER - increase shuffle partitions (Guideline 3)
spark.conf.set("spark.sql.shuffle.partitions", "400")
df.filter(col("amount") > 0) \
  .groupBy("user_id").agg(sum("amount"))

# BEST - all three approaches
spark.conf.set("spark.sql.shuffle.partitions", "400")
df.filter(col("amount") > 0) \
  .groupBy("user_id") \
  .agg(sum("amount").alias("total"))

Result: 70% less spill, 60% faster
```

---

## Reading Spark UI Spill Metrics

```
Bad Stage:
├─ Memory Spill: 19GB  = Data exceeded RAM
├─ Disk Spill: 15GB   = Actually wrote to disk
└─ Duration: 5+ min   = Because disk I/O is bottleneck

Good Stage:
├─ Memory Spill: 0B    = All fit in RAM
├─ Disk Spill: 0B     = No disk involved
└─ Duration: 30 sec   = Fast!
```

---

## Key Points

✅ **Disk Spill ≠ Disk Full**
→ It's not a space problem, it's a performance problem

✅ **Memory Spill → Disk Spill**
→ When RAM full, data goes to disk temporarily

✅ **Why So Slow?**
→ Disk (100 MB/s) vs RAM (10 GB/s) = 100x difference

✅ **Quick Fix**
→ Use G.4X worker type instead of G.2X

---

## When You See This, Ask Yourself

```
Memory Spill: 60GB
Data Size: 100GB
Executor Memory: 4GB

Question: Why fit 100GB into 4GB?
Answer: Get bigger executor (upgrade worker type)
```

**Bottom line**: Spill = Your data is too big for executor memory. Fix by increasing memory or nodes.