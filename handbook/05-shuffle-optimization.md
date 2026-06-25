# 05 - Shuffle Optimization

## What is Shuffle? (Complete Explanation)

**Definition:** Redistributing data across executors so records with the same key end up on the same partition.

**When it happens:**
```
df.groupBy("id").sum()      # GroupBy needs all records with same id together
df.join(df2, "key")          # Join needs matching records on same executor
df.repartition(50)           # Repartition redistributes all data
df.orderBy("timestamp")      # OrderBy needs all data sorted
df.distinct()                # Distinct needs to check for duplicates
```

### **Why Shuffle is Needed: A Real Example**

```
BEFORE SHUFFLE (Data scattered randomly):
Executor A: [customer 5, customer 3, customer 8, customer 1]
Executor B: [customer 7, customer 2, customer 5, customer 9]
Executor C: [customer 4, customer 6, customer 5, customer 2]
Executor D: [customer 1, customer 8, customer 3, customer 6]

Problem: Same customer's records are scattered!
├─ Customer 5 is on executors A, B, C (three places!)
├─ Customer 1 is on executors A, D
├─ Customer 3 is on executors A, D
└─ Can't do groupBy(customer).sum() until they're together!

AFTER SHUFFLE (Data reorganized by key):
Executor A: [customer 1, customer 1, customer 5, customer 5, customer 5]
Executor B: [customer 2, customer 2, customer 3, customer 3, customer 6, customer 6]
Executor C: [customer 4, customer 7, customer 8, customer 8, customer 9]
Executor D: [empty or other keys]

Now: All customer 5's are in one place (executor A)
└─ Can calculate: customer 5 total = sum([5, 5, 5]) ✓
```

### **How Shuffle Works (4-Step Process)**

```
STEP 1: PLAN (Catalyst Optimizer)
├─ SQL: df.groupBy("customer_id").sum("amount")
├─ Catalyst sees: Need to group by customer_id
└─ Plan: Repartition to 200 partitions by customer_id hash

STEP 2: SHUFFLE WRITE (Map Phase) - Write to Disk
├─ Each executor processes its input partitions
├─ For each record: hash(customer_id) % 200 = destination partition
├─ Group records by destination: bucket 0, bucket 1, ... bucket 199
├─ Write each bucket to disk (on executor's local SSD)
├─ Example:
│  ├─ Bucket 0 (customer_id % 200 == 0): customers 0, 200, 400, ...
│  ├─ Bucket 1 (customer_id % 200 == 1): customers 1, 201, 401, ...
│  └─ Bucket 199 (customer_id % 200 == 199): customers 199, 399, 599, ...
│
└─ Calculation for ONE executor:
   ├─ Input file: 100GB
   ├─ Split into partitions: 100GB ÷ 128MB per partition = ~780 partitions
   ├─ Distributed across: 10 executors
   ├─ Partitions per executor: 780 ÷ 10 = 78 partitions
   ├─ Each partition size: 128MB (= 1 row group from Parquet)
   ├─ Output from this executor: 78 partitions × 128MB = 9,984MB ≈ 10GB
   │
   └─ Result: 10GB of shuffle files on disk (from ONE executor)
      └─ Note: This data is REORGANIZED into 200 buckets, but total size stays ~10GB

STEP 3: SHUFFLE READ (Reduce Phase) - Fetch from Disk
├─ New set of executors assigned to read partitions
├─ Each read executor responsible for reading certain buckets
├─ Read executor 0: Fetches bucket 0 from ALL write executors
│  ├─ Fetch from executor A's disk: bucket 0 file (customers 0, 200, 400...)
│  ├─ Fetch from executor B's disk: bucket 0 file (customers 0, 200, 400...)
│  ├─ Fetch from executor C's disk: bucket 0 file (customers 0, 200, 400...)
│  └─ Network transfer between executors
├─ Read executor 1: Fetches bucket 1 from ALL write executors
├─ ... and so on
└─ Result: All customer 1's in read executor 1, all customer 2's in read executor 2, etc.

STEP 4: AGGREGATE (Final Computation)
├─ Read executor 0 has all records for bucket 0 (customers 0, 200, 400, ...)
├─ Groups them: customer 0 → [records], customer 200 → [records], ...
├─ Computes sum for each group
└─ Result: customer 0 sum = 1500, customer 200 sum = 2300, etc.
```

### **The Complete Data Flow Diagram**

```
INPUT: 100GB file → 780 partitions (randomly distributed customers)
                ↓
        STEP 1: PLAN
        └─ Catalyst decides: Repartition to 200 buckets by customer_id
                ↓
        STEP 2: SHUFFLE WRITE (Executors A-J)
        ├─ Executor A processes partitions 1, 11, 21, ... 771
        │  ├─ For each record: hash(customer_id) % 200
        │  ├─ Group into 200 buckets
        │  └─ Write 200 files to disk (128MB each = 10GB total from A)
        ├─ Executor B processes partitions 2, 12, 22, ... 772
        │  └─ Same: Write 200 files to disk (10GB total from B)
        ├─ ... same for executors C-J
        └─ Total disk usage: 10 executors × 10GB = 100GB (but temporary!)
                ↓
        NETWORK TRANSFER (All executors participate)
        ├─ Read executor 1 fetches bucket 0 from executors A-J
        │  └─ Network: A→1, B→1, C→1, ... J→1
        ├─ Read executor 2 fetches bucket 1 from executors A-J
        │  └─ Network: A→2, B→2, C→2, ... J→2
        └─ ... continues for all 200 buckets
                ↓
        STEP 3: SHUFFLE READ (Executors A-J as read executors)
        ├─ Read executor A receives bucket 0 from all write executors
        │  └─ Now has ALL customer 0, 200, 400, ... in one place
        ├─ Read executor B receives bucket 1 from all write executors
        │  └─ Now has ALL customer 1, 201, 401, ... in one place
        └─ ... data is now organized!
                ↓
        STEP 4: AGGREGATE (Compute sum per customer)
        ├─ Read executor A: sum([customer 0 records]) = amount1
        │                   sum([customer 200 records]) = amount2
        ├─ Read executor B: sum([customer 1 records]) = amount3
        │                   sum([customer 201 records]) = amount4
        └─ Result: One sum per customer!
                ↓
        OUTPUT: 200 partitions × sum per customer
```

### **Why Shuffle is Expensive**

```
Network Transfer:
├─ 100GB of data moves across network
├─ Network is slow compared to CPU
└─ Network is the bottleneck in shuffle!

Disk I/O:
├─ Write 100GB to local disk (map phase)
├─ Read 100GB from local disk (reduce phase)
├─ Even fast SSD is slower than memory
└─ But faster than network for same data!

Serialization:
├─ Convert records to bytes for network transfer
├─ Convert bytes back to objects for processing
├─ CPU overhead but necessary for network transfer

Example: 100GB shuffle on 10Gbps network
├─ Without compression: 100GB ÷ 10Gbps = 80 seconds
├─ With compression (2:1): 50GB ÷ 10Gbps = 40 seconds
└─ Network dominates the total shuffle time!
```

### **Why We Write to Disk (Summary)**

Based on everything we discussed:

```
EXECUTOR PROCESSES MANY PARTITIONS SEQUENTIALLY:
100GB file → 780 partitions → 1 executor processes 78 partitions

Each partition generates output for 200 shuffle buckets:
├─ Partition 1: Generate 200 buckets, total ~128MB
├─ Partition 11: Generate 200 buckets, total ~128MB
├─ Partition 21: Generate 200 buckets, total ~128MB
├─ ... continue ...
├─ Partition 778: Generate 200 buckets, total ~128MB
└─ Total output: 78 × 128MB = 10GB

WHERE TO STORE THIS 10GB OUTPUT?

Option 1: Execution Memory (1.5GB) ❌
├─ After 12 partitions: 1.5GB full
├─ Remaining 66 partitions: Nowhere to store!
└─ Result: OOM (Out of Memory)

Option 2: Storage Memory (1.5GB) ❌
├─ Wrong type (designed for persistent cache)
├─ Blocks user caching
└─ Architectural mismatch

Option 3: Local Disk ✅
├─ Write partition 1 output → 128MB → Free memory
├─ Write partition 11 output → 128MB → Free memory
├─ Write partition 21 output → 128MB → Free memory
├─ ... continue for all 78 partitions
└─ Memory always available, disk accumulates 10GB
```

---

## Table of Contents
1. [Shuffle Mechanics](#shuffle-mechanics)
2. [Partition Sizing](#partition-sizing)
3. [Shuffle Tuning Parameters](#shuffle-tuning-parameters)
4. [Real Examples](#real-examples)

---

## Shuffle Mechanics

🔗 **Related:** Section 01 - Partitioning | Section 04 - Memory Spill | Section 06 - Join Strategies

### Clarification: What are "Write Executors" vs "Read Executors"?

**Key Point:** They're the SAME pool of executors - just different roles in the same shuffle operation!

```
BEFORE SHUFFLE (Map stage):
├─ Your 100 input partitions distributed across 10 executors
├─ Each executor processes assigned partitions (e.g., Executor A has 10 partitions)
└─ Executors doing this work = "WRITE EXECUTORS" (writing output to shuffle files)

AFTER SHUFFLE (Reduce stage):
├─ Your 200 output shuffle partitions distributed across same 10 executors
├─ Each executor processes assigned output partitions (e.g., Executor A now has 20 output partitions)
└─ Executors doing this work = "READ EXECUTORS" (reading from shuffle files)

SAME CLUSTER, SAME 10 EXECUTORS, DIFFERENT JOBS/ROLES:
```

So it's not 2 sets of executors - it's the same executors taking on different responsibilities!

### Two-Phase Shuffle (Distributed Across the Cluster)

**Important:** Shuffle is a DISTRIBUTED operation - happens on ALL executors simultaneously

```
PHASE 1: SHUFFLE WRITE (happens on ALL write executors in parallel)
│
├─ Each write executor processes its assigned input partitions
│  └─ (could be on core or task nodes)
│
├─ Step 1: Map each record to shuffle bucket
│  └─ Bucket = destination partition for that record (based on key hash)
│
├─ Step 2: Sort records by key within each bucket
│  └─ Writing sorted data to executor's local disk
│
└─ Step 3: Write sorted buckets to disk
   └─ Each executor writes multiple files
      └─ 1 file per destination partition
      └─ Example: Executor A writes 200 files (one for each of 200 shuffle partitions)
      └─ Files stored locally on executor's machine

PHASE 2: SHUFFLE READ (happens on ALL read executors in parallel)
│
├─ Each read executor assigned to certain output partitions
│  └─ Example: Executor 1 → output partitions 0-10, Executor 2 → partitions 11-20
│
├─ Step 1: Fetch shuffle files from ALL write executors
│  └─ Network transfer: Read executor reaches across network to write executor disks
│  └─ Example: Executor 1 fetches "partition 5" files from Executor A, B, C, D, etc.
│  └─ Memory buffered: Limited by spark.reducer.maxSizeInFlight (default 48MB)
│
├─ Step 2: Merge-sort all fetched data
│  └─ Combine files from multiple write executors into single sorted stream
│
└─ Step 3: Group by key (for groupBy) or match keys (for join)
   └─ Now data is ready for next operation
```

**Key Point:** Network traffic is the bottleneck!
- Write phase: LOCAL disk I/O (fast)
- Read phase: NETWORK transfer between executors (slow, expensive)

---

### Why Write to Disk? (Not Memory!)

**This is a CRITICAL question.** Why doesn't Spark keep shuffle data in executor/storage memory instead of writing to disk?

#### **The Real Problem: Continuous Processing Pipeline**

Now that we know a 100GB file creates ~780 partitions, here's the actual issue:

```
SCENARIO: GroupBy on single 100GB Parquet file

Input: 1 file → 780 partitions (1 row group each = 128MB)
Output: 200 shuffle partitions

Executor A's workload:
├─ Partition 1 (rows 1-100k): Process → Output for all 200 shuffle buckets
├─ Partition 11 (rows 100k-200k): Process → Output for all 200 shuffle buckets
├─ Partition 21 (rows 200k-300k): Process → Output for all 200 shuffle buckets
│
│  EACH PARTITION = 128MB row group
│  EACH NEEDS 200 shuffle buckets of output
│
├─ Partition 31, 41, 51, ... (keeping processing!)
└─ Total: Executor A processes 78 partitions (780÷10 executors)

TIMELINE:
Time 1: Process partition 1 → Generate output for 200 buckets
Time 2: Process partition 11 → Generate output for 200 buckets (DIFFERENT DATA!)
Time 3: Process partition 21 → Generate output for 200 buckets (DIFFERENT DATA!)
...
Time 78: Process partition 778 → Generate output for 200 buckets

PROBLEM:
├─ Time 1 output (partition 1) still needed during Time 2-78
├─ Time 2 output (partition 11) still needed during Time 3-78
├─ Time 3 output (partition 21) still needed during Time 4-78
├─ Can't discard outputs → need to hold them
├─ But can't hold them all in memory (would be 10GB!)
└─ Must write to disk to free memory for next partition's output
```

**The key insight:** Each of the 78 partitions an executor processes generates output that must persist until the shuffle read phase. Disk is the only place to store temporary outputs while continuing to process new partitions.

**WHY DISK IS NECESSARY (Based on Real Partition Count):**

```
ATTEMPT 1: Keep All Shuffle Output in Executor Memory
├─ Executor A is processing 78 input partitions (780÷10)
├─ Each partition generates output for 200 shuffle buckets
├─ Tries to hold ALL 78 partitions' outputs in memory:
│  ├─ Output from partition 1: ~128MB (1 row group worth)
│  ├─ Output from partition 11: ~128MB
│  ├─ Output from partition 21: ~128MB
│  │  ...
│  └─ Output from partition 778: ~128MB
│  └─ Total: 78 × 128MB = ~10GB of outputs!
├─ Available memory: 1.5GB execution memory
└─ Result: ❌ FAILS at partition 12-13 (would be 10GB >> 1.5GB)

ATTEMPT 2: Keep in Storage Memory
├─ Storage memory designed for df.cache() (persistent caching)
├─ Shuffle is TEMPORARY (needed only once, then discarded)
├─ If we used storage memory:
│  ├─ 10GB of shuffle outputs fill 1.5GB storage memory
│  ├─ Can't cache ANY user dataframes for reuse
│  ├─ Storage memory wasted on temporary data
│  └─ Defeats the purpose of having dedicated storage memory
└─ Result: ❌ FAILS - Wrong memory type, defeats caching

SOLUTION: Write to Local Disk (THE RIGHT WAY)
├─ Executor A processes partition 1 (128MB loaded)
├─ Generates outputs for 200 shuffle buckets
├─ Writes buckets to disk progressively (frees memory as writes complete)
├─ Deletes outputs from memory ✓ MEMORY NOW FREE
├─ Loads partition 11 (next 128MB)
├─ Generates outputs for 200 shuffle buckets
├─ Writes buckets to disk (frees memory)
├─ Continues with partition 21, 31, ... 778
├─ Total disk used: ~10GB (temporary, discarded after shuffle read)
├─ Memory at any moment: Only current partition (~128MB)
└─ Result: ✅ WORKS PERFECTLY
   ├─ Executor memory stays free for continuous processing
   ├─ Disk stores all temporary outputs
   └─ Read executors fetch from disk later
```

#### **The Fundamental Reason:**

```
EXECUTOR PROCESSING IS CONTINUOUS:

Write Phase Timeline:
├─ T=0-1s: Load partition 1 → Process → Output → Write to disk → FREE MEMORY
├─ T=1-2s: Load partition 11 → Process → Output → Write to disk → FREE MEMORY
├─ T=2-3s: Load partition 21 → Process → Output → Write to disk → FREE MEMORY
├─ ...
├─ T=77-78s: Load partition 778 → Process → Output → Write to disk → FREE MEMORY
└─ Total: 78 partitions processed sequentially in ~78 seconds

IF WE KEPT IN MEMORY:
├─ T=0-1s: Load partition 1 → Process → Output → HOLD IN MEMORY (128MB used)
├─ T=1-2s: Load partition 11 → Process → Output → HOLD IN MEMORY (256MB used)
├─ T=2-3s: Load partition 21 → Process → Output → HOLD IN MEMORY (384MB used)
├─ ...
├─ T=11-12s: Load partition 121 → Process → Output → HOLD IN MEMORY (1.5GB+ USED!)
├─ T=12-13s: Load partition 131 → NO MEMORY LEFT! ❌ OOM
└─ Failed at partition 131, never finished 78 partitions!

DISK SOLUTION:
├─ Memory usage stays constant (~150MB per partition)
├─ Each partition's output flushed to disk before loading next
├─ Disk accumulates all 10GB of temporary outputs
├─ Read phase fetches from disk later
└─ Success: All 78 partitions processed!
```

#### **Real Numbers: Why Memory Isn't Enough**

```
SCENARIO: GroupBy on 100GB file with 200 shuffle partitions
Cluster: 10 executors (1 per core for simplicity)

Input Data Distribution:
├─ Total file: 100GB
├─ Split into 10 input partitions: 100GB / 10 = 10GB each
├─ Executor A gets: Input partition A (10GB of raw data)
└─ Other executors get: Input partitions B-J (10GB each)

Shuffle Output from Executor A:
├─ Executor A reads 10GB input partition
├─ Generates output buckets for ALL 200 shuffle partitions
│  ├─ Bucket 0: Records where key%200==0 (some of the 10GB)
│  ├─ Bucket 1: Records where key%200==1 (some of the 10GB)
│  ├─ ...
│  └─ Bucket 199: Records where key%200==199 (some of the 10GB)
├─ Total output from A: ~10GB (SAME data, just reorganized into 200 buckets)
└─ This 10GB must be written to 200 separate files (one per shuffle partition)

Memory Available per Executor: 4GB
├─ Execution memory: 1.5GB (for processing)
├─ Storage memory: 1.5GB (for caching)
└─ Reserved: 1GB (OS, GC)

PROBLEM: 10GB of output >>> 1.5GB execution memory!

If we try to keep in memory:
├─ First 1.5GB fits → OK
├─ At 1.5GB+: No more space, new data arrives
├─ Result: OOM (Out of Memory) or memory spill ❌

Disk Solution:
├─ Executor A writes bucket 0: 50MB → disk (memory freed)
├─ Executor A writes bucket 1: 50MB → disk (memory freed)
├─ Executor A writes bucket 2: 50MB → disk (memory freed)
├─ ... writes all 200 buckets progressively
├─ After writing all 10GB buckets to disk: Memory completely free
├─ Total disk used: 10GB (plenty of space on local SSD/HDD)
└─ Result: ✓ Works perfectly

Visual: Data Flow for Executor A

INPUT (10GB):
[Customer transactions from 100GB file]
    ↓
Executor A processes:
[Read 10GB partition → generate output for all 200 shuffle buckets]
    ↓
MEMORY (1.5GB execution space):
[Buffer bucket 0] → 50MB → flush to disk → free
[Buffer bucket 1] → 50MB → flush to disk → free
[Buffer bucket 2] → 50MB → flush to disk → free
...
[Buffer bucket 199] → 50MB → flush to disk → free
    ↓
DISK (10GB total):
file_0: Bucket 0 data (for read executor 0)
file_1: Bucket 1 data (for read executor 1)
file_2: Bucket 2 data (for read executor 2)
...
file_199: Bucket 199 data (for read executor 199)

Total: 10GB written progressively, memory never exceeds 50MB for any one bucket
```

---

### **Critical Question: How Does 1.5GB Memory Read 10GB Input?**

**This is the most important insight about Spark's architecture!**

#### **The Misconception:**
"Executor has 1.5GB memory but must process 10GB partition - shouldn't we use 100 partitions instead of 10?"

**Answer: NO! Here's the fundamental misunderstanding:**

```
WRONG ASSUMPTION:
"Read entire 10GB partition → hold in memory → process"
├─ 10GB needs to fit in memory
├─ But only 1.5GB available
└─ Solution: Use more partitions (100 instead of 10)

CORRECT REALITY:
"Stream data from disk → process BATCHES → write output → discard → repeat"
├─ Only 1 batch in memory at a time (~100MB)
├─ Process batch, output to shuffle
├─ Load next batch (previous one discarded)
└─ Works with any partition size!
```

#### **How Spark Actually Reads 10GB with 1.5GB Memory:**

```python
# Parquet file on disk: 10GB
# Stored in ROW GROUPS: 100 row groups × 100MB each

# Spark reads ONE ROW GROUP AT A TIME:

for i in range(100):  # 100 row groups
    batch = read_row_group(i)  # Read 100MB from disk

    # Process this batch
    for row in batch:
        bucket_id = hash(row.key) % 200
        shuffle_buffer[bucket_id].append(row)

        # When buffer reaches 50MB, flush it
        if len(shuffle_buffer[bucket_id]) > 50MB:
            write_to_disk(shuffle_buffer[bucket_id])
            shuffle_buffer[bucket_id].clear()  # FREE MEMORY!

    # Delete this batch from memory (critical!)
    del batch  # NOW MEMORY IS FREE FOR NEXT ROW GROUP

# Total memory used: 100MB (one row group) + 50MB (shuffle buffers) = 150MB
# NOT 10GB!
```

#### **Why Partition Count Doesn't Fix Memory Issues:**

```
CURRENT SETUP (10 partitions × 10GB):
├─ 10 executors working in parallel
├─ Each processes 1 partition
├─ Each reads partition in 100 batches (1.5GB execution memory total)
├─ Time: T minutes (all cores active)
└─ Parallelism: ✅ Maximum

PROPOSED FIX (100 partitions × 1GB):
├─ Still 10 executors working in parallel
├─ Each processes 10 partitions sequentially
├─ First round: 10 executors process partitions 1-10 → Time: T1
├─ Second round: 10 executors process partitions 11-20 → Time: T2
├─ ... repeat 10 times
├─ Total time: T1 + T2 + ... + T10 = ~10× T
└─ Parallelism: ❌ Lost (sequential waves instead of parallel)

RESULT: More partitions = SLOWER (sequential processing)
NOT: More partitions = fixes memory (memory was never the problem!)
```

#### **The KEY PRINCIPLE: Lazy Streaming Evaluation**

Spark doesn't load entire partitions into memory. Instead:

```
For each row group from disk:
1. DESERIALIZE (100MB) → temporary objects in memory
2. APPLY FILTERS (predicate pushdown) → optional, reduces data
3. TRANSFORM (map to shuffle buckets)
4. WRITE OUTPUT (flush 50MB buffers to disk)
5. DISCARD (delete row group objects, free memory)
6. REPEAT (load next row group)

Memory at any moment:
├─ Row group being processed: ~100MB
├─ Shuffle buffers: ~50MB
├─ Overhead: ~10MB
└─ Total: ~160MB (well within 1.5GB available!)
```

#### **Why Partition Size Still Matters (For Different Reasons)**

```
PARTITION SIZE affects PARALLELISM, not memory loading!

Goal: Keep all executors busy
├─ Too few partitions (fewer than executors): Some sit idle
├─ Too many partitions (more than executors): Sequential waves, slower
├─ Just right: As many partitions as executor cores

EXAMPLE: 100GB file, 10-executor cluster
├─ 10 partitions (10GB each) → 10 tasks → all busy → Good!
├─ 20 partitions (5GB each) → 20 tasks → 10 at a time, then 10 more → Still OK
├─ 100 partitions (1GB each) → 100 tasks → 10 at a time, then 10 more (10 times!)
└─ Memory per executor: Same! (row group size ~100MB, not partition size)
```

#### **The Real Constraint:**

```
Memory bottleneck is NOT partition size but ROW GROUP size!

Parquet file:
├─ Large row groups (256MB each) → Takes more memory to deserialize
├─ Small row groups (10MB each) → Takes less memory
└─ Spark default: ~128MB row groups

Partition count:
├─ CONTROLS: How many tasks run in parallel
├─ DOES NOT CONTROL: How much memory each task uses

Current setup (10 partitions):
├─ 10 tasks in parallel = 10 × 160MB = 1.6GB total
├─ (160MB = 100MB row group + 50MB shuffle + 10MB overhead)
└─ Fits in 10 × 4GB executor memory cluster! ✓
```

#### **Summary Table:**

| Question | Answer | Reason |
|----------|--------|--------|
| **How read 10GB with 1.5GB memory?** | Streaming row groups (100MB at a time) | Lazy evaluation |
| **Should we use 100 partitions instead of 10?** | NO - slower and more complex | Reduces parallelism |
| **What limits memory usage?** | Row group size (~100MB), not partition size | Spark deserializes one row group at a time |
| **What does partition count control?** | Parallelism (how many tasks run together) | More partitions = more potential parallel tasks |
| **Is 10GB input + 1.5GB memory a problem?** | NO - not a problem at all | Streaming solves it automatically |

**The fundamental insight:** Spark's lazy streaming evaluation means partition size ≠ memory requirement. You can process 100GB files with 1GB of memory, as long as you have disk for shuffle output!

---

### **How Does Spark Handle a Single 100GB File?**

**If you read a SINGLE 100GB file (not pre-split), Spark automatically partitions it based on block size.**

#### **Reading a Single 100GB Parquet File**

```python
df = spark.read.parquet("single_100gb_file.parquet")
print(df.rdd.getNumPartitions())  # How many partitions?

# Output: Depends on file format and block size
```

#### **How Spark Automatically Partitions:**

```
PARQUET FILES (most common):
├─ Parquet splits by ROW GROUPS (not file blocks)
├─ Default row group size: 128MB
├─ 100GB file ÷ 128MB per row group = ~780 partitions
├─ Each partition: 1 row group
└─ Result: 780 partitions running in parallel (across 10 executors)

CSV FILES:
├─ Spark can't split CSV row-by-row safely (each row might span lines)
├─ Default: Split by file size blocks (128MB or 256MB)
├─ 100GB file ÷ 128MB blocks = ~780 partitions
└─ Result: 780 partitions (but less efficient than Parquet)

ORC FILES:
├─ ORC also splits by stripes (similar to Parquet row groups)
├─ Default stripe size: 64MB
├─ 100GB file ÷ 64MB per stripe = ~1600 partitions
└─ Result: Many partitions (good for parallelism)
```

#### **Why NOT Just 10 Partitions?**

```
SCENARIO: 100GB single file read as 10 partitions
├─ Only 10 executors can work at once
├─ The other 10 cores on your cluster sit idle!
├─ Each executor processes 10GB sequentially
└─ Total processing time: Very slow (underutilized cluster)

REALITY: 100GB file → ~780 partitions (from row groups)
├─ 10 executors each grab multiple partitions
├─ Executor 1: Processes row groups 1, 11, 21, 31, ... (80 row groups)
├─ Executor 2: Processes row groups 2, 12, 22, 32, ... (80 row groups)
├─ ... all 10 executors working in parallel
├─ Each executor loads ONE row group at a time (128MB)
└─ Total time: Much faster (full parallelism!)
```

#### **Real Example: 100GB Parquet File**

```python
# Read a 100GB single parquet file
df = spark.read.parquet("s3://bucket/huge_file.parquet")

print(f"Partitions: {df.rdd.getNumPartitions()}")
# Output: 780 (if 100GB ÷ 128MB row group size)

# Now when you do a transformation:
df_filtered = df.filter(col("date") >= "2024-01-01")

# What happens:
# Task 1: Process row group 1 (128MB) on Executor 1
# Task 2: Process row group 2 (128MB) on Executor 2
# Task 3: Process row group 3 (128MB) on Executor 3
# ...
# Task 10: Process row group 10 (128MB) on Executor 10
# Task 11: Process row group 11 (128MB) on Executor 1 (when done with task 1)
# Task 12: Process row group 12 (128MB) on Executor 2 (when done with task 2)
# ... continues until all 780 row groups processed

# Memory per executor at any moment:
# ├─ Row group 1 (currently processing): 128MB
# ├─ Shuffle buffers (if groupBy later): 50MB
# └─ Total: ~180MB (well within 1.5GB!)
```

#### **Key Points:**

```
AUTOMATIC PARTITIONING BENEFITS:

Single 100GB file on disk:
├─ Parquet: ~780 partitions (128MB each)
├─ CSV: ~780 partitions (128MB blocks)
└─ ORC: ~1600 partitions (64MB each)

Executor memory needed:
├─ NOT: 100GB (impossible!)
├─ NOT: 10GB (if 10 partitions)
├─ BUT: 128MB × 1 row group (what it's currently processing)
└─ Per executor: ~180MB total (100% safe!)

Parallelism:
├─ With 10 executors: All stay busy (780 partitions ÷ 10 = 78 partitions each)
├─ With 100 executors: All stay busy (780 partitions ÷ 100 = 7-8 each)
└─ Full cluster utilization ✓
```

#### **You Don't Need to Manually Split!**

```python
# DON'T do this (unnecessary!):
df = spark.read.parquet("huge_file.parquet")
df_repartitioned = df.repartition(100)  # Adds expensive shuffle!

# DO this (automatic parallelism):
df = spark.read.parquet("huge_file.parquet")  # Already 780 partitions from file!
# Already maximum parallelism!

# Only repartition if you NEED different partition count:
df_repartitioned = df.repartition(200)  # If 780 is too many (overhead)
df_repartitioned = df.repartition(50)   # If too many small tasks
```

#### **To Check File Partitioning:**

```python
df = spark.read.parquet("huge_file.parquet")

# See partition count
print(f"Partitions: {df.rdd.getNumPartitions()}")  # ~780

# See partition sizes (in memory)
def get_partition_sizes(partition):
    size = 0
    count = 0
    for row in partition:
        size += 100  # rough estimate per row
        count += 1
    print(f"Partition: {count} rows, ~{size/1024/1024}MB")
    return partition

df.rdd.mapPartitions(get_partition_sizes).count()
```

**Bottom line:** When reading a single large file, **Spark automatically creates many partitions** (one per row group/block). You don't need to worry about memory - each executor only loads one row group at a time!


#### **The Key Insight: Executor Busy = Can't Store**

```
Timeline of Shuffle Write:

TIME 1: Processing Input Partition 1
├─ Executor memory: 600MB (processing partition 1)
├─ Shuffle buffers: 100MB (buffering output)
└─ Available: 800MB (for next partition)

TIME 2: Flush Shuffle Buffers to Disk
├─ Executor memory: 600MB (still processing partition 1)
├─ Write to disk: Copies 100MB to disk in background
├─ After write: Buffers freed → 100MB available again
└─ Executor continues: No memory conflict

What if we kept in memory instead?
TIME 2 (Alternative): Store in Memory
├─ Executor memory: 600MB (partition 1)
├─ Shuffle buffers: 100MB (partition 1 output)
├─ Time 3: Process partition 2
│  └─ Partition 2 data: 600MB
│  └─ Can't fit! (600 + 600 = 1200MB > 1.5GB execution memory)
│  └─ New partition 2 output also needs space
└─ Result: OOM ❌
```

#### **Summary: Why Disk is the Right Choice**

| Component | In-Memory Attempt | Disk Approach |
|-----------|-------------------|----------------|
| **Executor busy?** | Memory needed for processing | Disk writes don't block processing |
| **Multiple partitions?** | 10+ partitions = massive memory | Each executor writes progressively |
| **Read phase fetching?** | All data in memory → OOM | Fetches only what's needed |
| **Fault tolerance?** | Data lost if executor fails | Data persists on disk (replayable) |
| **Concurrent executors?** | 10 executors need 10 separate copies | Single copy on disk, all read from it |
| **Memory efficiency** | Requires 10× total data in cluster | Uses only temporary disk space |

**The real reason:** Shuffle data is **temporary and massive**. Disk is the only place to store it without blocking the executor from continuing work.


### Memory Usage During Shuffle: Write and Read Buffers

**See Section 04 - Memory Architecture** for complete executor memory breakdown (1.5GB execution, 1.5GB storage, 1GB reserved).

This section focuses on shuffle-specific buffers within execution memory:

#### Shuffle Write Buffer (Executor writes output files)

```
spark.shuffle.file.buffer (default 32KB per output partition):
├─ Example: 200 shuffle partitions × 32KB = 6.4MB total buffer per executor
├─ Purpose: Buffer records by destination partition before writing to disk
└─ Impact: More partitions = larger total buffer
```

#### Shuffle Read Buffer (Executor reads from remote executors)

```
spark.reducer.maxSizeInFlight (default 48MB):
├─ "In flight" = data being transferred over network but not yet processed
├─ Purpose: Prevent memory overflow while fetching from many write executors
└─ Back pressure: Pauses fetching when buffer full, resumes after processing
```

### Visual Example: 100 records, 4 output partitions

```
Original: [1,5,3,2,8,4,7,6,9,10, ...]
           ↓
Write Phase:
├─ Partition 0 (key%4==0): [4, 8, 12, ...]  → Sort & Write
├─ Partition 1 (key%4==1): [1, 5, 9, ...]   → Sort & Write
├─ Partition 2 (key%4==2): [2, 6, 10, ...]  → Sort & Write
└─ Partition 3 (key%4==3): [3, 7, 11, ...]  → Sort & Write
           ↓
Read Phase:
├─ Executor 0 fetches partition 0 data from all write executors
├─ Executor 1 fetches partition 1 data from all write executors
├─ Executor 2 fetches partition 2 data from all write executors
└─ Executor 3 fetches partition 3 data from all write executors
           ↓
Result: Each executor has all records for its partition, sorted by key
```

---

## Partition Sizing

### Default Shuffle Partitions

```python
# Spark default: 200 partitions for all wide operations
spark.conf.get("spark.sql.shuffle.partitions")  # Returns: "200"

# This is a ONE-SIZE-FITS-ALL default and causes problems:

# Small job example (10GB data):
# - 200 partitions × 50MB = 10GB total
# - Problem: Too many partitions for small data
# - Impact: 200 tasks in scheduler, 200 network fetches, extra overhead
# - Better: 50 partitions × 200MB = 10GB (fewer, larger, less overhead)

# Large job example (500GB data):
# - 200 partitions × 2.5GB = 500GB total
# - Problem: Partitions too large
# - Impact: Each partition needs 2.5GB in execution memory, likely spill
# - Better: 500 partitions × 1GB = 500GB (smaller partitions, fit in memory)

# RULE OF THUMB:
# - Target partition size: 128-256MB (fits in executor memory comfortably)
# - Too many partitions: >10 per executor core = scheduling overhead
# - Too few partitions: <1 per executor core = underutilization
```

### Choosing Optimal Partitions

```python
import math

total_data_size = 100 * 1024  # 100GB in MB
target_partition_size = 128    # 128MB per partition (ideal)

optimal_partitions = math.ceil(total_data_size / target_partition_size)
print(optimal_partitions)  # ~800 partitions

# But also consider practical constraints:

# 1. Executor cores available (avoid >2-4 partitions per core)
#    If 10 executors × 8 cores = 80 cores total
#    Can handle 160-320 partitions efficiently
#
# 2. Network bandwidth impact
#    Too many partitions = more files = more network metadata overhead
#    Too few partitions = fewer parallel transfers, underutilize network
#
# 3. Memory per executor
#    Fewer partitions = each executor processes larger partition in shuffle read
#    More partitions = each executor processes smaller partition (fits in memory better)
#
# 4. Task scheduling overhead
#    Each partition = 1 task in scheduler queue
#    1000 tasks = more scheduling overhead than 100 tasks
```

### Formula for Sizing

```python
# General formula:
num_shuffle_partitions = min(
    ceil(total_data / target_partition_size),  # Data-based
    num_executors * cores_per_executor * 2     # Executor-based
)

# Example: 500GB data, 10 executors (8 cores each)
data_based = ceil(500 * 1024 / 128)     # 4096 partitions
executor_based = 10 * 8 * 2             # 160 partitions
optimal = min(4096, 160)                # 160 partitions

# Result: 160 partitions = ~3.1GB per partition (manageable)
```

### Setting Shuffle Partitions

```python
# Session-wide configuration
spark.conf.set("spark.sql.shuffle.partitions", 100)

# Job-specific override
spark.conf.set("spark.sql.shuffle.partitions", 200)

# DataFrame-specific (less common)
df.repartition(150)  # Explicit repartition to 150
```

---

## Shuffle Tuning Parameters

### Key Parameters

```
spark.sql.shuffle.partitions (default 200)
├─ Number of output partitions for wide ops
├─ Rule: Max(executor cores × 2, data size / 128MB)
└─ Too high: Overhead; Too low: Memory spill

spark.shuffle.file.buffer (default 32KB)
├─ Buffer size per output partition during write
├─ Too high: Memory usage increases; Too low: Disk thrashing
└─ Typical: 32KB-1MB

spark.shuffle.compress (default true)
├─ Compress shuffle data on disk
├─ Tradeoff: 50% smaller files vs CPU cost
└─ Recommended: true (disk I/O is bottleneck)

spark.shuffle.spill.compress (default true)
├─ Compress spilled data to disk
├─ Similar tradeoff as shuffle.compress
└─ Recommended: true

spark.reducer.maxSizeInFlight (default 48MB)
├─ Max data BUFFERED IN MEMORY while fetching from remote executors
├─ "In flight" = data transferred over network but not yet processed
├─ How it works:
│  └─ Executor fetches partition data from 100 write executors
│  └─ Can buffer max 48MB of this incoming data
│  └─ Once buffer full, waits before fetching more (back pressure)
├─ Too high: Memory pressure, may cause spill
├─ Too low: Network underutilized, more round-trips, slower
└─ Typical: 48MB-100MB (10 executors) up to 256MB-512MB (50+ executors)

spark.shuffle.sort.bypassMergeThreshold (default 200)
├─ If num partitions < threshold, skip merge-sort
├─ Faster for small jobs
└─ Typical: 200
```

### Buffer Tuning Guide

#### When to Adjust Write Buffer (spark.shuffle.file.buffer)

```python
# DIAGNOSIS: Slow shuffle write or high disk I/O
# Symptom: Disk write latency, many small writes to disk

# Default: 32KB (good for most cases)
spark.conf.set("spark.shuffle.file.buffer", "32k")  # Default

# INCREASE to 64KB or 128KB if:
# ├─ You have many shuffle partitions (>1000)
# ├─ Disk I/O is the bottleneck (not memory)
# └─ You have plenty of memory available
spark.conf.set("spark.shuffle.file.buffer", "64k")

# DECREASE to 16KB if:
# ├─ Very tight memory constraints
# └─ Willing to trade disk I/O for memory savings
spark.conf.set("spark.shuffle.file.buffer", "16k")

# Memory cost calculation:
# buffer_cost = num_shuffle_partitions × buffer_size
# 200 partitions × 64KB = 12.8MB (acceptable)
# 5000 partitions × 64KB = 320MB (check if this causes memory issues!)
```

#### When to Adjust Read Buffer (spark.reducer.maxSizeInFlight)

```python
# DIAGNOSIS: Slow shuffle read or memory pressure during shuffle

# Rule of thumb:
# Small cluster (<=10 executors): 48MB-96MB
# Medium cluster (10-50 executors): 96MB-256MB
# Large cluster (>50 executors): 256MB-1GB

# Example 1: 5-executor cluster with 8 cores each
spark.conf.set("spark.reducer.maxSizeInFlight", "64m")

# Example 2: 20-executor cluster with 16 cores each
spark.conf.set("spark.reducer.maxSizeInFlight", "256m")

# Example 3: 100-executor cluster with 8 cores each
spark.conf.set("spark.reducer.maxSizeInFlight", "512m")

# INCREASE if:
# ├─ Network is the bottleneck (gaps in data transfer)
# ├─ You have plenty of memory (>8GB per executor)
# └─ Want to maximize network utilization
spark.conf.set("spark.reducer.maxSizeInFlight", "256m")  # From default 48m

# DECREASE if:
# ├─ Memory pressure (seeing spill messages)
# ├─ Executor OOM errors during shuffle
# └─ Running on memory-constrained cluster
spark.conf.set("spark.reducer.maxSizeInFlight", "24m")  # From default 48m
```

### Recommended Configuration

```python
# For general workloads (10 executors, 8 cores, 16GB RAM each)
spark.conf.set("spark.sql.shuffle.partitions", 150)
spark.conf.set("spark.shuffle.file.buffer", "64k")      # Balance write perf
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")  # Network efficient
spark.conf.set("spark.shuffle.compress", True)
spark.conf.set("spark.shuffle.spill.compress", True)

# For small jobs (< 10GB data)
spark.conf.set("spark.sql.shuffle.partitions", 30)
spark.conf.set("spark.shuffle.file.buffer", "32k")      # Less partitions, default OK
spark.conf.set("spark.reducer.maxSizeInFlight", "64m")  # Smaller footprint

# For large jobs (> 100GB data)
spark.conf.set("spark.sql.shuffle.partitions", 500)
spark.conf.set("spark.shuffle.file.buffer", "64k")      # More partitions, increase buffer
spark.conf.set("spark.reducer.maxSizeInFlight", "128m") # More data in flight OK

# For memory-constrained cluster (<4GB per executor)
spark.conf.set("spark.shuffle.file.buffer", "16k")      # Reduce write buffer
spark.conf.set("spark.reducer.maxSizeInFlight", "32m")  # Reduce network buffer
spark.conf.set("spark.sql.shuffle.partitions", 50)      # Fewer, safer partitions
```

### Concurrent Shuffle: Write and Read Happening Together

**Important Understanding:** Write and read phases don't happen strictly sequentially!

```
TIMELINE OF A LARGE SHUFFLE (simplified):

TIME 0-2s: WRITE PHASE (all write executors)
├─ Executor A: Processing partition 1 → buffering records → flush to disk
├─ Executor B: Processing partition 2 → buffering records → flush to disk
├─ Executor C: Processing partition 3 → buffering records → flush to disk
└─ Executor D: Processing partition 4 → buffering records → flush to disk

TIME 1-3s: EARLY READ PHASE (some read executors start fetching)
├─ Executor A (now as read executor): Starts fetching partition 1 data from A,B,C,D
│  └─ Reads into 48MB read buffer
├─ Executor B: Starts fetching partition 2 data from A,B,C,D
├─ Executor C: Starts fetching partition 3 data from A,B,C,D
└─ Executor D: Waits (still writing, hasn't started read phase yet)

TIME 2-4s: OVERLAP (write completing, read in progress)
├─ Executor A: Merging fetched partition 1 data
├─ Executor B: Merging fetched partition 2 data
├─ Executor C: Merging fetched partition 3 data
└─ Executor D: Finishing write phase, about to fetch partition 4

TIME 3-5s: READ PHASE (all read executors processing)
├─ All executors: Processing their assigned output partitions
├─ Memory: Read buffers now emptying as data processed
└─ Shuffle complete, ready for next stage

MEMORY PRESSURE TIMELINE:
├─ Time 0-2s: Write buffers use 6.4MB (manageable)
├─ Time 1-2s: Both write buffers + read buffers active!
│  └─ 6.4MB (write) + 48MB (read) = 54.4MB + execution memory
├─ Time 2-4s: Read buffers dominant
│  └─ 48MB read buffer + merge buffers + execution memory (peak pressure!)
└─ If total > 1.5GB execution memory: SPILL TO DISK
```

**Why This Matters:**
- Peak memory usage happens during overlap (write + read buffering simultaneously)
- Even if you have enough total memory, the TIMING of buffer fills can cause spill
- This is why tuning `spark.reducer.maxSizeInFlight` helps - prevents read buffer from growing too large during overlap period

---

## Real Examples

### Example 1: GroupBy Shuffle Optimization

```python
# Scenario: Aggregate 500GB sales data by store

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("GroupByOpt").getOrCreate()

# Configuration for this job
spark.conf.set("spark.sql.shuffle.partitions", 200)

# Read data
df = spark.read.parquet("s3://data-lake/sales/")  # 500GB
print(f"Partitions: {df.rdd.getNumPartitions()}")  # ~4000 (from file split)

# Problematic groupBy (4000 → 200 shuffle partitions)
result = df.groupBy("store_id").agg(
    F.sum("amount").alias("total_sales"),
    F.count("*").alias("transaction_count")
)
# Shuffle impact: 4000 partitions → 200 (aggressive reduction)
# Each partition: ~2.5GB (may cause spill!)

# Optimized: Pre-filter, then groupBy
spark.conf.set("spark.sql.shuffle.partitions", 150)

df_filtered = df.filter(
    (df.date >= "2024-01-01") & (df.amount > 0)
).coalesce(800)  # Reduce from 4000 to 800 partitions

result = df_filtered.groupBy("store_id").agg(
    F.sum("amount").alias("total_sales"),
    F.count("*").alias("transaction_count")
)
# Shuffle impact: 800 partitions → 150
# Each partition: ~300MB (safe!)

# Write result
result.write.mode("overwrite").parquet("s3://output/daily_summary/")
```

### Example 2: Join Shuffle Optimization

```python
# Scenario: Join large transaction log with product catalog

from pyspark.sql.functions import broadcast

# Read datasets
transactions = spark.read.parquet("s3://data/transactions/")  # 100GB
products = spark.read.parquet("s3://data/products/")          # 500MB

# Check sizes
print(f"Transactions: {transactions.count()} rows")  # 1B
print(f"Products: {products.count()} rows")         # 100K

# Approach 1: Without broadcast (both shuffle)
spark.conf.set("spark.sql.shuffle.partitions", 200)

result = transactions.join(products, "product_id")
# Shuffle: Both tables → 200 partitions
# Data moved: 100GB + 500MB = 100.5GB (expensive!)

# Approach 2: With broadcast (only one shuffles)
result = transactions.join(broadcast(products), "product_id")
# Shuffle: Only transactions → 200 partitions
# Data moved: 100GB (50% reduction!)
# Products cached on each executor (~500MB per executor)
# Benefit: Faster join, less memory spill

# Approach 3: If products were large (10GB)
spark.conf.set("spark.sql.shuffle.partitions", 100)

# Repartition both to same count to enable co-partitioned join
products_repart = products.repartition(100)  # Shuffle 1: redistribute products
transactions_repart = transactions.repartition(100)  # Shuffle 2: redistribute transactions

result = transactions_repart.join(products_repart, "product_id")
# Both have 100 partitions with SAME key distribution
# Join happens locally per partition (no additional shuffle before join)
# Still has 2 shuffles (to repartition) + join execution
# Benefit: Joins data that's already co-located, avoiding 3rd shuffle
```

### Example 3: Handling Shuffle Skew

```python
# Scenario: Some keys have 100x more data than others
# E.g., "USA" state has 100B records, others have 1B

df = spark.read.parquet("s3://data/events/")

# Problem: Uneven distribution after shuffle
# Partition 1: "USA" data (100B records)
# Partition 2: "UK" data (1B records)
# Partition 3: "DE" data (1B records)
# → Partition 1 executor needs 100GB memory, others need 1GB (imbalance!)

# Solution 1: Separate hot keys
df_hot = df.filter(df.country == "USA").repartition(50)
df_cold = df.filter(df.country != "USA")

result_hot = df_hot.groupBy("state").agg(F.sum("value"))
result_cold = df_cold.groupBy("country").agg(F.sum("value"))

result = result_hot.union(result_cold)

# Solution 2: Salt hot keys (add random suffix to split hot key across partitions)
from pyspark.sql.functions import when, col, rand, concat, lit

# Original: "USA" → all 100B records → 1 partition → 1 executor needs 100GB memory
# Problem: Single executor overloaded, others idle

# Salting: Add random suffix to "USA" records
df_salted = df.withColumn(
    "key_salted",
    when(col("country") == "USA",
         concat(col("state"), lit("_"), (rand() * 50).cast("int")))  # USA_0 ... USA_49
    .otherwise(col("country"))  # UK, DE, etc. stay same
)

result = df_salted.groupBy("key_salted").agg(F.sum("value"))

# Result after shuffle:
# - "USA_0" → partition 0 → 2B records → 1 executor (manageable)
# - "USA_1" → partition 1 → 2B records → 1 executor (manageable)
# - ... up to "USA_49"
# - "UK" → partition 50 → 1B records → 1 executor
# - "DE" → partition 51 → 1B records → 1 executor
#
# ADVANTAGE: Original 100B-to-1B ratio (100x imbalance) becomes 2B-to-1B (2x imbalance)
# Much more balanced load across executors!
#
# TRADEOFF: Need extra groupBy aggregation to combine back:
# result_final = result.groupBy("state").agg(F.sum("value"))
# This combines USA_0, USA_1, ..., USA_49 back into "USA" totals

# Solution 3: Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 500)  # Instead of 200
result = df.groupBy("country").agg(F.sum("value"))
# More partitions = data spread thinner
# But more overhead (use sparingly)
```

### Example 4: Optimal Shuffle Partition Sizing

```python
# Scenario: Multiple jobs, different data sizes
# We want to find optimal shuffle partitions for each

def calculate_optimal_partitions(
    total_data_gb,
    num_executors,
    cores_per_executor,
    target_partition_mb=128
):
    """Calculate optimal shuffle partitions"""
    import math

    # Data-based: How many partitions to fit target size
    total_mb = total_data_gb * 1024
    data_based = math.ceil(total_mb / target_partition_mb)

    # Executor-based: Avoid too many per executor
    executor_based = num_executors * cores_per_executor * 2

    # Final: Use conservative estimate
    optimal = max(
        30,  # Minimum
        min(data_based, executor_based)
    )

    return optimal

# Example 1: 100GB data, 5 executors, 8 cores each
opt1 = calculate_optimal_partitions(100, 5, 8)
print(f"100GB data: {opt1} partitions")  # 800

# Example 2: 10GB data, 5 executors, 8 cores each
opt2 = calculate_optimal_partitions(10, 5, 8)
print(f"10GB data: {opt2} partitions")  # 80

# Example 3: 500GB data, 20 executors, 8 cores each
opt3 = calculate_optimal_partitions(500, 20, 8)
print(f"500GB data: {opt3} partitions")  # 320 (limited by executor-based)

# Use in Spark
spark.conf.set("spark.sql.shuffle.partitions", opt1)
```

---

### **Direct Answer: Why Write to Disk Instead of Memory?**

**Original question from Phase 1:** "Why write to disk? Can't we keep shuffle output in executor or storage memory?"

**The complete answer (with real numbers from a 100GB file):**

```
SCENARIO: 100GB Parquet file → 780 partitions → 1 executor processes 78 partitions

Executor Memory Breakdown (4GB total):
├─ EXECUTION memory: 1.5GB ← SHARED across ALL cores in this executor!
│  └─ This 1.5GB is divided dynamically among parallel tasks
│  └─ Multiple cores = multiple tasks = share the same 1.5GB pool
├─ Storage memory: 1.5GB (for caching, also shared)
└─ Reserved: 1GB (OS, GC, overhead)

Each partition requires:
├─ Input: 128MB (1 row group from disk, loaded into execution memory temporarily)
├─ Processing: Generate outputs for ALL 200 shuffle partitions
└─ Output: ~128MB (reorganized data, same total size, stored in execution memory)

Timeline of what executor does:
├─ Time 1: Load partition 1 (128MB) → Generate 200 shuffle bucket outputs → ?
├─ Time 2: Load partition 11 (128MB) → Generate 200 shuffle bucket outputs → ?
├─ Time 3: Load partition 21 (128MB) → Generate 200 shuffle bucket outputs → ?
├─ ... 75 more partitions to process
└─ Total: 78 partitions × 128MB = 10GB worth of outputs!

WHERE TO PUT THE OUTPUTS?

OPTION 1: Execution Memory (1.5GB - the memory for transformations)
├─ Partition 1 outputs: 128MB ÷ 1.5GB = 8.5% of EXECUTION MEMORY
├─ Partition 11 outputs: 128MB ÷ 1.5GB = 8.5% of EXECUTION MEMORY → 17% total
├─ Partition 21 outputs: 128MB ÷ 1.5GB = 8.5% of EXECUTION MEMORY → 25% total
├─ ... keep adding partitions ...
├─ Partition 121 outputs: 128MB × 10 = 1.28GB = 85% → ALMOST FULL
├─ Partition 131 outputs: 128MB × 11 = 1.4GB = 93% → VERY FULL
├─ Partition 141 outputs: 128MB × 12 = 1.536GB = 102% → ❌ OOM! EXCEEDS 1.5GB!
└─ Result: Fails after processing ~12 partitions (out of 78), never finishes!

OPTION 2: Storage Memory (1.5GB for caching)
├─ Designed for: Persistent df.cache() across multiple operations
├─ Shuffle outputs are: Temporary, one-time use, then discarded
├─ If we use storage for shuffle:
│  ├─ Wastes 1.5GB on temporary data
│  ├─ Prevents user from caching permanent dataframes
│  ├─ Architectural mismatch (mixing temporary + permanent data)
│  └─ Defeats the purpose of having separate memory pools
└─ Result: Wrong memory type, architectural failure ❌

OPTION 3: Local Disk (Fast SSD, unlimited storage)
├─ Partition 1 outputs: Write to disk → 128MB on disk ✓ MEMORY FREED
├─ Partition 11 outputs: Write to disk → 128MB on disk ✓ MEMORY FREED
├─ Partition 21 outputs: Write to disk → 128MB on disk ✓ MEMORY FREED
├─ ... continue for all 78 partitions
├─ Total on disk: 10GB (temporary, auto-cleaned after shuffle)
├─ Memory at any time: Only 1 partition (~150MB) ✓ Always safe
└─ Result: SUCCESS - Process all 78 partitions! ✓✓✓
```

**Why this is the ONLY practical solution:**

```
Memory usage patterns (EXECUTION MEMORY only = 1.5GB):

WITH MEMORY STORAGE (fails - trying to hold all outputs):
├─ Partition 1 output: 128MB (8.5% of execution memory)
├─ Partition 11 output: 128MB (8.5% of execution memory) → 17% total used
├─ Partition 21 output: 128MB (8.5% of execution memory) → 25% total used
├─ Partition 31 output: 128MB (8.5% of execution memory) → 34% total used
├─ Partition 51 output: 128MB (8.5% of execution memory) → 51% total used
├─ Partition 71 output: 128MB (8.5% of execution memory) → 59% total used
├─ Partition 91 output: 128MB (8.5% of execution memory) → 68% total used
├─ Partition 111 output: 128MB (8.5% of execution memory) → 76% total used
├─ Partition 121 output: 128MB (8.5% of execution memory) → 85% total used
├─ Partition 131 output: 128MB (8.5% of execution memory) → 93% total used
├─ Partition 141 output: Would be 128MB + previous = 102% of execution memory
└─ FAIL: Out of memory after 12 partitions, can't process remaining 66 partitions!

WITH DISK STORAGE (works - write each output and free memory):
├─ Partition 1 output: Write 128MB to disk → Free from memory ✓
├─ Partition 11 output: Write 128MB to disk → Free from memory ✓
├─ Partition 21 output: Write 128MB to disk → Free from memory ✓
├─ Partition 31 output: Write 128MB to disk → Free from memory ✓
│  ... keep writing outputs and freeing memory ...
├─ Partition 751 output: Write 128MB to disk → Free from memory ✓
├─ Partition 778 (last): Write 128MB to disk → Free from memory ✓
│
└─ SUCCESS: Process all 78 partitions!
   ├─ Total written to disk: 78 × 128MB = 10GB (temporary)
   ├─ Memory never exceeds: 128MB + 50MB overhead = ~180MB at any moment
   └─ All within 1.5GB execution memory limit ✓✓✓
```

**The fundamental principle:**

Executors process partitions **sequentially**, but shuffle outputs must persist until the read phase begins. This creates a fundamental conflict:
- Executor memory too small to hold outputs from all sequential partitions
- Storage memory designed for persistent data, not temporary shuffle
- Local disk is the only place large enough for temporary outputs

This is why **all major distributed systems** (Spark, Hadoop, Flink) use disk-based shuffle!
```

---

## Shuffle Monitoring Checklist

```
□ Check shuffle write/read sizes in Spark UI
□ Compare to expected data size
□ Look for skewed partition sizes
□ Monitor disk I/O during shuffle
□ Check for memory spill messages
□ Measure job execution time

□ Adjust spark.sql.shuffle.partitions
□ Consider pre-filtering or pre-sorting
□ Use broadcast for small tables
□ Investigate skewed keys
□ Compress shuffle data
```

---

---

### **How Many Cores? Is 1.5GB Shared by All Cores?**

**YES - The 1.5GB execution memory is SHARED across ALL cores in that executor, not divided per core.**

#### **Example: Executor with 8 Cores**

```
Executor Instance (4GB total memory):
├─ 8 CPU cores (running in parallel)
├─ EXECUTION MEMORY: 1.5GB (SHARED across all 8 cores)
├─ STORAGE MEMORY: 1.5GB (SHARED across all 8 cores)
└─ RESERVED: 1GB

Running tasks in parallel:
├─ Core 1: Running Task 1 (processing partition 1)
├─ Core 2: Running Task 2 (processing partition 11)
├─ Core 3: Running Task 3 (processing partition 21)
├─ Core 4: Running Task 4 (processing partition 31)
├─ Core 5: Running Task 5 (processing partition 41)
├─ Core 6: Running Task 6 (processing partition 51)
├─ Core 7: Running Task 7 (processing partition 61)
└─ Core 8: Running Task 8 (processing partition 71)

Execution Memory Usage (shared pool):
├─ Task 1: Uses ~150MB for partition 1 data + shuffle buffer
├─ Task 2: Uses ~150MB for partition 11 data + shuffle buffer
├─ Task 3: Uses ~150MB for partition 21 data + shuffle buffer
├─ ...
├─ Task 8: Uses ~150MB for partition 71 data + shuffle buffer
│
├─ Total: 8 tasks × 150MB = 1.2GB
├─ Available: 1.5GB
└─ Headroom: 0.3GB remaining in shared pool ✓
```

#### **How Execution Memory is Allocated**

```
DYNAMIC ALLOCATION (Spark's approach):

Execution Memory Pool: 1.5GB (shared)
└─ Per-task memory limit: 1.5GB ÷ 8 cores = 187.5MB max per task

When tasks run:
├─ Task 1 requests memory for partition 1: Gets 150MB from pool
├─ Task 2 requests memory for partition 11: Gets 150MB from pool
├─ Task 3 requests memory for partition 21: Gets 150MB from pool
├─ Task 4 requests memory for partition 31: Gets 150MB from pool
├─ ...
├─ Task 8 requests memory for partition 71: Gets 150MB from pool
│
├─ Total allocated: 8 × 150MB = 1.2GB
├─ Pool remaining: 1.5GB - 1.2GB = 0.3GB
└─ All tasks happy, no spill ✓

If any task tries to exceed its share:
├─ Task 5 needs more than 187.5MB for partition 41
├─ Pool would overflow: 1.2GB + extra > 1.5GB
├─ Result: Spill to disk (write to temporary storage)
└─ Task 5 continues (slower due to disk I/O)
```

#### **Real Scenario: 8-Core Executor Processing 78 Partitions**

```
Wave 1 (T=0-1s): Tasks 1-8 run in parallel
├─ Task 1: Processes partition 1 → 150MB memory
├─ Task 2: Processes partition 11 → 150MB memory
├─ Task 3: Processes partition 21 → 150MB memory
├─ Task 4: Processes partition 31 → 150MB memory
├─ Task 5: Processes partition 41 → 150MB memory
├─ Task 6: Processes partition 51 → 150MB memory
├─ Task 7: Processes partition 61 → 150MB memory
├─ Task 8: Processes partition 71 → 150MB memory
└─ Total: 1.2GB execution memory used (safe!)

Wave 2 (T=1-2s): Tasks 9-16 run in parallel
├─ Task 9: Processes partition 81 → 150MB memory
├─ Task 10: Processes partition 91 → 150MB memory
├─ ... and so on
└─ Total: 1.2GB execution memory used (safe!)

Wave 10 (T=9-10s): Last tasks run
├─ Task 73: Processes partition 721
├─ Task 74: Processes partition 731
├─ Task 75: Processes partition 741
├─ Task 76: Processes partition 751
├─ Task 77: Processes partition 761
├─ Task 78: Processes partition 771
├─ Tasks 79-80: Sit idle (only 78 partitions total)
└─ All completed successfully!
```

#### **Key Points About Shared Memory**

```
Execution Memory is NOT divided:
├─ NOT: 1.5GB ÷ 8 cores = 187.5MB per core (WRONG!)
└─ YES: 8 tasks share 1.5GB dynamically (CORRECT!)

Per-task limits exist to prevent monopoly:
├─ Each task can use at most ~187.5MB (1.5GB ÷ 8)
├─ Prevents one task from using entire 1.5GB
├─ Allows fair sharing among tasks
└─ If exceeded: Task spills to disk

Multiple cores = better parallelism:
├─ 8 cores: 8 tasks in parallel
├─ 4 cores: 4 tasks in parallel
├─ Fewer cores = longer total processing time (more waves)
├─ More cores = faster total processing (fewer waves)
└─ Memory per executor stays same (1.5GB shared)
```

#### **Comparison: Different Executor Configurations**

```
SCENARIO: Same 100GB file, same 780 partitions

Configuration 1: 1 executor, 8 cores
├─ Cores: 8
├─ Execution memory: 1.5GB (shared)
├─ Tasks in parallel: 8
├─ Partitions per executor: 78 ÷ 8 = ~10 per task
├─ Waves needed: 10
├─ Total time: ~10 minutes
└─ Memory per executor: 1.5GB

Configuration 2: 2 executors, 4 cores each
├─ Total cores: 8 (same as above)
├─ Execution memory per executor: 1.5GB (shared among 4 cores)
├─ Tasks in parallel: 4 + 4 = 8 total
├─ Partitions per executor: 39 (78 ÷ 2)
├─ Waves per executor: 10
├─ Total time: ~10 minutes (similar!)
└─ Memory per executor: 1.5GB × 2 = 3GB total

Configuration 3: 10 executors, 1 core each
├─ Total cores: 10
├─ Execution memory per executor: 1.5GB (just 1 task)
├─ Tasks in parallel: 10
├─ Partitions per executor: 8 (78 ÷ 10)
├─ Waves per executor: 8
├─ Total time: ~8 minutes (faster!)
└─ Memory per executor: 1.5GB × 10 = 15GB total (more expensive!)

RULE: More cores/executors = faster (parallel), but costs more memory
```

**Bottom line:** Execution memory (1.5GB) is **SHARED across all cores** in that executor, not divided per core. Multiple cores allow multiple tasks to run in parallel from the same memory pool.

---

## Key Takeaways

✅ **Shuffle moves data between executors** - Expensive operation
✅ **Partition size matters** - Too many = overhead, too few = memory spill
✅ **Default 200 partitions** - Often wrong (too many for small jobs, too few for large)
✅ **Broadcast small tables** - Avoid shuffle entirely
✅ **Detect and handle skew** - Separate hot keys or salt them
✅ **Monitor shuffle metrics** - Watch Spark UI for signs of problems

---

## Next Steps

1. **Check your shuffle partitions** - Are they optimal?
2. **Adjust for your workload** - Small jobs need fewer, large jobs need more
3. **Monitor and measure** - Before/after execution times
4. **Move to Section 06** - Join strategies in detail

---

**Remember:** Shuffle is expensive. Every optimization here saves seconds/minutes per job × hundreds of jobs = huge production impact!
