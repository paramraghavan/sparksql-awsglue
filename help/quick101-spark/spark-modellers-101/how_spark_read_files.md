# How PySpark Reads Files - From 500GB to Your Executors

When PySpark encounters a large file like a 500GB dataset, it doesn't try to load it all into one executor. Instead, it intelligently splits the file and distributes the work across your cluster. Here's how it works at every level.

---

## 🟢 BEGINNER VERSION

**Simple Answer**: When PySpark reads a 500GB file, it **breaks it into smaller pieces** (like cutting a pizza into slices) so multiple workers can process it at the same time.

```
500GB File
    ↓
Split into pieces (~128MB each)
    ↓
Multiple executors work on pieces simultaneously
    ↓
Results combined
```

**Why?** One executor can't handle 500GB, and you want all your executors working in parallel, not sitting idle.

---

## 🟡 INTERMEDIATE VERSION

### How the Splitting Works

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

# Read 500GB file
df = spark.read.text("s3://bucket/huge_file.txt")

# PySpark automatically creates ~4,000 partitions (500GB ÷ 128MB)
print(df.rdd.getNumPartitions())  # Output: ~4000

# Each partition is processed by one executor
# If you have 10 executors, each does ~400 partitions in sequence
```

### Key Points

- **Default split size** = 128MB (configurable)
- **500GB file** = ~4,000 partitions
- **If you have 10 executors** = each executor handles ~400 tasks
- **Partitions ≠ Executors** (many partitions, fewer executors)

### Partition vs. Executor

Think of it like a restaurant:
- **Partitions** = Orders in the queue
- **Executors** = Chefs working
- You can have 1,000 orders but only 10 chefs. Each chef works on the next order when ready.

---

## 🔴 ADVANCED VERSION

### Deep Dive into Split Decision Logic

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

# 1. CHECK DEFAULT SPLIT SIZE
split_size = spark.conf.get("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize")
print(f"Current split size: {split_size}")  # Default: 128MB or 256MB

# 2. CHANGE SPLIT SIZE
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "256m")

# Now same file has fewer, larger partitions
df = spark.read.parquet("s3://bucket/huge_file.parquet")
print(df.rdd.getNumPartitions())  # Fewer partitions now

# 3. UNDERSTAND PARTITION PLACEMENT (LOCALITY)
# For HDFS: PySpark schedules tasks on nodes containing the data blocks
# For S3: Partitions don't have locality (data is remote)

# 4. MANUALLY OVERRIDE PARTITIONS
df = spark.read.text("s3://bucket/huge_file.txt")
df_repartitioned = df.repartition(100)  # Force 100 partitions
print(df_repartitioned.rdd.getNumPartitions())  # Output: 100
```

### Split Decision Matrix

| Scenario | Split Size | Partitions | Best For |
|----------|-----------|-----------|----------|
| HDFS file, many executors | 128MB | Many | Parallelism |
| Remote file (S3), fewer executors | 256MB-1GB | Fewer | Network efficiency |
| Skewed data | Custom | Variable | Load balancing |

### Advanced Concepts

#### 1. InputFormat Classes

Different file types use different splitters:

- **Text files** → `TextInputFormat` → byte-aligned splits
- **Parquet** → `ParquetInputFormat` → row group aware
- **Avro** → `AvroInputFormat` → block aware

```python
# Example: Parquet is smarter about splits
df_parquet = spark.read.parquet("s3://bucket/file.parquet")
# Splits align with Parquet row groups (more efficient)

df_text = spark.read.text("s3://bucket/file.txt")
# Splits are just byte-based (generic splitting)
```

#### 2. Locality Calculation

```python
# PySpark prefers HDFS node locality
# If block is on nodes A, B, C with IP 10.0.0.1-3
# Schedule task on those nodes first

# For S3 (no locality), all nodes equally distant
# Use coalesce() to reduce shuffle cost
df = df.coalesce(50)  # Reduce partitions, fewer network transfers
```

#### 3. Min/Max Split Size Configuration

```python
# Minimum split size prevents too many partitions
spark.conf.set("mapreduce.input.fileinputformat.split.minsize", "32m")

# Maximum split size prevents too few partitions
spark.conf.set("mapreduce.input.fileinputformat.split.maxsize", "512m")

# Example: 500GB file with 512MB max split size
# 500GB ÷ 512MB = ~976 partitions
```

---

## Quick Reference Table

| Question | Beginner | Advanced |
|----------|----------|----------|
| **Does it split?** | Yes, always | Yes, via InputFormat.getSplits() |
| **Default split size?** | ~128MB | Config: `mapreduce.input.fileinputformat.split.maxsize` |
| **Who decides partitions?** | PySpark | Hadoop InputFormat + file size + split config |
| **Can I change it?** | Yes, `repartition(n)` | Yes, config + coalesce() + custom InputFormat |
| **Why split?** | Parallelism | Locality + parallelism + memory efficiency |

---

## Common Scenarios & Solutions

### Scenario 1: File is Too Split Up (Too Many Partitions)

**Problem**: 500GB file creates 4,000 partitions, causing too much overhead

```python
df = spark.read.text("s3://bucket/huge_file.txt")
# Reduce partitions without shuffling
df = df.coalesce(200)  # Reduce to 200 partitions
```

### Scenario 2: Want to Parallelize More

**Problem**: File creates only 10 partitions, executors are idle

```python
df = spark.read.text("s3://bucket/file.txt")
# Increase partitions (causes shuffle)
df = df.repartition(100)
```

### Scenario 3: Optimize for Network (Cloud Storage)

**Problem**: Smaller splits = more network calls to S3

```python
# Increase split size for cloud storage
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "256m")

df = spark.read.parquet("s3://bucket/huge_file.parquet")
# Now ~1,953 partitions instead of 3,906
```

### Scenario 4: Understand Your Current Partitions

```python
df = spark.read.csv("s3://bucket/data.csv")

# Check partition info
num_partitions = df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

# See partition sizes (rough estimate)
row_count = df.count()
print(f"Average rows per partition: {row_count / num_partitions}")
```

---

## Key Takeaways

1. **PySpark always splits files** - It never assigns a 500GB file to one executor
2. **Default split is ~128MB** - Configurable based on your needs
3. **Partitions ≠ Executors** - Many partitions, few executors; tasks queue up
4. **Locality matters for HDFS** - Less relevant for cloud storage like S3
5. **Fine-tune based on your cluster** - More executors → more splits beneficial; Fewer executors → larger splits better

---

## When to Adjust Split Size

**Increase split size if:**
- Using cloud storage (S3, GCS) → fewer network round trips
- Have few executors → reduce overhead
- Data processing is I/O bound

**Decrease split size if:**
- Have many executors available → better parallelism
- Data processing is CPU bound
- Need fine-grained load balancing
