# 02 - Python for Big Data

## Why Python Matters for PySpark

You might think: *"Spark handles the big data, not Python!"*

**Reality:** Python code runs on EVERY partition in EVERY executor. Small inefficiencies multiply by 1000s of partitions.

**Example:**
- Inefficient Python: 1 second per partition × 1000 partitions = 1000 seconds (17 minutes!)
- Optimized Python: 0.1 second per partition × 1000 partitions = 100 seconds (1.7 minutes!)

---

## Table of Contents
1. [Data Structures for Performance](#data-structures-for-performance)
2. [Lambda Functions & Comprehensions](#lambda-functions--comprehensions)
3. [Memory-Aware Coding](#memory-aware-coding)
4. [Exception Handling in Production](#exception-handling-in-production)
5. [Performance Tips](#performance-tips)
6. [Real Examples](#real-examples)

---

## Data Structures for Performance

### Lists vs Dictionaries vs Sets

🔗 **Also see:** Section 04 - Memory Spill | Section 05 - Shuffle Optimization

```python
# SLOW: List lookup is O(n) - searches entire list
data_list = [1, 2, 3, 4, 5, 100000]
if 100000 in data_list:  # Has to check all 6 items
    pass

# FAST: Dictionary/Set lookup is O(1)
data_set = {1, 2, 3, 4, 5, 100000}
if 100000 in data_set:  # Instant lookup!
    pass

# Real impact in PySpark:
def filter_valid_ids(partition, valid_ids_list):  # Bad
    for record in partition:
        if record.id in valid_ids_list:  # O(n) per record!
            yield record

def filter_valid_ids(partition, valid_ids_set):  # Good
    for record in partition:
        if record.id in valid_ids_set:  # O(1) per record!
            yield record
```

### When to Use What

```python
# Use LIST when:
# - Order matters
# - You'll iterate through it
data = [1, 2, 3, 4, 5]

# Use DICTIONARY when:
# - Need key→value mapping
# - Fast lookups needed
config = {"timeout": 30, "retries": 3}

# Use SET when:
# - Checking membership (in)
# - Removing duplicates
# - Set operations (union, intersection)
valid_statuses = {"ACTIVE", "PENDING", "COMPLETED"}

# Use TUPLE when:
# - Immutable (safe in distributed context)
# - Using as dictionary key or in set
config_tuple = ("prod", "us-west-2")
```

### Real Example: Dimension Table Lookup

```python
# Scenario: Map product IDs to category names
# Need to look up category for each transaction

# Version 1: List (BAD) - O(n) lookup per transaction
categories_list = [
    {"id": 1, "name": "Electronics"},
    {"id": 2, "name": "Clothing"},
    # ... 100,000 more items
]

def map_category(partition):
    for txn in partition:
        # Searches entire list!
        for cat in categories_list:
            if cat["id"] == txn.product_id:
                yield txn.product_id, cat["name"]
                break

# Version 2: Dictionary (GOOD) - O(1) lookup per transaction
categories_dict = {
    1: "Electronics",
    2: "Clothing",
    # ... 100,000 more items
}

def map_category(partition):
    for txn in partition:
        # Instant lookup!
        category = categories_dict.get(txn.product_id, "Unknown")
        yield txn.product_id, category
```

---

## Lambda Functions & Comprehensions

### Lambda for Simple Operations

```python
# Good: Simple transformation
numbers = [1, 2, 3, 4, 5]
squared = map(lambda x: x ** 2, numbers)

# Bad: Complex logic in lambda (hard to read, debug, test)
result = map(lambda x: compute_complex_transformation(x) if validate(x) else handle_error(x), numbers)

# Better: Extract to named function
def compute_if_valid(x):
    if validate(x):
        return compute_complex_transformation(x)
    else:
        return handle_error(x)

result = map(compute_if_valid, numbers)
```

### List Comprehensions (Faster than map/filter)

```python
# Map version
squared = list(map(lambda x: x ** 2, range(1000)))

# Comprehension (20-30% faster!)
squared = [x ** 2 for x in range(1000)]

# With filter
even_squares = [x ** 2 for x in range(1000) if x % 2 == 0]

# Nested comprehension
matrix = [[1, 2], [3, 4]]
flattened = [x for row in matrix for x in row]  # [1, 2, 3, 4]
```

### In PySpark UDFs

```python
# UDF = User Defined Function (Python code on executors)

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType

# Simple UDF
@udf(IntegerType())
def multiply_by_two(x):
    return x * 2 if x is not None else 0

df_with_doubled = df.withColumn("doubled", multiply_by_two(df.value))

# Better: Vectorized UDF (10-100x faster)
import pandas as pd

@pandas_udf(IntegerType())
def multiply_by_two_vectorized(s: pd.Series) -> pd.Series:
    return s * 2

df_with_doubled = df.withColumn("doubled", multiply_by_two_vectorized(df.value))
```

---

## Memory-Aware Coding

### Understanding Executor Memory

```python
# Each executor: 4GB memory (typical)
# Breakdown:
# - 1.5GB: Execution memory (transformations)
# - 1.5GB: Storage memory (caching)
# - 1GB: Reserved for Python/OS

# If your Python function allocates >1.5GB on one partition:
# OutOfMemory error!
```

### Memory-Efficient Patterns

```python
# BAD: Collect entire partition into memory
def process_partition(partition):
    data = list(partition)  # Loads entire partition (500MB+!)
    result = expensive_operation(data)
    return result

# GOOD: Stream through partition
def process_partition(partition):
    for record in partition:
        yield process_record(record)  # One record at a time

# BAD: Create large intermediate lists
def summarize(partition):
    all_data = []
    for record in partition:
        all_data.append(transform(record))  # Grows indefinitely
    return all_data

# GOOD: Use generators or streaming
def summarize(partition):
    count = 0
    total = 0
    for record in partition:
        total += record.amount
        count += 1
    return count, total
```

### String Operations Impact

```python
# BAD: String concatenation in loop
def build_message(records):
    msg = ""
    for record in records:
        msg += str(record.id) + "," + str(record.name) + "\n"
    return msg

# GOOD: Use list + join (100x faster!)
def build_message(records):
    parts = []
    for record in records:
        parts.append(f"{record.id},{record.name}")
    return "\n".join(parts)
```

---

## Exception Handling in Production

### Try-Except in PySpark

```python
# Basic pattern
def safe_transform(value):
    try:
        return int(value) * 2
    except ValueError:
        return 0  # Default for invalid input

# In UDF
@udf(IntegerType())
def safe_multiply(x):
    try:
        return int(x) * 2
    except (ValueError, TypeError):
        return None  # NULL in SQL

df_result = df.withColumn("result", safe_multiply(df.input))
```

### Partition-Level Exception Handling

```python
def process_partition_safely(partition):
    """Process partition with proper error handling"""
    processed = 0
    errors = 0
    error_records = []

    for record in partition:
        try:
            # Transform
            result = complex_transform(record)
            processed += 1
            yield result
        except ValueError as e:
            errors += 1
            error_records.append((record.id, str(e)))
            # Log error but continue processing
            continue
        except Exception as e:
            errors += 1
            # Re-raise unexpected errors
            raise RuntimeError(f"Unexpected error at record {record.id}: {e}")

    # Log summary
    print(f"Partition: processed={processed}, errors={errors}")
    if error_records:
        print(f"Failed records: {error_records[:10]}")  # Show first 10

# Use in UDF
rdd = df.rdd.mapPartitions(process_partition_safely)
```

### Logging in Distributed Context

```python
import logging

# Setup logging (ONCE at driver)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_with_logging(partition):
    logger.info("Starting partition processing")

    for i, record in enumerate(partition):
        try:
            if i % 10000 == 0:  # Log every 10K records
                logger.info(f"Processed {i} records")

            yield transform(record)
        except Exception as e:
            logger.error(f"Error at record {i}: {e}")
            raise

# Use it
rdd = df.rdd.mapPartitions(process_with_logging)
```

---

## Performance Tips

### 1. Avoid Collect() on Large Data

```python
# BAD: Brings entire result to driver
result = df.filter(df.amount > 1000).collect()  # If result is 100GB, crashes!

# GOOD: Use iterators or write to storage
result = df.filter(df.amount > 1000).toLocalIterator()  # One partition at a time
for batch in result:
    process(batch)

# BEST: Write directly to storage
df.filter(df.amount > 1000).write.parquet("output_path")
```

### 2. Use Broadcast for Lookups

```python
from pyspark.sql.functions import broadcast

# Lookup table: 50MB
lookup = spark.read.parquet("lookup.parquet")

# Broadcast to all executors (cached in memory)
result = df.join(broadcast(lookup), "key")

# Without broadcast: Both tables shuffle (expensive)
# With broadcast: Only df shuffles, lookup cached on each executor
```

### 3. Pre-filter Before Expensive Operations

```python
# BAD: Process everything, then filter
df_all = df.groupBy("id").sum()  # Expensive aggregation on 100GB
df_large = df_all.filter(df_all.total > 1000)  # Then filter

# GOOD: Filter first, then process
df_large = df.filter(df.amount > 1000)  # Quick filter on 100GB
df_result = df_large.groupBy("id").sum()  # Expensive aggregation on 10GB
```

### 4. Batch Operations

```python
# BAD: Write one partition at a time
df.foreachPartition(lambda part: write_to_db(part))

# GOOD: Batch multiple rows
def write_partition(partition):
    batch = []
    for record in partition:
        batch.append(record)
        if len(batch) == 1000:  # Batch size of 1000
            write_to_db(batch)
            batch = []
    if batch:  # Write remaining
        write_to_db(batch)

df.foreachPartition(write_partition)
```

---

## Real Examples

### Example 1: Data Validation

```python
def validate_record(record):
    """Validate a single record"""
    errors = []

    # Check required fields
    if not record.get("customer_id"):
        errors.append("Missing customer_id")

    # Validate types
    try:
        amount = float(record.get("amount", 0))
        if amount < 0:
            errors.append("Negative amount")
    except ValueError:
        errors.append("Invalid amount format")

    # Validate business rules
    if record.get("country") not in VALID_COUNTRIES:
        errors.append(f"Invalid country: {record['country']}")

    return errors

# Use as UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

@udf(ArrayType(StringType()))
def validate_udf(customer_id, amount, country):
    record = {
        "customer_id": customer_id,
        "amount": amount,
        "country": country
    }
    return validate_record(record)

df_validated = df.withColumn(
    "validation_errors",
    validate_udf(df.customer_id, df.amount, df.country)
)

# Filter to valid records only
df_valid = df_validated.filter(F.size(df_validated.validation_errors) == 0)
```

### Example 2: Efficient String Parsing

```python
def parse_log_line(line):
    """Parse Apache log line efficiently"""
    try:
        # Use split with limit to avoid unnecessary splits
        parts = line.split(" ", 8)
        if len(parts) < 9:
            return None

        return {
            "ip": parts[0],
            "timestamp": parts[3].strip("[]"),
            "method": parts[5].split(" ")[0] if len(parts) > 5 else None,
            "status": parts[8].split(" ")[0] if len(parts) > 8 else None,
        }
    except Exception:
        return None

# Use in PySpark
rdd = spark.sparkContext.textFile("logs/*.log")
parsed_rdd = rdd.map(parse_log_line).filter(lambda x: x is not None)

# Convert to DataFrame
schema = StructType([
    StructField("ip", StringType()),
    StructField("timestamp", StringType()),
    StructField("method", StringType()),
    StructField("status", StringType()),
])

df_logs = spark.createDataFrame(parsed_rdd, schema)
```

### Example 3: Batch Processing to External System

```python
def write_batch_to_api(partition, api_endpoint, batch_size=100):
    """Write records to external API in batches"""
    batch = []
    successful = 0
    failed = 0

    for record in partition:
        batch.append(record)

        if len(batch) >= batch_size:
            try:
                # Call external API
                response = requests.post(
                    api_endpoint,
                    json={"records": batch},
                    timeout=30
                )

                if response.status_code == 200:
                    successful += len(batch)
                else:
                    failed += len(batch)
                    print(f"API error: {response.status_code}")
            except Exception as e:
                failed += len(batch)
                print(f"API call failed: {e}")

            batch = []

    # Process remaining records
    if batch:
        try:
            response = requests.post(api_endpoint, json={"records": batch})
            if response.status_code == 200:
                successful += len(batch)
            else:
                failed += len(batch)
        except Exception as e:
            failed += len(batch)

    print(f"Partition done: {successful} successful, {failed} failed")
    return [(successful, failed)]

# Use in PySpark
df.foreachPartition(
    lambda part: write_batch_to_api(part, "https://api.example.com/write")
)
```

---

## Key Takeaways

✅ **Data structures matter** - Use sets for lookups, lists for iteration
✅ **Generators over lists** - Stream data when processing large partitions
✅ **List comprehensions** - 20-30% faster than map/filter
✅ **Vectorized UDFs** - 10-100x faster than row-at-a-time UDFs
✅ **Filter early** - Before expensive operations
✅ **Batch operations** - Better throughput for external systems
✅ **Error handling** - Always handle exceptions in production code

---

## Next Steps

1. **Profile your code** - Where are the bottlenecks?
2. **Move to Section 03** - AWS EMR setup and configuration
3. **Use these patterns** in your own pipelines

---

**Remember:** Every 10% improvement in Python efficiency = 10% faster PySpark jobs across 1000s of partitions!
