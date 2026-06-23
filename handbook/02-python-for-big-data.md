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
3. [UDFs vs Built-in Functions: A Deep Dive](#udfs-vs-built-in-functions-a-deep-dive)
4. [Vectorized UDFs: Using Pandas with Large DataFrames](#vectorized-udfs-using-pandas-with-large-dataframes)
5. [Common Built-in Functions: Your First Choice](#common-built-in-functions-your-first-choice)
6. [Memory-Aware Coding](#memory-aware-coding)
7. [Exception Handling in Production](#exception-handling-in-production)
8. [Performance Tips](#performance-tips)
9. [Real Examples](#real-examples)

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
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# Simple UDF
# @udf(IntegerType()) means: function returns IntegerType (not the input type!)
# This tells Spark what data type the function returns
@udf(IntegerType())  # Return type: integer (output of multiply_by_two)
def multiply_by_two(x):
    """
    Input: x (any type passed in)
    Output: IntegerType (specified in decorator)
    """
    return x * 2 if x is not None else 0

df_with_doubled = df.withColumn("doubled", multiply_by_two(df.value))

# Better: Vectorized UDF (10-100x faster)
import pandas as pd

# @pandas_udf(DoubleType()) means: function returns DoubleType
# Vectorized UDFs take pd.Series (batch of values) and return pd.Series
@pandas_udf(DoubleType())  # Return type: double (output of function)
def multiply_by_two_vectorized(s: pd.Series) -> pd.Series:
    """
    Input: pd.Series (batch of ~1000 values, NOT single value)
    Output: pd.Series with same length (return type: double)
    """
    return s * 2

df_with_doubled = df.withColumn("doubled", multiply_by_two_vectorized(df.value))
```

**Important:** The decorator parameter (e.g., `IntegerType()`, `DoubleType()`) specifies the **RETURN TYPE** of the function, not the input type. Spark uses this to:
- Validate the DataFrame column type after the UDF executes
- Plan downstream operations
- Prevent type mismatches

---

## UDFs vs Built-in Functions: A Deep Dive

### The Problem with Simple UDFs

**Critical Issue:** Simple UDFs **CANNOT be optimized by Catalyst**

```python
# SIMPLE UDF - Row-at-a-time execution, NO Catalyst optimization
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType, IntegerType

# @udf(DoubleType()) - decorator parameter is RETURN TYPE of function
# DoubleType() means this function returns a Double (e.g., 19.99)
# Not the input type - could receive any inputs
@udf(DoubleType())  # Return type: double
def apply_discount(amount, discount_pct):
    """
    Inputs: amount (any numeric), discount_pct (any numeric)
    Output: DoubleType (specified in decorator)
    """
    return amount * (1 - discount_pct / 100)

df = df.withColumn("discounted_price", apply_discount(col("price"), col("discount")))

# What happens:
# 1. For each row in the partition:
#    - Call Python function
#    - Pass single row values
#    - Wait for result
#    - Move to next row
# 2. Catalyst can't optimize:
#    - Can't push down filters
#    - Can't prune columns
#    - Can't combine with other operations
# 3. Python/JVM boundary crossed for EVERY row
#    - Each row = overhead (serialization, deserialization)
#    - 1M rows = 1M boundary crossings!
```

### Why Catalyst Can't Optimize Simple UDFs

```
BUILT-IN FUNCTION (Catalyst-optimized):
├─ Catalyst sees: col("price") * (1 - col("discount") / 100)
├─ Optimizes: Combine expressions, push filters, prune columns
├─ Execution: Pure Java/Scala code (vectorized, very fast)
└─ Speed: 100-1000x faster

SIMPLE UDF (No optimization):
├─ Catalyst sees: BLACK BOX - can't peek inside
├─ Can't optimize: What does this function do?
├─ Execution: Python function row-by-row
├─ Overhead: Python/JVM boundary per row
└─ Speed: 10-100x slower than built-in functions
```

### Performance Comparison

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf
import pandas as pd
import time

spark = SparkSession.builder.appName("UDF_Comparison").getOrCreate()

# Create test data: 1M rows
df = spark.range(1000000).toDF("amount")
df = df.withColumn("discount", (col("amount") % 20))  # 0-19

# 1. SIMPLE UDF (Row-at-a-time, NO optimization)
@udf("double")
def simple_discount(amount, discount):
    return float(amount) * (1 - discount / 100)

start = time.time()
result1 = df.withColumn("price", simple_discount(col("amount"), col("discount")))
result1.count()  # Force evaluation
simple_time = time.time() - start
print(f"Simple UDF: {simple_time:.2f}s")  # ~8-12 seconds

# 2. BUILT-IN FUNCTIONS (Catalyst-optimized)
start = time.time()
result2 = df.withColumn("price", col("amount") * (1 - col("discount") / 100))
result2.count()
builtin_time = time.time() - start
print(f"Built-in: {builtin_time:.2f}s")  # ~0.5-1 second

# 3. VECTORIZED UDF (Pandas UDF, batched, better than simple)
@pandas_udf("double")
def vectorized_discount(amount: pd.Series, discount: pd.Series) -> pd.Series:
    return amount * (1 - discount / 100)

start = time.time()
result3 = df.withColumn("price", vectorized_discount(col("amount"), col("discount")))
result3.count()
vectorized_time = time.time() - start
print(f"Vectorized UDF: {vectorized_time:.2f}s")  # ~2-3 seconds

# Results:
# Built-in:         0.8s  (baseline)
# Vectorized UDF:   2.5s  (3x slower)
# Simple UDF:      10.2s  (12x slower)
```

### UDF Return Types Reference

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    IntegerType,    # int: -2147483648 to 2147483647
    LongType,       # long: very large integers
    DoubleType,     # double: 3.14, -42.5, etc.
    StringType,     # string: "hello", "world"
    BooleanType,    # boolean: True, False
    DateType,       # date: 2024-01-15
    TimestampType,  # timestamp: 2024-01-15 10:30:45
    ArrayType,      # array: [1, 2, 3]
)

# Example 1: Return integer
@udf(IntegerType())  # Return type: integer
def count_words(text):
    return len(text.split()) if text else 0

# Example 2: Return string
@udf(StringType())  # Return type: string
def format_name(first, last):
    return f"{first.upper()} {last.upper()}" if first and last else "UNKNOWN"

# Example 3: Return boolean
@udf(BooleanType())  # Return type: boolean (True/False)
def is_valid_email(email):
    return "@" in email and "." in email if email else False

# Example 4: Return double
@udf(DoubleType())  # Return type: double (decimal)
def calculate_tax(amount, tax_rate):
    return amount * (tax_rate / 100) if amount else 0.0

# Example 5: Return array
@udf(ArrayType(StringType()))  # Return type: array of strings
def split_text(text):
    return text.split() if text else []

# IMPORTANT: Decorator type MUST match what function returns!
# Bad:
@udf(IntegerType())  # Says return int
def wrong_function():
    return "hello"  # But returns string! ❌ Type mismatch

# Good:
@udf(StringType())  # Says return string
def correct_function():
    return "hello"  # Returns string ✅ Correct
```

## Common Built-in Functions: Your First Choice

As a PySpark Certified Trainer, I recommend **ALWAYS check if a built-in function exists before writing a UDF**. Here are the most common ones used in production:

### Math Functions

```python
from pyspark.sql.functions import col, abs, ceil, floor, round, sqrt, pow
from pyspark.sql.functions import greatest, least

# INSTEAD OF UDF: Use built-in math
# Bad:
@udf("double")
def calculate_discount(price, discount_pct):
    return price * (1 - discount_pct / 100)
df.withColumn("discounted", calculate_discount(col("price"), col("discount")))

# Good: Built-in operations
df.withColumn("discounted", col("price") * (1 - col("discount") / 100))

# Other math examples
df.withColumn("absolute", abs(col("value")))
df.withColumn("rounded", round(col("amount"), 2))  # 2 decimal places
df.withColumn("square_root", sqrt(col("area")))
df.withColumn("power", pow(col("base"), col("exponent")))
df.withColumn("max_value", greatest(col("a"), col("b"), col("c")))
df.withColumn("min_value", least(col("a"), col("b"), col("c")))
```

### String Functions

```python
from pyspark.sql.functions import (
    col, upper, lower, length, substring, trim, ltrim, rtrim,
    concat, concat_ws, split, array_join, regexp_replace,
    startswith, endswith, contains, instr
)

# INSTEAD OF UDF: Use built-in string functions
# Bad:
@udf("string")
def format_name(first, last):
    return f"{first.upper()} {last.upper()}"
df.withColumn("formatted", format_name(col("first"), col("last")))

# Good: Built-in functions
df.withColumn("formatted", concat(upper(col("first")), col(" "), upper(col("last"))))
# OR even simpler:
df.withColumn("formatted", concat_ws(" ", upper(col("first")), upper(col("last"))))

# Other string examples
df.withColumn("name_length", length(col("name")))  # String length
df.withColumn("first_3", substring(col("name"), 1, 3))  # Extract substring
df.withColumn("trimmed", trim(col("text")))  # Remove leading/trailing whitespace
df.withColumn("upper", upper(col("name")))  # Convert to uppercase
df.withColumn("lower", lower(col("name")))  # Convert to lowercase
df.withColumn("parts", split(col("text"), ","))  # Split into array
df.withColumn("replaced", regexp_replace(col("text"), "[0-9]", "X"))  # Regex replace
df.withColumn("has_prefix", startswith(col("email"), "test"))  # Check prefix
df.withColumn("is_gmail", contains(col("email"), "gmail"))  # Check contains
```

### Date & Time Functions

```python
from pyspark.sql.functions import (
    col, to_date, to_timestamp, year, month, dayofmonth, hour, minute,
    date_add, date_sub, datediff, months_between, from_unixtime,
    unix_timestamp, current_date, current_timestamp
)

# INSTEAD OF UDF: Use built-in date functions
# Bad:
@udf("integer")
def get_year(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%Y-%m-%d").year
df.withColumn("year", get_year(col("date")))

# Good: Built-in date functions
df.withColumn("year", year(col("date")))  # Extract year
df.withColumn("month", month(col("date")))  # Extract month
df.withColumn("day", dayofmonth(col("date")))  # Extract day
df.withColumn("hour", hour(col("timestamp")))  # Extract hour

# Date arithmetic
df.withColumn("next_week", date_add(col("date"), 7))  # Add 7 days
df.withColumn("last_month", date_sub(col("date"), 30))  # Subtract 30 days
df.withColumn("days_diff", datediff(col("end_date"), col("start_date")))  # Days between
df.withColumn("months_diff", months_between(col("end_date"), col("start_date")))  # Months between

# Convert between formats
df.withColumn("parsed_date", to_date(col("date_string"), "yyyy-MM-dd"))
df.withColumn("parsed_timestamp", to_timestamp(col("timestamp_string"), "yyyy-MM-dd HH:mm:ss"))
df.withColumn("unix_time", unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
```

### Conditional Functions (When/Otherwise)

```python
from pyspark.sql.functions import col, when, coalesce, ifnull

# INSTEAD OF UDF: Use when/otherwise
# Bad:
@udf("string")
def get_status(amount):
    if amount > 1000:
        return "HIGH"
    elif amount > 100:
        return "MEDIUM"
    else:
        return "LOW"
df.withColumn("status", get_status(col("amount")))

# Good: Built-in when/otherwise
df.withColumn("status",
    when(col("amount") > 1000, "HIGH")
    .when(col("amount") > 100, "MEDIUM")
    .otherwise("LOW")
)

# Handle nulls
df.withColumn("filled", coalesce(col("value"), col("backup"), 0))  # Use first non-null
df.withColumn("filled", ifnull(col("value"), 0))  # Replace null with 0
```

### Aggregation Functions

```python
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, min, max, count, stddev, variance,
    collect_list, collect_set, first, last, percentile_approx
)

# INSTEAD OF UDF: Use built-in aggregations
# Bad:
@udf("double")
def custom_sum(values):
    return sum(values)
df.groupBy("category").agg(custom_sum(col("amount")))

# Good: Built-in aggregation
df.groupBy("category").agg(
    spark_sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount"),
    count("amount").alias("count"),
    stddev("amount").alias("std_dev"),
    variance("amount").alias("variance"),
    percentile_approx("amount", 0.5).alias("median")
)

# Collect into arrays (useful for grouping)
df.groupBy("category").agg(
    collect_list("amount").alias("amounts"),  # Keep duplicates
    collect_set("id").alias("unique_ids")  # Keep unique
)
```

### Type Casting Functions

```python
from pyspark.sql.functions import col, cast
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType

# INSTEAD OF UDF: Use cast
# Bad:
@udf("double")
def to_double(x):
    return float(x) if x else 0.0
df.withColumn("amount", to_double(col("amount_string")))

# Good: Built-in cast
df.withColumn("amount", col("amount_string").cast(DoubleType()))
df.withColumn("count", col("count_string").cast(IntegerType()))
df.withColumn("date", col("date_string").cast(DateType()))
```

### Array Functions

```python
from pyspark.sql.functions import (
    col, size, array_contains, array_join, explode,
    filter as spark_filter, transform
)

# Array operations
df.withColumn("array_size", size(col("items")))  # Get array length
df.withColumn("has_item", array_contains(col("items"), "book"))  # Check if contains
df.withColumn("joined", array_join(col("items"), ", "))  # Join array to string
df.withColumn("rows", explode(col("items")))  # Convert array to multiple rows

# Filter array elements (keep only > 100)
df.withColumn("large_amounts",
    spark_filter(col("amounts"), lambda x: x > 100)
)

# Transform array elements (multiply each by 2)
df.withColumn("doubled",
    transform(col("amounts"), lambda x: x * 2)
)
```

### When to Use Which

```python
# ✅ USE BUILT-IN FUNCTIONS (99% of cases)
# - Math operations (*, +, -, /)
# - String operations (upper, lower, substring, length)
# - Date operations (year, month, datediff)
# - Conditionals (when, otherwise)
# - Aggregations (sum, avg, max, min)

result = df.withColumn(
    "discounted_price",
    col("price") * (1 - col("discount") / 100)  # Built-in: Fast!
)

# ✅ USE VECTORIZED UDF (Pandas UDF) when:
# - Logic can't be expressed in SQL/Spark functions
# - You need complex NumPy operations
# - ML feature engineering on batches
# - String parsing/regex that's complex

@pandas_udf("double")
def complex_calculation(prices: pd.Series, costs: pd.Series) -> pd.Series:
    # Can use pandas/numpy operations
    profit_margin = (prices - costs) / prices
    # Apply complex business logic
    return prices * (1 + profit_margin.clip(0.1, 0.5))

result = df.withColumn("adjusted_price", complex_calculation(col("price"), col("cost")))

# ❌ AVOID SIMPLE UDF (unless absolutely necessary)
# - Can't express as built-in? Maybe you need a different approach
# - Simple UDF = 10-100x slower, no Catalyst optimization
```

---

## Vectorized UDFs: Using Pandas with Large DataFrames

### How Vectorized UDFs Work

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# SIMPLE UDF: Row-by-row
@udf(DoubleType())
def row_by_row(x):
    """Called 1M times for 1M rows"""
    return x * 1.1  # 1M Python function calls!

# VECTORIZED UDF (Pandas UDF): Batch processing
@pandas_udf(DoubleType())
def batch_processing(s: pd.Series) -> pd.Series:
    """Called ~1000 times for 1M rows (batches of ~1000)"""
    return s * 1.1  # 1000 Python function calls!
```

**Key Difference:**
- Simple UDF: 1,000,000 Python calls for 1M rows
- Vectorized UDF: ~1,000 Python calls for 1M rows (batch size ~1000)
- Speed improvement: 100-1000x faster!

### Using Pandas Safely with Large DataFrames

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StructType, StructField
import numpy as np

# 1. SAFE: Process batches (automatic with pandas_udf)
@pandas_udf(DoubleType())
def safe_calculation(s: pd.Series) -> pd.Series:
    """
    Pandas UDFs automatically handle batching
    Each batch is small (~1000 rows), fits in memory
    """
    # This runs on ~1000 rows at a time (not 100M!)
    return np.sqrt(s) * 2.5

# 2. UNSAFE: Don't collect large data to driver
# BAD:
df_large = spark.read.parquet("100GB_file.parquet")
pandas_df = df_large.toPandas()  # CRASHES! Tries to load 100GB into memory

# GOOD: Use vectorized UDFs or iterate partitions
df_large.rdd.mapPartitions(process_partition).collect()

# 3. COMPLEX FEATURE ENGINEERING with Pandas
@pandas_udf(DoubleType())
def feature_engineering(
    prices: pd.Series,
    quantities: pd.Series,
    dates: pd.Series
) -> pd.Series:
    """
    Use pandas for complex transformations on batches
    Each batch is safe (small), but logic is flexible
    """
    # Create temporary DataFrame for batch calculations
    batch_df = pd.DataFrame({
        'price': prices,
        'qty': quantities,
        'date': pd.to_datetime(dates)
    })

    # Complex calculations
    batch_df['revenue'] = batch_df['price'] * batch_df['qty']
    batch_df['month'] = batch_df['date'].dt.month
    batch_df['seasonal_factor'] = batch_df['month'].apply(
        lambda m: 1.2 if m in [11, 12] else 0.9  # Holiday season boost
    )
    batch_df['adjusted_revenue'] = batch_df['revenue'] * batch_df['seasonal_factor']

    return batch_df['adjusted_revenue']

# Usage
df_features = df.withColumn(
    "adjusted_revenue",
    feature_engineering(col("price"), col("quantity"), col("order_date"))
)
```

### Real Example: Machine Learning Feature Engineering

```python
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def ml_feature_scaling(values: pd.Series) -> pd.Series:
    """
    Standard scaling: (x - mean) / std
    Safe because:
    - Input is batch of ~1000 values (not 100M)
    - Pandas handles efficiently
    - Returns same size batch
    """
    mean = values.mean()
    std = values.std()
    if std == 0:
        return pd.Series([0.0] * len(values))
    return (values - mean) / std

@pandas_udf(DoubleType())
def ml_log_transform(values: pd.Series) -> pd.Series:
    """
    Log transformation: log(x + 1)
    Safe for batch processing
    """
    return np.log1p(values)

# Usage in ML pipeline
df_ml = spark.read.parquet("customer_data.parquet")

df_scaled = df_ml \
    .withColumn("scaled_income", ml_feature_scaling(col("annual_income"))) \
    .withColumn("log_spending", ml_log_transform(col("lifetime_spending")))

# This runs efficiently:
# - 100M rows split into batches of ~1000
# - Each batch processed with pandas (fast, vectorized)
# - No memory explosion, no Catalyst limitations
```

### Memory Management: Batching for Large Datasets

```python
@pandas_udf("double")
def batch_safe_processing(
    series: pd.Series,
    batch_id: pd.Series  # Optional: which batch number
) -> pd.Series:
    """
    Pandas UDFs automatically batch data
    Even if processing 1 billion rows, each batch is small
    """
    # This batch has ~1000 rows, safe to process
    # Even if total dataset is 1 billion rows!

    # Memory usage: proportional to batch size, NOT total data
    # batch size ~1000 = ~100KB pandas DataFrame
    # Not: 1 billion rows = 100GB DataFrame

    return series * 2

# Guarantee:
# - DataFrame with 1B rows, each partition ~1M rows
# - Spark batches within partition into ~1K row chunks
# - Pandas memory usage: ~100KB per call
# - Total memory: never more than batch_size * data_types
```

### Comparison Table: Functions vs UDFs

```
╔═══════════════════════╦═════════════╦════════════════╦═══════════════════════╗
║ Type                  ║ Speed       ║ Catalyst       ║ Use Case              ║
╠═══════════════════════╬═════════════╬════════════════╬═══════════════════════╣
║ Built-in (col, F.*)   ║ 1x (fast)   ║ YES, optimized ║ Standard operations   ║
║ Vectorized UDF        ║ 10-100x     ║ Partial        ║ Complex logic, ML      ║
║ (Pandas UDF)          ║ slower      ║ optimization   ║ features              ║
╠═══════════════════════╬═════════════╬════════════════╬═══════════════════════╣
║ Simple UDF            ║ 100-1000x   ║ NO             ║ LAST RESORT ONLY      ║
║ (Row-by-row)          ║ slower      ║ optimization   ║                       ║
╚═══════════════════════╩═════════════╩════════════════╩═══════════════════════╝
```

### Best Practices Summary

```python
# 1. DEFAULT: Always try built-in functions first
df.withColumn("discounted", col("price") * 0.9)  # Use this

# 2. SECOND CHOICE: Vectorized UDF if needed
@pandas_udf("double")
def complex_logic(s: pd.Series) -> pd.Series:
    return s.apply(my_complex_function)

df.withColumn("result", complex_logic(col("amount")))

# 3. LAST RESORT: Simple UDF only if absolutely necessary
# (And document why you couldn't use built-in or vectorized!)

# 4. NEVER: Don't try to do what Catalyst can do
# BAD: df.withColumn("total", apply_math_udf(col("a"), col("b")))
# GOOD: df.withColumn("total", col("a") + col("b"))

# 5. REMEMBER: Vectorized UDFs use automatic batching
# Safe to use on 1B rows - Spark handles batching automatically
# Each batch small (~1000 rows), processed by pandas efficiently
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
