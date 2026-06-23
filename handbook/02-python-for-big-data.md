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
6. [PyArrow Optimization for PySpark](#pyarrow-optimization-for-pyspark)
7. [Memory-Aware Coding](#memory-aware-coding)
8. [Exception Handling in Production](#exception-handling-in-production)
9. [Performance Tips](#performance-tips)
10. [Real Examples](#real-examples)

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

### Is pandas_udf Part of Standard PySpark?

**YES - pandas_udf is officially part of PySpark!**

```python
# pandas_udf is in standard PySpark since 2.3.0
from pyspark.sql.functions import pandas_udf  # Official PySpark function

# No external packages needed to import it
# But you DO need pandas and pyarrow installed on your cluster
```

### Requirements for Using pandas_udf

```
PySpark Installation ✓ (comes with pandas_udf)
├─ PySpark >= 2.3.0 required
└─ pandas_udf available in pyspark.sql.functions

BUT you ALSO need:
├─ pandas library: pip install pandas
├─ pyarrow library: pip install pyarrow (recommended for performance)
└─ Both must be installed on DRIVER and ALL EXECUTORS
```

### Installation & Setup

```bash
# Step 1: Install PySpark (comes with pandas_udf support)
pip install pyspark>=3.0.0

# Step 2: Install pandas (required for pandas_udf to work)
pip install pandas

# Step 3: Install pyarrow (recommended for better performance)
pip install pyarrow

# Verify installation
python -c "import pyspark; from pyspark.sql.functions import pandas_udf; print('✓ Ready!')"
```

### On AWS EMR or Spark Cluster

```python
# pandas_udf is available by default (part of PySpark)
# But you must ensure pandas/pyarrow are on all nodes:

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.executorEnv.PYTHONPATH", "/opt/python-packages") \
    .appName("PandasUDFExample") \
    .getOrCreate()

# When submitting via spark-submit:
# spark-submit \
#   --packages org.apache.spark:spark-sql_2.12:3.0.0 \
#   --py-files /path/to/pandas-1.0.0.tar.gz \
#   script.py

# Pandas_udf will be available and working!
```

### Quick Verification

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

spark = SparkSession.builder.appName("Test").getOrCreate()

# Test if pandas_udf works
@pandas_udf("double")
def test_udf(s: pd.Series) -> pd.Series:
    return s * 2

df = spark.range(10).toDF("value")
result = df.withColumn("doubled", test_udf(col("value")))
result.show()

# If this works without errors, pandas_udf is properly set up ✓
```

### Summary

| Component | Status | Where From |
|-----------|--------|-----------|
| **pandas_udf** | ✅ Built-in | Standard PySpark (2.3.0+) |
| **pandas library** | ⚠️ Required | Separate install (`pip install pandas`) |
| **pyarrow** | ⚠️ Recommended | Separate install (`pip install pyarrow`) |

**Bottom Line:** pandas_udf is official PySpark, but you need to install pandas and pyarrow separately for it to work!

---

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

### Important: What Can Be Passed to Vectorized UDFs?

```python
# ❌ CANNOT pass: Full PySpark DataFrame
@pandas_udf(DoubleType())
def wrong_function(df: pd.DataFrame) -> pd.Series:  # ❌ WRONG!
    # This won't work - pandas_udf doesn't accept full DataFrames
    return df.sum()

# ✅ CAN pass: Individual Spark columns (become pd.Series)
@pandas_udf(DoubleType())
def correct_function(s: pd.Series) -> pd.Series:  # ✅ CORRECT!
    # s is a batch of values from ONE column
    # e.g., batch of 1000 salary values
    return s * 1.1

# ✅ CAN pass: Multiple columns (become multiple pd.Series)
@pandas_udf(DoubleType())
def multi_column(salary: pd.Series, tax_rate: pd.Series) -> pd.Series:
    # salary: batch of 1000 salary values
    # tax_rate: batch of 1000 tax_rate values (same length)
    return salary * tax_rate

# Usage:
df.withColumn("doubled_salary", batch_processing(col("salary")))
df.withColumn("tax", multi_column(col("salary"), col("tax_rate")))
```

### Understanding the pd.Series Parameter & Batch Size

```python
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

# What is pd.Series in the function signature?
@pandas_udf("double")
def multiply_by_two_vectorized(s: pd.Series) -> pd.Series:
    # ❌ NOT a full column: s is NOT the entire "amount" column (1M rows)
    # ✅ BUT a BATCH: s is a batch of rows from "amount" column

    # Example: if df has 1,000,000 rows
    # With default batch size (10,000 rows):
    # This function will be called ~100 times:
    # Call 1: s = pd.Series([100, 200, 150, ...10,000 values...])
    # Call 2: s = pd.Series([180, 220, 195, ...10,000 values...])
    # Call 3: s = pd.Series([90, 210, 160, ...10,000 values...])
    # ... and so on (100 total calls)

    return s * 2

# Get current batch size setting
batch_size = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch")
print(f"Current batch size: {batch_size} rows")  # Default: 10000
```

### Who Sets the Batch Size?

**Spark's Configuration:** `spark.sql.execution.arrow.maxRecordsPerBatch`

```python
from pyspark.sql import SparkSession

# Default batch size is 10,000 rows (not 1000!)
spark = SparkSession.builder.getOrCreate()

# Check current setting
current_batch = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch")
print(current_batch)  # Output: 10000

# Change batch size - during SparkSession creation
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
    .appName("MyApp") \
    .getOrCreate()

# Or change it after creation
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")

# Now pandas_udf will use 20,000 rows per batch instead of 10,000
```

### How Batch Size Affects Performance

```python
# Small batch size: 1,000 rows
# Pros: Less memory per batch, more frequent Python calls
# Cons: More overhead, slower (more function calls)
# 1M rows ÷ 1,000 = 1,000 function calls (overhead!)

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")
# For 1M rows: 1000 calls to pandas_udf

# Default batch size: 10,000 rows
# Balanced: 1M rows ÷ 10,000 = 100 function calls (good!)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
# For 1M rows: 100 calls to pandas_udf

# Large batch size: 50,000 rows
# Pros: Fewer function calls, less overhead, faster
# Cons: More memory per batch, risk of OutOfMemory
# 1M rows ÷ 50,000 = 20 function calls (fast!)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
# For 1M rows: 20 calls to pandas_udf
```

### Guidelines for Setting Batch Size

```python
# FACTORS THAT INFLUENCE BATCH SIZE CHOICE:
# 1. Memory available per executor
# 2. Complexity of pandas_udf function
# 3. Size of each row (width of DataFrame)
# 4. Whether you're I/O bound or CPU bound

# RECOMMENDATIONS:
# Small DataFrames (few columns):  10,000 rows ✅ (default)
# Medium DataFrames:               5,000 - 10,000 rows
# Large DataFrames (many columns): 1,000 - 5,000 rows
# Complex operations:              2,000 - 5,000 rows

# RULE OF THUMB:
# Memory per batch ≈ batch_size × avg_row_size_in_bytes
# Keep it < 100MB per batch for safety

# Example calculation:
row_size = 500  # bytes (5 columns of 100 bytes each)
max_batch_memory = 100 * 1024 * 1024  # 100MB
recommended_batch = max_batch_memory // row_size
print(f"Recommended batch size: {recommended_batch} rows")
# Output: Recommended batch size: 209715 rows
```

### Real Example: Tuning Batch Size

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
import pandas as pd
import time

# Create session with custom batch size
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
    .appName("BatchSizeTuning") \
    .getOrCreate()

# Create test data
df = spark.range(1000000).toDF("id")

# Define pandas_udf that prints batch info
@pandas_udf("long")
def process_batch(s: pd.Series) -> pd.Series:
    print(f"Processing batch of {len(s)} rows")  # Shows actual batch size
    return s * 2

# Apply UDF - watch console output
result = df.withColumn("doubled", process_batch(col("id")))
result.collect()

# Console output (with batch_size=5000):
# Processing batch of 5000 rows
# Processing batch of 5000 rows
# Processing batch of 5000 rows
# ... (200 total batches for 1M rows)
```

# How to use it:
result = df.withColumn("doubled_amount", multiply_by_two_vectorized(col("amount")))
# Spark internally:
# 1. Takes col("amount")
# 2. Batches it into chunks of ~1000 rows
# 3. Converts each batch to pd.Series
# 4. Calls function with each batch
# 5. Concatenates results
```

### What About DataFrames? Use Regular UDFs or SQL

```python
# If you need to process a full DataFrame, you have options:

# Option 1: Use df.rdd.mapPartitions (for complex logic)
def process_partition(partition):
    """Process one partition at a time (not all data)"""
    for row in partition:
        yield row.amount * 2

result_rdd = df.rdd.mapPartitions(process_partition)

# Option 2: Work with columns, not full DataFrame
# This is the pandas_udf approach - pass individual columns

# Option 3: Use PySpark SQL (even faster than pandas_udf)
from pyspark.sql.functions import col
result = df.withColumn("doubled", col("amount") * 2)
```

**Remember:** Vectorized UDFs work with **batches of individual columns** (pd.Series), not full DataFrames!

---

## Automatic Conversion: You DON'T Manually Convert!

### ❌ DON'T Do This (Manual Conversion)

```python
# WRONG! Manual conversion to pandas - CRASHES on large data!
import pandas as pd

# This tries to load ENTIRE DataFrame into memory at once!
# 100GB file → tries to create 100GB pandas DataFrame → OutOfMemory!
pandas_df = spark_df.toPandas()

# Now you have all data in memory (impossible for large files!)
result = pandas_df['amount'] * 2  # Process in memory

# Then convert back (more memory overhead!)
result_spark = spark.createDataFrame(result)
```

### ✅ DO This (Use pandas_udf - Automatic Conversion)

```python
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

# Step 1: Read PySpark DataFrame
df = spark.read.parquet("huge_file.parquet")  # ← PySpark DataFrame
# df is still a PySpark DataFrame (lazy, not loaded into memory)

# Step 2: Define pandas_udf function
@pandas_udf("double")  # Return type: double
def multiply_by_two(s: pd.Series) -> pd.Series:
    # Input: s is pd.Series (batch of ~1000 rows from "amount" column)
    # NOT a full column, NOT a full DataFrame
    # Spark automatically batches the column data
    return s * 2

# Step 3: Apply pandas_udf to PySpark DataFrame column
result = df.withColumn("doubled", multiply_by_two(col("amount")))
# df: PySpark DataFrame (input)
# col("amount"): Column reference from df
# multiply_by_two(): pandas_udf function
# result: PySpark DataFrame (output, with new "doubled" column)

# Spark internally:
# 1. Takes col("amount") from PySpark DataFrame df
# 2. Extracts column: 100GB of data
# 3. Splits into batches: [batch1, batch2, batch3, ...]
# 4. Converts each batch to pd.Series automatically
# 5. Calls multiply_by_two(batch) with each batch
# 6. Collects results from all batches
# 7. Reconstructs Spark column from results
# 8. Adds column back to PySpark DataFrame via withColumn()
# 9. Returns new PySpark DataFrame with "doubled" column
```

**Critical Points:**
- ✅ `df` is a **PySpark DataFrame** (the input)
- ✅ `col("amount")` is a **column reference** from that PySpark DataFrame
- ✅ `multiply_by_two()` is a **pandas_udf** that processes batches automatically
- ✅ `result` is a **PySpark DataFrame** (the output)
- ✅ **Spark converts to pandas automatically in small batches** - YOU DON'T DO IT!
- ✅ Safe for any size data (1GB, 100GB, 1TB+)

### How Spark Automatically Converts

```
PYSPARK DATAFRAME (100GB)
        ↓
    [Partition 1: 10GB]
        ↓
    [Batch 1: 100MB] ← Convert to pd.Series automatically
        ↓
    pandas_udf processes
        ↓
    Result batch → back to Spark
        ↓
    [Batch 2: 100MB] ← Convert to pd.Series automatically
        ↓
    pandas_udf processes
        ↓
    Continue for all batches...
        ↓
RESULT DATAFRAME (same size as input)
```

### Real Comparison

```python
from pyspark.sql.functions import col, pandas_udf
import pandas as pd

# Assume: df is a 100GB DataFrame with 1 billion rows

# ❌ WRONG - Manual conversion
pandas_df = df.toPandas()  # CRASHES! Tries to load 100GB into memory
result = pandas_df * 2
spark_result = spark.createDataFrame(result)

# ✅ RIGHT - Vectorized UDF (automatic batch conversion)
@pandas_udf("double")
def multiply(s: pd.Series) -> pd.Series:
    return s * 2

result = df.withColumn("doubled", multiply(col("amount")))
# Memory used: ~1GB at a time (for batch processing)
# Safe for 100GB file!

# ✅ EVEN BETTER - Built-in function (no UDF needed!)
result = df.withColumn("doubled", col("amount") * 2)
# Memory used: ~100MB (Catalyst optimized)
# Fastest option!
```

### When Would You Manually Convert?

**Only in these rare cases:**

```python
# Case 1: Small data (< 1GB) that fits in memory
df_small = spark.read.parquet("small_file.parquet")  # 100MB
pandas_small = df_small.toPandas()  # Safe, fits in memory

# Case 2: Need full DataFrame for complex pandas operations
# (But this is rare - usually you can do it in Spark!)
pandas_df = df.filter(col("status") == "ACTIVE").toPandas()
# Now do complex pandas-specific operations
result = pandas_df.groupby('date').apply(complex_function)

# Case 3: Debugging/exploration (interactive mode)
# In Jupyter for quick analysis:
sample = df.limit(1000).toPandas()  # Get sample for exploration
```

### Rule of Thumb

```
DataFrame Size | Approach
─────────────────────────────────────────
< 1GB          │ Can use .toPandas() or pandas_udf
1GB - 100GB    │ Use pandas_udf (auto batching)
> 100GB        │ NEVER use .toPandas() - use pandas_udf or built-in functions

✅ Always use pandas_udf for safety (works on any size)
❌ Never use .toPandas() unless data is small
✅ Prefer built-in functions (no conversion needed!)
```

**Bottom Line:** Spark automatically converts to pandas **in safe batches** when you use `pandas_udf`. You don't need to do anything manually!

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

## PyArrow Optimization for PySpark

As a **PySpark/AWS/Python Certified Trainer**, PyArrow is one of the most impactful optimizations you can enable. It can provide **10-100x speedup** for data transfer between JVM and Python. Here's the comprehensive guide.

### What is PyArrow?

**PyArrow** is Apache's columnar in-memory data format that enables efficient data transfer between JVM (Spark) and Python processes.

```
WITHOUT PyArrow:
┌─────────────────────────────────┐
│ Spark (JVM)                     │
│ Data in Scala/Java format       │
└────────────────┬────────────────┘
                 │ Pickle serialization (SLOW!)
                 ↓
        Conversion overhead
                 │
┌────────────────┴────────────────┐
│ Python                          │
│ Data in Python object format    │
└─────────────────────────────────┘

WITH PyArrow:
┌─────────────────────────────────┐
│ Spark (JVM)                     │
│ Data in Arrow columnar format   │
└────────────────┬────────────────┘
                 │ Arrow serialization (FAST!)
                 ↓
        Zero-copy data transfer
                 │
┌────────────────┴────────────────┐
│ Python                          │
│ Data in Arrow columnar format   │
│ (native pandas/numpy compatible)│
└─────────────────────────────────┘
```

### How to Enable PyArrow

```python
from pyspark.sql import SparkSession

# Method 1: Enable PyArrow by default
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .appName("PyArrowOptimization") \
    .getOrCreate()

# Method 2: Check if enabled
is_arrow_enabled = spark.conf.get("spark.sql.execution.arrow.enabled")
print(f"PyArrow enabled: {is_arrow_enabled}")  # Should be: true

# Method 3: Verify PyArrow is installed
import pyarrow
print(f"PyArrow version: {pyarrow.__version__}")
```

### Installation Requirements

```bash
# Install PyArrow (required for optimization)
pip install pyarrow>=2.0.0

# Install pandas (works best with PyArrow)
pip install pandas

# Install numpy (optional but recommended)
pip install numpy

# Verify installation
python -c "import pyarrow; import pandas; print('✓ Ready for optimization')"

# On AWS EMR, add to bootstrap action:
# #!/bin/bash
# sudo pip install pyarrow pandas
```

### Real Performance Benchmarks

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import time
import pandas as pd

# Create large test DataFrame
spark = SparkSession.builder.appName("PyArrowBench").getOrCreate()
df = spark.range(1000000).withColumn("value", rand())

# WITHOUT PyArrow (using default Pickle)
spark.conf.set("spark.sql.execution.arrow.enabled", "false")

start = time.time()
pandas_df_slow = df.toPandas()  # Convert to pandas
slow_time = time.time() - start
print(f"WITHOUT PyArrow: {slow_time:.2f}s")  # ~5-10 seconds

# WITH PyArrow (columnar format)
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

start = time.time()
pandas_df_fast = df.toPandas()  # Convert to pandas
fast_time = time.time() - start
print(f"WITH PyArrow: {fast_time:.2f}s")   # ~0.5-1 second

# Speedup calculation
speedup = slow_time / fast_time
print(f"Speedup: {speedup:.1f}x faster with PyArrow!")
# Output: Speedup: 10.2x faster with PyArrow!

# Verify data is identical
assert pandas_df_slow.equals(pandas_df_fast), "Data mismatch!"
print("✓ Data integrity verified")
```

### When to Use PyArrow (✅ DO THIS)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .appName("PyArrowGood") \
    .getOrCreate()

# ✅ USE CASE 1: Convert to Pandas (with PyArrow enabled)
# Scenario: Need small result for analysis
df = spark.read.parquet("s3://data/sales/2024/").filter(col("amount") > 1000)
pandas_df = df.toPandas()  # FAST with PyArrow! (10x speedup)
# Process with pandas operations
result = pandas_df.groupby('region').sum()

# ✅ USE CASE 2: Pandas UDFs (with PyArrow enabled)
# Scenario: Complex logic in pandas_udf
@pandas_udf("double")
def complex_feature_engineering(s: pd.Series) -> pd.Series:
    # PyArrow speeds up batch transfer between JVM and Python
    return s.rolling(window=7).mean()

result = df.withColumn("moving_avg", complex_feature_engineering(col("amount")))

# ✅ USE CASE 3: ML Feature Engineering (with PyArrow)
# Scenario: Using pandas for ML preprocessing
@pandas_udf("double")
def ml_feature_scaling(s: pd.Series) -> pd.Series:
    mean = s.mean()
    std = s.std()
    return (s - mean) / std if std > 0 else s

df_ml = df.withColumn("scaled_amount", ml_feature_scaling(col("amount")))

# ✅ USE CASE 4: Distributed Pandas Operations (with PyArrow)
# Scenario: pandas_udf with multiple columns
@pandas_udf("double")
def multicolumn_operation(amount: pd.Series, quantity: pd.Series) -> pd.Series:
    # PyArrow efficiently transfers multiple columns
    return amount * quantity

result = df.withColumn("total", multicolumn_operation(col("amount"), col("quantity")))

# ✅ USE CASE 5: Batch Processing in pandas_udf
df.groupby('region').applyInPandas(
    lambda batch: batch[batch['amount'] > batch['amount'].quantile(0.75)],
    schema="region STRING, amount DOUBLE, quantity INT"
)
# PyArrow speeds up batch transfers!
```

### When NOT to Use PyArrow (❌ AVOID THIS)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PyArrowBad").getOrCreate()

# ❌ DON'T: Large DataFrame to pandas (memory explosion)
# PyArrow speeds up transfer, but still loads entire DF in memory!
df_huge = spark.read.parquet("100GB_file.parquet")
pandas_df = df_huge.toPandas()  # Still uses 100GB memory! (PyArrow just faster)
# Better: Pre-filter before converting
pandas_df = df_huge.filter(col("year") == 2024).toPandas()  # Much smaller!

# ❌ DON'T: Use PyArrow when you don't need pandas anyway
# If you're staying in Spark, PyArrow doesn't help!
result = df.filter(col("amount") > 1000) \
    .groupBy("region").sum()  # No pandas needed, PyArrow not used

# ❌ DON'T: Enable PyArrow on every operation if memory is tight
# Each pandas_udf call batches data into pandas
# With limited memory, this can cause issues
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
# With small batches, less memory pressure

# ❌ DON'T: Assume PyArrow helps with built-in Spark functions
# PyArrow only speeds up Python ↔ JVM transfer
result = df.withColumn("doubled", col("amount") * 2)  # No Python, PyArrow not used!
```

### PyArrow Configuration Tuning

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
    .appName("PyArrowTuned") \
    .getOrCreate()

# Configuration options:

# 1. Enable PyArrow for all Arrow transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# 2. Enable PyArrow specifically for PySpark operations
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 3. Batch size for pandas UDFs (memory vs speed tradeoff)
# Smaller = less memory, more calls
# Larger = more memory, fewer calls
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")  # 5K rows per batch

# 4. Fallback behavior (what to do if PyArrow is unavailable)
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")  # Fall back to Pickle if needed

# 5. Timezone handling
spark.conf.set("spark.sql.execution.arrow.timezone", "UTC")
```

### Real-World Example: AWS EMR with PyArrow

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, rand
import pandas as pd
import pyarrow

# Initialize with PyArrow optimizations
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000") \
    .config("spark.sql.shuffle.partitions", "100") \
    .appName("EMRPyArrowOptimization") \
    .getOrCreate()

print(f"PyArrow version: {pyarrow.__version__}")
print(f"Arrow enabled: {spark.conf.get('spark.sql.execution.arrow.enabled')}")

# Read from S3 with PyArrow optimization
df = spark.read.parquet("s3://my-bucket/data/sales/")

# Define pandas_udf for feature engineering (PyArrow speeds this up!)
@pandas_udf("double")
def calculate_profit_margin(revenue: pd.Series, cost: pd.Series) -> pd.Series:
    # PyArrow efficiently transfers these columns as batches
    return ((revenue - cost) / revenue * 100).fillna(0)

# Apply transformation (PyArrow accelerates Python ↔ JVM transfer)
df_features = df.withColumn(
    "profit_margin",
    calculate_profit_margin(col("revenue"), col("cost"))
)

# Convert sample to pandas (PyArrow speeds this up!)
sample = df_features.filter(col("profit_margin") > 10).toPandas()
print(f"Sample shape: {sample.shape}")
print(f"Features: {sample.columns.tolist()}")

# Write results with optimized format
df_features.coalesce(10).write \
    .mode("overwrite") \
    .parquet("s3://my-bucket/output/features/")

print("✓ Complete with PyArrow optimization!")
```

### Guidelines: Decision Tree

```
Do you need to transfer data between Spark and Python?
│
├─ YES: Using pandas_udf or toPandas()?
│  │
│  ├─ YES: Is PyArrow installed?
│  │  │
│  │  ├─ YES: Enable it! (10-100x speedup)
│  │  │   spark.conf.set("spark.sql.execution.arrow.enabled", "true")
│  │  │   ✅ DO THIS
│  │  │
│  │  └─ NO: Install it!
│  │      pip install pyarrow
│  │
│  └─ NO: Staying in Spark?
│     └─ PyArrow won't help, use Spark SQL ✓
│
└─ NO: All Spark operations?
   └─ PyArrow not needed, use built-in functions ✓

WHEN TO ENABLE:
✅ pandas_udf operations
✅ .toPandas() conversions
✅ pandas operations in distributed UDFs
✅ ML feature engineering with pandas

WHEN NOT TO ENABLE:
❌ Pure Spark operations (SQL, built-in functions)
❌ When memory is extremely tight
❌ If you're not using pandas at all
```

### Performance Monitoring

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .appName("Monitor") \
    .getOrCreate()

df = spark.range(1000000).select("id")

# Measure toPandas() performance
start = time.time()
pandas_df = df.toPandas()
elapsed = time.time() - start

rows_per_sec = len(pandas_df) / elapsed
print(f"Transfer speed: {rows_per_sec:,.0f} rows/sec")
# With PyArrow: ~1M+ rows/sec
# Without PyArrow: ~100K rows/sec

# Check Arrow memory usage
import psutil
import os
process = psutil.Process(os.getpid())
memory_mb = process.memory_info().rss / 1024 / 1024
print(f"Memory usage: {memory_mb:.0f} MB")
```

### Summary Table: PyArrow Impact

```
Operation           | Without PyArrow | With PyArrow | Speedup
─────────────────────────────────────────────────────────
toPandas() 1M rows  | 8-10 sec        | 0.8-1 sec    | 10x
pandas_udf call     | 5 sec           | 0.5 sec      | 10x
batch transfer      | 100K rows/sec   | 1M+ rows/sec | 10x
Memory overhead     | ~2x data size   | ~1.1x        | Better
Pandas UDF latency  | 50ms/call       | 5ms/call     | 10x
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
