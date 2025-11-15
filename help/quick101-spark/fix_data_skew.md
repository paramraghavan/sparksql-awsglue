# The Problem: Data Skew

**Salting** is a technique to fix **data skew** in joins. Let me explain with a real example:

## The Problem: Data Skew

Imagine you're joining two tables on `customer_id`:

```python
# customers table (millions of rows)
# customer_id | name
# 12345       | John
# 12345       | Jane  
# 12345       | Bob
# 99999       | Alice
# ...

# orders table
# order_id | customer_id | amount
# ...
```

**Problem:** Customer ID `12345` has 1 million orders, but other customers have only 10-100 orders each.

When Spark joins these tables:

- ❌ ONE task processes 1 million rows for customer `12345` (takes 10 minutes)
- ✅ Other tasks process 10-100 rows each (takes 10 seconds)

**Result:** Your entire job waits for that ONE slow task! This is **data skew**.

## The Solution: Salting (Adding Random Keys)

**Salting** means adding a random number to "split up" the skewed key across multiple tasks:

### Step-by-Step Example

```python
from pyspark.sql.functions import rand, concat, lit, col

# BEFORE (skewed join)
result = customers.join(orders, "customer_id")  # ❌ Slow!

# AFTER (salted join)
# 1. Add "salt" to the skewed table (customers)
customers_salted = customers.withColumn(
    "salt",
    (rand() * 10).cast("int")  # Random number 0-9
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))  # "12345_3"
)

# 2. "Explode" the other table (orders) to match all salt values
from pyspark.sql.functions import explode, array

orders_exploded = orders.withColumn(
    "salt",
    explode(array([lit(i) for i in range(10)]))  # Create 10 copies
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

# 3. Join on the salted key
result = customers_salted.join(orders_exploded, "customer_id_salted")
```

### What Happens Now?

**Before Salting:**

```
Task 1: customer_id=12345 → 1,000,000 rows (10 minutes) ❌
Task 2: customer_id=67890 → 100 rows       (1 second)
Task 3: customer_id=11111 → 50 rows        (1 second)
```

**After Salting:**

```
Task 1: customer_id=12345_0 → 100,000 rows (1 minute) ✅
Task 2: customer_id=12345_1 → 100,000 rows (1 minute) ✅
Task 3: customer_id=12345_2 → 100,000 rows (1 minute) ✅
...
Task 10: customer_id=12345_9 → 100,000 rows (1 minute) ✅
Task 11: customer_id=67890_0 → 10 rows      (1 second) ✅
```

Now all tasks finish in ~1 minute instead of waiting 10 minutes for one task!

## Practical Example

```python
# Real-world scenario: Join user activity (skewed) with user profiles

# Step 1: Identify the skew
activity.groupBy("user_id").count().orderBy(col("count").desc()).show()
# Output shows user_id="bot_12345" has 10M records, others have ~100

# Step 2: Apply salting
SALT_RANGE = 20  # Use 20 salt values

# Salt the BIG table (activity)
activity_salted = activity.withColumn(
    "salt",
    (rand() * SALT_RANGE).cast("int")
).withColumn(
    "user_id_salted",
    concat(col("user_id"), lit("#"), col("salt"))
)

# Replicate the SMALL table (profiles)
from pyspark.sql.functions import array, lit, explode

profiles_exploded = profiles.withColumn(
    "salt",
    explode(array([lit(i) for i in range(SALT_RANGE)]))
).withColumn(
    "user_id_salted",
    concat(col("user_id"), lit("#"), col("salt"))
)

# Join on salted key
result = activity_salted.join(profiles_exploded, "user_id_salted")

# Clean up: remove salt columns if needed
result = result.drop("salt", "user_id_salted")
```

## When to Use Salting

✅ **Use salting when:**

- One or few keys have way more data than others
- You see tasks with very different durations (e.g., 1 task takes 10 minutes, others take 10 seconds)
- Join operations are slow despite having enough resources

❌ **Don't use salting when:**

- Data is evenly distributed
- The "big" table is actually small (use broadcast join instead)
- Skew is minimal (< 3x difference)

## Alternative: Spark's Built-in Skew Join (Spark 3.0+)

```python
# Enable Adaptive Query Execution with skew handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Spark will automatically handle skew!
result = customers.join(orders, "customer_id")
```

## Summary

**"Add salt keys"** = Add random numbers to split skewed data across multiple tasks

- **Why?** One key has too much data, causing one slow task
- **How?** Add random suffix to keys, distribute work evenly
- **Result:** All tasks finish in similar time ✅

