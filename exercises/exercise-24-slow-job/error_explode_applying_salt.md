# Fix for UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS Error

The error occurs because Spark doesn't support `explode()` inside conditional expressions (CASE WHEN). This likely
happens when you're trying to conditionally apply salting based on a skew flag.

## Root Cause

You're probably doing something like this (which causes the error):

```python
# ❌ THIS CAUSES THE ERROR
df_right.withColumn(
    "salt_key",
    F.when(F.col("is_skewed"), F.explode(salt_array))  # ❌ NESTED GENERATOR!
    .otherwise(F.lit(0))
)
```

## Solution: Separate DataFrames for Skewed/Non-Skewed Data

Replace your `apply_salting` function with this corrected version:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def apply_salting(df_left, df_right, key_cols, salt_factor):
    """Apply salting technique to handle skewed data"""
    print(f"\n{'=' * 60}")
    print(f"Applying salting with factor: {salt_factor}")
    print(f"{'=' * 60}")

    # Add random salt to left table
    df_left_salted = df_left.withColumn(
        "salt_key",
        (F.rand(seed=42) * salt_factor).cast(IntegerType())
    )

    print(f"Left table: Added random salt (0-{salt_factor - 1})")

    # Method 1: Using crossJoin (Recommended for large salt_factor)
    if salt_factor > 100:
        salt_df = spark.range(salt_factor).select(F.col("id").alias("salt_key").cast(IntegerType()))
        df_right_exploded = df_right.crossJoin(salt_df)
    else:
        # Method 2: Using explode with array (Better for small salt_factor)
        # Create the array once, not inside withColumn
        salt_values = list(range(salt_factor))
        df_right_exploded = df_right.withColumn(
            "salt_values_array",
            F.array([F.lit(i) for i in salt_values])
        ).withColumn(
            "salt_key",
            F.explode(F.col("salt_values_array"))
        ).drop("salt_values_array")

    print(f"Right table: Exploded with {salt_factor} salt values")

    return df_left_salted, df_right_exploded
```

## Complete Working Solution

Here's the full corrected approach for your skewed join:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ===== CONFIGURATION =====
NUM_SALTS = 200
SKEW_THRESHOLD = 1000000


# ===== FUNCTION: Apply Salting =====
def apply_salting_v2(df_left, df_right, salt_factor):
    """Apply salting without nested generators"""

    # Add random salt to left table
    df_left_salted = df_left.withColumn(
        "salt_key",
        (F.rand(seed=42) * salt_factor).cast(IntegerType())
    )

    # Create salt dimension table and crossJoin
    salt_df = spark.range(salt_factor).select(
        F.col("id").alias("salt_key").cast(IntegerType())
    )

    df_right_exploded = df_right.crossJoin(F.broadcast(salt_df))

    return df_left_salted, df_right_exploded


# ===== MAIN LOGIC =====
print("Step 1: Identifying skewed keys...")
skewed_keys = (
    df_tempo.groupBy("cusip", "effectivedate")
    .count()
    .filter(F.col("count") > SKEW_THRESHOLD)
    .select("cusip", "effectivedate")
    .cache()
)

skew_count = skewed_keys.count()
print(f"Found {skew_count} skewed key combinations")

if skew_count > 0:
    print("\nStep 2: Splitting datasets...")

    # Split LEFT table (df_tempo)
    df_tempo_skewed = df_tempo.join(
        F.broadcast(skewed_keys),
        on=["cusip", "effectivedate"],
        how="inner"
    )
    df_tempo_normal = df_tempo.join(
        F.broadcast(skewed_keys),
        on=["cusip", "effectivedate"],
        how="left_anti"
    )

    # Split RIGHT table (arm_loan_history)
    arm_loan_history_skewed = arm_loan_history.join(
        F.broadcast(skewed_keys),
        on=["cusip", "effectivedate"],
        how="inner"
    )
    arm_loan_history_normal = arm_loan_history.join(
        F.broadcast(skewed_keys),
        on=["cusip", "effectivedate"],
        how="left_anti"
    )

    print("\nStep 3: Joining normal data...")
    result_normal = df_tempo_normal.join(
        arm_loan_history_normal,
        on=["cusip", "effectivedate"],
        how="inner"  # or your join type
    )

    print(f"\nStep 4: Applying salting to skewed data (factor={NUM_SALTS})...")
    df_tempo_skewed_salted, arm_loan_history_skewed_exploded = apply_salting_v2(
        df_tempo_skewed,
        arm_loan_history_skewed,
        NUM_SALTS
    )

    print("\nStep 5: Joining salted skewed data...")
    result_skewed = df_tempo_skewed_salted.join(
        arm_loan_history_skewed_exploded,
        on=["cusip", "effectivedate", "salt_key"],
        how="inner"
    ).drop("salt_key")

    print("\nStep 6: Combining results...")
    final_result = result_normal.union(result_skewed)

else:
    print("No skew detected, performing standard join...")
    final_result = df_tempo.join(
        arm_loan_history,
        on=["cusip", "effectivedate"],
        how="inner"
    )

print("\n" + "=" * 60)
print("Join completed successfully!")
print("=" * 60)
```

## Alternative: More Memory-Efficient Salting for Very Large Salt Factors

If `NUM_SALTS = 200` is causing memory issues, use this optimized version:

```python
def apply_salting_optimized(df_left, df_right, salt_factor, batch_size=50):
    """
    Apply salting in batches to reduce memory pressure
    """
    df_left_salted = df_left.withColumn(
        "salt_key",
        (F.rand(seed=42) * salt_factor).cast(IntegerType())
    )

    # Process in batches
    result_parts = []
    for start in range(0, salt_factor, batch_size):
        end = min(start + batch_size, salt_factor)
        salt_values = list(range(start, end))

        df_right_batch = df_right.withColumn(
            "salt_key",
            F.explode(F.array([F.lit(i) for i in salt_values]))
        )

        result_batch = df_left_salted.filter(
            F.col("salt_key").between(start, end - 1)
        ).join(
            df_right_batch,
            on=["cusip", "effectivedate", "salt_key"],
            how="inner"
        )

        result_parts.append(result_batch)

    # Union all batches
    from functools import reduce
    final_result = reduce(lambda df1, df2: df1.union(df2), result_parts)

    return final_result.drop("salt_key")
```

## Quick Fix If You Just Need to Get It Running

If you're short on time, use this simplified version:

```python
# Replace your apply_salting call with this direct implementation:

# For skewed data only
df_tempo_skewed_salted = df_tempo_skewed.withColumn(
    "salt_key",
    (F.rand(seed=42) * 200).cast(IntegerType())
)

# Use crossJoin instead of explode
salt_dimension = spark.range(200).select(F.col("id").alias("salt_key").cast(IntegerType()))
arm_loan_history_skewed_exploded = arm_loan_history_skewed.crossJoin(F.broadcast(salt_dimension))

# Now join
result_skewed = df_tempo_skewed_salted.join(
    arm_loan_history_skewed_exploded,
    on=["cusip", "effectivedate", "salt_key"],
    how="inner"
).drop("salt_key")
```

## Key Takeaways

1. **Never use `explode()` inside `F.when()` or CASE expressions**
2. **Use `crossJoin` for large salt factors** (>100)
3. **Use separate DataFrames** for skewed vs non-skewed data
4. **Always broadcast** small dimension tables like salt_df

This should resolve your error and get your job running successfully!