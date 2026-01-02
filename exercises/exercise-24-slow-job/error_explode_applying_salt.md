Here's the corrected code:

```python
# ❌ WRONG - This causes the error:
# df_right.withColumn(
#     "salt_key",
#     F.when(F.col("is_skewed"), F.explode(salt_array))
#      .otherwise(F.lit(0))
# )

# ✅ CORRECT - Split into separate operations:

# Split the dataframe based on skew flag
df_right_skewed = df_right.filter(F.col("is_skewed") == True)
df_right_normal = df_right.filter(F.col("is_skewed") == False)

# Apply explode ONLY to skewed data
df_right_skewed_exploded = df_right_skewed.withColumn(
    "salt_key",
    F.explode(salt_array)
)

# Add salt_key = 0 to normal data
df_right_normal_with_salt = df_right_normal.withColumn(
    "salt_key",
    F.lit(0)
)

# Combine both
df_right_final = df_right_skewed_exploded.union(df_right_normal_with_salt)
```

Or, if you prefer using `crossJoin` (more efficient for large salt factors):

```python
# ✅ ALTERNATIVE using crossJoin:

df_right_skewed = df_right.filter(F.col("is_skewed") == True)
df_right_normal = df_right.filter(F.col("is_skewed") == False)

# Create salt dimension
salt_df = spark.range(len(salt_array)).select(
    F.col("id").alias("salt_key").cast(IntegerType())
)

# CrossJoin for skewed data
df_right_skewed_exploded = df_right_skewed.crossJoin(F.broadcast(salt_df))

# Add salt_key = 0 for normal data
df_right_normal_with_salt = df_right_normal.withColumn("salt_key", F.lit(0))

# Combine
df_right_final = df_right_skewed_exploded.union(df_right_normal_with_salt)
```

**Key principle**: You cannot use generator functions like `explode()` inside conditional expressions. Always **filter
first, then apply the generator**.