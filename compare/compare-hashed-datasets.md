# Comparing Hashed DataFrames in Spark

After creating the hashed versions of your dataframes (`df1_hash` and `df2_hash`), there are several effective ways to
compare them. Here's how you can do it:

## 1. Count-Based Comparison

This quick approach checks if the two datasets have the same content based on hash counts:

```python
from pyspark.sql.functions import col, count

# Count unique hashes in each dataframe
unique_hashes_df1 = df1_hash.select("row_hash").distinct().count()
unique_hashes_df2 = df2_hash.select("row_hash").distinct().count()

print(f"Unique hash count in df1: {unique_hashes_df1}")
print(f"Unique hash count in df2: {unique_hashes_df2}")

# Count total matching hashes
df1_hash.createOrReplaceTempView("df1_hash")
df2_hash.createOrReplaceTempView("df2_hash")

matching_count = spark.sql("""
    SELECT COUNT(*) 
    FROM (
        SELECT row_hash FROM df1_hash
        INTERSECT
        SELECT row_hash FROM df2_hash
    )
""").collect()[0][0]

print(f"Matching hashes: {matching_count}")
print(f"Total in df1: {df1_hash.count()}")
print(f"Total in df2: {df2_hash.count()}")
```

## 2. Finding Differences Between DataFrames

To identify specific records that exist in one dataframe but not the other:

```python
# Records in df1 but not in df2
in_df1_not_df2 = df1_hash.join(
    df2_hash.select("row_hash"),
    on="row_hash",
    how="left_anti"  # Returns rows from left df with no match in right df
)

# Records in df2 but not in df1
in_df2_not_df1 = df2_hash.join(
    df1_hash.select("row_hash"),
    on="row_hash",
    how="left_anti"
)

print(f"Records in df1 not in df2: {in_df1_not_df2.count()}")
print(f"Records in df2 not in df1: {in_df2_not_df1.count()}")

# Show sample differences if any exist
if in_df1_not_df2.count() > 0:
    print("Sample records in df1 not in df2:")
    in_df1_not_df2.show(5)

if in_df2_not_df1.count() > 0:
    print("Sample records in df2 not in df1:")
    in_df2_not_df1.show(5)
```

## 3. Comparing Full Datasets with Hash Join

For a detailed view of differences, joining the original dataframes using the hash:

```python
# Join the original dataframes on key columns plus the hash
join_cols = ["id"]  # Your key columns
full_comparison = df1_hash.join(
    df2_hash,
    on=join_cols,
    how="full_outer"
)

# Identify rows with hash differences
diff_rows = full_comparison.filter(
    (col("row_hash") != col("row_hash_2")) |
    col("row_hash").isNull() |
    col("row_hash_2").isNull()
)

print(f"Total rows with differences: {diff_rows.count()}")
```

## 4. Efficient Bulk Comparison for Very Large Datasets

When datasets are extremely large, aggregate the hashes before comparison:

```python
from pyspark.sql.functions import collect_list, sort_array

# Create hash frequency tables
hash_freq_df1 = df1_hash.groupBy("row_hash").count().withColumnRenamed("count", "count_df1")
hash_freq_df2 = df2_hash.groupBy("row_hash").count().withColumnRenamed("count", "count_df2")

# Join the frequency tables
hash_comparison = hash_freq_df1.join(
    hash_freq_df2,
    on="row_hash",
    how="full_outer"
).fillna(0)

# Identify differences in frequencies
hash_comparison = hash_comparison.withColumn(
    "difference",
    col("count_df1") - col("count_df2")
)

# Show hashes with different frequencies
hash_comparison.filter(col("difference") != 0).show()
```

## 5. Percentage Similarity Calculation

To get a high-level similarity measure between datasets:

```python
# Total records in both dataframes
total_df1 = df1_hash.count()
total_df2 = df2_hash.count()

# Records with matching hashes in both
common_hashes = df1_hash.select("row_hash").intersect(df2_hash.select("row_hash"))
common_count = common_hashes.count()

# Calculate similarity percentages
similarity_to_df1 = (common_count / total_df1) * 100 if total_df1 > 0 else 0
similarity_to_df2 = (common_count / total_df2) * 100 if total_df2 > 0 else 0

print(f"Dataset similarity: df1 is {similarity_to_df1:.2f}% contained in df2")
print(f"Dataset similarity: df2 is {similarity_to_df2:.2f}% contained in df1")
```

## Explain Intersect

The `INTERSECT` operation in SQL is powerful for comparing sets of data. Let me explain how it works in the context of
your hash comparison:

## INTERSECT Explained

`INTERSECT` is a set operation that returns only the distinct rows that appear in both result sets. In your query:

```sql
SELECT row_hash FROM df1_hash 
INTERSECT 
SELECT row_hash FROM df2_hash
```

This operation:

1. Takes all unique `row_hash` values from `df1_hash`
2. Takes all unique `row_hash` values from `df2_hash`
3. Returns only the hash values that exist in both sets
4. Automatically deduplicates results (returns distinct values only)

## How It Works Under the Hood

When Spark executes an `INTERSECT` query:

1. It first computes both input queries independently
2. Applies a `DISTINCT` operation to each result set (if not already distinct)
3. Performs a hash-based set intersection algorithm
4. Returns the matching values

## Visual Representation

If we visualize the data as sets:

```
df1_hash row_hashes: {A, B, C, D, E}
df2_hash row_hashes: {C, D, E, F, G}

INTERSECT result: {C, D, E}
```

## INTERSECT vs. JOIN

You could accomplish something similar with a join, but `INTERSECT` is more concise:

```sql
-- Equivalent to INTERSECT but using JOIN
SELECT DISTINCT a.row_hash
FROM df1_hash a
JOIN df2_hash b ON a.row_hash = b.row_hash
```

The `INTERSECT` operation is generally more efficient because:

- It's optimized specifically for set operations
- The query optimizer can apply special strategies for set operations
- It requires less code and is less prone to errors

## Performance Considerations

For very large datasets:

- `INTERSECT` performs well but could be expensive for extremely large tables
- Spark may need to shuffle data across the cluster to compute the intersection
- Consider using broadcast joins if one dataset is much smaller than the other

## Practical Applications

In your hash comparison, the `INTERSECT` operation helps you quickly identify identical records across both dataframes.
The count of this result tells you how many rows have exactly the same content in both datasets.

