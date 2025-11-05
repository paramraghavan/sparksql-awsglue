If the key columns are not known upfront, the comparison approach needs to adapt to identify or approximate keys to
effectively compare large Parquet files without explicit join keys.

### Approaches When Key Columns Are Unknown

1. **Automatic Key Discovery (Heuristics)**
    - Identify candidate key columns by checking columns with unique or near-unique values in each file.
    - Use Spark aggregates like count distinct per column or combinations of columns to find uniqueness patterns.
    - This may require progressively testing combinations of columns until a suitable key is found, which can be
      expensive for large files with many columns.

2. **Full Outer Join on All Columns (Not Recommended for Large Data)**
    - Perform a full outer join on all columns to identify mismatches.
    - This is typically computationally infeasible for large datasets due to combinatorial explosion and high memory
      use.

3. **Row-wise Comparison via Hashing**
    - Generate hash values for every row based on concatenation of all or selected columns.
    - Compare hash sets between two datasets to find mismatched rows.
    - This approach avoids needing explicit keys but is less granular (only flags row-level mismatches, not column-level
      differences).
    - You can further isolate columns contributing to differences by partial hashing or second-pass diff.

4. **Sampling & Manual Inspection**
    - Sample some rows and columns from both files to manually identify potential keys or columns with stable unique
      identifiers.
    - Use domain knowledge if available to reduce candidate keys.

### Practical Recommendation for Unknown Keys

- Start by inspecting the dataset schemas (column names, data types).
- Compute distinct counts and null counts for each column in both datasets.
- Identify columns with high distinct value ratios and low null counts as possible keys.
- Use these candidate keys in Spark to join datasets for detailed column-wise comparison.
- For initial quick checks or very large data, consider row hash comparison to identify if datasets differ at all.

### Example: Identify Candidate Keys with Spark

```python
from pyspark.sql.functions import col, countDistinct, count, lit

df1 = spark.read.parquet("s3://bucket/path/file1.parquet")
df2 = spark.read.parquet("s3://bucket/path/file2.parquet")

total_count = df1.count()

candidate_keys = []
for c in df1.columns:
    distinct_count = df1.select(countDistinct(col(c))).collect()[0][0]
    null_count = df1.filter(col(c).isNull()).count()
    distinct_ratio = distinct_count / total_count
    if distinct_ratio > 0.9 and null_count == 0:  # Heuristic
        candidate_keys.append(c)

print("Candidate key columns:", candidate_keys)
```

Use identified candidate keys for comparison join and difference reporting.

If no reliable key can be found, fallback to row hashing methods for coarse comparison.
