# Detect primary key candidates for large Parquet datasets

### PySpark Approach for Primary Key Detection

1. **Load Parquet files into DataFrames**:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PrimaryKeyDetection").getOrCreate()

df1 = spark.read.parquet("path/to/your/first.parquet")
df2 = spark.read.parquet("path/to/your/second.parquet")
```

2. **Calculate distinct counts and null counts for each column**:

```python
from pyspark.sql.functions import col, count, countDistinct, when


def analyze_columns(df):
    total_rows = df.count()
    analysis = {}
    for column in df.columns:
        distinct_count = df.select(countDistinct(col(column))).collect()[0][0]
        null_count = df.select(count(when(col(column).isNull(), column))).collect()[0][0]
        analysis[column] = {
            'distinct_count': distinct_count,
            'null_count': null_count,
            'total_rows': total_rows,
            'is_candidate': distinct_count == total_rows and null_count == 0
        }
    return analysis


column_analysis = analyze_columns(df1)
```

3. **Identify candidate primary key columns**:

```python
candidates = [col for col, stats in column_analysis.items() if stats['is_candidate']]
print("Candidate primary keys:", candidates)
```

4. **For composite keys, check combinations (optional and more costly)**:

```python
from itertools import combinations


def check_composite_keys(df, max_comb_size=3):
    total_rows = df.count()
    for r in range(2, max_comb_size + 1):
        for cols in combinations(df.columns, r):
            distinct_count = df.select(*cols).distinct().count()
            null_rows = df.filter(" OR ".join([f"{c} IS NULL" for c in cols])).count()
            if distinct_count == total_rows and null_rows == 0:
                return cols
    return None


composite_key = check_composite_keys(df1)
print("Composite primary key:", composite_key)
```

### Notes

- Counting distinct values on very large datasets can be expensive, so consider sampling if datasets are very large.
- Use Spark's distributed computation capabilities to handle large data efficiently.
- Once primary key candidates are found, you can proceed to join and compare the datasets based on them.

## explain combinations(df.columns, r)

The `combinations(df.columns, r)` call uses Python’s `itertools.combinations` function. It takes an iterable (here, the
list of DataFrame columns) and an integer `r`, then generates all possible unique combinations (without repetition) of
length `r` from that iterable.

For example, if your DataFrame has columns `[A, B, C]` and you do `combinations(df.columns, 2)`, it will generate:

- (A, B)
- (A, C)
- (B, C)

This is useful when you want to check combinations of 2 columns at a time for uniqueness (as candidate composite primary
keys) without considering order or repetitions.

In short, it produces all unique subsets of columns of size `r` for combinational checks, making it practical to test
multi-column uniqueness by iterating over these combinations systematically.
