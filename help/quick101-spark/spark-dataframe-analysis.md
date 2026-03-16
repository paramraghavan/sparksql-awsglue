All the essential Spark DataFrame diagnostics in one place:

---

## Explain Plan

```python
df.explain()                  # physical plan only — terse
df.explain(mode="simple")     # same as above

df.explain(mode="extended")   # 4 plans in sequence:
                              #   1. Parsed Logical Plan      ← what you wrote
                              #   2. Analyzed Logical Plan    ← types resolved
                              #   3. Optimized Logical Plan   ← rules applied (predicate pushdown, etc.)
                              #   4. Physical Plan            ← what Spark will actually execute

df.explain(mode="formatted")  # Physical plan only — but with:
                              #   • indented tree layout
                              #   • node IDs you can cross-reference with Spark UI
                              #   • output schema per node
                              #   • codegen stage boundaries marked

df.explain(mode="cost")       # Physical plan + Statistics per node
                              #   sizeInBytes, rowCount — where the optimizer got its estimates from

# Codegen — see the generated Java code per stage
df.explain(mode="codegen")
```

## Why physical is the one that matters
Logical plan        →   what you ASKED for
Physical plan       →   what Spark will ACTUALLY DO

## Number of Partitions

```python
df.rdd.getNumPartitions()

# After a shuffle you can also check
df.rdd.glom().map(len).collect()  # rows per partition — spots skew instantly
```

---

## Row Count

```python
df.count()  # triggers a full scan — expensive
df.cache();
df.count()  # cache first if you'll reuse the DF
```

---

## Columns

```python
df.columns  # list of column names
df.dtypes  # list of (name, type) tuples
df.printSchema()  # tree view with nullability
df.schema  # StructType object — programmatic access
```

---

## DataFrame Size & Avg Row Size

Spark doesn't track this natively, but the query plan's cost stats do:

```python
# Option 1 — read from optimized plan (no extra scan)
from pyspark.sql.functions import col

df.explain(mode="cost")
# Look for: Statistics(sizeInBytes=X, rowCount=Y) in the output

# Option 2 — compute explicitly (triggers an action)
from pyspark.sql.functions import lit
import sys

# Serialize a sample to estimate size
sample = df.limit(1000).toPandas()
sample_bytes = sum(sys.getsizeof(row) for _, row in sample.iterrows())
avg_row_bytes = sample_bytes / len(sample) if len(sample) > 0 else 0

total_rows = df.count()
est_size_mb = (avg_row_bytes * total_rows) / (1024 ** 2)

print(f"Rows          : {total_rows:,}")
print(f"Avg row size  : {avg_row_bytes:.0f} bytes")
print(f"Est. total    : {est_size_mb:.1f} MB")

# Option 3 — from Spark UI's cached RDD size (if df is cached)
df.cache()
df.count()  # materialise
for rdd in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(rdd.name(), "→", rdd.memSize())
```

---

## All-in-one diagnostic cell

```python
def df_info(df, label="DataFrame", sample_n=1000):
    import sys
    print(f"\n{'═' * 55}")
    print(f"  {label}")
    print(f"{'═' * 55}")
    print(f"  Partitions : {df.rdd.getNumPartitions():,}")
    print(f"  Columns    : {len(df.columns)}  → {df.columns}")

    row_count = df.count()
    print(f"  Rows       : {row_count:,}")

    sample = df.limit(sample_n).toPandas()
    if len(sample):
        avg_bytes = sum(sys.getsizeof(r) for _, r in sample.iterrows()) / len(sample)
        est_mb = avg_bytes * row_count / 1024 ** 2
        print(f"  Avg row    : {avg_bytes:.0f} bytes")
        print(f"  Est. size  : {est_mb:,.1f} MB")

    print(f"\n  Schema:")
    df.printSchema()

    print(f"\n  Physical plan (formatted):")
    df.explain(mode="formatted")


# Usage
df_info(df_joined, "after customer join")
```

---

**Quick rule of thumb for diagnosis:** run `explain(mode="cost")` first — if the `sizeInBytes` on a join's child node is
under your `autoBroadcastJoinThreshold` in one run but over it in the other, that's why one uses `BroadcastHashJoin` and
the other falls back to `SortMergeJoin`.