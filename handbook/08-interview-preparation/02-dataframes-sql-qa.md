# DataFrame & SQL Operations Q&A

40+ questions on DataFrame operations, window functions, aggregations, and SQL queries.

## Core DataFrame Operations

### Q1: Difference between filter() and where()?
**Answer**: No difference - they're aliases. Both filter data using predicates.

### Q2: How do you handle null values?
**Answer**:
```python
df.na.fill(0)  # Fill all nulls with 0
df.na.fill({"age": 0, "name": "Unknown"})
df.filter(df.col.isNotNull())  # Filter out nulls
df.coalesce(F.col("col1"), F.col("col2"))  # First non-null
```

### Q3: How to remove duplicates?
**Answer**:
```python
df.dropDuplicates()  # All columns
df.dropDuplicates(["email"])  # Specific columns
```

## Window Functions

### Q4: Explain Window specification
**Answer**:
```python
from pyspark.sql.window import Window

window = Window.partitionBy("dept").orderBy(F.desc("salary"))
df.withColumn("rank", F.rank().over(window))
```

### Q5: Difference between rank(), dense_rank(), row_number()
**Answer**:
- `rank()`: 1, 2, 2, 4 (skips numbers after tie)
- `dense_rank()`: 1, 2, 2, 3 (no gaps)
- `row_number()`: 1, 2, 3, 4 (each row unique)

## Joins

### Q6: How to broadcast a small table?
**Answer**:
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

### Q7: Difference between join types?
**Answer**:
- `inner`: Only matching rows
- `left`: All left rows + matches
- `right`: All right rows + matches
- `outer`: All rows from both
- `left_semi`: Left rows with right match (no right columns)
- `left_anti`: Left rows without right match

## Aggregations

### Q8: How to aggregate multiple ways?
**Answer**:
```python
df.groupBy("dept").agg(
    F.count("*").alias("total"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary")
)
```

### Q9: How to use HAVING clause?
**Answer**:
```python
df.groupBy("dept") \
    .agg(F.count("*").alias("count")) \
    .filter(F.col("count") > 5)  # HAVING equivalent
```

## SQL

### Q10: How to use SQL on DataFrame?
**Answer**:
```python
df.createOrReplaceTempView("table")
spark.sql("SELECT * FROM table WHERE age > 30").show()
```

---

See [03-performance-qa.md](03-performance-qa.md) for optimization questions.
