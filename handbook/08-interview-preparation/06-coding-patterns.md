# Common Coding Patterns

15 interview-ready patterns using DataFrame and SQL.

## Pattern 1: Find second highest salary per department
```python
window = Window.partitionBy("dept").orderBy(F.desc("salary"))
df.withColumn("rank", F.dense_rank().over(window)) \
    .filter(F.col("rank") == 2) \
    .select("dept", "name", "salary")
```

## Pattern 2: Remove duplicates keeping latest
```python
window = Window.partitionBy("user_id").orderBy(F.desc("updated_at"))
df.withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")
```

## Pattern 3: Running total
```python
window = Window.partitionBy("account").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(window))
```

## Pattern 4: Pivot data
```python
df.groupBy("product").pivot("quarter").agg(F.sum("amount"))
```

## Pattern 5: Explode arrays
```python
df.select("id", F.explode("tags").alias("tag"))
```

More patterns: See [07-system-design.md](07-system-design.md)
