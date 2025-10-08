You can apply it to **all columns** without specifying names! Here are the options:

## Fill All Columns

```python
# Fill all numeric columns with 0
df.na.fill(0).write.parquet("s3://...")

# Fill all string columns with empty string
df.na.fill("").write.parquet("s3://...")

# Fill both numeric and string columns
df.na.fill(0).na.fill("").write.parquet("s3://...")
```

## Fill Specific Columns (Dictionary)

```python
# Only fill specific columns
df.na.fill({
    "column1": 0,
    "column2": "unknown",
    "column3": -1
}).write.parquet("s3://...")
```

## Fill by Data Type

```python
# Fill all columns, but different defaults by type
df.na.fill(0)  # Fills all numeric (int, double, float, long)
.na.fill("")  # Fills all string columns
.write.parquet("s3://...")
```

## Most Practical for Your Case

Since you have a complex dataframe and just want to avoid null issues:

```python
# Quick fix - fill everything
df.na.fill(0).na.fill("").na.fill(False).write.parquet("s3://...")

# Or drop rows with any nulls (if that's acceptable)
df.na.drop().write.parquet("s3://...")
```

**The single value approach applies to all columns of compatible types**, so you don't need to list every column name
individually!