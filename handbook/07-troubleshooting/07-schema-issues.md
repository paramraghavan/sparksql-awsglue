# Schema Issues

## Symptom

- "Cannot cast ... to ..."
- Schema mismatch errors
- Null values where not expected
- Type conversion fails

## Common Causes

1. **Type mismatch** - Column is string but code treats as int
2. **Null handling** - Code doesn't handle nulls
3. **Schema evolution** - Source schema changed
4. **Type inference** - Inferring wrong type from data

## Solutions

### Solution 1: Define Explicit Schema
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.schema(schema).csv("data.csv")
```

### Solution 2: Cast Types Explicitly
```python
from pyspark.sql.functions import col, to_date

df = df.withColumn("age", col("age").cast("int")) \
       .withColumn("birth_date", to_date(col("birth_date"), "yyyy-MM-dd"))
```

### Solution 3: Handle Nulls
```python
df = df.na.fill({"age": 0, "name": "Unknown"})
```

### Solution 4: Debug Schema
```python
df.printSchema()
df.dtypes
```

---

**See Also**: [DataFrame Basics](../01-fundamentals/02-dataframes-basics.md)
