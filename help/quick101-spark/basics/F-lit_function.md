In PySpark, `F.lit()` stands for **"literal."** It is a function used to turn a regular Python value (like a string,
integer, or boolean) into a **Spark Column object**.

### Why do we need it?

Spark is a distributed engine. When you write a transformation, Spark expects to work with columns. If you try to
compare a column to a raw Python variable without `lit()`, Spark sometimes gets confused because it's looking for
another column with that name rather than the value itself.

Think of `F.lit()` as a "wrapper" that makes a Python variable "Spark-compatible."

---

### Common Use Cases

#### 1. Comparing a Column to a Constant

In your specific case, you want to compare a date column to a single date string.

```python
# This works because lit() makes the string act like a column with one value
df.filter(F.col("release_date") >= F.lit("2026-02-01"))

```

#### 2. Adding a New Column with a Constant Value

If you want every row in a dataset to have a column called "status" with the value "active":

```python
df = df.withColumn("status", F.lit("active"))

```

#### 3. Using Python variables in Spark functions

If you have a variable calculated in Python and want to use it inside a Spark function like `when()` or `concat()`:

```python
threshold = 100
df.withColumn("is_high", F.when(F.col("price") > F.lit(threshold), True).otherwise(False))

```

---

### What happens under the hood?

When you use `F.lit("2026-02-01")`, Spark effectively creates a "virtual column" where every single row contains that
exact same value. This allows the distributed workers to perform the comparison across all partitions simultaneously.

### Summary Table

| Feature     | Python Variable       | Spark Column (`F.lit`)         |
|-------------|-----------------------|--------------------------------|
| **Storage** | Local Memory (Driver) | Distributed Memory (Executors) |
| **Usage**   | Logic/Control flow    | Data transformations/Filtering |
| **Example** | `my_val = 10`         | `F.lit(10)`                    |

## `F.lit` and `F.when` for conditional logic?

Using `F.lit()` within `F.when()` is where you really see its power for data labeling and cleaning. In Spark, `F.when()`
acts like an `IF-THEN-ELSE` statement, but it requires both the condition and the result to be in a format Spark
understands.

If you want to create a "Category" column based on a numeric value, you use `F.lit()` to provide the "labels" (the
then/else values).

### Example: Categorizing Row Counts

Imagine you want to label your datasets as "High Volume" or "Low Volume" based on the row counts we just calculated.

```python
from pyspark.sql import functions as F

# Assume we are processing a DataFrame of logs
threshold = 1000

df_with_labels = df.withColumn("volume_category",
                               F.when(F.col("row_count") > F.lit(threshold), F.lit("High Volume"))
                               .otherwise(F.lit("Low Volume"))
                               )

```

### Why `F.lit()` is mandatory here:

1. **`F.col("row_count") > F.lit(threshold)`**: You are comparing a column to a fixed number.
2. **`F.lit("High Volume")`**: You are telling Spark: "If the condition is met, fill this cell with this specific
   string." Without `lit()`, Spark would look for a column named `High Volume`, fail to find it, and throw an error.

---

### Comparison: Standard Logic vs. Spark Logic

| Logic Type            | Standard Python  | PySpark (Distributed)                 |
|-----------------------|------------------|---------------------------------------|
| **Simple Assignment** | `val = "Active"` | `F.lit("Active")`                     |
| **Conditional**       | `if x > 10: ...` | `F.when(F.col("x") > F.lit(10), ...)` |
| **Null Handling**     | `x = None`       | `F.lit(None)`                         |

### Pro-Tip: The "Hidden" Lit

Interestingly, some PySpark functions have "implicit" literals. For example, in `df.filter(F.col("name") == "John")`,
Spark is smart enough to turn `"John"` into a literal for you. However, in `withColumn` or `when/otherwise`, Spark is
much stricter, and using `F.lit()` explicitly is the best practice to avoid "Column not found" errors.

## handle `None` values or fill missing data

Handling `None` or `Null` values with `F.lit()` is essential when cleaning S3 data. In Spark, you can't just use a
Python `None` in a column operation; you have to wrap it so Spark knows to treat it as a **Null Type** within the
DataFrame schema.

### 1. Filling Missing Values with a Default

If one of your S3 datasets has missing dates, you might want to fill them with a "placeholder" literal so your filter
doesn't skip them entirely.

```python
from pyspark.sql import functions as F

# If the date is missing, fill it with a very old date literal 
# so the '>= run_date' logic handles it predictably.
df_cleaned = df.withColumn(
    "release_date",
    F.coalesce(F.col("release_date"), F.lit("1900-01-01"))
)

```

### 2. Standardizing Nulls across Datasets

If you are merging multiple S3 buckets and one bucket is missing a column entirely (e.g., `dataset1` has
`effective_date` but `dataset2` doesn't), you can use `F.lit(None)` to create a placeholder column so the schemas match.

```python
# Create a dummy column of type String that is entirely Null
df_with_empty_col = df.withColumn("effective_date", F.lit(None).cast("string"))

```

---

### Why use `.cast()` with `F.lit(None)`?

When you use `F.lit(None)`, Spark doesn't know if that "nothing" should be an Integer, a String, or a Date. It defaults
to a "Void" type. To avoid errors during a `union` or a `write` operation, it is a best practice to cast it:

* `F.lit(None).cast("string")`
* `F.lit(None).cast("date")`
* `F.lit(None).cast("double")`

### Data Transformation Workflow

---

### Summary Checklist for your S3 Loop

Since you are looping through different S3 buckets, here is a quick "Safety Check" to add inside your function:

1. **Check Column Existence:** Use `if date_col in df.columns:` before filtering.
2. **Handle Data Types:** If the column is a string in S3, use `F.to_date(F.col(date_col))` before comparing it to your
   `F.lit()` date.
3. **Null Handling:** Use `F.col(date_col).isNotNull()` if you want to exclude missing records from your counts.
