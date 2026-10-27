# PySpark 101 Examples For SQL And Database Users

This guide covers the PySpark operations used in most beginner and intermediate data engineering jobs. It is written for
people who already know SQL, tables, joins, keys, filters, aggregates, and ETL concepts.

## 1. What You Will Learn

This guide is organized as a study path for SQL/database users who want their first data engineering job. Start with the
local PySpark sections first. After you are comfortable reading, transforming, joining, and writing files locally, move
to LocalStack and AWS Glue concepts.

Recommended study order:

1. Learn the SQL-to-PySpark mental model.
2. Run local examples with small data.
3. Practice common transformations: filter, join, aggregate, window functions.
4. Write Parquet and understand partitioned output.
5. Learn why Spark CRUD is different from database CRUD.
6. Practice LocalStack S3 as a local AWS simulator.
7. Learn Glue Data Catalog for interview and real-world data lake work.

By the end, you should be able to explain and build a small batch ETL pipeline: read raw files, clean and join data,
write partitioned Parquet, validate output, and describe how the dataset would be cataloged for Athena or Glue.

## 2. SQL To PySpark Mental Model

For SQL users, the easiest way to learn PySpark is to map familiar SQL operations to DataFrame operations.

- A Spark DataFrame feels like a SQL table or view.
- Transformations build a plan. Examples: `select`, `filter`, `join`, `groupBy`.
- Actions run the plan. Examples: `show`, `count`, `write`, `collect`.
- Spark is distributed, so output is usually a folder with many `part-*` files.
- S3 is object storage, not a database. Updates usually mean rewriting files or partitions unless you use Iceberg,
  Delta, or Hudi.

### PySpark To SQL Cheat Sheet


| SQL                       | PySpark                                                |
| ------------------------- | ------------------------------------------------------ |
| `select a, b from t`      | `df.select("a", "b")`                                  |
| `where amount > 100`      | `df.where(F.col("amount") > 100)`                      |
| `group by category`       | `df.groupBy("category").agg(...)`                      |
| `count(*)`                | `F.count("*")`                                         |
| `sum(amount)`             | `F.sum("amount")`                                      |
| `case when`               | `F.when(...).otherwise(...)`                           |
| `left join`               | `left.join(right, key, "left")`                        |
| `union all`               | `df1.unionByName(df2)`                                 |
| `row_number() over (...)` | `F.row_number().over(Window...)`                       |
| `create table as select`  | `df.write.saveAsTable(...)` or `df.write.parquet(...)` |

For guidance on choosing either style, see [When to use Spark SQL vs DataFrame functions](#when-to-use-spark-sql-vs-pyspark-dataframe-functions) after the sample data.

## 3. Setup Pattern Used In Examples

`F` below is an alias for `pyspark.sql.functions`, the module containing built-in Spark column functions. `Window` defines window specifications, and `SparkSession` is the entry point for working with Spark.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# local = run Spark on your own machine, not on a cluster.
# [2] = use 2 local worker threads.
spark = (
    SparkSession.builder
    .appName("pyspark-101")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
```

For local files:

```python
orders_path = "data/input/orders.csv"
customers_path = "data/input/customers.json"
```

For LocalStack S3, skip this until you reach the LocalStack sections later in the guide:

```python
spark = (
    SparkSession.builder
    .appName("pyspark-101-localstack")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
```

Run S3 examples with:

```bash
spark-submit \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  jobs/your_job.py
```

## 4. Create Sample Data

You can practice in two ways:

- Create DataFrames directly in code.
- Create small files under `data/input/` and read them like a real ETL job.

For interview and job practice, file-based examples are more realistic because most data engineering work starts by
reading files, tables, or streams.

Create sample files:

```bash
mkdir -p data/input data/output

cat > data/input/orders.csv <<'EOF'
order_id,customer_id,order_date,status,category,amount
1,C001,2026-01-01,COMPLETE,books,35.50
2,C002,2026-01-01,COMPLETE,electronics,299.99
3,C001,2026-01-02,CANCELLED,books,15.00
4,C003,2026-01-02,COMPLETE,grocery,42.25
5,C002,2026-01-03,COMPLETE,electronics,99.99
6,C004,2026-01-03,RETURNED,grocery,18.75
EOF

cat > data/input/customers.json <<'EOF'
{"customer_id":"C001","customer_name":"Asha","state":"CA","segment":"retail"}
{"customer_id":"C002","customer_name":"Ben","state":"NY","segment":"business"}
{"customer_id":"C003","customer_name":"Cara","state":"TX","segment":"retail"}
{"customer_id":"C004","customer_name":"Dev","state":"WA","segment":"retail"}
EOF
```

Or create small DataFrames directly in code:

```python
orders = spark.createDataFrame(
    [
        (1, "C001", "2026-01-01", "COMPLETE", "books", 35.50),
        (2, "C002", "2026-01-01", "COMPLETE", "electronics", 299.99),
        (3, "C001", "2026-01-02", "CANCELLED", "books", 15.00),
        (4, "C003", "2026-01-02", "COMPLETE", "grocery", 42.25),
        (5, "C002", "2026-01-03", "COMPLETE", "electronics", 99.99),
        (6, "C004", "2026-01-03", "RETURNED", "grocery", 18.75),
    ],
    ["order_id", "customer_id", "order_date", "status", "category", "amount"],
)

customers = spark.createDataFrame(
    [
        ("C001", "Asha", "CA", "retail"),
        ("C002", "Ben", "NY", "business"),
        ("C003", "Cara", "TX", "retail"),
        ("C004", "Dev", "WA", "retail"),
    ],
    ["customer_id", "customer_name", "state", "segment"],
)
```

Register as SQL views:

```python
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")
```

SQL equivalent:

```python
spark.sql("""
select category, count(*) as order_count, sum(amount) as total_amount
from orders
group by category
""").show()
```

### What The Sample DataFrames Look Like

`createDataFrame` turns the Python tuples into rows. The list of names becomes the column headers.

Before (the Python list of tuples above) and after `orders = spark.createDataFrame(...)`:


| order_id | customer_id | order_date | status    | category    | amount |
| -------- | ----------- | ---------- | --------- | ----------- | ------ |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  |

After `customers = spark.createDataFrame(...)`:


| customer_id | customer_name | state | segment  |
| ----------- | ------------- | ----- | -------- |
| C001        | Asha          | CA    | retail   |
| C002        | Ben           | NY    | business |
| C003        | Cara          | TX    | retail   |
| C004        | Dev           | WA    | retail   |

**SQL view example.** The `spark.sql(...)` query groups `orders` by `category`:


| category    | order_count | total_amount |
| ----------- | ----------- | ------------ |
| books       | 2           | 50.5         |
| electronics | 2           | 399.98       |
| grocery     | 2           | 61.0         |

Each category row sums only that category's orders. The view is temporary, so it disappears when the Spark session ends.

### When to Use Spark SQL vs PySpark DataFrame Functions

**Choose the style your team can most easily read, write, and review. If you already know SQL, start with Spark SQL for relational business logic.** Window functions, CASE WHEN, joins, and aggregations often read naturally as SQL. Use DataFrame functions where Python makes the pipeline simpler, especially for reusable rules, dynamic column lists, and file input/output. Both styles support these transformations.

Both interfaces use Spark's structured query engine and optimizer. Equivalent built-in expressions can produce the same execution plan; neither interface is automatically faster. `spark.sql(...)` returns a DataFrame, so you can freely combine them. See the [Spark SQL and DataFrame guide](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html).


| Decision factor        | Spark SQL is a good fit when…                                                         | DataFrame functions are a good fit when…                                            |
| ---------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Familiarity with SQL   | You or the reviewers already think in SELECT, CASE, JOIN, GROUP BY, and window clauses | The team is more comfortable with Python expressions and method chains               |
| Readability            | A query or a few named CTEs show the business logic clearly                            | Named DataFrames and small helper functions make each step easier to follow          |
| Ease of use            | You can express the transformation directly without generating SQL dynamically         | You need read/write options, schema handling, or columns selected from configuration |
| Reuse                  | Shared SQL queries or views fit the team's workflow                                    | The same validation/cleanup rule must be applied across many columns or datasets     |
| Review and maintenance | Analysts and SQL developers will own the rules                                         | Python developers will maintain a reusable ingestion framework                       |
| Existing project style | The surrounding transformations are already clear SQL                                  | The surrounding code is already a clear DataFrame pipeline                           |

**A practical starting point for SQL users:** read files with the DataFrame API, register a temporary view, express business rules in SQL, then use the returned DataFrame to validate or write the result. Use `F.expr` or `selectExpr` for a short SQL expression inside a DataFrame pipeline.

Choose locally within a job: a window calculation can stay in SQL while a configurable validation step uses DataFrame functions. Avoid switching styles for every small expression, and split long SQL queries or long method chains into named steps. The shortest code is not always the easiest to maintain.

**Example: the same CASE WHEN and window in both styles.** Run sections 3–4 first, including the sample `orders` DataFrame. These examples label orders of at least 100 as HIGH and number orders newest-first for each customer.

Spark SQL:

```python
orders.createOrReplaceTempView("orders_sql_demo")

ranked_sql = spark.sql("""
SELECT
    order_id,
    customer_id,
    amount,
    CASE WHEN amount >= 100 THEN 'HIGH' ELSE 'STANDARD' END AS amount_band,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY to_date(order_date) DESC NULLS LAST, order_id DESC
    ) AS rn
FROM orders_sql_demo
""")
```

PySpark DataFrame functions:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

latest_first = Window.partitionBy("customer_id").orderBy(
    F.to_date("order_date").desc_nulls_last(), F.col("order_id").desc()
)
ranked_df = (
    orders
    .withColumn("amount_band",
                F.when(F.col("amount") >= 100, F.lit("HIGH"))
                 .otherwise(F.lit("STANDARD")))
    .withColumn("rn", F.row_number().over(latest_first))
    .select("order_id", "customer_id", "amount", "amount_band", "rn")
)
```

Both keep all six sample rows. Only order 2 has the HIGH band. The secondary ordering by `order_id` breaks same-date ties in this sample; real pipelines need an agreed source version and a unique tie-breaker.

**Combine the styles:** apply DataFrame operations to the SQL result:

```python
latest_orders = ranked_sql.filter(F.col("rn") == 1)
latest_orders.orderBy("customer_id").show()
```

Expected latest orders: C001 → 3, C002 → 5, C003 → 4, C004 → 6. This selects the latest order of any status, including CANCELLED or RETURNED. Filtering to COMPLETE before ranking would answer a different business question.

For a single SQL expression, use `F.expr` directly:

```python
orders.select(
    "order_id",
    F.expr("CASE WHEN amount >= 100 THEN 'HIGH' ELSE 'STANDARD' END")
     .alias("amount_band"),
).show()
```

Useful habits:

- Prefer built-in Spark expressions (`F.when`, `F.sum`, `F.to_date`, SQL functions) before writing a Python UDF. A Python helper that builds a Column expression still uses native Spark operations; it is not a row-by-row Python UDF.
- Specify tie-breakers and the intended window frame. A running total over rows may need `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, or `.rowsBetween(...)`, to avoid peer-group behavior.
- Make null/default handling explicit. In the CASE example, a null amount takes the ELSE branch; add a separate missing-value rule if that is not intended.
- A temporary view belongs to its Spark session; creating one does not copy, persist, or register the data in Glue.
- Use `ranked_sql.explain("formatted")` and `ranked_df.explain("formatted")` when comparing execution plans. Syntax alone does not remove shuffle, sorting, or skew costs.

#### UDF: custom Python logic and its cost

A **user-defined function (UDF)** lets you apply custom logic that built-in Spark expressions cannot conveniently express. A regular scalar Python UDF, created with `F.udf`, evaluates Python logic for each input row processed by that expression. It can add substantial overhead on large datasets.

1. **Row-wise logic and data transfer:** Spark moves input values from an executor's JVM to a separate Python worker process and returns the results. Serialization, inter-process transfer, and Python function calls add cost. Transfers can be batched; this does not imply one JVM-to-Python round trip per row.
2. **Python runtime requirement:** Executors are JVM processes. The machines/containers running Python UDF tasks must also have a compatible Python interpreter and the UDF's dependencies available. Spark launches Python workers as needed and normally reuses them (`spark.python.worker.reuse=true`); it does not launch an interpreter for every row. Installing dependencies only on the submitting laptop is insufficient for remote executors.
3. **Optimization limits:** Spark still schedules and manages the computation, but its SQL optimizer generally cannot inspect arbitrary Python logic as it can built-in expressions. The Python boundary and reduced optimization opportunities can make a UDF slower than an equivalent native expression. Writing the call inside SQL does not remove that Python cost.

**Prefer built-in functions first:** use `F.when` for conditional logic, `F.upper`/`F.trim` for text, and native date, array, or aggregation functions where possible. A Python helper that returns these Column expressions is not a Python UDF and does not run that helper once per row.

Use a Python UDF when necessary custom logic or a Python library cannot reasonably be expressed with built-ins. Handle nulls, specify the return type, and benchmark representative data. Spark may retry or reevaluate work, so do not rely on a UDF being called exactly once or use it for per-row external writes.

**Simple example: uppercase a name using a Python UDF.** This demonstrates how a UDF works; for this task, the built-in Spark function below is the preferred choice.

Run in an existing `spark` session:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def uppercase_name(name):
    if name is None:
        return None
    return name.upper()  # Python str.upper()


uppercase_name_udf = F.udf(uppercase_name, StringType(), useArrow=False)

name_samples = spark.createDataFrame([
    (1, "Asha"),
    (2, "ben"),
    (3, None),
], "customer_id long, customer_name string")

uppercased = name_samples.withColumn(
    "name_upper_udf", uppercase_name_udf(F.col("customer_name"))
)
uppercased.orderBy("customer_id").show()
```

| customer_id | customer_name | name_upper_udf |
|---|---|---|
| 1 | Asha | ASHA |
| 2 | ben | BEN |
| 3 | NULL | NULL |

`uppercase_name` is a normal Python function. `F.udf` wraps it as a Spark Column expression with a string return type. `withColumn` applies it to the name column; `show()` triggers execution. The null check prevents calling `.upper()` on `None`.

**Preferred alternative: `pyspark.sql.functions.upper`.** Because `F` aliases `pyspark.sql.functions`, use `F.upper` directly:

```python
uppercased_builtin = name_samples.withColumn(
    "name_upper", F.upper(F.col("customer_name"))
)
uppercased_builtin.orderBy("customer_id").show()
```

This produces the same uppercase values for the sample and handles nulls automatically. **`F.upper` is generally faster and can be much faster on large datasets** because it uses Spark's native expression engine without Python UDF serialization, data transfer, or per-row Python calls. Use this UDF only to learn the mechanism; use the built-in for ordinary uppercase conversion.

Pandas UDFs process batches using Arrow and can reduce overhead for suitable vectorized logic. Arrow-optimized scalar UDFs improve data transfer but still apply scalar Python logic; neither option automatically makes arbitrary Python code as efficient as a built-in expression.

References: [Python UDF API](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html), [Python worker configuration](https://spark.apache.org/docs/3.5.6/configuration.html#runtime-environment), and [Arrow/Pandas UDFs](https://spark.apache.org/docs/3.5.6/api/python/user_guide/sql/arrow_pandas.html).

#### What does `F` mean?

```python
from pyspark.sql import functions as F
```

`F` is a conventional alias for the **`pyspark.sql.functions` module**. The import above is equivalent to `import pyspark.sql.functions as F`. It lets you write `F.col`, `F.when`, and `F.sum` without importing each function separately. `F` is not a DataFrame or a Spark session.


| Expression                              | Kind                                            | Meaning                                          |
| --------------------------------------- | ----------------------------------------------- | ------------------------------------------------ |
| `F.col("amount")`                       | Built-in function returning a Column expression | Reference the amount column                      |
| `F.lit("UNKNOWN")`                      | Built-in function returning a Column expression | Create a constant value                          |
| `F.upper("status")`                     | Built-in function returning a Column expression | Uppercase values from the status column          |
| `orders.withColumn(...)`                | DataFrame method                                | Return a DataFrame with an added/replaced column |
| `F.col("amount").cast("decimal(12,2)")` | Column method                                   | Convert the column expression to another type    |
| `orders.show()`                         | DataFrame action                                | Execute the plan and display rows                |

The DataFrame method controls the operation; the `F.*` expression describes a calculation inside it. For example, `orders.withColumn("source", F.lit("orders"))` adds the constant string `orders`; `F.col("orders")` would instead reference a column named `orders`.

#### More DataFrame examples and when to use them

Run these after the sample `orders` and `customers` DataFrames in section 4. They complement the SQL/window examples above.

**1. Standardize several source fields before applying business rules.** Use DataFrame functions for ingestion cleanup and type conversion:

```python
prepared_orders = (
    orders
    .withColumn("status", F.coalesce(F.upper(F.trim("status")), F.lit("UNKNOWN")))
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    .withColumn("source_system", F.lit("orders_feed"))
)
prepared_orders.select("order_id", "status", "order_date", "amount", "source_system").show()
```

This preserves six rows, converts the dates and amounts, and adds `source_system`. The fallback handles null statuses; empty strings need a separate rule. This sample assumes valid dates/numbers. Real ingestion should classify parse failures explicitly rather than silently accepting nulls or relying on a particular ANSI setting.

**2. Build reusable data-quality expressions.** Use Python helpers when the same rule applies to multiple columns or datasets:

```python
def has_required_value(column_name):
    value = F.col(column_name)
    return value.isNotNull() & (F.length(F.trim(value.cast("string"))) > 0)

checked_orders = orders.withColumn(
    "keys_present",
    has_required_value("order_id") & has_required_value("customer_id"),
)
accepted_orders = checked_orders.filter(F.col("keys_present"))
rejected_orders = checked_orders.filter(~F.col("keys_present"))
```

All six sample rows have both keys, so accepted = 6 and rejected = 0. The helper also rejects blank/whitespace values. It builds a native Spark Column expression; it does not execute a Python function once per row. This checks key presence, not uniqueness or whether the customer exists.

**3. Select columns from configuration.** Use DataFrame methods when the column list changes by dataset:

```python
selected_columns = ["order_id", "customer_id", "amount"]  # for example, from validated config
selected_orders = prepared_orders.select(*selected_columns).withColumn(
    "dataset", F.lit("orders")
)
selected_orders.show()
```

`*selected_columns` expands the Python list into select arguments. This is useful for reusable ingestion jobs and avoids constructing SQL strings for each column list. Validate that configured columns exist before running the transformation.

**4. Chain a join, filter, and aggregation in one Python pipeline.** Use this when it fits naturally with existing DataFrame-based code:

```python
state_totals = (
    prepared_orders
    .join(customers.select("customer_id", "state"), on="customer_id", how="left")
    .filter(F.col("status") == "COMPLETE")
    .groupBy("state")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("total_amount"),
    )
)
state_totals.orderBy("state").show()
```

Expected results:


| state | order_count | total_amount |
| ----- | ----------- | ------------ |
| CA    | 1           | 35.50        |
| NY    | 2           | 399.98       |
| TX    | 1           | 42.25        |

The customer lookup must have one row per `customer_id` for these totals; duplicate lookup keys can multiply orders. Unmatched customers remain in the left join and produce a null state group if their orders pass the filter. The same logic can be written in SQL—use whichever is easier for your team to maintain.

## 5. Read Files

Read CSV:

```python
orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/input/orders.csv")
)
```

Production habit: define schema instead of using `inferSchema`.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
])

orders = spark.read.option("header", True).schema(orders_schema).csv("data/input/orders.csv")
```

Read JSON:

```python
customers = spark.read.json("data/input/customers.json")
```

Read Parquet:

```python
orders_parquet = spark.read.parquet("data/output/orders_parquet")
```

Read from LocalStack S3:

```python
orders = spark.read.option("header", True).schema(orders_schema).csv(
    "s3a://de-lab/raw/orders/orders.csv"
)
```

### Read Files: Before And After

**Read CSV with `inferSchema`.** Before, the file on disk is plain text:

```text
order_id,customer_id,order_date,status,category,amount
1,C001,2026-01-01,COMPLETE,books,35.50
2,C002,2026-01-01,COMPLETE,electronics,299.99
...
```

After, `orders` is a DataFrame. Spark scanned the file to guess the types, so `order_date` became `date` and `order_id` became `integer`:


| order_id | customer_id | order_date | status    | category    | amount |
| -------- | ----------- | ---------- | --------- | ----------- | ------ |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  |

**Read CSV with an explicit schema.** The same file is read with `orders_schema`. The table values match the table above, but `order_date` is now a `string` because the schema says so. Spark did not guess the types. Section 7 converts `order_date` to a date.

**Read JSON.** Before, `customers.json` has one JSON object per line:

```json
{"customer_id":"C001","customer_name":"Asha","state":"CA","segment":"retail"}
```

After, `customers` is a DataFrame with one row per object. In this run the columns came out alphabetically (`segment` before `state`), not in the order they appear in the file. Use `select(...)` when you need a fixed column order.


| customer_id | customer_name | segment  | state |
| ----------- | ------------- | -------- | ----- |
| C001        | Asha          | retail   | CA    |
| C002        | Ben           | business | NY    |
| C003        | Cara          | retail   | TX    |
| C004        | Dev           | retail   | WA    |

**Read Parquet and read from LocalStack S3** need files that the sample run did not create, so no output is shown for them. Parquet keeps its schema in the file, so no `inferSchema` is needed.

## 6. Inspect Data

```python
orders.show()
orders.show(truncate=False)
orders.printSchema()
orders.columns
orders.dtypes
orders.count()
orders.limit(5).show()
orders.describe("amount").show()
```

Use `explain` when learning performance:

```python
orders.where("amount > 100").explain()
```

### Inspect Data: Before And After

These calls read a DataFrame and print information about it. They do not change it. The input is the `orders` table from section 4.

- **`orders.show()` and `orders.show(truncate=False)`** print the rows as a table. By default, `show()` shortens any cell longer than 20 characters and ends it with `...`. `truncate=False` shows long values in full and widens the table to fit.

  Example with a long text column. The DataFrame has two rows:

  ```python
  notes = spark.createDataFrame(
      [(1, "Spark makes large-scale data processing easy"), (2, "short")],
      "order_id int, note string",
  )

  notes.show()
  notes.show(truncate=False)
  ```

  `notes.show()` prints:

  ```text
  +--------+--------------------+
  |order_id|                note|
  +--------+--------------------+
  |       1|Spark makes large...|
  |       2|               short|
  +--------+--------------------+
  ```

  `notes.show(truncate=False)` prints:

  ```text
  +--------+--------------------------------------------+
  |order_id|note                                        |
  +--------+--------------------------------------------+
  |1       |Spark makes large-scale data processing easy|
  |2       |short                                       |
  +--------+--------------------------------------------+
  ```

  The `orders` table in this guide has only short values, so both calls print the same output for it.
- **`orders.printSchema()`** prints the column names and types:

  ```text
  root
   |-- order_id: long
   |-- customer_id: string
   |-- order_date: string
   |-- status: string
   |-- category: string
   |-- amount: double
  ```
- **`orders.columns`** returns the names as a list: `['order_id', 'customer_id', 'order_date', 'status', 'category', 'amount']`.
- **`orders.dtypes`** returns each name with its type: `[('order_id', 'bigint'), ('customer_id', 'string'), ..., ('amount', 'double')]`. Python integers become `bigint`, so the type differs from the CSV read above.
- **`orders.count()`** returns `6`, the number of rows.
- **`orders.limit(5).show()`** prints only the first five rows:


  | order_id | customer_id | order_date | status    | category    | amount |
  | -------- | ----------- | ---------- | --------- | ----------- | ------ |
  | 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   |
  | 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 |
  | 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   |
  | 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  |
  | 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  |
- **`orders.describe("amount").show()`** computes summary statistics for one column:


  | summary | amount             |
  | ------- | ------------------ |
  | count   | 6                  |
  | mean    | 85.24666666666667  |
  | stddev  | 109.55538337601976 |
  | min     | 15.0               |
  | max     | 299.99             |

**`orders.where("amount > 100").explain()`** prints the query plan instead of rows. It is not captured in the sample output.

## 7. Select, Rename, Cast, And Derive Columns

```python
clean_orders = (
    orders
    .select("order_id", "customer_id", "order_date", "status", "category", "amount")
    .withColumnRenamed("amount", "order_amount")
    .withColumn("order_amount", F.col("order_amount").cast("double"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_year", F.year("order_date"))
    .withColumn("load_ts", F.current_timestamp())
)
```

SQL equivalent:

```sql
select
  order_id,
  customer_id,
  cast(order_date as date) as order_date,
  year(cast(order_date as date)) as order_year,
  cast(amount as double) as order_amount,
  current_timestamp as load_ts
from orders
```

### Select, Rename, Cast, And Derive: Before And After

Before, `orders` is the table from section 4. The `amount` column is a `double`, and `order_date` is a `string`.

After the chain, the output has these columns: `order_id`, `customer_id`, `order_date` (now a `date`), `status`, `category`, `order_amount` (renamed from `amount`), and `order_year` (added).


| order_id | customer_id | order_date | status    | category    | order_amount | order_year |
| -------- | ----------- | ---------- | --------- | ----------- | ------------ | ---------- |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5         | 2026       |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99       | 2026       |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0         | 2026       |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25        | 2026       |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99        | 2026       |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75        | 2026       |

- `withColumnRenamed` changes the name only. The values are unchanged.
- `cast("double")` changes the type. Here `amount` was already a `double`, so the values do not change.
- `to_date` turns the `"2026-01-01"` text into a date.
- `year` reads the year out of the date.
- The SQL equivalent gives the same result.

The doc code also adds `load_ts` with `current_timestamp()`. It is left out of the table above because its value changes on every run.

## 8. Filter Rows

```python
complete_orders = orders.where(F.col("status") == "COMPLETE")
large_orders = orders.where(F.col("amount") >= 100)
books_or_grocery = orders.where(F.col("category").isin("books", "grocery"))
not_cancelled = orders.where(~(F.col("status") == "CANCELLED"))
```

String SQL style also works:

```python
orders.where("status = 'COMPLETE' and amount >= 25").show()
```

### Filter Rows: Before And After

The input is the `orders` table from section 4 (6 rows). Each filter keeps only the rows where its condition is true.

**`orders.where(F.col("status") == "COMPLETE")`** keeps 4 rows (orders 1, 2, 4, 5):


| order_id | customer_id | order_date | status   | category    | amount |
| -------- | ----------- | ---------- | -------- | ----------- | ------ |
| 1        | C001        | 2026-01-01 | COMPLETE | books       | 35.5   |
| 2        | C002        | 2026-01-01 | COMPLETE | electronics | 299.99 |
| 4        | C003        | 2026-01-02 | COMPLETE | grocery     | 42.25  |
| 5        | C002        | 2026-01-03 | COMPLETE | electronics | 99.99  |

**`orders.where(F.col("amount") >= 100)`** keeps 1 row (order 2):


| order_id | customer_id | order_date | status   | category    | amount |
| -------- | ----------- | ---------- | -------- | ----------- | ------ |
| 2        | C002        | 2026-01-01 | COMPLETE | electronics | 299.99 |

**`orders.where(F.col("category").isin("books", "grocery"))`** keeps 4 rows (orders 1, 3, 4, 6):


| order_id | customer_id | order_date | status    | category | amount |
| -------- | ----------- | ---------- | --------- | -------- | ------ |
| 1        | C001        | 2026-01-01 | COMPLETE  | books    | 35.5   |
| 3        | C001        | 2026-01-02 | CANCELLED | books    | 15.0   |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery  | 42.25  |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery  | 18.75  |

**`orders.where(~(F.col("status") == "CANCELLED"))`** removes the one cancelled order and keeps 5 rows (orders 1, 2, 4, 5, 6).

**`orders.where("status = 'COMPLETE' and amount >= 25")`** uses SQL text and keeps 4 rows (orders 1, 2, 4, 5):


| order_id | customer_id | order_date | status   | category    | amount |
| -------- | ----------- | ---------- | -------- | ----------- | ------ |
| 1        | C001        | 2026-01-01 | COMPLETE | books       | 35.5   |
| 2        | C002        | 2026-01-01 | COMPLETE | electronics | 299.99 |
| 4        | C003        | 2026-01-02 | COMPLETE | grocery     | 42.25  |
| 5        | C002        | 2026-01-03 | COMPLETE | electronics | 99.99  |

A filter never changes column values. It only removes rows. Each filter returns a new DataFrame, so `orders` itself is unchanged.

## 9. CASE WHEN Logic

```python
classified = orders.withColumn(
    "amount_bucket",
    F.when(F.col("amount") >= 100, "high")
    .when(F.col("amount") >= 25, "medium")
    .otherwise("low"),
)
```

SQL equivalent:

```sql
case
  when amount >= 100 then 'high'
  when amount >= 25 then 'medium'
  else 'low'
end as amount_bucket
```

### CASE WHEN: Before And After

The input is the `orders` table from section 4. The `when` conditions are checked in order, and the first one that is true wins.


| order_id | customer_id | order_date | status    | category    | amount | amount_bucket |
| -------- | ----------- | ---------- | --------- | ----------- | ------ | ------------- |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   | medium        |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 | high          |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   | low           |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  | medium        |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  | medium        |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  | low           |

`amount_bucket` is the only new column. Order 1 gets `medium` because 35.5 is at least 25 but under 100. Order 3 gets `low` because 15.0 is under 25. Put the most specific conditions first, because `>= 25` would also match everything at or above 100.

## 10. Handle Nulls

Each example below starts from the same `orders` DataFrame:


| order_id | customer_id | status   | amount | category |
| -------- | ----------- | -------- | ------ | -------- |
| 1        | 101         | COMPLETE | 25.0   | books    |
| 2        | NULL        | COMPLETE | 40.0   | toys     |
| 3        | 103         | NULL     | NULL   | books    |
| 4        | 104         | PENDING  | 0.0    | NULL     |

### `orders.na.drop()`

Removes every row that has a null in **any** column.

```python
orders.na.drop()
```

After:


| order_id | customer_id | status   | amount | category |
| -------- | ----------- | -------- | ------ | -------- |
| 1        | 101         | COMPLETE | 25.0   | books    |

Rows 2, 3, and 4 are removed because each has at least one null.

### `orders.na.drop(subset=["customer_id", "order_id"])`

Removes a row only when one of the listed columns is null. Nulls in other columns are ignored.

```python
orders.na.drop(subset=["customer_id", "order_id"])
```

After:


| order_id | customer_id | status   | amount | category |
| -------- | ----------- | -------- | ------ | -------- |
| 1        | 101         | COMPLETE | 25.0   | books    |
| 3        | 103         | NULL     | NULL   | books    |
| 4        | 104         | PENDING  | 0.0    | NULL     |

Row 2 is removed because its `customer_id` is null. Rows 3 and 4 stay even though they have other nulls.

### `orders.na.fill({"status": "UNKNOWN", "amount": 0.0})`

Replaces nulls only in the listed columns. Other columns keep their nulls.

```python
orders.na.fill({"status": "UNKNOWN", "amount": 0.0})
```

After:


| order_id | customer_id | status   | amount | category |
| -------- | ----------- | -------- | ------ | -------- |
| 1        | 101         | COMPLETE | 25.0   | books    |
| 2        | NULL        | COMPLETE | 40.0   | toys     |
| 3        | 103         | UNKNOWN  | 0.0    | books    |
| 4        | 104         | PENDING  | 0.0    | NULL     |

Row 3's null `status` becomes `UNKNOWN`, and its null `amount` becomes `0.0`. `customer_id` and `category` stay null because they were not listed.

### `F.coalesce(F.col("amount"), F.lit(0.0))`

Returns the first non-null value from its arguments. Here it replaces a null `amount` with `0.0`. It produces a column expression, so assign the result back with `withColumn`.

```python
df = orders.withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
```

After:


| order_id | customer_id | status   | amount | category |
| -------- | ----------- | -------- | ------ | -------- |
| 1        | 101         | COMPLETE | 25.0   | books    |
| 2        | NULL        | COMPLETE | 40.0   | toys     |
| 3        | 103         | NULL     | 0.0    | books    |
| 4        | 104         | PENDING  | 0.0    | NULL     |

Only the `amount` column changes. Row 3 now looks like a real $0.00 order, so sums and averages will count it.

### `F.col("customer_id").isNotNull()`

Adds a `true` or `false` column and keeps every row. Use it to flag rows, then filter on the flag if needed.

```python
df = orders.withColumn("has_customer", F.col("customer_id").isNotNull())
```

After:


| order_id | customer_id | status   | amount | category | has_customer |
| -------- | ----------- | -------- | ------ | -------- | ------------ |
| 1        | 101         | COMPLETE | 25.0   | books    | true         |
| 2        | NULL        | COMPLETE | 40.0   | toys     | false        |
| 3        | 103         | NULL     | NULL   | books    | true         |
| 4        | 104         | PENDING  | 0.0    | NULL     | true         |

## 11. Aggregations

```python
category_summary = (
    orders
    .groupBy("category")
    .agg(
        F.count("*").alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("category")
)
```

Multiple grouping columns:

```python
orders.groupBy("status", "category").agg(F.sum("amount").alias("total_amount")).show()
```

### Aggregations: Before And After

The input is the `orders` table from section 4. `groupBy` collapses each group of rows into one output row.

**Category summary.** Before, there are 6 order rows. After, there are 3 rows, one per category:


| category    | order_count | customer_count | total_amount | avg_amount | min_amount | max_amount |
| ----------- | ----------- | -------------- | ------------ | ---------- | ---------- | ---------- |
| books       | 2           | 1              | 50.5         | 25.25      | 15.0       | 35.5       |
| electronics | 2           | 1              | 399.98       | 199.99     | 99.99      | 299.99     |
| grocery     | 2           | 2              | 61.0         | 30.5       | 18.75      | 42.25      |

`order_count` counts all rows in the group, including the cancelled order. `customer_count` counts distinct customers, so the two `books` orders from C001 count as 1.

**Group by two columns.** After `groupBy("status", "category")`, there is one row per status and category pair:


| status    | category    | total_amount |
| --------- | ----------- | ------------ |
| COMPLETE  | books       | 35.5         |
| COMPLETE  | electronics | 399.98       |
| CANCELLED | books       | 15.0         |
| COMPLETE  | grocery     | 42.25        |
| RETURNED  | grocery     | 18.75        |

## 12. Joins

Inner join:

```python
enriched = orders.join(customers, "customer_id", "inner")
```

Left join:

```python
enriched = orders.join(customers, "customer_id", "left")
```

Join with different column names:

```python
joined = orders.alias("o").join(
    customers.alias("c"),
    F.col("o.customer_id") == F.col("c.customer_id"),
    "left",
)
```

Avoid duplicate columns:

```python
joined = (
    orders.alias("o")
    .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .select("o.*", "c.customer_name", "c.state", "c.segment")
)
```

Broadcast small lookup table:

```python
joined = orders.join(F.broadcast(customers), "customer_id", "left")
```

Use broadcast when one side is small enough to fit in executor memory.

### Joins: Before And After

The inputs are `orders` (6 rows) and `customers` (4 rows) from section 4. Every order here has a matching customer, so the inner and left joins return the same rows.

**Inner join** (`orders.join(customers, "customer_id", "inner")`) keeps only rows that match on both sides. The join key `customer_id` appears once, first in the output:


| customer_id | order_id | order_date | status    | category    | amount | customer_name | state | segment  |
| ----------- | -------- | ---------- | --------- | ----------- | ------ | ------------- | ----- | -------- |
| C001        | 1        | 2026-01-01 | COMPLETE  | books       | 35.5   | Asha          | CA    | retail   |
| C001        | 3        | 2026-01-02 | CANCELLED | books       | 15.0   | Asha          | CA    | retail   |
| C002        | 2        | 2026-01-01 | COMPLETE  | electronics | 299.99 | Ben           | NY    | business |
| C003        | 4        | 2026-01-02 | COMPLETE  | grocery     | 42.25  | Cara          | TX    | retail   |
| C002        | 5        | 2026-01-03 | COMPLETE  | electronics | 99.99  | Ben           | NY    | business |
| C004        | 6        | 2026-01-03 | RETURNED  | grocery     | 18.75  | Dev           | WA    | retail   |

**Left join** (`orders.join(customers, "customer_id", "left")`) keeps every order. Orders with no matching customer would show nulls in the customer columns. This sample has none, so the output matches the inner join.

**Join with different column names** (`orders.alias("o").join(customers.alias("c"), ...).select("o.*", ...)`) returns the same six rows, but keeps only the order columns plus the three selected customer columns. Here, `customer_id` appears once because `o.*` includes it.


| order_id | customer_id | order_date | status    | category    | amount | customer_name | state | segment  |
| -------- | ----------- | ---------- | --------- | ----------- | ------ | ------------- | ----- | -------- |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   | Asha          | CA    | retail   |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   | Asha          | CA    | retail   |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 | Ben           | NY    | business |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  | Cara          | TX    | retail   |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  | Ben           | NY    | business |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  | Dev           | WA    | retail   |

**Broadcast join** (`F.broadcast(customers)`) returns the same rows as the left join. The broadcast is a hint that sends the small `customers` table to every executor. It does not change the result, only how Spark runs the join.

## 13. De-Duplicate Data

```python
orders.dropDuplicates()
orders.dropDuplicates(["order_id"])
```

### De-Duplicate: Before And After

**Before**, `orders` has an extra copy of order 1 added to the end:


| order_id | customer_id | order_date | status    | category    | amount |
| -------- | ----------- | ---------- | --------- | ----------- | ------ |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   |

**After `dropDuplicates()`** removes rows that match on every column. Here that removes the extra order 1, leaving 6 rows. The row order is not guaranteed.

**After `dropDuplicates(["order_id"])`** keeps one row per `order_id`. Which copy is kept is not guaranteed.

For a deterministic choice, use the window pattern in section 14. It keeps the row with the latest `updated_at`.

**Before**, `updated` has two versions of order 1:


| order_id | status   | updated_at          |
| -------- | -------- | ------------------- |
| 1        | COMPLETE | 2026-01-05 10:00:00 |
| 1        | RETURNED | 2026-01-06 09:00:00 |
| 2        | COMPLETE | 2026-01-04 08:00:00 |

**After** `row_number()` over `order_id` ordered by `updated_at` descending, filtered to `rn = 1` and then `drop("rn")`:


| order_id | status   | updated_at          |
| -------- | -------- | ------------------- |
| 1        | RETURNED | 2026-01-06 09:00:00 |
| 2        | COMPLETE | 2026-01-04 08:00:00 |

The newest version of order 1 (`RETURNED`) is kept and the older one is dropped.

## 14. Window Functions

Keep the latest row per business key:

```python
latest = (
    df
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
        ),
    )
    .where(F.col("rn") == 1)
    .drop("rn")
)
```

Row number:

```python
w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())

latest_order = (
    orders
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
)
```

Running total:

```python
w = (
    Window
    .partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

orders.withColumn("running_amount", F.sum("amount").over(w)).show()
```

Rank within category:

```python
w = Window.partitionBy("category").orderBy(F.col("amount").desc())
orders.withColumn("amount_rank", F.dense_rank().over(w)).show()
```

### Window Functions: Before And After

The input is the `orders` table from section 4. A window function computes a value for each row using the other rows in its partition. Unlike `groupBy`, it keeps every row.

**Row number** (`row_number` partitioned by `customer_id`, ordered by `order_date` descending, filtered to `rn = 1`). Each customer keeps only their latest order. Order 1 and order 3 are both C001 orders; order 3 is later, so it is kept.


| order_id | customer_id | order_date | status    | category    | amount | rn |
| -------- | ----------- | ---------- | --------- | ----------- | ------ | -- |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   | 1  |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  | 1  |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  | 1  |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  | 1  |

The `rn` column is still in the output. Add `.drop("rn")` if you do not want it.

**Running total** (`sum` over rows from the start of each customer's partition up to the current row). Each row shows the total so far for its customer:


| order_id | customer_id | order_date | status    | category    | amount | running_amount |
| -------- | ----------- | ---------- | --------- | ----------- | ------ | -------------- |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   | 35.5           |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   | 50.5           |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 | 299.99         |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  | 399.98         |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  | 42.25          |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  | 18.75          |

Every row is kept. C001's second row shows 50.5 because 35.5 + 15.0 = 50.5.

**Rank within category** (`dense_rank` partitioned by `category`, ordered by `amount` descending). Rank 1 is the largest amount in each category:


| order_id | customer_id | order_date | status    | category    | amount | amount_rank |
| -------- | ----------- | ---------- | --------- | ----------- | ------ | ----------- |
| 1        | C001        | 2026-01-01 | COMPLETE  | books       | 35.5   | 1           |
| 3        | C001        | 2026-01-02 | CANCELLED | books       | 15.0   | 2           |
| 2        | C002        | 2026-01-01 | COMPLETE  | electronics | 299.99 | 1           |
| 5        | C002        | 2026-01-03 | COMPLETE  | electronics | 99.99  | 2           |
| 4        | C003        | 2026-01-02 | COMPLETE  | grocery     | 42.25  | 1           |
| 6        | C004        | 2026-01-03 | RETURNED  | grocery     | 18.75  | 2           |

`dense_rank` gives tied amounts the same rank and does not skip numbers afterward. Use `row_number` when every row needs a different number.

## 15. Date, String, Union, Pivot, And Explode

```python
df = (
    orders
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_ts", F.to_timestamp(F.concat_ws(" ", F.col("order_date"), F.lit("12:00:00"))))
    .withColumn("order_year", F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .withColumn("order_yyyymm", F.date_format("order_date", "yyyyMM"))
    .withColumn("next_day", F.date_add("order_date", 1))
)
```

### String Operations

```python
df = (
    customers
    .withColumn("customer_name_upper", F.upper("customer_name"))
    .withColumn("state_clean", F.trim(F.upper("state")))
    .withColumn("first_char", F.substring("customer_name", 1, 1))
)
```

Split and concatenate:

```python
df = customers.withColumn("customer_key", F.concat_ws("-", "state", "customer_id"))
```

### Union Data

Use `unionByName` for safer appends:

```python
combined = df_2025.unionByName(df_2026)
```

When one side has extra columns:

```python
combined = df_old.unionByName(df_new, allowMissingColumns=True)
```

This is similar to inserting rows into a table with column-name alignment.

### Pivot

```python
pivoted = (
    orders
    .groupBy("customer_id")
    .pivot("category")
    .agg(F.round(F.sum("amount"), 2))
    .na.fill(0)
)
```

SQL idea:

```sql
sum(case when category = 'books' then amount else 0 end) as books
```

Full SQL that produces the same output as the PySpark pivot above:

```sql
select
  customer_id,
  round(sum(case when category = 'books' then amount else 0 end), 2) as books,
  round(sum(case when category = 'electronics' then amount else 0 end), 2) as electronics,
  round(sum(case when category = 'grocery' then amount else 0 end), 2) as grocery
from orders
group by customer_id
```

The `else 0` gives `0.0` for categories a customer did not buy, which matches `.na.fill(0)` in the PySpark version. Unlike `pivot`, SQL needs each category listed by hand.

### Explode Arrays

```python
from pyspark.sql.types import ArrayType

events = spark.createDataFrame(
    [(1, ["view", "cart", "buy"]), (2, ["view"])],
    ["session_id", "events"],
)

events.select("session_id", F.explode("events").alias("event")).show()
```

Use this when JSON has arrays.

### Date, String, Union, Pivot, And Explode: Before And After

**Date functions.** The input is `orders`. Each date function adds a column:


| order_id | order_date | order_ts            | order_year | order_month | order_yyyymm | next_day   |
| -------- | ---------- | ------------------- | ---------- | ----------- | ------------ | ---------- |
| 1        | 2026-01-01 | 2026-01-01 12:00:00 | 2026       | 1           | 202601       | 2026-01-02 |
| 2        | 2026-01-01 | 2026-01-01 12:00:00 | 2026       | 1           | 202601       | 2026-01-02 |
| 3        | 2026-01-02 | 2026-01-02 12:00:00 | 2026       | 1           | 202601       | 2026-01-03 |
| 4        | 2026-01-02 | 2026-01-02 12:00:00 | 2026       | 1           | 202601       | 2026-01-03 |
| 5        | 2026-01-03 | 2026-01-03 12:00:00 | 2026       | 1           | 202601       | 2026-01-04 |
| 6        | 2026-01-03 | 2026-01-03 12:00:00 | 2026       | 1           | 202601       | 2026-01-04 |

The table shows only the columns this example creates, plus `order_id` and `order_date`. `to_timestamp` joins the date with `12:00:00`, `date_format` gives `202601`, and `date_add` adds one day.

**String operations.** The input is `customers`. Before:


| customer_id | customer_name | state | segment  |
| ----------- | ------------- | ----- | -------- |
| C001        | Asha          | CA    | retail   |
| C002        | Ben           | NY    | business |
| C003        | Cara          | TX    | retail   |
| C004        | Dev           | WA    | retail   |

After `upper`, `trim(upper(...))`, and `substring(..., 1, 1)`:


| customer_id | customer_name | state | segment  | customer_name_upper | state_clean | first_char |
| ----------- | ------------- | ----- | -------- | ------------------- | ----------- | ---------- |
| C001        | Asha          | CA    | retail   | ASHA                | CA          | A          |
| C002        | Ben           | NY    | business | BEN                 | NY          | B          |
| C003        | Cara          | TX    | retail   | CARA                | TX          | C          |
| C004        | Dev           | WA    | retail   | DEV                 | WA          | D          |

**Split and concatenate.** `concat_ws("-", "state", "customer_id")` builds one key column:


| customer_id | customer_name | state | segment  | customer_key |
| ----------- | ------------- | ----- | -------- | ------------ |
| C001        | Asha          | CA    | retail   | CA-C001      |
| C002        | Ben           | NY    | business | NY-C002      |
| C003        | Cara          | TX    | retail   | TX-C003      |
| C004        | Dev           | WA    | retail   | WA-C004      |

**Union with `unionByName`.** The sample uses two small DataFrames, because `df_2025` and `df_2026` are placeholders in the doc. Before:


| order_id | category | amount |
| -------- | -------- | ------ |
| 1        | books    | 10.0   |

and, with the columns in a different order:


| order_id | amount | category |
| -------- | ------ | -------- |
| 2        | 20.0   | grocery  |

After `df_2025.unionByName(df_2026)`, the rows are stacked and matched by column name, not by position:


| order_id | category | amount |
| -------- | -------- | ------ |
| 1        | books    | 10.0   |
| 2        | grocery  | 20.0   |

**Union with `allowMissingColumns=True`.** The two inputs have different columns. Before, `df_old` has `order_id` and `category`. `df_new` also has `status`. After the union, the missing `status` value for the old row is `NULL`:


| order_id | category | status   |
| -------- | -------- | -------- |
| 1        | books    | NULL     |
| 2        | books    | COMPLETE |

**Pivot.** The input is `orders` grouped by `customer_id`, with one column per category. Missing combinations are filled with `0`:


| customer_id | books | electronics | grocery |
| ----------- | ----- | ----------- | ------- |
| C001        | 50.5  | 0.0         | 0.0     |
| C002        | 0.0   | 399.98      | 0.0     |
| C003        | 0.0   | 0.0         | 42.25   |
| C004        | 0.0   | 0.0         | 18.75   |

Pivot column order is not guaranteed. Customer rows also come out in no particular order.

**Explode.** Before, `events` has one row per session, with an array in each row:


| session_id | events            |
| ---------- | ----------------- |
| 1          | [view, cart, buy] |
| 2          | [view]            |

After `explode("events")`, each array element becomes its own row. Session 1 has three rows, and session 2 has one:


| session_id | event |
| ---------- | ----- |
| 1          | view  |
| 1          | cart  |
| 1          | buy   |
| 2          | view  |

## 16. CRUD In Spark vs CRUD In Databases

SQL/database users usually think of CRUD like this:


| Database CRUD      | SQL example                      | Spark/file-lake equivalent                                         |
| ------------------ | -------------------------------- | ------------------------------------------------------------------ |
| Create             | `insert into table ...`          | Write a new dataset or append files                                |
| Read               | `select * from table`            | Read CSV, JSON, Parquet, or a catalog table                        |
| Update             | `update table set ... where ...` | Read old data, create corrected DataFrame, write replacement files |
| Delete rows        | `delete from table where ...`    | Filter out rows and write replacement files                        |
| Delete table/files | `drop table` or remove data      | Delete metadata, S3 objects, or local output folders               |

The important shift: plain CSV and Parquet files are not OLTP database tables. PySpark does not update one row inside an
existing Parquet file. It usually rewrites a new dataset.

## 17. Write Files

Write Parquet:

```python
orders.write.mode("overwrite").parquet("data/output/orders_parquet")
```

Write partitioned Parquet:

```python
orders_with_date = orders.withColumn("order_year", F.year(F.to_date("order_date")))

orders_with_date.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "data/output/orders_partitioned"
)
```

Write CSV:

```python
orders.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "data/output/orders_csv"
)
```

Save modes:


| Mode        | Meaning                               |
| ----------- | ------------------------------------- |
| `overwrite` | Replace existing output path          |
| `append`    | Add new files to existing output path |
| `error`     | Fail if output exists                 |
| `ignore`    | Do nothing if output exists           |

For a deeper beginner note on Parquet file format, file sizes, reads, troubleshooting slow jobs, partition updates, and lakehouse table formats, read `parquet-spark-performance-and-updates.md`.

## 18. Partitioning

Partitioning creates folders such as:

```text
orders_partitioned/order_year=2026/category=books/part-00000-...
```

Good partition columns:

- Frequently used in filters.
- Low or medium cardinality.
- Stable values such as date, country, region, category.

Bad partition columns:

- Unique IDs.
- Email addresses.
- Timestamps down to second/millisecond.

Read one partition efficiently:

```python
df = spark.read.parquet("data/output/orders_partitioned")
df.where("order_year = 2026 and category = 'books'").show()
```

## 19. Update And Delete Plain Parquet Or CSV

Plain CSV and Parquet files do not support database-style row-level `UPDATE` and `DELETE`.

Very important: PySpark does not update the existing Parquet file in place. It reads the old Parquet dataset, creates a
new DataFrame with the corrected data, and writes a new Parquet dataset. For small examples this may look like "updating
a file", but Spark is really recreating the output files.

For plain Parquet, an "update" usually means:

1. Read the existing Parquet dataset.
2. Create a new DataFrame with changed values.
3. Write the result to a replacement path.
4. Optionally replace the old path after validation.

### Update A Parquet Dataset Locally

This example changes `order_id = 2` from `COMPLETE` to `RETURNED`.

First, create a small Parquet dataset if you do not already have one:

```python
from pyspark.sql import functions as F

orders_with_date = (
    orders
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_year", F.year("order_date"))
)

orders_with_date.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "data/output/orders_partitioned"
)
```

Now update it by reading, changing, and writing a new Parquet dataset:

```python
df = spark.read.parquet("data/output/orders_partitioned")

updated = df.withColumn(
    "status",
    F.when(F.col("order_id") == 2, F.lit("RETURNED")).otherwise(F.col("status")),
).withColumn(
    "updated_at",
    F.when(F.col("order_id") == 2, F.current_timestamp()).otherwise(F.lit(None)),
)

updated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "data/output/orders_partitioned_updated"
)
```

Verify the update:

```python
spark.read.parquet("data/output/orders_partitioned_updated").where(
    F.col("order_id") == 2
).show(truncate=False)
```

Expected idea:

```text
order_id = 2 now has status = RETURNED
```

After validation, you can treat `data/output/orders_partitioned_updated` as the new dataset. In production, teams often
write to a temporary path, validate counts and quality checks, then promote the new path using orchestration.

**Note: when the temporary-path pattern is used, and why**

- **Readers must never see partial data.** `mode("overwrite")` deletes the old files before the new ones finish writing. If the job fails halfway, the dataset is empty or incomplete. Writing to a new path first keeps the old data readable until the new data is good.
- **Downstream consumers exist.** Athena, Glue jobs, dashboards, or other Spark jobs read the table while the pipeline runs. Promoting only after validation means they get either the old version or the new one, not a mix.
- **Validation gates the release.** Row counts, null checks, duplicate keys, and schema checks run against the temporary path. A failed check stops the promotion, and the old data stays in place.
- **Rollback is cheap.** If a bad version is promoted, point the table back to the previous path. You do not need to rebuild the data.
- **S3 has no atomic rename.** Moving a folder on S3 is a copy followed by a delete, so it is not one step. The promotion is usually a pointer change instead: update the Glue catalog table location or a `current` pointer, which is a single metadata change.

This pattern is common in scheduled batch pipelines (Airflow, Step Functions, Glue workflows) and in curated zones that feed analytics. It is less common for small, one-off local practice like this lab, where a direct overwrite is fine.

Some teams avoid the pattern altogether by using a table format such as Delta, Iceberg, or Hudi. Those formats commit each write atomically and keep older versions, so a failed write never shows up to readers.

### Update A Parquet Dataset In LocalStack S3

The same pattern works with LocalStack S3. Only the paths change.

```python
df = spark.read.parquet("s3a://de-lab/curated/orders_parquet")

updated = df.withColumn(
    "status",
    F.when(F.col("order_id") == 2, F.lit("RETURNED")).otherwise(F.col("status")),
).withColumn(
    "updated_at",
    F.when(F.col("order_id") == 2, F.current_timestamp()).otherwise(F.lit(None)),
)

updated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "s3a://de-lab/curated/orders_parquet_updated"
)
```

Verify in Spark:

```python
spark.read.parquet("s3a://de-lab/curated/orders_parquet_updated").where(
    F.col("order_id") == 2
).show(truncate=False)
```

Verify with LocalStack:

```bash
lstk aws s3 ls s3://de-lab/curated/orders_parquet_updated/ --recursive
```

Important: this does not edit one Parquet file in place. It writes a new Parquet dataset with the corrected records.

Simulate delete by filtering and rewriting:

```python
df = spark.read.parquet("data/output/orders_partitioned")

after_delete = df.where(F.col("order_id") != 3)

after_delete.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "data/output/orders_partitioned_after_delete"
)
```

Delete an entire output folder from S3:

```bash
lstk aws s3 rm s3://de-lab/curated/orders_csv/ --recursive
```

For real row-level `UPDATE`, `DELETE`, and `MERGE`, learn one of:

- Apache Iceberg
- Delta Lake
- Apache Hudi

These formats add table metadata and transaction-like behavior on top of object storage.

### Promote A New Dataset With A Pointer Change

Moving a folder on S3 is not one step. S3 has no rename. `aws s3 mv` copies each object, then deletes the source. If the job fails partway, some files are in the new place and some are not, so readers can see a mix.

Instead, write the new data to its own versioned path, validate it, and then change a small pointer that readers follow. The pointer change is one metadata update, so readers switch from the old version to the new one in a single step.

**Step 1: write to a new versioned path.** The old data stays where it is:

```python
new_path = "s3a://de-lab/curated/orders/run_date=2026-01-02/"

updated.write.mode("overwrite").parquet(new_path)
```

**Step 2: validate the new path.** Run the checks from section 21 against `new_path`. Stop here if they fail. The pointer still points at the old data.

**Step 3: promote by changing the pointer.** There are two common ways to do it.

Option A: update the Glue catalog table location. Readers that query the table through Athena or Spark read the new location:

```sql
ALTER TABLE curated.orders SET LOCATION 's3://de-lab/curated/orders/run_date=2026-01-02/'
```

For a partitioned table, each partition has its own location, so update those too. Changing only the table location does not move existing partitions.

Option B: write a `_current` pointer file. Readers read the path from the file first:

```bash
echo "s3a://de-lab/curated/orders/run_date=2026-01-02/" > current_pointer.txt
lstk aws s3 cp current_pointer.txt s3://de-lab/curated/orders/_current.txt
```

```python
current = spark.read.text("s3a://de-lab/curated/orders/_current.txt").first()[0].strip()
orders = spark.read.parquet(current)
```

A single-object upload is all-or-nothing, so a reader sees either the old pointer or the new one.

**Rollback** is the same step in reverse. Point the catalog or the `_current` file back at the previous `run_date`. The old data is still there, so nothing needs to be rebuilt.

These commands are not run in this lab. They show the pattern for a team that uses the Glue catalog or a pointer file.

## 20. Incremental Loads

Common incremental pattern using a timestamp or date column:

```python
last_processed_date = "2026-01-02"

new_orders = (
    orders
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_year", F.year("order_date"))
    .where(F.col("order_date") > F.lit(last_processed_date))
)
```

Append new data:

```python
new_orders.write.mode("append").partitionBy("order_year", "category").parquet(
    "data/output/orders_incremental"
)
```

AWS Glue job bookmarks help track processed inputs for some source types. Bookmarks are useful, but you should still
understand your business watermark, deduplication key, and late-arriving data rules.

## 21. Data Quality Checks

Basic checks:

```python
required_columns = ["order_id", "customer_id", "order_date", "amount"]

missing_columns = [c for c in required_columns if c not in orders.columns]
if missing_columns:
    raise ValueError(f"Missing columns: {missing_columns}")

bad_rows = orders.where(
    F.col("order_id").isNull()
    | F.col("customer_id").isNull()
    | F.col("amount").isNull()
    | (F.col("amount") < 0)
)

if bad_rows.count() > 0:
    bad_rows.show(truncate=False)
    raise ValueError("Data quality check failed")
```

Duplicate key check:

```python
duplicates = (
    orders
    .groupBy("order_id")
    .count()
    .where("count > 1")
)

if duplicates.count() > 0:
    duplicates.show()
    raise ValueError("Duplicate order_id values found")
```

## 22. Error Handling Pattern

```python
def main() -> None:
    spark = SparkSession.builder.appName("job-with-errors").getOrCreate()
    try:
        df = spark.read.option("header", True).csv("data/input/orders.csv")
        if df.count() == 0:
            raise ValueError("Input is empty")
        df.write.mode("overwrite").parquet("data/output/orders")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

In production, use logging instead of many `print` statements.

## 23. Performance Basics

Cache only when reused:

```python
enriched = orders.join(customers, "customer_id", "left").cache()
enriched.count()
enriched.groupBy("category").count().show()
enriched.groupBy("state").count().show()
enriched.unpersist()
```

Control output file count:

```python
df.repartition(8).write.mode("overwrite").parquet("data/output/repartitioned")
df.coalesce(1).write.mode("overwrite").csv("data/output/single_file_for_demo")
```

Rules of thumb:

- `repartition` can increase or decrease partitions and causes a shuffle.
- `coalesce` usually reduces partitions and avoids a full shuffle.
- Avoid `collect()` on large data.
- Avoid Python UDFs when built-in Spark functions can do the work.
- Use broadcast joins for small lookup tables.
- Use Parquet instead of CSV for curated data.

## 24. Local Files vs LocalStack S3

Not every example in this guide uses LocalStack.

All AWS-style examples in this guide are local AWS practice. They use Docker + LocalStack, not a real AWS account. Install Docker Desktop and start LocalStack before running any command that uses `lstk aws` or any Spark path that starts with `s3a://de-lab/...`.

There are two kinds of paths:


| Path style                            | Where it writes                                    | Example                               |
| ------------------------------------- | -------------------------------------------------- | ------------------------------------- |
| `data/input/...` or `data/output/...` | Your local computer, inside the lab/project folder | `data/output/orders_parquet`          |
| `s3a://de-lab/...`                    | The LocalStack S3 bucket named`de-lab`             | `s3a://de-lab/curated/orders_parquet` |

If your lab folder is `~/spark-glue-local-lab`, then this code:

```python
orders.write.mode("overwrite").parquet("data/output/orders_parquet")
```

writes here on Mac/Linux:

```text
~/spark-glue-local-lab/data/output/orders_parquet/
```

and here on Windows:

```text
C:\spark-glue-local-lab\data\output\orders_parquet\
```

That output is a folder, not a single file. Inside it you will usually see files such as:

```text
part-00000-....snappy.parquet
_SUCCESS
```

LocalStack is used only when the path starts with `s3a://` or when commands use `lstk aws s3 ...`.

## 25. What LocalStack Is

LocalStack is a local AWS simulator. For this lab, assume Docker is acting like a small local AWS account running on
your laptop.

Docker is required because LocalStack runs as a container. Install Docker Desktop first, confirm `docker ps` works, then start LocalStack.

When you start LocalStack, Docker runs a container that exposes AWS-like services on:

```text
http://localhost:4566
```

In this guide:

- Real AWS S3 is not used.
- LocalStack S3 runs inside Docker.
- `lstk aws s3 ...` talks to LocalStack instead of real AWS.
- Spark talks to LocalStack S3 when the path starts with `s3a://`.
- The bucket `de-lab` is a LocalStack bucket, not a real AWS bucket.

Think of it like this:

```text
Your laptop
  |
  |-- local project folder
  |     |-- data/input/orders.csv
  |     |-- data/output/orders_parquet/
  |
  |-- Docker
        |-- LocalStack container
              |-- fake/local AWS S3
                    |-- bucket: de-lab
                          |-- raw/orders/orders.csv
                          |-- curated/orders_parquet/
```

So this writes to your normal local folder:

```python
orders.write.mode("overwrite").parquet("data/output/orders_parquet")
```

But this writes to LocalStack S3 inside Docker:

```python
orders.write.mode("overwrite").parquet("s3a://de-lab/curated/orders_parquet")
```

You do not browse to the Docker folder directly. You inspect LocalStack S3 using AWS-style commands:

```bash
lstk aws s3 ls s3://de-lab/curated/orders_parquet/ --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 ls s3://your-real-bucket/curated/orders_parquet/ --recursive
```

To copy LocalStack output back to your project folder:

```bash
mkdir -p data/downloaded/orders_parquet

lstk aws s3 cp \
  s3://de-lab/curated/orders_parquet/ \
  data/downloaded/orders_parquet/ \
  --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 cp \
#   s3://your-real-bucket/curated/orders_parquet/ \
#   data/downloaded/orders_parquet/ \
#   --recursive
```

This downloads from fake/local S3 to:

```text
data/downloaded/orders_parquet/
```

## 26. LocalStack S3 End-To-End Example

Prerequisite: Docker Desktop must be running, `lstk` must be installed, and you must be logged in with `lstk login`. If you have not done that yet, follow Part 2 of `setup-mac-local-spark.md` or `setup-windows-local-spark.md`.

Start LocalStack:

```bash
lstk start
lstk aws s3 mb s3://de-lab
lstk aws s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv

# Real AWS equivalent, shown for learning only:
# aws s3 mb s3://your-real-unique-bucket-name
# aws s3 cp data/input/orders.csv s3://your-real-bucket/raw/orders/orders.csv
```

Job:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("localstack-s3-demo")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("s3a://de-lab/raw/orders/orders.csv")
)

curated = (
    orders
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_year", F.year("order_date"))
    .where("status = 'COMPLETE'")
)

curated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "s3a://de-lab/curated/orders_parquet"
)

spark.read.parquet("s3a://de-lab/curated/orders_parquet").show()
spark.stop()
```

Verify:

```bash
lstk aws s3 ls s3://de-lab/curated/orders_parquet/ --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 ls s3://your-real-bucket/curated/orders_parquet/ --recursive
```

At this point, you have only created data files in LocalStack S3. You have not created a Glue table yet.

To practice table metadata end to end, use the `Self-Contained LocalStack Catalog Lab` section below. That lab creates:

1. The actual Parquet files in LocalStack S3.
2. A database and external table definition in Spark's SQL catalog, which follows the same pattern as the Glue Data Catalog.

LocalStack's Glue service needs a paid license, so this lab does not use `lstk aws glue`.

## 27. AWS Glue Data Catalog

The AWS Glue Data Catalog is a central metadata store for data lake tables. For SQL/database users, think of it as a
shared metastore that tells tools where data is, what columns it has, how it is partitioned, and how to read it.

It does not store the actual rows for a normal S3 Parquet or CSV table. The actual data files stay in S3. The Data
Catalog stores metadata about those files.

SQL mental model:


| Database concept        | Glue Data Catalog equivalent                                     |
| ----------------------- | ---------------------------------------------------------------- |
| Database/schema         | Glue database                                                    |
| Table definition        | Glue table                                                       |
| Columns and data types  | Glue table schema                                                |
| External table location | S3 path such as`s3://company-curated/orders/`                    |
| Partition metadata      | Known partition folders such as`order_year=2026/category=books/` |
| `CREATE EXTERNAL TABLE` | Create metadata that points to files outside the catalog         |
| `DROP TABLE`            | Delete the table metadata, not necessarily the S3 files          |

### What Is An External Table?

An external table is a table definition whose data lives outside the metastore. In AWS data lakes, that usually means:

```text
Table metadata lives in Glue Data Catalog.
Actual data files live in S3.
```

Example:

```text
Glue database: curated
Glue table: orders
S3 location: s3://de-lab/curated/orders_parquet/
File format: Parquet
Columns: order_id, customer_id, order_date, status, amount
Partitions: order_year, category
```

The table is called external because Glue does not own the actual data files like a traditional database owns table
pages on disk. Glue only stores the definition. The Parquet files remain in S3.

Important behavior:

- Creating the external table does not copy data into Glue.
- Querying the table reads files from S3.
- Dropping the table usually removes metadata, not the underlying S3 files.
- If S3 files are deleted, the table can still exist but queries may fail or return no data.
- If table metadata is deleted, the S3 files can still exist but tools lose the convenient table name and schema.

### How Data Is Ingested Before The Table Exists

A common beginner data lake flow looks like this:

```text
1. Raw data lands in S3.
   Example: s3://de-lab/raw/orders/orders.csv

2. PySpark reads raw data.
   Example: spark.read.csv("s3a://de-lab/raw/orders/orders.csv")

3. PySpark cleans, casts, filters, joins, and validates the data.

4. PySpark writes curated Parquet to S3.
   Example: s3://de-lab/curated/orders_parquet/

5. A Glue Data Catalog external table is created over that curated S3 location.

6. Athena, Glue Spark, EMR, Redshift Spectrum, or other tools query the table by name.
   Example: select * from curated.orders;
```

The table usually comes after the data location exists, because the table definition needs to point to a real S3
location and use the correct schema and partition design.

### Example S3 Layout And Glue Table Metadata

Actual data files in S3:

```text
s3://de-lab/curated/orders_parquet/
  order_year=2026/
    category=books/
      part-00000-....snappy.parquet
    category=electronics/
      part-00001-....snappy.parquet
```

Glue Data Catalog metadata:

```text
database: curated
table: orders
location: s3://de-lab/curated/orders_parquet/
columns: order_id, customer_id, order_date, status, amount
partition keys: order_year, category
file format: parquet
```

After this metadata exists, tools can query the same dataset consistently:

- Athena can run SQL against the table.
- Glue Spark jobs can read the table by database and table name.
- EMR/Spark can use the catalog as a Hive-compatible metastore.
- Redshift Spectrum can query cataloged S3 tables.
- Lake Formation can apply permissions on catalog databases and tables.

### How The User Creates The Table

For beginners, the cleanest mental model is manual or scripted table creation: you define the database, table name,
schema, partition columns, file format, and S3 location.

Example SQL-style table definition:

```sql
create external table curated.orders (
  order_id int,
  customer_id string,
  order_date date,
  status string,
  amount double
)
partitioned by (order_year int, category string)
stored as parquet
location 's3://de-lab/curated/orders_parquet/';
```

What this means:

```text
Create a table name called curated.orders.
The rows are not stored inside Glue.
Read Parquet files from s3://de-lab/curated/orders_parquet/.
Treat order_year and category as partition columns.
Expose the columns order_id, customer_id, order_date, status, and amount to SQL tools.
```

In real AWS, users commonly create this metadata using one of these controlled approaches:

- Athena SQL DDL, such as `CREATE EXTERNAL TABLE`.
- Glue Data Catalog APIs, such as `create_table`.
- Infrastructure-as-code, such as Terraform, CloudFormation, or CDK.
- A deployment script owned by the data engineering team.

For a first data engineering job, it is enough to understand the SQL DDL version first. It is the clearest bridge from
database knowledge to data lake metadata.

### Self-Contained LocalStack Catalog Lab

This mini-lab creates both pieces needed for an external table:

1. Actual Parquet data in LocalStack S3.
2. A table definition that points to that S3 location, stored in Spark's SQL catalog.

This lab uses LocalStack S3 running inside Docker. It does not create anything in a real AWS account. It does not use the Glue Data Catalog, because LocalStack Glue needs a paid license.

Run these commands from your lab folder:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
```

Start LocalStack:

```bash
lstk start
lstk status
```

Set local AWS variables for this terminal:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
```

Create the S3 bucket:

```bash
lstk aws s3 mb s3://de-lab

# Real AWS equivalent, shown for learning only:
# aws s3 mb s3://your-real-unique-bucket-name
```

Create local sample input if you do not already have it:

```bash
mkdir -p data/input jobs

cat > data/input/orders.csv <<'EOF'
order_id,customer_id,order_date,status,category,amount
1,C001,2026-01-01,COMPLETE,books,35.50
2,C002,2026-01-01,COMPLETE,electronics,299.99
3,C001,2026-01-02,CANCELLED,books,15.00
4,C003,2026-01-02,COMPLETE,grocery,42.25
5,C002,2026-01-03,COMPLETE,electronics,99.99
6,C004,2026-01-03,RETURNED,grocery,18.75
EOF
```

Upload raw CSV to LocalStack S3:

```bash
lstk aws s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv

# Real AWS equivalent, shown for learning only:
# aws s3 cp data/input/orders.csv s3://your-real-bucket/raw/orders/orders.csv
```

Create a Spark job that reads raw CSV from LocalStack S3 and writes curated Parquet back to LocalStack S3:

```bash
cat > jobs/write_orders_parquet_to_localstack.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("write-orders-parquet-to-localstack")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("s3a://de-lab/raw/orders/orders.csv")
    )

    curated = (
        orders
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_year", F.year("order_date"))
    )

    curated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
        "s3a://de-lab/curated/orders_parquet"
    )

    spark.read.parquet("s3a://de-lab/curated/orders_parquet").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
EOF
```

Run the Spark job:

```bash
spark-submit \
  --master "local[2]" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  jobs/write_orders_parquet_to_localstack.py
```

Verify the Parquet files exist in LocalStack S3:

```bash
lstk aws s3 ls s3://de-lab/curated/orders_parquet/ --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 ls s3://your-real-bucket/curated/orders_parquet/ --recursive
```

Now create the table definition. This job creates a database and a partitioned Parquet table in Spark's SQL catalog, points it at the S3 folder, and reads it back with SQL:

```bash
cat > jobs/create_orders_catalog_table.py <<'EOF'
from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("create-orders-catalog-table")
        .master("local[2]")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS curated")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS curated.orders (
      order_id INT,
      customer_id STRING,
      order_date DATE,
      status STRING,
      amount DOUBLE
    )
    USING parquet
    PARTITIONED BY (order_year INT, category STRING)
    LOCATION 's3a://de-lab/curated/orders_parquet/'
    """)

    # Register the partition folders that already exist in S3.
    spark.sql("MSCK REPAIR TABLE curated.orders")

    spark.sql("SELECT * FROM curated.orders ORDER BY order_id").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
EOF
```

Run the job. It uses the same packages as the other S3 jobs:

```bash
spark-submit \
  --master "local[2]" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  jobs/create_orders_catalog_table.py
```

Important: the `CREATE TABLE` statement stores metadata only. It does not create Parquet files. Spark keeps this catalog for the length of the job, so the table is created and queried in the same run. A persistent metastore, such as the Glue Data Catalog in AWS, keeps the table between runs. The Parquet files are created by the Spark job and live in LocalStack S3 at:

```text
s3://de-lab/curated/orders_parquet/
```

The relationship is:

```text
LocalStack S3:
  s3://de-lab/curated/orders_parquet/
  actual Parquet data files

Spark SQL catalog (the lab's stand-in for the Glue Data Catalog):
  database: curated
  table: orders
  metadata pointing to the S3 location above
```

### When To Create The Table

Create the Glue table when:

- The curated S3 location is known.
- The file format is known, such as Parquet or CSV.
- The schema is stable enough to share with users.
- The partition columns are decided.
- You want Athena, Glue Spark, EMR, Redshift Spectrum, or Lake Formation to use the dataset by table name.

Do not rush to create a catalog table for temporary experiment output. For quick local or development tests, reading
directly from a path is fine:

```python
orders = spark.read.parquet("s3a://de-lab/curated/orders_parquet/")
```

Create a catalog table when the dataset becomes a shared, named, reusable data product.

### Can More Than One Table Point To The Same Location?

Technically, yes. You can create more than one Glue table pointing to the same S3 location.

Example:

```text
curated.orders
  location: s3://de-lab/curated/orders_parquet/

analytics.orders_for_bi
  location: s3://de-lab/curated/orders_parquet/
```

But use this carefully. Multiple tables over the same location can confuse users if the schemas, partition definitions,
or permissions differ.

Good reasons to create more than one table for the same location:

- You are exposing the same dataset through different databases for permission boundaries.
- You are testing a new schema definition before replacing the official table.
- You need a temporary table name for migration or validation.

Bad reasons:

- Different teams independently create duplicate table names without coordination.
- One table says the data is CSV and another says it is Parquet.
- One table defines partitions and another does not.
- One table has old columns and another has new columns over the same files.

Beginner rule:

```text
Prefer one official Glue table per curated dataset location.
Create extra tables for the same location only when there is a clear governance, testing, or migration reason.
```

### Partitions In The Data Catalog

Partitions are very important for data engineering interviews.

Physical S3 layout:

```text
s3://de-lab/curated/orders_parquet/order_year=2026/category=books/
s3://de-lab/curated/orders_parquet/order_year=2026/category=grocery/
s3://de-lab/curated/orders_parquet/order_year=2026/category=electronics/
```

Catalog table:

```text
table: curated.orders
partition keys: order_year, category
```

When a query filters on partition columns:

```sql
select *
from curated.orders
where order_year = 2026
  and category = 'books';
```

the query engine can skip unrelated folders. This is called partition pruning. It reduces data scanned and usually
improves performance and cost.

New partition folders must be known to the catalog. Common controlled ways to add them are:

- Add partitions through Glue APIs or ETL code.
- Run `MSCK REPAIR TABLE` in Athena for Hive-style partition folders.
- Use a table format such as Iceberg that manages table metadata differently.

Beginner warning: creating a Glue table with partition keys does not automatically mean every future S3 partition folder is visible to query engines.

Example:

```text
Glue table:
  curated.orders
  location: s3://de-lab/curated/orders_parquet/
  partition keys: order_year, category

Known catalog partitions:
  order_year=2026/category=books
  order_year=2026/category=grocery
```

Later, Spark writes a new folder:

```text
s3://de-lab/curated/orders_parquet/order_year=2027/category=books/
```

The files exist in S3, but Athena may not query that new partition until the catalog knows about it. A query like this may return no 2027 rows:

```sql
select *
from curated.orders
where order_year = 2027
  and category = 'books';
```

Fix it by adding the partition through controlled ETL/catalog code, or by running a repair command for Hive-style folders:

```sql
MSCK REPAIR TABLE curated.orders;
```

The exact command depends on the query engine and table format. The important idea is:

```text
S3 folder exists does not always mean Glue partition metadata exists.
```

### Register And Remove Partitions

Files in S3 are not enough for a query to see them. The query engine reads only the partitions that the catalog lists. A new folder such as `order_year=2027/` returns zero rows until its partition is registered. The query does not raise an error, so the problem is easy to miss.

**Register one partition.** Use the folder path as the `LOCATION`:

```sql
ALTER TABLE curated.orders ADD IF NOT EXISTS PARTITION (order_year=2027, category='books')
LOCATION 's3://de-lab/curated/orders_parquet/order_year=2027/category=books/'
```

**Register every missing partition.** Scan the table location and add each partition that is not in the catalog:

```sql
MSCK REPAIR TABLE curated.orders
```

`MSCK REPAIR TABLE` is fine for labs, backfills, and tables with a few partitions. It lists the whole table location and adds partitions one at a time, so it becomes slow when a table has thousands of partitions. For a scheduled pipeline, register each new partition right after the job writes it, with `ALTER TABLE ... ADD PARTITION` in Spark or Athena. In AWS Glue, use the `BatchCreatePartition` API, or run a Glue crawler on a schedule. Use `MSCK REPAIR TABLE` for catch-up work.

**Remove one partition.** Removing a partition from the catalog does not delete its files when the table is external. Those are two separate steps:

```sql
-- 1. Remove the catalog entry first.
ALTER TABLE curated.orders DROP IF EXISTS PARTITION (order_year=2027, category='books');
```

```bash
# 2. Then delete the files in S3.
lstk aws s3 rm s3://de-lab/curated/orders_parquet/order_year=2027/category=books/ --recursive
```

Drop the catalog entry first. If you delete the files first, queries can point at folders that no longer exist.

To delete the files from PySpark instead of the AWS CLI, use Hadoop's filesystem API through Spark. Run this in the same session that has the S3 settings:

```python
# 1. Remove the catalog entry first.
spark.sql("ALTER TABLE curated.orders DROP IF EXISTS PARTITION (order_year=2027, category='books')")

# 2. Delete the partition folder in S3 with Hadoop's filesystem API.
jvm = spark.sparkContext._jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

path = jvm.org.apache.hadoop.fs.Path("s3a://de-lab/curated/orders_parquet/order_year=2027/category=books")
fs = path.getFileSystem(hadoop_conf)
fs.delete(path, True)  # True = delete recursively
```

`spark.sparkContext._jvm` and `_jsc` are internal Spark APIs. They work in the PySpark versions in this guide, but they are not a public interface. I tested the same delete locally on a folder on disk; I have not run it against LocalStack S3.

**Remove stale entries.** If a folder was deleted but its catalog entry remains, Spark can remove the stale entry:

```sql
MSCK REPAIR TABLE curated.orders SYNC PARTITIONS;
```

Athena's `MSCK REPAIR TABLE` only adds partitions, so use `ALTER TABLE ... DROP PARTITION` there. In AWS Glue, use the `BatchDeletePartition` API or the Glue console.

I tested these Spark SQL commands on local files with Spark 4.0.1, using an external table. The check was: before `ADD PARTITION`, the 2027 query returned 0 rows; after it, the query returned 1 row. Dropping the partition left the files on disk. I have not tested these commands against LocalStack S3, Athena, or the Glue Data Catalog.

### When To Use Glue Data Catalog

Use Glue Data Catalog when:

- Data is stored in S3 and should be queried by Athena, Spark, EMR, Redshift Spectrum, or Lake Formation.
- Multiple teams need one shared table definition for the same dataset.
- You want SQL users to query S3 files as tables.
- You have partitioned data and want query engines to understand the partition columns.
- You want a central place to document dataset schema, file format, and S3 location.
- You want access control through Lake Formation or IAM-integrated AWS analytics tools.

You may not need it when:

- You are doing a tiny one-off local PySpark test.
- Your application needs OLTP-style row reads/writes; use a database instead.
- Your data is not meant to be discovered or queried by other AWS analytics tools.
- You are reading a single file path directly in a simple script.

### Data Catalog vs Actual Data

This distinction is critical:

```text
Glue Data Catalog = metadata
S3 = actual data files
```

If you create this catalog table:

```text
database: curated
table: orders
location: s3://de-lab/curated/orders_parquet/
```

you have not copied the data into Glue. You only told AWS:

```text
"There is a table called curated.orders, and its files live at this S3 path."
```

### Interview Explanation

Use this answer in interviews:

```text
AWS Glue Data Catalog is a central metadata repository for data lake tables. For S3-backed external tables, it stores the database name, table name, schema, partition keys, file format, and S3 location. The actual rows remain in S3. Data is usually ingested into S3 first, then a Glue external table is created so Athena, Glue Spark, EMR, Redshift Spectrum, and Lake Formation can discover and query the same dataset consistently.
```

## 28. Can Glue Do Create, Read, Update, Delete?

Short answer: yes for some meanings of CRUD, but not like an OLTP database.


| CRUD Meaning               | Glue Can Help? | Explanation                                                       |
| -------------------------- | -------------- | ----------------------------------------------------------------- |
| Create a dataset           | Yes            | Glue Spark writes files/tables to S3                              |
| Read a dataset             | Yes            | Glue Spark reads S3, JDBC, catalog tables                         |
| Update metadata            | Yes            | Glue Data Catalog APIs update databases/tables/partitions         |
| Delete metadata            | Yes            | Glue Data Catalog APIs delete databases/tables/partitions         |
| Update rows in CSV/Parquet | Indirectly     | Rewrite files/partitions or use Iceberg/Hudi/Delta                |
| Delete rows in CSV/Parquet | Indirectly     | Filter and rewrite, or use Iceberg/Hudi/Delta                     |
| OLTP row CRUD              | No             | Use a database such as PostgreSQL, MySQL, DynamoDB, or SQL Server |

Interview explanation:

```text
Glue is best for batch and streaming ETL, data lake processing, catalog metadata, and orchestration. It is not an OLTP database. For S3 data lakes, row-level update/delete semantics usually require a lakehouse table format such as Iceberg, Hudi, or Delta.
```

## 29. Common Interview Tasks And PySpark Patterns

Find top customers:

```python
orders.groupBy("customer_id").agg(F.sum("amount").alias("total_amount")).orderBy(
    F.col("total_amount").desc()
).show(10)
```

Find customers with no orders:

```python
customers.join(orders, "customer_id", "left_anti").show()
```

Find duplicate orders:

```python
orders.groupBy("order_id").count().where("count > 1").show()
```

Join and aggregate:

```python
orders.join(customers, "customer_id", "left").groupBy("state").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("total_amount"),
).show()
```

Latest record per key:

```python
w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
orders.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn").show()
```

Slowly changing dimension style current flag:

```python
w = Window.partitionBy("customer_id").orderBy(F.col("effective_date").desc())

current_customers = (
    customer_history
    .withColumn("rn", F.row_number().over(w))
    .withColumn("is_current", F.col("rn") == 1)
)
```

## 30. What To Practice For The First Data Engineering Job

Practice until you can do these without copying:

- Read CSV, JSON, and Parquet.
- Define a schema.
- Clean column names and cast data types.
- Filter rows.
- Join fact and dimension data.
- Aggregate by date/category/customer.
- Use window functions for latest record per key.
- De-duplicate records.
- Write partitioned Parquet.
- Explain why Parquet is preferred over CSV for curated data.
- Explain why S3 files are not OLTP tables.
- Simulate update/delete by rewriting data.
- Use LocalStack S3 for local testing.
- Explain Glue Data Catalog, external tables, S3 locations, and partition metadata.

End every local Spark script with:

```python
spark.stop()
```
