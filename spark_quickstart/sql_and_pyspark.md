# PySpark for SQL Users: Keep Your Transformations in SQL

If you already know SQL well, you can use that knowledge immediately in PySpark. You do not need to rewrite every CASE expression, join, or window function as a chain of Python methods.

**Learn a small toolkit first:** read, inspect, name, shape, and write. Keep joins, aggregations, CASE logic, and windows in SQL when that is clearer; section 9 introduces the DataFrame methods worth remembering.

A useful working pattern is:

```text
PySpark: read data → register temporary views
Spark SQL: validate → deduplicate → join → transform
PySpark: check results → write output
```

**Scope:** A local, batch-learning example using Spark 3.5.x and small in-memory inputs. No AWS account or LocalStack is required. Set up Spark using the [macOS](setup-mac-local-spark.md) or [Windows](setup-windows-local-spark.md) guide first. The examples build on each other; run the Python blocks in sections 2–10 in order.

## Contents

1. [What changes for a SQL developer?](#1-what-changes-for-a-sql-developer)
2. [Start Spark](#2-start-spark)
3. [Create inputs and expose them to SQL](#3-create-inputs-and-expose-them-to-sql)
4. [Parse fields and separate rejected rows](#4-parse-fields-and-separate-rejected-rows)
5. [Select the latest version and use CASE WHEN](#5-select-the-latest-version-and-use-case-when)
6. [Join customers and calculate totals](#6-join-customers-and-calculate-totals)
7. [Use LAG and running totals](#7-use-lag-and-running-totals)
8. [Pass values into SQL](#8-pass-values-into-sql)
9. [Combine SQL and DataFrame functions](#9-combine-sql-and-dataframe-functions)
10. [Write and read Parquet](#10-write-and-read-parquet)
11. [Choose the right style and avoid surprises](#11-choose-the-right-style-and-avoid-surprises)
12. [Practice and further reading](#12-practice-and-further-reading)

## 1. What changes for a SQL developer?

| Familiar idea | Spark/PySpark equivalent |
|---|---|
| Database connection/session | `SparkSession`, commonly named `spark` |
| Table-like result | DataFrame: distributed rows with a schema |
| SQL query execution | `spark.sql("SELECT ...")` returns a DataFrame |
| Named intermediate relation | A temporary view or a query-scoped CTE |
| Read a source | DataFrame reader for files/tables; then register a view |
| Save a result | DataFrame writer or supported table SQL |
| Stored procedure control flow | Python functions/configuration coordinating SQL steps |

Spark SQL and the DataFrame API use the same structured query engine. Equivalent built-in expressions can have equivalent plans; choosing SQL is not inherently slower or faster. Choose the expression that is easiest for your team to read and maintain. See the [Spark SQL guide](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html).

A DataFrame is not a pandas DataFrame containing every row on your laptop. Most transformations build a plan; actions such as `show`, `count`, and writes execute work across Spark's resources.

## 2. Start Spark

For interactive practice, start `pyspark --master "local[2]"`, then run the Python blocks below in that session or an equivalently configured notebook.

For a script, create `jobs/sql_and_pyspark.py`, copy the Python blocks from sections 2–10 into it in order, and run:

```bash
spark-submit --master "local[2]" --deploy-mode client jobs/sql_and_pyspark.py
```

The script file is one you create from this note; it is not bundled separately. Output will be written relative to the working directory.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("sql-and-pyspark")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")  # small local exercise only
    .getOrCreate()
)
```

`F` aliases **`pyspark.sql.functions`**. For example, `F.col("amount")` references a column and `F.lit("orders")` creates a constant. You can use these when a Python expression is convenient; SQL transformations do not require `F`.

Master/deploy choices belong in the submission command. Do not hardcode `.master("local[2]")` in code you intend to submit to a remote cluster.

## 3. Create inputs and expose them to SQL

The orders feed contains eight event rows: a correction to order 2 and one invalid order. Amounts and dates arrive as strings, as they often do in CSV ingestion. All amounts use one currency.

```python
raw_orders = spark.createDataFrame([
    ("E001", 1, "C001", "2026-01-01", "COMPLETE",  "books",       "35.50",  "2026-01-01 10:00:00"),
    ("E002", 2, "C002", "2026-01-01", "COMPLETE",  "electronics", "299.99", "2026-01-01 11:00:00"),
    ("E003", 3, "C001", "2026-01-02", "CANCELLED", "books",       "15.00",  "2026-01-02 10:00:00"),
    ("E004", 4, "C003", "2026-01-02", "COMPLETE",  "grocery",     "42.25",  "2026-01-02 11:00:00"),
    ("E005", 5, "C002", "2026-01-03", "COMPLETE",  "electronics", "99.99",  "2026-01-03 10:00:00"),
    ("E006", 6, "C004", "2026-01-03", "RETURNED",  "grocery",     "18.75",  "2026-01-03 11:00:00"),
    ("E007", 2, "C002", "2026-01-01", "COMPLETE",  "electronics", "279.99", "2026-01-04 09:00:00"),
    ("E008", 7, None,   "2026-01-03", "COMPLETE",  "books",       "-5.00",  "2026-01-03 12:00:00"),
], """event_id string, order_id long, customer_id string, order_date string,
        status string, category string, amount string, updated_at string""")

customers = spark.createDataFrame([
    ("C001", "Asha", "CA"),
    ("C002", "Ben", "NY"),
    ("C003", "Cara", "TX"),
    ("C004", "Dev", "WA"),
], "customer_id string, customer_name string, state string")

raw_orders.createOrReplaceTempView("raw_orders")
customers.createOrReplaceTempView("customers")
```

`createOrReplaceTempView` makes a DataFrame available by name to SQL in the same Spark session. It does not copy the data, create a permanent table, cache results, or register anything in Glue. A CTE (`WITH ...`) lasts only for its SQL statement; a temporary view can be reused by later statements in the session.

In a file-based job, replace `createDataFrame` with a configured reader such as `spark.read.schema(...).option("header", True).csv(input_path)`, then register the resulting DataFrame in the same way. Validate required columns before querying them.

## 4. Parse fields and separate rejected rows

Use SQL built-ins to normalize text and convert source values. `TRY_CAST` returns null when a value cannot be converted, allowing a validation step to classify it.

```python
typed_orders = spark.sql("""
SELECT
    event_id,
    order_id,
    NULLIF(TRIM(customer_id), '') AS customer_id,
    TRY_CAST(order_date AS DATE) AS order_date,
    UPPER(TRIM(status)) AS status,
    LOWER(TRIM(category)) AS category,
    TRY_CAST(amount AS DECIMAL(12,2)) AS amount,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM raw_orders
""")
typed_orders.createOrReplaceTempView("typed_orders")

validated_orders = spark.sql("""
SELECT *,
    CASE
        WHEN order_id IS NULL THEN 'missing_order_id'
        WHEN customer_id IS NULL THEN 'missing_customer_id'
        WHEN order_date IS NULL THEN 'invalid_order_date'
        WHEN amount IS NULL THEN 'invalid_amount'
        WHEN amount < 0 THEN 'negative_amount'
        WHEN updated_at IS NULL THEN 'invalid_updated_at'
        ELSE NULL
    END AS dq_error
FROM typed_orders
""")
validated_orders.createOrReplaceTempView("validated_orders")

accepted_events = spark.sql("SELECT * FROM validated_orders WHERE dq_error IS NULL")
rejected_events = spark.sql("SELECT * FROM validated_orders WHERE dq_error IS NOT NULL")
accepted_events.createOrReplaceTempView("accepted_events")
rejected_events.select("event_id", "order_id", "dq_error").show()
```

Expected: **7 accepted events and 1 rejected event**. E008/order 7 is rejected as `missing_customer_id`. It also has a negative amount; this CASE records only the first matching reason. Extend it if you need all reasons.

`DECIMAL(12,2)` represents monetary values without binary floating-point rounding; define how excess fractional digits, currency, and overflow should be handled. Retain raw inputs for diagnosis because invalid typed fields become null.

These checks cover the sample rules, not a complete source contract. For example, allowed status values and missing event IDs need additional rules in production.

## 5. Select the latest version and use CASE WHEN

A normal `DISTINCT` cannot decide which correction to keep. Use a window function and an explicit ordering rule:

```python
current_orders = spark.sql("""
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC, event_id DESC
        ) AS rn
    FROM accepted_events
)
SELECT
    order_id, customer_id, order_date, status, category, amount, updated_at,
    CASE WHEN amount >= 100 THEN 'HIGH' ELSE 'STANDARD' END AS amount_band
FROM ranked
WHERE rn = 1
""")
current_orders.createOrReplaceTempView("current_orders")
current_orders.orderBy("order_id").show()
```

Expected: **6 current orders**. Order 2 has amount **279.99** and band HIGH; the older 299.99 event is no longer selected. The other orders have band STANDARD.

`event_id` breaks timestamp ties in this sample. Real feeds need an agreed source sequence/version and deterministic tie-breaker. This query selects the latest **accepted** event within the input, not across all historical runs. If an invalid latest correction must block the whole key instead of falling back to an older valid event, enforce that policy before publication.

Use a CTE/subquery to filter `ROW_NUMBER()` output; do not assume a database-specific `QUALIFY` clause works in your Spark runtime. The window's `PARTITION BY` groups rows for calculation; it does not create output folders.

## 6. Join customers and calculate totals

Use a CTE to keep the business rule separate from the join and aggregation:

```python
state_summary = spark.sql("""
WITH completed_orders AS (
    SELECT order_id, customer_id, amount
    FROM current_orders
    WHERE status = 'COMPLETE'
)
SELECT
    COALESCE(c.state, 'UNKNOWN') AS state,
    COUNT(*) AS order_count,
    SUM(o.amount) AS total_amount
FROM completed_orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
GROUP BY COALESCE(c.state, 'UNKNOWN')
""")
state_summary.orderBy("state").show()
```

Expected:

| state | order_count | total_amount |
|---|---|---|
| CA | 1 | 35.50 |
| NY | 2 | 379.98 |
| TX | 1 | 42.25 |

The totals exclude CANCELLED and RETURNED orders and include the corrected order 2 amount. The lookup has one row per customer key; duplicates in it could multiply orders and inflate totals. A left join preserves unmatched orders, which the query groups under UNKNOWN.

For conditional aggregation, SQL remains straightforward:

```python
customer_counts = spark.sql("""
SELECT customer_id,
       COUNT(*) AS all_orders,
       SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END) AS completed_orders
FROM current_orders
GROUP BY customer_id
""")
customer_counts.orderBy("customer_id").show()
```

C002 has two completed orders; C004 has one order but zero completed orders.

## 7. Use LAG and running totals

Windows keep detail rows while calculating across related rows:

```python
order_history = spark.sql("""
SELECT
    customer_id, order_id, order_date, amount,
    LAG(amount) OVER (
        PARTITION BY customer_id ORDER BY order_date, order_id
    ) AS previous_order_amount,
    SUM(amount) OVER (
        PARTITION BY customer_id ORDER BY order_date, order_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_amount
FROM current_orders
WHERE status = 'COMPLETE'
""")
order_history.orderBy("customer_id", "order_date", "order_id").show()
```

For C002:

| order_id | amount | previous_order_amount | running_amount |
|---|---|---|---|
| 2 | 279.99 | NULL | 279.99 |
| 5 | 99.99 | 279.99 | 379.98 |

Use an explicit `ROWS` frame for a row-by-row cumulative sum. Default ordered frames can include peer rows with equal sort values. The `WHERE` filter is applied before these windows, so previous order means previous **completed** order here. These windows order by business order date, not by the correction's arrival timestamp.

## 8. Pass values into SQL

For Spark 3.5.x, bind Python values using named SQL parameters:

```python
from datetime import date

recent_orders = spark.sql(
    "SELECT * FROM current_orders WHERE order_date >= :start_date",
    args={"start_date": date(2026, 1, 2)},
)
recent_orders.orderBy("order_id").show()
```

Expected order IDs: **3, 4, 5, 6**. Bind values instead of concatenating untrusted text into SQL strings. Parameter values are not arbitrary SQL fragments or column lists; validate dynamic identifiers/configuration separately. Check runtime support when moving to a different managed Spark version. See [`SparkSession.sql`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html).

## 9. Combine SQL and DataFrame functions

For SQL users, remember **read → inspect → name → shape → write**. You can do useful work without memorizing the whole DataFrame API.

| Remember | Small toolkit | When it adds value around SQL |
|---|---|---|
| Read | `spark.read.schema(...).csv(...)`, `spark.read.parquet(...)` | Control input schema, file format, and read options |
| Inspect | `df.printSchema()`, `df.show(5, truncate=False)` | Check types and a small sample while developing |
| Name | `df.createOrReplaceTempView("name")` | Make a DataFrame available to your SQL queries |
| Shape | `df.select(...)`, `df.filter(...)`, `df.withColumn(...)` | Select configured columns, apply a small rule, or add metadata |
| Write | `df.write.mode(...).parquet(...)` | Control output format, destination, and write behavior |

`select`, `filter`, and `withColumn` return new DataFrames. `show` executes work; `printSchema` displays the known schema. `count()` also executes work, so use it for intentional checks rather than inspecting every step automatically.

### Inspect: check types and sample rows

Run after the SQL examples above:

```python
state_summary.printSchema()
state_summary.orderBy("state").show(5, truncate=False)
```

Confirm three states and a decimal `total_amount`. This is often more helpful during development than collecting the whole result into Python.

### Shape: choose columns and add metadata

SQL already produced the business result. Use `select` for a configurable output contract and `withColumn` to add a constant:

```python
selected_columns = ["state", "order_count", "total_amount"]
report = (
    state_summary
    .select(*selected_columns)
    .withColumn("report_name", F.lit("completed_orders_by_state"))
)
report.orderBy("state").show()
```

`*selected_columns` expands the Python list into arguments. This is useful when a shared pipeline chooses columns from validated configuration. `withColumn` adds a column, or replaces it if the name already exists. All three state totals remain unchanged.

### Filter: apply a small Python-supplied rule

```python
minimum_total = 100
large_state_totals = report.filter(F.col("total_amount") >= minimum_total)
large_state_totals.select("state", "total_amount").show()
```

Expected: **NY, 379.98**. `filter` and `where` are aliases; remember whichever feels natural. This does not change `report`, so section 10 still writes all three states.

### Remember a few `F` expressions

`F` is an alias for **`pyspark.sql.functions`**, imported in section 2. DataFrame methods describe the operation; `F.*` functions build the expressions inside it.

| Expression | Remember it as | Example use |
|---|---|---|
| `F.col("name")` | A column | Reference `total_amount` in a condition |
| `F.lit(value)` | A constant | Add a source name or run identifier |
| `F.when(...).otherwise(...)` | CASE WHEN | Add a small conditional label |
| `F.coalesce(...)` | First non-null value | Apply an agreed default; empty strings are not null |
| `F.upper(...)` | Uppercase text | Normalize text with a built-in rather than a Python UDF |

For example, label the existing report using built-ins:

```python
labeled_report = (
    report
    .withColumn("state", F.upper(F.coalesce(F.col("state"), F.lit("UNKNOWN"))))
    .withColumn("total_band",
                F.when(F.col("total_amount") >= 100, F.lit("HIGH"))
                 .otherwise(F.lit("STANDARD")))
)
labeled_report.select("state", "total_amount", "total_band").orderBy("state").show()
```

Expected bands: **CA → STANDARD, NY → HIGH, TX → STANDARD**. The SQL query already handles missing state values; this shows how to express that default when another input requires it. These are native Spark expressions, not Python UDFs.

Keep complex CASE logic, joins, CTEs, and windows in SQL if your team reads them more easily there. Use this small DataFrame toolkit for the surrounding pipeline work, and learn more methods when a concrete requirement calls for them. The [PySpark 101 comparison](pyspark-101-examples.md#when-to-use-spark-sql-vs-pyspark-dataframe-functions) contains additional examples.

## 10. Write and read Parquet

Validate small exercise outputs, then write to a fresh local destination:

```python
assert raw_orders.count() == 8
assert accepted_events.count() == 7
assert rejected_events.count() == 1
assert current_orders.count() == 6
assert state_summary.count() == 3

output_path = "data/output/sql_and_pyspark/demo001"
report.write.mode("errorifexists").partitionBy("state").parquet(output_path)

saved_report = spark.read.parquet(output_path)
saved_report.orderBy("state").show()
spark.stop()
```

The output is a directory with paths such as `state=NY/part-....parquet`. Reading the root reconstructs the partition column. The order of files or rows on disk is not guaranteed; use ORDER BY when displaying ordered results.

A second run against `demo001` fails deliberately. Choose a fresh attempt path for another exercise; do not blindly append a full recomputed report or overwrite a location readers depend on. Production publication, replay, and table updates need a defined protocol—see the [pipeline production design](pyspark-pipeline-production-design.md) and [Glue/Iceberg guide](aws_glue_iceberg/README.md).

The assertions and repeated counts are for learning. At scale, avoid unnecessary repeated actions and define quality checks appropriate to your workload. SQL transformations do not make these writes a transaction across multiple destinations.

## 11. Choose the right style and avoid surprises

| Situation | Practical habit |
|---|---|
| Team is very comfortable with SQL | Start with SQL transformations; introduce DataFrame expressions where they simplify the job |
| SQL or method chains become long | Split into named, reviewable steps; avoid changing styles for every small expression |
| Source SQL comes from another database | Check Spark's dialect, functions, date handling, and supported table operations |
| A null comparison behaves unexpectedly | Use `IS NULL`; ordinary comparisons can return unknown and filters keep only true rows |
| You need null-safe equality | Spark SQL provides `<=>`; use deliberately rather than treating all nulls as equal automatically |
| A window chooses inconsistent rows | Supply a deterministic tie-breaker and the intended frame |
| Data is large | Use distributed operations; do not `collect()` or `toPandas()` the whole dataset |
| Query looks slow | Inspect `result.explain("formatted")`, input scans, joins, shuffles, and skew; API choice alone is not the explanation |
| Custom Python logic seems necessary | Try built-in SQL/DataFrame functions first; a Python UDF adds worker/transfer cost even when called from SQL |
| You want UPDATE/DELETE/MERGE | Check the engine and table format; a temporary view over plain files is not a transactional database table |

A temporary view is a named plan, not a materialized result. Referencing it again may recompute work; a CTE does not automatically cache data either. Cache only where measured reuse warrants it, and release the cache when finished.

## 12. Practice and further reading

Try these exercises by changing the inputs and rerunning in a fresh session/output attempt:

- Add an unknown customer and confirm the summary retains its completed order under UNKNOWN.
- Add a later correction to order 5 and verify deduplication and totals.
- Change the CASE threshold and add a separate missing-amount category where appropriate.
- Calculate monthly category totals and top two orders per customer.
- Compare SQL and DataFrame versions using the same expected result, not just row counts.

Continue with [PySpark 101](pyspark-101-examples.md), including its [SQL/DataFrame comparison and UDF note](pyspark-101-examples.md#when-to-use-spark-sql-vs-pyspark-dataframe-functions).

Official references: [getting started with SQL and temporary views](https://spark.apache.org/docs/3.5.6/sql-getting-started.html), [CTEs](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-cte.html), [window functions](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-window.html), and [null semantics](https://spark.apache.org/docs/3.5.6/sql-ref-null-semantics.html).
