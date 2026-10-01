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
|---------------------------|--------------------------------------------------------|
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

## 3. Setup Pattern Used In Examples

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

## 10. Handle Nulls

```python
orders.na.drop()
orders.na.drop(subset=["customer_id", "order_id"])
orders.na.fill({"status": "UNKNOWN", "amount": 0.0})
```

Common null-safe expressions:

```python
df = orders.withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
df = orders.withColumn("has_customer", F.col("customer_id").isNotNull())
```

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

## 13. De-Duplicate Data

```python
orders.dropDuplicates()
orders.dropDuplicates(["order_id"])
```

Keep the latest row per business key:

```python
w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

latest = (
    df
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)
```

## 14. Window Functions

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

## 16. CRUD In Spark vs CRUD In Databases

SQL/database users usually think of CRUD like this:

| Database CRUD      | SQL example                      | Spark/file-lake equivalent                                         |
|--------------------|----------------------------------|--------------------------------------------------------------------|
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
|-------------|---------------------------------------|
| `overwrite` | Replace existing output path          |
| `append`    | Add new files to existing output path |
| `error`     | Fail if output exists                 |
| `ignore`    | Do nothing if output exists           |

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
awslocal s3 ls s3://de-lab/curated/orders_parquet_updated/ --recursive
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
awslocal s3 rm s3://de-lab/curated/orders_csv/ --recursive
```

For real row-level `UPDATE`, `DELETE`, and `MERGE`, learn one of:

- Apache Iceberg
- Delta Lake
- Apache Hudi

These formats add table metadata and transaction-like behavior on top of object storage.

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

There are two kinds of paths:

| Path style                            | Where it writes                                    | Example                               |
|---------------------------------------|----------------------------------------------------|---------------------------------------|
| `data/input/...` or `data/output/...` | Your local computer, inside the lab/project folder | `data/output/orders_parquet`          |
| `s3a://de-lab/...`                    | The LocalStack S3 bucket named `de-lab`            | `s3a://de-lab/curated/orders_parquet` |

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

LocalStack is used only when the path starts with `s3a://` or when commands use `awslocal s3 ...`.

## 25. What LocalStack Is

LocalStack is a local AWS simulator. For this lab, assume Docker is acting like a small local AWS account running on
your laptop.

When you start LocalStack, Docker runs a container that exposes AWS-like services on:

```text
http://localhost:4566
```

In this guide:

- Real AWS S3 is not used.
- LocalStack S3 runs inside Docker.
- `awslocal s3 ...` talks to LocalStack instead of real AWS.
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
awslocal s3 ls s3://de-lab/curated/orders_parquet/ --recursive
```

To copy LocalStack output back to your project folder:

```bash
mkdir -p data/downloaded/orders_parquet

awslocal s3 cp \
  s3://de-lab/curated/orders_parquet/ \
  data/downloaded/orders_parquet/ \
  --recursive
```

This downloads from fake/local S3 to:

```text
data/downloaded/orders_parquet/
```

## 26. LocalStack S3 End-To-End Example

Start LocalStack:

```bash
localstack start -d
awslocal s3 mb s3://de-lab
awslocal s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv
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
awslocal s3 ls s3://de-lab/curated/orders_parquet/ --recursive
```

At this point, you have only created data files in LocalStack S3. You have not created a Glue table yet.

To practice Glue on LocalStack end to end, use the `Self-Contained LocalStack Glue Catalog Lab` section below. That lab creates:

1. The actual Parquet files in LocalStack S3.
2. The Glue database and external table metadata in LocalStack Glue Data Catalog.

## 27. AWS Glue Data Catalog

The AWS Glue Data Catalog is a central metadata store for data lake tables. For SQL/database users, think of it as a
shared metastore that tells tools where data is, what columns it has, how it is partitioned, and how to read it.

It does not store the actual rows for a normal S3 Parquet or CSV table. The actual data files stay in S3. The Data
Catalog stores metadata about those files.

SQL mental model:

| Database concept        | Glue Data Catalog equivalent                                      |
|-------------------------|-------------------------------------------------------------------|
| Database/schema         | Glue database                                                     |
| Table definition        | Glue table                                                        |
| Columns and data types  | Glue table schema                                                 |
| External table location | S3 path such as `s3://company-curated/orders/`                    |
| Partition metadata      | Known partition folders such as `order_year=2026/category=books/` |
| `CREATE EXTERNAL TABLE` | Create metadata that points to files outside the catalog          |
| `DROP TABLE`            | Delete the table metadata, not necessarily the S3 files           |

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

### Self-Contained LocalStack Glue Catalog Lab

This mini-lab creates both pieces needed for a Glue external table:

1. Actual Parquet data in LocalStack S3.
2. Glue Catalog metadata that points to that S3 location.

Run these commands from your lab folder:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
```

Start LocalStack:

```bash
localstack start -d
localstack status services
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
awslocal s3 mb s3://de-lab
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
awslocal s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv
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
awslocal s3 ls s3://de-lab/curated/orders_parquet/ --recursive
```

Now create Glue Catalog metadata in LocalStack. First create the Glue database:

```bash
awslocal glue create-database \
  --database-input '{"Name":"curated"}'
```

Then create a table definition file:

```bash
cat > /tmp/orders_glue_table.json <<'EOF'
{
  "Name": "orders",
  "TableType": "EXTERNAL_TABLE",
  "Parameters": {
    "classification": "parquet",
    "EXTERNAL": "TRUE"
  },
  "PartitionKeys": [
    {"Name": "order_year", "Type": "int"},
    {"Name": "category", "Type": "string"}
  ],
  "StorageDescriptor": {
    "Location": "s3://de-lab/curated/orders_parquet/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    },
    "Columns": [
      {"Name": "order_id", "Type": "int"},
      {"Name": "customer_id", "Type": "string"},
      {"Name": "order_date", "Type": "date"},
      {"Name": "status", "Type": "string"},
      {"Name": "amount", "Type": "double"}
    ]
  }
}
EOF
```

Create the table:

```bash
awslocal glue create-table \
  --database-name curated \
  --table-input file:///tmp/orders_glue_table.json
```

Verify the table metadata:

```bash
awslocal glue get-table \
  --database-name curated \
  --name orders
```

Important: `awslocal glue create-table` creates Glue Catalog metadata only. It does not create Parquet files. The files are created by the Spark job and live in LocalStack S3 at:

```text
s3://de-lab/curated/orders_parquet/
```

The relationship is:

```text
LocalStack S3:
  s3://de-lab/curated/orders_parquet/
  actual Parquet data files

LocalStack Glue Data Catalog:
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
|----------------------------|----------------|-------------------------------------------------------------------|
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
