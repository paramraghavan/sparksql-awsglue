# Spark New User Use Cases

This note helps SQL and Python users understand how to approach PySpark work in real projects.

The most common beginner mistake is to start coding PySpark immediately. A better approach is:

```text
Understand the data -> understand the SQL logic -> understand the size/performance risks -> then write PySpark
```

Use this note in two passes:

1. First pass: understand the workflow and the questions to ask.
2. Second pass: use the mini template as a starting shape for your own job.

Two rules will save you many hours:

- Know the grain of every DataFrame before you join it.
- Validate row counts after each join, especially when translating known SQL logic.

## Use Case 1: Simple Ingestion And Transformation

This is the best first use case for new Spark users.

Example:

```text
Input:
  raw/orders/orders.csv

Output:
  trusted/orders/
  transformed/orders_summary/
```

Typical steps:

1. Read a raw file.
2. Validate required columns.
3. Cast data types.
4. Filter bad records.
5. Select interested columns.
6. Add metadata columns such as `run_id`, `source_file`, and `ingestion_ts`.
7. Write trusted Parquet.
8. Optionally aggregate or transform trusted data into a final output.

SQL mental model:

```sql
select
  cast(order_id as int) as order_id,
  customer_id,
  cast(order_date as date) as order_date,
  cast(amount as double) as amount
from raw_orders
where order_id is not null
  and amount >= 0;
```

PySpark mental model:

```python
from pyspark.sql import functions as F

trusted = (
    raw
    .where(F.col("order_id").isNotNull())
    .where(F.col("amount").cast("double") >= 0)
    .select(
        F.col("order_id").cast("int").alias("order_id"),
        "customer_id",
        F.to_date("order_date").alias("order_date"),
        F.col("amount").cast("double").alias("amount"),
    )
    .withColumn("ingestion_ts", F.current_timestamp())
)

trusted.write.mode("overwrite").parquet("data/trusted/orders")
```

Production note: filtering bad rows is fine for learning, but real ingestion jobs usually write rejected records with a reason. That gives support teams a way to explain what happened to source records that did not enter trusted data.

This use case teaches the core Spark skills:

- read files,
- inspect schema,
- cast columns,
- filter rows,
- write Parquet,
- understand partitions and `part-*` files.

## Use Case 2: Large Tables And Final Score Calculation

This is closer to real enterprise PySpark work.

Example source tables:

```text
loan
pool
pricing
borrower
property
rate_history
```

Goal:

```text
Read all required tables -> perform SQL-style joins/views -> calculate final_score -> write final_score table
```

If you already know the SQL joins needed to create the final score, do not start by translating SQL line by line. First analyze the data and execution risk.

Good beginner approach:

```text
1. Run the known SQL on a small sample or one date.
2. Recreate the same result in PySpark.
3. Compare row counts, keys, nulls, and sample scores.
4. Only then scale to the full dataset.
```

## Before You Write PySpark: Analysis Checklist

### 1. Understand The Final Output

Ask:

- What is the final output table name?
- What is the grain of the final output?
- Is it one row per loan?
- One row per loan and month?
- One row per pool?
- Which columns are required?
- Which column is the primary business key?
- Can there be duplicates?

Example:

```text
final_score grain:
  one row per loan_id per as_of_date

primary key:
  loan_id, as_of_date
```

This matters because every join should preserve or intentionally change the grain.

Write this down before coding. If the final score is one row per `loan_id, as_of_date`, every transformation should protect that grain unless you intentionally aggregate or explode it.

### 2. Understand Each Input Table

Create a small table inventory.

| Table | Grain | Key columns | Approx size | Partition columns | Notes |
|---|---|---|---:|---|---|
| `loan` | one row per loan | `loan_id` | large | `as_of_date` | base/fact table |
| `pool` | one row per pool | `pool_id` | medium | none or `as_of_date` | lookup/dimension |
| `pricing` | one row per loan/date | `loan_id`, `as_of_date` | large | `as_of_date` | time-sensitive |
| `borrower` | one row per borrower | `borrower_id` | medium | none | may duplicate if history |
| `property` | one row per property | `property_id` | medium | none | lookup/dimension |

For each table, check:

```python
df.count()
df.printSchema()
df.select("key_column").distinct().count()
df.groupBy("key_column").count().where("count > 1").show()
```

For large tables, do not run many full counts repeatedly in production. Use table statistics, metadata, sampling, or counts already produced by the pipeline.

Also capture:

- source path or catalog table name,
- file format,
- partition columns,
- expected refresh pattern,
- date/time columns,
- nullable columns,
- known data quality issues.

### 3. Identify The Driving Table

The driving table is usually the table that defines the output grain.

For final score, the driving table might be:

```text
loan snapshot for the current as_of_date
```

Example:

```python
loan_base = (
    spark.read.parquet("s3a://de-lab/trusted/loan")
    .where(F.col("as_of_date") == F.lit("2025-09-30"))
)
```

Then join other tables to this base.

If you choose the wrong driving table, the rest of the job becomes harder. For example, starting from `pricing` instead of `loan` may accidentally drop loans that do not have pricing yet, or create duplicate score rows.

### 4. Document Join Keys And Join Types

Before coding, write the join plan.

| Step | Left table | Right table | Join keys | Join type | Expected row impact |
|---|---|---|---|---|---|
| 1 | `loan_base` | `pool` | `pool_id` | left | same row count as loan |
| 2 | result | `pricing` | `loan_id`, `as_of_date` | left | same row count if pricing is unique |
| 3 | result | `borrower` | `borrower_id` | left | same row count if borrower is unique |

This is one of the most important habits for SQL users moving to Spark.

If a join unexpectedly increases row count, you likely have duplicate keys on the right side.

If a join unexpectedly decreases row count, check whether you used `inner` join when the SQL view used `left` join.

### 5. Check For Duplicate Join Keys

Before joining:

```python
pricing_dupes = (
    pricing
    .groupBy("loan_id", "as_of_date")
    .count()
    .where(F.col("count") > 1)
)

pricing_dupes.show(20, truncate=False)
```

If the right side has duplicates, decide the business rule:

- keep latest record,
- aggregate first,
- reject duplicates,
- choose one using `row_number`,
- join to all records intentionally.

Example keep latest:

```python
from pyspark.sql import Window

w = Window.partitionBy("loan_id", "as_of_date").orderBy(F.col("updated_ts").desc())

pricing_latest = (
    pricing
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)
```

### 6. Estimate Table Sizes And Join Strategy

Spark joins can be expensive because data may shuffle across executors.

Ask:

- Which tables are large?
- Which tables are small lookups?
- Can any lookup be broadcast?
- Are the large tables partitioned by the filter column?
- Are the join keys skewed?

Broadcast a small lookup table:

```python
from pyspark.sql import functions as F

joined = loan_base.join(F.broadcast(pool_lookup), "pool_id", "left")
```

Use broadcast only when the lookup is small enough to fit in executor memory.

Never broadcast a table just because it is called a lookup. First confirm it is actually small. A dimension table can still be too large to broadcast.

### 7. Filter Early And Select Only Needed Columns

Do not carry 200 columns through every join if final score needs only 25.

Good habit:

```python
loan_base = (
    spark.read.parquet("s3a://de-lab/trusted/loan")
    .where(F.col("as_of_date") == F.lit("2025-09-30"))
    .select(
        "loan_id",
        "pool_id",
        "borrower_id",
        "property_id",
        "as_of_date",
        "current_balance",
        "delinquency_status",
    )
)
```

This reduces memory, network shuffle, and file scanning.

For partitioned tables, filter on partition columns as early as possible. If a table is partitioned by `as_of_date`, this is good:

```python
pricing = spark.read.parquet("s3a://de-lab/trusted/pricing").where(
    F.col("as_of_date") == F.lit("2025-09-30")
)
```

This allows Spark to skip unrelated folders when the partition layout supports pruning.

### 8. Translate SQL Views Into DataFrame Steps

If your SQL has views like:

```sql
create view score_input as
select ...
from loan l
left join pool p on l.pool_id = p.pool_id
left join pricing pr on l.loan_id = pr.loan_id
```

Translate into named PySpark DataFrames:

```python
loan_base = ...
pool_lookup = ...
pricing_latest = ...

score_input = (
    loan_base.alias("l")
    .join(pool_lookup.alias("p"), "pool_id", "left")
    .join(pricing_latest.alias("pr"), ["loan_id", "as_of_date"], "left")
)
```

Use names that match the business steps. This makes the code easier for SQL users to review.

### 8A. Use Spark SQL As A Bridge When Helpful

If the team already trusts the SQL, you can use Spark SQL first, then translate to DataFrames later.

```python
loan_base.createOrReplaceTempView("loan_base")
pool_lookup.createOrReplaceTempView("pool_lookup")
pricing_latest.createOrReplaceTempView("pricing_latest")

score_input_sql = spark.sql("""
select
  l.loan_id,
  l.as_of_date,
  l.pool_id,
  l.current_balance,
  l.loan_to_value,
  p.pool_type,
  pr.pricing_amount
from loan_base l
left join pool_lookup p
  on l.pool_id = p.pool_id
left join pricing_latest pr
  on l.loan_id = pr.loan_id
 and l.as_of_date = pr.as_of_date
""")
```

This is not "less Spark." Spark SQL and the DataFrame API both create Spark execution plans. For SQL-heavy users, Spark SQL can be the fastest path to a correct first version.

### 9. Calculate Final Score In Clear Steps

Avoid putting every rule into one giant expression.

Example:

```python
score = (
    score_input
    .withColumn(
        "ltv_score",
        F.when(F.col("loan_to_value") <= 60, F.lit(100))
        .when(F.col("loan_to_value") <= 80, F.lit(80))
        .otherwise(F.lit(50)),
    )
    .withColumn(
        "delinquency_score",
        F.when(F.col("delinquency_status") == "CURRENT", F.lit(100))
        .when(F.col("delinquency_status") == "30_DAYS", F.lit(70))
        .otherwise(F.lit(30)),
    )
    .withColumn(
        "final_score",
        F.round((F.col("ltv_score") * 0.6) + (F.col("delinquency_score") * 0.4), 2),
    )
)
```

For complex scoring, keep the business rules in config or separate functions when the project pattern supports it.

Also decide how to handle missing inputs:

```python
score = score.withColumn(
    "final_score_status",
    F.when(F.col("pricing_amount").isNull(), F.lit("MISSING_PRICING"))
    .otherwise(F.lit("SCORED")),
)
```

Do not silently convert important missing fields to zero unless the business rule says zero is correct.

### 10. Validate Row Counts After Each Join

For development, capture counts after major joins.

```python
loan_count = loan_base.count()
after_pool_count = after_pool.count()
after_pricing_count = after_pricing.count()

print("loan_count", loan_count)
print("after_pool_count", after_pool_count)
print("after_pricing_count", after_pricing_count)
```

Expected:

```text
If joins are left joins to unique lookup tables, row count should usually stay the same.
```

Also check nulls introduced by joins:

```python
score_input.where(F.col("pricing_amount").isNull()).count()
```

Nulls may be valid, but they should be understood.

For final-score work, also validate uniqueness:

```python
score.groupBy("loan_id", "as_of_date").count().where("count > 1").show()
```

If this returns rows, the final output violates the expected grain.

### 11. Look At The Spark Plan

Use:

```python
score_input.explain()
```

Look for:

- broadcast hash join for small lookups,
- sort merge join for large joins,
- exchange/shuffle steps,
- filters being pushed down,
- partition pruning.

You do not need to understand every line on day one. Start by noticing where shuffles happen.

If the plan has many `Exchange` steps, the job is moving data across the cluster. That is common for joins and aggregations, but it is where many slow Spark jobs spend time.

### 12. Write The Final Score Output

Example:

```python
(
    score
    .select(
        "loan_id",
        "as_of_date",
        "pool_id",
        "final_score",
        "ltv_score",
        "delinquency_score",
    )
    .write
    .mode("overwrite")
    .partitionBy("as_of_date")
    .parquet("s3a://de-lab/transformed/final_score")
)
```

For production, be clear whether this is:

- full refresh,
- overwrite one `as_of_date` partition,
- append-only,
- merge/update using Iceberg, Hudi, or Delta.

If you are processing only one `as_of_date`, prefer replacing only that date partition over replacing the entire final-score table.

## Beginner Development Workflow

Use this workflow when converting known SQL into PySpark:

1. Write the final output grain and key.
2. Inventory every input table.
3. Identify the driving table.
4. List joins, keys, and join types.
5. Check duplicate keys on every join input.
6. Filter early by date or partition.
7. Select only needed columns.
8. Build one join at a time.
9. Validate row counts after every join.
10. Add scoring columns.
11. Validate final output against SQL/sample expected results.
12. Write Parquet with the correct partition strategy.
13. Check Spark UI for slow stages, shuffle, spill, skew, and failed tasks.

## Practical Day-One Contribution Plan

If you are new to the project and need to contribute quickly:

1. Ask for one known working SQL query or view definition.
2. Ask for one small date or batch to test, such as `as_of_date = '2025-09-30'`.
3. Ask for expected row count and a few expected sample records.
4. Read only the required input columns.
5. Recreate the SQL logic in Spark SQL or DataFrames.
6. Compare output against the known SQL result.
7. Add validation checks for row count, duplicate keys, nulls, and score range.
8. Write output to a development path first.
9. Review the Spark UI before calling the job done.

This is how you avoid the classic mistake: "The job ran successfully, but the output is wrong."

## Questions To Ask Before Starting The Final Score Job

- What is the final score grain?
- What is the business key?
- What date or snapshot am I processing?
- Are all input tables for the same `as_of_date`?
- Which tables are large facts?
- Which tables are small lookups?
- Are lookup keys unique?
- Are any joins many-to-many?
- Are any keys skewed?
- Which columns are actually needed?
- Should output overwrite a partition or append?
- How will I validate the final score?
- What score range is valid?
- What should happen if pricing, pool, borrower, or property data is missing?
- Should late-arriving corrections replace prior scores?
- Who owns the business rules for the score?

## Validation Checklist For Final Score

Before handing off the result, check:

| Check | Example |
|---|---|
| Row count | output row count equals expected grain count |
| Duplicate key | no duplicate `loan_id, as_of_date` |
| Required columns | no missing required output columns |
| Join misses | count rows with missing pool/pricing/borrower/property |
| Score range | `final_score` is within expected min/max |
| SQL reconciliation | compare against known SQL for a sample date |
| Partition output | only intended `as_of_date` partition was written |

Example checks:

```python
final_score_count = final_score.count()

duplicate_scores = (
    final_score
    .groupBy("loan_id", "as_of_date")
    .count()
    .where("count > 1")
)

missing_pricing_count = final_score.where(F.col("pricing_amount").isNull()).count()

invalid_score_count = final_score.where(
    (F.col("final_score") < 0) | (F.col("final_score") > 100)
).count()

print("final_score_count", final_score_count)
print("duplicate_scores", duplicate_scores.count())
print("missing_pricing_count", missing_pricing_count)
print("invalid_score_count", invalid_score_count)
```

For very large tables, avoid running all expensive checks repeatedly. Run them during development, then keep the most important checks in production metrics.

## Mini Template For A Large Join Job

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("final-score")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    as_of_date = "2025-09-30"

    # Driving table: this defines the output grain.
    # Expected final grain: one row per loan_id per as_of_date.
    loan = (
        spark.read.parquet("s3a://de-lab/trusted/loan")
        .where(F.col("as_of_date") == F.lit(as_of_date))
        .select("loan_id", "pool_id", "borrower_id", "as_of_date", "current_balance", "loan_to_value")
    )

    # Lookup table: keep only required columns and make the join key unique.
    # In a real job, do not use dropDuplicates blindly if business rules require
    # latest/effective-dated lookup logic.
    pool = (
        spark.read.parquet("s3a://de-lab/trusted/pool")
        .select("pool_id", "pool_type", "pool_rating")
        .dropDuplicates(["pool_id"])
    )

    # Time-sensitive table: filter to the same snapshot date before joining.
    pricing = (
        spark.read.parquet("s3a://de-lab/trusted/pricing")
        .where(F.col("as_of_date") == F.lit(as_of_date))
        .select("loan_id", "as_of_date", "pricing_amount", "updated_ts")
    )

    # If pricing has multiple rows per loan/date, keep the latest one so the
    # left join does not multiply final-score rows.
    w = Window.partitionBy("loan_id", "as_of_date").orderBy(F.col("updated_ts").desc())
    pricing_latest = (
        pricing
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    # Build joins one step at a time during development and validate row counts
    # after each join. The compact version is shown here.
    score_input = (
        loan.alias("l")
        .join(F.broadcast(pool).alias("p"), "pool_id", "left")
        .join(pricing_latest.alias("pr"), ["loan_id", "as_of_date"], "left")
    )

    # Keep scoring rules readable. A reviewer should be able to map each
    # withColumn back to a business rule.
    final_score = (
        score_input
        .withColumn(
            "balance_score",
            F.when(F.col("current_balance") < 100000, F.lit(100))
            .when(F.col("current_balance") < 500000, F.lit(80))
            .otherwise(F.lit(60)),
        )
        .withColumn(
            "ltv_score",
            F.when(F.col("loan_to_value") <= 60, F.lit(100))
            .when(F.col("loan_to_value") <= 80, F.lit(80))
            .otherwise(F.lit(50)),
        )
        .withColumn("final_score", F.round((F.col("balance_score") + F.col("ltv_score")) / 2, 2))
        .withColumn(
            "score_status",
            F.when(F.col("pricing_amount").isNull(), F.lit("MISSING_PRICING"))
            .otherwise(F.lit("SCORED")),
        )
    )

    # Development validations. Keep the important ones as production metrics.
    print("loan rows", loan.count())
    print("final_score rows", final_score.count())
    print(
        "duplicate final_score keys",
        final_score.groupBy("loan_id", "as_of_date").count().where("count > 1").count(),
    )

    final_score.explain()

    # Dynamic partition overwrite means this run replaces only the as_of_date
    # present in final_score instead of replacing the entire table path.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    final_score.write.mode("overwrite").partitionBy("as_of_date").parquet(
        "s3a://de-lab/transformed/final_score"
    )

    spark.stop()


if __name__ == "__main__":
    main()
```

## Interview Explanation

Use this explanation:

```text
When converting SQL joins to PySpark for a large final-score table, I first identify the output grain, driving table, join keys, table sizes, partition filters, duplicate-key risks, and expected row counts. Then I read only needed columns, filter early, deduplicate lookup inputs where required, join one step at a time, validate row counts after each join, calculate score columns, and write partitioned Parquet. If performance is slow, I check the Spark UI for shuffle, spill, skew, task duration, and executor issues.
```
