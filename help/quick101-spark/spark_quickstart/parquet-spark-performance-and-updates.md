# Parquet, Spark Reads/Writes, Slow Jobs, And Updates

This note is for SQL and Python users who are learning how file-based data lakes behave differently from database tables.

## 1. What Is Parquet?

Parquet is a columnar binary file format used heavily in Spark, Athena, Glue, EMR, Databricks, and data lake projects.

For SQL users:

```text
CSV/JSON = text files
Parquet = analytics-optimized binary files with schema, compression, and column metadata
```

Parquet stores values by column instead of storing every row as plain text. That matters because analytics queries often read only a few columns from a wide table.

Example:

```sql
select customer_id, order_date, amount
from orders
where order_year = 2025;
```

If `orders` has 100 columns, Parquet can mostly read the columns needed by the query. CSV and JSON usually require parsing the whole text row.

## 2. Parquet vs CSV vs JSON

| Feature | CSV | JSON | Parquet |
|---|---|---|---|
| Human readable | Yes | Yes | No, binary |
| Stores schema | No | Not reliably | Yes |
| Good for nested data | No | Yes | Yes |
| Efficient compression | Limited | Limited | Yes, column-aware |
| Reads selected columns efficiently | No | No | Yes |
| Good for raw landing | Yes | Yes | Sometimes |
| Good for trusted/curated analytics | Usually no | Usually no | Yes |

Common data lake pattern:

```text
raw zone       -> source-shaped CSV, JSON, API extracts
trusted zone   -> clean Parquet with schema and partitions
transformed    -> Parquet tables ready for analytics
```

## 3. How Spark Decides Parquet File Size When Writing

Spark usually writes a folder of `part-*` files, not one file.

Example:

```python
df.write.mode("overwrite").parquet("data/output/orders")
```

Output:

```text
data/output/orders/
  part-00000-....snappy.parquet
  part-00001-....snappy.parquet
  _SUCCESS
```

Spark output file size depends mainly on:

- how many partitions the DataFrame has at write time,
- whether you used `repartition` or `coalesce`,
- whether you used `partitionBy`,
- how much data each task receives,
- how well the data compresses,
- write options such as `maxRecordsPerFile`.

Useful rule:

```text
One Spark task usually writes one output file per target partition folder.
```

Example:

```python
df.repartition(8).write.mode("overwrite").parquet("data/output/orders")
```

This usually creates around 8 Parquet part files.

Partitioned writes can create more files:

```python
df.repartition(8).write.mode("overwrite").partitionBy("order_month").parquet(
    "data/output/orders_by_month"
)
```

If the 8 Spark tasks contain records for several months, each task can write files into several `order_month=...` folders.

For tiny demos only:

```python
df.coalesce(1).write.mode("overwrite").parquet("data/output/small_demo")
```

Do not use `coalesce(1)` for large production output. It forces too much data through one task and can create a bottleneck.

To limit records per output file:

```python
df.write.option("maxRecordsPerFile", 500000).parquet("data/output/orders")
```

Production teams often aim for files around 128 MB to 1 GB compressed, depending on the query engine and workload.

Avoid:

- too many tiny files, because query planning and file listing become slow,
- very huge files, because there is less parallelism and more memory pressure per task.

## 4. What Happens When I Say `spark.read()`?

This surprises many beginners:

```python
df = spark.read.parquet("data/output/orders")
```

This does not immediately read all data into memory.

Spark is lazy. `spark.read.parquet(...)` creates a DataFrame and a plan. The actual data is read only when you run an action.

Actions include:

```python
df.show()
df.count()
df.collect()
df.write.parquet("data/output/copy")
```

Spark may read some metadata early, such as schema and Parquet footers, but it does not load the whole dataset into the driver.

## 5. Does Spark Read The Entire Parquet File Into Memory?

Usually no.

At a high level:

1. Spark lists the Parquet files.
2. Spark creates read tasks.
3. Executors read assigned file splits.
4. Parquet readers read column chunks and pages.
5. Spark processes batches of rows/columns.

Mental model:

```text
Spark reads Parquet in parallel chunks across executors.
It does not read the entire dataset into one Python process.
```

Parquet also helps with:

- column pruning: read fewer columns,
- predicate pushdown: skip some row groups using statistics,
- partition pruning: skip folders such as `order_month=2025-09`.

Example:

```python
df = spark.read.parquet("data/output/orders")

small = df.select("order_id", "amount").where("order_month = '2025-09'")
small.count()
```

Spark can avoid reading many unneeded columns and partition folders.

## 6. What If The Data To Read Is Bigger Than Executor Memory?

It depends on what operation you are doing.

For a simple scan:

```python
df = spark.read.parquet("data/output/orders")
df.where("amount > 100").count()
```

Spark can usually process data in chunks. The full dataset can be much larger than cluster memory.

For joins, group by, sort, window functions, and deduplication, Spark may need to shuffle and hold intermediate data. If a task gets too much data:

- Spark may spill data to disk.
- The job may become slow.
- A task may fail with out-of-memory.
- One executor may run much longer than the others.

Important idea:

```text
Total executor memory is not one shared pool.
Each executor and each task has its own memory limits.
One oversized task can fail even when the whole cluster has plenty of memory.
```

Common fixes:

- Select fewer columns early.
- Filter early.
- Repartition before large joins or aggregations.
- Avoid skewed keys where one key has huge data.
- Avoid using `collect()` on large DataFrames.
- Increase executor memory for real clusters.
- Rewrite very large or tiny-file datasets into healthier file sizes.

## 7. If My Spark Job Is Too Slow, Where Should I Look?

Start with the Spark UI.

Check:

1. Jobs
2. Stages inside each job
3. Tasks inside slow stages
4. Executors
5. SQL tab for the physical plan

Beginner checklist:

| Symptom | What to check |
|---|---|
| One task takes much longer than others | Data skew, one huge partition, bad partitioning |
| Large shuffle read/write | Join, group by, distinct, window, repartition |
| Memory spill or disk spill | Not enough memory for shuffle/sort/aggregation |
| Many tiny tasks | Too many small files or too many partitions |
| Very few tasks | Not enough parallelism or huge files |
| Slow file listing | Too many tiny files or deeply nested partitions |
| Executor lost or OOM | Task memory too large, skew, bad join strategy |

In the Spark UI, look for:

- stage duration,
- task duration distribution,
- shuffle read and shuffle write,
- spill memory and spill disk,
- input size per task,
- failed tasks,
- executor memory usage.

Practical beginner rule:

```text
If one executor or one task is much slower than the others, suspect skew or an oversized partition.
If every task is slow, suspect too much data, too many columns, expensive transformations, or poor file layout.
```

## 8. Updating September 2025 Parquet Data

Scenario:

```text
You already wrote this partition:

s3://company-trusted/orders/order_month=2025-09/

Now a few corrected rows arrive for September 2025.
```

Plain Parquet does not update a few rows inside an existing file.

You normally use one of these patterns:

1. Read the September 2025 partition.
2. Read the correction rows.
3. Merge old rows and correction rows in Spark.
4. Write a replacement September 2025 partition.

### Example: Replace One Affected Partition

Assume the business key is `order_id`.

```python
from pyspark.sql import functions as F
from pyspark.sql import Window

base_path = "s3a://de-lab/trusted/orders"
corrections_path = "s3a://de-lab/raw/order_corrections/sep2025.csv"
replacement_path = "s3a://de-lab/tmp/orders/order_month=2025-09"

sept_2025 = spark.read.parquet(f"{base_path}/order_month=2025-09")

corrections = (
    spark.read
    .option("header", True)
    .csv(corrections_path)
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("order_month", F.lit("2025-09"))
    .withColumn("update_ts", F.current_timestamp())
)

old_rows_not_corrected = sept_2025.join(
    corrections.select("order_id").dropDuplicates(),
    "order_id",
    "left_anti",
)

replacement = old_rows_not_corrected.unionByName(corrections, allowMissingColumns=True)

replacement.write.mode("overwrite").parquet(replacement_path)
```

Then validate:

```python
spark.read.parquet(replacement_path).count()
spark.read.parquet(replacement_path).where("order_id is null").count()
```

After validation, promote the replacement partition.

For a local or LocalStack learning lab, that often means deleting the old partition folder and copying/writing the replacement into its place.

For real AWS, use an orchestrated process with backups and validation:

```bash
# LocalStack learning version:
awslocal s3 rm s3://de-lab/trusted/orders/order_month=2025-09/ --recursive
awslocal s3 cp s3://de-lab/tmp/orders/order_month=2025-09/ \
  s3://de-lab/trusted/orders/order_month=2025-09/ \
  --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 rm s3://your-real-bucket/trusted/orders/order_month=2025-09/ --recursive
# aws s3 cp s3://your-real-bucket/tmp/orders/order_month=2025-09/ \
#   s3://your-real-bucket/trusted/orders/order_month=2025-09/ \
#   --recursive
```

Important warning: deleting and copying S3 prefixes is not an atomic database transaction. A reader may see partial data if it queries during the replacement. Production systems avoid that risk with orchestration, versioned paths, or a lakehouse table format.

## 9. Dynamic Partition Overwrite

Spark can overwrite only the partitions present in the incoming DataFrame when configured carefully.

Conceptual example:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

replacement.write.mode("overwrite").partitionBy("order_month").parquet(
    "s3a://de-lab/trusted/orders"
)
```

If `replacement` contains only `order_month = '2025-09'`, Spark overwrites only that partition instead of the whole table path.

Use this carefully:

- Validate the incoming DataFrame contains only the intended partitions.
- Keep a backup or versioned output path.
- Understand how your storage and catalog handle concurrent readers.

## 10. Delta Lake, Iceberg, And Hudi Conceptually

Plain Parquet files are just files. They do not know how to do safe row-level `UPDATE`, `DELETE`, or `MERGE`.

Lakehouse table formats add a transaction/metadata layer on top of files in object storage.

Common formats:

- Delta Lake
- Apache Iceberg
- Apache Hudi

Conceptually, they work like this:

```text
S3 stores data files.
The table format stores metadata about which files belong to the current table version.
Updates write new files.
The table metadata is committed to point to the new version.
Readers see a consistent snapshot.
Old files can be cleaned up later.
```

So an update is still not editing one Parquet file in place. It is more like:

```text
1. Read current table snapshot.
2. Write new Parquet data files with changed rows.
3. Commit metadata that says these new files are now part of the table.
4. Mark old files as no longer active for the latest snapshot.
```

In AWS-style projects, the storage is usually S3. The catalog is often AWS Glue Data Catalog. The processing engine may be Spark on Glue, EMR, Databricks, or another engine that supports the chosen table format.

For beginners, remember:

| Plain Parquet | Lakehouse table format |
|---|---|
| Files only | Files plus table metadata/log |
| No row-level update by itself | Supports update/delete/merge through table format |
| Manual partition replacement | Safer commits and snapshots |
| Readers may see partial replacement if badly orchestrated | Readers can see a consistent table version |

Interview explanation:

```text
Parquet is the file format. Delta, Iceberg, and Hudi are table formats that manage many Parquet files as one table with metadata, snapshots, and merge/update/delete behavior.
```

