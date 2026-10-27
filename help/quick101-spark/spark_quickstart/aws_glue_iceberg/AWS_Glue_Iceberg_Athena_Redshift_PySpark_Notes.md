# AWS Glue Data Catalog, Apache Iceberg, Athena, Redshift & PySpark

**Practical guide — concepts, worked examples, catalog internals, and snapshot management**

> **Core idea:** AWS Glue Data Catalog is a metadata service, not a row-level data store. Athena, Spark and Redshift execute reads and writes. For a Glue-backed **Iceberg** table, a successful write transaction commits a new Iceberg metadata version and automatically advances Glue's pointer (`metadata_location`) to that version.

**New here?** Read sections **1 → 3 → 8 → 9 → 10** for the basics and examples, then **4–7 and 11–15** for deeper reference. The [folder README](README.md) provides direct links.

**Example setup:** Use Athena engine v3, Iceberg **format v2**, Parquet data files, and conventional S3 buckets. Create the Glue database `analytics`, configure Athena's query-result location, and provide the required S3/Glue/KMS permissions plus Lake Formation grants where applicable. Replace sample bucket names, IAM roles, account IDs, and Region. Use an unused prefix for each new table.

The examples use distinct names: `sales_parquet` for traditional external files, `sales_iceberg` for Iceberg, and `public.sales_demo` for a Redshift internal table. INSERT/append examples add rows again if repeated.

| Term | Meaning |
|---|---|
| Metadata | Information describing data, such as schema, locations, and file references |
| Schema | Column names and data types |
| Partition | A group of data, often by date, used to reduce scanning |
| Snapshot | A committed Iceberg table state that can reuse unchanged files |
| CRUD / DML | Create/read/update/delete operations; DML refers to statements that change rows |
| Commit | The step that publishes a transaction's changes |

## 1. Three layers of an AWS lakehouse

![Query engines, metadata and physical storage](images/01_metadata_layers.png)

| Layer | Examples | Responsibility |
|---|---|---|
| Processing | Athena, AWS Glue PySpark, EMR Spark, Redshift | Executes SQL and transformation logic |
| Metadata | Glue Data Catalog, Iceberg metadata, Redshift system catalog | Identifies tables, schemas, files and/or current committed states |
| Storage | S3 and Redshift managed storage | Holds actual records and, for Iceberg, metadata files |

**Parquet vs Iceberg:** Parquet is a file format. Iceberg is a transactional table format that often stores its records in Parquet. Plain Parquet fits simple append-only data; Iceberg fits corrections, upserts, and historical reporting.

**Glue vs Iceberg:** Glue registers an Iceberg table and stores a reference to its current table metadata. Iceberg metadata, normally in S3, describes the actual current snapshot and its data/delete files.

## 2. CRUD support at a glance

| Engine / table type | INSERT | UPDATE | DELETE | How metadata is handled |
|---|---|---|---|---|
| Athena + regular external Parquet | Yes | No | No | Glue partitions may need registration |
| Athena + Iceberg | Yes | Yes | Yes | Iceberg commit advances Glue metadata pointer automatically |
| Glue PySpark + Parquet | Writes files | Rewrite affected data | Rewrite affected data | Manual/configured partition synchronization |
| Glue PySpark + Iceberg | Yes | See runtime note | Check runtime support | Supported Iceberg writers commit through catalog |
| Redshift internal table | Yes | Yes | Yes | Redshift manages its own metadata/transactions |
| Redshift Spectrum + traditional external table | Supported `INSERT` | No | No | Supported INSERT can register new partitions |
| Redshift writable Iceberg table | Yes | Yes | Yes | Compatible Iceberg transactions and catalog commits |

**Runtime note:** AWS's [Glue Iceberg guide](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) explicitly lists standalone UPDATE as unsupported. Do not infer managed Glue support from upstream Spark syntax. Section 9 illustrates MERGE; confirm support on your chosen runtime. Iceberg library versions and table format versions are different. Redshift's writable Iceberg capability is distinct from legacy read-oriented Spectrum tables.

## 3. Traditional Parquet with Glue Data Catalog

### 3.1 What Glue stores without Iceberg

**For a conventional Glue-backed Parquet/CSV/ORC external table, Glue does not maintain an Iceberg `metadata_location` or a `current-snapshot-id`.** Instead, the Glue table definition has a **`StorageDescriptor.Location`**, normally pointing directly to the S3 root *prefix* for the table. It also holds column definitions, input/output formats, SerDe information (how the engine decodes records), and `PartitionKeys`. For ordinary registered partitions, Glue maintains separate partition entries with values and often their own `StorageDescriptor.Location`.

![Traditional Glue external table versus Glue-backed Iceberg metadata pointers](images/07_glue_vs_iceberg_pointer.png)

**Illustrative regular Glue table (`aws glue get-table` — abbreviated):**

```json
{
  "Table": {
    "Name": "sales_parquet",
    "DatabaseName": "analytics",
    "TableType": "EXTERNAL_TABLE",
    "StorageDescriptor": {
      "Location": "s3://company-lake/sales-parquet/",
      "Columns": [
        {
          "Name": "order_id",
          "Type": "bigint"
        },
        {
          "Name": "customer_id",
          "Type": "bigint"
        },
        {
          "Name": "amount",
          "Type": "decimal(12,2)"
        }
      ],
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    },
    "PartitionKeys": [
      {
        "Name": "year",
        "Type": "string"
      },
      {
        "Name": "month",
        "Type": "string"
      }
    ]
  }
}
```

**Illustrative registered partition (`aws glue get-partitions` — abbreviated):**

```json
{
  "Partitions": [
    {
      "Values": [
        "2026",
        "10"
      ],
      "StorageDescriptor": {
        "Location": "s3://company-lake/sales-parquet/year=2026/month=10/"
      }
    }
  ]
}
```

**Athena read path without Iceberg:**

```text
Athena SELECT
  -> Glue GetTable (schema / table S3 root / partition keys)
  -> Glue GetPartitions (for registered partitions, if applicable)
  -> S3 LIST + READ the Parquet files under selected locations
  -> Return matching rows
```

Glue generally **does not track each individual Parquet filename**. Athena lists the S3 objects under the chosen data locations. If a writer replaces files within an **already registered** partition and leaves schema/partition locations unchanged, Glue normally requires **no metadata update**. Athena may observe the changed files on a subsequent query, but this is **not a transactional snapshot**: concurrent readers/writers can encounter inconsistent intermediate states if replacement is not safely coordinated.

For a **new partition**, register it with Glue (`ALTER TABLE ADD PARTITION`, crawler/API, appropriately configured writer), **or** use Athena partition projection where applicable. With partition projection, Athena computes partition paths from Glue table properties and does **not** need `GetPartitions` calls; note that other engines may not honor Athena's projection configuration.

| Question | Glue + ordinary Parquet | Glue + Iceberg |
|---|---|---|
| What is the primary S3 reference? | `StorageDescriptor.Location` (table/partition data prefixes) | `Parameters.metadata_location` (current Iceberg metadata JSON) |
| Is there a snapshot ID? | No | Yes, in Iceberg metadata JSON |
| Are individual data files tracked in table metadata? | No; S3 objects discovered at read | Yes, through manifest hierarchy |
| Does an append to an existing partition update Glue? | Usually not | Commit advances current Iceberg metadata pointer |
| What happens with a new partition? | Register partition or use projection | Iceberg tracks partition/file information in its metadata |
| What happens when rows are updated/deleted? | External rewrite and controlled publish; catalog may be unchanged | Iceberg commits new transactional state and pointer |

**CLI inspection:**

```bash
aws glue get-table --database-name analytics --name sales_parquet \
  --query 'Table.{Location:StorageDescriptor.Location,PartitionKeys:PartitionKeys,Parameters:Parameters}'

aws glue get-partitions --database-name analytics --table-name sales_parquet \
  --query 'Partitions[].{Values:Values,Location:StorageDescriptor.Location}'
```

The S3 **location is not a version pointer**: it remains the same while the underlying files may change. Registering a regular Glue table does not add Iceberg-style atomic visibility, snapshots, time travel, or transactional row-level changes.

Example files:

```text
s3://company-lake/sales-parquet/
  year=2026/
    month=10/
      part-001.parquet
      part-002.parquet
    month=11/
      part-003.parquet
```

Glue table definition (simplified):

```text
Database: analytics
Table: sales_parquet
Format: PARQUET
Columns: order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2)
Partition keys: year STRING, month STRING
Location: s3://company-lake/sales-parquet/
```

### 3.2 Create and read the example table

![Traditional Parquet SELECT using Athena and Glue](images/02_parquet_read.png)

Run this DDL (table-definition SQL) in Athena to register the table. Files shown above are illustrative; section 3.3 writes sample data. The October query returns no rows until October files and its partition are available.

```sql
CREATE EXTERNAL TABLE analytics.sales_parquet (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2)
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://company-lake/sales-parquet/';

SELECT * FROM analytics.sales_parquet
WHERE year = '2026' AND month = '10';
```

Athena retrieves table/partition metadata from Glue, locates the applicable S3 prefixes and scans matching Parquet data. **Glue does not itself execute the SELECT or return the records.**

### 3.3 Append with PySpark

In a PySpark session with `spark` available, create a small December batch:

```python
from decimal import Decimal

df = spark.createDataFrame([
    (101, 1001, Decimal("500.00"), "2026", "12"),
    (102, 1002, Decimal("700.00"), "2026", "12"),
    (103, 1003, Decimal("900.00"), "2026", "12"),
], "order_id long, customer_id long, amount decimal(12,2), year string, month string")

df.write.mode("append") \
  .partitionBy("year", "month") \
  .parquet("s3://company-lake/sales-parquet/")
```

Spark creates files in S3. If files land in an **existing registered partition**, a Glue partition update normally is not needed. If a **new partition** appears, register it through an appropriate approach:

```sql
ALTER TABLE analytics.sales_parquet
ADD IF NOT EXISTS PARTITION (year='2026', month='12')
LOCATION 's3://company-lake/sales-parquet/year=2026/month=12/';

SELECT order_id, amount FROM analytics.sales_parquet
WHERE year = '2026' AND month = '12'
ORDER BY order_id;
```

Expected December amounts after one append: `101 → 500.00`, `102 → 700.00`, `103 → 900.00`.

Alternatives: Glue crawlers, Glue APIs, `MSCK REPAIR TABLE` for compatible Hive layouts, configured Glue Catalog writers or partition projection in supported setups. **Simply writing files to S3 does not automatically update Glue.**

### 3.4 Correct or delete rows with plain Parquet

Plain Parquet is a **file format**, not an ACID table format. ACID describes guarantees for atomicity, consistency, isolation, and durability of transactions. Athena does not offer regular row-level `UPDATE` / `DELETE` on traditional external Parquet tables. A Spark job can transform rows but must safely publish replacement files:

```python
from pyspark.sql.functions import col, lit, when

# Use df from section 3.3. These are independent transformations.
updated_df = df.withColumn(
    "amount",
    when(col("order_id") == 102, lit("750.00").cast("decimal(12,2)"))
    .otherwise(col("amount"))
)

remaining_df = df.filter(col("order_id") != 103)
```

**These transformations do not publish changes by themselves.** Use a controlled stage-and-replace approach; avoid blindly overwriting a path being used as the input. **If the existing schema, table location and registered partitions stay the same, Glue Catalog typically does not change.** Glue is not a row-level index, and standard external-table metadata does not enumerate every Parquet file.

## 4. Iceberg metadata hierarchy — where snapshots live

![Iceberg metadata hierarchy](images/03_iceberg_hierarchy.png)

| Object | Where it resides | What it contains |
|---|---|---|
| Glue table entry | AWS-managed regional Glue Catalog | Table registration and `metadata_location` |
| Iceberg table metadata JSON | Usually table's S3 `metadata/` directory | Schema, partition spec, snapshots, `current-snapshot-id` |
| Manifest list | S3 | Manifests belonging to a snapshot |
| Manifest files (Avro) | S3 | References to individual data/delete files; stats |
| Data and delete files | S3 | Records and supported row-deletion information |

Illustrative S3 layout:

```text
s3://company-lake/iceberg/sales/
  data/
    file-001.parquet
    file-002.parquet
    delete-001.parquet
  metadata/
    v1.metadata.json
    v2.metadata.json
    v3.metadata.json
    snap-101.avro
    manifest-a.avro
    manifest-b.avro
```

A manifest list lists manifests; each manifest lists file references and statistics. Snapshots can reuse unchanged files instead of copying all rows. Real filenames and layout vary by writer/version. Iceberg tracks **exact referenced files**, unlike a traditional partition directory listing.

## 5. How an Iceberg write updates the Glue pointer

For an Iceberg table registered in Glue, the writer advances the catalog reference when it commits new table metadata. Athena initiates and executes the write; the Iceberg writer constructs the new table state and commits it using the configured Glue-backed catalog; Glue persists the reference to the new metadata JSON.

![Before and after Athena Iceberg UPDATE: Glue pointer and snapshot](images/05_pointer_before_after.png)

### Before the UPDATE

Assume the Iceberg table from section 8 has been created and loaded, before its UPDATE/DELETE statements run. This section explains one commit; its file names and snapshot IDs are illustrative.

Glue table properties (simplified):

```json
{
  "table_type": "ICEBERG",
  "metadata_location": "s3://company-lake/iceberg/sales/metadata/v1.metadata.json"
}
```

Inside `v1.metadata.json` (simplified):

```json
{
  "current-snapshot-id": 101
}
```

Snapshot 101 produces these logical rows:

| order_id | amount |
|---|---:|
| 101 | 500 |
| 102 | 700 |
| 103 | 900 |

### Athena UPDATE

```sql
UPDATE analytics.sales_iceberg
SET amount = 750
WHERE order_id = 102;
```

On a successfully changed Athena Iceberg transaction, Athena writes appropriate change files (positional deletes and replacement data for row updates), Iceberg prepares the corresponding manifests, manifest list and metadata JSON, and the Glue-backed catalog commits the new metadata reference.

### After the successful commit

Glue table properties now point to:

```json
{
  "table_type": "ICEBERG",
  "metadata_location": "s3://company-lake/iceberg/sales/metadata/v2.metadata.json"
}
```

The **new JSON**, not Glue directly, identifies the new snapshot:

```json
{
  "current-snapshot-id": 102
}
```

The logical result is `order_id=102, amount=750`.

**Precisely what changes?**

| Layer | Before | After |
|---|---|---|
| Glue `metadata_location` | `v1.metadata.json` | `v2.metadata.json` |
| Iceberg JSON's `current-snapshot-id` | `101` | `102` |
| Logical table records | Amount 700 | Amount 750 |

**Glue points to an Iceberg metadata file; it does not directly point to snapshot 102.** This distinction explains why a new Athena or Spark reader can locate the latest committed state.

### Who does what?

| Step | Primary component |
|---|---|
| Parse/plan `UPDATE` and select affected rows | Athena |
| Write data/delete files | Athena Iceberg writer |
| Build manifests and table metadata | Iceberg writer |
| Coordinate atomic commit and check for conflicts | Iceberg catalog integration |
| Persist new `metadata_location` | AWS Glue Catalog |
| Resolve latest committed state in subsequent reads | Athena, Spark or compatible engine |

### Does a SELECT advance the Glue pointer?

**No.** `SELECT` reads existing Glue/Iceberg metadata and then data files. It creates neither a new Iceberg snapshot nor a catalog pointer commit. A no-op DML statement also need not result in a new committed state. Athena may write query results to S3; that does not change the source table.

**Metadata version is not the same as snapshot.** A schema or table-property commit can advance the metadata pointer while keeping the same current data snapshot. Snapshot IDs are not sequential counters.

## 6. Iceberg commit flow and concurrency

![Iceberg commit sequence](images/04_iceberg_commit.png)

1. Resolve the current Glue catalog `metadata_location`.
2. Read Iceberg metadata and plan changes.
3. Write new data/delete files and produce manifests and a new metadata JSON.
4. Attempt an atomic catalog commit advancing the `metadata_location`.
5. On success, new readers see the new snapshot. On commit conflict, the writer may retry or fail depending on implementation/operation.

Already running queries continue using their planned snapshot. A failed write can leave unreferenced files for later cleanup.

The **file writes alone do not complete the transaction**. The catalog commit publishes the new table state. This is why you should **not** use plain `df.write.parquet(...)` directly in an existing Iceberg table's data directory: those files are not automatically referenced by its committed snapshot.

## 7. Iceberg UPDATE strategies: COW and MOR

| Strategy | Internal action | Tradeoff |
|---|---|---|
| Copy-on-write (COW) | Rewrite affected data files and commit references to replacements | More write I/O; potentially simpler reads |
| Merge-on-read (MOR) | Add delete information and replacement data; readers combine both | Usually less immediate rewrite; accumulated delete files may slow reads |

Athena Iceberg `UPDATE`, `DELETE` and `MERGE` use **merge-on-read with positional deletes** for row-level operations, regardless of a requested copy-on-write setting. Some optimizations can remove entire files through metadata without writing per-row deletes. Other Iceberg engines may support different strategies.

## 8. Athena Iceberg CRUD and introspection

Run the following in Athena. The database `analytics` must exist and the S3 prefix must be unused. Do not add the `EXTERNAL` keyword for Athena Iceberg tables:

```sql
CREATE TABLE analytics.sales_iceberg (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2),
  order_date DATE
)
PARTITIONED BY (month(order_date))
LOCATION 's3://company-lake/iceberg/sales/'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');
```

```sql
INSERT INTO analytics.sales_iceberg VALUES
  (101, 1001, 500.00, DATE '2026-10-26'),
  (102, 1002, 700.00, DATE '2026-10-26'),
  (103, 1003, 900.00, DATE '2026-10-27');

UPDATE analytics.sales_iceberg
SET amount = 750.00 WHERE order_id = 102;

DELETE FROM analytics.sales_iceberg WHERE order_id = 103;

SELECT order_id, amount
FROM analytics.sales_iceberg ORDER BY order_id;
```

Expected final rows:

| order_id | amount |
|---|---:|
| 101 | 500.00 |
| 102 | 750.00 |

This models a corrected order and a removed order. `DECIMAL` represents money exactly. Iceberg derives the monthly partition from `order_date`; a separate month column is unnecessary.

Each successfully committed data change creates a new Iceberg table state and advances Glue's metadata pointer. **You do not manually add Iceberg partitions to Glue.**

Inspect Iceberg system tables in Athena:

```sql
SELECT * FROM "analytics"."sales_iceberg$snapshots";
SELECT * FROM "analytics"."sales_iceberg$history";
SELECT * FROM "analytics"."sales_iceberg$files";
SELECT * FROM "analytics"."sales_iceberg$manifests";
SELECT * FROM "analytics"."sales_iceberg$partitions";
```

Lake Formation row/cell filters or limited column permissions can restrict metadata-table access.

## 9. Glue PySpark + Iceberg

### 9.1 Configure the job

Use Glue 5.0 with bundled Iceberg 1.7.1 as a concrete baseline. Enable `--datalake-formats iceberg`. Configure the settings below **before Spark starts**. In the Glue job's `--conf` value, enter the first `key=value`, followed by `--conf key=value` for each additional setting:

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.warehouse=s3://company-lake/iceberg/
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

`glue_catalog` is the Spark catalog alias defined here. The warehouse supplies a default for new tables; existing tables retain their registered locations. Lake Formation integration needs runtime-specific setup; see the [Glue guide](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html).

### 9.2 Append a complete order

Continue after section 8 in the configured Glue job, or reuse an existing configured notebook session:

```python
from datetime import date
from decimal import Decimal
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Reuse `spark` instead if the notebook already provides it.
glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session

schema = "order_id long, customer_id long, amount decimal(12,2), order_date date"
new_orders = spark.createDataFrame(
    [(104, 1004, Decimal("125.00"), date(2026, 10, 28))], schema
)
new_orders.writeTo("glue_catalog.analytics.sales_iceberg").append()
```

### 9.3 Upsert a batch with MERGE

An **upsert** updates matching rows and inserts new ones. This full-record batch corrects order 102 and introduces order 105:

```python
updates_df = spark.createDataFrame([
    (102, 1002, Decimal("775.00"), date(2026, 10, 26)),
    (105, 1005, Decimal("300.00"), date(2026, 10, 29)),
], schema)
updates_df.createOrReplaceTempView("sales_updates")

spark.sql("""
MERGE INTO glue_catalog.analytics.sales_iceberg AS t
USING sales_updates AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  t.customer_id = s.customer_id,
  t.amount = s.amount,
  t.order_date = s.order_date
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount, order_date)
VALUES (s.order_id, s.customer_id, s.amount, s.order_date)
""")

spark.table("glue_catalog.analytics.sales_iceberg").orderBy("order_id").show()
```

Expected amounts: `101 → 500.00`, `102 → 775.00`, `104 → 125.00`, `105 → 300.00`.

Use one source record per updating key. Iceberg does not enforce primary-key uniqueness. Real CDC (change data capture) feeds need deterministic event ordering, deduplication, and rules for partial updates/deletes. This example assumes complete records and ordered batches; replaying an append duplicates rows.

Supported writes using `GlueCatalog` commit the new metadata reference automatically. No Hive partition registration is needed. Confirm MERGE support on your selected runtime before production use.

## 10. Redshift: distinguish three different table types

### 10.1 Redshift internal tables

Run this independent example in Redshift:

```sql
CREATE TABLE public.sales_demo (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2)
);

INSERT INTO public.sales_demo VALUES
  (102, 1002, 700.00), (103, 1003, 900.00);
UPDATE public.sales_demo SET amount = 750.00 WHERE order_id = 102;
DELETE FROM public.sales_demo WHERE order_id = 103;
```

Data and transaction metadata are managed by Redshift. Glue Catalog is **not necessary** for ordinary internal-table CRUD.

### 10.2 Redshift Spectrum traditional external tables

```sql
CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG
DATABASE 'analytics'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole';

SELECT * FROM spectrum.sales_parquet;
```

Assume section 3's Glue table exists. Supported INSERT into an existing external table can write S3 data and register newly created partitions automatically:

```sql
INSERT INTO spectrum.sales_parquet
SELECT order_id, customer_id, amount, '2026', '10'
FROM public.sales_demo;
```

The SELECT supplies three data columns followed by the two partition columns, matching section 3. The execution role needs S3/catalog write access. External INSERT has format/encryption restrictions and cannot run inside a transaction block. Traditional Spectrum external tables do **not** support direct UPDATE or DELETE.

### 10.3 Redshift writable Iceberg tables

With supported Redshift Iceberg writes and access to the auto-mounted `awsdatacatalog` root catalog, compatible Glue-registered tables support DML such as:

```sql
UPDATE awsdatacatalog.analytics.sales_iceberg
SET amount = 800.00 WHERE order_id = 102;

DELETE FROM awsdatacatalog.analytics.sales_iceberg
WHERE order_id = 105;
```

`awsdatacatalog.analytics.sales_iceberg` means `catalog.database.table`; it refers to the same Glue table used by Spark through the alias `glue_catalog`.

After section 9, these statements change order 102 to 800.00 and remove order 105. Such writes update Iceberg files/metadata and commit the new state through the supported Glue-backed catalog. For externally created tables, check format version, Parquet default format, metadata-compression, and Write-Audit-Publish requirements in the [Redshift SQL documentation](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-sql-syntax.html). **Do not generalize this to legacy Spectrum external Parquet tables.**

Redshift does not automatically retry Iceberg commit conflicts; resolve conflicts and rerun failed statements. Do not assume multi-statement Iceberg transactions behave like internal-table transactions; see [transaction semantics](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-transaction-semantics.html).

## 11. Where to inspect Glue vs Iceberg metadata

### AWS Glue API / CLI

Use credentials for the table's account and Region. To get only the pointer, add `--query 'Table.Parameters.metadata_location' --output text`:

```bash
aws glue get-table \
  --database-name analytics \
  --name sales_iceberg
```

Illustrative subset of a Glue table response (actual fields vary):

```json
{
  "Table": {
    "DatabaseName": "analytics",
    "Name": "sales_iceberg",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {
      "table_type": "ICEBERG",
      "metadata_location": "s3://company-lake/iceberg/sales/metadata/v3.metadata.json"
    }
  }
}
```

### S3 Iceberg metadata file

Illustrative subset only (not a complete valid Iceberg JSON):

```json
{
  "format-version": 2,
  "location": "s3://company-lake/iceberg/sales/",
  "current-snapshot-id": 2003,
  "snapshots": [
    {
      "snapshot-id": 2002,
      "manifest-list": "s3://company-lake/iceberg/sales/metadata/snap-2002.avro"
    },
    {
      "snapshot-id": 2003,
      "manifest-list": "s3://company-lake/iceberg/sales/metadata/snap-2003.avro"
    }
  ]
}
```

Glue Catalog is an **AWS-managed metadata service**, not a directory you can browse in S3. Iceberg metadata files, on the other hand, normally live in the table's S3 location.

### Historical query in Athena

Choose an actual retained snapshot ID from `"analytics"."sales_iceberg$snapshots"`. Substitute it for the illustrative `101`:

```sql
SELECT order_id, amount
FROM analytics.sales_iceberg FOR VERSION AS OF 101;
```

This reads an older state without changing the current table. A snapshot taken just after the initial INSERT in section 8 still contains order 103 and amount 700.00 for order 102. Historical queries require retained snapshots and files.

## 12. Do you need Glue for Spark + Iceberg?

**No.** Iceberg requires a suitable catalog implementation, but this need not be AWS Glue. Options include Glue, Hive Metastore, Hadoop, JDBC and REST catalogs, depending on your environment. Spark can use Iceberg on EMR, EC2, Kubernetes and elsewhere without Glue ETL jobs.

Important distinctions:

- **Glue Data Catalog != Glue ETL.** You can use Glue as a catalog without running Glue Spark jobs.
- **Iceberg != Glue.** Iceberg can use another catalog.
- **Athena Iceberg** is designed around its supported Glue-backed catalog integration.
- For a shared AWS lakehouse across Athena, Spark and Redshift, **Glue Catalog is often the simplest common catalog choice**.

## 13. Athena and PySpark accessing the same table

```text
Initial:    Glue → v1.metadata.json → snapshot 101
Spark INSERT and commit
Next:       Glue → v2.metadata.json → snapshot 102
Athena SELECT sees committed snapshot 102
Athena UPDATE and commit
Next:       Glue → v3.metadata.json → snapshot 103
Spark SELECT (refresh/expiration as needed) sees snapshot 103
```

A long-running Spark session may cache table metadata; refresh/expiration may be necessary to observe the newest snapshot. Concurrent writers rely on Iceberg's catalog-based commit control; conflicting commits may need retries.

For a fresh Spark read when needed:

```python
spark.sql("REFRESH TABLE glue_catalog.analytics.sales_iceberg")
spark.table("glue_catalog.analytics.sales_iceberg").show()
```

An existing cached result does not automatically become live data.

## 14. Recommended AWS BI lakehouse architecture

![AWS lakehouse and BI architecture](images/06_aws_lakehouse.png)

Use Spark/Glue/EMR for batch transformations and CDC/upserts, Iceberg on S3 for transactional lakehouse tables, Glue Data Catalog as shared table registration, and Athena or Redshift for analytics and BI. Athena and Redshift read the catalog metadata directly; Glue does not sit in the data path as a query proxy.

Operational considerations: file compaction, accumulating delete files, snapshot expiration, schema changes, concurrent writes, query-cost management, and precomputed BI aggregates.

### 14.1 Maintenance and troubleshooting

| Symptom or requirement | First action |
|---|---|
| Too many small files or accumulated deletes | Compact data files using an Iceberg-aware operation |
| Historical files consume too much storage | Define retention, then expire snapshots and clean eligible files |
| Files exist but Iceberg rows are missing | Check whether an Iceberg commit referenced those files |
| New Parquet month is missing | Check Glue partition registration or projection configuration |
| Glue metadata is accessible but data is not | Check S3/KMS and applicable Lake Formation access |
| MERGE reports multiple source matches | Resolve source events to one updating record per target key |

Snapshot expiration removes historical access to expired states. Cleanup must preserve files needed by retained snapshots and active jobs. Avoid S3 lifecycle rules that delete files still referenced by Iceberg. See [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/).

## 15. Final behavior matrix

“S3 change” below means source table files; query-result files and logs are excluded.

| Operation | Change in S3? | Glue change? | Iceberg snapshot? |
|---|---|---|---|
| Append Parquet to existing registered partition | Yes | Usually no | N/A |
| Add new Parquet partition | Yes | Register if needed | N/A |
| Rewrite existing Parquet files | Yes | Usually no if table/partitions unchanged | N/A |
| Athena Iceberg SELECT | No | No | No |
| Athena Iceberg INSERT / UPDATE / DELETE | Yes or metadata-only optimization | **Advance Iceberg metadata pointer** on changed commit | Yes |
| Spark Iceberg append / supported MERGE | Yes | **Advance pointer** on changed commit | Yes |
| Iceberg schema/property commit | Metadata JSON | **Advance pointer** | Not necessarily |
| Iceberg data-file compaction | Yes | **Advance pointer** | Yes, even if row values are unchanged |
| Redshift internal UPDATE | Redshift-managed storage | Not applicable | N/A |
| Redshift Spectrum traditional Parquet INSERT | Yes | New partitions automatically registered when needed/supported | N/A |
| Redshift writable Iceberg DML | Yes or appropriate metadata changes | **Advance pointer** on changed commit | Yes |

### Five key takeaways

1. Glue Data Catalog holds table metadata; query engines perform CRUD.
2. Ordinary Parquet is a file format, while Iceberg is a transactional table format that frequently uses Parquet as its data files.
3. **Without Iceberg**, Glue points to table/partition S3 data prefixes via `StorageDescriptor.Location` and has no snapshot pointer. **With Iceberg**, Glue's `metadata_location` points to the current Iceberg metadata JSON, which identifies the current snapshot.
4. Athena or Spark **automatically advances that pointer on a successful Iceberg commit**; a SELECT does not.
5. Redshift's behavior depends on whether you use internal tables, traditional Spectrum external tables or writable Iceberg tables.

## 16. Official references

- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Glue Table API (StorageDescriptor, PartitionKeys)](https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html)
- [Athena partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
- [AWS Glue: using Apache Iceberg](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
- [Athena: updating Iceberg table data](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html)
- [Athena: querying Iceberg tables](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [Redshift: Iceberg writes](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes.html)
- [Redshift: external-table INSERT](https://docs.aws.amazon.com/redshift/latest/dg/r_INSERT_external_table.html)
- [Redshift Spectrum considerations](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-considerations.html)

*Diagrams show conceptual relationships. Paths, filenames, IDs, and metadata fragments are illustrative. The examples have been reviewed as documentation; they have not been run against an AWS account.*
