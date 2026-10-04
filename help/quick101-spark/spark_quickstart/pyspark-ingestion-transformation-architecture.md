# PySpark Ingestion And Transformation Architecture

This document describes a simple data engineering architecture using PySpark and S3-style data lake buckets.

The goal is to ingest raw files, perform basic data quality checks, save clean baseline data to a trusted bucket, and optionally transform the trusted data into a transformed bucket.

Runnable local implementation:

```text
pipeline_demo/
```

Start with:

```text
pipeline_demo/README.md
```

## 1. Architecture Diagram

```mermaid
flowchart LR
    source["Source Systems<br/>CSV, JSON, Parquet, APIs, DB exports"] --> raw["Raw S3 Bucket<br/>s3://company-raw/source/table/"]

    raw --> spark_dq["PySpark Ingestion Job<br/>Read raw files"]
    spark_dq --> dq["Data Quality Checks<br/>Required columns<br/>Non-null keys<br/>Valid date<br/>Valid amount<br/>Duplicate check"]

    dq -->|DQ passed| select_cols["Select Interested Columns<br/>Cast data types<br/>Add metadata columns"]
    dq -->|DQ failed| rejected["Rejected S3 Bucket<br/>s3://company-rejected/source/table/run_id/<br/>Bad records + error reason"]

    select_cols --> trusted["Trusted S3 Bucket<br/>s3://company-trusted/source/table/<br/>Baseline clean dataset"]

    trusted --> config_check{"Transformation enabled?"}
    config_check -->|No| end_baseline["Stop<br/>Trusted data is the baseline output"]
    config_check -->|Yes| transform["PySpark Transformation Job<br/>Business rules<br/>Derived columns<br/>Aggregations<br/>Joins"]

    transform --> transformed["Transformed S3 Bucket<br/>s3://company-transformed/domain/table/<br/>Consumption-ready dataset"]

    trusted -. metadata .-> catalog["Glue Data Catalog<br/>External table metadata"]
    transformed -. metadata .-> catalog
```

## 2. Event-Driven AWS Orchestration Diagram

This diagram shows how the pipeline can run automatically when a file lands in the raw S3 bucket.

```mermaid
sequenceDiagram
    autonumber
    participant Source as Source System
    participant Raw as Raw S3 Bucket
    participant Lambda as Lambda Submitter
    participant AllowList as Supported Landing List<br/>S3 config file
    participant SSM as AWS Systems Manager
    participant SparkHost as Spark Host<br/>EC2 or EMR Edge Node
    participant Trusted as Trusted S3 Bucket
    participant Rejected as Rejected S3 Bucket
    participant Transformed as Transformed S3 Bucket
    participant Logs as CloudWatch Logs

    Source->>Raw: Put file<br/>s3://company-raw/orders/file.csv
    Raw-->>Lambda: S3 ObjectCreated event
    Lambda->>Lambda: Read event metadata<br/>bucket, key, run_id
    Lambda->>AllowList: Load supported file/folder names
    Lambda->>Lambda: Validate landed key is supported

    alt Unsupported file or folder
        Lambda->>Logs: Log unsupported landing key<br/>no SSM command submitted
    else Supported file or folder
        Lambda->>SSM: send-command<br/>submit pipeline command to Spark host
        SSM-->>Lambda: Return command id
        Lambda->>Logs: Log command id and exit
        SSM->>SparkHost: Execute shell command
        SparkHost->>SparkHost: Load pipeline config<br/>schema, DQ rules, transform_enabled
        SparkHost->>SparkHost: spark-submit ingestion job<br/>wait for completion
        SparkHost->>Raw: Read raw file
        SparkHost->>SparkHost: Perform DQ checks

        alt DQ failed
            SparkHost->>Rejected: Write failed records<br/>with dq_error
            SparkHost->>Logs: Write DQ failure summary
            SparkHost-->>SSM: Command failed or completed with rejected status
            SSM->>Logs: Store command result
        else DQ passed
            SparkHost->>Trusted: Write trusted baseline data
            SparkHost->>Logs: Write ingestion success summary

            alt Transformation enabled
                SparkHost->>SparkHost: spark-submit transformation job<br/>wait for completion
                SparkHost->>Trusted: Read trusted baseline data
                SparkHost->>SparkHost: Apply transformations
                SparkHost->>Transformed: Write transformed data
                SparkHost->>Logs: Write transformation success summary
            else Transformation disabled
                SparkHost->>Logs: Skip transformation
            end

            SparkHost-->>SSM: Command completed
            SSM->>Logs: Store final command status
        end
    end
```

## 3. Event-Driven Processing Flow

1. A source system writes a file to the raw S3 bucket.
2. The S3 `ObjectCreated` event triggers a Lambda function.
3. Lambda reads event metadata such as bucket, key, and run id.
4. Lambda reads a supported landing list from a config file in S3. This list contains valid file or folder names that are allowed to land in the raw bucket.
5. Lambda performs only basic validation: is this raw S3 key supported?
6. If the file or folder is unsupported, Lambda logs the unsupported key and stops. It does not submit an SSM command.
7. If the file or folder is supported, Lambda calls AWS Systems Manager Run Command.
8. SSM returns a command id to Lambda.
9. Lambda logs the command id and exits. Lambda does not run Spark, does not run DQ, and does not wait for Spark completion.
10. SSM runs a shell command on a Spark host, such as an EC2 instance or EMR edge node.
11. The shell command loads the pipeline configuration for that source or table.
12. The shell command starts a PySpark ingestion job with `spark-submit` and waits for it to complete.
13. The ingestion job reads the raw S3 file and applies DQ checks.
14. If DQ fails, the job writes rejected records to the rejected bucket and returns a failed or warning status.
15. If DQ succeeds, the job writes standardized baseline data to the trusted bucket.
16. If transformations are enabled in the config, the shell command runs a second `spark-submit` transformation job and waits for it to complete.
17. The transformation job reads from trusted, applies business rules, and writes to transformed.
18. SSM captures the final command status and logs.

Important implementation note: Lambda is only the submitter and basic landing-key validator. It should not run Spark, should not run DQ, and should not wait for the Spark job. The SSM command or shell script on the Spark host is responsible for running `spark-submit`, waiting for completion, and returning the final command status.

## 4. How The S3 Object Key Is Passed To Spark

The S3 event contains the bucket and object key. Lambda uses those values to
build the Spark input path.

Example S3 event shape:

```json
{
  "Records": [
    {
      "s3": {
        "bucket": { "name": "company-raw" },
        "object": { "key": "orders/2026/01/orders_20260101.csv" }
      }
    }
  ]
}
```

Lambda extracts:

```text
bucket = company-raw
key    = orders/2026/01/orders_20260101.csv
```

Then Lambda builds:

```text
input_path = s3://company-raw/orders/2026/01/orders_20260101.csv
```

The first folder in the key can be used to identify the dataset:

```text
orders/2026/01/orders_20260101.csv -> dataset = orders
```

Then Lambda sends an SSM command similar to:

```bash
spark-submit jobs/ingest.py \
  --dataset orders \
  --input s3://company-raw/orders/2026/01/orders_20260101.csv \
  --trusted-output s3://company-trusted/orders/ \
  --rejected-output s3://company-rejected/orders/
```

In production, Lambda should return after SSM accepts the command:

```json
{
  "status": "accepted",
  "ssm_command_id": "command-123",
  "input": "s3://company-raw/orders/2026/01/orders_20260101.csv"
}
```

Lambda should not poll SSM until the job finishes. The Spark job may run longer
than Lambda's 15-minute limit. Completion tracking should happen through SSM
command status, CloudWatch logs, Spark logs, and success/failure markers in S3.

Minimal Lambda submitter logic:

```python
from urllib.parse import unquote_plus
import boto3

ssm = boto3.client("ssm")


def handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])

    dataset = key.split("/", 1)[0]
    input_path = f"s3://{bucket}/{key}"

    command = (
        "spark-submit jobs/ingest.py "
        f"--dataset {dataset} "
        f"--input {input_path} "
        f"--trusted-output s3://company-trusted/{dataset}/ "
        f"--rejected-output s3://company-rejected/{dataset}/"
    )

    response = ssm.send_command(
        InstanceIds=["i-spark-edge-node"],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [command]},
    )

    return {
        "status": "accepted",
        "ssm_command_id": response["Command"]["CommandId"],
        "input": input_path,
    }
```

## 5. Bucket Roles

| Zone | Example S3 path | Purpose |
|---|---|---|
| Raw | `s3://company-raw/orders/` | Stores files exactly as received from source systems |
| Rejected | `s3://company-rejected/orders/run_id=.../` | Stores records or files that fail DQ checks |
| Trusted | `s3://company-trusted/orders/` | Stores clean, standardized baseline data |
| Transformed | `s3://company-transformed/orders_summary/` | Stores business-transformed data for analytics or downstream use |

The trusted bucket is the baseline bucket. It should contain clean data with standardized columns and data types, but without heavy business-specific transformation.

## 6. Logical Processing Flow

1. Files land in the raw S3 bucket.
2. A PySpark ingestion job reads raw files.
3. The job applies data quality checks.
4. Failed records are written to the rejected bucket with failure reasons.
5. Passed records are standardized and written to the trusted bucket.
6. If transformation is disabled, the pipeline stops after trusted output.
7. If transformation is enabled, PySpark reads trusted data, applies business transformations, and writes to the transformed bucket.
8. Glue Data Catalog tables can be created over trusted and transformed locations.

## 7. How Spark Actually Reads Data

Spark DataFrames are lazy. This line does not immediately read the whole file
into memory:

```python
df = spark.read.parquet("s3://company-raw/orders/")
```

It creates a logical plan that says: "when an action runs, read this data."

Examples of transformations that stay lazy:

```python
df = spark.read.parquet("s3://company-raw/orders/")
selected = df.select("order_id", "customer_id", "amount")
filtered = selected.filter("amount >= 0")
```

Examples of actions that make Spark execute the plan:

```python
filtered.count()
filtered.show()
filtered.write.parquet("s3://company-trusted/orders/")
filtered.collect()
filtered.toPandas()
```

### What Happens During `count()`

When you run:

```python
df.count()
```

Spark must count all relevant rows. For Parquet input, Spark creates scan tasks
from the files and file splits, then schedules those tasks across executor
cores.

If you have:

```text
100 Parquet files
4 executors
4 cores per executor
```

Spark can run roughly:

```text
4 executors x 4 cores = 16 tasks at the same time
```

It does not mean Spark reads only 4 files total. It means Spark processes a
limited number of tasks concurrently, then schedules the next tasks as executor
cores become free.

Mental model:

```text
count() reads all relevant data logically,
but physically reads it in parallel batches based on available executor cores.
```

For Parquet, Spark may use metadata and column pruning where possible, but you
should still treat `count()` as a real distributed action that scans the
dataset enough to compute the result.

### Why `collect()` Is Dangerous

This warning applies to a PySpark DataFrame:

```python
rows = df.collect()
```

`collect()` brings all rows from Spark executors back to the driver Python
process as a local list of `Row` objects. If the dataset is large, the driver
can run out of memory.

This is also dangerous:

```python
pandas_df = df.toPandas()
```

`toPandas()` collects all Spark rows to the driver and converts them into a
pandas DataFrame. Use it only for small results.

Safer patterns:

```python
df.show(20)                 # small display sample
df.limit(100).collect()     # small local sample
df.count()                  # distributed count, only final number returns
df.write.parquet(path)      # distributed write
```

Rule of thumb:

```text
Use collect() only when the result is small enough to fit comfortably in driver memory.
```

## 8. Example Data Quality Rules

For a beginner-friendly pipeline, start with simple rules.

Example input columns:

```text
order_id, customer_id, order_date, status, category, amount
```

Basic DQ rules:

| Rule | Example |
|---|---|
| Required columns exist | `order_id`, `customer_id`, `order_date`, `amount` must be present |
| Primary key is not null | `order_id is not null` |
| Customer is not null | `customer_id is not null` |
| Date is valid | `order_date` can be cast to date |
| Amount is valid | `amount >= 0` |
| Duplicate check | No duplicate `order_id` values within the batch |

Records that fail row-level checks should be written to the rejected bucket with a `dq_error` column.

Example rejected record:

```text
order_id,customer_id,order_date,status,category,amount,dq_error
7,C009,not-a-date,COMPLETE,books,19.99,invalid_order_date
8,,2026-01-05,COMPLETE,grocery,12.50,missing_customer_id
```

## 9. Interested Columns For Trusted Zone

The trusted zone should keep only the columns the data platform wants to standardize and support.

Example:

```text
order_id
customer_id
order_date
status
category
amount
ingestion_ts
source_file
run_id
```

Example standardization:

| Column | Trusted type |
|---|---|
| `order_id` | integer |
| `customer_id` | string |
| `order_date` | date |
| `status` | string |
| `category` | string |
| `amount` | double |
| `ingestion_ts` | timestamp |
| `source_file` | string |
| `run_id` | string |

## 10. Optional Transformation

Transformation should run only when enabled in configuration.

Example transformations:

- Filter to completed orders.
- Add `order_year` and `order_month`.
- Create `amount_bucket`.
- Aggregate total amount by category.
- Join with customer or product reference data.

Example transformed output:

```text
category,order_year,order_month,order_count,total_amount,avg_amount
books,2026,1,2,50.50,25.25
electronics,2026,1,2,399.98,199.99
grocery,2026,1,1,42.25,42.25
```

## 11. Incremental Load vs Full Load

Most batch ingestion jobs use one of these two patterns.

| Pattern | Example dataset | What happens |
|---|---|---|
| Incremental load | `orders` | Each file contains new records. The trusted baseline grows over time. |
| Full load | `house_price_growth` | Each file contains the complete current table. The trusted baseline is replaced each run. |

For the local demo:

- `orders` uses `incremental_append_new_keys`. It appends new `order_id` values and skips keys already present in trusted.
- `house_price_growth` uses `full_overwrite`. It replaces the trusted baseline every run because the file is treated as a complete snapshot.

Important production note: plain Parquet in S3 does not behave like a database table. Incremental append handles new rows, but it does not truly update/delete/merge old rows. For row-level changes, production data lakes usually use Apache Iceberg, Apache Hudi, or Delta Lake.

## 12. Configuration-Driven Design

The future pipeline should be driven by configuration instead of hardcoding each source.

Example config shape:

```yaml
pipeline_name: orders_ingestion
source:
  format: csv
  path: s3://company-raw/orders/
  options:
    header: true
    inferSchema: false

target:
  trusted_path: s3://company-trusted/orders/
  rejected_path: s3://company-rejected/orders/
  transformed_path: s3://company-transformed/orders_summary/

load_strategy:
  type: incremental_append_new_keys

schema:
  columns:
    - name: order_id
      type: integer
      required: true
    - name: customer_id
      type: string
      required: true
    - name: order_date
      type: date
      required: true
    - name: status
      type: string
      required: false
    - name: category
      type: string
      required: false
    - name: amount
      type: double
      required: true

dq_rules:
  primary_key: order_id
  non_negative_columns:
    - amount
  duplicate_check: true

trusted:
  interested_columns:
    - order_id
    - customer_id
    - order_date
    - status
    - category
    - amount

transformation:
  enabled: true
  output_mode: overwrite
  partition_by:
    - order_year
  rules:
    - add_date_parts
    - add_amount_bucket
    - aggregate_by_category_month
```

## 13. LocalStack Mapping For Local Practice

In local practice, Docker plus LocalStack acts like a small local AWS account.

Use LocalStack buckets like:

```text
s3://de-lab-raw/orders/
s3://de-lab-rejected/orders/
s3://de-lab-trusted/orders/
s3://de-lab-transformed/orders_summary/
```

The same architecture works locally:

```text
Raw LocalStack S3 -> PySpark DQ -> Trusted LocalStack S3 -> Optional transform -> Transformed LocalStack S3
```

## 14. Review Questions Before Building

Before building the config-driven pipeline, confirm:

1. Should rejected data be stored as full failed records or only error summaries?
2. Should duplicate `order_id` fail the entire batch or only reject duplicate rows?
3. Should trusted output be partitioned by `order_date`, `order_year`, or source system?
4. Should transformation output be one table per source or one table per business use case?
5. Should the pipeline support only CSV first, or CSV, JSON, and Parquet from the beginning?
