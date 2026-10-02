# Config-Driven PySpark Ingestion And Transformation Demo

This folder implements the architecture from `pyspark-ingestion-transformation-architecture.md` as a local runnable demo.

It simulates:

- S3 raw landing event
- Lambda submitter
- supported file/folder validation
- SSM command runner
- PySpark DQ and ingestion
- trusted baseline output
- optional PySpark transformation

For the quickstart, local folders stand in for S3 buckets.

This demo teaches two common ingestion patterns:

| Dataset | Load pattern | Why |
|---|---|---|
| `orders` | Incremental append of new business keys | Transaction-like facts usually arrive in batches; the trusted baseline should grow over time |
| `house_price_growth` | Full overwrite | Reference/snapshot data often arrives as the complete table each time; the trusted baseline is replaced |

AWS note: this demo does not call real AWS. Local folders stand in for S3 so beginners can learn the orchestration pattern first. When you practice actual AWS-style S3 or Glue commands elsewhere in this quickstart, use Docker + LocalStack, not a real AWS account.

## Folder Map

| Path | Purpose |
|---|---|
| `data/raw/orders/` | Raw landing zone |
| `data/raw/house_price_growth/` | Raw landing zone for the full-load example |
| `data/rejected/orders/` | DQ rejected records |
| `data/trusted/orders/` | Trusted baseline output |
| `data/trusted/house_price_growth/` | Trusted full-load baseline output |
| `data/transformed/orders_summary/` | Transformed output |
| `config/allowed_landings.json` | Lambda supported landing list |
| `config/orders_pipeline.json` | Source-specific pipeline config |
| `config/house_price_growth_pipeline.json` | Full-load source config |
| `jobs/lambda_submitter.py` | Local Lambda stand-in |
| `jobs/ssm_runner.py` | Local SSM command stand-in |
| `jobs/ingest_with_dq.py` | PySpark DQ and trusted ingestion job |
| `jobs/transform_trusted.py` | Optional PySpark transformation job |

## Install Everything Needed

This demo needs:

- Java 17
- Python 3.11
- PySpark
- A Python virtual environment

LocalStack is not required for this demo because local folders stand in for S3 buckets. For AWS-style examples using `awslocal`, `s3a://de-lab/...`, or Glue Catalog commands, install Docker Desktop and start LocalStack using the Mac or Windows setup guide.

### Option A: If You Already Completed The Spark Quickstart

Activate the virtual environment you created in the setup guide:

Mac:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue/help/quick101-spark/spark_quickstart/pipeline_demo
source ~/spark-glue-local-lab/.venv/bin/activate
```

Windows PowerShell:

```powershell
cd C:\path\to\sparksql-awsglue\help\quick101-spark\spark_quickstart\pipeline_demo
C:\spark-glue-local-lab\.venv\Scripts\Activate.ps1
```

### Option B: Fresh Mac Install

Install Java 17 and Python 3.11:

```bash
brew install openjdk@17 python@3.11
```

Set Java 17 for this terminal.

Apple Silicon:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
```

Intel Mac:

```bash
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
```

Create and activate a virtual environment:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue/help/quick101-spark/spark_quickstart/pipeline_demo
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install project dependencies:

```bash
python -m pip install "pyspark==3.5.6" pyarrow pandas
```

### Option C: Fresh Windows Install

Install:

- Java 17 JDK from Adoptium: `https://adoptium.net/temurin/releases/?version=17`
- Python 3.11 from `https://www.python.org/downloads/windows/`

Then open PowerShell:

```powershell
cd C:\path\to\sparksql-awsglue\help\quick101-spark\spark_quickstart\pipeline_demo
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
python -m pip install pyspark==3.5.6 pyarrow pandas
```

If PowerShell blocks activation scripts:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\.venv\Scripts\Activate.ps1
```

### Verify The Install

Run:

```bash
java -version
python --version
python -c "import pyspark; print(pyspark.__version__)"
```

Expected:

```text
Java 17
Python 3.11.x
PySpark 3.5.x
```

Optional Spark launcher check:

```bash
which spark-submit
spark-submit --version
```

On Windows PowerShell:

```powershell
where.exe spark-submit
spark-submit --version
```

If `spark-submit` is not on `PATH`, the demo scripts can run the PySpark jobs with `python`.

For the easiest local run, set:

Mac:

```bash
export USE_PYTHON_SUBMIT=1
```

Windows PowerShell:

```powershell
$env:USE_PYTHON_SUBMIT="1"
```

That tells the local SSM runner to run the PySpark jobs with the active Python interpreter instead of `spark-submit`.

If `python -c "import pyspark"` fails, the virtual environment is not ready. Recreate it using the steps above or finish one of these setup guides:

```text
../setup-mac-local-spark.md
../setup-windows-local-spark.md
```

## Important: What `--wait` Means

Real AWS Lambda does not wait for Spark completion in this architecture. Lambda validates the landing key, submits the SSM command, logs the command id, and exits.

The local demo uses `--wait` only so beginners can run one command and see the whole result in the terminal. Treat `--wait` as a local training shortcut, not as Lambda behavior.

## Run Orders Initial Incremental Load

This simulates a supported raw file landing:

```bash
export USE_PYTHON_SUBMIT=1

python jobs/lambda_submitter.py \
  --bucket company-raw \
  --key orders/orders_clean.csv \
  --wait
```

Expected behavior:

1. Lambda validates `orders/orders_clean.csv` against `config/allowed_landings.json`.
2. Lambda submits local SSM runner. With `--wait`, the local script waits only for demo readability.
3. SSM runner runs the ingestion job.
4. Ingestion DQ passes.
5. New `order_id` values are appended to `data/trusted/orders`.
6. Transformation is enabled in config.
7. The transform reads the trusted baseline and refreshes `data/transformed/orders_summary`.

Inspect output:

```bash
find data/trusted/orders -maxdepth 5 -type f | sort
find data/transformed/orders_summary -maxdepth 5 -type f | sort
```

Read the trusted output:

```bash
python - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read-trusted").master("local[2]").getOrCreate()
spark.read.parquet("data/trusted/orders").show(truncate=False)
spark.read.parquet("data/transformed/orders_summary").show(truncate=False)
spark.stop()
PY
```

## Run Orders Second Incremental Load

This file has two new orders and one already-loaded `order_id`. The ingestion job appends only new business keys.

```bash
export USE_PYTHON_SUBMIT=1

python jobs/lambda_submitter.py \
  --bucket company-raw \
  --key orders/orders_incremental.csv \
  --wait
```

Expected behavior:

1. `order_id` 7 and 8 are appended to trusted.
2. `order_id` 2 is skipped because it already exists in the trusted baseline.
3. The orders transform runs again and refreshes the aggregate output from the current trusted baseline.

This is a beginner-friendly incremental pattern. It handles new rows, not updates to existing rows. In real projects, updates/deletes/merges usually use Apache Iceberg, Apache Hudi, or Delta Lake.

## Run House Price Growth Full Load

This simulates a snapshot/reference-style dataset where each landing file is the complete table.

```bash
export USE_PYTHON_SUBMIT=1

python jobs/lambda_submitter.py \
  --bucket company-raw \
  --key house_price_growth/house_price_growth_2026.csv \
  --wait
```

Expected behavior:

1. Lambda validates `house_price_growth/house_price_growth_2026.csv`.
2. SSM runs the same generic ingestion job.
3. DQ passes.
4. Trusted Parquet is written to `data/trusted/house_price_growth`.
5. The trusted baseline is overwritten because this config uses `full_overwrite`.
6. Transformation is skipped because `config/house_price_growth_pipeline.json` has `"enabled": false`.

Inspect output:

```bash
find data/trusted/house_price_growth -maxdepth 5 -type f | sort
```

## Run DQ Failure Path

This file has an invalid date, missing customer, negative amount, and duplicate `order_id`.

```bash
export USE_PYTHON_SUBMIT=1

python jobs/lambda_submitter.py \
  --bucket company-raw \
  --key orders/orders_bad.csv \
  --wait
```

Expected behavior:

1. Lambda validates the landing key and submits SSM.
2. Ingestion job runs DQ.
3. DQ fails.
4. Rejected CSV output is written under `data/rejected/orders/run_id=...`.
5. Transformation is skipped.

Inspect rejected output:

```bash
find data/rejected/orders -maxdepth 5 -type f | sort
```

## Run Unsupported Landing Path

This simulates a file/folder that is not allowed to trigger a pipeline.

```bash
python jobs/lambda_submitter.py \
  --bucket company-raw \
  --key unsupported/file.csv \
  --wait
```

Expected behavior:

```text
unsupported landing key
no SSM command submitted
```

## Disable Transformation

Edit `config/orders_pipeline.json`:

```json
"transformation": {
  "enabled": false
}
```

Then rerun:

```bash
python jobs/lambda_submitter.py \
  --bucket company-raw \
  --key orders/orders_clean.csv \
  --wait
```

Expected behavior:

- trusted output is written
- transformed output is skipped

## How This Maps To AWS

| Local demo | AWS architecture |
|---|---|
| `jobs/lambda_submitter.py` | Lambda function triggered by S3 `ObjectCreated` |
| `config/allowed_landings.json` | Supported landing list stored in S3 |
| `jobs/ssm_runner.py` | SSM Run Command on Spark host |
| `jobs/ingest_with_dq.py` | PySpark ingestion job submitted by SSM |
| `jobs/transform_trusted.py` | Optional PySpark transform job submitted by SSM |
| local `data/raw` | raw S3 bucket |
| local `data/rejected` | rejected S3 bucket |
| local `data/trusted` | trusted S3 bucket |
| local `data/transformed` | transformed S3 bucket |

In AWS, Lambda should still only validate and submit. It should not run Spark, run DQ, or wait for Spark completion.

## Production Notes

Incremental append of new keys is useful for learning and for simple insert-only facts. It is not a full database `MERGE`. If an existing order changes status or amount, this quickstart skips the duplicate key. Production pipelines commonly use one of these patterns:

- append immutable events and derive current state later,
- overwrite only affected partitions after validation,
- use Apache Iceberg, Hudi, or Delta Lake for row-level update/delete/merge semantics.

Full overwrite is common for small reference or snapshot datasets where the source sends the whole table every time. It is risky for large fact tables because a bad delivery can replace a good trusted baseline.

Schema failures are different from row-level DQ failures. If required columns are missing, the ingestion job writes a small schema-failure artifact under `data/rejected/<source>/run_id=...` and stops. If individual rows fail DQ, the bad rows are written with a `dq_error` column.
