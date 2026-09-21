# Mac Local Spark Submit Lab

Step-by-step setup for running PySpark and `spark-submit` on macOS from this project:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
```

This guide is written for interview preparation and local hands-on practice with CSV, JSON, and Parquet reads/writes.

## Recommended Version Choice

For this repo and AWS Glue-style practice, use:

| Tool | Recommended local version | Why |
|---|---:|---|
| Java | 17 | Matches modern Spark and Glue 5.x local behavior |
| Python | 3.11 | Good match for Glue 5.x and modern PySpark |
| Spark / PySpark | 3.5.x | Closest practical match for AWS Glue 5.x labs |

Apache Spark also has newer 4.x releases. As of September 2026, Apache Spark documentation lists Spark 4.2.0 as a stable release, running on Java 17/21/25 and Python 3.10+. AWS Glue 5.x, however, is Spark 3.5.x based, so Spark 3.5.x is the safer interview/practice default for Glue and EMR-adjacent examples.

**Python 3.11 vs 3.12 note:** use Python 3.11 for this lab. Python 3.12 is fine for many Python projects, but Python 3.11 is the better choice for Spark 3.5.x and AWS Glue 5.x alignment. This reduces version mismatch issues when you later compare local `spark-submit` behavior with Glue or EMR jobs.

Useful references:

- Apache Spark downloads: https://spark.apache.org/downloads
- Apache Spark 4.2.0 docs: https://spark.apache.org/docs/4.2.0/
- AWS Glue versions: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html

## 1. Install Homebrew

Skip this if `brew --version` already works.

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Apple Silicon Macs usually need this in `~/.zshrc`:

```bash
eval "$(/opt/homebrew/bin/brew shellenv)"
```

Intel Macs usually use:

```bash
eval "$(/usr/local/bin/brew shellenv)"
```

Reload your shell:

```bash
source ~/.zshrc
brew --version
```

## 2. Install Java 17

```bash
brew install openjdk@17
```

Add Java 17 to `~/.zshrc`.

For Apple Silicon:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
```

For Intel:

```bash
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
```

Reload and verify:

```bash
source ~/.zshrc
java -version
echo $JAVA_HOME
```

Expected: Java 17.

## 3. Install Python 3.11

```bash
brew install python@3.11
python3.11 --version
```

## 4. Create A Project Virtual Environment

Run these commands from the repo root:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install PySpark and useful local development libraries:

```bash
python -m pip install \
  --no-cache-dir \
  --timeout 120 \
  --retries 10 \
  "pyspark==3.5.6" pandas pyarrow jupyterlab ipykernel
```

Why `3.5.6`? AWS Glue 5.1 uses Spark 3.5.6. If you want Glue 5.0 alignment instead, use:

```bash
python -m pip install "pyspark==3.5.4"
```

Verify:

```bash
python - <<'PY'
import pyspark
print("PySpark version:", pyspark.__version__)
PY
```

## 5. Set Spark Environment Variables

When PySpark is installed with `pip`, Spark scripts live inside the virtual environment package. Add this to `~/.zshrc`:

```bash
export SPARK_HOME="/Users/paramraghavan/dev/sparksql-awsglue/.venv/lib/python3.11/site-packages/pyspark"
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON="/Users/paramraghavan/dev/sparksql-awsglue/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="/Users/paramraghavan/dev/sparksql-awsglue/.venv/bin/python"
```

Reload:

```bash
source ~/.zshrc
```

Verify the commands:

```bash
which spark-submit
spark-submit --version
which pyspark
pyspark --version
```

Expected: `spark-submit` and `pyspark` resolve under:

```text
/Users/paramraghavan/dev/sparksql-awsglue/.venv/lib/python3.11/site-packages/pyspark/bin
```

## 6. Quick Spark Shell Test

```bash
pyspark --master "local[*]"
```

Inside the PySpark shell:

```python
spark.range(5).show()
spark.stop()
exit()
```

`local[*]` means Spark uses all local CPU cores. For interview examples, `local[2]` is also useful because it makes parallelism easier to reason about.

## 7. Smoke Test With A Small PySpark File

Use this before the larger ETL example. It proves that Python, Java, PySpark, and `spark-submit` are all working together.

Create the lab folders:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
mkdir -p local_spark_lab/input local_spark_lab/output local_spark_lab/jobs
```

Create this file:

```text
/Users/paramraghavan/dev/sparksql-awsglue/local_spark_lab/jobs/smoke_test.py
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("local-spark-smoke-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    data = [
        ("books", 10.0),
        ("books", 15.5),
        ("grocery", 7.25),
        ("electronics", 199.99),
        ("electronics", 99.99),
    ]

    df = spark.createDataFrame(data, ["category", "amount"])

    result = (
        df.groupBy("category")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("category")
    )

    print("Raw data")
    df.show()

    print("Aggregated result")
    result.show()

    print("Spark version:", spark.version)
    print("Default parallelism:", spark.sparkContext.defaultParallelism)

    spark.stop()


if __name__ == "__main__":
    main()
```

Run it:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
source .venv/bin/activate

spark-submit \
  --master "local[2]" \
  --name "smoke-test" \
  local_spark_lab/jobs/smoke_test.py
```

Expected output includes:

```text
Spark version: 3.5.x
Default parallelism: 2
```

You should also see grouped totals for `books`, `electronics`, and `grocery`.

## 8. Create Local Input Data

Create a CSV file:

```bash
cat > local_spark_lab/input/orders.csv <<'EOF'
order_id,customer_id,order_date,category,amount
1,C001,2026-01-01,books,35.50
2,C002,2026-01-01,electronics,299.99
3,C001,2026-01-02,books,15.00
4,C003,2026-01-02,grocery,42.25
5,C002,2026-01-03,electronics,99.99
6,C004,2026-01-03,grocery,18.75
EOF
```

Create a JSON file:

```bash
cat > local_spark_lab/input/customers.json <<'EOF'
{"customer_id":"C001","name":"Asha","state":"CA"}
{"customer_id":"C002","name":"Ben","state":"NY"}
{"customer_id":"C003","name":"Cara","state":"TX"}
{"customer_id":"C004","name":"Dev","state":"WA"}
EOF
```

## 9. Simple Read Transform Write Test

This is the first full `spark-submit` test you should run after installation. It reads a CSV file, transforms the data, and writes both Parquet and CSV outputs.

Create:

```text
/Users/paramraghavan/dev/sparksql-awsglue/local_spark_lab/jobs/read_transform_write.py
```

```python
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    input_path = project_root / "local_spark_lab" / "input" / "orders.csv"
    output_dir = project_root / "local_spark_lab" / "output" / "read_transform_write"

    spark = (
        SparkSession.builder
        .appName("read-transform-write-local")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    orders_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )

    transformed_df = (
        orders_df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_year", F.year("order_date"))
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount") >= 100, F.lit("high"))
            .when(F.col("amount") >= 25, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_year",
            "category",
            "amount",
            "amount_bucket",
        )
    )

    summary_df = (
        transformed_df
        .groupBy("order_year", "category", "amount_bucket")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("category", "amount_bucket")
    )

    print("Input CSV")
    orders_df.show(truncate=False)
    orders_df.printSchema()

    print("Transformed DataFrame")
    transformed_df.show(truncate=False)
    transformed_df.printSchema()

    print("Summary DataFrame")
    summary_df.show(truncate=False)

    (
        transformed_df.write
        .mode("overwrite")
        .partitionBy("order_year", "category")
        .parquet(str(output_dir / "orders_parquet"))
    )

    (
        summary_df.coalesce(1).write
        .mode("overwrite")
        .option("header", True)
        .csv(str(output_dir / "summary_csv"))
    )

    print(f"Wrote Parquet to: {output_dir / 'orders_parquet'}")
    print(f"Wrote CSV to: {output_dir / 'summary_csv'}")

    spark.stop()


if __name__ == "__main__":
    main()
```

Run with `spark-submit`:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
source .venv/bin/activate

spark-submit \
  --master "local[2]" \
  --name "read-transform-write-local" \
  local_spark_lab/jobs/read_transform_write.py
```

Verify files were written:

```bash
find local_spark_lab/output/read_transform_write -maxdepth 5 -type f | sort
```

Expected output paths:

```text
local_spark_lab/output/read_transform_write/orders_parquet/order_year=2026/category=books/...
local_spark_lab/output/read_transform_write/orders_parquet/order_year=2026/category=electronics/...
local_spark_lab/output/read_transform_write/orders_parquet/order_year=2026/category=grocery/...
local_spark_lab/output/read_transform_write/summary_csv/part-....csv
```

Read the files back with a one-off PySpark command:

```bash
python - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify-outputs").master("local[2]").getOrCreate()

parquet_df = spark.read.parquet("local_spark_lab/output/read_transform_write/orders_parquet")
csv_df = spark.read.option("header", True).csv("local_spark_lab/output/read_transform_write/summary_csv")

print("Parquet output")
parquet_df.show(truncate=False)
parquet_df.printSchema()

print("CSV summary output")
csv_df.show(truncate=False)
csv_df.printSchema()

spark.stop()
PY
```

Interview explanation:

```text
read.csv() reads the input file into a DataFrame.
withColumn(), select(), and groupBy() are transformations.
write.parquet() and write.csv() are actions because they trigger execution.
partitionBy() writes Parquet into folder partitions for faster filtered reads later.
coalesce(1) creates one CSV data file for local learning, but it is not ideal for large production data.
```

## 10. Create Your First Spark Submit Job

Create:

```text
/Users/paramraghavan/dev/sparksql-awsglue/local_spark_lab/jobs/orders_etl.py
```

```python
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    input_dir = project_root / "local_spark_lab" / "input"
    output_dir = project_root / "local_spark_lab" / "output"

    spark = (
        SparkSession.builder
        .appName("local-orders-etl")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_dir / "orders.csv"))
    )

    customers = spark.read.json(str(input_dir / "customers.json"))

    enriched = (
        orders.join(customers, on="customer_id", how="left")
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_year", F.year("order_date"))
    )

    category_summary = (
        enriched.groupBy("state", "category")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .orderBy("state", "category")
    )

    print("Input orders")
    orders.show(truncate=False)

    print("Enriched orders")
    enriched.show(truncate=False)

    print("Category summary")
    category_summary.show(truncate=False)

    (
        enriched.write
        .mode("overwrite")
        .partitionBy("order_year", "state")
        .parquet(str(output_dir / "orders_enriched_parquet"))
    )

    (
        category_summary.coalesce(1).write
        .mode("overwrite")
        .option("header", True)
        .csv(str(output_dir / "category_summary_csv"))
    )

    (
        category_summary.coalesce(1).write
        .mode("overwrite")
        .json(str(output_dir / "category_summary_json"))
    )

    spark.stop()


if __name__ == "__main__":
    main()
```

## 11. Run With spark-submit

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
source .venv/bin/activate

spark-submit \
  --master "local[*]" \
  --name "orders-etl-local" \
  local_spark_lab/jobs/orders_etl.py
```

Check outputs:

```bash
find local_spark_lab/output -maxdepth 4 -type f | sort
```

You should see:

```text
local_spark_lab/output/orders_enriched_parquet/...
local_spark_lab/output/category_summary_csv/...
local_spark_lab/output/category_summary_json/...
```

## 12. Read The Output Back

Create:

```text
/Users/paramraghavan/dev/sparksql-awsglue/local_spark_lab/jobs/read_outputs.py
```

```python
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "local_spark_lab" / "output"

    spark = (
        SparkSession.builder
        .appName("read-local-outputs")
        .master("local[*]")
        .getOrCreate()
    )

    parquet_df = spark.read.parquet(str(output_dir / "orders_enriched_parquet"))
    csv_df = spark.read.option("header", True).csv(str(output_dir / "category_summary_csv"))
    json_df = spark.read.json(str(output_dir / "category_summary_json"))

    print("Parquet output")
    parquet_df.printSchema()
    parquet_df.show(truncate=False)

    print("CSV output")
    csv_df.printSchema()
    csv_df.show(truncate=False)

    print("JSON output")
    json_df.printSchema()
    json_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
```

Run:

```bash
spark-submit \
  --master "local[*]" \
  --name "read-local-outputs" \
  local_spark_lab/jobs/read_outputs.py
```

## 13. Core Interview Concepts In This Example

### Read APIs

```python
spark.read.option("header", True).option("inferSchema", True).csv(path)
spark.read.json(path)
spark.read.parquet(path)
```

### Transformations

Transformations build a logical plan. They are lazy.

```python
select
filter
withColumn
join
groupBy
orderBy
repartition
coalesce
```

### Actions

Actions trigger execution.

```python
show
count
collect
write
take
foreach
```

### Narrow vs Wide Transformations

Narrow transformations usually avoid shuffle:

```python
select
filter
withColumn
```

Wide transformations usually create shuffle:

```python
groupBy
join
distinct
repartition
orderBy
```

Interview wording:

> Spark is lazy. It builds a plan until an action is called. Wide transformations create shuffle boundaries, which split the job into stages.

## 14. Useful spark-submit Options

Local mode:

```bash
spark-submit \
  --master "local[*]" \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  local_spark_lab/jobs/orders_etl.py
```

Verbose troubleshooting:

```bash
spark-submit \
  --master "local[2]" \
  --verbose \
  local_spark_lab/jobs/orders_etl.py
```

Pass job arguments:

```bash
spark-submit \
  --master "local[*]" \
  local_spark_lab/jobs/my_job.py \
  --input local_spark_lab/input/orders.csv \
  --output local_spark_lab/output/orders
```

## 15. Spark UI On Your Mac

While a Spark job is running, open:

```text
http://localhost:4040
```

Important tabs:

| Tab | What to inspect |
|---|---|
| Jobs | One action usually creates one job |
| Stages | Shuffle boundaries and task counts |
| SQL/DataFrame | Physical plan, scan, join, aggregate |
| Executors | Memory, cores, task time |
| Environment | Spark config used by the job |

For small local jobs, the UI may disappear quickly after the job finishes. Add a temporary pause when teaching yourself:

```python
input("Open http://localhost:4040, then press Enter to stop Spark...")
```

Put it before `spark.stop()`.

## 16. Optional: Install Apache Spark With Homebrew

The `pip install pyspark` approach is the cleanest for local PySpark interview practice. If you want a system-level Spark install:

```bash
brew install apache-spark
brew info apache-spark
```

Then set `SPARK_HOME` to the `libexec` path shown by Homebrew.

Apple Silicon example:

```bash
export SPARK_HOME=/opt/homebrew/opt/apache-spark/libexec
export PATH="$SPARK_HOME/bin:$PATH"
```

Intel example:

```bash
export SPARK_HOME=/usr/local/opt/apache-spark/libexec
export PATH="$SPARK_HOME/bin:$PATH"
```

Verify:

```bash
spark-submit --version
```

Note: Homebrew may install the latest Spark version, which can move ahead of AWS Glue/EMR runtimes. For Glue-style reproducibility, prefer the project virtual environment with a pinned PySpark version.

## 17. Optional: JupyterLab With PySpark

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
source .venv/bin/activate
python -m ipykernel install --user --name sparksql-awsglue --display-name "sparksql-awsglue"
jupyter lab
```

In a notebook:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("notebook-local-spark")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

df = spark.range(10)
df.show()
```

## 18. Optional: Reading From S3 Locally

For interview prep, local files are enough. For S3 practice, you need AWS credentials and compatible Hadoop AWS jars.

With Spark 3.5.x / Hadoop 3.3.x style setups, the common packages are:

```bash
spark-submit \
  --master "local[*]" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider \
  local_spark_lab/jobs/read_s3.py
```

Use an AWS profile:

```bash
export AWS_PROFILE=your-profile-name
aws sts get-caller-identity
```

Example Spark read:

```python
df = spark.read.parquet("s3a://your-bucket/path/")
df.show()
```

If your real target is AWS Glue 6.x or Spark 4.x, re-check S3 connector versions because Spark, Hadoop, and AWS SDK versions must be compatible.

## 19. Common Errors And Fixes

### `JAVA_HOME is not set`

Fix:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
source ~/.zshrc
```

Use the Intel path if your Homebrew is under `/usr/local`.

### `UnsupportedClassVersionError`

Cause: Java version mismatch.

Fix:

```bash
java -version
echo $JAVA_HOME
```

Use Java 17 for Spark 3.5.x and modern Glue-style work.

### `ModuleNotFoundError: No module named pyspark`

Cause: virtual environment is not active or `PYSPARK_PYTHON` points to another Python.

Fix:

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
source .venv/bin/activate
python -m pip show pyspark
echo $PYSPARK_PYTHON
```

### `Python worker failed to connect back`

Often caused by Python path mismatch.

Fix:

```bash
export PYSPARK_PYTHON="/Users/paramraghavan/dev/sparksql-awsglue/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="/Users/paramraghavan/dev/sparksql-awsglue/.venv/bin/python"
```

### Port `4040` Is Already In Use

Spark will try `4041`, `4042`, and so on. Check terminal logs for the actual Spark UI URL.

### Output Folder Already Exists

Use:

```python
df.write.mode("overwrite").parquet(path)
```

For interview answers, mention that production overwrite needs care because it can delete prior data.

## 20. Clean Up Local Output

```bash
cd /Users/paramraghavan/dev/sparksql-awsglue
rm -rf local_spark_lab/output
mkdir -p local_spark_lab/output
```

## 21. Practice Checklist

Run each task with `spark-submit`:

- Read CSV with header and inferred schema.
- Read newline-delimited JSON.
- Write Parquet partitioned by one or two columns.
- Read Parquet back and inspect schema.
- Convert CSV to Parquet.
- Convert Parquet to CSV.
- Use `filter`, `select`, `withColumn`, `groupBy`, and `join`.
- Explain which lines are transformations and which lines are actions.
- Open Spark UI and identify jobs, stages, and tasks.
- Change `spark.sql.shuffle.partitions` from `4` to `200` and explain what changes locally.

## 22. Mental Model For Interviews

Use this simple explanation:

> A Spark application starts a driver. The driver creates a SparkSession and builds a logical plan from DataFrame transformations. Nothing runs until an action is called. When an action runs, Spark optimizes the plan, creates jobs, splits jobs into stages at shuffle boundaries, and runs tasks across partitions. In local mode, my Mac acts as both driver and executor machine. In EMR or Glue, executors run across cluster workers.
