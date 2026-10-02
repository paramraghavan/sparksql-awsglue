# Mac Local Spark, AWS Glue, and LocalStack Setup

This guide helps SQL/database users run PySpark locally, practice AWS Glue-style ETL, and simulate S3 reads/writes with LocalStack.

Goal:

- Run `pyspark` and `spark-submit` on macOS.
- Use Spark DataFrames with SQL-like operations.
- Practice S3-style file work locally with LocalStack.
- Understand where AWS Glue fits in real data engineering jobs.

How to use this guide:

1. Complete sections 1-10 first. That gives you Java, Python, PySpark, sample input files, and a working local Spark job.
2. Then study `pyspark-101-examples.md` from this documentation folder while running its commands from `~/spark-glue-local-lab`.
3. Complete the LocalStack sections only when you reach the LocalStack S3 examples in `pyspark-101-examples.md`.

LocalStack is optional for the first PySpark lessons. You can learn reads, filters, joins, aggregations, writes, partitions, and Parquet update-by-rewrite using only local files.

All AWS-style practice in this guide uses Docker + LocalStack. You do not need a real AWS account. Commands such as `awslocal s3 ...`, `awslocal glue ...`, and Spark paths such as `s3a://de-lab/...` talk to LocalStack running inside Docker.

## 1. Recommended Versions

For Glue-oriented learning, use versions that stay close to AWS Glue 5.x.

| Tool | Recommended | Why |
|---|---:|---|
| Java | 17 | Good Spark 3.5.x choice |
| Python | 3.11 | Matches Glue 5.x Python direction |
| PySpark | 3.5.x | Closest practical local match for Glue 5.x |
| Docker Desktop | Current stable | Required only for optional LocalStack S3 practice |
| LocalStack | Current stable | Simulates AWS services locally |

AWS Glue version 5.0 supports Spark 3.5.4 and Python 3.11. AWS announced Glue 5.1 with Spark 3.5.6 and Python 3.11. For local learning, either `pyspark==3.5.4` or `pyspark==3.5.6` is a good choice.

References:

- AWS Glue release notes: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html
- AWS Glue version support: https://docs.aws.amazon.com/glue/latest/dg/glue-version-support-policy.html
- PySpark installation: https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html
- LocalStack S3 docs: https://docs.localstack.cloud/aws/services/s3/
- LocalStack Glue docs: https://docs.localstack.cloud/aws/services/glue/

## 2. Install Homebrew

Skip this if `brew --version` works.

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Apple Silicon:

```bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
source ~/.zshrc
```

Intel Mac:

```bash
echo 'eval "$(/usr/local/bin/brew shellenv)"' >> ~/.zshrc
source ~/.zshrc
```

Verify:

```bash
brew --version
```

## 3. Install Java 17

```bash
brew install openjdk@17
```

Apple Silicon:

```bash
cat >> ~/.zshrc <<'EOF'
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
EOF
source ~/.zshrc
```

Intel Mac:

```bash
cat >> ~/.zshrc <<'EOF'
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
EOF
source ~/.zshrc
```

Verify:

```bash
java -version
echo "$JAVA_HOME"
```

Expected: Java 17.

## 4. Install Python 3.11

```bash
brew install python@3.11
python3.11 --version
```

## 5. Create A Local Lab Folder

Use any folder you like. This guide uses:

```bash
mkdir -p ~/spark-glue-local-lab
cd ~/spark-glue-local-lab
```

Create the working folders:

```bash
mkdir -p data/input data/output jobs warehouse
```

## 6. Create A Virtual Environment

```bash
cd ~/spark-glue-local-lab
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install the libraries:

```bash
python -m pip install \
  "pyspark==3.5.6" \
  pandas \
  pyarrow \
  boto3 \
  awscli-local \
  localstack \
  jupyterlab \
  ipykernel
```

If your target company uses Glue 5.0 specifically, use this instead:

```bash
python -m pip install "pyspark==3.5.4"
```

Verify:

```bash
python - <<'PY'
import pyspark
print("PySpark:", pyspark.__version__)
PY
```

## 7. Set Spark Environment Variables

If you installed Spark through `pip install pyspark`, the Spark scripts live inside the virtual environment.

Add this to `~/.zshrc`:

```bash
cat >> ~/.zshrc <<'EOF'
export SPARK_LAB_HOME="$HOME/spark-glue-local-lab"
export SPARK_HOME="$SPARK_LAB_HOME/.venv/lib/python3.11/site-packages/pyspark"
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON="$SPARK_LAB_HOME/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$SPARK_LAB_HOME/.venv/bin/python"
EOF
source ~/.zshrc
```

Verify:

```bash
which pyspark
which spark-submit
spark-submit --version
```

## 8. Run A PySpark Smoke Test

```bash
pyspark --master "local[2]"
```

Inside the PySpark shell:

```python
spark.range(5).show()
spark.sql("select current_date() as today").show()
spark.stop()
exit()
```

`local[2]` means Spark runs locally with two worker threads. `local[*]` uses all available cores.

## 9. Create First Input Files

```bash
cd ~/spark-glue-local-lab
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

## 10. Create A Basic Spark Submit Job

Create `jobs/orders_etl.py`:

```python
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    input_dir = root / "data" / "input"
    output_dir = root / "data" / "output" / "orders_etl"

    spark = (
        SparkSession.builder
        .appName("orders-etl-local")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
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
        orders
        .join(customers, "customer_id", "left")
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("is_successful", F.col("status") == F.lit("COMPLETE"))
    )

    summary = (
        enriched
        .where("is_successful")
        .groupBy("state", "category")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .orderBy("state", "category")
    )

    enriched.write.mode("overwrite").partitionBy("order_year", "category").parquet(
        str(output_dir / "orders_parquet")
    )

    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir / "summary_csv")
    )

    print("Enriched orders")
    enriched.show(truncate=False)

    print("Summary")
    summary.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
```

Run it:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate

spark-submit \
  --master "local[2]" \
  --name "orders-etl-local" \
  jobs/orders_etl.py
```

This first job does not use LocalStack. It writes to your local computer because the output path is built from:

```python
output_dir = root / "data" / "output" / "orders_etl"
```

If your lab folder is `~/spark-glue-local-lab`, then the Parquet output is written to:

```text
~/spark-glue-local-lab/data/output/orders_etl/orders_parquet/
```

The CSV summary is written to:

```text
~/spark-glue-local-lab/data/output/orders_etl/summary_csv/
```

Spark writes folders containing `part-*` files, not one single file. LocalStack starts later in this guide; LocalStack paths begin with `s3a://de-lab/...` or use `awslocal s3 ...`.

Verify output:

```bash
find data/output/orders_etl -maxdepth 5 -type f | sort
```

## 11. Next Step: Study The PySpark 101 Examples

At this point your local Spark setup is ready.

Use this file next:

```text
/Users/paramraghavan/dev/sparksql-awsglue/help/quick101-spark/spark_quickstart/pyspark-101-examples.md
```

Run the examples from your lab folder:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
```

Start with the local file examples. Come back to the next sections in this setup guide when you need LocalStack S3.

## 12. Optional: Install Docker Desktop For LocalStack

LocalStack runs inside Docker, so Docker Desktop must be installed and running before you start LocalStack.

Install Docker Desktop:

```text
https://www.docker.com/products/docker-desktop/
```

Verify:

```bash
docker --version
docker ps
```

If `docker ps` cannot connect to Docker, open Docker Desktop and wait until it finishes starting.

## 13. Optional: Start LocalStack

Start Docker Desktop first.

LocalStack is a local AWS simulator. For this lab, assume Docker is acting like a small local AWS account running on your laptop.

When LocalStack starts, it runs inside Docker and exposes AWS-like services at:

```text
http://localhost:4566
```

In this guide:

- `awslocal s3 ...` talks to LocalStack S3, not real AWS S3.
- `s3a://de-lab/...` paths write to the LocalStack bucket named `de-lab`.
- LocalStack data lives inside Docker-managed storage, not inside `data/output`.
- Use `awslocal s3 ls ...` to inspect what Spark wrote to LocalStack.
- Use `awslocal s3 cp ... --recursive` to download LocalStack output into your local project folder.

Mental model:

```text
data/output/...       -> normal folder on your computer
s3a://de-lab/...      -> fake/local S3 bucket running in Docker through LocalStack
```

Then run:

```bash
localstack start -d
localstack status services
```

Set local AWS environment variables:

```bash
cat >> ~/.zshrc <<'EOF'
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
EOF
source ~/.zshrc
```

Create a bucket:

```bash
awslocal s3 mb s3://de-lab
awslocal s3 ls

# Real AWS equivalent, shown for learning only:
# aws s3 mb s3://your-real-unique-bucket-name
# aws s3 ls
```

Upload your input files:

```bash
awslocal s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv
awslocal s3 cp data/input/customers.json s3://de-lab/raw/customers/customers.json
awslocal s3 ls s3://de-lab/raw/ --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 cp data/input/orders.csv s3://your-real-bucket/raw/orders/orders.csv
# aws s3 cp data/input/customers.json s3://your-real-bucket/raw/customers/customers.json
# aws s3 ls s3://your-real-bucket/raw/ --recursive
```

To download anything written to LocalStack S3 back to your local folder:

```bash
mkdir -p data/downloaded/orders_parquet

awslocal s3 cp \
  s3://de-lab/curated/orders_parquet/ \
  data/downloaded/orders_parquet/ \
  --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 cp \
#   s3://your-real-bucket/curated/orders_parquet/ \
#   data/downloaded/orders_parquet/ \
#   --recursive
```

## 14. Optional: Read And Write LocalStack S3 From Spark

Spark needs Hadoop AWS libraries to read `s3a://` paths. The easiest local approach is to let Spark download them with `--packages`.

Create `jobs/localstack_s3_etl.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("localstack-s3-etl")
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
        .where(F.col("status") == "COMPLETE")
    )

    curated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
        "s3a://de-lab/curated/orders_parquet"
    )

    curated.coalesce(1).write.mode("overwrite").option("header", True).csv(
        "s3a://de-lab/curated/orders_csv"
    )

    spark.read.parquet("s3a://de-lab/curated/orders_parquet").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
```

Run it:

```bash
spark-submit \
  --master "local[2]" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  jobs/localstack_s3_etl.py
```

Verify with LocalStack:

```bash
awslocal s3 ls s3://de-lab/curated/ --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 ls s3://your-real-bucket/curated/ --recursive
```

## 15. Optional: Simulate Delete And Update On S3 Files

S3 is object storage, not a database. Spark usually does not update one row in one CSV or Parquet file. Instead, it reads data, creates a new DataFrame, and writes replacement files.

Delete objects:

```bash
awslocal s3 rm s3://de-lab/curated/orders_csv/ --recursive

# Real AWS equivalent, shown for learning only:
# aws s3 rm s3://your-real-bucket/curated/orders_csv/ --recursive
```

Update data by rewrite:

```python
from pyspark.sql import functions as F

df = spark.read.parquet("s3a://de-lab/curated/orders_parquet")

updated = (
    df
    .withColumn(
        "status",
        F.when(F.col("order_id") == 2, F.lit("RETURNED")).otherwise(F.col("status")),
    )
)

updated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "s3a://de-lab/curated/orders_parquet_updated"
)
```

For production row-level `UPDATE`, `DELETE`, and `MERGE`, use a table format such as Apache Iceberg, Delta Lake, or Apache Hudi. AWS Glue works well with these table formats, but plain CSV and plain Parquet files in S3 do not behave like database tables.

## 16. AWS Glue Data Catalog Pointer

For this beginner path, focus on the AWS Glue Data Catalog first. It is the part SQL/database users most need to understand because it maps S3 data files to table metadata that tools such as Athena, Glue Spark, EMR, Redshift Spectrum, and Lake Formation can use.

Detailed notes are in:

```text
pyspark-101-examples.md
```

Read the sections:

- `AWS Glue Data Catalog`
- `Can Glue Do Create, Read, Update, Delete?`

## 17. Troubleshooting

`Java gateway process exited`

- Check `java -version`.
- Check `echo "$JAVA_HOME"`.
- Use Java 17 for this lab.

`No FileSystem for scheme s3a`

- Run `spark-submit` with the Hadoop AWS packages.
- Confirm the package versions are compatible with your Spark Hadoop version.

`Connection refused localhost:4566`

- Start LocalStack: `localstack start -d`.
- Check Docker Desktop is running.
- Check `localstack status services`.

`AccessDenied` or credential errors

- For LocalStack use `AWS_ACCESS_KEY_ID=test` and `AWS_SECRET_ACCESS_KEY=test`.
- Confirm `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`.

`CSV output has many part files`

- Spark writes distributed output.
- Use `coalesce(1)` only for tiny local examples.
- In production, many partitioned files are normal, but too many tiny files should be compacted.
