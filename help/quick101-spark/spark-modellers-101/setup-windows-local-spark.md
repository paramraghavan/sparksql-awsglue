# Windows Local Spark, AWS Glue, and LocalStack Setup

This guide helps SQL/database users run PySpark locally on Windows, practice AWS Glue-style ETL, and simulate S3 reads/writes with LocalStack.

The recommended path is Windows 11 with PowerShell and Python 3.11. If your company uses WSL2 heavily, you can also follow the Mac/Linux style setup inside Ubuntu on WSL2.

How to use this guide:

1. Complete sections 1-10 first. That gives you Java, Python, PySpark, sample input files, and a working local Spark job.
2. Then study `pyspark-101-examples.md` from this documentation folder while running its commands from `C:\spark-glue-local-lab`.
3. Complete the LocalStack sections only when you reach the LocalStack S3 examples in `pyspark-101-examples.md`.

LocalStack is optional for the first PySpark lessons. You can learn reads, filters, joins, aggregations, writes, partitions, and Parquet update-by-rewrite using only local files.

## 1. Recommended Versions

| Tool | Recommended | Why |
|---|---:|---|
| Windows | Windows 10 or 11 | Both can work; Windows 11 is smoother |
| Java | 17 | Good Spark 3.5.x choice |
| Python | 3.11 | Good Glue 5.x alignment |
| PySpark | 3.5.x | Practical local match for Glue 5.x |
| Docker Desktop | Current stable | Required only for optional LocalStack S3 practice |
| LocalStack | Current stable | Simulates AWS services locally |

References:

- AWS Glue release notes: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html
- AWS Glue version support: https://docs.aws.amazon.com/glue/latest/dg/glue-version-support-policy.html
- PySpark installation: https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html
- LocalStack S3 docs: https://docs.localstack.cloud/aws/services/s3/
- LocalStack Glue docs: https://docs.localstack.cloud/aws/services/glue/

## 2. Install Java 17

Install Temurin 17 from Adoptium:

```text
https://adoptium.net/temurin/releases/?version=17
```

Choose:

- Operating System: Windows
- Architecture: x64
- Package type: JDK
- Version: 17

During installation, enable the option to set `JAVA_HOME` if available.

Open a new PowerShell window and verify:

```powershell
java -version
echo $env:JAVA_HOME
```

Expected: Java 17.

If `JAVA_HOME` is missing, set it manually. Adjust the path if your installed folder is different:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17", "User")
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";%JAVA_HOME%\bin", "User")
```

Close and reopen PowerShell.

## 3. Install Python 3.11

Install Python 3.11 from:

```text
https://www.python.org/downloads/windows/
```

During installation, select:

- Add Python to PATH
- Install launcher for all users

Verify:

```powershell
py -3.11 --version
python --version
```

If `python` points to a different version, use `py -3.11` in the setup commands.

## 4. Create A Local Lab Folder

```powershell
New-Item -ItemType Directory -Force C:\spark-glue-local-lab
cd C:\spark-glue-local-lab
New-Item -ItemType Directory -Force data
New-Item -ItemType Directory -Force data\input
New-Item -ItemType Directory -Force data\output
New-Item -ItemType Directory -Force jobs
New-Item -ItemType Directory -Force warehouse
```

## 5. Create A Virtual Environment

```powershell
cd C:\spark-glue-local-lab
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
```

If PowerShell blocks activation scripts, run:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\.venv\Scripts\Activate.ps1
```

Install libraries:

```powershell
python -m pip install pyspark==3.5.6 pandas pyarrow boto3 awscli-local localstack jupyterlab ipykernel
```

For Glue 5.0 alignment, use:

```powershell
python -m pip install pyspark==3.5.4
```

Verify:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

## 6. Set Spark Environment Variables

In PowerShell:

```powershell
[Environment]::SetEnvironmentVariable("SPARK_LAB_HOME", "C:\spark-glue-local-lab", "User")
[Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark-glue-local-lab\.venv\Lib\site-packages\pyspark", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "C:\spark-glue-local-lab\.venv\Scripts\python.exe", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "C:\spark-glue-local-lab\.venv\Scripts\python.exe", "User")
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\spark-glue-local-lab\.venv\Lib\site-packages\pyspark", "User")
```

Add Spark scripts to your user `Path`:

```powershell
$sparkBin = "C:\spark-glue-local-lab\.venv\Lib\site-packages\pyspark\bin"
$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($currentPath -notlike "*$sparkBin*") {
    [Environment]::SetEnvironmentVariable("Path", "$currentPath;$sparkBin", "User")
}
```

Close and reopen PowerShell, then activate the virtual environment:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1
spark-submit --version
pyspark --version
```

## 7. Windows Hadoop Utility Note

Some Windows Spark setups need `winutils.exe` for local file permissions. Many simple PySpark 3.5 `pip` installs work without it, but if you see Hadoop permission or `winutils.exe` errors:

1. Create `C:\hadoop\bin`.
2. Add a compatible `winutils.exe` to `C:\hadoop\bin`.
3. Set `HADOOP_HOME=C:\hadoop`.
4. Add `C:\hadoop\bin` to `Path`.

For job interviews, you should understand why this exists: Spark uses Hadoop file APIs internally, even for local files.

## 8. Run A PySpark Smoke Test

```powershell
pyspark --master "local[2]"
```

Inside the shell:

```python
spark.range(5).show()
spark.sql("select current_date() as today").show()
spark.stop()
exit()
```

## 9. Create Input Files

Create `C:\spark-glue-local-lab\data\input\orders.csv`:

```csv
order_id,customer_id,order_date,status,category,amount
1,C001,2026-01-01,COMPLETE,books,35.50
2,C002,2026-01-01,COMPLETE,electronics,299.99
3,C001,2026-01-02,CANCELLED,books,15.00
4,C003,2026-01-02,COMPLETE,grocery,42.25
5,C002,2026-01-03,COMPLETE,electronics,99.99
6,C004,2026-01-03,RETURNED,grocery,18.75
```

Create `C:\spark-glue-local-lab\data\input\customers.json`:

```json
{"customer_id":"C001","customer_name":"Asha","state":"CA","segment":"retail"}
{"customer_id":"C002","customer_name":"Ben","state":"NY","segment":"business"}
{"customer_id":"C003","customer_name":"Cara","state":"TX","segment":"retail"}
{"customer_id":"C004","customer_name":"Dev","state":"WA","segment":"retail"}
```

PowerShell alternative:

```powershell
@'
order_id,customer_id,order_date,status,category,amount
1,C001,2026-01-01,COMPLETE,books,35.50
2,C002,2026-01-01,COMPLETE,electronics,299.99
3,C001,2026-01-02,CANCELLED,books,15.00
4,C003,2026-01-02,COMPLETE,grocery,42.25
5,C002,2026-01-03,COMPLETE,electronics,99.99
6,C004,2026-01-03,RETURNED,grocery,18.75
'@ | Set-Content -Path data\input\orders.csv

@'
{"customer_id":"C001","customer_name":"Asha","state":"CA","segment":"retail"}
{"customer_id":"C002","customer_name":"Ben","state":"NY","segment":"business"}
{"customer_id":"C003","customer_name":"Cara","state":"TX","segment":"retail"}
{"customer_id":"C004","customer_name":"Dev","state":"WA","segment":"retail"}
'@ | Set-Content -Path data\input\customers.json
```

## 10. Create A Basic Spark Submit Job

Create `C:\spark-glue-local-lab\jobs\orders_etl.py`:

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
        .appName("orders-etl-local-windows")
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
        .where(F.col("is_successful"))
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

Run:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1

spark-submit --master "local[2]" --name "orders-etl-local-windows" jobs\orders_etl.py
```

This first job does not use LocalStack. It writes to your local computer because the output path is built from:

```python
output_dir = root / "data" / "output" / "orders_etl"
```

If your lab folder is `C:\spark-glue-local-lab`, then the Parquet output is written to:

```text
C:\spark-glue-local-lab\data\output\orders_etl\orders_parquet\
```

The CSV summary is written to:

```text
C:\spark-glue-local-lab\data\output\orders_etl\summary_csv\
```

Spark writes folders containing `part-*` files, not one single file. LocalStack starts later in this guide; LocalStack paths begin with `s3a://de-lab/...` or use `awslocal s3 ...`.

Verify:

```powershell
Get-ChildItem -Recurse data\output\orders_etl
```

## 11. Next Step: Study The PySpark 101 Examples

At this point your local Spark setup is ready.

Use this file next:

```text
pyspark-101-examples.md
```

Run the examples from your lab folder:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1
```

Start with the local file examples. Come back to the next sections in this setup guide when you need LocalStack S3.

## 12. Optional: Install Docker Desktop For LocalStack

Install Docker Desktop:

```text
https://www.docker.com/products/docker-desktop/
```

Recommended Docker Desktop settings:

- Use WSL2 based engine if available.
- Allocate at least 4 GB memory.
- Start Docker Desktop before LocalStack.

Verify:

```powershell
docker --version
docker ps
```

## 13. Optional: Start LocalStack

LocalStack is a local AWS simulator. For this lab, assume Docker is acting like a small local AWS account running on your laptop.

When LocalStack starts, it runs inside Docker and exposes AWS-like services at:

```text
http://localhost:4566
```

In this guide:

- `awslocal s3 ...` talks to LocalStack S3, not real AWS S3.
- `s3a://de-lab/...` paths write to the LocalStack bucket named `de-lab`.
- LocalStack data lives inside Docker-managed storage, not inside `data\output`.
- Use `awslocal s3 ls ...` to inspect what Spark wrote to LocalStack.
- Use `awslocal s3 cp ... --recursive` to download LocalStack output into your local project folder.

Mental model:

```text
data\output\...       -> normal folder on your computer
s3a://de-lab/...      -> fake/local S3 bucket running in Docker through LocalStack
```

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1
localstack start -d
localstack status services
```

Set local AWS variables:

```powershell
[Environment]::SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "test", "User")
[Environment]::SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "test", "User")
[Environment]::SetEnvironmentVariable("AWS_DEFAULT_REGION", "us-east-1", "User")
[Environment]::SetEnvironmentVariable("AWS_ENDPOINT_URL", "http://localhost:4566", "User")
```

Set them in the current shell too:

```powershell
$env:AWS_ACCESS_KEY_ID="test"
$env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"
$env:AWS_ENDPOINT_URL="http://localhost:4566"
```

Create a bucket:

```powershell
awslocal s3 mb s3://de-lab
awslocal s3 ls
```

Upload files:

```powershell
awslocal s3 cp data\input\orders.csv s3://de-lab/raw/orders/orders.csv
awslocal s3 cp data\input\customers.json s3://de-lab/raw/customers/customers.json
awslocal s3 ls s3://de-lab/raw/ --recursive
```

To download anything written to LocalStack S3 back to your local folder:

```powershell
New-Item -ItemType Directory -Force data\downloaded\orders_parquet

awslocal s3 cp `
  s3://de-lab/curated/orders_parquet/ `
  data\downloaded\orders_parquet\ `
  --recursive
```

## 14. Optional: Read And Write LocalStack S3 From Spark

Create `jobs\localstack_s3_etl.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("localstack-s3-etl-windows")
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

Run:

```powershell
spark-submit `
  --master "local[2]" `
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  jobs\localstack_s3_etl.py
```

Verify:

```powershell
awslocal s3 ls s3://de-lab/curated/ --recursive
```

## 15. Optional: Simulate Delete And Update

Delete CSV output:

```powershell
awslocal s3 rm s3://de-lab/curated/orders_csv/ --recursive
```

Update a Parquet dataset by rewriting it. In Spark, this is the common plain-file pattern:

```python
from pyspark.sql import functions as F

df = spark.read.parquet("s3a://de-lab/curated/orders_parquet")

updated = df.withColumn(
    "status",
    F.when(F.col("order_id") == 2, F.lit("RETURNED")).otherwise(F.col("status")),
)

updated.write.mode("overwrite").partitionBy("order_year", "category").parquet(
    "s3a://de-lab/curated/orders_parquet_updated"
)
```

Important idea for SQL users:

- Spark plus S3 is not the same as PostgreSQL, SQL Server, or Oracle.
- A CSV or Parquet folder is a dataset, not an OLTP table.
- For row-level `UPDATE`, `DELETE`, and `MERGE`, use Apache Iceberg, Delta Lake, or Apache Hudi.
- Glue can run jobs that use those table formats, but Glue by itself does not make plain S3 files behave like database rows.

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

`spark-submit is not recognized`

- Reopen PowerShell after changing environment variables.
- Check `echo $env:SPARK_HOME`.
- Check that `%SPARK_HOME%\bin` is on the user `Path`.

`Java gateway process exited`

- Check `java -version`.
- Check `echo $env:JAVA_HOME`.
- Use Java 17.

`No FileSystem for scheme s3a`

- Use the `--packages` argument shown above.
- Check your internet connection for the first run because Spark downloads packages.

`Connection refused localhost:4566`

- Start Docker Desktop.
- Run `localstack start -d`.
- Check `localstack status services`.

PowerShell script activation is blocked

- Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`.
