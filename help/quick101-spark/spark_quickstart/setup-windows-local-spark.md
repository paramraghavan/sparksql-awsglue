# Windows Local Spark, AWS Glue, and LocalStack Setup

This guide sets up PySpark on Windows so you can practice Spark, then optionally adds a fake AWS account (LocalStack) so you can practice S3 and Glue.

All commands in this guide use PowerShell. Open PowerShell from the Start menu. Do not use Command Prompt.

The guide has three parts:

| Part | What you get | Required? |
|---|---|---|
| Part 1: Local Spark | Java, Python, PySpark, and a working Spark job | Yes |
| Part 2: LocalStack | A fake AWS account in Docker with S3 and Glue | Optional, needed for the S3 examples |
| Part 3: Troubleshooting | Common errors and fixes | Use when something breaks |

Do Part 1 first. Come back to Part 2 when you reach the LocalStack S3 examples in `pyspark-101-examples.md`.

## Versions Used

| Tool | Version | Why |
|---|---|---|
| Windows | 10 or 11 | Windows 11 is smoother |
| Java | 17 | Works with Spark 3.5.x |
| Python | 3.11 | Matches AWS Glue 5.x |
| PySpark | 3.5.6 | Close to AWS Glue 5.x |
| Docker Desktop | Current stable | Runs LocalStack |

References:

- AWS Glue release notes: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html
- PySpark installation: https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html
- LocalStack S3 docs: https://docs.localstack.cloud/aws/services/s3/
- LocalStack Glue docs: https://docs.localstack.cloud/aws/services/glue/

---

# Part 1: Local Spark (Required)

## Step 1: Install Java 17

1. Download Temurin 17 (JDK) for Windows x64 from:

   ```text
   https://adoptium.net/temurin/releases/?version=17
   ```

2. Run the installer. If it offers to set `JAVA_HOME`, turn that option on.
3. Close PowerShell and open a new window. Check:

   ```powershell
   java -version
   echo $env:JAVA_HOME
   ```

   You should see Java 17.

If `JAVA_HOME` is empty, set it yourself. Change the path if your install folder is different:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17", "User")
$javaBin = "$env:JAVA_HOME\bin"
[Environment]::SetEnvironmentVariable("Path", "$([Environment]::GetEnvironmentVariable('Path', 'User'));$javaBin", "User")
```

Close and reopen PowerShell after this step.

## Step 2: Install Python 3.11

1. Download Python 3.11 from `https://www.python.org/downloads/windows/`.
2. In the installer, select **Add python.exe to PATH** and **Install launcher for all users**.
3. Check:

   ```powershell
   py -3.11 --version
   ```

If `python` points to a different version, use `py -3.11` in every command in this guide.

## Step 3: Create The Lab Folder And Virtual Environment

```powershell
New-Item -ItemType Directory -Force C:\spark-glue-local-lab\data\input, C:\spark-glue-local-lab\data\output, C:\spark-glue-local-lab\jobs
cd C:\spark-glue-local-lab
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
```

If PowerShell blocks the activation script, allow local scripts once, then activate again:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\.venv\Scripts\Activate.ps1
```

Every new PowerShell window needs the virtual environment turned on. Run these two lines first:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1
```

## Step 4: Install PySpark And The Python Libraries

```powershell
python -m pip install pyspark==3.5.6 pandas pyarrow boto3 awscli-local jupyterlab ipykernel
```

`awscli-local` gives you the `awslocal` command, which you use in Part 2.

Check that PySpark installed:

```powershell
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
```

If your company uses AWS Glue 5.0 specifically, use `pyspark==3.5.4` instead.

## Step 5: Set The Spark Environment Variables

Run these once. They are saved for your Windows user:

```powershell
[Environment]::SetEnvironmentVariable("SPARK_LAB_HOME", "C:\spark-glue-local-lab", "User")
[Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark-glue-local-lab\.venv\Lib\site-packages\pyspark", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "C:\spark-glue-local-lab\.venv\Scripts\python.exe", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "C:\spark-glue-local-lab\.venv\Scripts\python.exe", "User")
```

Add the Spark scripts folder to your `Path`:

```powershell
$sparkBin = "C:\spark-glue-local-lab\.venv\Lib\site-packages\pyspark\bin"
$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($currentPath -notlike "*$sparkBin*") {
    [Environment]::SetEnvironmentVariable("Path", "$currentPath;$sparkBin", "User")
}
```

Close PowerShell, open a new window, and check:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1
spark-submit --version
```

If you see `spark-submit is not recognized`, see the troubleshooting section.

## Step 6: Run A PySpark Smoke Test

```powershell
pyspark --master "local[2]"
```

Inside the PySpark shell, run:

```python
spark.range(5).show()
spark.sql("select current_date() as today").show()
spark.stop()
exit()
```

`local[2]` means Spark runs with two worker threads on your PC.

## Step 7: Hadoop Utility Note (Only If Needed)

Some Windows Spark setups need `winutils.exe` for local file permissions. Many simple PySpark 3.5 installs from `pip` work without it. Use this step only if you see errors that mention `winutils.exe` or Hadoop permissions:

1. Create the folder `C:\hadoop\bin`.
2. Add a `winutils.exe` that matches Hadoop 3.3.x to `C:\hadoop\bin`.
3. Set `HADOOP_HOME` to `C:\hadoop`.
4. Add `C:\hadoop\bin` to your `Path`.

Spark uses Hadoop file APIs internally, even for local files, which is why this utility sometimes matters.

## Step 8: Create The Sample Input Files

Run this in PowerShell from `C:\spark-glue-local-lab`:

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

## Step 9: Run Your First Spark Job

Create `C:\spark-glue-local-lab\jobs\orders_etl.py` with this content:

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

Run it:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1

spark-submit --master "local[2]" --name "orders-etl-local-windows" jobs\orders_etl.py
```

This job reads and writes only your PC's folders. The results go to:

- `C:\spark-glue-local-lab\data\output\orders_etl\orders_parquet\`
- `C:\spark-glue-local-lab\data\output\orders_etl\summary_csv\`

Spark writes folders with `part-*` files, not one file. Check the output:

```powershell
Get-ChildItem -Recurse data\output\orders_etl
```

## Step 10: Continue With The PySpark 101 Examples

Your local Spark setup is ready. Open `pyspark-101-examples.md` next. Run the examples from `C:\spark-glue-local-lab` with the virtual environment on.

---

# Part 2: LocalStack In Docker (Optional)

LocalStack is a local AWS simulator. It runs as a Docker container and listens on `http://localhost:4566`. Your Spark jobs and `awslocal` commands send their S3 requests to that address, so you can practice S3 without a real AWS account.

```text
data\output\...   -> a normal folder on your PC
s3a://de-lab/...  -> a fake S3 bucket inside the LocalStack container
```

LocalStack is not a Python library. The only Python package this part needs is `awscli-local`, which you installed in Step 4.

Recent LocalStack versions need a free auth token before they start. Get the token first (Step 12).

## Step 11: Start Docker Desktop

1. Install Docker Desktop from `https://www.docker.com/products/docker-desktop/`.
2. Use the WSL 2 engine if the installer offers it. Give Docker at least 4 GB of memory.
3. Open Docker Desktop and wait until it says it is running.
4. Check from PowerShell:

   ```powershell
   docker ps
   ```

   If this shows a table, even an empty one, Docker is ready.

## Step 12: Get A Free LocalStack Auth Token

1. Sign up at `https://app.localstack.cloud/sign-up?plan=free`, or sign in if you have an account.
2. Find your auth token on the account page and copy it.
3. Keep the token private. Do not paste it into this guide, a repository, or chat.
4. Set the token in your PowerShell window. Replace the placeholder with your token:

   ```powershell
   $env:LOCALSTACK_AUTH_TOKEN = "paste-your-token-here"
   ```

   This setting lasts only for the current PowerShell window. Run it again in each new window.

Check that the token is set. This prints only the length, not the token:

```powershell
$env:LOCALSTACK_AUTH_TOKEN.Length
```

A number greater than 0 means the token is set. An empty result means it is not set in this window.

## Step 13: Start The LocalStack Container

Run this in the same PowerShell window where you set the token. The first run downloads the image, which can take a few minutes:

```powershell
docker run -d `
  --name localstack-main `
  -e "LOCALSTACK_AUTH_TOKEN=$env:LOCALSTACK_AUTH_TOKEN" `
  -p 127.0.0.1:4566:4566 `
  -p 127.0.0.1:4510-4559:4510-4559 `
  localstack/localstack
```

The backtick at the end of each line continues the command in PowerShell.

Use the image named exactly `localstack/localstack`. Similar names such as `localstack-light`, `localstack-full`, `localstack-pro`, and the Lambda or Snowflake images are different.

## Step 14: Check That LocalStack Is Running

Check the container:

```powershell
docker ps --filter name=localstack-main
docker logs localstack-main
```

The container should show `Up`. The logs show the services starting.

Check the endpoint:

```powershell
Invoke-RestMethod http://localhost:4566/_localstack/health
```

The response lists the services, including `s3` and `glue`.

If the container exits, the logs explain why. The most common cause is a missing token. The message is `License activation failed ... No credentials were found`. Go back to Step 12, set the token in the same PowerShell window, and run the `docker run` command again.

### LocalStack Service URLs

All AWS services share one address, `http://localhost:4566`. You do not need a separate port for each service. The AWS CLI and Spark pick the service from the command or the request, not from the URL.

| What | URL or command | Use it for |
|---|---|---|
| Base endpoint | `http://localhost:4566` | Address every AWS call uses |
| Health | `http://localhost:4566/_localstack/health` | See which services are running |
| Info | `http://localhost:4566/_localstack/info` | See the LocalStack version |
| S3 object (path style) | `http://localhost:4566/de-lab/raw/orders/orders.csv` | Fetch an uploaded file, once the bucket exists |
| S3 bucket list | `awslocal s3 ls` | List buckets |
| Glue | `awslocal glue get-databases` | List Glue databases (Glue has no browser page) |

The `s3a://de-lab/...` paths in Spark use the same endpoint through the `spark.hadoop.fs.s3a.endpoint` setting from Step 17. The `path.style.access` setting makes Spark put the bucket name in the URL path, which is the form in the table above.

Check which services are running:

```powershell
Invoke-RestMethod http://localhost:4566/_localstack/health | ConvertTo-Json -Depth 5
```

Check the LocalStack version:

```powershell
Invoke-RestMethod http://localhost:4566/_localstack/info | ConvertTo-Json -Depth 5
```

Fetch an uploaded file. Run this after Step 16 uploads it:

```powershell
curl.exe -s http://localhost:4566/de-lab/raw/orders/orders.csv
```

List the Glue databases:

```powershell
awslocal glue get-databases
```

## Step 15: Set The AWS Environment Variables

These values are fake and only work with LocalStack. Run this once. They are saved for your Windows user:

```powershell
[Environment]::SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "test", "User")
[Environment]::SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "test", "User")
[Environment]::SetEnvironmentVariable("AWS_DEFAULT_REGION", "us-east-1", "User")
[Environment]::SetEnvironmentVariable("AWS_ENDPOINT_URL", "http://localhost:4566", "User")
```

Close and reopen PowerShell so the new values load.

## Step 16: Create The Bucket And Upload The Input Files

Activate the lab environment, then create the `de-lab` bucket:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1

awslocal s3 mb s3://de-lab
awslocal s3 ls
```

Upload the input files:

```powershell
awslocal s3 cp data\input\orders.csv s3://de-lab/raw/orders/orders.csv
awslocal s3 cp data\input\customers.json s3://de-lab/raw/customers/customers.json
awslocal s3 ls s3://de-lab/raw/ --recursive
```

To copy anything from LocalStack back to your PC:

```powershell
New-Item -ItemType Directory -Force data\downloaded\orders_parquet

awslocal s3 cp `
  s3://de-lab/curated/orders_parquet/ `
  data\downloaded\orders_parquet\ `
  --recursive
```

## Step 17: Read And Write S3 From Spark

Spark needs the Hadoop AWS libraries to read `s3a://` paths. Spark downloads them when you pass `--packages`. The first run can take a while.

Create `C:\spark-glue-local-lab\jobs\localstack_s3_etl.py` with this content:

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

Run it:

```powershell
spark-submit `
  --master "local[2]" `
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
  jobs\localstack_s3_etl.py
```

Check the output in LocalStack:

```powershell
awslocal s3 ls s3://de-lab/curated/ --recursive
```

## Step 18: Stop, Restart, Or Remove LocalStack

Stop the container when you finish:

```powershell
docker stop localstack-main
```

Start it again later. Set the token in the new PowerShell window first, then start the container:

```powershell
$env:LOCALSTACK_AUTH_TOKEN = "paste-your-token-here"
docker start localstack-main
```

To delete the container and everything stored in it:

```powershell
docker rm -f localstack-main
```

Download anything you need from LocalStack S3 before you delete the container.

## Lesson: Update Data On S3 By Rewriting It

S3 is object storage, not a database. Spark does not change one row inside a CSV or Parquet file. It reads the data, builds a new DataFrame, and writes replacement files.

Delete a folder:

```powershell
awslocal s3 rm s3://de-lab/curated/orders_csv/ --recursive
```

Update by rewrite, in the PySpark shell or a job:

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

For row-level `UPDATE`, `DELETE`, and `MERGE` in production, use a table format such as Apache Iceberg, Delta Lake, or Apache Hudi. Plain CSV and Parquet folders in S3 do not behave like database tables.

## Lesson: AWS Glue Data Catalog

The Glue Data Catalog maps S3 files to table definitions. Tools such as Athena, Glue Spark jobs, and EMR use it to find and read the data. Read these sections in `pyspark-101-examples.md`:

- `AWS Glue Data Catalog`
- `Can Glue Do Create, Read, Update, Delete?`

---

# Part 3: Troubleshooting

**`spark-submit is not recognized`**

- Close and reopen PowerShell after you change environment variables.
- Check `echo $env:SPARK_HOME`. It should point to the `pyspark` folder in `.venv`.
- Check that the Spark `bin` folder is on your user `Path`, as shown in Step 5.

**`Java gateway process exited`**

- Run `java -version`. You need Java 17.
- Run `echo $env:JAVA_HOME`. It should point to the Java 17 folder.

**`No FileSystem for scheme s3a`**

- Run `spark-submit` with the `--packages` option from Step 17.
- The first run needs an internet connection so Spark can download the packages.

**`awslocal` is not recognized (or `command not found`)**

- `awslocal` is installed only inside the lab's virtual environment. Turn the environment on first:

  ```powershell
  cd C:\spark-glue-local-lab
  .\.venv\Scripts\Activate.ps1
  awslocal --version
  ```

- Run `.\.venv\Scripts\Activate.ps1` again in every new PowerShell window before you use `awslocal`.
- If `awslocal` is still missing after you activate the environment, run `python -m pip install awscli-local` inside the environment.

**`Connection refused localhost:4566`**

- LocalStack is not running. Check `docker ps --filter name=localstack-main`.
- If the container is missing, run the command in Step 13.
- Check that Docker Desktop is running.

**The container exits with code 55 and mentions `LOCALSTACK_AUTH_TOKEN`**

- The token is missing from the PowerShell window where you ran `docker run`. Check it with `$env:LOCALSTACK_AUTH_TOKEN.Length`. Go back to Step 12.

**PowerShell says script activation is blocked**

- Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`, then activate again.

**Hadoop permission or `winutils.exe` errors**

- Follow Step 7.

**`AccessDenied` or credential errors**

- Use `test` for the access key and secret key with LocalStack.
- Check `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` in your job.

---

# Next: Spark Monitoring And Hadoop

Follow the [Spark UI, History Server, and YARN teaching lab](spark-ui-history-server-lab.md) to watch jobs in the Spark UI. No LocalStack is needed for that lab.

For a separate Hadoop cluster, follow the [self-contained Hadoop/YARN installation lab](setup-hadoop-yarn-local-lab.md).
