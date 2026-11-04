# Windows Local Spark, AWS Glue, and LocalStack Setup

This guide sets up PySpark on Windows so you can practice Spark, then optionally adds a fake AWS account (LocalStack) so you can practice S3 and Glue.

All commands in this guide use PowerShell. Open PowerShell from the Start menu. Do not use Command Prompt.

The guide has three parts:

| Part | What you get | Required? |
|---|---|---|
| Part 1: Local Spark | Java, Python, PySpark, and a working Spark job | Yes |
| Part 2: LocalStack | A fake AWS account, started with the `lstk` CLI | Optional, needed for the S3 examples |
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
| `lstk` | Current | Starts LocalStack and runs AWS commands against it |

References:

- AWS Glue release notes: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html
- PySpark installation: https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html
- LocalStack `lstk` docs: https://docs.localstack.cloud/aws/developer-tools/running-localstack/lstk/
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
python -m pip install pyspark==3.5.6 pandas pyarrow boto3 jupyterlab ipykernel
```

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

# Part 2: LocalStack (Optional)

LocalStack is a local AWS simulator. It runs in a Docker container and listens on `http://localhost:4566`. Your Spark jobs and AWS commands send their requests to that address, so you can practice S3 and Glue without a real AWS account.

```text
data\output\...   -> a normal folder on your PC
s3a://de-lab/...  -> a fake S3 bucket inside the LocalStack container
```

You use the `lstk` command to start and manage LocalStack, and `aws` with the `localstack` profile to run AWS commands against it.

Note: the older `awslocal` command is deprecated. Use `lstk aws` instead, as shown below.

## Step 11: Start Docker Desktop

1. Install Docker Desktop from `https://www.docker.com/products/docker-desktop/`.
2. Use the WSL 2 engine if the installer offers it. Give Docker at least 4 GB of memory.
3. Open Docker Desktop and wait until it says it is running.
4. Check from PowerShell:

   ```powershell
   docker ps
   ```

   If this shows a table, even an empty one, Docker is ready.

## Step 12: Install lstk And The AWS CLI

Install the AWS CLI from the MSI installer:

```text
https://awscli.amazonaws.com/AWSCLIV2.msi
```

Install `lstk` by following the Windows instructions in the LocalStack docs:

```text
https://docs.localstack.cloud/aws/developer-tools/running-localstack/lstk/
```

Close PowerShell and open a new window. Check both:

```powershell
lstk --version
aws --version
```

## Step 13: Log In With lstk

```powershell
lstk login
```

Follow the prompts. If you do not have an account yet, sign up for free at `https://app.localstack.cloud/sign-up?plan=free`. `lstk` stores your credentials in Windows, so you do not need to paste the token anywhere in this guide.

Recent LocalStack versions need this login before they start.

## Step 14: Start LocalStack

```powershell
lstk start
lstk status
```

`lstk start` runs the LocalStack container for you. `lstk status` shows whether it is running and which resources exist.

Stop and restart it with:

```powershell
lstk stop
lstk restart
```

## Step 15: Set Up The LocalStack AWS Profile

```powershell
lstk setup aws
```

This adds a `localstack` profile to your AWS config and credentials files. The profile points AWS commands at LocalStack instead of real AWS. Run the command again with `--force` if you need to overwrite an existing `localstack` profile.

## Step 16: Check That LocalStack Is Running

Check the services:

```powershell
lstk status
Invoke-RestMethod http://localhost:4566/_localstack/health | ConvertTo-Json -Depth 5
```

The health response lists the services. The free plan includes `s3`. Glue needs a paid LocalStack license, so this lab uses Spark's SQL catalog for table metadata instead (see Table Metadata Without A Glue License after Step 18).

### LocalStack Service URLs

All AWS services share one address, `http://localhost:4566`. You do not need a separate port for each service. The AWS CLI and Spark pick the service from the command or the request, not from the URL.

| What | URL or command | Use it for |
|---|---|---|
| Base endpoint | `http://localhost:4566` | Address every AWS call uses |
| Health | `http://localhost:4566/_localstack/health` | See which services are running |
| Info | `http://localhost:4566/_localstack/info` | See the LocalStack version |
| S3 object (path style) | `http://localhost:4566/de-lab/raw/orders/orders.csv` | Fetch an uploaded file, once the bucket exists |
| S3 bucket list | `lstk aws s3 ls` | List buckets |
| Glue | Not on the free plan | Use the Spark SQL catalog after Step 18 |

The `s3a://de-lab/...` paths in Spark use the same endpoint through the `spark.hadoop.fs.s3a.endpoint` setting in Step 18. The `path.style.access` setting makes Spark put the bucket name in the URL path, which is the form in the table above.

Check the LocalStack version:

```powershell
Invoke-RestMethod http://localhost:4566/_localstack/info | ConvertTo-Json -Depth 5
```

## Step 17: Create The Bucket And Upload The Input Files

Activate the lab environment, then create the `de-lab` bucket:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1

lstk aws s3 mb s3://de-lab
lstk aws s3 ls
```

Upload the input files:

```powershell
lstk aws s3 cp data\input\orders.csv s3://de-lab/raw/orders/orders.csv
lstk aws s3 cp data\input\customers.json s3://de-lab/raw/customers/customers.json
lstk aws s3 ls s3://de-lab/raw/ --recursive
```

Fetch an uploaded file to check the URL pattern:

```powershell
curl.exe -s http://localhost:4566/de-lab/raw/orders/orders.csv
```

To copy anything from LocalStack back to your PC:

```powershell
New-Item -ItemType Directory -Force data\downloaded\orders_parquet

lstk aws s3 cp `
  s3://de-lab/curated/orders_parquet/ `
  data\downloaded\orders_parquet\ `
  --recursive
```

## Step 18: Read And Write S3 From Spark

Spark needs the Hadoop AWS libraries to read `s3a://` paths. Spark downloads them when you pass `--packages`. The first run can take a while.

**Why `s3a://` and not `s3://`?** On EMR, `s3://` works because EMR ships its own S3 driver (EMRFS) and the AWS libraries. Open-source Spark on your PC has no `s3://` driver. The open-source option is `s3a://`, which comes from the `hadoop-aws` jar and its AWS SDK dependency, `aws-java-sdk-bundle`. Without those jars, Spark fails with `No FileSystem for scheme`. The `--packages` option downloads both jars. The `spark.hadoop.fs.s3a.endpoint` setting then points Spark at LocalStack instead of AWS. EMRFS needs no endpoint setting because it always talks to AWS.

**The same session on EMR.** The transformation code stays the same. The session setup and the paths change:

| Setting | This lab (LocalStack) | EMR |
|---|---|---|
| Creating the session | `SparkSession.builder` with `master("local[2]")` | `SparkSession.builder.getOrCreate()`. Some EMR notebooks create `spark` for you |
| Hadoop AWS jars | `--packages` downloads them | Already on the cluster, so there is no `--packages` |
| S3 scheme | `s3a://de-lab/...` | `s3://your-real-bucket/...` (EMRFS) |
| Endpoint | `spark.hadoop.fs.s3a.endpoint` = `http://localhost:4566` | Not set, because EMR talks to AWS |
| Credentials | `test` / `test` access keys | IAM role attached to the cluster or job |
| Path style | `path.style.access` = `true` | Not needed |
| Table metadata | Spark SQL catalog table over the S3 folder | `spark.table("curated.orders_raw")`, if the cluster uses the Glue Data Catalog as its metastore |

The same job on EMR looks like this:

```python
spark = SparkSession.builder.appName("orders-on-emr").getOrCreate()

orders = spark.read.option("header", True).csv("s3://your-real-bucket/raw/orders/orders.csv")
orders_from_glue = spark.table("curated.orders_raw")
```

The exact setup depends on how your team configures the cluster, so check with your platform team before you rely on the Glue table lookup.

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
lstk aws s3 ls s3://de-lab/curated/ --recursive
```

## Step 19: Stop Or Remove LocalStack

Stop LocalStack when you finish:

```powershell
lstk stop
```

Start it again later with `lstk start`.

Download anything you need from LocalStack S3 before you remove it. Check the `lstk` help for how to remove the emulator and its data:

```powershell
lstk --help
```

### Remove The LocalStack AWS Profile (Optional)

`lstk` has no command that deletes the `localstack` profile. `lstk logout` removes only your login credentials. To remove the profile, edit two files by hand. Both are in your user folder, `%USERPROFILE%\.aws`.

1. Back up both files first:

   ```powershell
   Copy-Item $HOME\.aws\config $HOME\.aws\config.bak
   Copy-Item $HOME\.aws\credentials $HOME\.aws\credentials.bak
   ```

2. Open `$HOME\.aws\config` in Notepad and delete the `[profile localstack]` block:

   ```powershell
   notepad $HOME\.aws\config
   ```

3. Open `$HOME\.aws\credentials` in Notepad and delete the `[localstack]` block:

   ```powershell
   notepad $HOME\.aws\credentials
   ```

4. Check that the profile is gone:

   ```powershell
   aws configure list-profiles
   ```

   `localstack` should not appear in the list. To set it up again, run `lstk setup aws`.

## Lesson: Update Data On S3 By Rewriting It

S3 is object storage, not a database. Spark does not change one row inside a CSV or Parquet file. It reads the data, builds a new DataFrame, and writes replacement files.

Delete a folder:

```powershell
lstk aws s3 rm s3://de-lab/curated/orders_csv/ --recursive
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

### Table Metadata Without A Glue License

LocalStack's Glue service needs a paid license, so this lab uses Spark's built-in SQL catalog for table metadata. Run these lines in a PySpark session, for example `pyspark --master "local[2]"` with the S3 settings from Step 18:

```python
spark.sql("CREATE DATABASE IF NOT EXISTS curated")

spark.sql("""
CREATE TABLE IF NOT EXISTS curated.orders_raw (
  order_id INT,
  customer_id STRING,
  order_date STRING,
  status STRING,
  category STRING,
  amount DOUBLE
)
USING csv
OPTIONS (header 'true')
LOCATION 's3a://de-lab/raw/orders/'
""")

spark.sql("SELECT status, COUNT(*) AS order_count FROM curated.orders_raw GROUP BY status").show()
```

The table is metadata only. The data stays in S3. Spark keeps this catalog in the current session, so run the `CREATE` lines again after you restart Spark. Point `LOCATION` at a folder that holds only this dataset.

## Lesson: AWS Glue Data Catalog

The Glue Data Catalog maps S3 files to table definitions. Tools such as Athena, Glue Spark jobs, and EMR use it to find and read the data. Spark's SQL catalog does the same job for practice, as shown above.

LocalStack's Glue service needs a paid license, so the `lstk aws glue ...` commands are not part of this free lab. Read these sections in `pyspark-101-examples.md`:

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

- Run `spark-submit` with the `--packages` option from Step 18.
- The first run needs an internet connection so Spark can download the packages.

**`awslocal` is deprecated, or `lstk` is not recognized**

- Use `lstk aws` instead of `awslocal`, for example `lstk aws s3 ls`.
- If `lstk` is not recognized, install it again following the Step 12 link, then open a new PowerShell window.

**`No AWS profile found`**

- Run `lstk setup aws` (Step 15).

**`License activation failed` or the container exits with code 55**

- Run `lstk login` (Step 13), then `lstk start` again.

**`Table or view not found: curated.orders_raw`**

- Spark's catalog lives only in the current session. Run the `CREATE` lines from the Table Metadata section again.

**`Glue service is not included in your LocalStack license`**

- Glue needs a paid LocalStack license. Use the Spark SQL catalog instead.

**`Connection refused localhost:4566`**

- LocalStack is not running. Run `lstk status`, then `lstk start` if needed.
- Check that Docker Desktop is running with `docker ps`.

**PowerShell says script activation is blocked**

- Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`, then activate again.

**Hadoop permission or `winutils.exe` errors**

- Follow Step 7.

**`AccessDenied` or credential errors**

- Use `test` for the access key and secret key in the Spark job (Step 18).
- Check `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`.

---

# Next: Spark Monitoring And Hadoop

Follow the [Spark UI, History Server, and YARN teaching lab](spark-ui-history-server-lab.md) to watch jobs in the Spark UI. No LocalStack is needed for that lab.

For a separate Hadoop cluster, follow the [self-contained Hadoop/YARN installation lab](setup-hadoop-yarn-local-lab.md).
