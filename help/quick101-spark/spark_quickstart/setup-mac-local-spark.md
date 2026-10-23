# Mac Local Spark, AWS Glue, and LocalStack Setup

This guide sets up PySpark on your Mac so you can practice Spark, then optionally adds a fake AWS account (LocalStack) so you can practice S3 and Glue.

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

## Step 1: Install Homebrew

Skip this step if `brew --version` already works.

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Then add Homebrew to your shell. Use the command for your Mac:

```bash
# Apple Silicon Mac
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc

# Intel Mac
echo 'eval "$(/usr/local/bin/brew shellenv)"' >> ~/.zshrc
```

Open a new terminal, then check:

```bash
brew --version
```

## Step 2: Install Java 17

```bash
brew install openjdk@17
```

Set `JAVA_HOME` once. Use the path for your Mac:

```bash
# Apple Silicon Mac
cat >> ~/.zshrc <<'EOF'
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
EOF

# Intel Mac
cat >> ~/.zshrc <<'EOF'
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
EOF
```

Open a new terminal, then check:

```bash
java -version
echo "$JAVA_HOME"
```

You should see Java 17.

## Step 3: Install Python 3.11

```bash
brew install python@3.11
python3.11 --version
```

## Step 4: Create The Lab Folder And Virtual Environment

```bash
mkdir -p ~/spark-glue-local-lab/{data/input,data/output,jobs}
cd ~/spark-glue-local-lab
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Every new terminal needs the virtual environment turned on. Run this from the lab folder:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
```

## Step 5: Install PySpark And The Python Libraries

```bash
python -m pip install \
  "pyspark==3.5.6" \
  pandas \
  pyarrow \
  boto3 \
  awscli-local \
  jupyterlab \
  ipykernel
```

`awscli-local` gives you the `awslocal` command, which you use in Part 2.

Check that PySpark installed:

```bash
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
```

If your company uses AWS Glue 5.0 specifically, use `pyspark==3.5.4` instead.

## Step 6: Set The Spark Environment Variables

Add these lines to `~/.zshrc` once:

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

Check that Spark is found:

```bash
which spark-submit
spark-submit --version
```

## Step 7: Run A PySpark Smoke Test

```bash
pyspark --master "local[2]"
```

Inside the PySpark shell, run:

```python
spark.range(5).show()
spark.sql("select current_date() as today").show()
spark.stop()
exit()
```

`local[2]` means Spark runs with two worker threads on your Mac.

## Step 8: Create The Sample Input Files

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

## Step 9: Run Your First Spark Job

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

This job reads and writes only your Mac's folders. The results go to:

- `data/output/orders_etl/orders_parquet/`
- `data/output/orders_etl/summary_csv/`

Spark writes folders with `part-*` files, not one file. Check the output:

```bash
find data/output/orders_etl -maxdepth 5 -type f | sort
```

## Step 10: Continue With The PySpark 101 Examples

Your local Spark setup is ready. Open the examples file next:

```text
/Users/paramraghavan/dev/sparksql-awsglue/help/quick101-spark/spark_quickstart/pyspark-101-examples.md
```

Run the examples from your lab folder, with the virtual environment on.

---

# Part 2: LocalStack In Docker (Optional)

LocalStack is a local AWS simulator. It runs as a Docker container and listens on `http://localhost:4566`. Your Spark jobs and `awslocal` commands send their S3 requests to that address, so you can practice S3 without a real AWS account.

```text
data/output/...   -> a normal folder on your Mac
s3a://de-lab/...  -> a fake S3 bucket inside the LocalStack container
```

LocalStack is not a Python library. The only Python package this part needs is `awscli-local`, which you installed in Step 5.

Recent LocalStack versions need a free auth token before they start. Get the token first (Step 12).

## Step 11: Start Docker Desktop

1. Install Docker Desktop from `https://www.docker.com/products/docker-desktop/`.
2. Open Docker Desktop and wait until it says it is running.
3. Check from a terminal:

   ```bash
   docker ps
   ```

   If this shows a table, even an empty one, Docker is ready. If it reports that it cannot connect, wait for Docker Desktop to finish starting.

## Step 12: Get A Free LocalStack Auth Token

1. Sign up at `https://app.localstack.cloud/sign-up?plan=free`, or sign in if you have an account.
2. Find your auth token on the account page and copy it.
3. Keep the token private. Do not paste it into this guide, a repository, or chat.
4. Set the token in your terminal. Replace the placeholder with your token:

   ```bash
   export LOCALSTACK_AUTH_TOKEN="paste-your-token-here"
   ```

   This setting lasts only for the current terminal. Run the export again in each new terminal, or add the line to `~/.zshrc` yourself.

Check that the variable is set. This prints only the length, not the token:

```bash
echo ${#LOCALSTACK_AUTH_TOKEN}
```

A number greater than 0 means the token is set. `0` means it is not set in this terminal.

## Step 13: Start The LocalStack Container

Run this in the same terminal where you exported the token. The first run downloads the image, which can take a few minutes:

```bash
docker run -d \
  --name localstack-main \
  -e LOCALSTACK_AUTH_TOKEN="$LOCALSTACK_AUTH_TOKEN" \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  localstack/localstack
```

Use the image named exactly `localstack/localstack`. Similar names such as `localstack-light`, `localstack-full`, `localstack-pro`, and the Lambda or Snowflake images are different.

## Step 14: Check That LocalStack Is Running

Check the container:

```bash
docker ps --filter name=localstack-main
docker logs localstack-main
```

The container should show `Up`. The logs show the services starting.

Check the endpoint:

```bash
curl -s http://localhost:4566/_localstack/health
```

The response is JSON that lists the services, including `s3` and `glue`.

If the container exits, the logs explain why. The most common cause is a missing token. The message is `License activation failed ... No credentials were found`. Go back to Step 12 and set the token in the same terminal before you run `docker run` again.

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

```bash
curl -s http://localhost:4566/_localstack/health | python -m json.tool
```

Check the LocalStack version:

```bash
curl -s http://localhost:4566/_localstack/info | python -m json.tool
```

Fetch an uploaded file. Run this after Step 16 uploads it:

```bash
curl -s http://localhost:4566/de-lab/raw/orders/orders.csv
```

List the Glue databases:

```bash
awslocal glue get-databases
```

## Step 15: Set The AWS Environment Variables

Tools that talk to AWS need some settings. These values are fake and only work with LocalStack. Add them to `~/.zshrc` once:

```bash
cat >> ~/.zshrc <<'EOF'
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
EOF
source ~/.zshrc
```

## Step 16: Create The Bucket And Upload The Input Files

Activate the lab environment, then create the `de-lab` bucket:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate

awslocal s3 mb s3://de-lab
awslocal s3 ls
```

Upload the input files:

```bash
awslocal s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv
awslocal s3 cp data/input/customers.json s3://de-lab/raw/customers/customers.json
awslocal s3 ls s3://de-lab/raw/ --recursive
```

To copy anything from LocalStack back to your Mac:

```bash
mkdir -p data/downloaded/orders_parquet

awslocal s3 cp \
  s3://de-lab/curated/orders_parquet/ \
  data/downloaded/orders_parquet/ \
  --recursive
```

## Step 17: Read And Write S3 From Spark

Spark needs the Hadoop AWS libraries to read `s3a://` paths. Spark downloads them when you pass `--packages`.

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

Check the output in LocalStack:

```bash
awslocal s3 ls s3://de-lab/curated/ --recursive
```

## Step 18: Stop, Restart, Or Remove LocalStack

Stop the container when you finish:

```bash
docker stop localstack-main
```

Start it again later with the same token exported:

```bash
docker start localstack-main
```

To delete the container and everything stored in it:

```bash
docker rm -f localstack-main
```

Download anything you need from LocalStack S3 before you delete the container.

## Lesson: Update Data On S3 By Rewriting It

S3 is object storage, not a database. Spark does not change one row inside a CSV or Parquet file. It reads the data, builds a new DataFrame, and writes replacement files.

Delete a folder:

```bash
awslocal s3 rm s3://de-lab/curated/orders_csv/ --recursive
```

Update by rewriting:

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

For row-level `UPDATE`, `DELETE`, and `MERGE` in production, use a table format such as Apache Iceberg, Delta Lake, or Apache Hudi. Plain CSV and Parquet files in S3 do not behave like database tables.

## Lesson: AWS Glue Data Catalog

The Glue Data Catalog maps S3 files to table definitions. Tools such as Athena, Glue Spark jobs, and EMR use it to find and read the data. Read these sections in `pyspark-101-examples.md`:

- `AWS Glue Data Catalog`
- `Can Glue Do Create, Read, Update, Delete?`

---

# Part 3: Troubleshooting

**`Java gateway process exited`**

- Run `java -version`. You need Java 17.
- Run `echo "$JAVA_HOME"`. It should point to the Java 17 folder.

**`No FileSystem for scheme s3a`**

- Run `spark-submit` with the `--packages` option from Step 17.

**`command not found: awslocal`**

- `awslocal` is installed only inside the lab's virtual environment. Turn the environment on first:

  ```bash
  cd ~/spark-glue-local-lab
  source .venv/bin/activate
  awslocal --version
  ```

- Run `source .venv/bin/activate` again in every new terminal before you use `awslocal`.
- If `awslocal` is still missing after you activate the environment, run `python -m pip install awscli-local` inside the environment.

**`Connection refused localhost:4566`**

- LocalStack is not running. Check `docker ps --filter name=localstack-main`.
- If the container is missing, run the command in Step 13.
- Check that Docker Desktop is running.

**The container exits with code 55 and mentions `LOCALSTACK_AUTH_TOKEN`**

- The token is missing from the terminal where you ran `docker run`. Check it with `echo ${#LOCALSTACK_AUTH_TOKEN}`. Go back to Step 12.

**`AccessDenied` or credential errors**

- Use `AWS_ACCESS_KEY_ID=test` and `AWS_SECRET_ACCESS_KEY=test` for LocalStack.
- Check `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` in your job.

**`CSV output has many part files`**

- This is normal. Spark writes one file per partition.
- `coalesce(1)` is only for small local examples.

---

# Next: Spark Monitoring And Hadoop

Follow the [Spark UI, History Server, and YARN teaching lab](spark-ui-history-server-lab.md) to watch jobs in the Spark UI. No LocalStack is needed for that lab.

For a separate Hadoop cluster, follow the [self-contained Hadoop/YARN installation lab](setup-hadoop-yarn-local-lab.md).
