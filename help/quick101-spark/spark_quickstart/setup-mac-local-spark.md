# Mac Local Spark, AWS Glue, and LocalStack Setup

This guide sets up PySpark on your Mac so you can practice Spark, then optionally adds a fake AWS account (LocalStack) so you can practice S3 and Glue.

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

Every new terminal needs the virtual environment turned on. Run these two lines first:

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
  jupyterlab \
  ipykernel
```

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

# Part 2: LocalStack (Optional)

LocalStack is a local AWS simulator. It runs in a Docker container and listens on `http://localhost:4566`. Your Spark jobs and AWS commands send their requests to that address, so you can practice S3 and Glue without a real AWS account.

```text
data/output/...   -> a normal folder on your Mac
s3a://de-lab/...  -> a fake S3 bucket inside the LocalStack container
```

You use the `lstk` command to start and manage LocalStack, and `aws --profile localstack` to run AWS commands against it.

Note: the older `awslocal` command is deprecated. Use `lstk aws` instead, as shown below.

## Step 11: Start Docker Desktop

1. Install Docker Desktop from `https://www.docker.com/products/docker-desktop/`.
2. Open Docker Desktop and wait until it says it is running.
3. Check from a terminal:

   ```bash
   docker ps
   ```

   If this shows a table, even an empty one, Docker is ready.

## Step 12: Install lstk And The AWS CLI

```bash
brew tap localstack/tap
brew install localstack/tap/lstk awscli
```

Check both:

```bash
lstk --version
aws --version
```

## Step 13: Log In With lstk

```bash
lstk login
```

Follow the prompts. If you do not have an account yet, sign up for free at `https://app.localstack.cloud/sign-up?plan=free`. `lstk` stores your credentials in your Mac's keychain, so you do not need to paste the token anywhere in this guide.

Recent LocalStack versions need this login before they start.

## Step 14: Start LocalStack

```bash
lstk start
lstk status
```

`lstk start` runs the LocalStack container for you. `lstk status` shows whether it is running and which resources exist.

Stop and restart it with:

```bash
lstk stop
lstk restart
```

## Step 15: Set Up The LocalStack AWS Profile

```bash
lstk setup aws
```

This adds a `localstack` profile to `~/.aws/config` and `~/.aws/credentials`. The profile points AWS commands at LocalStack instead of real AWS. Run the command again with `--force` if you need to overwrite an existing `localstack` profile.

## Step 16: Check That LocalStack Is Running

Check the services:

```bash
lstk status
curl -s http://localhost:4566/_localstack/health | python -m json.tool
```

The health response lists the services. The free plan includes `s3`. Glue needs a paid LocalStack license, so this lab uses Spark's SQL catalog for table metadata instead (Step 25).

### LocalStack Service URLs

All AWS services share one address, `http://localhost:4566`. You do not need a separate port for each service. The AWS CLI and Spark pick the service from the command or the request, not from the URL.

| What | URL or command | Use it for |
|---|---|---|
| Base endpoint | `http://localhost:4566` | Address every AWS call uses |
| Health | `http://localhost:4566/_localstack/health` | See which services are running |
| Info | `http://localhost:4566/_localstack/info` | See the LocalStack version |
| S3 object (path style) | `http://localhost:4566/de-lab/raw/orders/orders.csv` | Fetch an uploaded file, once the bucket exists |
| S3 bucket list | `lstk aws s3 ls` | List buckets |
| Glue | Not on the free plan | Use the Spark SQL catalog in Step 25 |

The `s3a://de-lab/...` paths in Spark use the same endpoint through the `spark.hadoop.fs.s3a.endpoint` setting in Step 18. The `path.style.access` setting makes Spark put the bucket name in the URL path, which is the form in the table above.

Check the LocalStack version:

```bash
curl -s http://localhost:4566/_localstack/info | python -m json.tool
```

## Step 17: Create The Bucket And Upload The Input Files

Activate the lab environment, then create the `de-lab` bucket:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate

lstk aws s3 mb s3://de-lab
lstk aws s3 ls
```

Upload the input files:

```bash
lstk aws s3 cp data/input/orders.csv s3://de-lab/raw/orders/orders.csv
lstk aws s3 cp data/input/customers.json s3://de-lab/raw/customers/customers.json
lstk aws s3 ls s3://de-lab/raw/ --recursive
```

Fetch an uploaded file to check the URL pattern:

```bash
curl -s http://localhost:4566/de-lab/raw/orders/orders.csv
```

To copy anything from LocalStack back to your Mac:

```bash
mkdir -p data/downloaded/orders_parquet

lstk aws s3 cp \
  s3://de-lab/curated/orders_parquet/ \
  data/downloaded/orders_parquet/ \
  --recursive
```

## Step 18: Read And Write S3 From Spark

Spark needs the Hadoop AWS libraries to read `s3a://` paths. Spark downloads them when you pass `--packages`.

**Why `s3a://` and not `s3://`?** On EMR, `s3://` works because EMR ships its own S3 driver (EMRFS) and the AWS libraries. Open-source Spark on your Mac has no `s3://` driver. The open-source option is `s3a://`, which comes from the `hadoop-aws` jar and its AWS SDK dependency, `aws-java-sdk-bundle`. Without those jars, Spark fails with `No FileSystem for scheme`. The `--packages` option downloads both jars. The `spark.hadoop.fs.s3a.endpoint` setting then points Spark at LocalStack instead of AWS. EMRFS needs no endpoint setting because it always talks to AWS.

**The same session on EMR.** The transformation code stays the same. The session setup and the paths change:

| Setting | This lab (LocalStack) | EMR |
|---|---|---|
| Creating the session | `SparkSession.builder` with `master("local[2]")` | `SparkSession.builder.getOrCreate()`. Some EMR notebooks create `spark` for you |
| Hadoop AWS jars | `--packages` downloads them | Already on the cluster, so there is no `--packages` |
| S3 scheme | `s3a://de-lab/...` | `s3://your-real-bucket/...` (EMRFS) |
| Endpoint | `spark.hadoop.fs.s3a.endpoint` = `http://localhost:4566` | Not set, because EMR talks to AWS |
| Credentials | `test` / `test` access keys | IAM role attached to the cluster or job |
| Path style | `path.style.access` = `true` | Not needed |
| Table metadata | Spark SQL catalog table over the S3 folder (Step 25) | `spark.table("curated.orders_raw")`, if the cluster uses the Glue Data Catalog as its metastore |

The same job on EMR looks like this:

```python
spark = SparkSession.builder.appName("orders-on-emr").getOrCreate()

orders = spark.read.option("header", True).csv("s3://your-real-bucket/raw/orders/orders.csv")
orders_from_glue = spark.table("curated.orders_raw")
```

The exact setup depends on how your team configures the cluster, so check with your platform team before you rely on the Glue table lookup.

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

### Before Running: `--master` And `--deploy-mode`

`spark-submit` launches the Python job. These options answer two different questions:

| Option | What it controls | Default when not otherwise configured |
|---|---|---|
| `--master` | Where Spark runs the work: locally or through a cluster manager | `local[*]` for Spark 3.5.6 |
| `--deploy-mode` | Where the **driver** runs: the process coordinating the job | `client` |

Common `--master` values:

| Value | Meaning |
|---|---|
| `local` | One worker thread on your Mac |
| `local[2]` | Two worker threads on your Mac; used in this lab |
| `local[*]` | As many worker threads as available logical CPU cores |
| `yarn` | Use a configured Hadoop YARN cluster, such as EMR on EC2 |
| `spark://host:7077` | Connect to a Spark standalone cluster |
| `k8s://host:443` | Connect to a configured Kubernetes cluster |

These are execution targets, independent of whether your files are on local disk or S3.

**`--deploy-mode` accepts two values: `client` (default) and `cluster`.** selects the execution target.

- **`client`:** The driver runs on the submitting machine. Here, that is your Mac, with logs in the terminal. With a remote cluster, task execution can still be remote.
- **`cluster`:** The cluster manager launches the driver inside the cluster. This needs a supported, configured cluster; it cannot be used with `--master local[...]`. Spark standalone does not support Python applications in cluster deploy mode.

For this lab, use **`--master "local[2]" --deploy-mode client`**. Omitting `--deploy-mode` gives the same behavior unless configuration changes its default. The flag is `--deploy-mode`, not `--deploy`.

**Defaults can be overridden.** `spark-defaults.conf` can set `spark.master` and `spark.submit.deployMode`; `MASTER` can also supply a master when none is configured. This script's `.master("local[2]")` explicitly sets the application master too. Remove that builder line when adapting the script so `--master` selects the target. Cluster deploy mode also needs driver-side settings, paths, and credentials suited to that environment.

See [Spark 3.5.6 submission options](https://spark.apache.org/docs/3.5.6/submitting-applications.html) and the [Spark 3.5.6 master fallback](https://github.com/apache/spark/blob/v3.5.6/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L42). Use `spark-submit --help` for the installed version's options.

### Do I Need `--packages` If The JARs Are Already Installed?

**Only omit `--packages` when compatible JARs are already available on the Spark JVM classpaths that need them.** A classpath tells Java where to load libraries. Shell `PATH` locates commands such as `spark-submit`; placing JARs in a directory on `PATH` does not load them into Spark.

| Execution setup | Where the S3A libraries must be available |
|---|---|
| `--master "local[2]" --deploy-mode client` | The local Spark JVM on your Mac |
| Remote cluster, `--deploy-mode client` | The submitting machine's driver **and** cluster executors |
| Remote cluster, `--deploy-mode cluster` | The cluster-hosted driver **and** executors; local client JARs alone are insufficient |

For the S3A connector, `hadoop-aws` must match the runtime's Hadoop version, with its compatible AWS SDK. This lab uses Hadoop 3.3.4 and AWS SDK bundle 1.12.262. Check versions before reusing these coordinates on another Spark distribution.

- **JARs already on every required classpath:** You can omit `--packages`. Examples include a correctly provisioned Spark installation or cluster/container image. Avoid adding conflicting copies.
- **JARs downloaded into a Maven/Ivy cache only:** Keep `--packages`. It resolves the coordinates, reuses cached artifacts where available, and adds/distributes the libraries for the job. Being cached does not automatically put them on Spark's classpath.
- **JAR files downloaded manually:** Use `--jars /absolute/path/hadoop-aws-3.3.4.jar,/absolute/path/aws-java-sdk-bundle-1.12.262.jar` instead, with paths accessible to the submission process. Spark adds/distributes these JARs; `--jars` does not resolve missing transitive dependencies.

**For this Mac lab, keep the `--packages` line below** unless you have explicitly installed the matching libraries on Spark's classpath. Deploy mode alone does not supply S3A dependencies. `hadoop-aws` also declares the AWS SDK bundle as a dependency; listing both coordinates here makes the lab's versions explicit.

See [Spark dependency distribution](https://spark.apache.org/docs/3.5.6/submitting-applications.html#advanced-dependency-management) and [Hadoop 3.3.4 S3A setup](https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html#getting_started).

Run it (put Spark options before the Python filename):

```bash
spark-submit \
  --master "local[2]" \
  --deploy-mode client \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  jobs/localstack_s3_etl.py
```

Check the output in LocalStack and the localstack console:

```bash
lstk aws s3 ls s3://de-lab/curated/ --recursive
```

### local stack console
https://app.localstack.cloud/inst/default/resources/s3/de-lab


## Step 19: Stop Or Remove LocalStack

Stop LocalStack when you finish:

```bash
lstk stop
```

Start it again later with `lstk start`.

Download anything you need from LocalStack S3 before you remove it. Check the `lstk` help for how to remove the emulator and its data:

```bash
lstk --help
```

### Remove The LocalStack AWS Profile (Optional)

`lstk` has no command that deletes the `localstack` profile. `lstk logout` removes only your login credentials. To remove the profile, edit two files by hand.

1. Back up both files first:

   ```bash
   cp ~/.aws/config ~/.aws/config.bak
   cp ~/.aws/credentials ~/.aws/credentials.bak
   ```

2. Open `~/.aws/config` and delete the `[profile localstack]` block:

   ```bash
   nano ~/.aws/config
   ```

3. Open `~/.aws/credentials` and delete the `[localstack]` block:

   ```bash
   nano ~/.aws/credentials
   ```

4. Check that the profile is gone:

   ```bash
   aws configure list-profiles
   ```

   `localstack` should not appear in the list. To set it up again, run `lstk setup aws`.

## Lesson: Update Data On S3 By Rewriting It

S3 is object storage, not a database. Spark does not change one row inside a CSV or Parquet file. It reads the data, builds a new DataFrame, and writes replacement files.

Delete a folder:

```bash
lstk aws s3 rm s3://de-lab/curated/orders_csv/ --recursive
```

Update by rewrite:

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

The Glue Data Catalog maps S3 files to table definitions. Tools such as Athena, Glue Spark jobs, and EMR use it to find and read the data. Spark's SQL catalog does the same job for practice: a table definition points at an S3 folder, and the data stays in S3.

LocalStack's Glue service needs a paid license, so the `lstk aws glue ...` commands are not part of this free lab. Read these sections in `pyspark-101-examples.md`:

- `AWS Glue Data Catalog`
- `Can Glue Do Create, Read, Update, Delete?`

### Register And Remove Partitions

Files in S3 are not enough for a query to see them. The query engine reads only the partitions that the catalog lists. A new folder such as `order_year=2027/` returns zero rows until its partition is registered. The query does not raise an error, so the problem is easy to miss.

**Register a partition**

Add one partition by hand. Use the folder path as the `LOCATION`:

```sql
ALTER TABLE curated.orders ADD IF NOT EXISTS PARTITION (order_year=2027, category='books')
LOCATION 's3://de-lab/curated/orders_parquet/order_year=2027/category=books/'
```

Scan the table location and add every partition that is missing from the catalog:

```sql
MSCK REPAIR TABLE curated.orders
```

`MSCK REPAIR TABLE` is fine for labs, backfills, and tables with a few partitions. It lists the whole table location and adds partitions one at a time, so it becomes slow when a table has thousands of partitions. For a scheduled pipeline, register each new partition right after the job writes it, with `ALTER TABLE ... ADD PARTITION` in Spark or Athena. In AWS Glue, use the `BatchCreatePartition` API, or run a Glue crawler on a schedule. Use `MSCK REPAIR TABLE` for catch-up work.

**Remove a partition**

Removing a partition from the catalog does not delete its files when the table is external. Those are two separate steps.

```sql
-- 1. Remove the catalog entry first.
ALTER TABLE curated.orders DROP IF EXISTS PARTITION (order_year=2027, category='books')

-- 2. Then delete the files in S3.
```

```bash
lstk aws s3 rm s3://de-lab/curated/orders_parquet/order_year=2027/category=books/ --recursive
```

Drop the catalog entry first. If you delete the files first, queries can point at folders that no longer exist.

To delete the files from PySpark instead of the AWS CLI, use Hadoop's filesystem API through Spark. Run this in the same session that has the S3 settings:

```python
# 1. Remove the catalog entry first.
spark.sql("ALTER TABLE curated.orders DROP IF EXISTS PARTITION (order_year=2027, category='books')")

# 2. Delete the partition folder in S3 with Hadoop's filesystem API.
jvm = spark.sparkContext._jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

path = jvm.org.apache.hadoop.fs.Path("s3a://de-lab/curated/orders_parquet/order_year=2027/category=books")
fs = path.getFileSystem(hadoop_conf)
fs.delete(path, True)  # True = delete recursively
```

`spark.sparkContext._jvm` and `_jsc` are internal Spark APIs. They work in the PySpark versions in this guide, but they are not a public interface. I tested the same delete locally on a folder on disk; I have not run it against LocalStack S3.

If a folder was deleted but its catalog entry remains, Spark can remove the stale entry:

```sql
MSCK REPAIR TABLE curated.orders SYNC PARTITIONS
```

Athena's `MSCK REPAIR TABLE` only adds partitions, so use `ALTER TABLE ... DROP PARTITION` there. In AWS Glue, use the `BatchDeletePartition` API or the Glue console.

I tested these Spark SQL commands on local files with Spark 4.0.1. I have not tested them against LocalStack S3, Athena, or the Glue Data Catalog.

---

# Part 3: Notebook Access From Another Computer (Optional)

This part runs a Jupyter notebook on your Mac, then opens it from another computer on the same home network. The notebook runs Spark on the Mac, so the Spark code can reach LocalStack at `localhost:4566`. The other computer only shows the notebook in its browser.

The network and password details are in `setup-jupyter-remote-access.md`. The steps below cover only what this lab needs.

Use this only on a trusted home network. Do not set up router port forwarding for port `8888`.

## Step 20: Set Up JupyterLab On The Mac

JupyterLab was installed in Step 5. Turn on the lab environment, then set a password:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
jupyter server password
```

Enter a real password when prompted. Anyone on your network who can reach the Mac can try to log in.

## Step 21: Start JupyterLab So Other Computers Can Reach It

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
```

Leave this terminal open. Stop Jupyter with `Ctrl+C` when you finish.

## Step 22: Find The Mac's Address And Open It From The Other Computer

On the Mac, find the address on your home network:

```bash
ipconfig getifaddr en0
```

If that prints nothing, try `ifconfig | grep "inet "` and use the address on your Wi-Fi or Ethernet interface, for example `192.168.1.25`.

On the other computer, open a browser and go to:

```text
http://192.168.1.25:8888/lab
```

Replace `192.168.1.25` with your Mac's address. Enter the password from Step 20.

If the page does not open, check the firewall and network steps in `setup-jupyter-remote-access.md`.

## Step 23: Register A Named Kernel For The Lab

This makes the lab's virtual environment show up in Jupyter under a clear name. Run this on the Mac, with the environment on:

```bash
python -m ipykernel install --user --name spark-lab --display-name "Spark Lab (LocalStack)"
```

Refresh the Jupyter page on the other computer. When you create a notebook, choose the **Spark Lab (LocalStack)** kernel.

## Step 24: Create A Notebook And Connect Spark To LocalStack

In JupyterLab, create a new notebook with the **Spark Lab (LocalStack)** kernel. Save it as `spark_localstack.ipynb` in `~/spark-glue-local-lab`.

The first Spark session downloads the Hadoop AWS libraries, so it needs an internet connection the first time.

Cell 1: start Spark with the LocalStack settings:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("notebook-localstack")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark
```

Cell 2: read the orders file from LocalStack S3. This file was uploaded in Step 17:

```python
orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("s3a://de-lab/raw/orders/orders.csv")
)

orders.show()
```

You should see the six order rows. The notebook's `localhost` is your Mac, so the other computer never connects to LocalStack directly.

## Step 25: Create A Spark SQL Catalog Table

LocalStack's Glue service needs a paid license, so this lab uses Spark's built-in SQL catalog for table metadata. It teaches the same idea: a table definition points at an S3 folder, and the data stays in S3.

Cell 3: create a database and a table that points at the raw orders folder:

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
```

Check the table:

```python
spark.sql("SHOW TABLES IN curated").show()
```

Notes:

- The table is metadata only. The data stays in S3.
- Spark keeps this catalog in the current session. If you restart the kernel, run Cell 3 again.
- Point `LOCATION` at a folder that holds only this dataset. Spark reads every file in the folder.

## Step 26: Query The Table With SQL

Cell 4: read the table by name and run SQL against it:

```python
orders_from_catalog = spark.table("curated.orders_raw")
orders_from_catalog.show()

spark.sql("""
SELECT status, COUNT(*) AS order_count
FROM curated.orders_raw
GROUP BY status
""").show()
```

The result matches Cell 2. The SQL is the same kind you would write against a Glue table in AWS. The difference is that Spark stores the table definition here, not Glue.

Stop the Spark session when you finish:

```python
spark.stop()
```

---

# Part 4: Troubleshooting

**`Java gateway process exited`**

- Run `java -version`. You need Java 17.
- Run `echo "$JAVA_HOME"`. It should point to the Java 17 folder.

**`No FileSystem for scheme s3a`**

- Run `spark-submit` with the `--packages` option from Step 18.

**`command not found: awslocal` or the deprecation warning**

- `awslocal` is deprecated. Use `lstk aws` instead, for example `lstk aws s3 ls`.

**`command not found: lstk`**

- Install it again with `brew tap localstack/tap` and `brew install localstack/tap/lstk`.

**`No AWS profile found`**

- Run `lstk setup aws` (Step 15).

**`License activation failed` or the container exits with code 55**

- Run `lstk login` (Step 13), then `lstk start` again.

**`Connection refused localhost:4566`**

- LocalStack is not running. Run `lstk status`, then `lstk start` if needed.
- Check that Docker Desktop is running with `docker ps`.

**`AccessDenied` or credential errors**

- Use `test` for the access key and secret key in the Spark job (Step 18).
- Check `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`.

**The notebook cannot be reached from another computer**

- Check that `jupyter lab --ip=0.0.0.0 --port=8888 --no-browser` is still running on the Mac.
- Check the Mac's address with `ipconfig getifaddr en0`.
- Check the firewall and network steps in `setup-jupyter-remote-access.md`.

**The notebook kernel cannot find `pyspark` or `boto3`**

- Install the kernel again from the activated environment: `python -m ipykernel install --user --name spark-lab --display-name "Spark Lab (LocalStack)"`.
- Choose the **Spark Lab (LocalStack)** kernel for the notebook.

**The first Spark cell hangs or fails to download packages**

- The first run downloads the Hadoop AWS libraries. Check your internet connection and try again.

**`Table or view not found: curated.orders_raw`**

- Spark's catalog lives only in the current session. Restart the kernel if needed, then run Step 25 Cell 3 again.

**`Glue service is not included in your LocalStack license`**

- Glue needs a paid LocalStack license. Use the Spark SQL catalog in Step 25 instead.

**`CSV output has many part files`**

- This is normal. Spark writes one file per partition.
- `coalesce(1)` is only for small local examples.

---

# Next: Spark Monitoring And Hadoop

Follow the [Spark UI, History Server, and YARN teaching lab](spark-ui-history-server-lab.md) to watch jobs in the Spark UI. No LocalStack is needed for that lab.

For a separate Hadoop cluster, follow the [self-contained Hadoop/YARN installation lab](setup-hadoop-yarn-local-lab.md).
