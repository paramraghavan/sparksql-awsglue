# Spark Quickstart For SQL And Python Users

This folder is a jumpstart for developers who already know SQL and Python and need to contribute to a PySpark data engineering project quickly.

The goal is not to memorize every Spark API. The goal is to learn the working mental model used in real projects:

```text
raw files -> PySpark DQ -> trusted baseline data -> optional transformations -> cataloged tables
```

## Recommended Study Order

1. Set up Spark locally.
   - Mac: `setup-mac-local-spark.md`
   - Windows: `setup-windows-local-spark.md`
2. Work through `pyspark-101-examples.md`.
   - Start with local files.
   - Learn DataFrame operations by mapping them to SQL.
   - Practice Parquet, partitioning, joins, window functions, DQ, and update-by-rewrite.
3. Complete the [Spark UI and History Server lab](spark-ui-history-server-lab.md).
   - Run slow, straggler, and failed jobs; compare the live UI with recorded history.
   - Learn when to use the YARN ResourceManager UI on a cluster.
   - For a full Hadoop cluster, follow the [self-contained Hadoop/YARN setup](setup-hadoop-yarn-local-lab.md) using WSL2 or a macOS Linux VM.
4. Use `setup-jupyter-remote-access.md` if you want notebook access from another computer on your home network.
5. Read `pyspark-ingestion-transformation-architecture.md`.
   - This explains raw, rejected, trusted, transformed, Lambda, SSM, Spark jobs, and Glue Data Catalog.
6. Run [the config-driven CSV example](etl_config_driven/README.md).
   - This is a plain-Python example of configuration and DQ; it does not run a Spark pipeline.

## AWS Practice Uses Docker And LocalStack

All AWS-style practice in this quickstart is local practice.

You do not need a real AWS account for these lessons. When the notes show AWS-style services such as S3 or Glue Data Catalog, the lab uses:

```text
Docker Desktop -> LocalStack container -> local AWS-like services
```

Beginner mental model:

| Term in the notes | What it means in this quickstart |
|---|---|
| S3 bucket | LocalStack S3 bucket running inside Docker |
| Glue Data Catalog | LocalStack Glue metadata service |
| `lstk aws s3 ...` | AWS CLI command pointed at LocalStack, not real AWS (replaces the deprecated `awslocal`) |
| `lstk aws glue ...` | Glue CLI command pointed at LocalStack. Needs a paid LocalStack license, so the lab uses Spark's SQL catalog instead |
| `s3a://de-lab/...` | Spark path that reads/writes LocalStack S3 |
| Docker | The local runtime that hosts LocalStack |

Install Docker before running LocalStack sections:

- Mac: see `setup-mac-local-spark.md`, Part 2 `LocalStack (Optional)`, Steps 11 to 19.
- Windows: see `setup-windows-local-spark.md`, Part 2 `LocalStack (Optional)`, Steps 11 to 19.

The local-file PySpark lessons, monitoring lab, and config-driven CSV example run without LocalStack.

## What Each File Is For

| File or folder | Purpose |
|---|---|
| `setup-mac-local-spark.md` | Install Java, Python, PySpark, LocalStack, and run a first local Spark job on macOS |
| `setup-windows-local-spark.md` | Same setup path for Windows and PowerShell |
| `setup-jupyter-remote-access.md` | Install JupyterLab first, then Jupyter Notebook if needed, and access it from another PC with a password |
| [spark-ui-history-server-lab.md](spark-ui-history-server-lab.md) | macOS/Windows monitoring commands, bounded slow/failure exercises, and YARN access |
| [setup-hadoop-yarn-local-lab.md](setup-hadoop-yarn-local-lab.md) | Complete single-node Linux Hadoop/YARN installation for WSL2/macOS VMs, Spark submission, and monitoring |
| [monitoring_lab.py](monitoring_lab.py) | Runnable local Spark teaching scenarios with event logging |
| `pyspark-101-examples.md` | Main PySpark study guide for SQL/database users |
| `spark-new-user-use-cases.md` | How to approach simple ingestion and large SQL-style join transformations in PySpark |
| `parquet-spark-performance-and-updates.md` | Separate note on Parquet, Spark file sizing, reads, slow jobs, partition updates, and Delta/Iceberg/Hudi concepts |
| `pyspark-ingestion-transformation-architecture.md` | Architecture diagrams and concepts for ingestion, DQ, trusted, transformed, Lambda, SSM, and Glue |
| `etl_config_driven/` | Runnable plain-Python CSV example of configuration and DQ |

## Learning Outcomes

After completing this folder, a new contributor should be able to:

- Read CSV, JSON, and Parquet with PySpark.
- Explain `local[2]`, actions vs transformations, partitions, and `part-*` files.
- Translate common SQL into PySpark DataFrame operations.
- Apply beginner DQ rules and write rejected records.
- Write trusted Parquet output and explain why Parquet is preferred over CSV for curated data.
- Explain why plain S3 Parquet is not an OLTP database table.
- Describe incremental load vs full load patterns.
- Explain what AWS Glue Data Catalog stores and what it does not store.
- Understand why Lambda submits work, while Spark jobs perform DQ and transformations.

## Production Versus Training Shortcut

Some examples intentionally keep the code small so learners can understand the flow in one sitting. The docs call out where a production system would use stronger patterns such as:

- idempotent ingestion,
- schema contracts,
- partition repair or partition registration,
- CloudWatch metrics and alerts,
- IAM and Lake Formation permissions,
- Apache Iceberg, Hudi, or Delta Lake for row-level update/delete/merge behavior.

Use the quickstart to build the mental model first, then use the production notes to understand what changes in real AWS projects.
