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
3. Use `setup-jupyter-remote-access.md` if you want notebook access from another computer on your home network.
4. Read `pyspark-ingestion-transformation-architecture.md`.
   - This explains raw, rejected, trusted, transformed, Lambda, SSM, Spark jobs, and Glue Data Catalog.
5. Run `pipeline_demo/README.md`.
   - This is the runnable local demo of the architecture.

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
| `awslocal s3 ...` | AWS CLI-style command pointed at LocalStack, not real AWS |
| `awslocal glue ...` | Glue CLI-style command pointed at LocalStack, not real AWS |
| `s3a://de-lab/...` | Spark path that reads/writes LocalStack S3 |
| Docker | The local runtime that hosts LocalStack |

Install Docker before running LocalStack sections:

- Mac: see `setup-mac-local-spark.md`, sections `Optional: Install Docker Desktop For LocalStack` and `Optional: Start LocalStack`.
- Windows: see `setup-windows-local-spark.md`, sections `Optional: Install Docker Desktop For LocalStack` and `Optional: Start LocalStack`.

Only the first local PySpark lessons and `pipeline_demo/` can run without LocalStack because they use local folders to stand in for S3.

## What Each File Is For

| File or folder | Purpose |
|---|---|
| `setup-mac-local-spark.md` | Install Java, Python, PySpark, LocalStack, and run a first local Spark job on macOS |
| `setup-windows-local-spark.md` | Same setup path for Windows and PowerShell |
| `setup-jupyter-remote-access.md` | Install JupyterLab first, then Jupyter Notebook if needed, and access it from another PC with a password |
| `pyspark-101-examples.md` | Main PySpark study guide for SQL/database users |
| `spark-new-user-use-cases.md` | How to approach simple ingestion and large SQL-style join transformations in PySpark |
| `parquet-spark-performance-and-updates.md` | Separate note on Parquet, Spark file sizing, reads, slow jobs, partition updates, and Delta/Iceberg/Hudi concepts |
| `pyspark-ingestion-transformation-architecture.md` | Architecture diagrams and concepts for ingestion, DQ, trusted, transformed, Lambda, SSM, and Glue |
| `pipeline_demo/` | Config-driven runnable demo with local folders standing in for S3 buckets |

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
