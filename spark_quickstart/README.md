# Spark Quickstart for SQL and Python Users

Learn the PySpark skills needed to read, validate, transform, and publish data. This guide assumes you already know basic SQL and Python.

The main data flow is:

```text
Raw files → data quality checks → trusted data → optional transformations → tables for querying
                 ↓
           Rejected records
```

**Data quality (DQ)** checks identify missing keys, invalid values, duplicates, and other records that need attention. Cataloged tables describe where data lives and how to read it; the catalog does not store the rows.

## Start Here

1. Set up local Spark using the [macOS guide](setup-mac-local-spark.md) or [Windows guide](setup-windows-local-spark.md). Complete the local-file job before adding LocalStack.
2. If you already know SQL, start with [PySpark for SQL users](sql_and_pyspark.md), a SQL-first exercise with expected results. Then work through the [PySpark 101 examples](pyspark-101-examples.md). Start with local files, then practice SQL-to-DataFrame operations, joins, window functions, data quality, and Parquet output.
3. Apply those skills using the [beginner use cases](spark-new-user-use-cases.md), then read the [S3 ingestion pipeline walkthrough](etl_pipeline_walkthrough.md) for an introduction to S3 events, Lambda, SSM, and Spark submission.
4. Read the [PySpark pipeline production design](pyspark-pipeline-production-design.md) for data zones, quality rules, optional transformations, load strategies, and configuration.
5. Explore [Parquet performance and updates](parquet-spark-performance-and-updates.md) and the [Glue/Iceberg guide](aws_glue_iceberg/README.md) when you are ready for file layout, transactional tables, and multiple query engines.

Use the [remote Jupyter guide](setup-jupyter-remote-access.md) only if you want to access a notebook running on one computer from another computer on your home network.

For API selection and matching window/CASE examples, see [Spark SQL vs PySpark DataFrame functions](pyspark-101-examples.md#when-to-use-spark-sql-vs-pyspark-dataframe-functions).

## Guide Index

| Guide | What it covers |
|---|---|
| [macOS setup](setup-mac-local-spark.md) | Java, Python, PySpark, a first job, optional LocalStack, and notebooks |
| [Windows setup](setup-windows-local-spark.md) | Local Spark and optional LocalStack using PowerShell |
| [Remote Jupyter access](setup-jupyter-remote-access.md) | JupyterLab, password setup, network access, and troubleshooting |
| [PySpark for SQL users](sql_and_pyspark.md) | SQL-first PySpark workflow: views, validation, CASE, CTEs, joins, windows, parameters, and Parquet output |
| [PySpark 101 examples](pyspark-101-examples.md) | DataFrames, SQL, joins, windows, data quality, partitioning, and file writes |
| [Beginner use cases](spark-new-user-use-cases.md) | Ingestion and SQL-style transformation patterns |
| [Parquet performance and updates](parquet-spark-performance-and-updates.md) | File sizing, reads, slow jobs, partition updates, and table formats |
| [S3 ingestion pipeline walkthrough](etl_pipeline_walkthrough.md) | S3 → Lambda → SSM → Spark flow, minimal customer-ingestion code, and a LocalStack setup outline |
| [PySpark pipeline production design](pyspark-pipeline-production-design.md) | Raw, rejected, trusted, and transformed zones; Spark execution, quality rules, load patterns, and configuration |
| [Glue, Iceberg, Athena, and Redshift](aws_glue_iceberg/README.md) | Catalogs, snapshots, CRUD, PySpark upserts, and shared lake tables |

The pipeline guide introduces the orchestration pattern; the architecture guide expands the processing design. They contain illustrative code and configuration, rather than a complete deployed application. Create or supply the supporting job files, sample inputs, and AWS resources before running the examples.

## Local Files, LocalStack, and Real AWS

| Learning activity | What you need |
|---|---|
| Local-file PySpark examples | Local Spark setup; no AWS account or LocalStack |
| LocalStack S3 exercises | Docker and the optional LocalStack setup in your operating system's guide |
| Spark SQL catalog exercises | A configured Spark session; the setup guides use this for metadata practice without LocalStack Glue |
| AWS examples in the pipeline and Glue/Iceberg guides | The AWS resources, permissions, and runtime configuration specified by that guide |

For the LocalStack exercises:

- `lstk aws s3 ...` sends commands to the local emulator.
- `s3a://de-lab/...` reaches LocalStack when Spark uses the lab's endpoint configuration. The URI alone does not select LocalStack.
- LocalStack Glue availability depends on the license; the setup guides provide Spark SQL catalog exercises as an alternative.

The local labs do not require a real AWS account. The AWS reference sections describe real service behavior and are not automatically reproduced by the local emulator.

## What You Should Be Able to Do

After the core lessons, you should be able to:

- Read CSV, JSON, and Parquet, and translate common SQL into DataFrame operations.
- Explain `local[2]`, transformations versus actions, partitions, and `part-*` output files.
- Validate records and write trusted and rejected outputs.
- Describe incremental versus full loads and the roles of orchestration and Spark processing.
- Explain what Glue Catalog stores and how plain Parquet differs from transactional Iceberg tables.

The examples keep code small for learning. For production, consider retry-safe ingestion, schema validation, partition management, monitoring, access controls, and the table format's transaction and maintenance requirements.
