#!/usr/bin/env python3
"""Config-driven PySpark ingestion with simple DQ checks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


TYPE_CASTS = {
    "integer": "int",
    "int": "int",
    "string": "string",
    "date": "date",
    "double": "double",
    "float": "double",
}


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def as_list(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def write_schema_failure(config: dict, run_id: str, missing_columns: list[str]) -> None:
    # Missing columns are a schema failure, not a row-level DQ failure.
    # There may be no safe row output to write, so create a small run artifact.
    rejected_path = Path(f"{config['targets']['rejected_path']}/run_id={run_id}")
    rejected_path.mkdir(parents=True, exist_ok=True)
    status_path = rejected_path / "_SCHEMA_FAILURE.txt"
    status_path.write_text(
        "Schema validation failed.\n"
        f"Missing required input columns: {', '.join(missing_columns)}\n",
        encoding="utf-8",
    )


def apply_derived_columns(df, config: dict):
    # Derived columns keep common partition fields, such as order_year, in config
    # instead of hardcoding them for every dataset.
    for column in config.get("trusted", {}).get("derived_columns", []):
        expression = column["expression"]
        source = column["source"]
        name = column["name"]

        if expression == "year":
            df = df.withColumn(name, F.year(F.col(source)))
        elif expression == "month":
            df = df.withColumn(name, F.month(F.col(source)))
        else:
            raise ValueError(f"Unsupported derived column expression: {expression}")

    return df


def existing_trusted_df(spark: SparkSession, trusted_path: str):
    # This local demo uses local paths. In AWS this would point to an S3 prefix.
    path = Path(trusted_path)
    if not path.exists():
        return None
    return spark.read.parquet(trusted_path)


def write_trusted_output(spark: SparkSession, trusted_df, config: dict) -> tuple[int, str]:
    trusted_path = config["targets"]["trusted_path"]
    strategy = config.get("load_strategy", {}).get("type", "full_overwrite")
    partition_by = config.get("trusted", {}).get("partition_by", [])

    output_df = trusted_df
    mode = "overwrite"

    if strategy == "incremental_append_new_keys":
        # Incremental append means "load new business keys only." It is not a
        # database MERGE. Existing keys are skipped, not updated.
        primary_key = as_list(config.get("dq_rules", {}).get("primary_key"))
        if not primary_key:
            raise ValueError("incremental_append_new_keys requires dq_rules.primary_key")

        existing_df = existing_trusted_df(spark, trusted_path)
        if existing_df is not None:
            # left_anti keeps rows from today's file that do not already exist
            # in trusted. SQL equivalent: where not exists (...).
            existing_keys = existing_df.select(*primary_key).dropDuplicates()
            output_df = trusted_df.join(existing_keys, primary_key, "left_anti")
        mode = "append"
    elif strategy == "full_overwrite":
        # Full load means today's file is the complete trusted table snapshot.
        # This is good for small reference/snapshot datasets, not large fact tables.
        mode = "overwrite"
    else:
        raise ValueError(f"Unsupported load_strategy.type: {strategy}")

    rows_to_write = output_df.count()
    if rows_to_write == 0:
        return 0, strategy

    writer = output_df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(trusted_path)
    return rows_to_write, strategy


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DQ and write trusted output.")
    parser.add_argument("--pipeline-config", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    config = load_json(Path(args.pipeline_config))
    spark = (
        SparkSession.builder
        .appName(f"{config['pipeline_name']}-ingestion")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    try:
        source_options = config["source"].get("options", {})
        reader = spark.read
        for key, value in source_options.items():
            reader = reader.option(key, str(value).lower() if isinstance(value, bool) else value)

        if config["source"]["format"] != "csv":
            raise ValueError("This quickstart implementation supports CSV input first.")

        raw_df = reader.csv(args.raw_path)
        expected_columns = [column["name"] for column in config["schema"]["columns"]]
        missing_columns = [column for column in expected_columns if column not in raw_df.columns]
        if missing_columns:
            write_schema_failure(config, args.run_id, missing_columns)
            raise ValueError(f"Missing required input columns: {missing_columns}")

        df = raw_df
        dq_errors = []

        # Cast every configured column into its trusted type. Invalid casts become
        # null, which lets the DQ rules detect bad dates, bad numbers, and so on.
        for column in config["schema"]["columns"]:
            name = column["name"]
            target_type = TYPE_CASTS[column["type"]]
            cast_col = f"_{name}_cast"

            if target_type == "date":
                df = df.withColumn(cast_col, F.to_date(F.col(name)))
            else:
                df = df.withColumn(cast_col, F.col(name).cast(target_type))

            if column.get("required", False):
                dq_errors.append(
                    F.when(F.col(name).isNull() | (F.trim(F.col(name).cast("string")) == ""), F.lit(f"missing_{name}"))
                )

            dq_errors.append(
                F.when(F.col(name).isNotNull() & F.col(cast_col).isNull(), F.lit(f"invalid_{name}"))
            )

        # Example business rule: amount-like columns cannot be negative.
        for column_name in config.get("dq_rules", {}).get("non_negative_columns", []):
            dq_errors.append(F.when(F.col(f"_{column_name}_cast") < 0, F.lit(f"negative_{column_name}")))

        # Duplicate checks run inside the incoming batch. For this beginner demo,
        # all duplicate-key rows are rejected so the user can inspect them.
        primary_key = as_list(config.get("dq_rules", {}).get("primary_key"))
        if primary_key and config.get("dq_rules", {}).get("duplicate_check", False):
            window = Window.partitionBy(*primary_key)
            df = df.withColumn("_pk_count", F.count("*").over(window))
            dq_errors.append(F.when(F.col("_pk_count") > 1, F.lit(f"duplicate_{'_'.join(primary_key)}")))

        df = df.withColumn("dq_error", F.concat_ws(",", *dq_errors))
        rejected_df = df.where(F.col("dq_error") != "")
        rejected_count = rejected_df.count()

        if rejected_count > 0:
            # Row-level DQ failures keep the original record plus dq_error, which
            # is the common pattern for support/debugging in data pipelines.
            rejected_path = f"{config['targets']['rejected_path']}/run_id={args.run_id}"
            (
                rejected_df
                .drop(*[f"_{column['name']}_cast" for column in config["schema"]["columns"]], "_pk_count")
                .write.mode("overwrite")
                .option("header", True)
                .csv(rejected_path)
            )
            print(f"[ingestion] DQ failed. rejected_count={rejected_count}")
            print(f"[ingestion] rejected_path={rejected_path}")
            return 2

        trusted_columns = []
        for column in config["schema"]["columns"]:
            name = column["name"]
            if name in config["trusted"]["interested_columns"]:
                trusted_columns.append(F.col(f"_{name}_cast").alias(name))

        # Trusted is the clean baseline: selected columns, correct data types,
        # lineage metadata, and partition columns.
        trusted_df = (
            df.select(*trusted_columns)
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("source_file", F.lit(args.raw_path))
            .withColumn("run_id", F.lit(args.run_id))
        )
        trusted_df = apply_derived_columns(trusted_df, config)

        rows_written, strategy = write_trusted_output(spark, trusted_df, config)

        print(f"[ingestion] DQ passed. load_strategy={strategy} rows_written={rows_written}")
        print(f"[ingestion] trusted_path={config['targets']['trusted_path']}")
        trusted_df.show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
