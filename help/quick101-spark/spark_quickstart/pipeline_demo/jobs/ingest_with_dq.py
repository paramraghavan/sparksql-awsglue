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
            raise ValueError(f"Missing required input columns: {missing_columns}")

        df = raw_df
        dq_errors = []

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

        for column_name in config.get("dq_rules", {}).get("non_negative_columns", []):
            dq_errors.append(F.when(F.col(f"_{column_name}_cast") < 0, F.lit(f"negative_{column_name}")))

        primary_key = config.get("dq_rules", {}).get("primary_key")
        if primary_key and config.get("dq_rules", {}).get("duplicate_check", False):
            window = Window.partitionBy(primary_key)
            df = df.withColumn("_pk_count", F.count("*").over(window))
            dq_errors.append(F.when(F.col("_pk_count") > 1, F.lit(f"duplicate_{primary_key}")))

        df = df.withColumn("dq_error", F.concat_ws(",", *dq_errors))
        rejected_df = df.where(F.col("dq_error") != "")
        rejected_count = rejected_df.count()

        if rejected_count > 0:
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

        trusted_df = (
            df.select(*trusted_columns)
            .withColumn("order_year", F.year("order_date"))
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
            .withColumn("run_id", F.lit(args.run_id))
        )

        trusted_path = config["targets"]["trusted_path"]
        writer = trusted_df.write.mode("overwrite")
        partition_by = config.get("trusted", {}).get("partition_by", [])
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(trusted_path)

        print(f"[ingestion] DQ passed. trusted_path={trusted_path}")
        trusted_df.show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
