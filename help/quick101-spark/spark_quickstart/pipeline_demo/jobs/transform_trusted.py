#!/usr/bin/env python3
"""Config-driven PySpark transformation over trusted baseline data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run optional transformation over trusted data.")
    parser.add_argument("--pipeline-config", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    config = load_json(Path(args.pipeline_config))
    spark = (
        SparkSession.builder
        .appName(f"{config['pipeline_name']}-transformation")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    try:
        trusted_df = spark.read.parquet(config["targets"]["trusted_path"])

        transformed_df = (
            trusted_df
            .where(F.col("status") == "COMPLETE")
            .withColumn("order_month", F.month("order_date"))
            .withColumn(
                "amount_bucket",
                F.when(F.col("amount") >= 100, F.lit("high"))
                .when(F.col("amount") >= 25, F.lit("medium"))
                .otherwise(F.lit("low")),
            )
            .groupBy("category", "order_year", "order_month", "amount_bucket")
            .agg(
                F.count("*").alias("order_count"),
                F.round(F.sum("amount"), 2).alias("total_amount"),
                F.round(F.avg("amount"), 2).alias("avg_amount"),
            )
            .withColumn("run_id", F.lit(args.run_id))
            .orderBy("category", "order_year", "order_month", "amount_bucket")
        )

        writer = transformed_df.write.mode(config["transformation"].get("output_mode", "overwrite"))
        partition_by = config["transformation"].get("partition_by", [])
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(config["targets"]["transformed_path"])

        print(f"[transform] transformed_path={config['targets']['transformed_path']}")
        transformed_df.show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
