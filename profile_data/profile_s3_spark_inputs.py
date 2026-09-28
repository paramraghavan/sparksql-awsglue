#!/usr/bin/env python3
"""
Profile S3 datasets before running large Spark/EMR/Glue jobs.

This script inspects S3 file layout and produces practical Spark sizing and
partitioning recommendations. It is intentionally S3-only: it does not need a
Spark cluster and it does not read the actual data content.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse


SPLITTABLE_EXTENSIONS = {
    ".parquet",
    ".orc",
    ".avro",
    ".csv",
    ".txt",
    ".json",
    ".tsv",
    ".bz2",
}

UNSPLITTABLE_EXTENSIONS = {
    ".gz",
    ".gzip",
    ".zip",
}

TEXT_EXTENSIONS = {".csv", ".txt", ".json", ".tsv"}
COLUMNAR_EXTENSIONS = {".parquet", ".orc"}


@dataclass
class ClusterConfig:
    worker_nodes: int
    worker_cores: int
    worker_memory_gb: float
    executor_cores: int
    memory_overhead_fraction: float
    reserve_memory_gb_per_node: float

    @property
    def total_worker_cores(self) -> int:
        return self.worker_nodes * self.worker_cores

    @property
    def usable_memory_gb_per_node(self) -> float:
        return max(self.worker_memory_gb - self.reserve_memory_gb_per_node, 1.0)

    @property
    def executors_per_node(self) -> int:
        return max(self.worker_cores // self.executor_cores, 1)

    @property
    def total_executors(self) -> int:
        return self.worker_nodes * self.executors_per_node

    @property
    def executor_memory_gb(self) -> float:
        raw = self.usable_memory_gb_per_node / self.executors_per_node
        return max(raw * (1.0 - self.memory_overhead_fraction), 1.0)

    @property
    def executor_overhead_gb(self) -> float:
        raw = self.usable_memory_gb_per_node / self.executors_per_node
        return max(raw * self.memory_overhead_fraction, 0.5)


@dataclass
class FileStats:
    table: str
    path: str
    total_files: int
    data_files: int
    total_size_bytes: int
    avg_file_size_bytes: int
    max_file_size_bytes: int
    max_file_key: str
    extension_counts: Dict[str, int]
    compression_counts: Dict[str, int]
    likely_format: str
    splittability_risk: str
    estimated_input_partitions: int
    recommended_partitions_256mb: int
    recommended_partitions_512mb: int
    recommendation: str


@dataclass
class SparkConfigRecommendation:
    driver_cores: int
    driver_memory_gb: int
    executor_instances: int
    executor_cores: int
    executor_memory_gb: int
    executor_memory_overhead_gb: int
    default_parallelism: int
    shuffle_partitions: int
    notes: List[str]


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Not a valid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def human_bytes(num_bytes: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} PB"


def extension_for_key(key: str) -> str:
    lower = key.lower()
    for ext in [".csv.gz", ".json.gz", ".tsv.gz", ".txt.gz"]:
        if lower.endswith(ext):
            return ext
    suffix = Path(lower).suffix
    return suffix or "<none>"


def compression_for_key(key: str) -> str:
    lower = key.lower()
    if lower.endswith((".gz", ".gzip")):
        return "gzip"
    if lower.endswith(".bz2"):
        return "bzip2"
    if lower.endswith(".snappy") or ".snappy." in lower:
        return "snappy"
    if lower.endswith(".zip"):
        return "zip"
    return "none/unknown"


def is_data_file(key: str, size: int) -> bool:
    name = Path(key).name
    if size <= 0:
        return False
    if name.startswith("_") or name.startswith("."):
        return False
    if name in {"_SUCCESS", "_started", "_committed"}:
        return False
    return True


def read_tables_file(path: str) -> List[Tuple[str, str]]:
    tables: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                table, uri = [part.strip() for part in line.split(",", 1)]
            else:
                uri = line
                table = uri.rstrip("/").split("/")[-1] or f"table_{line_no}"
            if not table or not uri:
                raise ValueError(f"Invalid line {line_no} in {path}: {raw!r}")
            tables.append((table, uri))
    return tables


def list_s3_objects(s3_client, bucket: str, prefix: str) -> Iterable[Dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            yield obj


def infer_format(extension_counts: Dict[str, int]) -> str:
    if not extension_counts:
        return "unknown"
    ext = max(extension_counts.items(), key=lambda kv: kv[1])[0]
    if ext in {".csv.gz", ".json.gz", ".tsv.gz", ".txt.gz"}:
        return ext.lstrip(".")
    return ext.lstrip(".") or "unknown"


def estimate_input_partitions(
    total_size_bytes: int,
    data_files: int,
    max_partition_bytes: int,
    extension_counts: Dict[str, int],
) -> int:
    if data_files == 0 or total_size_bytes == 0:
        return 0
    has_unsplittable = any(
        ext in UNSPLITTABLE_EXTENSIONS or ext.endswith(".gz") or ext == ".zip"
        for ext in extension_counts
    )
    if has_unsplittable:
        return data_files
    return max(math.ceil(total_size_bytes / max_partition_bytes), data_files)


def classify_splittability(extension_counts: Dict[str, int]) -> str:
    if not extension_counts:
        return "unknown"
    extensions = set(extension_counts)
    if any(ext.endswith(".gz") or ext == ".zip" for ext in extensions):
        return "high risk: gzip/zip files are usually not splittable"
    if extensions & COLUMNAR_EXTENSIONS:
        return "low risk: columnar format is usually splittable"
    if extensions & TEXT_EXTENSIONS:
        return "medium: uncompressed text is usually splittable but inefficient"
    return "unknown: verify format and compression"


def build_recommendation(
    stats: FileStats,
    target_partition_mb: int,
    cluster: ClusterConfig,
) -> str:
    size_gb = stats.total_size_bytes / (1024**3)
    avg_mb = stats.avg_file_size_bytes / (1024**2) if stats.avg_file_size_bytes else 0
    max_gb = stats.max_file_size_bytes / (1024**3) if stats.max_file_size_bytes else 0
    parallelism_floor = cluster.total_worker_cores * 2
    recommended = max(stats.recommended_partitions_256mb, parallelism_floor)

    notes: List[str] = []
    if stats.data_files == 0:
        return "No data files found under this path."

    if "high risk" in stats.splittability_risk:
        notes.append(
            "High risk layout: one or more gzip/zip files may be unsplittable. "
            "If reads are slow or one Spark task does most work, split/decompress/re-export before Spark repartition."
        )
    if stats.data_files < cluster.total_worker_cores:
        notes.append(
            f"Few files for cluster size: {stats.data_files} data files vs "
            f"{cluster.total_worker_cores} worker cores. Parallelism may be low."
        )
    if max_gb >= 5 and "high risk" in stats.splittability_risk:
        notes.append(
            f"Largest file is {human_bytes(stats.max_file_size_bytes)} and may become one large task."
        )
    if avg_mb < 32 and stats.data_files > 1000:
        notes.append(
            "Small-file risk: many tiny files can create scheduling/listing overhead. Consider compaction."
        )
    if stats.estimated_input_partitions < parallelism_floor and size_gb >= 10:
        notes.append(
            f"Estimated input partitions ({stats.estimated_input_partitions}) are below a healthy starting parallelism "
            f"({parallelism_floor}). Repartition after read if the input is readable."
        )

    notes.append(
        f"Suggested partition target: {target_partition_mb} MB. "
        f"Start around {recommended} partitions for this table if a shuffle/rewrite is needed."
    )

    if stats.likely_format not in {"parquet", "orc"}:
        notes.append("For repeated analytics, rewrite curated output as Parquet/ORC with Snappy.")
    else:
        notes.append("Parquet/ORC layout looks generally suitable; validate Spark UI for skew and task imbalance.")

    return " ".join(notes)


def round_up_gb(value: float) -> int:
    return max(int(math.ceil(value)), 1)


def recommend_driver_memory_gb(stats: List[FileStats]) -> int:
    total_files = sum(item.data_files for item in stats)
    total_size_gb = sum(item.total_size_bytes for item in stats) / (1024**3)
    table_count = len(stats)

    if table_count >= 20 or total_files >= 100_000 or total_size_gb >= 2_000:
        return 16
    if table_count >= 10 or total_files >= 25_000 or total_size_gb >= 500:
        return 8
    return 4


def recommend_driver_cores(stats: List[FileStats]) -> int:
    total_files = sum(item.data_files for item in stats)
    table_count = len(stats)
    if table_count >= 20 or total_files >= 50_000:
        return 4
    return 2


def recommend_shuffle_partitions(stats: List[FileStats], cluster: ClusterConfig) -> int:
    total_256mb_partitions = sum(item.recommended_partitions_256mb for item in stats)
    core_floor = cluster.total_worker_cores * 2
    core_ceiling = cluster.total_worker_cores * 6
    if total_256mb_partitions <= 0:
        return core_floor
    return max(core_floor, min(total_256mb_partitions, core_ceiling))


def build_spark_config_recommendation(
    stats: List[FileStats],
    cluster: ClusterConfig,
) -> SparkConfigRecommendation:
    total_files = sum(item.data_files for item in stats)
    risky_tables = [
        item.table
        for item in stats
        if "high risk" in item.splittability_risk or item.estimated_input_partitions < cluster.total_worker_cores
    ]
    driver_memory_gb = recommend_driver_memory_gb(stats)
    driver_cores = recommend_driver_cores(stats)
    shuffle_partitions = recommend_shuffle_partitions(stats, cluster)
    default_parallelism = max(cluster.total_worker_cores * 2, shuffle_partitions)

    notes = [
        "These are starting values. Validate with Spark UI, YARN metrics, spill, GC, and executor failures.",
        "Driver memory helps with planning, metadata, and scheduling; it does not fix executor OOM from large partitions.",
    ]
    if total_files >= 25_000:
        notes.append("Large file count detected; driver memory is increased for metadata/task scheduling pressure.")
    if risky_tables:
        notes.append(
            "Some tables have low parallelism or unsplittable-file risk: "
            + ", ".join(risky_tables)
            + ". Fix file layout before relying on repartition."
        )

    return SparkConfigRecommendation(
        driver_cores=driver_cores,
        driver_memory_gb=driver_memory_gb,
        executor_instances=cluster.total_executors,
        executor_cores=cluster.executor_cores,
        executor_memory_gb=round_up_gb(cluster.executor_memory_gb),
        executor_memory_overhead_gb=round_up_gb(cluster.executor_overhead_gb),
        default_parallelism=default_parallelism,
        shuffle_partitions=shuffle_partitions,
        notes=notes,
    )


def profile_table(
    s3_client,
    table: str,
    uri: str,
    max_partition_bytes: int,
    target_partition_mb: int,
    cluster: ClusterConfig,
) -> FileStats:
    bucket, prefix = parse_s3_uri(uri)
    total_files = 0
    data_files = 0
    total_size = 0
    max_size = 0
    max_key = ""
    extension_counts: Dict[str, int] = {}
    compression_counts: Dict[str, int] = {}

    for obj in list_s3_objects(s3_client, bucket, prefix):
        total_files += 1
        key = obj["Key"]
        size = int(obj["Size"])
        if not is_data_file(key, size):
            continue
        data_files += 1
        total_size += size
        ext = extension_for_key(key)
        compression = compression_for_key(key)
        extension_counts[ext] = extension_counts.get(ext, 0) + 1
        compression_counts[compression] = compression_counts.get(compression, 0) + 1
        if size > max_size:
            max_size = size
            max_key = key

    avg_size = int(total_size / data_files) if data_files else 0
    likely_format = infer_format(extension_counts)
    splittability_risk = classify_splittability(extension_counts)
    estimated_input_partitions = estimate_input_partitions(
        total_size, data_files, max_partition_bytes, extension_counts
    )
    recommended_256 = max(math.ceil(total_size / (256 * 1024 * 1024)), 1) if total_size else 0
    recommended_512 = max(math.ceil(total_size / (512 * 1024 * 1024)), 1) if total_size else 0

    stats = FileStats(
        table=table,
        path=uri,
        total_files=total_files,
        data_files=data_files,
        total_size_bytes=total_size,
        avg_file_size_bytes=avg_size,
        max_file_size_bytes=max_size,
        max_file_key=max_key,
        extension_counts=extension_counts,
        compression_counts=compression_counts,
        likely_format=likely_format,
        splittability_risk=splittability_risk,
        estimated_input_partitions=estimated_input_partitions,
        recommended_partitions_256mb=recommended_256,
        recommended_partitions_512mb=recommended_512,
        recommendation="",
    )
    stats.recommendation = build_recommendation(stats, target_partition_mb, cluster)
    return stats


def write_csv_report(stats: List[FileStats], output_path: str) -> None:
    fields = [
        "table",
        "path",
        "data_files",
        "total_size",
        "avg_file_size",
        "max_file_size",
        "likely_format",
        "splittability_risk",
        "estimated_input_partitions",
        "recommended_partitions_256mb",
        "recommended_partitions_512mb",
        "recommendation",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for item in stats:
            writer.writerow(
                {
                    "table": item.table,
                    "path": item.path,
                    "data_files": item.data_files,
                    "total_size": human_bytes(item.total_size_bytes),
                    "avg_file_size": human_bytes(item.avg_file_size_bytes),
                    "max_file_size": human_bytes(item.max_file_size_bytes),
                    "likely_format": item.likely_format,
                    "splittability_risk": item.splittability_risk,
                    "estimated_input_partitions": item.estimated_input_partitions,
                    "recommended_partitions_256mb": item.recommended_partitions_256mb,
                    "recommended_partitions_512mb": item.recommended_partitions_512mb,
                    "recommendation": item.recommendation,
                }
            )


def write_json_report(
    stats: List[FileStats],
    cluster: ClusterConfig,
    spark_config: SparkConfigRecommendation,
    output_path: str,
) -> None:
    payload = {
        "cluster_recommendation": {
            **asdict(cluster),
            "executors_per_node": cluster.executors_per_node,
            "total_executors": cluster.total_executors,
            "executor_memory_gb": round(cluster.executor_memory_gb, 2),
            "executor_overhead_gb": round(cluster.executor_overhead_gb, 2),
            "total_worker_cores": cluster.total_worker_cores,
        },
        "spark_submit_recommendation": asdict(spark_config),
        "tables": [asdict(item) for item in stats],
    }
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def write_markdown_report(
    stats: List[FileStats],
    cluster: ClusterConfig,
    spark_config: SparkConfigRecommendation,
    output_path: str,
) -> None:
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write("# Spark Input Profile Report\n\n")
        handle.write("## Cluster Starting Point\n\n")
        handle.write(f"- Worker nodes: `{cluster.worker_nodes}`\n")
        handle.write(f"- Worker cores per node: `{cluster.worker_cores}`\n")
        handle.write(f"- Worker memory per node: `{cluster.worker_memory_gb} GB`\n")
        handle.write(f"- Suggested executor cores: `{cluster.executor_cores}`\n")
        handle.write(f"- Estimated executors per node: `{cluster.executors_per_node}`\n")
        handle.write(f"- Estimated total executors: `{cluster.total_executors}`\n")
        handle.write(f"- Estimated executor memory: `{cluster.executor_memory_gb:.1f} GB`\n")
        handle.write(f"- Estimated executor memory overhead: `{cluster.executor_overhead_gb:.1f} GB`\n\n")
        handle.write("> Treat these as starting values. Validate with Spark UI, YARN metrics, spill, GC, and runtime.\n\n")

        handle.write("## Spark Submit Starting Recommendation\n\n")
        handle.write("| Setting | Suggested Value |\n")
        handle.write("|---|---:|\n")
        handle.write(f"| `spark.driver.cores` | `{spark_config.driver_cores}` |\n")
        handle.write(f"| `spark.driver.memory` | `{spark_config.driver_memory_gb}g` |\n")
        handle.write(f"| `spark.executor.instances` | `{spark_config.executor_instances}` |\n")
        handle.write(f"| `spark.executor.cores` | `{spark_config.executor_cores}` |\n")
        handle.write(f"| `spark.executor.memory` | `{spark_config.executor_memory_gb}g` |\n")
        handle.write(f"| `spark.executor.memoryOverhead` | `{spark_config.executor_memory_overhead_gb}g` |\n")
        handle.write(f"| `spark.default.parallelism` | `{spark_config.default_parallelism}` |\n")
        handle.write(f"| `spark.sql.shuffle.partitions` | `{spark_config.shuffle_partitions}` |\n\n")

        handle.write("Example `spark-submit` options:\n\n")
        handle.write("```bash\n")
        handle.write(f"--driver-cores {spark_config.driver_cores} \\\n")
        handle.write(f"--driver-memory {spark_config.driver_memory_gb}g \\\n")
        handle.write(f"--num-executors {spark_config.executor_instances} \\\n")
        handle.write(f"--executor-cores {spark_config.executor_cores} \\\n")
        handle.write(f"--executor-memory {spark_config.executor_memory_gb}g \\\n")
        handle.write(f"--conf spark.executor.memoryOverhead={spark_config.executor_memory_overhead_gb}g \\\n")
        handle.write(f"--conf spark.default.parallelism={spark_config.default_parallelism} \\\n")
        handle.write(f"--conf spark.sql.shuffle.partitions={spark_config.shuffle_partitions}\n")
        handle.write("```\n\n")

        handle.write("Notes:\n\n")
        for note in spark_config.notes:
            handle.write(f"- {note}\n")
        handle.write("\n")

        handle.write("## Caveats\n\n")
        handle.write("- These recommendations are based on S3 object layout, file sizes, extensions, and cluster shape only.\n")
        handle.write("- The profiler does not know row counts, join keys, data skew, shuffle volume, UDF cost, or transformation complexity.\n")
        handle.write("- Driver memory helps with planning, metadata, and scheduling; it does not fix executor OOM from large partitions.\n")
        handle.write("- `repartition()` helps only after Spark can read the input; it does not fix the initial read of a huge unsplittable `.gz` file.\n")
        handle.write("- `spark.sql.shuffle.partitions` is a starting point. Join-heavy and aggregation-heavy jobs may need different values.\n")
        handle.write("- If dynamic allocation is enabled, fixed `--num-executors` may not be the final runtime executor count.\n")
        handle.write("- Validate with Spark UI, event logs, spill metrics, GC time, executor failures, and YARN resource usage.\n\n")

        handle.write("## Table Summary\n\n")
        handle.write(
            "| Table | Size | Files | Avg File | Max File | Format | Estimated Input Partitions | Risk |\n"
        )
        handle.write("|---|---:|---:|---:|---:|---|---:|---|\n")
        for item in stats:
            handle.write(
                f"| {item.table} | {human_bytes(item.total_size_bytes)} | {item.data_files} | "
                f"{human_bytes(item.avg_file_size_bytes)} | {human_bytes(item.max_file_size_bytes)} | "
                f"{item.likely_format} | {item.estimated_input_partitions} | {item.splittability_risk} |\n"
            )

        handle.write("\n## Recommendations\n\n")
        for item in stats:
            handle.write(f"### {item.table}\n\n")
            handle.write(f"- Path: `{item.path}`\n")
            handle.write(f"- Total size: `{human_bytes(item.total_size_bytes)}`\n")
            handle.write(f"- Data files: `{item.data_files}`\n")
            handle.write(f"- Largest file: `{human_bytes(item.max_file_size_bytes)}`\n")
            if item.max_file_key:
                handle.write(f"- Largest file key: `{item.max_file_key}`\n")
            handle.write(f"- Extensions: `{item.extension_counts}`\n")
            handle.write(f"- Compression: `{item.compression_counts}`\n")
            handle.write(f"- Estimated input partitions: `{item.estimated_input_partitions}`\n")
            handle.write(f"- Suggested partitions at 256 MB: `{item.recommended_partitions_256mb}`\n")
            handle.write(f"- Suggested partitions at 512 MB: `{item.recommended_partitions_512mb}`\n")
            handle.write(f"- Recommendation: {item.recommendation}\n\n")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile S3 table layouts and recommend Spark/EMR partitioning starting points."
    )
    parser.add_argument("--tables-file", required=True, help="File with table_name,s3://bucket/prefix per line.")
    parser.add_argument("--output-dir", default="profile_output", help="Directory for report files.")
    parser.add_argument("--region", default=None, help="AWS region. Defaults to boto3 environment/profile.")
    parser.add_argument("--target-partition-mb", type=int, default=256, help="Target Spark partition size in MB.")
    parser.add_argument(
        "--spark-max-partition-mb",
        type=int,
        default=128,
        help="Spark input split estimate, similar to spark.sql.files.maxPartitionBytes.",
    )
    parser.add_argument("--worker-nodes", type=int, required=True, help="Number of EMR/Glue worker nodes.")
    parser.add_argument("--worker-cores", type=int, required=True, help="CPU cores per worker node.")
    parser.add_argument("--worker-memory-gb", type=float, required=True, help="Memory GB per worker node.")
    parser.add_argument("--executor-cores", type=int, default=4, help="Suggested executor cores.")
    parser.add_argument(
        "--memory-overhead-fraction",
        type=float,
        default=0.15,
        help="Fraction of executor container memory reserved for overhead.",
    )
    parser.add_argument(
        "--reserve-memory-gb-per-node",
        type=float,
        default=8.0,
        help="Memory to reserve per node for OS/YARN/EMR daemons.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cluster = ClusterConfig(
        worker_nodes=args.worker_nodes,
        worker_cores=args.worker_cores,
        worker_memory_gb=args.worker_memory_gb,
        executor_cores=args.executor_cores,
        memory_overhead_fraction=args.memory_overhead_fraction,
        reserve_memory_gb_per_node=args.reserve_memory_gb_per_node,
    )

    try:
        import boto3
    except ImportError:
        print(
            "Missing dependency: boto3. Install it with `pip install boto3` "
            "or `pip install -r requirements.txt`.",
            file=sys.stderr,
        )
        return 2

    session = boto3.Session(region_name=args.region)
    s3_client = session.client("s3")
    tables = read_tables_file(args.tables_file)

    if not tables:
        print(f"No tables found in {args.tables_file}", file=sys.stderr)
        return 2

    max_partition_bytes = args.spark_max_partition_mb * 1024 * 1024
    stats: List[FileStats] = []
    for table, uri in tables:
        print(f"Profiling {table}: {uri}")
        stats.append(
            profile_table(
                s3_client=s3_client,
                table=table,
                uri=uri,
                max_partition_bytes=max_partition_bytes,
                target_partition_mb=args.target_partition_mb,
                cluster=cluster,
            )
        )

    spark_config = build_spark_config_recommendation(stats, cluster)

    write_csv_report(stats, str(output_dir / "table_profile_report.csv"))
    write_json_report(stats, cluster, spark_config, str(output_dir / "spark_recommendations.json"))
    write_markdown_report(stats, cluster, spark_config, str(output_dir / "table_profile_report.md"))

    print(f"\nWrote reports to {output_dir}")
    print(f"- {output_dir / 'table_profile_report.md'}")
    print(f"- {output_dir / 'table_profile_report.csv'}")
    print(f"- {output_dir / 'spark_recommendations.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
