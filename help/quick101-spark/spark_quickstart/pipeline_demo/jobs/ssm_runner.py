#!/usr/bin/env python3
"""Local stand-in for AWS Systems Manager Run Command.

This script represents the shell command executed on the Spark host. It runs
spark-submit for ingestion, waits for completion, and optionally runs a second
spark-submit for transformation.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def spark_submit_command(project_root: Path, job_name: str, args: list[str]) -> list[str]:
    # Real SSM would execute a shell command on an EC2/EMR Spark host.
    # USE_PYTHON_SUBMIT=1 keeps the local demo simple when spark-submit is not on PATH.
    if os.environ.get("USE_PYTHON_SUBMIT") == "1":
        print("[ssm] USE_PYTHON_SUBMIT=1; running PySpark job with python")
        return [sys.executable, str(project_root / "jobs" / job_name), *args]

    spark_submit = os.environ.get("SPARK_SUBMIT") or shutil.which("spark-submit")
    job_path = project_root / "jobs" / job_name

    if spark_submit:
        return [spark_submit, str(job_path), *args]

    print("[ssm] spark-submit not found; falling back to python for local demo")
    return [sys.executable, str(job_path), *args]


def run_command(command: list[str], cwd: Path) -> int:
    print("[ssm] running:", " ".join(command))
    completed = subprocess.run(command, cwd=str(cwd), check=False)
    print(f"[ssm] exit_code={completed.returncode}")
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ingestion and optional transformation like an SSM command.")
    parser.add_argument("--pipeline-config", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / args.pipeline_config
    config = load_json(config_path)

    # Ingestion always runs first. It owns schema validation, row-level DQ,
    # rejected output, and writing the trusted baseline.
    ingestion_args = [
        "--pipeline-config",
        str(config_path),
        "--raw-path",
        args.raw_path,
        "--run-id",
        args.run_id,
    ]
    ingestion_command = spark_submit_command(project_root, "ingest_with_dq.py", ingestion_args)
    ingestion_status = run_command(ingestion_command, project_root)

    if ingestion_status != 0:
        print("[ssm] ingestion failed or completed with DQ failure; transformation skipped")
        return ingestion_status

    # Some datasets stop at trusted. Example: house_price_growth is a full-load
    # trusted baseline and does not need a transformed aggregate in this demo.
    if not config.get("transformation", {}).get("enabled", False):
        print("[ssm] transformation disabled; pipeline complete after trusted output")
        return 0

    # Transformation runs only after trusted data exists. This mirrors a common
    # production pattern: raw-to-trusted first, trusted-to-business-output second.
    transform_args = [
        "--pipeline-config",
        str(config_path),
        "--run-id",
        args.run_id,
    ]
    transform_command = spark_submit_command(project_root, "transform_trusted.py", transform_args)
    transform_status = run_command(transform_command, project_root)

    if transform_status != 0:
        print("[ssm] transformation failed")
        return transform_status

    print("[ssm] pipeline completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
