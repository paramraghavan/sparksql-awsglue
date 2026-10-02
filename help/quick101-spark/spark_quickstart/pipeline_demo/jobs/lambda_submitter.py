#!/usr/bin/env python3
"""Local stand-in for the S3-triggered Lambda submitter.

Lambda's responsibility is intentionally small:
- read the S3 event metadata
- validate that the landed raw key is supported
- submit an SSM command
- log the command id and exit

It does not run Spark, does not run DQ, and does not wait for Spark completion.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def find_supported_landing(allowed_config: dict, raw_key: str) -> dict | None:
    for landing in allowed_config["supported_landings"]:
        if raw_key.startswith(landing["raw_key_prefix"]):
            return landing
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit a supported raw landing key to the local SSM runner.")
    parser.add_argument("--bucket", default="company-raw", help="Raw bucket name from the S3 event.")
    parser.add_argument("--key", required=True, help="Raw object key, such as orders/orders_clean.csv.")
    parser.add_argument("--allowed-config", default="config/allowed_landings.json")
    parser.add_argument("--wait", action="store_true", help="Demo convenience: wait for the local SSM process.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    allowed_config = load_json(project_root / args.allowed_config)
    landing = find_supported_landing(allowed_config, args.key)
    run_id = str(int(time.time()))

    print(f"[lambda] event bucket={args.bucket} key={args.key} run_id={run_id}")

    if landing is None:
        print(f"[lambda] unsupported landing key: {args.key}")
        print("[lambda] no SSM command submitted")
        return 0

    raw_path = project_root / "data" / "raw" / args.key
    if not raw_path.exists():
        print(f"[lambda] supported key, but local demo file does not exist: {raw_path}", file=sys.stderr)
        return 1

    command = [
        sys.executable,
        str(project_root / "jobs" / "ssm_runner.py"),
        "--pipeline-config",
        landing["pipeline_config"],
        "--raw-path",
        str(raw_path),
        "--run-id",
        run_id,
    ]

    process = subprocess.Popen(command, cwd=str(project_root))
    print(f"[lambda] submitted local SSM command id={process.pid}")
    print("[lambda] exiting; SSM runner owns Spark submit, DQ, transform, and final status")

    if args.wait:
        return process.wait()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
