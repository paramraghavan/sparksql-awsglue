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
from pathlib import PurePosixPath


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def find_supported_landing(allowed_config: dict, raw_key: str) -> dict | None:
    # The allow-list is the first guardrail. Lambda should submit only known
    # landing folders, such as orders/ or house_price_growth/.
    for landing in allowed_config["supported_landings"]:
        if raw_key.startswith(landing["raw_key_prefix"]):
            return landing
    return None


def is_safe_s3_key(raw_key: str) -> bool:
    # S3 keys are not local file paths, but this local demo maps keys to files.
    # Reject path traversal patterns so a test key cannot escape data/raw.
    key = PurePosixPath(raw_key)
    return (
        raw_key
        and not raw_key.startswith("/")
        and "\\" not in raw_key
        and all(part not in {"", ".", ".."} for part in key.parts)
    )


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

    # Lambda validates only the landing key. It does not inspect records,
    # run DQ, start Spark directly, or wait for the business result.
    if not is_safe_s3_key(args.key):
        print(f"[lambda] invalid landing key: {args.key}", file=sys.stderr)
        print("[lambda] no SSM command submitted")
        return 1

    if landing is None:
        print(f"[lambda] unsupported landing key: {args.key}")
        print("[lambda] no SSM command submitted")
        return 0

    raw_base = (project_root / "data" / "raw").resolve()
    raw_path = (raw_base / args.key).resolve()
    if raw_base not in raw_path.parents:
        print(f"[lambda] invalid landing key outside raw base: {args.key}", file=sys.stderr)
        print("[lambda] no SSM command submitted")
        return 1

    if not raw_path.exists():
        print(f"[lambda] supported key, but local demo file does not exist: {raw_path}", file=sys.stderr)
        return 1

    # In real AWS this would be ssm:SendCommand. CLI shape:
    # aws ssm send-command --document-name AWS-RunShellScript --targets ...
    # Here it starts the local SSM stand-in process with the source-specific
    # config and raw file path.
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

    # --wait exists only to make the local training demo easier to observe.
    # Production Lambda should log the command id and return immediately.
    if args.wait:
        return process.wait()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
