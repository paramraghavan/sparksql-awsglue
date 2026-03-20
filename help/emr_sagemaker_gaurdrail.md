# EMR Framework for SageMaker Studio

**Automated pre-flight checks, runtime guardrails, and friendly error messages for data scientists using Jupyter notebooks connected to Amazon EMR.**

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [IAM Policies](#iam-policies)
5. [Phase 0 — S3 Setup](#phase-0--s3-setup)
6. [Phase 1 — Python Modules](#phase-1--python-modules)
7. [Phase 2 — Dr. Elephant on EMR](#phase-2--dr-elephant-on-emr)
8. [Phase 3 — SageMaker Studio Lifecycle Config](#phase-3--sagemaker-studio-lifecycle-config)
9. [Updating the Framework](#updating-the-framework)
10. [Updating the EMR DNS Variable](#updating-the-emr-dns-variable)
11. [Troubleshooting](#troubleshooting)
12. [Variable Reference](#variable-reference)

---

## Overview

Data scientists using Jupyter notebooks on SageMaker Studio to connect to Amazon EMR frequently run into resource errors (out of memory, shuffle failures, disk exhaustion) that require data engineer intervention to diagnose. This framework removes that dependency by:

- **Intercepting** cells before they run and warning about bad Spark configs
- **Watching** jobs while they run via Dr. Elephant (automatic severity scoring)
- **Translating** raw Spark/YARN Java errors into plain-English explanations with fix steps
- **Auto-deploying** everything to every user via SageMaker Studio Lifecycle Configs — zero setup required from data scientists

---

## Architecture

```
S3 Bucket (YOUR-BUCKET)
└── emr-framework/
    ├── config/
    │   └── emr_config.env          ← single source of truth for DNS + settings
    ├── modules/
    │   ├── emr_config.py           ← reads env file, exports all constants
    │   ├── emr_preflight.py        ← YARN checks, cluster_status()
    │   ├── emr_errors.py           ← error translation dictionary
    │   ├── emr_advisor.py          ← Dr. Elephant bridge, check_my_last_job()
    │   └── emr_hooks.py            ← IPython kernel wiring
    └── bootstrap/
        └── install_dr_elephant.sh  ← EMR bootstrap action

SageMaker Studio (per-user kernel)
└── KernelGateway Lifecycle Config
    └── studio_lifecycle.sh
        ├── aws s3 sync → /home/sagemaker-user/emr_framework/
        └── IPython startup → 00_emr_framework.py (auto-activates on kernel start)

EMR Cluster (master node)
└── Dr. Elephant (port 8080)        ← analyzes YARN + Spark History Server jobs
└── YARN ResourceManager (port 8088) ← queried for live cluster metrics
```

**Key design principle:** The only file you ever edit when your cluster changes is `emr_config.env` in S3. Every other component reads from it. No user machines are touched directly.

---

## Prerequisites

Before starting, confirm the following:

| Requirement | Details |
|---|---|
| SageMaker Studio Domain | Studio (not classic notebook instances) |
| EMR cluster | Running in the same VPC as your Studio Domain |
| S3 bucket | Accessible from your SageMaker execution role |
| AWS CLI | Configured locally with admin or sufficient permissions |
| EMR master DNS | Private DNS or IP of your EMR master node |

> **Finding your EMR master DNS:** AWS Console → EMR → your cluster → Summary tab → "Master public DNS". Despite the label, this resolves to the private IP when called from within the same VPC.

---

## IAM Policies

Two IAM updates are required: one for the **SageMaker execution role** (used by Studio users) and one for the **EMR cluster role** (optional, for bootstrap action S3 access).

---

### Policy 1 — SageMaker Studio Execution Role

Attach this policy to the IAM role that your SageMaker Studio users assume. You can find this role in: AWS Console → SageMaker → Domains → your domain → "Execution role".

**Policy name (suggested):** `EMRFrameworkSageMakerPolicy`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadEMRFrameworkFromS3",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET",
        "arn:aws:s3:::YOUR-BUCKET/emr-framework/*"
      ]
    },
    {
      "Sid": "DescribeEMRCluster",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:ListInstances"
      ],
      "Resource": "arn:aws:elasticmapreduce:YOUR-REGION:YOUR-ACCOUNT-ID:cluster/*"
    }
  ]
}
```

**How to attach:**

```bash
# Create the policy
aws iam create-policy \
  --policy-name EMRFrameworkSageMakerPolicy \
  --policy-document file://sagemaker_policy.json

# Attach to your SageMaker execution role
aws iam attach-role-policy \
  --role-name YOUR-SAGEMAKER-EXECUTION-ROLE-NAME \
  --policy-arn arn:aws:iam::YOUR-ACCOUNT-ID:policy/EMRFrameworkSageMakerPolicy
```

> Replace `YOUR-BUCKET`, `YOUR-REGION`, `YOUR-ACCOUNT-ID`, and `YOUR-SAGEMAKER-EXECUTION-ROLE-NAME` with your actual values.

---

### Policy 2 — EMR EC2 Instance Profile (Bootstrap Action S3 Access)

The EMR master node needs to read the bootstrap script from S3. This is usually already allowed by your default EMR EC2 instance profile (`EMR_EC2_DefaultRole`), but if not, add this statement.

**Policy name (suggested):** `EMRFrameworkBootstrapPolicy`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadBootstrapFromS3",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET/emr-framework/bootstrap/*"
      ]
    }
  ]
}
```

**How to attach:**

```bash
# Create the policy
aws iam create-policy \
  --policy-name EMRFrameworkBootstrapPolicy \
  --policy-document file://emr_bootstrap_policy.json

# Attach to your EMR EC2 instance profile role (default name shown below)
aws iam attach-role-policy \
  --role-name EMR_EC2_DefaultRole \
  --policy-arn arn:aws:iam::YOUR-ACCOUNT-ID:policy/EMRFrameworkBootstrapPolicy
```

---

### Policy 3 — SageMaker Lifecycle Config Execution

The lifecycle config script itself runs under a service role. Ensure the SageMaker service role can create and attach lifecycle configs.

**Statement to add to your SageMaker admin role:**

```json
{
  "Sid": "ManageStudioLifecycleConfigs",
  "Effect": "Allow",
  "Action": [
    "sagemaker:CreateStudioLifecycleConfig",
    "sagemaker:UpdateDomain",
    "sagemaker:UpdateUserProfile",
    "sagemaker:DescribeDomain",
    "sagemaker:ListStudioLifecycleConfigs"
  ],
  "Resource": "*"
}
```

---

## Phase 0 — S3 Setup

Run this once to create the folder structure in your S3 bucket.

```bash
# Replace YOUR-BUCKET throughout this entire guide
BUCKET=YOUR-BUCKET

aws s3api put-object --bucket $BUCKET --key emr-framework/
aws s3api put-object --bucket $BUCKET --key emr-framework/config/
aws s3api put-object --bucket $BUCKET --key emr-framework/modules/
aws s3api put-object --bucket $BUCKET --key emr-framework/bootstrap/
```

### Create and upload `emr_config.env`

This is the only file you edit when your EMR cluster changes.

Create a file called `emr_config.env` with the following content:

```bash
# EMR Framework Configuration
# ─────────────────────────────────────────────────────────────────────────────
# REPLACE all values marked REPLACE_WITH_... before uploading.

# EMR master node DNS or private IP
# Find at: AWS Console → EMR → your cluster → Summary → "Master public DNS"
EMR_MASTER_DNS=REPLACE_WITH_YOUR_EMR_MASTER_DNS

# YARN ResourceManager port (default: 8088 — only change if custom)
YARN_PORT=8088

# Dr. Elephant port (default: 8080)
DR_ELEPHANT_PORT=8080

# Your EMR cluster ID (e.g. j-XXXXXXXXX) — visible in EMR console URL
EMR_CLUSTER_ID=REPLACE_WITH_YOUR_CLUSTER_ID

# AWS region your cluster runs in
AWS_REGION=us-east-1

# Resource limits — tune to match your EMR node type
# Check node type in: EMR console → your cluster → Hardware tab
MAX_EXECUTOR_MEMORY_GB=48
MAX_EXECUTOR_COUNT=50
MAX_SHUFFLE_PARTITIONS=2000
```

Upload to S3:

```bash
aws s3 cp emr_config.env s3://YOUR-BUCKET/emr-framework/config/emr_config.env
```

---

## Phase 1 — Python Modules

Create each file below locally, then upload all five to S3 in one command.

### `emr_config.py`

Reads `emr_config.env` and exposes all settings as importable constants. All other modules import from here — the DNS string appears nowhere else.

```python
import os

_ENV_PATH = "/home/sagemaker-user/emr_framework/emr_config.env"

def _load_env(path):
    cfg = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return cfg

_cfg = _load_env(_ENV_PATH)

def _get(key, default=""):
    return _cfg.get(key) or os.environ.get(key) or default

EMR_MASTER_DNS         = _get("EMR_MASTER_DNS",         "REPLACE_WITH_YOUR_EMR_MASTER_DNS")
YARN_PORT              = int(_get("YARN_PORT",           "8088"))
DR_ELEPHANT_PORT       = int(_get("DR_ELEPHANT_PORT",   "8080"))
EMR_CLUSTER_ID         = _get("EMR_CLUSTER_ID",         "REPLACE_WITH_YOUR_CLUSTER_ID")
AWS_REGION             = _get("AWS_REGION",             "us-east-1")
MAX_EXECUTOR_MEMORY_GB = int(_get("MAX_EXECUTOR_MEMORY_GB",  "48"))
MAX_EXECUTOR_COUNT     = int(_get("MAX_EXECUTOR_COUNT",      "50"))
MAX_SHUFFLE_PARTITIONS = int(_get("MAX_SHUFFLE_PARTITIONS",  "2000"))

YARN_METRICS_URL     = f"http://{EMR_MASTER_DNS}:{YARN_PORT}/ws/v1/cluster/metrics"
DR_ELEPHANT_BASE_URL = f"http://{EMR_MASTER_DNS}:{DR_ELEPHANT_PORT}"
```

---

### `emr_preflight.py`

Validates Spark resource configs in cells before they run. Also exposes `cluster_status()` for data scientists to call on demand.

```python
import re
import requests
from emr_config import (
    YARN_METRICS_URL, MAX_EXECUTOR_MEMORY_GB,
    MAX_EXECUTOR_COUNT, MAX_SHUFFLE_PARTITIONS
)

def get_yarn_metrics():
    try:
        resp = requests.get(YARN_METRICS_URL, timeout=3)
        m = resp.json()["clusterMetrics"]
        return {
            "available_mb":     m["availableMB"],
            "total_mb":         m["totalMB"],
            "available_vcores": m["availableVirtualCores"],
        }
    except Exception:
        return None

def parse_spark_config(cell_code: str) -> dict:
    cfg = {}
    m = re.search(r'executor\.memory["\s,]+["\']?(\d+)([gGmM])', cell_code)
    if m:
        val, unit = int(m.group(1)), m.group(2).lower()
        cfg["executor_memory_gb"] = val if unit == "g" else round(val / 1024, 1)
    m = re.search(r'num[-_]executors["\s,]+["\']?(\d+)', cell_code)
    if m:
        cfg["num_executors"] = int(m.group(1))
    m = re.search(r'shuffle\.partitions["\s,]+["\']?(\d+)', cell_code)
    if m:
        cfg["shuffle_partitions"] = int(m.group(1))
    return cfg

def validate_cell(cell_code: str) -> list:
    warnings = []
    cfg = parse_spark_config(cell_code)

    if cfg.get("executor_memory_gb", 0) > MAX_EXECUTOR_MEMORY_GB:
        warnings.append(
            f"Executor memory ({cfg['executor_memory_gb']}g) exceeds the "
            f"cluster limit of {MAX_EXECUTOR_MEMORY_GB}g. YARN will reject this job."
        )
    if cfg.get("num_executors", 0) > MAX_EXECUTOR_COUNT:
        warnings.append(
            f"Requesting {cfg['num_executors']} executors — cluster cap is "
            f"{MAX_EXECUTOR_COUNT}. Other users' jobs will be starved."
        )
    if cfg.get("shuffle_partitions", 0) > MAX_SHUFFLE_PARTITIONS:
        warnings.append(
            f"spark.sql.shuffle.partitions={cfg['shuffle_partitions']} is very high. "
            f"Values above {MAX_SHUFFLE_PARTITIONS} often cause driver OOM. "
            f"Try 200-500 for most workloads."
        )
    yarn = get_yarn_metrics()
    if yarn:
        avail_gb = yarn["available_mb"] / 1024
        if "executor_memory_gb" in cfg and "num_executors" in cfg:
            req_gb = cfg["executor_memory_gb"] * cfg["num_executors"]
            if req_gb > avail_gb:
                warnings.append(
                    f"Job requests ~{req_gb:.0f}g total but only {avail_gb:.0f}g "
                    f"is available on the cluster right now. Job will queue or fail."
                )
    else:
        warnings.append(
            "Could not reach YARN metrics — cluster resource check skipped. "
            "Verify EMR is running."
        )
    return warnings

def cluster_status():
    """
    Call from a notebook cell to see live cluster capacity before submitting a large job.

    Usage:
        from emr_preflight import cluster_status
        cluster_status()
    """
    yarn = get_yarn_metrics()
    if not yarn:
        print("Cannot reach YARN — check EMR cluster is running.")
        return
    total_gb = yarn["total_mb"] / 1024
    avail_gb = yarn["available_mb"] / 1024
    used_pct = round(100 * (1 - yarn["available_mb"] / yarn["total_mb"]))
    status = (
        "HEAVILY LOADED — consider running later" if used_pct > 85
        else "MODERATELY BUSY — large jobs may queue" if used_pct > 60
        else "PLENTY OF CAPACITY — good time to run"
    )
    print(f"\n{'='*50}")
    print(f"  EMR Cluster Status")
    print(f"  Memory : {avail_gb:.0f} GB free of {total_gb:.0f} GB total")
    print(f"  Load   : {used_pct}% used")
    print(f"  vCores : {yarn['available_vcores']} free")
    print(f"  Status : {status}")
    print(f"{'='*50}\n")
```

---

### `emr_errors.py`

Maps raw Spark/YARN exception text to plain-English explanations with fix steps. Add new entries to `EMR_ERROR_MAP` as your team discovers new patterns.

```python
import re
import traceback

EMR_ERROR_MAP = [
    (
        r"ExecutorLostFailure.*OutOfMemoryError|java\.lang\.OutOfMemoryError",
        "An executor ran out of RAM and crashed (Out of Memory).\n"
        "  Fix A: Reduce the data you load — add .filter() or .limit() earlier.\n"
        "  Fix B: Increase spark.executor.memory (e.g. change '4g' to '8g').\n"
        "  Fix C: Increase spark.sql.shuffle.partitions to spread data across more tasks."
    ),
    (
        r"FetchFailedException|shuffle.*fetch.*failed",
        "Shuffle fetch failure — an executor died while transferring shuffle data.\n"
        "  This usually means the cluster ran out of DISK SPACE during shuffle.\n"
        "  Fix A: Add .repartition(200) before large joins or groupBys.\n"
        "  Fix B: Filter your dataset earlier before the join.\n"
        "  Fix C: Call cluster_status() — the cluster may be overloaded."
    ),
    (
        r"Job aborted due to stage failure|TaskSetManager.*Stage.*failed",
        "A stage of your job failed after retries — often caused by data skew.\n"
        "  (Data skew = one partition has far more rows than others.)\n"
        "  Fix A: Add .repartition() to distribute data evenly.\n"
        "  Fix B: Check the Spark UI — which task number keeps failing?"
    ),
    (
        r"SparkContext.*stopped|Cannot call methods on a stopped SparkContext",
        "Your Spark session has stopped (often after an OOM crash or idle timeout).\n"
        "  Fix: Restart the kernel then re-run your SparkSession creation cell."
    ),
    (
        r"Connection refused.*8088|Connection refused.*ResourceManager",
        "Cannot reach the YARN ResourceManager (port 8088 on EMR master).\n"
        "  Fix A: Check EMR cluster is in RUNNING state in AWS Console.\n"
        "  Fix B: Confirm SageMaker Studio and EMR are in the same VPC.\n"
        "  Fix C: Check the EMR master security group allows inbound 8088 from your SageMaker subnet."
    ),
    (
        r"AnalysisException.*cannot resolve|Column.*cannot be resolved",
        "Column not found — Spark cannot find a column you referenced.\n"
        "  Fix: Run df.printSchema() to see exact column names (check typos and case)."
    ),
    (
        r"Py4JJavaError",
        "A Java error occurred inside Spark. Read the full message above for the root cause.\n"
        "  If you see 'OutOfMemory' or 'GC overhead limit exceeded' — reduce data size or increase executor memory.\n"
        "  If you see 'Connection refused' — your EMR cluster may be stopped."
    ),
]

def translate_error(raw_error: str):
    for pattern, msg in EMR_ERROR_MAP:
        if re.search(pattern, raw_error, re.IGNORECASE):
            return msg
    return None

def install_error_hook():
    from IPython import get_ipython
    ip = get_ipython()
    if not ip:
        return
    _original = ip.showtraceback

    def _friendly(*args, **kwargs):
        _original(*args, **kwargs)
        friendly = translate_error(traceback.format_exc())
        if friendly:
            print("\n" + "─" * 60)
            print("What this means and how to fix it:")
            print()
            print(friendly)
            print("─" * 60)

    ip.showtraceback = _friendly
    print("[EMR framework] Friendly error messages active")
```

---

### `emr_advisor.py`

Fetches Dr. Elephant's analysis for a Spark job after it finishes and prints it in plain English.

```python
import requests
from emr_config import DR_ELEPHANT_BASE_URL

SEVERITY_LABEL = {
    "NONE": "OK", "LOW": "LOW",
    "MODERATE": "MODERATE", "SEVERE": "SEVERE", "CRITICAL": "CRITICAL",
}

def check_my_last_job(app_id: str = None):
    """
    Fetches Dr. Elephant's analysis for a finished Spark job.

    Usage:
        from emr_advisor import check_my_last_job
        check_my_last_job()                             # most recent job
        check_my_last_job("application_1234567890_42")  # specific job
    """
    url = (
        f"{DR_ELEPHANT_BASE_URL}/rest/job?id={app_id}"
        if app_id
        else f"{DR_ELEPHANT_BASE_URL}/rest/jobs?limit=1"
    )
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json()
    except Exception as e:
        print(f"Could not reach Dr. Elephant: {e}")
        print(f"  Expected at: {DR_ELEPHANT_BASE_URL}")
        print("  Check that Dr. Elephant is running on your EMR master node.")
        return

    if isinstance(data, list):
        if not data:
            print("No jobs analyzed yet — job may still be running.")
            return
        data = data[0]

    heuristics = data.get("heuristicResults", [])
    if not heuristics:
        print("No analysis results yet — job may still be running.")
        return

    print(f"\n{'='*60}")
    print(f"  Job Analysis: {data.get('id', 'unknown')}")
    print(f"  Overall: {data.get('severity', '?')}")
    print(f"{'='*60}")

    issues_found = False
    for h in heuristics:
        sev = h.get("severity", "NONE")
        if sev == "NONE":
            continue
        issues_found = True
        print(f"\n  [{SEVERITY_LABEL.get(sev, sev)}]  {h['heuristicName']}")
        print(f"  {h.get('details', 'No details available.')}")

    if not issues_found:
        print("  No issues detected — job looks healthy.")

    print(f"\n  Full report: {DR_ELEPHANT_BASE_URL}/app?id={data.get('id', '')}")
    print(f"{'='*60}\n")
```

---

### `emr_hooks.py`

Wires the pre-flight validator and error hook into IPython's kernel event system.

```python
from IPython import get_ipython
from emr_preflight import validate_cell
from emr_errors import install_error_hook

def register_hooks():
    ip = get_ipython()
    if not ip:
        return

    def pre_execute_hook(info):
        cell_code = info.raw_cell or ""
        if not cell_code.strip() or cell_code.strip().startswith(("%", "!", "#")):
            return
        warnings = validate_cell(cell_code)
        if warnings:
            print("\n" + "─" * 55)
            print("  EMR Pre-flight Check")
            for w in warnings:
                print(f"  WARNING: {w}")
            print("─" * 55 + "\n")

    ip.events.register("pre_run_cell", pre_execute_hook)
    install_error_hook()
    print("[EMR framework] Ready — pre-flight checks and friendly errors active.")
```

### Upload all five modules to S3

```bash
for f in emr_config.py emr_preflight.py emr_errors.py emr_advisor.py emr_hooks.py; do
  aws s3 cp $f s3://YOUR-BUCKET/emr-framework/modules/$f
done
```

---

## Phase 2 — Dr. Elephant on EMR

Dr. Elephant is an open-source Spark job advisor by LinkedIn. It runs on your EMR master node and analyzes finished and running jobs, producing severity-scored heuristics in plain English.

### Option A — Bootstrap Action (recommended for new clusters)

Create `install_dr_elephant.sh`:

```bash
#!/bin/bash
# EMR Bootstrap Action — runs on master node at cluster startup

# Only execute on the master node
if [ $(cat /mnt/var/lib/info/instance.json | python3 -c \
  "import sys,json; print(json.load(sys.stdin)['isMaster'])") != "True" ]; then
  exit 0
fi

set -e

cd /home/hadoop
wget -q https://github.com/linkedin/dr-elephant/releases/download/2.1.7/dr-elephant-2.1.7.zip
unzip -q dr-elephant-2.1.7.zip
cd dr-elephant-2.1.7

cat > conf/application.conf << 'CONF'
yarn_resourcemanager_address="localhost"
yarn_resourcemanager_port=8088
spark_history_server_url="http://localhost:18080"
fetcher_executor_num=2
CONF

bin/start.sh
echo "Dr. Elephant started on port 8080"
```

Upload and attach:

```bash
# Upload bootstrap script
aws s3 cp install_dr_elephant.sh \
  s3://YOUR-BUCKET/emr-framework/bootstrap/install_dr_elephant.sh

# Attach when creating a new cluster via CLI:
# --bootstrap-actions Path=s3://YOUR-BUCKET/emr-framework/bootstrap/install_dr_elephant.sh

# Or in the console:
# EMR → Create cluster → Bootstrap actions → Add bootstrap action
# Script location: s3://YOUR-BUCKET/emr-framework/bootstrap/install_dr_elephant.sh
```

### Option B — Manual install on a running cluster

```bash
# SSH to the EMR master node
ssh -i your-key.pem hadoop@YOUR-EMR-MASTER-DNS

# Then run the script body directly on the master node
wget -q https://github.com/linkedin/dr-elephant/releases/download/2.1.7/dr-elephant-2.1.7.zip
unzip -q dr-elephant-2.1.7.zip
cd dr-elephant-2.1.7

cat > conf/application.conf << 'CONF'
yarn_resourcemanager_address="localhost"
yarn_resourcemanager_port=8088
spark_history_server_url="http://localhost:18080"
fetcher_executor_num=2
CONF

bin/start.sh
```

---

## Phase 3 — SageMaker Studio Lifecycle Config

> **Important:** SageMaker Studio uses `KernelGateway` lifecycle configs — not the classic notebook instance lifecycle configs. The script type and attachment commands differ.

### Create `studio_lifecycle.sh`

```bash
#!/bin/bash
# SageMaker Studio KernelGateway Lifecycle Script
# Runs at the start of every kernel session for every user.

set -e

FRAMEWORK_DIR="/home/sagemaker-user/emr_framework"
S3_PREFIX="s3://YOUR-BUCKET/emr-framework"    # ← replace YOUR-BUCKET — only change needed

echo "[EMR framework] Starting setup..."

mkdir -p $FRAMEWORK_DIR

# Pull latest config and modules from S3 on every kernel start
# This means S3 updates reach users automatically on next kernel restart
aws s3 sync $S3_PREFIX/config/   $FRAMEWORK_DIR/ --quiet
aws s3 sync $S3_PREFIX/modules/  $FRAMEWORK_DIR/ --quiet

# Add framework to Python path via .pth file
SITE_PACKAGES=$(python3 -c \
  "import site; print(site.getsitepackages()[0])" 2>/dev/null \
  || echo "/opt/conda/lib/python3.10/site-packages")
echo "$FRAMEWORK_DIR" > $SITE_PACKAGES/emr_framework.pth

# Create IPython autostart directory
IPYTHON_STARTUP="/home/sagemaker-user/.ipython/profile_default/startup"
mkdir -p $IPYTHON_STARTUP

# Write the IPython autostart file — runs inside the kernel at launch
cat > $IPYTHON_STARTUP/00_emr_framework.py << 'PYEOF'
import sys, os

_fw = "/home/sagemaker-user/emr_framework"
if _fw not in sys.path:
    sys.path.insert(0, _fw)

try:
    from emr_hooks import register_hooks
    register_hooks()
except Exception as e:
    print(f"[EMR framework] Could not load — {e}")
    print(f"[EMR framework] Check s3://YOUR-BUCKET/emr-framework/ has all modules.")
PYEOF

echo "[EMR framework] Setup complete."
```

### Register and attach via AWS CLI

```bash
# 1. Encode script as base64 (required by the API)
SCRIPT_B64=$(base64 -w 0 studio_lifecycle.sh)

# 2. Create the lifecycle config
aws sagemaker create-studio-lifecycle-config \
  --studio-lifecycle-config-name emr-framework-kernel \
  --studio-lifecycle-config-app-type KernelGateway \
  --studio-lifecycle-config-content $SCRIPT_B64

# 3a. Attach to the entire Studio Domain (all users)
aws sagemaker update-domain \
  --domain-id YOUR-DOMAIN-ID \
  --default-user-settings \
    '{"KernelGatewayAppSettings":{"LifecycleConfigArns":["arn:aws:sagemaker:YOUR-REGION:YOUR-ACCOUNT-ID:studio-lifecycle-config/emr-framework-kernel"]}}'

# 3b. OR attach to a specific User Profile only (for staged rollout)
aws sagemaker update-user-profile \
  --domain-id YOUR-DOMAIN-ID \
  --user-profile-name TARGET-USER-PROFILE-NAME \
  --user-settings \
    '{"KernelGatewayAppSettings":{"LifecycleConfigArns":["arn:aws:sagemaker:YOUR-REGION:YOUR-ACCOUNT-ID:studio-lifecycle-config/emr-framework-kernel"]}}'
```

> **Recommended rollout:** Attach to one test User Profile first. Verify it works. Then attach to the full Domain. Existing running kernel sessions are not affected — the change takes effect on the next kernel start.

### Verify installation

When a data scientist opens a notebook and starts a kernel, they will see:

```
[EMR framework] Friendly error messages active
[EMR framework] Ready — pre-flight checks and friendly errors active.
```

No action is required from the data scientist. The framework is fully transparent.

---

## Updating the Framework

### Update a Python module (e.g. add a new error pattern)

```bash
# Edit emr_errors.py locally, then:
aws s3 cp emr_errors.py s3://YOUR-BUCKET/emr-framework/modules/emr_errors.py

# Users get the update on their next kernel restart.
# No changes to Studio config, no touching user machines.
```

### Update all modules at once

```bash
for f in emr_config.py emr_preflight.py emr_errors.py emr_advisor.py emr_hooks.py; do
  aws s3 cp $f s3://YOUR-BUCKET/emr-framework/modules/$f
done
```

### Update the lifecycle script itself

```bash
# Edit studio_lifecycle.sh locally, then re-register:
SCRIPT_B64=$(base64 -w 0 studio_lifecycle.sh)

aws sagemaker update-studio-lifecycle-config \
  --studio-lifecycle-config-name emr-framework-kernel \
  --studio-lifecycle-config-content $SCRIPT_B64
```

---

## Updating the EMR DNS Variable

When your EMR cluster is replaced or restarted with a new DNS:

1. Open `emr_config.env` locally
2. Update the `EMR_MASTER_DNS` value (and `EMR_CLUSTER_ID` if the cluster ID changed)
3. Upload:

```bash
aws s3 cp emr_config.env s3://YOUR-BUCKET/emr-framework/config/emr_config.env
```

4. Users get the new DNS on their next kernel restart. No other files change.

> This is the **only** file you touch when a cluster changes. The DNS value is not hardcoded anywhere else in the framework.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `[EMR framework] Could not load` at kernel start | Module missing from S3 or PYTHONPATH not set | Verify all 5 `.py` files are in `s3://YOUR-BUCKET/emr-framework/modules/` |
| `Could not reach YARN metrics` in pre-flight | EMR cluster stopped, or VPC/security group issue | Check EMR cluster state; confirm SageMaker subnet can reach port 8088 on EMR master |
| `Could not reach Dr. Elephant` from `check_my_last_job()` | Dr. Elephant not running or wrong port | SSH to EMR master, run `ps aux | grep elephant` to check if process is running |
| Pre-flight warnings not appearing | Lifecycle config not attached, or kernel not restarted after attachment | Restart the kernel; verify lifecycle config ARN is in the Domain/User Profile settings |
| Wrong DNS being used | `emr_config.env` has old value | Update and re-upload `emr_config.env`; restart kernel |
| `AccessDenied` on S3 sync | SageMaker execution role missing S3 policy | Attach `EMRFrameworkSageMakerPolicy` to the execution role (see IAM Policies section) |

### Test YARN connectivity manually from a notebook cell

```python
import requests
from emr_config import YARN_METRICS_URL

try:
    r = requests.get(YARN_METRICS_URL, timeout=3)
    print("YARN reachable:", r.status_code)
    print(r.json()["clusterMetrics"])
except Exception as e:
    print("YARN not reachable:", e)
```

---

## Variable Reference

All variables are set in `emr_config.env` in S3. This is the complete reference.

| Variable | Default | Description |
|---|---|---|
| `EMR_MASTER_DNS` | _(required)_ | Private DNS or IP of the EMR master node |
| `YARN_PORT` | `8088` | YARN ResourceManager REST API port |
| `DR_ELEPHANT_PORT` | `8080` | Dr. Elephant web service port |
| `EMR_CLUSTER_ID` | _(required)_ | EMR cluster ID (e.g. `j-XXXXXXXXX`) |
| `AWS_REGION` | `us-east-1` | AWS region of your EMR cluster |
| `MAX_EXECUTOR_MEMORY_GB` | `48` | Maximum executor memory before a warning is issued |
| `MAX_EXECUTOR_COUNT` | `50` | Maximum number of executors before a warning is issued |
| `MAX_SHUFFLE_PARTITIONS` | `2000` | Maximum shuffle partitions before a warning is issued |

> Tune `MAX_*` values based on your EMR node type. Check node specifications in: EMR console → your cluster → Hardware tab.

---

## Data Scientist Quick Reference

Data scientists do not need to read this document. Share only the following with them:

```python
# Check if the cluster has capacity before running a large job
from emr_preflight import cluster_status
cluster_status()

# After a job finishes, get plain-English analysis of what happened
from emr_advisor import check_my_last_job
check_my_last_job()                             # most recent job
check_my_last_job("application_1234567890_42")  # specific job by ID
```

Everything else (pre-flight warnings, error translations) is automatic and requires no action from data scientists.

---

*Last updated: March 2026 — EMR Framework v1.0*
