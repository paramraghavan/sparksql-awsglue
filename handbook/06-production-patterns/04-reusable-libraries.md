# Building Reusable Spark Utility Libraries

## Overview

When managing multiple Spark projects, common patterns, utilities, and helper functions get duplicated across codebases. The solution is to **build a reusable Python library** that can be installed via pip, versioned, and distributed to teams.

This guide covers building, versioning, testing, and distributing a production-ready Spark utility library using **spark_de_utils** (Spark Data Engineering Utilities) as a real-world example.

---

## Why Build a Shared Library?

### Problems with Copy-Paste Code
```python
# Bad: Same code in 5 different projects
def parse_s3_path(s3_path: str):
    if not s3_path.startswith("s3://"):
        raise Exception(f"{s3_path} is not valid S3 path")
    parts = s3_path.split("/")
    if len(parts) < 3 or (len(parts) >= 3 and parts[3] == ""):
        raise Exception(f"{s3_path} is not a full S3 object path")
    bucket = parts[2]
    key = "/".join(parts[3:])
    return bucket, key
```

### Benefits of a Shared Library
✅ **Single source of truth** - Update logic once, all projects benefit
✅ **Version control** - Tag releases, pin versions in projects
✅ **Testing** - Comprehensive unit tests before distribution
✅ **Documentation** - Functions documented once in one place
✅ **Team productivity** - Reuse instead of reinvent

---

## Project Structure

A minimal but production-ready library structure:

```
spark_de_utils/
├── setup.py                    # Package metadata and dependencies
├── requirements.txt            # pip install requirements
├── Pipfile                     # Pipenv configuration
├── .bumpversion.cfg            # Version bump configuration
├── bump-version.sh             # Versioning script
├── README.md                   # Installation and usage docs
├── spark_de_utils/
│   ├── __init__.py            # Module exports
│   ├── s3_utils.py            # S3 path/data utilities
│   ├── spark_utils.py         # Spark-specific helpers
│   ├── validation.py          # Data validation functions
│   └── test/
│       ├── __init__.py
│       ├── s3_utils_test.py
│       ├── spark_utils_test.py
│       └── data/              # Test fixtures
│           ├── test_data.json
│           └── test_parquet/
```

---

## Core Files

### 1. setup.py - Package Metadata

```python
import os
from setuptools import setup

# Read dependencies from requirements.txt
path = os.path.dirname(os.path.realpath(__file__))
requirement_path = os.path.join(path, "requirements.txt")

if os.path.isfile(requirement_path):
    with open(requirement_path) as f_h:
        install_requires = f_h.read().splitlines()

setup(
    name="spark_de_utils",
    version="0.8.0",
    packages=["spark_de_utils"],
    python_requires=">= 3.6",
    description="Common utilities for Spark data engineering",
    author="Data Platform Team",
    maintainer="Data Platform Team",
    url="https://github.com/your-org/spark_de_utils",
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    install_requires=install_requires
)
```

### 2. requirements.txt - Minimal Dependencies

Keep dependencies minimal to avoid conflicts with project dependencies:

```
urllib3>=1.26.5
wheel>=0.36.2
pyspark>=3.1.1
boto3>=1.17.48
bumpversion>=0.6.0
```

### 3. Pipfile - Development Environment

```ini
[[source]]
name = "pypi"
url = "https://pypi.org/simple"
verify_ssl = true

[dev-packages]
pytest = ">=6.2.4"
moto = "==2.2.0"              # Mock AWS services for testing
pytest-cov = ">=2.12.0"

[packages]
urllib3 = ">=1.26.5"
wheel = ">=0.36.2"
pyspark = "==3.1.3"
boto3 = ">=1.17.48"
bumpversion = "==0.6.0"

[requires]
python_version = "3.7"
```

---

## Example Utilities: S3 Helpers

### Module: spark_de_utils/s3_utils.py

```python
import typing
import os


def parse_s3_path_to_bucket_key(s3_full_path: str) -> typing.Tuple[str, str]:
    """
    Parse S3 path into bucket and key.

    Args:
        s3_full_path: Full S3 path (e.g., s3://my-bucket/data/year=2024/file.parquet)

    Returns:
        Tuple of (bucket, key)

    Raises:
        Exception: If path is not valid S3 format or is incomplete

    Example:
        >>> bucket, key = parse_s3_path_to_bucket_key("s3://my-data/sales/data.parquet")
        >>> bucket
        'my-data'
        >>> key
        'sales/data.parquet'
    """
    if not s3_full_path.startswith("s3://"):
        raise Exception(f"{s3_full_path}: is not a valid s3 path")

    s3_path_split = s3_full_path.split("/")
    len_split_path = len(s3_path_split)

    if len_split_path < 3 or (len_split_path >= 3 and s3_path_split[3] == ""):
        raise Exception(f"{s3_full_path} is not a full s3 object path")

    bucket = s3_path_split[2]
    key = os.path.join(*s3_path_split[3:])

    return bucket, key


def get_s3_partition_keys(s3_path: str) -> typing.Dict[str, str]:
    """
    Extract partition keys from S3 path (e.g., year=2024, month=03).

    Args:
        s3_path: S3 path with partition structure

    Returns:
        Dict of partition key-value pairs

    Example:
        >>> get_s3_partition_keys("s3://data/year=2024/month=03/day=15/file.parquet")
        {'year': '2024', 'month': '03', 'day': '15'}
    """
    _, key = parse_s3_path_to_bucket_key(s3_path)
    partitions = {}

    for part in key.split("/")[:-1]:  # Exclude filename
        if "=" in part:
            k, v = part.split("=", 1)
            partitions[k] = v

    return partitions
```

### Tests: spark_de_utils/test/s3_utils_test.py

```python
import pytest
from spark_de_utils.s3_utils import parse_s3_path_to_bucket_key, get_s3_partition_keys


class TestParseS3Path:
    """Test S3 path parsing utility."""

    def test_valid_s3_path(self):
        """Test parsing valid S3 path."""
        bucket, key = parse_s3_path_to_bucket_key("s3://my-bucket/data/file.parquet")
        assert bucket == "my-bucket"
        assert key == "data/file.parquet"

    def test_s3_path_with_partitions(self):
        """Test parsing S3 path with partition structure."""
        bucket, key = parse_s3_path_to_bucket_key(
            "s3://analytics/year=2024/month=03/day=15/data.parquet"
        )
        assert bucket == "analytics"
        assert key == "year=2024/month=03/day=15/data.parquet"

    def test_invalid_prefix(self):
        """Test rejection of non-S3 paths."""
        with pytest.raises(Exception) as exc_info:
            parse_s3_path_to_bucket_key("gs://my-bucket/data/file.parquet")
        assert "not a valid s3 path" in str(exc_info.value)

    def test_incomplete_path(self):
        """Test rejection of paths without object key."""
        with pytest.raises(Exception) as exc_info:
            parse_s3_path_to_bucket_key("s3://my-bucket/")
        assert "not a full s3 object path" in str(exc_info.value)

    def test_bucket_only(self):
        """Test rejection of bucket-only path."""
        with pytest.raises(Exception) as exc_info:
            parse_s3_path_to_bucket_key("s3://my-bucket")
        assert "not a full s3 object path" in str(exc_info.value)


class TestGetPartitionKeys:
    """Test partition extraction from S3 paths."""

    def test_simple_partitions(self):
        """Test extracting partition keys."""
        partitions = get_s3_partition_keys(
            "s3://data/year=2024/month=03/day=15/file.parquet"
        )
        assert partitions == {"year": "2024", "month": "03", "day": "15"}

    def test_no_partitions(self):
        """Test path with no partition structure."""
        partitions = get_s3_partition_keys("s3://data/raw/file.parquet")
        assert partitions == {}

    def test_mixed_path_and_partitions(self):
        """Test path with directory and partitions."""
        partitions = get_s3_partition_keys(
            "s3://data/raw_layer/year=2024/month=03/file.parquet"
        )
        assert partitions == {"year": "2024", "month": "03"}
```

**Run tests:**
```bash
pipenv run pytest spark_de_utils/test/ -v
pipenv run pytest spark_de_utils/test/ --cov=spark_de_utils
```

---

## Versioning Strategy

### .bumpversion.cfg - Semantic Versioning

```ini
[bumpversion]
current_version = 0.8.0
commit = True
tag = True
tag_name = v{new_version}

[bumpversion:file:setup.py]
```

### bump-version.sh - Automated Versioning Script

```bash
#!/bin/bash

# Bump version and create git tag
# Usage: sh bump-version.sh [major|minor|patch]
# Example: sh bump-version.sh minor

if [ -z "$1" ]; then
    echo "Usage: sh bump-version.sh [major|minor|patch]"
    exit 1
fi

# Run bumpversion with the specified part (major, minor, or patch)
pipenv run bumpversion --verbose --list $1 setup.py

# Push commits and tags to origin
git push origin --tags
git push
```

**Workflow:**

```bash
# Increment version
sh bump-version.sh minor  # 0.8.0 → 0.9.0
sh bump-version.sh patch  # 0.8.0 → 0.8.1
sh bump-version.sh major  # 0.8.0 → 1.0.0

# This automatically:
# ✅ Updates setup.py version
# ✅ Updates .bumpversion.cfg
# ✅ Creates git commit with version bump message
# ✅ Creates git tag (v0.9.0)
# ✅ Pushes to origin
```

---

## Installation Methods

### 1. From GitHub (for Development)

**With HTTPS (requires ~/.git-credentials):**
```bash
pip3 install -e git+https://github.com/your-org/spark_de_utils.git@v0.8.0#egg=spark_de_utils
```

**With SSH (requires SSH key configured):**
```bash
pip3 install -e git+ssh://git@github.com/your-org/spark_de_utils.git@v0.8.0#egg=spark_de_utils
```

**With pipenv:**
```bash
pipenv install -e git+ssh://git@github.com/your-org/spark_de_utils.git@v0.8.0#egg=spark_de_utils
```

**Update .gitconfig for HTTPS fallback to SSH:**
```ini
[url "ssh://git@github.com/"]
    insteadOf = https://github.com/
```

### 2. From Private Python Package Repository

```bash
pip3 install spark_de_utils==0.8.0
```

### 3. Development Installation (for contributors)

```bash
git clone https://github.com/your-org/spark_de_utils.git
cd spark_de_utils
pipenv install --dev
pipenv run pytest
```

### 4. AWS CodeBuild Integration

When using in CodeBuild, add to `buildspec.yml`:

```yaml
version: 0.2

phases:
  pre_build:
    commands:
      - echo "Setting up git credentials for private packages..."
      - git config --global credential.helper store
      - echo "https://${GITHUB_TOKEN}:x-oauth-basic@github.com" >> ~/.git-credentials

  install:
    commands:
      - pip install -e git+https://github.com/your-org/spark_de_utils.git@v0.8.0#egg=spark_de_utils
      - pip install -r requirements.txt

  build:
    commands:
      - python -m pytest tests/
      - python deploy.py
```

---

## Real-World Usage Examples

### Example 1: ETL Job Using the Library

**File: etl_sales_job.py**

```python
from pyspark.sql import SparkSession
from spark_de_utils.s3_utils import parse_s3_path_to_bucket_key, get_s3_partition_keys

spark = SparkSession.builder.appName("SalesETL").getOrCreate()

# Input from S3
input_path = "s3://raw-data/sales/year=2024/month=03/sales.parquet"

# Use library utilities
bucket, key = parse_s3_path_to_bucket_key(input_path)
partitions = get_s3_partition_keys(input_path)

print(f"Reading from bucket: {bucket}")
print(f"Object key: {key}")
print(f"Partitions: {partitions}")

# Read data
df = spark.read.parquet(input_path)

# Transform
result = df.filter(df.amount > 100).groupBy("region").count()

# Write output
output_path = f"s3://processed-data/{partitions['year']}/{partitions['month']}/sales_summary.parquet"
result.write.mode("overwrite").parquet(output_path)
```

### Example 2: Dependency Declaration

**File: requirements.txt in main project**

```
spark_de_utils==0.8.0
pyspark==3.5.0
pandas==1.5.3
```

**File: Pipfile in main project**

```ini
[[source]]
name = "pypi"
url = "https://pypi.org/simple"
verify_ssl = true

[packages]
spark_de_utils = "==0.8.0"
pyspark = "==3.5.0"
pandas = "==1.5.3"
```

---

## Common Patterns in Utility Libraries

### 1. Configuration Utilities

```python
# spark_de_utils/config.py
from typing import Dict, Any
import json

def load_config(config_path: str) -> Dict[str, Any]:
    """Load job configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def validate_config(config: Dict[str, Any], required_keys: list) -> bool:
    """Validate required configuration keys exist."""
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required config key: {key}")
    return True
```

### 2. Data Validation Utilities

```python
# spark_de_utils/validation.py
from pyspark.sql import DataFrame

def validate_schema(df: DataFrame, required_columns: list) -> bool:
    """Validate DataFrame has required columns."""
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return True

def validate_not_null(df: DataFrame, column: str) -> bool:
    """Check column has no null values."""
    null_count = df.filter(f"{column} is null").count()
    if null_count > 0:
        raise ValueError(f"Column '{column}' has {null_count} null values")
    return True
```

### 3. Spark-Specific Helpers

```python
# spark_de_utils/spark_utils.py
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, config: dict = None) -> SparkSession:
    """Create and configure SparkSession with defaults."""
    builder = SparkSession.builder.appName(app_name)

    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    # Smart defaults for Glue/EMR
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    return builder.getOrCreate()
```

---

## Distribution Options

### Option 1: GitHub Releases + pip

1. Push code to GitHub
2. Run `bash bump-version.sh` (creates tag)
3. Create GitHub Release from tag
4. Users install via pip: `pip install -e git+https://...@vX.X.X`

### Option 2: Private PyPI Repository

1. Host on private PyPI (JFrog Artifactory, AWS CodeArtifact, Gemfury)
2. Publish library: `python setup.py upload`
3. Users install: `pip install spark_de_utils==0.8.0`

### Option 3: S3 Python Package Repository

```bash
# Build wheel
python setup.py bdist_wheel

# Upload to S3
aws s3 cp dist/spark_de_utils-0.8.0-py3-none-any.whl s3://my-packages/

# Install from S3
pip install https://my-packages.s3.amazonaws.com/spark_de_utils-0.8.0-py3-none-any.whl
```

---

## Best Practices

✅ **Keep dependencies minimal** - Avoid conflicts with project dependencies
✅ **Version religiously** - Use semantic versioning (major.minor.patch)
✅ **Test thoroughly** - Every utility needs unit tests
✅ **Document functions** - Include docstrings with examples
✅ **Update documentation** - Keep README and examples current
✅ **Avoid tight coupling** - Libraries should be flexible and configurable
✅ **Handle errors gracefully** - Clear error messages help users
✅ **Support multiple Python versions** - Test with 3.6, 3.7, 3.8+

---

## Troubleshooting

### Issue: Installation Fails with Git Credentials Error

```bash
# Solution 1: Set up ~/.git-credentials
echo "https://username:token@github.com" > ~/.git-credentials
git config --global credential.helper store

# Solution 2: Use SSH instead of HTTPS
pip install -e git+ssh://git@github.com/org/spark_de_utils.git@v0.8.0#egg=spark_de_utils
```

### Issue: Version Mismatch Across Projects

```bash
# Use specific versions in requirements.txt
spark_de_utils==0.8.0  # Exact version

# Not this:
spark_de_utils  # Latest (unpredictable)
```

### Issue: AWS Glue Conflicts with Spark Version

In buildspec or Glue job configuration:
```bash
# AWS Glue 3.0 supports PySpark 3.1.1
# Specify compatible version in requirements.txt
pyspark==3.1.1
```

---

## Next Steps

1. **Create your first utility library** following this structure
2. **Set up Git tags and releases** for version management
3. **Document all functions** with docstrings and examples
4. **Test thoroughly** before distributing
5. **Version bump** when adding features or fixing bugs
6. **Monitor usage** across projects

For more on production patterns, see:
- [Config-Driven ETL](01-config-driven-etl.md)
- [Data Comparison](02-data-comparison.md)
- [Utility Functions](00-utility-functions.md)

