# Packaging a PySpark Project for EMR Cluster Mode Submission

## The Problem

When submitting a PySpark job in **cluster mode** on EMR, Spark only sends the main script to the executors. Additional modules (like `helper.py`) and data files (like SQL parameters) are not automatically included, resulting in errors like:

```
ModuleNotFoundError: No module named 'helper'
```

## The Solution: Package as a Python Package

This guide walks through converting your PySpark project into a proper Python package that can be cleanly submitted to EMR.

---

## Original Project Structure

```
prepay/
├── main_et_pool.py
├── helper.py
└── parameters/
    ├── file1.sql
    ├── file2.sql
    └── 2026Q1/
        ├── file1.sql
        └── file2.sql
```

---

## Step 1: Restructure Your Project

Convert your flat structure into a proper Python package:

```
prepay/
├── setup.py                  # Package configuration (NEW)
├── run.py                    # Entry point script (NEW)
├── prepay/                   # Your actual package - same name as parent (RENAMED)
│   ├── __init__.py           # Makes it a Python package (NEW - can be empty)
│   ├── main_et_pool.py       # (MOVED here)
│   └── helper.py             # (MOVED here)
└── parameters/               # Keep outside the package
    ├── file1.sql
    ├── file2.sql
    └── 2026Q1/
        ├── file1.sql
        └── file2.sql
```

### Key Changes

| Before | After |
|--------|-------|
| `prepay/main_et_pool.py` | `prepay/prepay/main_et_pool.py` |
| `prepay/helper.py` | `prepay/prepay/helper.py` |
| (none) | `prepay/prepay/__init__.py` |
| (none) | `prepay/setup.py` |
| (none) | `prepay/run.py` |

---

## Step 2: Create `setup.py`

Create `setup.py` in the root directory:

```python
from setuptools import setup, find_packages

setup(
    name="prepay",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        # List external dependencies here (see explanation below)
    ],
)
```

### Understanding `setup.py` Components

#### `find_packages()`

This automatically discovers all directories containing `__init__.py` and includes them in the package. This is how `helper.py` gets bundled - it's inside the `prepay/` package directory.

#### `install_requires`

This is for **external third-party libraries only** - packages you install via `pip`. 

| Type | Examples | Where It Goes |
|------|----------|---------------|
| **Your code** | `helper.py`, `main_et_pool.py` | Auto-discovered by `find_packages()` |
| **External packages** | `boto3`, `pandas`, `requests` | Listed in `install_requires` |
| **Built-in Python** | `os`, `sys`, `json`, `datetime` | Don't list (comes with Python) |
| **Spark-provided** | `pyspark` | Don't list (EMR provides it) |

#### Example with External Dependencies

If your code uses external libraries:

```python
# In helper.py
import boto3          # External - needs to be listed
import pandas         # External - needs to be listed
import json           # Built-in - don't list
from pyspark.sql import SparkSession  # Spark provides - don't list
```

Then your `setup.py` would be:

```python
from setuptools import setup, find_packages

setup(
    name="prepay",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "pandas",
    ],
)
```

**Note for EMR**: Common libraries like `boto3`, `pandas`, and `numpy` are pre-installed on EMR clusters, so you often don't need to list them. Only add packages that aren't already available on the cluster.

---

## Step 3: Create `__init__.py`

Create `prepay/__init__.py`. It can be empty or expose key functions:

```python
# prepay/__init__.py

# Option 1: Empty file (minimum required)
# Just leave it empty

# Option 2: Expose key functions for cleaner imports
from .helper import *
```

---

## Step 4: Update Your Imports

Modify `main_et_pool.py` to use absolute package imports:

```python
# BEFORE (relative import - breaks in cluster mode)
import helper
from helper import some_function

# AFTER (absolute package import - works in cluster mode)
from prepay import helper
from prepay.helper import some_function
```

---

## Step 5: Create Entry Point Script

Create `run.py` at the root level:

```python
# run.py
from prepay.main_et_pool import main

if __name__ == "__main__":
    main()
```

Then refactor `main_et_pool.py` to have a `main()` function:

```python
# prepay/main_et_pool.py
from prepay.helper import some_function

def main():
    """Main entry point for the ETL job."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("PrepayETL") \
        .getOrCreate()
    
    # Your existing ETL logic here
    # ...
    
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## Step 6: Build the Package

From the root `prepay/` directory, build the distributable package:

### Option A: Build as Egg (Recommended)

```bash
python setup.py bdist_egg
```

This creates: `dist/prepay-0.1-py3.x.egg`

### Option B: Build as Zip

```bash
cd prepay  # the inner package directory
zip -r ../prepay.zip .
cd ..
```

### Verify Package Contents

Confirm `helper.py` is included:

```bash
# For egg file
unzip -l dist/prepay-0.1-py3.10.egg
```

Expected output:
```
prepay/__init__.py
prepay/main_et_pool.py
prepay/helper.py        # <-- Confirmed included
```

---

## Step 7: Handle Parameter Files

Since parameter files (SQL files) are **data files**, not Python code, handle them separately.

### Option A: Read from S3 (Recommended)

Upload parameters to S3 and read from there:

```bash
# Upload to S3
aws s3 cp --recursive parameters/ s3://your-bucket/prepay/parameters/
```

In your code:

```python
# Read SQL file from S3
sql_content = spark.sparkContext \
    .textFile("s3://your-bucket/prepay/parameters/2026Q1/file1.sql") \
    .collect()
sql_query = "\n".join(sql_content)

# Or using boto3
import boto3
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='your-bucket', Key='prepay/parameters/2026Q1/file1.sql')
sql_query = obj['Body'].read().decode('utf-8')
```

### Option B: Use `--archives` Flag

Bundle parameters and distribute with the job:

```bash
# Create archive
zip -r parameters.zip parameters/

# Include in spark-submit (see Step 8)
```

In your code, access via relative path:

```python
with open("./parameters/2026Q1/file1.sql", "r") as f:
    sql_query = f.read()
```

---

## Step 8: Submit the Job

### Basic Submission (Parameters on S3)

```bash
spark-submit \
    --deploy-mode cluster \
    --py-files dist/prepay-0.1-py3.10.egg \
    run.py
```

### With Parameters Archive

```bash
spark-submit \
    --deploy-mode cluster \
    --py-files dist/prepay-0.1-py3.10.egg \
    --archives parameters.zip#parameters \
    run.py
```

The `#parameters` creates an alias, so files are accessible at `./parameters/`.

### From S3 (Production Setup)

```bash
# Upload everything to S3 first
aws s3 cp dist/prepay-0.1-py3.10.egg s3://your-bucket/prepay/
aws s3 cp run.py s3://your-bucket/prepay/

# Submit from S3
spark-submit \
    --deploy-mode cluster \
    --py-files s3://your-bucket/prepay/prepay-0.1-py3.10.egg \
    s3://your-bucket/prepay/run.py
```

---

## Complete Final Structure

```
prepay/
├── setup.py
├── run.py
├── prepay/
│   ├── __init__.py
│   ├── main_et_pool.py
│   └── helper.py
├── parameters/           # Local copy (for development)
│   ├── file1.sql
│   └── 2026Q1/
│       └── file1.sql
└── dist/                 # Generated by build
    └── prepay-0.1-py3.10.egg
```

---

## Complete Example Files

### `setup.py`

```python
from setuptools import setup, find_packages

setup(
    name="prepay",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        # Add any external dependencies not pre-installed on EMR
    ],
)
```

### `prepay/__init__.py`

```python
# Can be empty or expose commonly used functions
from .helper import *
```

### `prepay/helper.py`

```python
"""Helper functions for the Prepay ETL job."""

def read_sql_from_s3(spark, s3_path):
    """Read a SQL file from S3 and return as string."""
    content = spark.sparkContext.textFile(s3_path).collect()
    return "\n".join(content)

def apply_sql_to_dataframe(spark, df, sql_query, temp_view_name="temp_view"):
    """Apply a SQL query to a DataFrame."""
    df.createOrReplaceTempView(temp_view_name)
    return spark.sql(sql_query)

# Add your other helper functions here
```

### `prepay/main_et_pool.py`

```python
"""Main ETL job for Prepay processing."""

from pyspark.sql import SparkSession
from prepay.helper import read_sql_from_s3, apply_sql_to_dataframe

def main():
    """Main entry point."""
    spark = SparkSession.builder \
        .appName("PrepayETL") \
        .getOrCreate()
    
    try:
        # Read data from S3
        df = spark.read.parquet("s3://your-bucket/data/input/")
        
        # Read and apply SQL transformations
        sql_query = read_sql_from_s3(
            spark, 
            "s3://your-bucket/prepay/parameters/2026Q1/transform.sql"
        )
        result_df = apply_sql_to_dataframe(spark, df, sql_query)
        
        # Write results
        result_df.write \
            .mode("overwrite") \
            .parquet("s3://your-bucket/data/output/")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

### `run.py`

```python
"""Entry point for spark-submit."""

from prepay.main_et_pool import main

if __name__ == "__main__":
    main()
```

---

## Why This Approach Works

1. **Proper dependency resolution** - Python understands the package structure
2. **Scalable** - Easy to add more modules and subpackages
3. **Testable** - You can write unit tests and import modules cleanly
4. **Reusable** - Install locally for development with `pip install -e .`
5. **EMR compatible** - Works reliably in cluster mode

---

## Quick Reference: Build and Submit Commands

```bash
# Build
python setup.py bdist_egg

# Upload to S3
aws s3 cp dist/prepay-0.1-py3.10.egg s3://your-bucket/prepay/
aws s3 cp run.py s3://your-bucket/prepay/
aws s3 cp --recursive parameters/ s3://your-bucket/prepay/parameters/

# Submit
spark-submit \
    --deploy-mode cluster \
    --py-files s3://your-bucket/prepay/prepay-0.1-py3.10.egg \
    s3://your-bucket/prepay/run.py
```

---

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError: No module named 'prepay'` | Package not included | Verify `--py-files` points to correct egg |
| `ModuleNotFoundError: No module named 'helper'` | Old import style | Change to `from prepay.helper import ...` |
| `FileNotFoundError` for SQL files | Parameters not accessible | Use S3 paths or `--archives` |
| Package missing modules | `__init__.py` missing | Ensure `prepay/__init__.py` exists |
