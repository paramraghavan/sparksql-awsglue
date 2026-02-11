```text
Submitting a pyspark job on EMR
here is my project structure
prepay
   |- main_et_pool.py
   |- helper.py
   |-parameters
           |- files1 thru n 
           |-2026Q1
                  |- files1 thru n

i do spark-submit in cluster mode ./main_et_pool.py
main_et_pool.py - reads all the files under parametes, these files have sql, which are applied on dataframes read from s3
Once the job starts running I get error module helper not found

I did a tree of the current dir, and in the applicataion i only see the file main_et_pool.py

What's the right way to submit this pyspark job on emr
```

The issue is that in cluster mode, Spark only sends the main script to the executors. You need to explicitly include
your helper modules and parameter files.

## Solutions

### Option 1: Use `--py-files` and `--files`

```bash
spark-submit \
  --deploy-mode cluster \
  --py-files helper.py \
  --files parameters/ \
  main_et_pool.py
```

However, `--files` doesn't recursively include directories well. Better approach:

### Option 2: Zip your dependencies (Recommended)

```bash
# Create a zip of your helper modules
zip -r dependencies.zip helper.py

# Create a zip/archive of your parameters
zip -r parameters.zip parameters/

# Submit
spark-submit \
  --deploy-mode cluster \
  --py-files dependencies.zip \
  --archives parameters.zip#parameters \
  main_et_pool.py
```

In your code, access parameters via `parameters/` (the alias after `#`).

### Option 3: Upload everything to S3 (Most Reliable for EMR)

```bash
# Upload your project to S3
aws s3 cp helper.py s3://your-bucket/prepay/
aws s3 cp main_et_pool.py s3://your-bucket/prepay/
aws s3 cp --recursive parameters/ s3://your-bucket/prepay/parameters/

# Submit from S3
spark-submit \
  --deploy-mode cluster \
  --py-files s3://your-bucket/prepay/helper.py \
  s3://your-bucket/prepay/main_et_pool.py
```

Then modify your code to read parameters from S3:

```python
# Instead of local file reads
df = spark.read.text("s3://your-bucket/prepay/parameters/2026Q1/file1.sql")
```

### Option 4: Package as a proper Python package

## Option 4: Package as a Python Package (Detailed)

This approach treats your project as an installable Python package, which is the cleanest solution for larger projects
with multiple modules.

### Step 1: Restructure Your Project

```
prepay/
├── setup.py                  # Package configuration
├── prepay/                   # Your actual package (same name as parent)
│   ├── __init__.py           # Makes it a Python package (can be empty)
│   ├── main_et_pool.py
│   └── helper.py
├── parameters/               # Keep outside the package
│   ├── file1.sql
│   └── 2026Q1/
│       └── file1.sql
└── run.py                    # Entry point script (optional)
```

### Step 2: Create `setup.py`

```python
from setuptools import setup, find_packages

setup(
    name="prepay",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        # list any non-Spark dependencies here
    ],
)
```

### Step 3: Create `__init__.py`

Can be empty, or you can expose key functions:

```python
# prepay/__init__.py
from .helper import *  # optional - makes imports cleaner
```

### Step 4: Update Your Imports

In `main_et_pool.py`, change your imports:

```python
# Before (relative import that breaks in cluster mode)
import helper
from helper import some_function

# After (absolute package import)
from prepay import helper
from prepay.helper import some_function
```

### Step 5: Create an Entry Point Script (Optional)

Create `run.py` at the root level:

```python
# run.py
from prepay.main_et_pool import main

if __name__ == "__main__":
    main()
```

And refactor `main_et_pool.py` to have a `main()` function:

```python
# prepay/main_et_pool.py
from prepay.helper import some_function


def main():
    # Your existing code here
    spark = SparkSession.builder.appName("PrepayETL").getOrCreate()
    # ... rest of your logic


if __name__ == "__main__":
    main()
```

### Step 6: Build the Package

```bash
# From the root prepay/ directory
python setup.py bdist_egg

# This creates:
# dist/prepay-0.1-py3.x.egg
```

Alternatively, build a zip:

```bash
cd prepay  # the inner package directory
zip -r ../prepay.zip .
cd ..
```

### Step 7: Submit the Job

```bash
# Using the egg file
spark-submit \
  --deploy-mode cluster \
  --py-files dist/prepay-0.1-py3.10.egg \
  run.py

# Or if your main is inside the package
spark-submit \
  --deploy-mode cluster \
  --py-files dist/prepay-0.1-py3.10.egg \
  prepay/main_et_pool.py
```

### For Your Parameters Files

Since parameters aren't Python code, handle them separately:

**Option A: Read from S3 (Recommended)**

```python
# In your code
sql_content = spark.sparkContext.textFile("s3://bucket/prepay/parameters/2026Q1/file1.sql").collect()
```

**Option B: Use `--archives`**

```bash
zip -r parameters.zip parameters/

spark-submit \
  --deploy-mode cluster \
  --py-files dist/prepay-0.1-py3.10.egg \
  --archives parameters.zip#parameters \
  run.py
```

Then in code, access via `./parameters/file1.sql`.

> see [pyspark_packaging_guide.md](pyspark_packaging_guide.md) for more details
---

### Why This Approach is Better

1. **Proper dependency resolution** - Python understands the package structure
2. **Scalable** - Easy to add more modules, subpackages
3. **Testable** - You can write unit tests and import modules cleanly
4. **Reusable** - Can install the package locally for development with `pip install -e .`

---

**For your SQL parameter files specifically**, since they're data files (not Python), the cleanest approach is to keep
them on S3 and read them from there rather than trying to distribute them with the job.

