# Production Utility Functions

## Overview

Common utility functions used in production Spark jobs for S3 operations, data preparation, and configuration management.

---

## S3 Path Parsing

### parse_s3_path_to_bucket_key()

Parse S3 URI into bucket and key components:

```python
from spdek.functions import parse_s3_path_to_bucket_key

s3_path = "s3://my-bucket/path/to/data/file.parquet"
bucket, key = parse_s3_path_to_bucket_key(s3_path)

print(bucket)  # "my-bucket"
print(key)     # "path/to/data/file.parquet"
```

**Use cases:**
- Extracting components for S3 operations
- Validating S3 paths
- Building dynamic paths

**Error handling:**
```python
try:
    bucket, key = parse_s3_path_to_bucket_key("invalid/path")
except Exception as e:
    print(f"Invalid S3 path: {e}")
    # Raises: "invalid/path: is not a valid s3 path"
```

---

## DataFrame Repartitioning

### repartition_frame_for_even_write()

Repartition DataFrame to achieve target rows per output file:

```python
from spdek.spark import repartition_frame_for_even_write

df = spark.read.parquet("s3://bucket/large_dataset/")

# Target 1 million rows per file
df_balanced = repartition_frame_for_even_write(df, rows_per_file=1000000)

# Results in ceil(total_rows / 1000000) partitions
# Example: 10M row DataFrame → 10 partitions/files
df_balanced.write.parquet("s3://bucket/output/")
```

**Why use this:**
- Avoids "small files problem" (many tiny output files)
- Avoids "large file problem" (few huge output files)
- Optimizes downstream read performance
- Reduces memory pressure on single executor

**Math:**
```python
num_partitions = ceil(df.count() / rows_per_file)

# Example
# df has 10,000,000 rows
# target: 1,000,000 rows per file
# result: 10 partitions/files of ~1M rows each
```

---

## Data Skew Handling

### salt_frame()

Add random salt column to break up skewed partitions:

```python
from spdek.spark import salt_frame

df_skewed = spark.read.parquet("data/")

# Add random salt (0-9)
df_salted = salt_frame(df_skewed, salt_bins=10)

# Result: New column "salt" with random integers 0-9
df_salted.show()
# | id | value | salt |
# | 1  | 100   | 3    |
# | 2  | 200   | 7    |
# | 3  | 150   | 2    |
```

**Use in joins:**
```python
# Salt both tables
left_salted = salt_frame(left_df, salt_bins=10)
right_salted = right_df.withColumn(
    "_salt",
    F.explode(F.array([F.lit(i) for i in range(10)]))
)

# Join on both key and salt
result = left_salted.join(
    right_salted,
    ["join_key", "salt"],
    "inner"
).drop("salt", "_salt")
```

**Benefits:**
- Distributes hot keys across multiple partitions
- Prevents single executor from processing entire hot key
- Enables parallel processing of skewed joins

---

## Argument Parsing

### Standard Argument Parsers

#### get_standard_arg_parser()
```python
from spdek.util import get_standard_arg_parser

parser = get_standard_arg_parser("MyJob")
parser.add_argument("--custom_param", required=True)

args = parser.parse_args()
# Available args:
# - --env (required): dev, tst, stg, prd
# - --bucket (optional): Config bucket
# - --write_override (optional): 0 or 1
# - --dt (optional): YYYY-MM-DD format
```

#### get_date_range_job_arg_parser()
```python
from spdek.util import get_date_range_job_arg_parser

parser = get_date_range_job_arg_parser("BackfillJob")
args = parser.parse_args()

# Available args:
# - --start: Start date (YYYY-MM-DD)
# - --end: End date (YYYY-MM-DD)
# - --env: Environment
# - --bucket: Config bucket
# - --write_override: 0 or 1
```

#### get_standard_date_arg_parser_with_bucket_prefix()
```python
from spdek.util import get_standard_date_arg_parser_with_bucket_prefix

parser = get_standard_date_arg_parser_with_bucket_prefix("MyJob")
args = parser.parse_args()

# Available args:
# - --dt (required): YYYY-MM-DD
# - --bucket_prefix (optional): S3 prefix for configs
```

---

## Configuration Management

### get_conf_data()
Load configuration data from S3 or local file:

```python
from spdek.util import BaseSparkJob

job = BaseSparkJob(env="prod", bucket="config-bucket", bucket_prefix="configs/")

# Load from S3
prod_config = job.get_conf_data("prod.ini")

# Or local file (for testing)
job_local = BaseSparkJob(env="dev", bucket=None)
local_config = job_local.get_conf_data("dev.ini")
```

### get_conf_location()
Build configuration file path:

```python
# With S3 bucket
job = BaseSparkJob(env="prod", bucket="my-bucket", bucket_prefix="configs/")
location = job.get_conf_location("schemas.json")
# Returns: "configs/schemas.json"

# Without S3 bucket (local path)
job_local = BaseSparkJob(env="dev", bucket=None)
location = job_local.get_conf_location("schemas.json")
# Returns: "schemas.json"
```

---

## Schema Management

### set_tables_schema()
Load schemas from JSON configuration:

```python
job = BaseSparkJob(env="prod", bucket="config-bucket", bucket_prefix="configs/")

# Load schemas.json from S3
job.set_tables_schema("schemas.json")

# schemas.json format:
# {
#   "customers": {
#     "type": "struct",
#     "fields": [...]
#   },
#   "orders": {...}
# }
```

### get_spark_schema()
Get StructType for specific table:

```python
schema = job.get_spark_schema("customers")

# Use in read operation
df = job.spark.read.schema(schema).parquet("s3://bucket/customers/")
```

---

## S3 Operations

### is_s3_prefix_populated()
Check if S3 prefix contains data:

```python
job = BaseSparkJob(env="prod", bucket="my-bucket")

exists = job.is_s3_prefix_populated(
    bucket="my-bucket",
    prefix="data/daily/2024-03-16/"
)

if exists:
    print("Data exists, proceed with processing")
else:
    print("No data found, skip processing")
```

### get_latest_partition_val()
Find latest date partition in S3:

```python
latest_dt = job.get_latest_partition_val(
    bucket="my-bucket",
    prefix="data/daily/"
)
print(f"Latest partition: {latest_dt}")
# Returns: "2024-03-16"

# Use for incremental processing
current_dt = latest_dt
input_path = f"s3://my-bucket/data/daily/{current_dt}/"
df = spark.read.parquet(input_path)
```

### get_latest_dt_partition_val()
Find latest "dt" partition in DataFrame:

```python
df = spark.read.parquet("s3://bucket/data/")

latest_dt = job.get_latest_dt_partition_val(
    base_path="s3://bucket/data/",
    format="parquet"
)
print(f"Latest dt: {latest_dt}")
```

---

## Tenant/Business Logic

### get_good_tenants()
Get active, non-fake tenants:

```python
# Load tenant configuration
good_tenants = job.get_good_tenants("s3://bucket/tenant_config/")

# Filter data to valid tenants
df = spark.read.parquet("s3://bucket/events/")
filtered_df = df.join(good_tenants, "tenant", "inner")

# Result: Only data from active, legitimate tenants
```

---

## Complete Example: Using Utilities

```python
from spdek.util import BaseSparkDateJob, get_standard_arg_parser
from spdek.spark import repartition_frame_for_even_write, salt_frame
from spdek.functions import parse_s3_path_to_bucket_key

class ProductionETLJob(BaseSparkDateJob):

    def run(self):
        self.set_spark_conf("ProductionETL", "yarn")

        # 1. Check if input exists
        input_path = f"s3://bucket/input/{self.dt}/"
        bucket, key = parse_s3_path_to_bucket_key(input_path)

        if not self.is_s3_prefix_populated(bucket, key):
            print(f"No data for {self.dt}, skipping")
            return

        # 2. Load with explicit schema
        schema = self.get_spark_schema("input_table")
        df = self.spark.read.schema(schema).parquet(input_path)

        # 3. Transform
        result = df.filter(df.value > 0) \
            .withColumn("normalized", df.value / df.max_value) \
            .groupBy("category").count()

        # 4. Handle skew if needed
        if result.count() > 10000000:
            result = salt_frame(result, salt_bins=10)

        # 5. Repartition for even write
        result = repartition_frame_for_even_write(result, rows_per_file=1000000)

        # 6. Write with controlled mode
        write_mode = self.get_frame_write_mode("output")
        result.write.mode(write_mode).parquet(f"s3://bucket/output/{self.dt}/")

        print(f"✓ Job completed for {self.dt}")

# Execute
if __name__ == "__main__":
    parser = get_standard_arg_parser("ProductionETL")
    args = parser.parse_args()

    job = ProductionETLJob(
        dt=args.dt,
        env=args.env,
        bucket=args.bucket,
        write_override=args.write_override
    )
    job.run()
```

---

## See Also
- [Config-Driven ETL](01-config-driven-etl.md)
- [Data Comparison](02-data-comparison.md)
- [Error Handling](04-error-handling.md)
