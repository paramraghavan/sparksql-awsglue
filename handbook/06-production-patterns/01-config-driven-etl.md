# Config-Driven ETL Frameworks

## Overview

Production Spark jobs need:
- Configuration management (different configs per environment)
- Environment handling (dev, test, staging, production)
- Standard argument parsing
- Schema management
- Write mode control
- Error handling
- Logging

This section covers proven production frameworks that handle these requirements.

---

## BaseSparkJob Framework

The **BaseSparkJob** class provides a foundation for all Spark ETL jobs:

```python
from spdek.util import BaseSparkJob
import logging

class MyETLJob(BaseSparkJob):
    """
    Extends BaseSparkJob for environment-aware configuration
    """

    def __init__(self, env: str, bucket: str = None, bucket_prefix: str = None,
                 write_override: int = 0):
        super().__init__(env, bucket, bucket_prefix, write_override)
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main job logic"""
        # Set Spark configuration
        self.set_spark_conf(
            app_name="MyETLJob",
            master="yarn"  # EMR: set via spark-submit
        )

        # Load data
        df = self.spark.read.parquet("s3://bucket/input/")

        # Transform
        result = df.filter(df.age > 30).groupBy("dept").count()

        # Write with configured mode
        write_mode = self.get_frame_write_mode(data_set_key="output_dataset")
        result.write.mode(write_mode).parquet("s3://bucket/output/")
```

### Key Features

#### 1. **Environment Configuration**
```python
# Read different configs per environment
job = MyETLJob(env="prod", bucket="config-bucket", bucket_prefix="configs/")

# Config files: dev.ini, test.ini, stg.ini, prd.ini
# Read from S3 or local file
config_data = job.get_conf_data("prd.ini")
```

#### 2. **Argument Parsing**
```python
from spdek.util import get_standard_arg_parser

# Standard parser with --env, --bucket, --write_override
parser = get_standard_arg_parser("MyJob")
parser.add_argument("--custom_param", required=True)

args = parser.parse_args()
job = MyETLJob(env=args.env, bucket=args.bucket)
```

#### 3. **Write Mode Control**
```python
# Control write behavior per environment or dataset
job.output_data_sets_dict = {
    "output_path_1": "overwrite",  # Always overwrite
    "output_path_2": "append",     # Append new data
}

# Enable override at runtime
spark-submit job.py --write_override 1  # Force overwrite mode
```

#### 4. **Schema Management**
```python
# Load schemas from configuration
job.set_tables_schema("schemas.json")  # Load from config

# Get schema for specific table
output_schema = job.get_spark_schema("output_table")

# Use schema when reading
df = job.spark.read.schema(output_schema).csv("input.csv")
```

#### 5. **Optimized Spark Configuration**
```python
# Automatic configuration for production
[
    ("spark.shuffle.registration.timeout", "20000"),
    ("spark.sql.broadcastTimeout", "20000"),
    ("spark.shuffle.registration.maxAttempts", "6"),
    ("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true"),
    ("spark.driver.maxResultSize", "0")  # Unlimited result size
]

# Hadoop optimizations
- mapreduce.fileoutputcommitter.marksuccessfuljobs = false
- parquet.enable.summary-metadata = false
```

---

## Date-Based Jobs

For jobs processing daily data:

```python
from spdek.util import BaseSparkDateJob, get_standard_arg_parser

class DailyETLJob(BaseSparkDateJob):

    def run(self):
        self.set_spark_conf("DailyJob", "yarn")

        # Use date from arguments
        input_path = f"s3://bucket/data/dt={self.dt}/"

        df = self.spark.read.parquet(input_path)
        result = df.groupBy("key").count()

        output_path = f"s3://bucket/output/dt={self.dt}/"
        result.write.mode("overwrite").parquet(output_path)

# Usage
parser = get_standard_arg_parser("DailyJob")
args = parser.parse_args()  # --dt YYYY-MM-DD, --env prod, etc.

job = DailyETLJob(
    dt=args.dt,
    env=args.env,
    bucket=args.bucket,
    write_override=args.write_override
)
job.run()
```

### Command Line
```bash
spark-submit job.py \
    --dt 2024-03-16 \
    --env prod \
    --bucket my-config-bucket \
    --bucket_prefix glue_scripts
```

---

## Date Range Jobs

For backfill or range processing:

```python
from spdek.util import BaseSparkDateRangeJob

class BackfillJob(BaseSparkDateRangeJob):

    def run(self):
        self.set_spark_conf("BackfillJob", "yarn")

        # Process date range
        df = self.spark.read.parquet(f"s3://bucket/data/dt={self.start}/")

        # Can also process multiple dates
        for dt in self.get_date_range(self.start, self.end):
            path = f"s3://bucket/data/dt={dt}/"
            df = self.spark.read.parquet(path)
            # Process...

# Usage: --start 2024-01-01 --end 2024-03-31
```

---

## Configuration Files

### Example dev.ini
```ini
[database]
host = localhost
port = 5432

[s3]
bucket = dev-bucket
prefix = data/

[spark]
shuffle_partitions = 50
memory = 4g

[processing]
batch_size = 10000
timeout = 300
```

### Example schemas.json
```json
{
  "customers": {
    "type": "struct",
    "fields": [
      {"name": "id", "type": "long", "nullable": false},
      {"name": "name", "type": "string", "nullable": true},
      {"name": "age", "type": "integer", "nullable": true}
    ]
  },
  "output": {
    "type": "struct",
    "fields": [
      {"name": "customer_id", "type": "long"},
      {"name": "total_amount", "type": "decimal(10,2)"}
    ]
  }
}
```

---

## Real Production Example

### The Job
```python
from spdek.util import BaseSparkDateJob, get_standard_arg_parser
import pyspark.sql.functions as F

class DailyCustomerMetricsJob(BaseSparkDateJob):

    def __init__(self, dt, env, bucket=None, write_override=0, bucket_prefix="glue_scripts"):
        super().__init__(dt, env, bucket, write_override, bucket_prefix)
        # Load schemas
        self.set_tables_schema("schemas.json")

    def run(self):
        # Setup Spark
        self.set_spark_conf(
            app_name="DailyCustomerMetrics",
            master="yarn"
        )

        # Load customers with schema
        customers_schema = self.get_spark_schema("customers")
        customers = self.spark.read \
            .schema(customers_schema) \
            .parquet(f"s3://bucket/customers/dt={self.dt}/")

        # Load transactions
        transactions = self.spark.read \
            .parquet(f"s3://bucket/transactions/dt={self.dt}/")

        # Transform
        metrics = customers.join(transactions, "customer_id") \
            .groupBy("customer_id", "name") \
            .agg(
                F.sum("amount").alias("total_amount"),
                F.count("*").alias("transaction_count"),
                F.avg("amount").alias("avg_amount")
            )

        # Write with configured mode
        write_mode = self.get_frame_write_mode("metrics_output")
        metrics.write \
            .mode(write_mode) \
            .parquet(f"s3://bucket/metrics/dt={self.dt}/")

if __name__ == "__main__":
    parser = get_standard_arg_parser("DailyCustomerMetrics")
    args = parser.parse_args()

    job = DailyCustomerMetricsJob(
        dt=args.dt,
        env=args.env,
        bucket=args.bucket,
        write_override=args.write_override
    )
    job.run()
```

### Command Line
```bash
# Development (local)
spark-submit --master local[*] job.py \
    --dt 2024-03-16 \
    --env dev

# Production (EMR)
spark-submit --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-memory 8g \
    --conf spark.sql.shuffle.partitions=500 \
    job.py \
    --dt 2024-03-16 \
    --env prod \
    --bucket my-config-bucket \
    --bucket_prefix glue_scripts
```

---

## Best Practices

### 1. **Environment-Specific Configuration**
- Keep dev/test/prod configs separate
- Never hardcode values
- Store credentials in AWS Secrets Manager or IAM roles

### 2. **Schema Validation**
```python
# Always use explicit schemas in production
df = self.spark.read.schema(schema).parquet(path)

# Not this:
df = self.spark.read.option("inferSchema", "true").parquet(path)
```

### 3. **Write Mode Control**
```python
# Production: use "ignore" by default (don't overwrite)
write_mode = self.get_frame_write_mode()

# Only override with caution and explicit flag
spark-submit job.py --write_override 1
```

### 4. **Logging**
```python
self.logger.info(f"Processing data for {self.dt}")
self.logger.warning(f"Processed {row_count} rows")
if error:
    self.logger.error(f"Job failed: {error}")
```

### 5. **Error Handling**
```python
try:
    result = df.groupBy("key").count()
except Exception as e:
    self.logger.error(f"Aggregation failed: {e}")
    raise
```

---

## Common Issues & Solutions

### Issue: "Cannot read configuration"
```python
# Make sure bucket_prefix is set if using bucket
if bucket and not bucket_prefix:
    raise Exception("if passing bucket, must pass with bucket prefix")

# Or use default
job = MyETLJob(
    env="prod",
    bucket="config-bucket",
    bucket_prefix="glue_scripts"  # Default
)
```

### Issue: "Write mode conflict"
```python
# Check what write mode will be used
write_mode = job.get_frame_write_mode("dataset_key")
print(f"Writing with mode: {write_mode}")

# Control via config or flag
job.output_data_sets_dict = {"dataset_key": "overwrite"}
# OR
spark-submit job.py --write_override 1
```

---

## See Also
- [Error Handling](04-error-handling.md)
- [Monitoring & Logging](05-monitoring-logging.md)
- [SparkSession Configuration](../01-fundamentals/05-spark-session.md)
