# Robust PySpark Dataset Comparator

A production-ready solution for comparing large Parquet datasets (50GB+) on AWS EMR without job failures.

## Key Features

✅ **Avoids expensive `df.count()` operations** that cause job failures  
✅ **Incremental processing** - writes intermediate results to disk  
✅ **Uses efficient join strategies** - left anti join instead of full outer join  
✅ **Handles 100+ columns** without memory issues  
✅ **Null-safe comparisons** - properly handles NULL values  
✅ **HTML report generation** with summary statistics  
✅ **Configurable** - skip columns, multiple key columns  

## Why This Works Better

### Problems with Traditional Approaches:
1. **Full outer joins** on 50GB datasets cause massive shuffles
2. **`df.count()`** on large joined DataFrames runs out of memory
3. **Collecting all differences** to driver causes OOM errors
4. **Not persisting intermediate results** causes recomputation

### Our Solutions:
1. ✅ Use **left anti joins** to find unique rows (much more efficient)
2. ✅ Use **`rdd.count()`** which is more stable than `df.count()`
3. ✅ **Write to Parquet** then count from files (avoids recomputation)
4. ✅ Process differences **incrementally** by column
5. ✅ **Sample for HTML report** instead of collecting everything
6. ✅ Use **adaptive query execution** for better performance

## Quick Start

### 1. Configure Your Comparison

Edit the configuration section in `parquet_dataset_compare.py`:

```python
# Paths to your parquet datasets
DS1_PATH = "s3://your-bucket/path/to/dataset1/"
DS2_PATH = "s3://your-bucket/path/to/dataset2/"

# Key columns for joining
KEY_COLUMNS = ["customer_id", "transaction_date"]

# Columns to skip from comparison
SKIP_COLUMNS = ["updated_at", "processed_timestamp", "etl_run_id"]

# Output path for results
OUTPUT_PATH = "s3://your-bucket/comparison-output/"
```

### 2. Submit to EMR

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  parquet_dataset_compare.py
```

### 3. Review Results

The script generates:
- **Parquet files** with detailed differences: `s3://your-bucket/comparison-output/differences/`
- **HTML report**: `/tmp/comparison_report.html` (also check your output path)
- **Console summary**: Printed at the end of job

## Output Files Structure

```
comparison-output/
├── ds1_only/           # Rows only in dataset 1 (Parquet)
├── ds2_only/           # Rows only in dataset 2 (Parquet)
├── common_keys/        # Common rows between datasets (Parquet)
├── differences/        # All column-level differences (Parquet)
└── comparison_report.html  # HTML summary report
```

## Understanding the Output

### Console Output Example:
```
📊 COMPARISON SUMMARY
--------------------------------------------------------------------------------
  Ds1 Total: 1,234,567,890
  Ds2 Total: 1,234,500,000
  Ds1 Only: 67,890
  Ds2 Only: 0
  Common Rows: 1,234,500,000
  Rows With Differences: 15,432
  Total Differences: 45,289
--------------------------------------------------------------------------------
```

### Differences DataFrame Schema:
```
root
 |-- customer_id: string          # Key column(s)
 |-- transaction_date: date       # Key column(s)
 |-- column_name: string          # Which column differs
 |-- ds1_value: string            # Value in dataset 1
 |-- ds2_value: string            # Value in dataset 2
```

## Advanced Configuration

### For Very Large Datasets (100GB+)

```python
spark = SparkSession.builder \
    .appName("DatasetComparison") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "400")  # Increase for larger data \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()
```

### EMR Cluster Recommendations

For 50GB datasets:
- **Instance type**: r5.4xlarge or r5.8xlarge (memory-optimized)
- **Core nodes**: 10-20 nodes
- **EBS storage**: 100GB+ per node
- **Spark config**: Enable dynamic allocation

```bash
# EMR configuration
aws emr create-cluster \
  --name "Dataset-Comparison" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --instance-type r5.4xlarge \
  --instance-count 15 \
  --use-default-roles \
  --configurations file://spark-config.json
```

spark-config.json:
```json
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.sql.adaptive.enabled": "true",
      "spark.dynamicAllocation.enabled": "true",
      "spark.dynamicAllocation.minExecutors": "5",
      "spark.dynamicAllocation.maxExecutors": "50",
      "spark.sql.shuffle.partitions": "200"
    }
  }
]
```

## Troubleshooting

### Issue: Job still fails on count operations
**Solution**: Increase shuffle partitions
```python
.config("spark.sql.shuffle.partitions", "400")  # or higher
```

### Issue: Out of memory errors
**Solution**: 
1. Use larger executor memory: `--executor-memory 32g`
2. Reduce executor cores: `--executor-cores 2`
3. More memory per core = better for large datasets

### Issue: Job takes too long
**Solution**:
1. Check if your data is partitioned - use partitioned reads
```python
ds1 = spark.read.parquet(DS1_PATH).repartition(200)
```
2. Enable S3 optimizations:
```python
.config("spark.hadoop.fs.s3a.connection.maximum", "100")
.config("spark.hadoop.fs.s3a.threads.max", "50")
```

### Issue: Too many small files in output
**Solution**: Coalesce before writing
```python
all_diffs.coalesce(10).write.mode("overwrite").parquet(output_file)
```

### Issue: Need approximate counts faster
**Solution**: Use sampling
```python
# Approximate count (much faster)
approx_count = ds1.sample(0.01).count() * 100  # 1% sample
```

## Analyzing Results

### Query differences using Spark SQL:
```python
# Read the differences
diffs = spark.read.parquet("s3://your-bucket/comparison-output/differences/")

# Find most common differences
diffs.groupBy("column_name").count().orderBy(F.desc("count")).show()

# Get differences for specific keys
diffs.filter(F.col("customer_id") == "CUST123").show(truncate=False)

# Export specific differences to CSV
diffs.filter(F.col("column_name") == "amount") \
     .coalesce(1) \
     .write.csv("s3://bucket/amount_diffs.csv", header=True)
```

### Using AWS Athena:
```sql
-- Create external table
CREATE EXTERNAL TABLE comparison_diffs (
    customer_id STRING,
    transaction_date DATE,
    column_name STRING,
    ds1_value STRING,
    ds2_value STRING
)
STORED AS PARQUET
LOCATION 's3://your-bucket/comparison-output/differences/';

-- Query differences
SELECT column_name, COUNT(*) as diff_count
FROM comparison_diffs
GROUP BY column_name
ORDER BY diff_count DESC;
```

## Performance Tips

1. **Partition your input data** by date or other logical keys
2. **Use columnar storage** (Parquet with snappy compression)
3. **Filter early** if you only need to compare recent data
4. **Use broadcast joins** for small lookup tables (disable for large joins)
5. **Monitor Spark UI** to identify bottlenecks
6. **Clean up S3** - remove old comparison outputs

## Common Use Cases

### Compare only recent data:
```python
ds1 = spark.read.parquet(DS1_PATH).filter(F.col("date") >= "2025-01-01")
ds2 = spark.read.parquet(DS2_PATH).filter(F.col("date") >= "2025-01-01")
```

### Compare specific columns only:
```python
# Select only needed columns before comparison
needed_cols = KEY_COLUMNS + ["amount", "status", "category"]
ds1 = spark.read.parquet(DS1_PATH).select(needed_cols)
ds2 = spark.read.parquet(DS2_PATH).select(needed_cols)
```

### Handle schema differences:
```python
# Align schemas before comparison
common_cols = set(ds1.columns).intersection(set(ds2.columns))
ds1 = ds1.select(*common_cols)
ds2 = ds2.select(*common_cols)
```

## Monitoring Job Progress

Watch for these indicators in Spark UI:
- ✅ **Even task distribution** - tasks should be balanced across executors
- ✅ **No shuffle spill** - if you see spill, increase executor memory
- ✅ **Low GC time** - should be <10% of task time
- ❌ **Stragglers** - indicates data skew, repartition input

