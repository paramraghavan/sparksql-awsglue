# Large-Scale Parquet Comparison for EMR

High-performance PySpark solution for comparing massive parquet files (50GB+, 200+ columns) on AWS EMR.

## Features

✅ **Scalable**: Handles 50GB+ files with 200+ columns efficiently  
✅ **Comprehensive**: Record counts, schema differences, unique records, column-level comparisons  
✅ **Smart**: Adaptive query execution, skew join handling, intelligent caching  
✅ **Reporting**: Beautiful, interactive HTML reports with pagination  
✅ **Flexible**: Configurable key columns and skip columns  

## Quick Start

### 1. Upload Script to S3

```bash
aws s3 cp parquet_comparison.py s3://your-bucket/scripts/
```

### 2. Submit EMR Job

**Option A: Using the submission script**

```bash
# Edit submit_comparison.sh with your paths and settings
chmod +x submit_comparison.sh
./submit_comparison.sh
```

**Option B: Direct spark-submit on EMR cluster**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 8g \
  --conf spark.sql.shuffle.partitions=400 \
  s3://your-bucket/scripts/parquet_comparison.py \
  --path1 s3://bucket/file1.parquet \
  --path2 s3://bucket/file2.parquet \
  --keys "id,date,customer_id" \
  --skip "timestamp,updated_at" \
  --name1 "Production" \
  --name2 "Staging" \
  --output s3://bucket/reports/report.html \
  --partitions 400
```

### 3. Download Report

```bash
aws s3 cp s3://your-bucket/reports/comparison_report.html ./
open comparison_report.html
```

## Command-Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--path1` | Yes | Path to first parquet file/directory | `s3://bucket/data1.parquet` |
| `--path2` | Yes | Path to second parquet file/directory | `s3://bucket/data2.parquet` |
| `--keys` | Yes | Comma-separated key columns | `"id,date"` |
| `--skip` | No | Comma-separated columns to skip | `"timestamp,audit_col"` |
| `--name1` | No | Name for first dataset | `"Production"` |
| `--name2` | No | Name for second dataset | `"Staging"` |
| `--output` | No | Output path for HTML report | `s3://bucket/report.html` |
| `--partitions` | No | Number of shuffle partitions | `400` |

## Performance Optimization

### Sizing Guidelines

For **50GB files with 200+ columns**, recommended EMR configuration:

```
Instance Type: r5.2xlarge (8 cores, 64GB RAM)
Number of Nodes: 5-10
Executors: 20-40
Executor Memory: 8-12g
Executor Cores: 4-5
Shuffle Partitions: 400-800
```

### Tuning Parameters

#### Shuffle Partitions

```bash
# Small data (< 10GB): 100-200
--partitions 150

# Medium data (10-50GB): 200-400
--partitions 300

# Large data (50-100GB): 400-800
--partitions 600

# Very large (100GB+): 800-1600
--partitions 1200
```

#### Executor Configuration

```bash
# Memory-intensive comparisons (many columns)
--executor-memory 12g --driver-memory 12g

# CPU-intensive comparisons (complex keys)
--executor-cores 5 --num-executors 30

# Balanced approach
--executor-cores 4 --executor-memory 8g --num-executors 20
```

### AWS EMR Best Practices

1. **Use Instance Fleets**
   ```bash
   # Mix of instance types for cost optimization
   # On-demand for master, Spot for core/task nodes
   ```

2. **Enable Dynamic Allocation** (for variable workloads)
   ```bash
   --conf spark.dynamicAllocation.enabled=true
   --conf spark.dynamicAllocation.minExecutors=5
   --conf spark.dynamicAllocation.maxExecutors=40
   ```

3. **Optimize Parquet Reading**
   ```bash
   --conf spark.sql.files.maxPartitionBytes=134217728  # 128MB
   --conf spark.sql.parquet.filterPushdown=true
   --conf spark.sql.parquet.mergeSchema=false
   ```

4. **Network Optimization**
   ```bash
   --conf spark.network.timeout=800s
   --conf spark.executor.heartbeatInterval=60s
   ```

## Comparison Process

The script performs the following steps:

1. **Load Data**: Reads both parquet files with optimizations
2. **Schema Comparison**: Identifies common and unique columns
3. **Record Counts**: Compares total record counts
4. **Key-Based Matching**: Finds records unique to each dataset
5. **Column-Level Comparison**: Compares values for matching records
6. **Report Generation**: Creates interactive HTML report

## Output

### Console Output

```
================================================================================
STARTING PARQUET COMPARISON
================================================================================
Loading Dataset1 from s3://bucket/file1.parquet...
Dataset1 loaded: 10,000,000 records, 250 columns

Comparing schemas...
Common columns: 240
Only in DF1: 5
Only in DF2: 5

Finding unique records...
Records only in DF1: 50,000
Records only in DF2: 45,000

Comparing column values for matching records...
Comparing 235 columns...
  price: 1,234 differences (0.01%)
  status: 5,678 differences (0.06%)

================================================================================
COMPARISON COMPLETE
================================================================================
```

### HTML Report Features

- **Summary Dashboard**: Key metrics with visual cards
- **Schema Comparison**: Columns unique to each dataset
- **Unique Records**: Counts and samples
- **Column Differences**: Sortable table with search
- **Sample Data**: Expandable sections with actual differences
- **Interactive**: Collapsible sections, search, pagination

### Generated Files

```
/tmp/Dataset1_unique_records/  # Sample of unique records (1000 max)
/tmp/Dataset2_unique_records/  # Sample of unique records (1000 max)
s3://bucket/reports/comparison_report.html  # Main report
```

## Troubleshooting

### Out of Memory Errors

**Symptom**: `java.lang.OutOfMemoryError` or executors lost

**Solutions**:
```bash
# Increase executor memory
--executor-memory 12g --driver-memory 12g

# Increase shuffle partitions
--partitions 800

# Reduce executor cores (more memory per core)
--executor-cores 3

# Enable memory overhead
--conf spark.executor.memoryOverhead=2048
```

### Slow Performance

**Symptom**: Job takes hours or appears stuck

**Solutions**:
```bash
# Check data skew - increase partitions
--partitions 1000

# Enable adaptive query execution (already enabled in script)
--conf spark.sql.adaptive.enabled=true

# Use more executors
--num-executors 40

# Check if data is partitioned - read specific partitions
--path1 "s3://bucket/data/year=2024/month=11/*"
```

### Network Timeouts

**Symptom**: `Connection timeout` or `Lost executor`

**Solutions**:
```bash
--conf spark.network.timeout=1200s
--conf spark.executor.heartbeatInterval=120s
--conf spark.rpc.message.maxSize=512
```

### Too Many Small Files

**Symptom**: Job spends long time in listing files

**Solutions**:
```bash
# Coalesce input partitions
--conf spark.sql.files.maxPartitionBytes=268435456  # 256MB

# Or pre-process data
spark-submit coalesce_parquet.py \
  --input s3://bucket/small_files/ \
  --output s3://bucket/coalesced/ \
  --target-size 256
```

## Advanced Features

### Comparing Specific Partitions

```bash
# Compare only specific date range
--path1 "s3://bucket/data/year=2024/month={10,11}/*" \
--path2 "s3://bucket/data2/year=2024/month={10,11}/*"
```

### Handling Schema Evolution

```python
# The script automatically handles schema differences
# Columns only in one dataset are reported but don't cause failures
```

### Custom Key Combinations

```bash
# Single key
--keys "transaction_id"

# Composite key
--keys "customer_id,order_date,product_id"

# Including timestamps (if part of uniqueness)
--keys "user_id,event_timestamp"
```

### Skipping Metadata Columns

```bash
# Skip audit/system columns
--skip "created_at,updated_at,last_modified,etl_timestamp"

# Skip derived columns
--skip "full_name,total_price,calculated_field"
```

## Monitoring

### EMR Console

1. Go to EMR Console → Clusters → Your Cluster
2. Click "Steps" tab to see job status
3. Click step ID → "View logs" for detailed logs

### Spark UI

```bash
# Get master public DNS from EMR console
ssh -i your-key.pem -N -L 8157:localhost:8088 hadoop@<master-public-dns>

# Open in browser
http://localhost:8157
```

### CloudWatch Metrics

Monitor these metrics:
- `AppsRunning`: Should be 1 when job is running
- `ContainerAllocated`: Should match your executor count
- `MemoryAvailableMB`: Ensure sufficient memory
- `YARNMemoryAvailablePercentage`: Should not hit 0

## Cost Optimization

### Spot Instances

```bash
# Use Spot for core/task nodes (save 70-90%)
# Use On-Demand for master node (stability)
```

### Right-Sizing

```bash
# Start small, then scale up if needed
# 5 nodes → 10 nodes → 20 nodes
# Monitor CPU and memory utilization

# Rule of thumb: Data size * 4 = Total cluster memory
# 50GB data → 200GB total memory → 4x r5.2xlarge (64GB each)
```

### Auto-Termination

```bash
# Add to EMR cluster configuration
--auto-terminate \
--termination-protected=false
```

## Example Scenarios

### Scenario 1: Daily Production vs Staging Validation

```bash
spark-submit parquet_comparison.py \
  --path1 s3://prod-data/transactions/date=2024-11-13/ \
  --path2 s3://staging-data/transactions/date=2024-11-13/ \
  --keys "transaction_id" \
  --skip "loaded_at,pipeline_version" \
  --name1 "Production" \
  --name2 "Staging" \
  --output s3://reports/validation-2024-11-13.html
```

### Scenario 2: Migration Validation

```bash
spark-submit parquet_comparison.py \
  --path1 s3://legacy-system/customers.parquet \
  --path2 s3://new-system/customers.parquet \
  --keys "customer_id,effective_date" \
  --skip "legacy_id,migration_timestamp" \
  --name1 "Legacy" \
  --name2 "NewSystem" \
  --output s3://migration/validation-report.html \
  --partitions 800
```

### Scenario 3: Incremental Data Validation

```bash
spark-submit parquet_comparison.py \
  --path1 s3://data/full/snapshot-2024-11-12.parquet \
  --path2 s3://data/full/snapshot-2024-11-13.parquet \
  --keys "id,version" \
  --name1 "Yesterday" \
  --name2 "Today" \
  --output s3://reports/daily-diff-2024-11-13.html
```

## Extending the Script

### Adding Custom Metrics

```python
# In compare_column_values method, add:
null_count_df1 = joined.filter(F.col(col).isNull()).count()
null_count_df2 = joined.filter(joined[col].isNull()).count()
```

### Adding Data Profiling

```python
# Add statistical profiling
df1.select([F.mean(col), F.stddev(col), F.min(col), F.max(col)])
```

### Custom Report Formats

```python
# Export to CSV, JSON, or Excel
results_df = spark.createDataFrame(column_differences)
results_df.write.csv("s3://bucket/results.csv")
```

## FAQ

**Q: How long will it take to compare 50GB files?**  
A: With proper configuration (20 executors, r5.2xlarge), expect 15-30 minutes.

**Q: Can I compare files with different schemas?**  
A: Yes! The script handles schema differences gracefully and reports them.

**Q: What if my key columns have nulls?**  
A: The script handles nulls by coalescing them to "NULL" string for matching.

**Q: Can I run this on local Spark instead of EMR?**  
A: Yes, but EMR is recommended for 50GB+ files. For local: `spark-submit --master local[*]`

**Q: How do I compare more than 2 files?**  
A: Run pairwise comparisons or modify the script to accept multiple files.

**Q: Can I compare Delta Lake or Hive tables?**  
A: Yes! Change `spark.read.parquet()` to `spark.read.format("delta")` or `spark.table()`

## Support

For issues or questions:
1. Check CloudWatch logs for errors
2. Review Spark UI for performance bottlenecks
3. Adjust configuration parameters based on your data size
4. Consider data sampling for initial testing

## License

MIT License - Feel free to modify and extend for your needs.
