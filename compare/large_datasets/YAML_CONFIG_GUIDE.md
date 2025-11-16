# Using YAML Configuration for Dataset Comparison

## 🎯 Why YAML Configuration?

**Benefits:**
- ✅ **Clean separation** of code and config
- ✅ **Version control** your comparison parameters
- ✅ **Reusable configs** for different comparison scenarios
- ✅ **No code changes** needed between runs
- ✅ **Easy to share** and document
- ✅ **Type safety** and validation

## 🚀 Quick Start

### 1. Create Your Config File

```bash
# Copy the template
cp compare_config.yaml my_comparison.yaml

# Edit with your settings
vim my_comparison.yaml
```

### 2. Update Key Settings

```yaml
datasets:
  ds1_path: "s3://your-bucket/dataset1/"
  ds2_path: "s3://your-bucket/dataset2/"
  output_path: "s3://your-bucket/output/"

comparison:
  key_columns:
    - "id"
  skip_columns:
    - "updated_at"
```

### 3. Run the Comparison

```bash
# Using default config file (compare_config.yaml)
spark-submit optimized_compare.py

# Using custom config file
spark-submit optimized_compare.py --config my_comparison.yaml
```

## 📋 Configuration Sections

### 1. Datasets (Required)

```yaml
datasets:
  ds1_path: "s3://bucket/path1/"       # First dataset
  ds2_path: "s3://bucket/path2/"       # Second dataset
  output_path: "s3://bucket/output/"   # Where to save results
```

### 2. Comparison (Required)

```yaml
comparison:
  # Single key column
  key_columns: "id"
  
  # OR multiple key columns
  key_columns:
    - "customer_id"
    - "date"
  
  # Columns to exclude from comparison
  skip_columns:
    - "updated_at"
    - "etl_timestamp"
  
  # Optional: Compare only specific columns
  compare_columns_only:
    - "amount"
    - "status"
```

### 3. Filtering (Optional)

```yaml
filtering:
  enabled: true
  date_column: "date"
  start_date: "2025-01-01"
  end_date: "2025-01-31"
  
  # Custom SQL filters
  ds1_filter: "region = 'US'"
  ds2_filter: "region = 'US'"
```

### 4. Sampling (Optional - for Testing)

```yaml
sampling:
  enabled: true
  fraction: 0.1    # 10% sample
  seed: 42         # For reproducibility
```

### 5. Numeric Tolerance (Optional)

```yaml
numeric_tolerance:
  enabled: true
  columns:
    - "amount"
    - "price"
  tolerance: 0.01  # Allow ±$0.01 difference
```

### 6. Output Settings

```yaml
output:
  max_sample_in_html: 10000
  write_intermediate: false
  
  formats:
    parquet: true   # Always recommended
    csv: false      # For Excel
    json: false     # For APIs
  
  coalesce:
    enabled: true
    num_files: 10
```

### 7. Spark Configuration

```yaml
spark:
  app_name: "MyComparison"
  
  configs:
    spark.sql.adaptive.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    # Add more Spark configs as needed
```

### 8. Caching Strategy

```yaml
caching:
  enabled: true
  storage_level: "MEMORY_AND_DISK"
  # Options: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY
```

### 9. Quality Checks

```yaml
quality_checks:
  enabled: true
  fail_on_null_keys: true
  report_null_values: true
  check_duplicates: true
```

### 10. Logging

```yaml
logging:
  level: "INFO"  # DEBUG, INFO, WARN, ERROR
  save_to_s3: false
  log_path: "s3://bucket/logs/"
```

## 📁 Example Configs

We provide pre-built configs for common scenarios:

### Daily Transaction Comparison
```bash
spark-submit optimized_compare.py --config config_examples/daily_transactions.yaml
```
**Use for:** Daily reconciliation between systems

### Financial Reconciliation
```bash
spark-submit optimized_compare.py --config config_examples/financial_reconciliation.yaml
```
**Use for:** Financial data with rounding tolerance

### Quick Validation
```bash
spark-submit optimized_compare.py --config config_examples/quick_validation.yaml
```
**Use for:** Fast 10% sample test before full run

### Large Dataset Comparison
```bash
spark-submit optimized_compare.py --config config_examples/large_dataset.yaml
```
**Use for:** 100GB+ datasets with optimizations

## 🔧 Common Scenarios

### Scenario 1: Daily Incremental Comparison

```yaml
filtering:
  enabled: true
  date_column: "transaction_date"
  start_date: "2025-01-16"  # Today's date
  end_date: "2025-01-16"

output:
  formats:
    parquet: true
    csv: true    # For business users

notification:
  enabled: true
  email:
    to_addresses:
      - "team@company.com"
```

**Run daily via cron/Airflow:**
```bash
spark-submit optimized_compare.py --config daily_incremental.yaml
```

### Scenario 2: Full Monthly Reconciliation

```yaml
filtering:
  enabled: true
  date_column: "date"
  start_date: "2025-01-01"
  end_date: "2025-01-31"

quality_checks:
  enabled: true
  fail_on_null_keys: true
  check_duplicates: true

spark:
  configs:
    spark.sql.shuffle.partitions: "400"
```

### Scenario 3: Test Run with Sample

```yaml
sampling:
  enabled: true
  fraction: 0.01  # 1% sample

output:
  max_sample_in_html: 500

performance:
  use_approximate_counts: true
```

**Fast validation:**
```bash
spark-submit optimized_compare.py --config test_sample.yaml
# Completes in 2-3 minutes
```

### Scenario 4: Compare Specific Columns Only

```yaml
comparison:
  key_columns: ["id"]
  
  compare_columns_only:
    - "amount"
    - "status"
    - "category"
  
  skip_columns: []  # Not needed when using compare_columns_only
```

**Faster:** Only compares 3 columns instead of 100+

## 🎨 Config Management Best Practices

### 1. Use Environment-Specific Configs

```bash
configs/
├── compare_config.yaml        # Base template
├── prod.yaml                  # Production settings
├── staging.yaml               # Staging settings
└── dev.yaml                   # Development/testing
```

### 2. Version Control Your Configs

```bash
git add config_examples/*.yaml
git commit -m "Add comparison configs"
```

### 3. Use Variables for Dates

```bash
# In your shell script
TODAY=$(date +%Y-%m-%d)

# Update YAML programmatically
sed -i "s/start_date: .*/start_date: \"$TODAY\"/" my_config.yaml

# Then run
spark-submit optimized_compare.py --config my_config.yaml
```

### 4. Document Your Configs

```yaml
# Production Daily Comparison
# Owner: Data Engineering Team
# Schedule: Daily at 2 AM UTC
# Notification: #data-alerts Slack channel

datasets:
  ds1_path: "s3://prod/transactions/"
  # ... rest of config
```

## 🔍 Validation

The script validates your config before running:

```bash
$ spark-submit optimized_compare.py --config bad_config.yaml

❌ Error loading configuration: Missing required field in config: datasets.ds1_path
```

**Common validation errors:**
- Missing required fields
- Invalid paths
- Invalid storage levels
- Invalid log levels

## 📊 Config vs Command Line

| Aspect | YAML Config | Hardcoded |
|--------|------------|-----------|
| **Flexibility** | ✅ Easy to change | ❌ Requires code edit |
| **Versioning** | ✅ Git-friendly | ❌ Code changes |
| **Reusability** | ✅ Multiple configs | ❌ One config |
| **Documentation** | ✅ Self-documenting | ❌ Need comments |
| **Sharing** | ✅ Easy to share | ❌ Share code |

## 🚀 Advanced Usage

### Environment Variables in Config

```yaml
datasets:
  ds1_path: "${DS1_PATH}"
  ds2_path: "${DS2_PATH}"
```

```bash
export DS1_PATH="s3://bucket/data1/"
export DS2_PATH="s3://bucket/data2/"
spark-submit optimized_compare.py --config config.yaml
```

### Programmatic Config Generation

```python
import yaml

config = {
    'datasets': {
        'ds1_path': 's3://bucket/data1/',
        'ds2_path': 's3://bucket/data2/',
        'output_path': f's3://bucket/output/{today}/'
    },
    'comparison': {
        'key_columns': ['id']
    }
}

with open('generated_config.yaml', 'w') as f:
    yaml.dump(config, f)
```

### Config Inheritance (Extend Base Config)

```python
# Load base config
with open('base_config.yaml') as f:
    config = yaml.safe_load(f)

# Override specific settings
config['filtering']['start_date'] = today
config['filtering']['end_date'] = today

# Save custom config
with open('today_config.yaml', 'w') as f:
    yaml.dump(config, f)
```

## 📝 Config Checklist

Before running, verify:

- [ ] `ds1_path` and `ds2_path` are correct
- [ ] `key_columns` exist in both datasets
- [ ] `output_path` has write permissions
- [ ] Date ranges are valid (if using filtering)
- [ ] Spark configs match cluster capacity
- [ ] Sample fraction is reasonable (if sampling)
- [ ] Notification settings are configured (if enabled)

## 🐛 Troubleshooting

### Config file not found
```bash
❌ Error: Config file not found: my_config.yaml

✅ Solution: Use full path or ensure file is in current directory
spark-submit optimized_compare.py --config /full/path/to/my_config.yaml
```

### Invalid YAML syntax
```bash
❌ Error: yaml.scanner.ScannerError

✅ Solution: Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

### Missing required field
```bash
❌ Error: Missing required field in config: comparison.key_columns

✅ Solution: Add the required field to your config
```

## 📚 Complete Example

**File: `production_comparison.yaml`**
```yaml
datasets:
  ds1_path: "s3://prod/transactions/2025/01/"
  ds2_path: "s3://prod/transactions_backup/2025/01/"
  output_path: "s3://prod/comparison-output/2025-01-16/"

comparison:
  key_columns: ["transaction_id"]
  skip_columns: ["updated_at", "sync_time"]

filtering:
  enabled: true
  date_column: "transaction_date"
  start_date: "2025-01-16"
  end_date: "2025-01-16"

output:
  formats:
    parquet: true
    csv: true

quality_checks:
  enabled: true
  fail_on_null_keys: true

notification:
  enabled: true
  email:
    enabled: true
    to_addresses: ["data-team@company.com"]
```

**Run it:**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 16g \
  --num-executors 10 \
  optimized_compare.py \
  --config production_comparison.yaml
```

## 🎯 Summary

**YAML configuration makes your comparison jobs:**
- ✅ More maintainable
- ✅ Easier to version
- ✅ Simpler to reuse
- ✅ Better documented
- ✅ Production-ready

**Start with:** `compare_config.yaml` (template)  
**Customize:** Copy and modify for your needs  
**Run:** `spark-submit optimized_compare.py --config your_config.yaml`

Happy comparing! 🚀
