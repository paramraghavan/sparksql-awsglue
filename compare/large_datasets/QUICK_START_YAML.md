# 🚀 Quick Start - YAML Configuration Edition

## What's New: YAML Configuration

**No more hardcoded paths!** Now you configure everything via YAML:

```yaml
# compare_config.yaml
datasets:
  ds1_path: "s3://your-bucket/dataset1/"
  ds2_path: "s3://your-bucket/dataset2/"
  output_path: "s3://your-bucket/output/"

comparison:
  key_columns: ["id"]
  skip_columns: ["updated_at"]
```

## 📦 Complete Solution Package

### ✅ **Use This** → [optimized_compare.py](computer:///mnt/user-data/outputs/optimized_compare.py)
The main script with all optimizations (YAML-enabled)

### ⚙️ **Configure** → [compare_config.yaml](computer:///mnt/user-data/outputs/compare_config.yaml)
Your configuration file template

### 🏃 **Run** → [run_comparison.sh](computer:///mnt/user-data/outputs/run_comparison.sh)
Convenience runner script

### 📚 **Learn** → [YAML_CONFIG_GUIDE.md](computer:///mnt/user-data/outputs/YAML_CONFIG_GUIDE.md)
Complete YAML configuration documentation

---

## 🎯 Get Started in 3 Steps

### Step 1: Edit Configuration (2 minutes)

```bash
# Copy the template
cp compare_config.yaml my_comparison.yaml

# Edit paths and key columns
vim my_comparison.yaml
```

**Minimum required config:**
```yaml
datasets:
  ds1_path: "s3://your-bucket/dataset1/"
  ds2_path: "s3://your-bucket/dataset2/"
  output_path: "s3://your-bucket/output/"

comparison:
  key_columns: ["id"]  # Your join key(s)
```

### Step 2: Run Comparison

#### Option A: Using the Runner Script (Easiest)
```bash
# Local test
./run_comparison.sh --local --config my_comparison.yaml

# On EMR cluster
./run_comparison.sh --config my_comparison.yaml --cluster j-XXXXX
```

#### Option B: Direct spark-submit
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 16g \
  --num-executors 10 \
  optimized_compare.py \
  --config my_comparison.yaml
```

### Step 3: Check Results

Results are saved to your `output_path`:
```
s3://your-bucket/output/
├── differences/              # Main results (Parquet)
└── comparison_report.html    # HTML summary
```

---

## 📊 Example Configurations

We provide ready-to-use configs for common scenarios:

### 1. Daily Transaction Comparison
```bash
./run_comparison.sh \
  --config config_examples/daily_transactions.yaml \
  --cluster j-XXXXX
```
**Use for:** Daily reconciliation between systems

### 2. Financial Reconciliation
```bash
./run_comparison.sh \
  --config config_examples/financial_reconciliation.yaml \
  --cluster j-XXXXX
```
**Features:**
- Numeric tolerance ($0.01)
- Email notifications
- Audit trail

### 3. Quick Validation (10% Sample)
```bash
./run_comparison.sh \
  --config config_examples/quick_validation.yaml \
  --cluster j-XXXXX
```
**Perfect for:** Testing before full run (completes in 2-3 min)

### 4. Large Dataset (100GB+)
```bash
./run_comparison.sh \
  --config config_examples/large_dataset.yaml \
  --cluster j-XXXXX
```
**Optimizations:**
- Higher shuffle partitions (400)
- Longer timeouts
- Serialized caching

---

## 🔧 Common Configurations

### Filter by Date
```yaml
filtering:
  enabled: true
  date_column: "transaction_date"
  start_date: "2025-01-16"
  end_date: "2025-01-16"
```

### Multiple Key Columns
```yaml
comparison:
  key_columns:
    - "customer_id"
    - "order_id"
    - "date"
```

### Skip Metadata Columns
```yaml
comparison:
  skip_columns:
    - "updated_at"
    - "processed_date"
    - "etl_run_id"
```

### Compare Specific Columns Only
```yaml
comparison:
  compare_columns_only:
    - "amount"
    - "status"
    - "quantity"
```

### Allow Rounding Differences
```yaml
numeric_tolerance:
  enabled: true
  columns:
    - "amount"
    - "price"
  tolerance: 0.01  # $0.01 tolerance
```

### Export to CSV for Excel
```yaml
output:
  formats:
    parquet: true
    csv: true      # Enable CSV export
    json: false
```

---

## 📈 Performance by Dataset Size

| Dataset Size | Cluster Config | Expected Time |
|--------------|----------------|---------------|
| **10GB** | 5 × r5.2xlarge | 5-10 min |
| **50GB** | 10 × r5.4xlarge | 15-20 min |
| **100GB** | 20 × r5.4xlarge | 30-45 min |
| **200GB** | 30 × r5.4xlarge | 1-2 hours |

**Config adjustment for size:**
```yaml
# For 100GB+
spark:
  configs:
    spark.sql.shuffle.partitions: "400"  # Increase from 200
```

---

## 🎨 Workflow Examples

### Daily Automated Comparison

**1. Create config:** `daily_comparison.yaml`
```yaml
filtering:
  enabled: true
  date_column: "date"
  start_date: "{{TODAY}}"  # Replaced by script
  end_date: "{{TODAY}}"

notification:
  enabled: true
  email:
    to_addresses: ["team@company.com"]
```

**2. Shell script:** `run_daily.sh`
```bash
#!/bin/bash
TODAY=$(date +%Y-%m-%d)
sed "s/{{TODAY}}/$TODAY/g" daily_comparison.yaml > today.yaml
./run_comparison.sh --config today.yaml --cluster j-XXXXX
```

**3. Schedule with cron:**
```bash
0 2 * * * /path/to/run_daily.sh  # Daily at 2 AM
```

### Multi-Environment Setup

```bash
configs/
├── base.yaml           # Common settings
├── production.yaml     # Prod paths
├── staging.yaml        # Staging paths
└── development.yaml    # Dev paths
```

**Run different environments:**
```bash
# Production
./run_comparison.sh --config configs/production.yaml --cluster j-PROD

# Staging
./run_comparison.sh --config configs/staging.yaml --cluster j-STAGING
```

---

## 🐛 Troubleshooting

### Config file not found
```bash
❌ Error: Config file not found: my_config.yaml

✅ Solution: Check path or use full path
./run_comparison.sh --config /full/path/to/my_config.yaml
```

### Missing key columns
```bash
❌ Error: Key column 'id' not found in DS1

✅ Solution: Verify column name in your data
aws s3 cp s3://bucket/dataset1/ - --recursive | head -1
```

### Job fails with OOM
```bash
❌ Error: java.lang.OutOfMemoryError

✅ Solution: Increase shuffle partitions in config
spark:
  configs:
    spark.sql.shuffle.partitions: "400"  # Up from 200
```

### Too slow?
```bash
✅ Solution 1: Add date filtering
filtering:
  enabled: true
  start_date: "2025-01-01"
  end_date: "2025-01-31"

✅ Solution 2: Test with sample first
sampling:
  enabled: true
  fraction: 0.1
```

---

## 📚 Documentation

| File | Purpose |
|------|---------|
| [YAML_CONFIG_GUIDE.md](computer:///mnt/user-data/outputs/YAML_CONFIG_GUIDE.md) | Complete YAML reference |
| [BUG_FIX_EXPLAINED.md](computer:///mnt/user-data/outputs/BUG_FIX_EXPLAINED.md) | Why old code failed |
| [BEFORE_AFTER_COMPARISON.md](computer:///mnt/user-data/outputs/BEFORE_AFTER_COMPARISON.md) | Visual before/after |
| [CACHE_VS_DISK_GUIDE.md](computer:///mnt/user-data/outputs/CACHE_VS_DISK_GUIDE.md) | Caching strategies |
| [OPTIMIZATION_SUMMARY.md](computer:///mnt/user-data/outputs/OPTIMIZATION_SUMMARY.md) | Performance improvements |

---

## ✅ Pre-Flight Checklist

Before running your comparison:

- [ ] Config file has correct paths
- [ ] Key columns exist in both datasets
- [ ] S3 paths have proper permissions
- [ ] EMR cluster is running (if not local)
- [ ] Date ranges are valid (if filtering)
- [ ] Output path has write access

---

## 🎯 Summary

**Old Way (Hardcoded):**
```python
DS1_PATH = "s3://..."  # Edit code every time
DS2_PATH = "s3://..."
KEY_COLUMNS = ["id"]
spark-submit script.py
```

**New Way (YAML):**
```yaml
# my_comparison.yaml
datasets:
  ds1_path: "s3://..."
  ds2_path: "s3://..."
comparison:
  key_columns: ["id"]
```
```bash
./run_comparison.sh --config my_comparison.yaml --cluster j-XXXXX
```

**Benefits:**
- ✅ No code changes
- ✅ Version control configs
- ✅ Reusable across teams
- ✅ Environment-specific settings
- ✅ Self-documenting

---

## 🚀 Next Steps

1. **Copy template:**
   ```bash
   cp compare_config.yaml my_first_comparison.yaml
   ```

2. **Edit paths:**
   ```bash
   vim my_first_comparison.yaml
   ```

3. **Test with sample:**
   ```yaml
   sampling:
     enabled: true
     fraction: 0.01  # 1% sample
   ```

4. **Run:**
   ```bash
   ./run_comparison.sh --config my_first_comparison.yaml --cluster j-XXXXX
   ```

5. **Check results:**
   ```bash
   aws s3 ls s3://your-bucket/output/
   ```

6. **Full comparison:**
   ```yaml
   sampling:
     enabled: false  # Disable sampling
   ```

**That's it!** You're now comparing datasets like a pro! 🎉

---

## 💡 Pro Tips

1. **Start small:** Always test with 1-10% sample first
2. **Version configs:** Git is your friend
3. **Use examples:** Copy and modify the example configs
4. **Monitor:** Watch Spark UI for bottlenecks
5. **Iterate:** Test → Tune → Scale
