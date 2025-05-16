The `sample_size` parameter is a key feature in the script that allows you to control how much data is processed during the comparison. Let me explain how it works and why it's useful:

### What `sample_size: 0.5` Does

When you set `sample_size` to `0.5` in the command:

```bash
spark-submit compare-s3-tos3-html-report-ver1.py --source_path s3://bucket/source/ --target_path s3://bucket/target/ --sample_size 0.5
```

It means the script will:

1. **Take a random 50% sample** of both the source and target datasets
2. **Perform the comparison on this sample** rather than the entire dataset
3. **Generate the report based on this sample**

### How It's Implemented

In the code, you can see this happening in the `load_and_prepare_data` function:

```python
def load_and_prepare_data(spark, path, key_columns, sample_size=1.0):
    """Load parquet data from S3 with optional sampling"""
    log_info(spark, f"Loading data from {path}")
    df = spark.read.parquet(path)
    
    # Apply sampling if specified
    if sample_size < 1.0:
        log_info(spark, f"Sampling {sample_size * 100}% of data")
        df = df.sample(fraction=sample_size, seed=42)
    
    # Rest of function...
```

The `sample()` method uses PySpark's random sampling to select approximately 50% of the rows. The `seed=42` ensures both datasets are sampled consistently.

### Benefits of Using Sampling

1. **Resource Efficiency**: With 3.5 million rows and 450 columns, processing the full dataset could strain your EMR cluster. Sampling allows you to:
   - Reduce memory consumption
   - Decrease processing time
   - Lower the risk of cluster failure

2. **Faster Iterations**: You can quickly get initial comparison results to:
   - Validate your comparison logic
   - Identify major discrepancies
   - Adjust parameters before running on the full dataset

3. **Cost Efficiency**: Sampling reduces:
   - Compute resources needed
   - EMR cluster runtime
   - Storage I/O costs

### When to Use Different Sample Sizes

- **0.1 (10%)**: For very quick initial checks or extremely large datasets
- **0.5 (50%)**: Good balance between accuracy and performance
- **0.8 (80%)**: Higher confidence while still saving resources
- **1.0 (100%)**: Full comparison with maximum accuracy

### Impact on Results

The HTML report will indicate that it's based on a sample, and all metrics (counts, percentages) will be approximations based on the sample. The statistical validity is generally good for detecting:

- Column value differences
- Schema discrepancies
- General patterns of mismatches

For absolute precision, you would eventually want to run with `sample_size=1.0` after confirming the script works properly with a smaller sample.

### Best Practice

A common workflow is:
1. Start with a small sample (0.1) to verify the script works
2. Use a medium sample (0.5) to get meaningful insights
3. Run the full comparison (1.0) for production/final reports

This step-by-step approach helps you avoid wasting resources on potentially problematic comparisons.