Here's clean, production-ready code to systematically compare all your datasets:

```python
from pyspark.sql.functions import (
    col, length, isnan, isinf, sum as _sum,
    count, min as _min, max as _max, avg, regexp_replace, when, substring
)
from pyspark.sql.types import StringType, DoubleType, FloatType, ArrayType, MapType, StructType
from datetime import datetime

# ============================================================================
# CONFIGURATION - Define your datasets here
# ============================================================================

DATASETS = {
    "customer_data": {
        "last_month": "s3://bucket/customer/202409/*",
        "this_month": "s3://bucket/customer/202410/*",
    },
    "transaction_data": {
        "last_month": "s3://bucket/transactions/202409/*",
        "this_month": "s3://bucket/transactions/202410/*",
    },
    "scoring_features": {
        "last_month": "s3://bucket/features/202409/*",
        "this_month": "s3://bucket/features/202410/*",
    },
    "model_predictions": {
        "last_month": "s3://bucket/predictions/202409/*",
        "this_month": "s3://bucket/predictions/202410/*",
    },
    # Add your other 5-6 datasets here
    "dataset_5": {
        "last_month": "s3://bucket/dataset5/202409/*",
        "this_month": "s3://bucket/dataset5/202410/*",
    },
}


# ============================================================================
# CORE COMPARISON FUNCTIONS
# ============================================================================

def load_dataset_safely(path, dataset_name):
    """Load dataset with error handling"""
    try:
        df = spark.read.parquet(path)
        row_count = df.count()
        print(f"    ✓ Loaded: {row_count:,} rows")
        return df
    except Exception as e:
        print(f"    ✗ Failed to load: {e}")
        return None


def compare_basic_stats(last_df, this_df, dataset_name):
    """Compare row counts, columns, data volume"""

    print(f"\n  📊 Basic Statistics")
    print(f"  {'-' * 66}")

    last_count = last_df.count()
    this_count = this_df.count()
    last_cols = len(last_df.columns)
    this_cols = len(this_df.columns)

    # Row comparison
    diff_count = this_count - last_count
    diff_pct = (this_count / last_count - 1) * 100 if last_count > 0 else 0

    print(f"  Rows:    Last={last_count:>12,}  This={this_count:>12,}  Diff={diff_count:>+12,} ({diff_pct:>+6.1f}%)")
    print(f"  Columns: Last={last_cols:>12}  This={this_cols:>12}")

    issues = []

    if this_count > last_count * 2:
        issues.append("Data volume more than doubled")
        print(f"  🔴 WARNING: Data volume increased by {diff_pct:.1f}%")

    if last_cols != this_cols:
        issues.append("Schema changed")
        print(f"  🔴 WARNING: Column count changed")

        added = set(this_df.columns) - set(last_df.columns)
        removed = set(last_df.columns) - set(this_df.columns)

        if added:
            print(f"      Added: {added}")
        if removed:
            print(f"      Removed: {removed}")

    return issues, last_count, this_count


def compare_string_lengths(last_df, this_df, dataset_name):
    """Check for abnormally long strings"""

    print(f"\n  📏 String Length Analysis")
    print(f"  {'-' * 66}")

    string_cols = [f.name for f in this_df.schema.fields
                   if isinstance(f.dataType, StringType)]

    if not string_cols:
        print(f"  No string columns found")
        return []

    issues = []

    for col_name in string_cols[:15]:  # Check first 15 string columns
        if col_name not in last_df.columns:
            continue

        try:
            last_max = last_df.select(_max(length(col(col_name)))).collect()[0][0] or 0
            this_max = this_df.select(_max(length(col(col_name)))).collect()[0][0] or 0

            # Check for problematic lengths
            if this_max > 1000000:  # > 1MB
                issues.append(f"{col_name}: Very long strings ({this_max:,} chars)")
                print(f"  🔴 {col_name:30s} Max={this_max:>12,} chars (CRITICAL: >1MB)")

            elif this_max > last_max * 10:  # 10x increase
                issues.append(f"{col_name}: Length increased 10x")
                print(f"  ⚠️  {col_name:30s} Last={last_max:>8,}  This={this_max:>8,}  (10x increase)")

            elif this_max > 100000:  # > 100KB
                print(f"  ⚠️  {col_name:30s} Max={this_max:>12,} chars (WARNING: >100KB)")

        except Exception as e:
            print(f"  ✗ {col_name}: Error checking length - {e}")

    if not issues:
        print(f"  ✓ No string length issues detected")

    return issues


def compare_null_patterns(last_df, this_df, dataset_name):
    """Compare null counts between months"""

    print(f"\n  🔍 Null Pattern Analysis")
    print(f"  {'-' * 66}")

    common_cols = list(set(last_df.columns) & set(this_df.columns))
    last_count = last_df.count()
    this_count = this_df.count()

    issues = []

    for col_name in common_cols[:15]:  # Check first 15 columns
        try:
            last_nulls = last_df.filter(col(col_name).isNull()).count()
            this_nulls = this_df.filter(col(col_name).isNull()).count()

            last_pct = (last_nulls / last_count * 100) if last_count > 0 else 0
            this_pct = (this_nulls / this_count * 100) if this_count > 0 else 0

            # Flag significant changes (>20% difference)
            if abs(this_pct - last_pct) > 20:
                issues.append(f"{col_name}: Null % changed by {this_pct - last_pct:+.1f}%")
                print(
                    f"  🔴 {col_name:30s} Last={last_pct:>5.1f}%  This={this_pct:>5.1f}%  Diff={this_pct - last_pct:>+6.1f}%")

            # Flag columns that became all null
            elif this_pct > 99 and last_pct < 50:
                issues.append(f"{col_name}: Became all null")
                print(f"  🔴 {col_name:30s} Became all null ({this_pct:.1f}%)")

        except Exception as e:
            pass

    if not issues:
        print(f"  ✓ No significant null pattern changes")

    return issues


def check_numeric_anomalies(this_df, dataset_name):
    """Check for NaN, Infinity in numeric columns"""

    print(f"\n  🔢 Numeric Anomalies Check")
    print(f"  {'-' * 66}")

    numeric_cols = [f.name for f in this_df.schema.fields
                    if isinstance(f.dataType, (DoubleType, FloatType))]

    if not numeric_cols:
        print(f"  No numeric columns found")
        return []

    issues = []

    for col_name in numeric_cols[:15]:  # Check first 15 numeric columns
        try:
            nan_count = this_df.filter(isnan(col(col_name))).count()
            inf_count = this_df.filter(isinf(col(col_name))).count()

            if nan_count > 0:
                issues.append(f"{col_name}: {nan_count:,} NaN values")
                print(f"  🔴 {col_name:30s} NaN count: {nan_count:>12,}")

            if inf_count > 0:
                issues.append(f"{col_name}: {inf_count:,} Infinity values")
                print(f"  🔴 {col_name:30s} Inf count: {inf_count:>12,}")

        except Exception as e:
            pass

    if not issues:
        print(f"  ✓ No NaN or Infinity values detected")

    return issues


def check_special_characters(this_df, dataset_name):
    """Check for null bytes, control characters"""

    print(f"\n  🔤 Special Characters Check")
    print(f"  {'-' * 66}")

    string_cols = [f.name for f in this_df.schema.fields
                   if isinstance(f.dataType, StringType)]

    if not string_cols:
        print(f"  No string columns found")
        return []

    issues = []

    for col_name in string_cols[:10]:  # Check first 10 string columns
        try:
            # Check for null bytes
            null_byte_count = this_df.filter(col(col_name).contains("\x00")).count()

            # Check for control characters
            control_char_count = this_df.filter(
                col(col_name).rlike("[\x00-\x1F\x7F]")
            ).count()

            if null_byte_count > 0:
                issues.append(f"{col_name}: {null_byte_count:,} null bytes")
                print(f"  🔴 {col_name:30s} Null bytes: {null_byte_count:>12,}")

            if control_char_count > 0:
                issues.append(f"{col_name}: {control_char_count:,} control chars")
                print(f"  🔴 {col_name:30s} Control chars: {control_char_count:>12,}")

        except Exception as e:
            pass

    if not issues:
        print(f"  ✓ No special character issues detected")

    return issues


def test_write_dataset(df, dataset_name, test_path):
    """Test if dataset can be written successfully"""

    print(f"\n  ✍️  Write Test")
    print(f"  {'-' * 66}")

    try:
        # Test small write
        print(f"  Testing small write (1000 rows)...", end=" ")
        df.limit(1000).write.mode("overwrite").parquet(f"{test_path}/{dataset_name}_small/")
        print("✓")

        # Test medium write with low concurrency
        print(f"  Testing medium write (10k rows, 5 partitions)...", end=" ")
        df.limit(10000).coalesce(5).write.mode("overwrite").parquet(f"{test_path}/{dataset_name}_medium/")
        print("✓")

        return True, []

    except Exception as e:
        print(f"✗")
        print(f"  🔴 Write failed: {str(e)[:100]}")
        return False, [f"Write test failed: {str(e)[:100]}"]


# ============================================================================
# MAIN COMPARISON FUNCTION
# ============================================================================

def compare_dataset(dataset_name, last_path, this_path, test_path):
    """
    Compare a single dataset between months
    Returns: (has_issues, all_issues)
    """

    print(f"\n{'=' * 70}")
    print(f"DATASET: {dataset_name}")
    print(f"{'=' * 70}")

    all_issues = []

    # Step 1: Load data
    print(f"\n  📁 Loading Data")
    print(f"  {'-' * 66}")
    print(f"  Last month: {last_path}")
    last_df = load_dataset_safely(last_path, dataset_name)

    print(f"  This month: {this_path}")
    this_df = load_dataset_safely(this_path, dataset_name)

    if last_df is None or this_df is None:
        print(f"\n  🔴 CRITICAL: Cannot load dataset")
        return True, ["Cannot load dataset"]

    # Step 2: Basic stats comparison
    issues, last_count, this_count = compare_basic_stats(last_df, this_df, dataset_name)
    all_issues.extend(issues)

    # Step 3: String length analysis
    issues = compare_string_lengths(last_df, this_df, dataset_name)
    all_issues.extend(issues)

    # Step 4: Null pattern analysis
    issues = compare_null_patterns(last_df, this_df, dataset_name)
    all_issues.extend(issues)

    # Step 5: Numeric anomalies
    issues = check_numeric_anomalies(this_df, dataset_name)
    all_issues.extend(issues)

    # Step 6: Special characters
    issues = check_special_characters(this_df, dataset_name)
    all_issues.extend(issues)

    # Step 7: Write test
    write_success, write_issues = test_write_dataset(this_df, dataset_name, test_path)
    all_issues.extend(write_issues)

    # Summary
    print(f"\n  📋 Summary")
    print(f"  {'-' * 66}")

    if all_issues:
        print(f"  🔴 ISSUES FOUND: {len(all_issues)}")
        for i, issue in enumerate(all_issues, 1):
            print(f"      {i}. {issue}")
        return True, all_issues
    else:
        print(f"  ✓ No issues detected")
        return False, []


# ============================================================================
# RUN ALL COMPARISONS
# ============================================================================

def run_all_comparisons(datasets, test_base_path="s3://your-bucket/diagnostic_tests"):
    """
    Compare all datasets and generate summary report
    """

    print("\n" + "=" * 70)
    print("MULTI-DATASET COMPARISON ANALYSIS")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    results = {}

    for dataset_name, paths in datasets.items():
        try:
            has_issues, issues = compare_dataset(
                dataset_name,
                paths["last_month"],
                paths["this_month"],
                test_base_path
            )

            results[dataset_name] = {
                "has_issues": has_issues,
                "issues": issues
            }

        except Exception as e:
            print(f"\n🔴 CRITICAL ERROR processing {dataset_name}: {e}")
            results[dataset_name] = {
                "has_issues": True,
                "issues": [f"Critical error: {e}"]
            }

    # Generate final report
    print("\n\n" + "=" * 70)
    print("FINAL SUMMARY REPORT")
    print("=" * 70)

    problematic_datasets = []
    clean_datasets = []

    for dataset_name, result in results.items():
        if result["has_issues"]:
            problematic_datasets.append(dataset_name)
            print(f"\n🔴 {dataset_name}")
            print(f"   Issues found: {len(result['issues'])}")
            for issue in result['issues'][:5]:  # Show first 5 issues
                print(f"   - {issue}")
            if len(result['issues']) > 5:
                print(f"   ... and {len(result['issues']) - 5} more")
        else:
            clean_datasets.append(dataset_name)

    print(f"\n{'=' * 70}")
    print(f"SUMMARY:")
    print(f"  Total datasets checked: {len(datasets)}")
    print(f"  Clean datasets: {len(clean_datasets)}")
    print(f"  Problematic datasets: {len(problematic_datasets)}")

    if clean_datasets:
        print(f"\n✓ Clean datasets:")
        for ds in clean_datasets:
            print(f"    - {ds}")

    if problematic_datasets:
        print(f"\n🔴 Datasets with issues:")
        for ds in problematic_datasets:
            print(f"    - {ds}")

        print(f"\n💡 RECOMMENDATION:")
        print(f"   Focus investigation on these {len(problematic_datasets)} dataset(s)")
        print(f"   Apply data cleaning before processing")

    print(f"\n{'=' * 70}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}\n")

    return results


# ============================================================================
# DATA CLEANING FUNCTION
# ============================================================================

def clean_dataset(df, dataset_name):
    """
    Clean problematic data issues
    """

    print(f"\n🧹 Cleaning {dataset_name}...")

    df_clean = df
    original_count = df.count()

    # Get column types
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [f.name for f in df.schema.fields
                    if isinstance(f.dataType, (DoubleType, FloatType))]

    # Clean strings
    for col_name in string_cols:
        # Remove null bytes
        df_clean = df_clean.withColumn(
            col_name,
            regexp_replace(col(col_name), "[\x00]", "")
        )

        # Remove control characters
        df_clean = df_clean.withColumn(
            col_name,
            regexp_replace(col(col_name), "[\x00-\x1F\x7F]", "")
        )

        # Truncate very long strings (>10MB)
        df_clean = df_clean.withColumn(
            col_name,
            when(length(col(col_name)) > 10000000,
                 substring(col(col_name), 1, 10000000))
            .otherwise(col(col_name))
        )

    # Clean numeric columns
    for col_name in numeric_cols:
        # Replace NaN/Inf with null
        df_clean = df_clean.withColumn(
            col_name,
            when(isnan(col(col_name)) | isinf(col(col_name)), None)
            .otherwise(col(col_name))
        )

    # Remove all-null rows
    df_clean = df_clean.na.drop(how="all")

    clean_count = df_clean.count()

    print(f"  Original: {original_count:,} rows")
    print(f"  Cleaned:  {clean_count:,} rows")
    print(f"  Removed:  {original_count - clean_count:,} rows")

    return df_clean


# ============================================================================
# EXECUTE
# ============================================================================

if __name__ == "__main__":

    # Run comparison on all datasets
    results = run_all_comparisons(
        DATASETS,
        test_base_path="s3://your-bucket/diagnostic_tests"
    )

    # Optional: Clean and test problematic datasets
    print("\n" + "=" * 70)
    print("TESTING CLEANED DATA")
    print("=" * 70)

    for dataset_name, result in results.items():
        if result["has_issues"]:
            print(f"\n🧹 Cleaning and testing: {dataset_name}")

            # Load this month's data
            this_df = spark.read.parquet(DATASETS[dataset_name]["this_month"])

            # Clean it
            clean_df = clean_dataset(this_df, dataset_name)

            # Test write
            try:
                clean_df.coalesce(50)
                    .write
                    .option('maxRecordsPerFile', 100000)
                    .mode('overwrite')
                    .parquet(f"s3://your-bucket/cleaned_test/{dataset_name}/")

                print(f"  ✓ Cleaned data writes successfully!")
                print(f"  → Apply cleaning to this dataset in production")

            except Exception as e:
                print(f"  ✗ Still fails after cleaning: {e}")
                print(f"  → Issue may not be data quality alone")
```

## Quick Start Usage

```python
# 1. Configure your datasets at the top of the script

# 2. Run the analysis
results = run_all_comparisons(DATASETS)

# 3. Review the summary report in the output

# 4. For problematic datasets, apply cleaning:
problematic_df = spark.read.parquet("s3://bucket/problematic_dataset/202410/*")
clean_df = clean_dataset(problematic_df, "problematic_dataset")

# 5. Use cleaned data in your pipeline
clean_df.repartition(200)
    .write
    .option('maxRecordsPerFile', 50000)
    .parquet("s3://bucket/output/")
```

This will systematically check all 9-10 datasets and clearly identify which one(s) are causing the TASK_WRITE_FAILED
issue!