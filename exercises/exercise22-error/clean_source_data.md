```markdown
i have spark where in the middle of transformation  it is failing with Task Write FAiled for parquet writes to s3
* it is not s3 permission issue as i test wrte small dataset to same s3 buclet
* i scaled up the bode it did no thelp
* i coalesed dataframe to 50 partitons it do dnot help
* before writing the parquet also fixed all the null value - df.na.fill(0).na.fill("").na.fill(False).write.parquet("s3://..."), even this did not help
coudl it be a data issue
help me check  each source dataframe for data issues which can cause this failure a
indetify the rows in each dataframe write it out and clean this dataframe and fix these issues
```

```python
from pyspark.sql.functions import (
    col, length, isnan, isinf, sum as _sum, when, lit, 
    regexp_replace, substring, size, concat_ws, sha2, row_number
)
from pyspark.sql.types import StringType, DoubleType, FloatType, ArrayType, MapType, StructType
from pyspark.sql.window import Window

# ============================================================================
# DATA QUALITY SCANNER - Finds problematic rows
# ============================================================================

def scan_dataframe_for_issues(df, df_name, output_path):
    """
    Comprehensive scan to find rows that cause parquet write failures
    Returns: (clean_df, bad_rows_df, issue_summary)
    """
    
    print(f"\n{'='*70}")
    print(f"SCANNING: {df_name}")
    print(f"{'='*70}")
    
    total_rows = df.count()
    print(f"Total rows: {total_rows:,}")
    
    # Add row identifier for tracking
    df_with_id = df.withColumn("_row_id", row_number().over(Window.orderBy(lit(1))))
    
    bad_row_conditions = []
    issue_counts = {}
    
    # ========================================================================
    # CHECK 1: Extremely Long Strings (>10MB)
    # ========================================================================
    print(f"\n[1/8] Checking for extremely long strings...")
    
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    
    for col_name in string_cols:
        # Check for strings > 10MB (10,000,000 chars)
        very_long = df_with_id.filter(length(col(col_name)) > 10000000).count()
        
        if very_long > 0:
            issue_counts[f"{col_name}_very_long_strings"] = very_long
            print(f"  🔴 {col_name}: {very_long:,} rows with strings >10MB")
            bad_row_conditions.append(length(col(col_name)) > 10000000)
        
        # Check for strings > 1MB (potential issue)
        long = df_with_id.filter(length(col(col_name)) > 1000000).count()
        
        if long > 0:
            print(f"  ⚠️  {col_name}: {long:,} rows with strings >1MB")
    
    # ========================================================================
    # CHECK 2: Null Bytes in Strings
    # ========================================================================
    print(f"\n[2/8] Checking for null bytes...")
    
    for col_name in string_cols:
        null_bytes = df_with_id.filter(col(col_name).contains("\x00")).count()
        
        if null_bytes > 0:
            issue_counts[f"{col_name}_null_bytes"] = null_bytes
            print(f"  🔴 {col_name}: {null_bytes:,} rows with null bytes")
            bad_row_conditions.append(col(col_name).contains("\x00"))
    
    # ========================================================================
    # CHECK 3: Control Characters
    # ========================================================================
    print(f"\n[3/8] Checking for control characters...")
    
    for col_name in string_cols:
        control_chars = df_with_id.filter(col(col_name).rlike("[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")).count()
        
        if control_chars > 0:
            issue_counts[f"{col_name}_control_chars"] = control_chars
            print(f"  🔴 {col_name}: {control_chars:,} rows with control characters")
            bad_row_conditions.append(col(col_name).rlike("[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"))
    
    # ========================================================================
    # CHECK 4: NaN and Infinity in Numeric Columns
    # ========================================================================
    print(f"\n[4/8] Checking for NaN/Infinity...")
    
    numeric_cols = [f.name for f in df.schema.fields 
                    if isinstance(f.dataType, (DoubleType, FloatType))]
    
    for col_name in numeric_cols:
        nan_count = df_with_id.filter(isnan(col(col_name))).count()
        inf_count = df_with_id.filter(isinf(col(col_name))).count()
        
        if nan_count > 0:
            issue_counts[f"{col_name}_nan"] = nan_count
            print(f"  🔴 {col_name}: {nan_count:,} NaN values")
            bad_row_conditions.append(isnan(col(col_name)))
        
        if inf_count > 0:
            issue_counts[f"{col_name}_inf"] = inf_count
            print(f"  🔴 {col_name}: {inf_count:,} Infinity values")
            bad_row_conditions.append(isinf(col(col_name)))
    
    # ========================================================================
    # CHECK 5: Large Arrays/Maps
    # ========================================================================
    print(f"\n[5/8] Checking for large arrays/maps...")
    
    complex_cols = [f.name for f in df.schema.fields 
                    if isinstance(f.dataType, (ArrayType, MapType))]
    
    for col_name in complex_cols:
        try:
            large_collections = df_with_id.filter(size(col(col_name)) > 10000).count()
            
            if large_collections > 0:
                issue_counts[f"{col_name}_large_collection"] = large_collections
                print(f"  🔴 {col_name}: {large_collections:,} rows with >10k elements")
                bad_row_conditions.append(size(col(col_name)) > 10000)
        except:
            pass
    
    # ========================================================================
    # CHECK 6: Invalid UTF-8 (Replacement Character)
    # ========================================================================
    print(f"\n[6/8] Checking for invalid UTF-8...")
    
    for col_name in string_cols:
        invalid_utf8 = df_with_id.filter(col(col_name).contains("\uFFFD")).count()
        
        if invalid_utf8 > 0:
            issue_counts[f"{col_name}_invalid_utf8"] = invalid_utf8
            print(f"  🔴 {col_name}: {invalid_utf8:,} rows with invalid UTF-8")
            bad_row_conditions.append(col(col_name).contains("\uFFFD"))
    
    # ========================================================================
    # CHECK 7: All-Null Rows
    # ========================================================================
    print(f"\n[7/8] Checking for all-null rows...")
    
    non_id_cols = [c for c in df.columns if c != "_row_id"]
    all_null_condition = " AND ".join([f"`{c}` IS NULL" for c in non_id_cols[:50]])
    all_null_count = df_with_id.filter(all_null_condition).count()
    
    if all_null_count > 0:
        issue_counts["all_null_rows"] = all_null_count
        print(f"  🔴 Found {all_null_count:,} all-null rows")
        bad_row_conditions.append(all_null_condition)
    
    # ========================================================================
    # CHECK 8: Test Write Sample
    # ========================================================================
    print(f"\n[8/8] Testing write on sample...")
    
    sample_size = min(10000, total_rows)
    test_sample = df.limit(sample_size)
    
    try:
        test_path = f"{output_path}/{df_name}_write_test/"
        test_sample.coalesce(5).write.mode("overwrite").parquet(test_path)
        print(f"  ✓ Sample write successful ({sample_size:,} rows)")
    except Exception as e:
        print(f"  🔴 Sample write FAILED: {str(e)[:100]}")
        print(f"  → Even sample has bad data - need row-by-row analysis")
    
    # ========================================================================
    # EXTRACT BAD ROWS
    # ========================================================================
    print(f"\n{'='*70}")
    print(f"EXTRACTING BAD ROWS")
    print(f"{'='*70}")
    
    if bad_row_conditions:
        # Combine all conditions with OR
        combined_condition = bad_row_conditions[0]
        for condition in bad_row_conditions[1:]:
            combined_condition = combined_condition | condition
        
        bad_rows_df = df_with_id.filter(combined_condition)
        bad_count = bad_rows_df.count()
        
        print(f"Total bad rows: {bad_count:,} ({bad_count/total_rows*100:.2f}%)")
        
        if bad_count > 0:
            # Write bad rows for inspection
            bad_rows_path = f"{output_path}/{df_name}_bad_rows/"
            print(f"Writing bad rows to: {bad_rows_path}")
            
            try:
                # Convert to string to avoid write failures
                bad_rows_string = bad_rows_df.select(
                    [col(c).cast("string").alias(c) for c in bad_rows_df.columns]
                )
                bad_rows_string.coalesce(10).write.mode("overwrite").parquet(bad_rows_path)
                print(f"  ✓ Bad rows written successfully")
            except Exception as e:
                print(f"  ⚠️  Could not write bad rows: {e}")
                print(f"  Showing sample instead:")
                bad_rows_df.show(10, truncate=50)
        
        # Get clean rows
        clean_df = df_with_id.filter(~combined_condition).drop("_row_id")
        clean_count = clean_df.count()
        
        print(f"Clean rows remaining: {clean_count:,} ({clean_count/total_rows*100:.2f}%)")
        
    else:
        print(f"✓ No obvious data quality issues found")
        bad_rows_df = None
        clean_df = df
        bad_count = 0
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print(f"\n{'='*70}")
    print(f"SUMMARY: {df_name}")
    print(f"{'='*70}")
    print(f"Total rows: {total_rows:,}")
    print(f"Bad rows: {bad_count:,}")
    print(f"Clean rows: {total_rows - bad_count:,}")
    print(f"\nIssues found:")
    
    if issue_counts:
        for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"  - {issue}: {count:,} rows")
    else:
        print(f"  None")
    
    return clean_df, bad_rows_df, issue_counts


# ============================================================================
# ADVANCED CLEANER - More aggressive cleaning
# ============================================================================

def clean_dataframe_aggressively(df, df_name):
    """
    Apply aggressive cleaning transformations
    """
    
    print(f"\n🧹 CLEANING: {df_name}")
    print(f"{'='*70}")
    
    df_clean = df
    original_count = df.count()
    
    # Get column types
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [f.name for f in df.schema.fields 
                    if isinstance(f.dataType, (DoubleType, FloatType))]
    complex_cols = [f.name for f in df.schema.fields 
                    if isinstance(f.dataType, (ArrayType, MapType))]
    
    # Clean strings
    print(f"[1/5] Cleaning {len(string_cols)} string columns...")
    for col_name in string_cols:
        # Remove null bytes
        df_clean = df_clean.withColumn(
            col_name,
            regexp_replace(col(col_name), "[\x00]", "")
        )
        
        # Remove control characters (except tab, newline, carriage return)
        df_clean = df_clean.withColumn(
            col_name,
            regexp_replace(col(col_name), "[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "")
        )
        
        # Remove invalid UTF-8
        df_clean = df_clean.withColumn(
            col_name,
            regexp_replace(col(col_name), "[\uFFFD]", "")
        )
        
        # Truncate very long strings (10MB max)
        df_clean = df_clean.withColumn(
            col_name,
            when(length(col(col_name)) > 10000000, 
                 substring(col(col_name), 1, 10000000))
            .otherwise(col(col_name))
        )
    
    # Clean numeric columns
    print(f"[2/5] Cleaning {len(numeric_cols)} numeric columns...")
    for col_name in numeric_cols:
        # Replace NaN/Inf with null
        df_clean = df_clean.withColumn(
            col_name,
            when(isnan(col(col_name)) | isinf(col(col_name)), None)
            .otherwise(col(col_name))
        )
    
    # Handle complex types
    print(f"[3/5] Handling {len(complex_cols)} complex columns...")
    for col_name in complex_cols:
        # Set null if size > 10000
        try:
            df_clean = df_clean.withColumn(
                col_name,
                when(size(col(col_name)) > 10000, None)
                .otherwise(col(col_name))
            )
        except:
            pass
    
    # Remove all-null rows
    print(f"[4/5] Removing all-null rows...")
    df_clean = df_clean.na.drop(how="all")
    
    # Remove duplicate rows
    print(f"[5/5] Removing duplicates...")
    df_clean = df_clean.dropDuplicates()
    
    clean_count = df_clean.count()
    removed = original_count - clean_count
    
    print(f"\n✓ Cleaning complete")
    print(f"  Original: {original_count:,} rows")
    print(f"  Cleaned:  {clean_count:,} rows")
    print(f"  Removed:  {removed:,} rows ({removed/original_count*100:.2f}%)")
    
    return df_clean


# ============================================================================
# TEST WRITE FUNCTION
# ============================================================================

def test_write_cleaned_data(df, df_name, output_path):
    """
    Test if cleaned data can be written successfully
    """
    
    print(f"\n✍️  TESTING WRITE: {df_name}")
    print(f"{'='*70}")
    
    test_results = {}
    
    # Test 1: Small write (1k rows, 5 partitions)
    print(f"[1/4] Small write test (1k rows, 5 partitions)...", end=" ")
    try:
        df.limit(1000).coalesce(5).write.mode("overwrite") \
          .parquet(f"{output_path}/{df_name}_test_small/")
        print("✓")
        test_results["small"] = True
    except Exception as e:
        print(f"✗ {str(e)[:80]}")
        test_results["small"] = False
        return test_results
    
    # Test 2: Medium write (10k rows, 10 partitions)
    print(f"[2/4] Medium write test (10k rows, 10 partitions)...", end=" ")
    try:
        df.limit(10000).coalesce(10).write.mode("overwrite") \
          .parquet(f"{output_path}/{df_name}_test_medium/")
        print("✓")
        test_results["medium"] = True
    except Exception as e:
        print(f"✗ {str(e)[:80]}")
        test_results["medium"] = False
        return test_results
    
    # Test 3: Large write (100k rows, 20 partitions)
    print(f"[3/4] Large write test (100k rows, 20 partitions)...", end=" ")
    try:
        df.limit(100000).coalesce(20).write.mode("overwrite") \
          .parquet(f"{output_path}/{df_name}_test_large/")
        print("✓")
        test_results["large"] = True
    except Exception as e:
        print(f"✗ {str(e)[:80]}")
        test_results["large"] = False
        return test_results
    
    # Test 4: Full write (all rows, 50 partitions)
    print(f"[4/4] Full write test (all rows, 50 partitions)...", end=" ")
    try:
        df.coalesce(50).write.mode("overwrite") \
          .option('maxRecordsPerFile', 100000) \
          .parquet(f"{output_path}/{df_name}_test_full/")
        print("✓")
        test_results["full"] = True
    except Exception as e:
        print(f"✗ {str(e)[:80]}")
        test_results["full"] = False
    
    return test_results


# ============================================================================
# PROCESS ALL DATAFRAMES
# ============================================================================

def process_all_dataframes(dataframes_dict, output_base_path):
    """
    Process all source dataframes
    
    Args:
        dataframes_dict: {"df_name": dataframe, ...}
        output_base_path: S3 path for outputs
    
    Returns:
        Dictionary with cleaned dataframes and results
    """
    
    print("\n" + "="*70)
    print("PROCESSING ALL SOURCE DATAFRAMES")
    print("="*70)
    print(f"Total dataframes: {len(dataframes_dict)}")
    print(f"Output path: {output_base_path}")
    
    results = {}
    
    for df_name, df in dataframes_dict.items():
        
        try:
            # Step 1: Scan for issues
            clean_df, bad_rows_df, issues = scan_dataframe_for_issues(
                df, 
                df_name, 
                f"{output_base_path}/scans"
            )
            
            # Step 2: Apply aggressive cleaning
            if issues:
                print(f"\n🔧 Applying aggressive cleaning to {df_name}...")
                clean_df = clean_dataframe_aggressively(clean_df, df_name)
            
            # Step 3: Test write
            test_results = test_write_cleaned_data(
                clean_df,
                df_name,
                f"{output_base_path}/write_tests"
            )
            
            # Store results
            results[df_name] = {
                "original_df": df,
                "clean_df": clean_df,
                "bad_rows_df": bad_rows_df,
                "issues": issues,
                "test_results": test_results,
                "can_write": test_results.get("full", False)
            }
            
            print(f"\n{'='*70}")
            print(f"✓ {df_name} processing complete")
            print(f"{'='*70}\n")
            
        except Exception as e:
            print(f"\n🔴 CRITICAL ERROR processing {df_name}: {e}")
            results[df_name] = {
                "error": str(e),
                "can_write": False
            }
    
    # Generate final report
    generate_final_report(results, output_base_path)
    
    return results


# ============================================================================
# GENERATE REPORT
# ============================================================================

def generate_final_report(results, output_path):
    """
    Generate comprehensive report of all findings
    """
    
    print("\n\n" + "="*70)
    print("FINAL ANALYSIS REPORT")
    print("="*70)
    
    total_dfs = len(results)
    can_write = sum(1 for r in results.values() if r.get("can_write", False))
    cannot_write = total_dfs - can_write
    
    print(f"\nTotal dataframes analyzed: {total_dfs}")
    print(f"Can write successfully: {can_write}")
    print(f"Cannot write: {cannot_write}")
    
    # Show problematic dataframes
    if cannot_write > 0:
        print(f"\n🔴 PROBLEMATIC DATAFRAMES:")
        for df_name, result in results.items():
            if not result.get("can_write", False):
                print(f"\n  {df_name}:")
                if "error" in result:
                    print(f"    Error: {result['error']}")
                elif "issues" in result:
                    for issue, count in list(result["issues"].items())[:5]:
                        print(f"    - {issue}: {count:,} rows")
                if "test_results" in result:
                    print(f"    Test results: {result['test_results']}")
    
    # Show clean dataframes
    if can_write > 0:
        print(f"\n✓ CLEAN DATAFRAMES (ready to use):")
        for df_name, result in results.items():
            if result.get("can_write", False):
                print(f"  - {df_name}")
    
    # Recommendations
    print(f"\n{'='*70}")
    print(f"RECOMMENDATIONS:")
    print(f"{'='*70}")
    
    if cannot_write > 0:
        print(f"\n1. Use the cleaned dataframes from results[df_name]['clean_df']")
        print(f"2. Inspect bad rows at: {output_path}/scans/*/bad_rows/")
        print(f"3. Apply additional business logic to handle removed rows")
        print(f"4. Re-run your pipeline with cleaned dataframes")
    else:
        print(f"\n✓ All dataframes can write successfully after cleaning!")
        print(f"  Use: results[df_name]['clean_df'] in your pipeline")
    
    print(f"\n{'='*70}\n")


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

# Define your source dataframes
dataframes = {
    "customer_df": customer_df,
    "transactions_df": transactions_df,
    "features_df": features_df,
    "scores_df": scores_df,
    "segment_df": segment_df,
    # ... add all your 9-10 dataframes
}

# Process all dataframes
results = process_all_dataframes(
    dataframes,
    output_base_path="s3://your-bucket/data_quality_analysis"
)

# Use cleaned dataframes in your pipeline
cleaned_customer = results["customer_df"]["clean_df"]
cleaned_transactions = results["transactions_df"]["clean_df"]
# etc...

# Run your transformations with cleaned data
final_df = your_complex_transformation(
    cleaned_customer,
    cleaned_transactions,
    # ... other cleaned dfs
)

# Write final output
final_df.coalesce(50) \
       .write \
       .option('maxRecordsPerFile', 100000) \
       .mode('overwrite') \
       .parquet("s3://your-bucket/final_output/")
```

## Quick Start - Minimal Version

If you want a faster check:

```python
def quick_check_all_dataframes(dataframes_dict):
    """Quick check to find the problematic dataframe"""
    
    print("QUICK CHECK")
    print("="*70)
    
    for df_name, df in dataframes_dict.items():
        print(f"\n{df_name}:")
        
        # Test write
        try:
            df.limit(1000).coalesce(5).write.mode("overwrite") \
              .parquet(f"s3://bucket/quick_test/{df_name}/")
            print(f"  ✓ Can write")
        except Exception as e:
            print(f"  🔴 CANNOT WRITE: {str(e)[:100]}")
            print(f"  → This dataframe has the problem!")
            
            # Quick issue check
            string_cols = [f.name for f in df.schema.fields 
                          if isinstance(f.dataType, StringType)]
            
            for col_name in string_cols[:5]:
                max_len = df.select(length(col(col_name))).agg({f"length({col_name})": "max"}).collect()[0][0] or 0
                if max_len > 1000000:
                    print(f"    Issue: {col_name} has {max_len:,} char strings")

# Run quick check
quick_check_all_dataframes(dataframes)
```

This will systematically:
1. ✅ Find all problematic rows in each dataframe
2. ✅ Write bad rows to S3 for inspection
3. ✅ Create cleaned versions
4. ✅ Test if cleaned versions write successfully
5. ✅ Give you a clear report of which dataframe(s) cause the issue

Use the cleaned dataframes in your pipeline and the write should succeed!