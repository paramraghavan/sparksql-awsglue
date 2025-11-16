"""
OPTIMIZED Dataset Comparison - Using df.count() Efficiently
Key insight: Avoid expensive operations BEFORE count, not avoid count itself
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
from datetime import datetime
import yaml
import argparse
import sys
import os

class OptimizedDatasetComparator:
    """
    Optimized comparison that uses df.count() correctly
    The key is avoiding expensive operations (like full outer joins) before counting
    """
    
    def __init__(self, spark, ds1_path, ds2_path, key_columns, skip_columns=None, 
                 output_path="/tmp/comparison_output", config=None):
        self.spark = spark
        self.ds1_path = ds1_path
        self.ds2_path = ds2_path
        self.key_columns = key_columns if isinstance(key_columns, list) else [key_columns]
        self.skip_columns = skip_columns or []
        self.output_path = output_path
        self.config = config or {}
        self.stats = {}
        self.ds1 = None  # Can be set externally
        self.ds2 = None  # Can be set externally
    
    def run_comparison(self):
        """Main comparison workflow"""
        print("=" * 80)
        print("OPTIMIZED DATASET COMPARISON")
        print("=" * 80)
        
        # Load datasets
        print("\n[1/7] Loading datasets...")
        ds1, ds2 = self._load_datasets()
        
        # Count using df.count() - this is fine on original datasets
        print("\n[2/7] Counting rows...")
        self._count_rows_optimized(ds1, ds2)
        
        # Find unique rows efficiently
        print("\n[3/7] Finding rows only in DS1...")
        self._find_unique_rows_optimized(ds1, ds2, "ds1_only")
        
        print("\n[4/7] Finding rows only in DS2...")
        self._find_unique_rows_optimized(ds2, ds1, "ds2_only")
        
        # Count common rows
        print("\n[5/7] Counting common rows...")
        self._count_common_rows_optimized(ds1, ds2)
        
        # Find differences
        print("\n[6/7] Finding differences...")
        diff_df = self._find_differences_optimized(ds1, ds2)
        
        # Generate report
        print("\n[7/7] Generating HTML report...")
        self._generate_html_report(diff_df)
        
        print("\n" + "=" * 80)
        print("COMPARISON COMPLETE!")
        print("=" * 80)
        self._print_summary()
        
        return diff_df
    
    def _load_datasets(self):
        """Load datasets (or use pre-loaded ones)"""
        # Use pre-loaded datasets if available (set externally)
        if self.ds1 is not None and self.ds2 is not None:
            ds1 = self.ds1
            ds2 = self.ds2
        else:
            # Load from paths
            ds1 = self.spark.read.parquet(self.ds1_path)
            ds2 = self.spark.read.parquet(self.ds2_path)
        
        # Validate key columns exist
        for col in self.key_columns:
            if col not in ds1.columns:
                raise ValueError(f"Key column '{col}' not found in DS1")
            if col not in ds2.columns:
                raise ValueError(f"Key column '{col}' not found in DS2")
        
        print(f"DS1 columns: {len(ds1.columns)}")
        print(f"DS2 columns: {len(ds2.columns)}")
        
        return ds1, ds2
    
    def _count_rows_optimized(self, ds1, ds2):
        """
        Count rows efficiently using df.count()
        This is safe because we're counting the original datasets, not joined ones
        """
        print("  Counting DS1 rows...")
        # df.count() is optimized and fine here - no expensive join yet
        count1 = ds1.count()
        self.stats['ds1_total'] = count1
        print(f"  DS1 Total Rows: {count1:,}")
        
        print("  Counting DS2 rows...")
        count2 = ds2.count()
        self.stats['ds2_total'] = count2
        print(f"  DS2 Total Rows: {count2:,}")
    
    def _find_unique_rows_optimized(self, left_df, right_df, result_key):
        """
        Find unique rows using left anti join
        KEY OPTIMIZATION: We count DISTINCT keys only, not full rows
        """
        # Select only key columns and get distinct - much smaller dataset
        left_keys = left_df.select(self.key_columns).distinct()
        right_keys = right_df.select(self.key_columns).distinct()
        
        # Left anti join on keys
        unique_keys = left_keys.join(right_keys, self.key_columns, "left_anti")
        
        # Cache for immediate use (faster than disk I/O)
        unique_keys.cache()
        
        # NOW we can safely count - we're counting distinct keys, not full rows
        # This DataFrame is small, so df.count() is perfectly fine
        count = unique_keys.count()
        self.stats[result_key] = count
        print(f"  {result_key}: {count:,} rows")
        
        # Optionally save to disk if you want to analyze these rows later
        # Uncomment for persistence:
        # unique_keys.write.mode("overwrite").parquet(f"{self.output_path}/{result_key}")
        
        # Unpersist after use to free memory
        unique_keys.unpersist()
        
        return unique_keys
    
    def _count_common_rows_optimized(self, ds1, ds2):
        """
        Count common rows using inner join on distinct keys only
        OPTIMIZATION: Join on keys only, not full rows
        """
        # Get distinct keys from each dataset
        ds1_keys = ds1.select(self.key_columns).distinct()
        ds2_keys = ds2.select(self.key_columns).distinct()
        
        # Inner join to find common keys
        common_keys = ds1_keys.join(ds2_keys, self.key_columns, "inner")
        
        # Cache for reuse (faster than writing to disk)
        # Use MEMORY_AND_DISK in case it's too large for memory
        common_keys.persist()  # Default: MEMORY_AND_DISK
        
        # Count the distinct keys - this is safe with df.count()
        # This also materializes the cache
        count = common_keys.count()
        self.stats['common_rows'] = count
        print(f"  Common rows: {count:,}")
        
        # Optionally save to disk for later analysis (commented out for speed)
        # Uncomment if you need to examine common keys later:
        # common_keys.write.mode("overwrite").parquet(f"{self.output_path}/common_keys")
        
        return common_keys  # Return for potential reuse
    
    def _find_differences_optimized(self, ds1, ds2):
        """
        Find differences efficiently without expensive full outer joins
        CRITICAL: We use INNER join (not full outer) and UNPIVOT once (not filter per column)
        """
        # Get columns to compare
        all_cols = set(ds1.columns).intersection(set(ds2.columns))
        compare_cols = [col for col in all_cols 
                       if col not in self.key_columns 
                       and col not in self.skip_columns]
        
        print(f"  Comparing {len(compare_cols)} columns")
        
        # Select only needed columns FIRST, then rename (more efficient)
        ds1_subset = ds1.select(self.key_columns + compare_cols)
        ds2_subset = ds2.select(self.key_columns + compare_cols)
        
        # Rename ds2 columns to avoid conflicts
        for col in compare_cols:
            ds2_subset = ds2_subset.withColumnRenamed(col, f"{col}_ds2")
        
        # INNER join on common keys only (not full outer!)
        # No need for select() again - we already selected what we need
        joined = ds1_subset.join(ds2_subset, self.key_columns, "inner")
        
        # Build difference conditions for counting
        diff_conditions = []
        for col in compare_cols:
            diff_cond = (
                (F.col(col).isNull() & F.col(f"{col}_ds2").isNotNull()) |
                (F.col(col).isNotNull() & F.col(f"{col}_ds2").isNull()) |
                ((F.col(col) != F.col(f"{col}_ds2")) & 
                 F.col(col).isNotNull() & 
                 F.col(f"{col}_ds2").isNotNull())
            )
            diff_conditions.append(diff_cond)
        
        # Filter to rows with any differences (for counting)
        if diff_conditions:
            any_diff = diff_conditions[0]
            for cond in diff_conditions[1:]:
                any_diff = any_diff | cond
            
            differences = joined.filter(any_diff)
            
            # Count rows with differences
            try:
                differences.cache()  # Cache since we'll use it twice
                rows_with_diffs = differences.select(self.key_columns).distinct().count()
                self.stats['rows_with_differences'] = rows_with_diffs
                print(f"  Rows with differences: {rows_with_diffs:,}")
            except Exception as e:
                print(f"  Warning: Could not count distinct difference rows: {e}")
                self.stats['rows_with_differences'] = "N/A (too large)"
            
            # OPTIMIZED: Unpivot differences ONCE instead of filtering per column
            print("  Unpivoting differences (single pass)...")
            
            # Create array of structs for all column comparisons
            column_comparisons = []
            for col in compare_cols:
                # Only include if this specific column differs
                column_comparisons.append(
                    F.when(
                        (F.col(col).isNull() & F.col(f"{col}_ds2").isNotNull()) |
                        (F.col(col).isNotNull() & F.col(f"{col}_ds2").isNull()) |
                        ((F.col(col) != F.col(f"{col}_ds2")) & 
                         F.col(col).isNotNull() & 
                         F.col(f"{col}_ds2").isNotNull()),
                        F.struct(
                            F.lit(col).alias("column_name"),
                            F.col(col).cast(StringType()).alias("ds1_value"),
                            F.col(f"{col}_ds2").cast(StringType()).alias("ds2_value")
                        )
                    )
                )
            
            # Create array of all differences for each row
            differences_with_array = differences.select(
                *self.key_columns,
                F.array(*column_comparisons).alias("diff_array")
            )
            
            # Explode to get one row per column difference
            # Filter out nulls (columns that didn't differ)
            all_diffs = differences_with_array.select(
                *self.key_columns,
                F.explode(F.expr("filter(diff_array, x -> x is not null)")).alias("diff")
            ).select(
                *self.key_columns,
                F.col("diff.column_name").alias("column_name"),
                F.col("diff.ds1_value").alias("ds1_value"),
                F.col("diff.ds2_value").alias("ds2_value")
            )
            
            # Unpersist the cached differences
            differences.unpersist()
            
            # Write to disk
            output_file = f"{self.output_path}/differences"
            all_diffs.write.mode("overwrite").parquet(output_file)
            
            # Count from written file
            final_diffs = self.spark.read.parquet(output_file)
            diff_count = final_diffs.count()
            self.stats['total_differences'] = diff_count
            print(f"  Total differences found: {diff_count:,}")
            
            return final_diffs
        else:
            print("  No differences found!")
            self.stats['total_differences'] = 0
            self.stats['rows_with_differences'] = 0
            return self.spark.createDataFrame([], 
                StructType().add("message", StringType()))
    
    def _generate_html_report(self, diff_df):
        """Generate HTML report"""
        html_file = f"{self.output_path}/comparison_report.html"
        
        # Get sample size from config
        sample_size = self.config.get('output', {}).get('max_sample_in_html', 10000)
        
        # Sample for report
        sample_diffs = diff_df.limit(sample_size).toPandas() if not diff_df.rdd.isEmpty() else None
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dataset Comparison Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
                .stats {{ background-color: white; padding: 20px; margin: 20px 0; border-radius: 5px; 
                         box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .stat-item {{ padding: 10px; margin: 5px 0; border-left: 4px solid #3498db; 
                             background-color: #ecf0f1; }}
                table {{ width: 100%; border-collapse: collapse; background-color: white; margin: 20px 0; 
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                th {{ background-color: #34495e; color: white; padding: 12px; text-align: left; }}
                td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .diff-value {{ font-family: monospace; background-color: #fff3cd; padding: 2px 5px; 
                              border-radius: 3px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Dataset Comparison Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            <div class="stats">
                <h2>Summary Statistics</h2>
                <div class="stat-item"><strong>DS1 Total Rows:</strong> {self.stats.get('ds1_total', 'N/A'):,}</div>
                <div class="stat-item"><strong>DS2 Total Rows:</strong> {self.stats.get('ds2_total', 'N/A'):,}</div>
                <div class="stat-item"><strong>Rows only in DS1:</strong> {self.stats.get('ds1_only', 'N/A'):,}</div>
                <div class="stat-item"><strong>Rows only in DS2:</strong> {self.stats.get('ds2_only', 'N/A'):,}</div>
                <div class="stat-item"><strong>Common Rows:</strong> {self.stats.get('common_rows', 'N/A'):,}</div>
                <div class="stat-item"><strong>Rows with Differences:</strong> {self.stats.get('rows_with_differences', 'N/A'):,}</div>
                <div class="stat-item"><strong>Total Column Differences:</strong> {self.stats.get('total_differences', 'N/A'):,}</div>
            </div>
        """
        
        if sample_diffs is not None and len(sample_diffs) > 0:
            html_content += "<h2>Sample Differences</h2><table><tr>"
            for col in sample_diffs.columns:
                html_content += f"<th>{col}</th>"
            html_content += "</tr>"
            for _, row in sample_diffs.iterrows():
                html_content += "<tr>"
                for col in sample_diffs.columns:
                    value = str(row[col]) if row[col] is not None else "NULL"
                    if col in ['ds1_value', 'ds2_value']:
                        html_content += f'<td><span class="diff-value">{value}</span></td>'
                    else:
                        html_content += f"<td>{value}</td>"
                html_content += "</tr>"
            html_content += "</table>"
        
        html_content += "</body></html>"
        
        with open("/tmp/comparison_report.html", "w") as f:
            f.write(html_content)
        
        print(f"  HTML report saved to: /tmp/comparison_report.html")
    
    def _print_summary(self):
        """Print summary"""
        print("\n📊 COMPARISON SUMMARY")
        print("-" * 80)
        for key, value in self.stats.items():
            formatted_value = f"{value:,}" if isinstance(value, int) else value
            print(f"  {key.replace('_', ' ').title()}: {formatted_value}")
        print("-" * 80)
    
    def cleanup(self):
        """Clean up cached DataFrames to free memory"""
        try:
            self.spark.catalog.clearCache()
            print("\n🧹 Cleaned up cached DataFrames")
        except Exception as e:
            print(f"Warning: Could not clear cache: {e}")


def load_config(config_path):
    """
    Load and validate configuration from YAML file
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Validate required fields
    required_fields = [
        ('datasets', 'ds1_path'),
        ('datasets', 'ds2_path'),
        ('datasets', 'output_path'),
        ('comparison', 'key_columns'),
    ]
    
    for section, field in required_fields:
        if section not in config:
            raise ValueError(f"Missing required section in config: {section}")
        if field not in config[section]:
            raise ValueError(f"Missing required field in config: {section}.{field}")
    
    # Ensure key_columns is a list
    if isinstance(config['comparison']['key_columns'], str):
        config['comparison']['key_columns'] = [config['comparison']['key_columns']]
    
    return config


def create_spark_session(config):
    """
    Create Spark session with configuration from YAML
    """
    spark_config = config.get('spark', {})
    app_name = spark_config.get('app_name', 'DatasetComparison')
    configs = spark_config.get('configs', {})
    
    builder = SparkSession.builder.appName(app_name)
    
    # Apply Spark configurations
    for key, value in configs.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def apply_filtering(df, config, dataset_name):
    """
    Apply filtering based on config
    """
    filtering_config = config.get('filtering', {})
    
    if not filtering_config.get('enabled', False):
        return df
    
    # Date filtering
    if 'date_column' in filtering_config:
        date_col = filtering_config['date_column']
        start_date = filtering_config.get('start_date')
        end_date = filtering_config.get('end_date')
        
        if start_date and end_date:
            print(f"  Filtering {dataset_name} by {date_col}: {start_date} to {end_date}")
            df = df.filter(
                (F.col(date_col) >= start_date) & (F.col(date_col) <= end_date)
            )
    
    # Custom filter for specific dataset
    filter_key = f"{dataset_name.lower()}_filter"
    custom_filter = filtering_config.get(filter_key)
    if custom_filter:
        print(f"  Applying custom filter to {dataset_name}: {custom_filter}")
        df = df.filter(custom_filter)
    
    return df


def apply_sampling(df, config, dataset_name):
    """
    Apply sampling based on config
    """
    sampling_config = config.get('sampling', {})
    
    if not sampling_config.get('enabled', False):
        return df
    
    fraction = sampling_config.get('fraction', 0.1)
    seed = sampling_config.get('seed', 42)
    
    print(f"  Sampling {dataset_name}: {fraction*100}% (seed={seed})")
    return df.sample(fraction, seed=seed)


def main():
    """Main execution"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Compare two Parquet datasets')
    parser.add_argument(
        '--config',
        type=str,
        default='compare_config.yaml',
        help='Path to YAML configuration file (default: compare_config.yaml)'
    )
    args = parser.parse_args()
    
    # Load configuration
    print("=" * 80)
    print("LOADING CONFIGURATION")
    print("=" * 80)
    print(f"Config file: {args.config}")
    
    try:
        config = load_config(args.config)
        print("✅ Configuration loaded successfully")
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel(config.get('logging', {}).get('level', 'WARN'))
    
    # Extract configuration
    ds1_path = config['datasets']['ds1_path']
    ds2_path = config['datasets']['ds2_path']
    output_path = config['datasets']['output_path']
    key_columns = config['comparison']['key_columns']
    skip_columns = config['comparison'].get('skip_columns', [])
    
    print(f"\nDataset 1: {ds1_path}")
    print(f"Dataset 2: {ds2_path}")
    print(f"Output: {output_path}")
    print(f"Key columns: {key_columns}")
    if skip_columns:
        print(f"Skip columns: {skip_columns}")
    
    try:
        # Load datasets
        print("\n" + "=" * 80)
        print("LOADING DATASETS")
        print("=" * 80)
        
        ds1 = spark.read.parquet(ds1_path)
        ds2 = spark.read.parquet(ds2_path)
        
        # Apply filtering
        ds1 = apply_filtering(ds1, config, "DS1")
        ds2 = apply_filtering(ds2, config, "DS2")
        
        # Apply sampling
        ds1 = apply_sampling(ds1, config, "DS1")
        ds2 = apply_sampling(ds2, config, "DS2")
        
        # Run data quality checks if enabled
        if config.get('quality_checks', {}).get('enabled', False):
            print("\n" + "=" * 80)
            print("DATA QUALITY CHECKS")
            print("=" * 80)
            run_quality_checks(ds1, ds2, config, key_columns)
        
        # Create comparator instance
        comparator = OptimizedDatasetComparator(
            spark=spark,
            ds1_path=None,  # Already loaded
            ds2_path=None,  # Already loaded
            key_columns=key_columns,
            skip_columns=skip_columns,
            output_path=output_path,
            config=config
        )
        
        # Pass pre-loaded datasets
        comparator.ds1 = ds1
        comparator.ds2 = ds2
        
        # Run comparison
        diff_df = comparator.run_comparison()
        
        # Export to additional formats if configured
        export_formats(diff_df, output_path, config)
        
        # Clean up cached data
        comparator.cleanup()
        
        # Send notification if configured
        send_notification(comparator.stats, config)
        
        print("\n✅ Comparison completed successfully!")
        print(f"📁 Results saved to: {output_path}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


def run_quality_checks(ds1, ds2, config, key_columns):
    """
    Run data quality checks on datasets
    """
    quality_config = config.get('quality_checks', {})
    
    # Check for null keys
    if quality_config.get('fail_on_null_keys', True):
        for col in key_columns:
            null_count_ds1 = ds1.filter(F.col(col).isNull()).count()
            null_count_ds2 = ds2.filter(F.col(col).isNull()).count()
            
            if null_count_ds1 > 0:
                raise ValueError(f"DS1 has {null_count_ds1:,} NULL values in key column '{col}'")
            if null_count_ds2 > 0:
                raise ValueError(f"DS2 has {null_count_ds2:,} NULL values in key column '{col}'")
        
        print("  ✅ No NULL values in key columns")
    
    # Check for duplicates
    if quality_config.get('check_duplicates', True):
        dup_count_ds1 = ds1.groupBy(key_columns).count().filter("count > 1").count()
        dup_count_ds2 = ds2.groupBy(key_columns).count().filter("count > 1").count()
        
        if dup_count_ds1 > 0:
            print(f"  ⚠️  Warning: DS1 has {dup_count_ds1:,} duplicate keys")
        if dup_count_ds2 > 0:
            print(f"  ⚠️  Warning: DS2 has {dup_count_ds2:,} duplicate keys")
        
        if dup_count_ds1 == 0 and dup_count_ds2 == 0:
            print("  ✅ No duplicate keys found")


def export_formats(diff_df, output_path, config):
    """
    Export differences to additional formats based on config
    """
    output_config = config.get('output', {})
    formats = output_config.get('formats', {})
    coalesce_config = output_config.get('coalesce', {})
    
    if not diff_df or diff_df.rdd.isEmpty():
        return
    
    # Apply coalescing if configured
    if coalesce_config.get('enabled', False):
        num_files = coalesce_config.get('num_files', 10)
        diff_df = diff_df.coalesce(num_files)
    
    # Export to CSV
    if formats.get('csv', False):
        print("  Exporting to CSV...")
        diff_df.write.mode("overwrite").option("header", "true").csv(
            f"{output_path}/differences_csv"
        )
    
    # Export to JSON
    if formats.get('json', False):
        print("  Exporting to JSON...")
        diff_df.write.mode("overwrite").json(f"{output_path}/differences_json")


def send_notification(stats, config):
    """
    Send notification about comparison results
    """
    notification_config = config.get('notification', {})
    
    if not notification_config.get('enabled', False):
        return
    
    # Placeholder for notification logic
    # Would integrate with AWS SNS or email service
    print("\n📧 Notification: Comparison completed")
    print(f"   Total differences: {stats.get('total_differences', 0):,}")



if __name__ == "__main__":
    main()
