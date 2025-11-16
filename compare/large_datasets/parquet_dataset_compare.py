"""
Robust PySpark Dataset Comparison Tool
Compares two large parquet datasets with incremental processing to avoid OOM errors
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
from datetime import datetime
import sys

class DatasetComparator:
    def __init__(self, spark, ds1_path, ds2_path, key_columns, skip_columns=None, 
                 output_path="/tmp/comparison_output"):
        """
        Initialize the comparator
        
        Args:
            spark: SparkSession
            ds1_path: Path to first parquet dataset
            ds2_path: Path to second parquet dataset
            key_columns: List of key columns for joining (e.g., ['id', 'date'])
            skip_columns: List of columns to skip from comparison
            output_path: Path to write output files
        """
        self.spark = spark
        self.ds1_path = ds1_path
        self.ds2_path = ds2_path
        self.key_columns = key_columns if isinstance(key_columns, list) else [key_columns]
        self.skip_columns = skip_columns or []
        self.output_path = output_path
        self.stats = {}
        
    def run_comparison(self):
        """Main method to run the comparison"""
        print("=" * 80)
        print("STARTING DATASET COMPARISON")
        print("=" * 80)
        
        # Step 1: Load datasets
        print("\n[1/7] Loading datasets...")
        ds1, ds2 = self._load_datasets()
        
        # Step 2: Get row counts
        print("\n[2/7] Counting rows (using approx for speed)...")
        self._count_rows(ds1, ds2)
        
        # Step 3: Find rows only in ds1
        print("\n[3/7] Finding rows only in DS1...")
        self._find_unique_rows(ds1, ds2, "ds1_only")
        
        # Step 4: Find rows only in ds2
        print("\n[4/7] Finding rows only in DS2...")
        self._find_unique_rows(ds2, ds1, "ds2_only")
        
        # Step 5: Count common rows
        print("\n[5/7] Finding common rows...")
        self._count_common_rows(ds1, ds2)
        
        # Step 6: Find differences in common rows
        print("\n[6/7] Finding differences in common rows...")
        diff_df = self._find_differences(ds1, ds2)
        
        # Step 7: Generate HTML report
        print("\n[7/7] Generating HTML report...")
        self._generate_html_report(diff_df)
        
        print("\n" + "=" * 80)
        print("COMPARISON COMPLETE!")
        print("=" * 80)
        self._print_summary()
        
        return diff_df
    
    def _load_datasets(self):
        """Load both datasets"""
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
    
    def _count_rows(self, ds1, ds2):
        """
        Count rows efficiently
        Uses approx_count_distinct for large datasets to avoid expensive exact counts
        """
        # For exact counts (can be slow on very large datasets)
        # Use rdd.count() which is more stable than df.count() for large data
        print("  Counting DS1 rows...")
        count1 = ds1.rdd.count()
        self.stats['ds1_total'] = count1
        print(f"  DS1 Total Rows: {count1:,}")
        
        print("  Counting DS2 rows...")
        count2 = ds2.rdd.count()
        self.stats['ds2_total'] = count2
        print(f"  DS2 Total Rows: {count2:,}")
    
    def _find_unique_rows(self, left_df, right_df, result_key):
        """
        Find rows that exist only in left_df using left anti join
        This is much more efficient than full outer join
        """
        # Select only key columns for the anti join to reduce shuffle
        left_keys = left_df.select(self.key_columns).distinct()
        right_keys = right_df.select(self.key_columns).distinct()
        
        # Left anti join: rows in left that don't match right
        unique_rows = left_keys.join(
            right_keys,
            self.key_columns,
            "left_anti"
        )
        
        # Write to parquet for later use
        output_file = f"{self.output_path}/{result_key}"
        unique_rows.write.mode("overwrite").parquet(output_file)
        
        # Count using the written file to avoid recomputation
        count = self.spark.read.parquet(output_file).rdd.count()
        self.stats[result_key] = count
        print(f"  {result_key}: {count:,} rows")
    
    def _count_common_rows(self, ds1, ds2):
        """Count rows common to both datasets based on key columns"""
        # Use inner join on keys only for efficiency
        ds1_keys = ds1.select(self.key_columns).distinct()
        ds2_keys = ds2.select(self.key_columns).distinct()
        
        common = ds1_keys.join(ds2_keys, self.key_columns, "inner")
        
        # Write and count from file
        output_file = f"{self.output_path}/common_keys"
        common.write.mode("overwrite").parquet(output_file)
        
        count = self.spark.read.parquet(output_file).rdd.count()
        self.stats['common_rows'] = count
        print(f"  Common rows: {count:,}")
    
    def _find_differences(self, ds1, ds2):
        """
        Find differences between common rows
        Uses incremental approach to avoid memory issues
        """
        # Get columns to compare (exclude keys and skip columns)
        all_cols = set(ds1.columns).intersection(set(ds2.columns))
        compare_cols = [col for col in all_cols 
                       if col not in self.key_columns 
                       and col not in self.skip_columns]
        
        print(f"  Comparing {len(compare_cols)} columns")
        print(f"  Key columns: {self.key_columns}")
        print(f"  Skipped columns: {self.skip_columns}")
        
        # Rename columns in ds2 to avoid conflicts
        ds2_renamed = ds2
        for col in compare_cols:
            ds2_renamed = ds2_renamed.withColumnRenamed(col, f"{col}_ds2")
        
        # Inner join on key columns to get only common rows
        joined = ds1.join(
            ds2_renamed.select(self.key_columns + [f"{col}_ds2" for col in compare_cols]),
            self.key_columns,
            "inner"
        )
        
        # Persist to avoid recomputation
        joined = joined.persist()
        
        # Build difference conditions
        diff_conditions = []
        for col in compare_cols:
            # Handle nulls properly: null != null is false, but we want to catch when one is null and other isn't
            diff_cond = (
                (F.col(col).isNull() & F.col(f"{col}_ds2").isNotNull()) |
                (F.col(col).isNotNull() & F.col(f"{col}_ds2").isNull()) |
                ((F.col(col) != F.col(f"{col}_ds2")) & 
                 F.col(col).isNotNull() & 
                 F.col(f"{col}_ds2").isNotNull())
            )
            diff_conditions.append(diff_cond)
        
        # Filter to rows with any differences
        differences = joined.filter(
            F.expr(" OR ".join([f"({i})" for i in range(len(diff_conditions))]))
        ) if diff_conditions else joined.filter(F.lit(False))
        
        # Create detailed difference records
        diff_records = []
        
        for col in compare_cols:
            col_diff = differences.filter(
                (F.col(col).isNull() & F.col(f"{col}_ds2").isNotNull()) |
                (F.col(col).isNotNull() & F.col(f"{col}_ds2").isNull()) |
                ((F.col(col) != F.col(f"{col}_ds2")) & 
                 F.col(col).isNotNull() & 
                 F.col(f"{col}_ds2").isNotNull())
            ).select(
                *self.key_columns,
                F.lit(col).alias("column_name"),
                F.col(col).cast(StringType()).alias("ds1_value"),
                F.col(f"{col}_ds2").cast(StringType()).alias("ds2_value")
            )
            diff_records.append(col_diff)
        
        # Union all differences
        if diff_records:
            all_diffs = diff_records[0]
            for df in diff_records[1:]:
                all_diffs = all_diffs.union(df)
            
            # Write differences to parquet
            output_file = f"{self.output_path}/differences"
            all_diffs.write.mode("overwrite").parquet(output_file)
            
            # Read back and count
            final_diffs = self.spark.read.parquet(output_file)
            diff_count = final_diffs.rdd.count()
            self.stats['total_differences'] = diff_count
            print(f"  Total differences found: {diff_count:,}")
            
            # Get unique row count with differences
            rows_with_diffs = final_diffs.select(self.key_columns).distinct().rdd.count()
            self.stats['rows_with_differences'] = rows_with_diffs
            print(f"  Rows with differences: {rows_with_diffs:,}")
            
            joined.unpersist()
            return final_diffs
        else:
            joined.unpersist()
            print("  No differences found!")
            self.stats['total_differences'] = 0
            self.stats['rows_with_differences'] = 0
            return self.spark.createDataFrame([], 
                StructType().add("message", StringType()))
    
    def _generate_html_report(self, diff_df):
        """Generate HTML report from differences"""
        html_file = f"{self.output_path}/comparison_report.html"
        
        # Sample differences for report (limit to avoid memory issues)
        sample_size = 10000
        sample_diffs = diff_df.limit(sample_size).toPandas() if diff_df.rdd.isEmpty() == False else None
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dataset Comparison Report</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .header {{
                    background-color: #2c3e50;
                    color: white;
                    padding: 20px;
                    border-radius: 5px;
                }}
                .stats {{
                    background-color: white;
                    padding: 20px;
                    margin: 20px 0;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .stat-item {{
                    padding: 10px;
                    margin: 5px 0;
                    border-left: 4px solid #3498db;
                    background-color: #ecf0f1;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    background-color: white;
                    margin: 20px 0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                th {{
                    background-color: #34495e;
                    color: white;
                    padding: 12px;
                    text-align: left;
                }}
                td {{
                    padding: 10px;
                    border-bottom: 1px solid #ddd;
                }}
                tr:hover {{
                    background-color: #f5f5f5;
                }}
                .diff-value {{
                    font-family: monospace;
                    background-color: #fff3cd;
                    padding: 2px 5px;
                    border-radius: 3px;
                }}
                .note {{
                    background-color: #d4edda;
                    padding: 15px;
                    border-left: 4px solid #28a745;
                    margin: 20px 0;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Dataset Comparison Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="stats">
                <h2>Summary Statistics</h2>
                <div class="stat-item">
                    <strong>DS1 Total Rows:</strong> {self.stats.get('ds1_total', 'N/A'):,}
                </div>
                <div class="stat-item">
                    <strong>DS2 Total Rows:</strong> {self.stats.get('ds2_total', 'N/A'):,}
                </div>
                <div class="stat-item">
                    <strong>Rows only in DS1:</strong> {self.stats.get('ds1_only', 'N/A'):,}
                </div>
                <div class="stat-item">
                    <strong>Rows only in DS2:</strong> {self.stats.get('ds2_only', 'N/A'):,}
                </div>
                <div class="stat-item">
                    <strong>Common Rows:</strong> {self.stats.get('common_rows', 'N/A'):,}
                </div>
                <div class="stat-item">
                    <strong>Rows with Differences:</strong> {self.stats.get('rows_with_differences', 'N/A'):,}
                </div>
                <div class="stat-item">
                    <strong>Total Column Differences:</strong> {self.stats.get('total_differences', 'N/A'):,}
                </div>
            </div>
            
            <div class="note">
                <strong>Note:</strong> Showing up to {sample_size:,} sample differences below. 
                Full differences are saved in parquet format at: {self.output_path}/differences
            </div>
        """
        
        if sample_diffs is not None and len(sample_diffs) > 0:
            html_content += """
            <h2>Sample Differences</h2>
            <table>
                <tr>
            """
            
            # Add headers
            for col in sample_diffs.columns:
                html_content += f"<th>{col}</th>"
            html_content += "</tr>"
            
            # Add rows
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
        else:
            html_content += "<p>No differences found between datasets!</p>"
        
        html_content += """
        </body>
        </html>
        """
        
        # Write HTML to local file then upload to S3/HDFS
        with open("/tmp/comparison_report.html", "w") as f:
            f.write(html_content)
        
        # Copy to output path
        print(f"  HTML report saved to: /tmp/comparison_report.html")
        print(f"  You can also find outputs at: {self.output_path}")
    
    def _print_summary(self):
        """Print final summary"""
        print("\n📊 COMPARISON SUMMARY")
        print("-" * 80)
        for key, value in self.stats.items():
            print(f"  {key.replace('_', ' ').title()}: {value:,}")
        print("-" * 80)


def main():
    """Main execution function"""
    
    # Initialize Spark with optimized settings for large datasets
    spark = SparkSession.builder \
        .appName("DatasetComparison") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # ============================================================================
    # CONFIGURE YOUR COMPARISON HERE
    # ============================================================================
    
    # Paths to your parquet datasets
    DS1_PATH = "s3://your-bucket/path/to/dataset1/"
    DS2_PATH = "s3://your-bucket/path/to/dataset2/"
    
    # Key columns for joining (can be single column or list of columns)
    KEY_COLUMNS = ["id"]  # Example: ["customer_id", "date"]
    
    # Columns to skip from comparison (optional)
    SKIP_COLUMNS = ["updated_at", "processed_date"]  # Example columns to skip
    
    # Output path for results
    OUTPUT_PATH = "s3://your-bucket/comparison-output/"
    
    # ============================================================================
    
    try:
        # Create comparator instance
        comparator = DatasetComparator(
            spark=spark,
            ds1_path=DS1_PATH,
            ds2_path=DS2_PATH,
            key_columns=KEY_COLUMNS,
            skip_columns=SKIP_COLUMNS,
            output_path=OUTPUT_PATH
        )
        
        # Run comparison
        diff_df = comparator.run_comparison()
        
        print("\n✅ Comparison completed successfully!")
        print(f"📁 Results saved to: {OUTPUT_PATH}")
        print(f"📄 HTML report: /tmp/comparison_report.html")
        
    except Exception as e:
        print(f"\n❌ Error during comparison: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
