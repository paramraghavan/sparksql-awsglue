"""
Large-scale Parquet File Comparison with PySpark
Optimized for EMR with large datasets (50GB+, 200+ columns)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json
from datetime import datetime
from typing import List, Dict, Tuple
import argparse


class ParquetComparator:
    def __init__(self, spark: SparkSession, key_columns: List[str], skip_columns: List[str] = None):
        """
        Initialize the comparator
        
        Args:
            spark: SparkSession
            key_columns: List of column names that form the composite key
            skip_columns: List of column names to skip during comparison
        """
        self.spark = spark
        self.key_columns = key_columns
        self.skip_columns = skip_columns or []
        self.results = {}
        
    def load_parquet(self, path: str, alias: str) -> DataFrame:
        """Load parquet file with optimizations"""
        print(f"Loading {alias} from {path}...")
        df = self.spark.read.parquet(path)
        
        # Add source identifier
        df = df.withColumn("_source", F.lit(alias))
        
        print(f"{alias} loaded: {df.count():,} records, {len(df.columns)} columns")
        return df
    
    def compare_schemas(self, df1: DataFrame, df2: DataFrame) -> Dict:
        """Compare schemas between two dataframes"""
        print("\nComparing schemas...")
        
        cols1 = set(df1.columns) - {"_source"}
        cols2 = set(df2.columns) - {"_source"}
        
        common_cols = cols1.intersection(cols2)
        only_in_df1 = cols1 - cols2
        only_in_df2 = cols2 - cols1
        
        schema_diff = {
            "total_columns_df1": len(cols1),
            "total_columns_df2": len(cols2),
            "common_columns": len(common_cols),
            "only_in_df1": sorted(list(only_in_df1)),
            "only_in_df2": sorted(list(only_in_df2))
        }
        
        print(f"Common columns: {len(common_cols)}")
        print(f"Only in DF1: {len(only_in_df1)}")
        print(f"Only in DF2: {len(only_in_df2)}")
        
        return schema_diff
    
    def compare_record_counts(self, df1: DataFrame, df2: DataFrame) -> Dict:
        """Compare record counts"""
        print("\nComparing record counts...")
        
        count1 = df1.count()
        count2 = df2.count()
        
        count_diff = {
            "df1_count": count1,
            "df2_count": count2,
            "difference": count1 - count2,
            "difference_pct": round(((count1 - count2) / max(count1, count2)) * 100, 2) if max(count1, count2) > 0 else 0
        }
        
        print(f"DF1 records: {count1:,}")
        print(f"DF2 records: {count2:,}")
        print(f"Difference: {count_diff['difference']:,} ({count_diff['difference_pct']}%)")
        
        return count_diff
    
    def find_unique_records(self, df1: DataFrame, df2: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Find records unique to each dataframe based on key columns"""
        print("\nFinding unique records...")
        
        # Create composite key
        key_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in self.key_columns])
        
        df1_keys = df1.select(self.key_columns).withColumn("_key", key_expr).select("_key").distinct()
        df2_keys = df2.select(self.key_columns).withColumn("_key", key_expr).select("_key").distinct()
        
        # Cache for multiple operations
        df1_keys.cache()
        df2_keys.cache()
        
        # Find unique keys
        only_in_df1_keys = df1_keys.join(df2_keys, on="_key", how="left_anti")
        only_in_df2_keys = df2_keys.join(df1_keys, on="_key", how="left_anti")
        
        count_only_df1 = only_in_df1_keys.count()
        count_only_df2 = only_in_df2_keys.count()
        
        print(f"Records only in DF1: {count_only_df1:,}")
        print(f"Records only in DF2: {count_only_df2:,}")
        
        # Get full records for unique keys (limit for reporting)
        df1_with_key = df1.withColumn("_key", key_expr)
        df2_with_key = df2.withColumn("_key", key_expr)
        
        only_in_df1 = df1_with_key.join(only_in_df1_keys, on="_key", how="inner").drop("_key", "_source")
        only_in_df2 = df2_with_key.join(only_in_df2_keys, on="_key", how="inner").drop("_key", "_source")
        
        self.results['unique_records'] = {
            "only_in_df1_count": count_only_df1,
            "only_in_df2_count": count_only_df2
        }
        
        return only_in_df1, only_in_df2
    
    def compare_column_values(self, df1: DataFrame, df2: DataFrame) -> Dict:
        """Compare column values for matching records"""
        print("\nComparing column values for matching records...")
        
        # Get common columns excluding key, skip, and source columns
        common_cols = list(set(df1.columns).intersection(set(df2.columns)))
        compare_cols = [c for c in common_cols if c not in self.key_columns and c not in self.skip_columns and c != "_source"]
        
        print(f"Comparing {len(compare_cols)} columns...")
        
        # Create composite key for join
        key_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in self.key_columns])
        
        df1_compare = df1.withColumn("_key", key_expr)
        df2_compare = df2.withColumn("_key", key_expr)
        
        # Inner join to get matching records
        df1_compare = df1_compare.select("_key", *self.key_columns, *compare_cols)
        df2_compare = df2_compare.select("_key", *compare_cols)
        
        joined = df1_compare.join(
            df2_compare,
            on="_key",
            how="inner"
        )
        
        matching_count = joined.count()
        print(f"Matching records: {matching_count:,}")
        
        if matching_count == 0:
            return {"matching_records": 0, "column_differences": []}
        
        # Persist for multiple aggregations
        joined.cache()
        
        # Compare each column
        column_differences = []
        
        for col in compare_cols:
            # Cast both columns to string for comparison (handles different types)
            col_df1 = F.coalesce(F.col(col).cast("string"), F.lit("NULL"))
            col_df2 = F.coalesce(joined[col].cast("string"), F.lit("NULL"))  # From df2 side
            
            # Count differences
            diff_count = joined.filter(col_df1 != col_df2).count()
            
            if diff_count > 0:
                diff_pct = round((diff_count / matching_count) * 100, 2)
                column_differences.append({
                    "column": col,
                    "differences": diff_count,
                    "percentage": diff_pct
                })
                print(f"  {col}: {diff_count:,} differences ({diff_pct}%)")
        
        # Get sample of differences for top columns
        samples = []
        for col_diff in sorted(column_differences, key=lambda x: x['differences'], reverse=True)[:10]:
            col = col_diff['column']
            
            # Define comparison expressions once
            col_df1 = F.coalesce(F.col(col).cast("string"), F.lit("NULL"))
            col_df2 = F.coalesce(joined[col].cast("string"), F.lit("NULL"))
            
            sample_df = joined.filter(col_df1 != col_df2).select(
                *self.key_columns,
                F.col(col).alias(f"{col}_df1"),
                joined[col].alias(f"{col}_df2")
            ).limit(100).collect()
            
            samples.append({
                "column": col,
                "sample_differences": [row.asDict() for row in sample_df]
            })
        
        joined.unpersist()
        
        return {
            "matching_records": matching_count,
            "columns_compared": len(compare_cols),
            "columns_with_differences": len(column_differences),
            "column_differences": sorted(column_differences, key=lambda x: x['differences'], reverse=True),
            "sample_differences": samples
        }
    
    def generate_html_report(self, output_path: str, df1_name: str, df2_name: str):
        """Generate paginated HTML report"""
        print(f"\nGenerating HTML report: {output_path}")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Parquet Comparison Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            color: #2c3e50;
            margin-bottom: 10px;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        
        h2 {{
            color: #34495e;
            margin-top: 30px;
            margin-bottom: 15px;
            border-left: 4px solid #3498db;
            padding-left: 15px;
        }}
        
        h3 {{
            color: #555;
            margin-top: 20px;
            margin-bottom: 10px;
        }}
        
        .metadata {{
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            font-size: 14px;
        }}
        
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        
        .metric-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        .metric-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        
        .metric-card.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        
        .metric-card.info {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        
        .metric-label {{
            font-size: 14px;
            opacity: 0.9;
            margin-bottom: 5px;
        }}
        
        .metric-value {{
            font-size: 32px;
            font-weight: bold;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        
        th {{
            background: #3498db;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
        }}
        
        td {{
            padding: 12px;
            border-bottom: 1px solid #ddd;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }}
        
        .badge-success {{
            background: #d4edda;
            color: #155724;
        }}
        
        .badge-danger {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .badge-warning {{
            background: #fff3cd;
            color: #856404;
        }}
        
        .pagination {{
            display: flex;
            justify-content: center;
            gap: 10px;
            margin: 20px 0;
            flex-wrap: wrap;
        }}
        
        .page-btn {{
            padding: 8px 16px;
            background: #3498db;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }}
        
        .page-btn:hover {{
            background: #2980b9;
        }}
        
        .page-btn.active {{
            background: #2c3e50;
        }}
        
        .page-btn:disabled {{
            background: #bdc3c7;
            cursor: not-allowed;
        }}
        
        .section {{
            display: none;
        }}
        
        .section.active {{
            display: block;
        }}
        
        .tab-container {{
            display: flex;
            gap: 10px;
            margin: 20px 0;
            border-bottom: 2px solid #ddd;
        }}
        
        .tab {{
            padding: 10px 20px;
            background: #ecf0f1;
            border: none;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px 5px 0 0;
            transition: all 0.3s;
        }}
        
        .tab.active {{
            background: #3498db;
            color: white;
        }}
        
        .collapsible {{
            background: #f1f1f1;
            padding: 10px;
            cursor: pointer;
            border: none;
            width: 100%;
            text-align: left;
            font-size: 16px;
            margin-top: 10px;
            border-radius: 4px;
        }}
        
        .collapsible:hover {{
            background: #ddd;
        }}
        
        .collapsible.active {{
            background: #3498db;
            color: white;
        }}
        
        .content {{
            display: none;
            overflow: auto;
            padding: 15px;
            background: #fafafa;
            border: 1px solid #ddd;
            border-radius: 0 0 4px 4px;
        }}
        
        .content.show {{
            display: block;
        }}
        
        pre {{
            background: #2c3e50;
            color: #ecf0f1;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
            font-size: 13px;
        }}
        
        .search-box {{
            padding: 10px;
            width: 300px;
            border: 2px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            margin: 10px 0;
        }}
        
        .search-box:focus {{
            outline: none;
            border-color: #3498db;
        }}
        
        @media print {{
            .pagination, .tab-container, .search-box {{
                display: none;
            }}
            .section {{
                display: block !important;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Parquet File Comparison Report</h1>
        
        <div class="metadata">
            <strong>Generated:</strong> {timestamp}<br>
            <strong>Dataset 1:</strong> {df1_name}<br>
            <strong>Dataset 2:</strong> {df2_name}<br>
            <strong>Key Columns:</strong> {', '.join(self.key_columns)}<br>
            <strong>Skipped Columns:</strong> {', '.join(self.skip_columns) if self.skip_columns else 'None'}
        </div>
"""
        
        # Summary metrics
        record_diff = self.results.get('record_counts', {})
        schema_diff = self.results.get('schema', {})
        unique_recs = self.results.get('unique_records', {})
        col_compare = self.results.get('column_comparison', {})
        
        html += f"""
        <h2>📈 Summary</h2>
        <div class="summary-grid">
            <div class="metric-card info">
                <div class="metric-label">Total Records (DF1)</div>
                <div class="metric-value">{record_diff.get('df1_count', 0):,}</div>
            </div>
            <div class="metric-card info">
                <div class="metric-label">Total Records (DF2)</div>
                <div class="metric-value">{record_diff.get('df2_count', 0):,}</div>
            </div>
            <div class="metric-card {'success' if record_diff.get('difference', 0) == 0 else 'warning'}">
                <div class="metric-label">Record Difference</div>
                <div class="metric-value">{record_diff.get('difference', 0):,}</div>
            </div>
            <div class="metric-card {'success' if col_compare.get('columns_with_differences', 0) == 0 else 'warning'}">
                <div class="metric-label">Columns with Differences</div>
                <div class="metric-value">{col_compare.get('columns_with_differences', 0)}</div>
            </div>
        </div>
"""
        
        # Schema differences
        html += f"""
        <h2>🔍 Schema Comparison</h2>
        <div class="summary-grid">
            <div class="metric-card">
                <div class="metric-label">Common Columns</div>
                <div class="metric-value">{schema_diff.get('common_columns', 0)}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Only in DF1</div>
                <div class="metric-value">{len(schema_diff.get('only_in_df1', []))}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Only in DF2</div>
                <div class="metric-value">{len(schema_diff.get('only_in_df2', []))}</div>
            </div>
        </div>
"""
        
        if schema_diff.get('only_in_df1'):
            html += f"""
        <button class="collapsible">Columns Only in Dataset 1 ({len(schema_diff['only_in_df1'])})</button>
        <div class="content">
            <pre>{', '.join(schema_diff['only_in_df1'])}</pre>
        </div>
"""
        
        if schema_diff.get('only_in_df2'):
            html += f"""
        <button class="collapsible">Columns Only in Dataset 2 ({len(schema_diff['only_in_df2'])})</button>
        <div class="content">
            <pre>{', '.join(schema_diff['only_in_df2'])}</pre>
        </div>
"""
        
        # Unique records
        html += f"""
        <h2>🔑 Unique Records by Key</h2>
        <div class="summary-grid">
            <div class="metric-card {'success' if unique_recs.get('only_in_df1_count', 0) == 0 else 'warning'}">
                <div class="metric-label">Only in Dataset 1</div>
                <div class="metric-value">{unique_recs.get('only_in_df1_count', 0):,}</div>
            </div>
            <div class="metric-card {'success' if unique_recs.get('only_in_df2_count', 0) == 0 else 'warning'}">
                <div class="metric-label">Only in Dataset 2</div>
                <div class="metric-value">{unique_recs.get('only_in_df2_count', 0):,}</div>
            </div>
            <div class="metric-card info">
                <div class="metric-label">Matching Records</div>
                <div class="metric-value">{col_compare.get('matching_records', 0):,}</div>
            </div>
        </div>
"""
        
        # Column value differences
        if col_compare.get('column_differences'):
            html += """
        <h2>📋 Column Value Differences</h2>
        <input type="text" class="search-box" id="columnSearch" placeholder="Search columns...">
        <table id="diffTable">
            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Differences Count</th>
                    <th>Percentage</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
"""
            for col_diff in col_compare['column_differences']:
                status = 'success' if col_diff['differences'] == 0 else ('warning' if col_diff['percentage'] < 10 else 'danger')
                html += f"""
                <tr>
                    <td><strong>{col_diff['column']}</strong></td>
                    <td>{col_diff['differences']:,}</td>
                    <td>{col_diff['percentage']}%</td>
                    <td><span class="badge badge-{status}">{'Match' if col_diff['differences'] == 0 else 'Diff'}</span></td>
                </tr>
"""
            html += """
            </tbody>
        </table>
"""
        
        # Sample differences
        if col_compare.get('sample_differences'):
            html += """
        <h2>🔬 Sample Differences (Top Columns)</h2>
"""
            for sample in col_compare['sample_differences'][:5]:
                if sample['sample_differences']:
                    html += f"""
        <button class="collapsible">Column: {sample['column']} (Showing {len(sample['sample_differences'])} samples)</button>
        <div class="content">
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr>
                            {''.join([f'<th>{key}</th>' for key in self.key_columns])}
                            <th>Dataset 1 Value</th>
                            <th>Dataset 2 Value</th>
                        </tr>
                    </thead>
                    <tbody>
"""
                    for row in sample['sample_differences'][:20]:
                        html += "<tr>"
                        for key_col in self.key_columns:
                            html += f"<td>{row.get(key_col, '')}</td>"
                        html += f"<td>{row.get(f'{sample['column']}_df1', '')}</td>"
                        html += f"<td>{row.get(f'{sample['column']}_df2', '')}</td>"
                        html += "</tr>"
                    
                    html += """
                    </tbody>
                </table>
            </div>
        </div>
"""
        
        # JavaScript for interactivity
        html += """
    </div>
    
    <script>
        // Collapsible sections
        var coll = document.getElementsByClassName("collapsible");
        for (var i = 0; i < coll.length; i++) {
            coll[i].addEventListener("click", function() {
                this.classList.toggle("active");
                var content = this.nextElementSibling;
                content.classList.toggle("show");
            });
        }
        
        // Column search
        const searchInput = document.getElementById('columnSearch');
        if (searchInput) {
            searchInput.addEventListener('keyup', function() {
                const filter = this.value.toUpperCase();
                const table = document.getElementById('diffTable');
                const tr = table.getElementsByTagName('tr');
                
                for (let i = 1; i < tr.length; i++) {
                    const td = tr[i].getElementsByTagName('td')[0];
                    if (td) {
                        const txtValue = td.textContent || td.innerText;
                        tr[i].style.display = txtValue.toUpperCase().indexOf(filter) > -1 ? '' : 'none';
                    }
                }
            });
        }
    </script>
</body>
</html>
"""
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        print(f"Report generated successfully: {output_path}")
    
    def run_comparison(self, path1: str, path2: str, df1_name: str = "Dataset1", df2_name: str = "Dataset2"):
        """Run complete comparison workflow"""
        print("=" * 80)
        print("STARTING PARQUET COMPARISON")
        print("=" * 80)
        
        # Load data
        df1 = self.load_parquet(path1, df1_name)
        df2 = self.load_parquet(path2, df2_name)
        
        # Compare schemas
        self.results['schema'] = self.compare_schemas(df1, df2)
        
        # Compare record counts
        self.results['record_counts'] = self.compare_record_counts(df1, df2)
        
        # Find unique records
        only_df1, only_df2 = self.find_unique_records(df1, df2)
        
        # Save samples of unique records
        if self.results['unique_records']['only_in_df1_count'] > 0:
            only_df1.limit(1000).write.mode('overwrite').parquet(f'/tmp/{df1_name}_unique_records')
            print(f"Sample of unique {df1_name} records saved to /tmp/{df1_name}_unique_records")
        
        if self.results['unique_records']['only_in_df2_count'] > 0:
            only_df2.limit(1000).write.mode('overwrite').parquet(f'/tmp/{df2_name}_unique_records')
            print(f"Sample of unique {df2_name} records saved to /tmp/{df2_name}_unique_records")
        
        # Compare column values
        self.results['column_comparison'] = self.compare_column_values(df1, df2)
        
        print("\n" + "=" * 80)
        print("COMPARISON COMPLETE")
        print("=" * 80)
        
        return self.results


def main():
    parser = argparse.ArgumentParser(description='Compare two large parquet files')
    parser.add_argument('--path1', required=True, help='Path to first parquet file/directory')
    parser.add_argument('--path2', required=True, help='Path to second parquet file/directory')
    parser.add_argument('--keys', required=True, help='Comma-separated key columns')
    parser.add_argument('--skip', default='', help='Comma-separated columns to skip')
    parser.add_argument('--name1', default='Dataset1', help='Name for first dataset')
    parser.add_argument('--name2', default='Dataset2', help='Name for second dataset')
    parser.add_argument('--output', default='/tmp/comparison_report.html', help='Output HTML report path')
    parser.add_argument('--partitions', type=int, default=200, help='Number of shuffle partitions')
    
    args = parser.parse_args()
    
    # Parse columns
    key_columns = [k.strip() for k in args.keys.split(',')]
    skip_columns = [s.strip() for s in args.skip.split(',')] if args.skip else []
    
    # Initialize Spark with optimizations for large data
    spark = SparkSession.builder \
        .appName("ParquetComparison") \
        .config("spark.sql.shuffle.partitions", args.partitions) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\nSpark Configuration:")
    print(f"  Shuffle Partitions: {args.partitions}")
    print(f"  Adaptive Query Execution: Enabled")
    print(f"  Key Columns: {key_columns}")
    print(f"  Skip Columns: {skip_columns if skip_columns else 'None'}\n")
    
    try:
        # Create comparator and run
        comparator = ParquetComparator(spark, key_columns, skip_columns)
        results = comparator.run_comparison(args.path1, args.path2, args.name1, args.name2)
        
        # Generate report
        comparator.generate_html_report(args.output, args.name1, args.name2)
        
        print(f"\n✅ Comparison complete! Report available at: {args.output}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Record Count Difference: {results['record_counts']['difference']:,}")
        print(f"Unique to {args.name1}: {results['unique_records']['only_in_df1_count']:,}")
        print(f"Unique to {args.name2}: {results['unique_records']['only_in_df2_count']:,}")
        print(f"Columns with Differences: {results['column_comparison']['columns_with_differences']}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
