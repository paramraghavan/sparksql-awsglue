from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, concat_ws, expr
import argparse
import time
import os
from datetime import datetime


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Compare two large Parquet datasets in S3")
    parser.add_argument("--source_path", required=True, help="S3 path to source Parquet data")
    parser.add_argument("--target_path", required=True, help="S3 path to target Parquet data")
    parser.add_argument("--output_path", required=True, help="S3 path for output HTML report")
    parser.add_argument("--key_columns", required=True, help="Comma-separated list of key columns for joining")
    parser.add_argument("--exclude_columns", default="",
                        help="Comma-separated list of columns to exclude from comparison")
    parser.add_argument("--max_executor_nodes", type=int, default=5, help="Maximum number of executor nodes to use")
    parser.add_argument("--executor_memory", default="4g", help="Memory per executor")
    parser.add_argument("--executor_cores", type=int, default=2, help="Cores per executor")
    parser.add_argument("--driver_memory", default="8g", help="Driver memory")
    parser.add_argument("--sample_size", type=float, default=1.0,
                        help="Sample size as fraction (0.0-1.0), default=1.0 (full dataset)")
    return parser.parse_args()


def create_spark_session(args):
    """Create and configure Spark session based on args"""
    spark = (
        SparkSession.builder
        .appName("Large Parquet Comparison")
        .config("spark.executor.instances", args.max_executor_nodes)
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.executor.cores", args.executor_cores)
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", args.max_executor_nodes * args.executor_cores * 2)
        .config("spark.default.parallelism", args.max_executor_nodes * args.executor_cores * 2)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", args.max_executor_nodes)
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.memory.fraction", "0.8")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.broadcastTimeout", "1200")
        .getOrCreate()
    )
    return spark


def log_info(spark, message):
    """Log information with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    spark.sparkContext.setJobDescription(message)


def load_and_prepare_data(spark, path, key_columns, sample_size=1.0):
    """Load parquet data from S3 with optional sampling"""
    log_info(spark, f"Loading data from {path}")
    df = spark.read.parquet(path)

    # Apply sampling if specified
    if sample_size < 1.0:
        log_info(spark, f"Sampling {sample_size * 100}% of data")
        df = df.sample(fraction=sample_size, seed=42)

    # Handle potential key column issues
    for key in key_columns:
        if key not in df.columns:
            raise ValueError(f"Key column '{key}' not found in dataset at {path}")

    # Optimize by caching since we'll use this dataframe multiple times
    df = df.cache()

    # Trigger execution and cache population
    count = df.count()
    log_info(spark, f"Loaded {count} rows from {path}")

    return df


def compare_schemas(source_df, target_df):
    """Compare schemas between two dataframes"""
    source_columns = set(source_df.columns)
    target_columns = set(target_df.columns)

    only_in_source = source_columns - target_columns
    only_in_target = target_columns - source_columns
    common_columns = source_columns.intersection(target_columns)

    return {
        'only_in_source': sorted(list(only_in_source)),
        'only_in_target': sorted(list(only_in_target)),
        'common_columns': sorted(list(common_columns))
    }


def compare_data(spark, source_df, target_df, key_columns, exclude_columns):
    """Compare data between source and target dataframes"""
    log_info(spark, "Starting data comparison")

    # Create a join condition based on key columns
    join_condition = None
    for key in key_columns:
        if join_condition is None:
            join_condition = (source_df[key] == target_df[key])
        else:
            join_condition = join_condition & (source_df[key] == target_df[key])

    # Count rows in each dataset
    source_count = source_df.count()
    target_count = target_df.count()

    # Identify rows only in source or target using full outer join and filters
    log_info(spark, "Identifying rows present only in source or target")

    # To limit memory usage, just collect the keys
    source_keys = source_df.select(key_columns).distinct()
    target_keys = target_df.select(key_columns).distinct()

    # Find keys only in source
    keys_only_in_source = source_keys.join(target_keys, key_columns, "left_anti")
    only_in_source_count = keys_only_in_source.count()

    # Find keys only in target
    keys_only_in_target = target_keys.join(source_keys, key_columns, "left_anti")
    only_in_target_count = keys_only_in_target.count()

    # Process common rows in efficient batches to avoid memory issues
    log_info(spark, "Processing common rows to identify value differences")

    # Get common keys
    common_keys = source_keys.join(target_keys, key_columns, "inner")
    common_keys_count = common_keys.count()

    # Process comparison in batches if needed for large datasets
    batch_size = min(100000, common_keys_count)  # Adjust based on available memory

    # Prepare for column-wise comparison
    schema_comparison = compare_schemas(source_df, target_df)
    common_columns = [col for col in schema_comparison['common_columns']
                      if col not in key_columns and col not in exclude_columns]

    # Compare column values for common rows
    column_differences = {}

    # Process in batches if dataset is large
    if common_keys_count > batch_size:
        log_info(spark, f"Processing comparison in batches of {batch_size}")
        common_keys = common_keys.repartition(max(50, spark.sparkContext.defaultParallelism))
        common_keys.cache()

    for column in common_columns:
        log_info(spark, f"Comparing column: {column}")

        # Join and compare specific column
        comparison_expr = when(source_df[column].isNull() & target_df[column].isNull(), True). \
            when(source_df[column].isNull() | target_df[column].isNull(), False). \
            otherwise(source_df[column] == target_df[column])

        source_prefixed = source_df.select([col(c).alias(f"source_{c}") for c in [*key_columns, column]])
        target_prefixed = target_df.select([col(c).alias(f"target_{c}") for c in [*key_columns, column]])

        # Join on key columns
        join_condition = None
        for key in key_columns:
            source_key = f"source_{key}"
            target_key = f"target_{key}"
            if join_condition is None:
                join_condition = (source_prefixed[source_key] == target_prefixed[target_key])
            else:
                join_condition = join_condition & (source_prefixed[source_key] == target_prefixed[target_key])

        joined = source_prefixed.join(target_prefixed, join_condition, "inner")

        # Calculate differences
        diff_count = joined.filter(~((joined[f"source_{column}"].isNull() & joined[f"target_{column}"].isNull()) |
                                     (joined[f"source_{column}"] == joined[f"target_{column}"]))).count()

        if diff_count > 0:
            column_differences[column] = diff_count

    return {
        'source_count': source_count,
        'target_count': target_count,
        'only_in_source_count': only_in_source_count,
        'only_in_target_count': only_in_target_count,
        'common_keys_count': common_keys_count,
        'column_differences': column_differences,
        'keys_only_in_source': keys_only_in_source.limit(100),
        'keys_only_in_target': keys_only_in_target.limit(100)
    }


def generate_html_report(comparison_results, schema_comparison, args):
    """Generate HTML report from comparison results"""
    source_path = args.source_path
    target_path = args.target_path

    # Sample data for differences (limited to protect memory)
    sample_source_only = comparison_results['keys_only_in_source'].limit(10).collect()
    sample_target_only = comparison_results['keys_only_in_target'].limit(10).collect()

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Parquet Comparison Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2, h3 {{ color: #333366; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .summary {{ background-color: #e6f7ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .diff-highlight {{ background-color: #ffcccc; }}
            .match {{ background-color: #ccffcc; }}
        </style>
    </head>
    <body>
        <h1>Parquet Comparison Report</h1>
        <div class="summary">
            <h2>Summary</h2>
            <p><strong>Source:</strong> {source_path}</p>
            <p><strong>Target:</strong> {target_path}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            <p><strong>Total Rows in Source:</strong> {comparison_results['source_count']:,}</p>
            <p><strong>Total Rows in Target:</strong> {comparison_results['target_count']:,}</p>
            <p><strong>Rows Only in Source:</strong> {comparison_results['only_in_source_count']:,}</p>
            <p><strong>Rows Only in Target:</strong> {comparison_results['only_in_target_count']:,}</p>
            <p><strong>Common Rows:</strong> {comparison_results['common_keys_count']:,}</p>
            <p><strong>Columns with Differences:</strong> {len(comparison_results['column_differences'])}</p>
        </div>

        <h2>Schema Comparison</h2>
        <h3>Columns Only in Source ({len(schema_comparison['only_in_source'])})</h3>
        <table>
            <tr><th>Column Name</th></tr>
    """

    for col in schema_comparison['only_in_source']:
        html += f"<tr><td>{col}</td></tr>"

    html += f"""
        </table>

        <h3>Columns Only in Target ({len(schema_comparison['only_in_target'])})</h3>
        <table>
            <tr><th>Column Name</th></tr>
    """

    for col in schema_comparison['only_in_target']:
        html += f"<tr><td>{col}</td></tr>"

    html += f"""
        </table>

        <h2>Column Value Differences</h2>
        <table>
            <tr>
                <th>Column Name</th>
                <th>Differences Count</th>
                <th>Percentage</th>
            </tr>
    """

    # Sort columns by number of differences (descending)
    sorted_diffs = sorted(comparison_results['column_differences'].items(),
                          key=lambda x: x[1], reverse=True)

    for col, diff_count in sorted_diffs:
        percentage = (diff_count / comparison_results['common_keys_count']) * 100 if comparison_results[
                                                                                         'common_keys_count'] > 0 else 0
        html += f"<tr><td>{col}</td><td>{diff_count:,}</td><td>{percentage:.2f}%</td></tr>"

    html += f"""
        </table>

        <h2>Sample Rows Only in Source</h2>
        <table>
            <tr>
    """

    # Add headers for sample rows only in source
    key_columns = args.key_columns.split(",")
    for key in key_columns:
        html += f"<th>{key}</th>"

    html += "</tr>"

    # Add sample rows
    for row in sample_source_only:
        html += "<tr>"
        for key in key_columns:
            html += f"<td>{row[f'source_{key}']}</td>"
        html += "</tr>"

    html += f"""
        </table>

        <h2>Sample Rows Only in Target</h2>
        <table>
            <tr>
    """

    # Add headers for sample rows only in target
    for key in key_columns:
        html += f"<th>{key}</th>"

    html += "</tr>"

    # Add sample rows
    for row in sample_target_only:
        html += "<tr>"
        for key in key_columns:
            html += f"<td>{row[f'target_{key}']}</td>"
        html += "</tr>"

    html += """
        </table>

        <h2>Configuration</h2>
        <table>
            <tr><th>Parameter</th><th>Value</th></tr>
    """

    for key, value in vars(args).items():
        html += f"<tr><td>{key}</td><td>{value}</td></tr>"

    html += """
        </table>
    </body>
    </html>
    """

    return html


def main():
    """Main function to run the comparison"""
    start_time = time.time()

    # Parse command line arguments
    args = parse_arguments()

    # Create and configure Spark session
    spark = create_spark_session(args)

    try:
        # Split comma-separated strings into lists
        key_columns = [col.strip() for col in args.key_columns.split(",")]
        exclude_columns = [col.strip() for col in args.exclude_columns.split(",")] if args.exclude_columns else []

        log_info(spark, f"Starting comparison job with max {args.max_executor_nodes} executors")

        # Load datasets
        source_df = load_and_prepare_data(spark, args.source_path, key_columns, args.sample_size)
        target_df = load_and_prepare_data(spark, args.target_path, key_columns, args.sample_size)

        # Compare schemas
        log_info(spark, "Comparing schemas")
        schema_comparison = compare_schemas(source_df, target_df)

        # Compare data
        comparison_results = compare_data(spark, source_df, target_df, key_columns, exclude_columns)

        # Generate HTML report
        log_info(spark, "Generating HTML report")
        html_report = generate_html_report(comparison_results, schema_comparison, args)

        # Write HTML report to local file first
        local_report_path = "comparison_report.html"
        with open(local_report_path, "w") as f:
            f.write(html_report)

        # Copy to S3
        log_info(spark, f"Writing report to {args.output_path}")

        # Create a single DataFrame with the HTML content to write to S3
        html_df = spark.createDataFrame([("comparison_report.html", html_report)], ["filename", "content"])

        # Save HTML to S3
        html_df.select("content").write.mode("overwrite").text(args.output_path)

        log_info(spark, f"Comparison completed in {time.time() - start_time:.2f} seconds")
        log_info(spark, f"Report available at: {args.output_path}")

    finally:
        # Clean up
        spark.stop()


if __name__ == "__main__":
    """
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      compare_parquet.py \
      --source_path s3://bucket-name/path/to/source/ \
      --target_path s3://bucket-name/path/to/target/ \
      --output_path s3://bucket-name/path/to/output/ \
      --key_columns id,date,customer_id \
      --exclude_columns updated_at,processed_flag \
      --max_executor_nodes 10 \
      --executor_memory 8g \
      --sample_size 0.5
    """
    main()

