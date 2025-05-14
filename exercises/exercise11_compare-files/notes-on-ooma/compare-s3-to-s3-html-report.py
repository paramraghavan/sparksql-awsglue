from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, md5, concat_ws, count, sum as spark_sum, broadcast
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import logging
import os
import sys
import time
from datetime import datetime
import pandas as pd
import jinja2
import boto3
from io import StringIO

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Compare two datasets in S3 using PySpark')
    parser.add_argument('--dat-file', type=str, required=True,
                        help='Path to input DAT/CSV/Parquet file (e.g., s3://bucket/path/file.dat)')
    parser.add_argument('--snowflake-data', type=str, required=True,
                        help='Path to the Snowflake data in S3 (e.g., s3://bucket/snowflake_data/)')
    parser.add_argument('--file-type', type=str, choices=['dat', 'csv', 'parquet'], default='dat',
                        help='Format of the input file')
    parser.add_argument('--snowflake-format', type=str, choices=['csv', 'parquet'], default='parquet',
                        help='Format of the Snowflake data in S3')
    parser.add_argument('--delimiter', type=str, default='|', help='Delimiter used in DAT/CSV file')
    parser.add_argument('--key-columns', type=str, required=True,
                        help='Comma-separated list of column names to use as keys for comparison')
    parser.add_argument('--column-names', type=str,
                        help='Comma-separated list of column names for the DAT file (if no header)')
    parser.add_argument('--output-dir', type=str, default='s3://your-bucket/comparison_results',
                        help='Directory to store comparison results')
    parser.add_argument('--sample-fraction', type=float, default=1.0,
                        help='Fraction of records to sample for testing (e.g., 0.1 for 10%)')
    parser.add_argument('--num-partitions', type=int, default=200,
                        help='Number of partitions to use for processing')
    parser.add_argument('--column-batch-size', type=int, default=10,
                        help='Number of columns to process in each batch')
    return parser.parse_args()


def create_output_directory(output_dir):
    """Handle S3 output directory path."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # For S3 paths, just append the timestamp to the path
    if output_dir.startswith('s3://'):
        if output_dir.endswith('/'):
            result_dir = f"{output_dir}comparison_{timestamp}"
        else:
            result_dir = f"{output_dir}/comparison_{timestamp}"
    else:
        # Local directory handling
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        result_dir = os.path.join(output_dir, f"comparison_{timestamp}")
        os.makedirs(result_dir)

    return result_dir


def read_snowflake_data_from_s3(spark, path, format_type, num_partitions):
    """Read Snowflake data that was exported to S3."""
    logger.info(f"Reading Snowflake data from S3: {path}, format: {format_type}")

    if format_type == 'parquet':
        df = spark.read.parquet(path)
    elif format_type == 'csv':
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    else:
        raise ValueError(f"Unsupported format for Snowflake data: {format_type}")

    # Repartition to ensure even distribution
    return df.repartition(num_partitions)


def read_file_to_spark(spark, file_path, file_type, delimiter, num_partitions, column_names=None):
    """Read input file into Spark DataFrame."""
    logger.info(f"Reading {file_type} file: {file_path}")

    if file_type == 'parquet':
        df = spark.read.parquet(file_path)
    else:  # csv or dat
        # Set up options for CSV/DAT reading
        options = {"delimiter": delimiter}

        if column_names:
            # If column names are provided (for headerless files)
            cols = [col.strip() for col in column_names.split(',')]
            df = spark.read.option("header", "false").options(**options).csv(file_path)

            # Rename columns
            for i, col_name in enumerate(cols):
                df = df.withColumnRenamed(f"_c{i}", col_name)
        else:
            # Assume file has headers
            df = spark.read.option("header", "true").options(**options).csv(file_path)

    # Repartition to ensure even distribution
    return df.repartition(num_partitions)


def compare_record_counts(df1, df2, name1, name2):
    """Compare record counts between DataFrames."""
    count1 = df1.count()
    count2 = df2.count()

    logger.info(f"{name1} record count: {count1}")
    logger.info(f"{name2} record count: {count2}")

    if count1 == count2:
        logger.info("Record counts match")
    else:
        logger.info(f"Record count mismatch: {abs(count1 - count2)} records difference")

    return count1, count2


def find_key_differences(df1, df2, key_columns, name1, name2):
    """Find records that exist in one DataFrame but not the other based on keys."""
    # Create a composite key column for easier comparison
    logger.info("Creating composite keys for comparison")
    df1_with_key = df1.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))
    df2_with_key = df2.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))

    # Select only the keys to reduce memory pressure
    logger.info("Extracting key columns only")
    df1_keys = df1_with_key.select("composite_key")
    df2_keys = df2_with_key.select("composite_key")

    # Persist keys since we'll use them multiple times
    logger.info("Persisting key DataFrames")
    df1_keys.persist()
    df2_keys.persist()

    # Find keys only in df1 using subtract (more memory efficient than left_anti join)
    logger.info("Finding keys only in first dataset")
    keys_only_in_df1 = df1_keys.subtract(df2_keys)
    keys_only_in_df1.persist()
    count_only_in_df1 = keys_only_in_df1.count()

    # Find keys only in df2
    logger.info("Finding keys only in second dataset")
    keys_only_in_df2 = df2_keys.subtract(df1_keys)
    keys_only_in_df2.persist()
    count_only_in_df2 = keys_only_in_df2.count()

    # Log results
    logger.info(f"Records only in {name1}: {count_only_in_df1}")
    logger.info(f"Records only in {name2}: {count_only_in_df2}")

    # Join back to get full records (if needed)
    logger.info("Getting full records for keys only in first dataset")
    only_in_df1 = df1_with_key.join(keys_only_in_df1, "composite_key").drop("composite_key")

    logger.info("Getting full records for keys only in second dataset")
    only_in_df2 = df2_with_key.join(keys_only_in_df2, "composite_key").drop("composite_key")

    # Clean up
    df1_keys.unpersist()
    df2_keys.unpersist()
    keys_only_in_df1.unpersist()
    keys_only_in_df2.unpersist()

    return only_in_df1, only_in_df2, count_only_in_df1, count_only_in_df2


def compare_column_values(df1, df2, key_columns, name1, name2, column_batch_size=10):
    """Compare column values for records that exist in both DataFrames using a memory-efficient approach."""
    # Create a composite key column for the join
    logger.info("Creating composite keys for value comparison")
    df1_with_key = df1.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))
    df2_with_key = df2.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))

    # Get the list of common columns for comparison
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))
    common_columns = [c for c in common_columns if c not in key_columns]
    logger.info(f"Found {len(common_columns)} common columns for comparison")

    # Get only the keys for each DataFrame
    logger.info("Extracting key columns only")
    df1_keys = df1_with_key.select("composite_key")
    df2_keys = df2_with_key.select("composite_key")

    # Find common keys - these are the records we need to compare
    logger.info("Finding common keys between datasets")
    common_keys = df1_keys.intersect(df2_keys)

    # Cache this as we'll use it multiple times
    common_keys.persist()
    common_key_count = common_keys.count()
    logger.info(f"Found {common_key_count} records with common keys")

    # Initialize a schema for the differences DataFrame
    spark_session = df1.sparkSession
    diff_schema = StructType([
        StructField("composite_key", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField(f"{name1}_value", StringType(), True),
        StructField(f"{name2}_value", StringType(), True)
    ])

    # Create an empty DataFrame for differences
    diff_data = spark_session.createDataFrame([], diff_schema)

    # Track keys with differences to count unique records with differences
    diff_keys = set()

    # Process columns in batches to reduce memory pressure
    column_batches = [common_columns[i:i + column_batch_size] for i in range(0, len(common_columns), column_batch_size)]

    for batch_idx, column_batch in enumerate(column_batches):
        logger.info(f"Processing column batch {batch_idx + 1}/{len(column_batches)}: {column_batch}")

        # Prepare batch of columns to compare
        df1_batch_cols = ["composite_key"] + column_batch
        df2_batch_cols = ["composite_key"] + column_batch

        # Select only the needed columns for this batch
        df1_batch = df1_with_key.select(*df1_batch_cols)
        df2_batch = df2_with_key.select(*df2_batch_cols)

        # Join with common keys to reduce data size
        df1_common = df1_batch.join(common_keys, "composite_key")
        df2_common = df2_batch.join(common_keys, "composite_key")

        # To avoid confusion with column names in the join, rename df2 columns
        for c in column_batch:
            df2_common = df2_common.withColumnRenamed(c, f"{c}_df2")

        # Join the datasets on composite key
        joined = df1_common.join(df2_common, "composite_key")

        # For each column in this batch, check for differences
        batch_diff_data = None

        for column_name in column_batch:
            logger.info(f"Comparing column: {column_name}")

            # Create a comparison condition
            col1 = col(column_name)
            col2 = col(f"{column_name}_df2")

            # Find differences for this column
            comparison = joined.filter(
                (col1.isNull() & col2.isNotNull()) |
                (col1.isNotNull() & col2.isNull()) |
                ((col1 != col2) & col1.isNotNull() & col2.isNotNull())
            ).select(
                col("composite_key"),
                lit(column_name).alias("column_name"),
                col1.cast("string").alias(f"{name1}_value"),
                col2.cast("string").alias(f"{name2}_value")
            )

            # Count differences for this column
            diff_count_col = comparison.count()
            logger.info(f"Found {diff_count_col} differences in column {column_name}")

            if diff_count_col > 0:
                # Collect keys with differences for this column (memory-efficient approach)
                for row in comparison.select("composite_key").distinct().collect():
                    diff_keys.add(row.composite_key)

                # Add differences to batch results
                if batch_diff_data is None:
                    batch_diff_data = comparison
                else:
                    batch_diff_data = batch_diff_data.union(comparison)

        # Add batch differences to overall results
        if batch_diff_data is not None and batch_diff_data.count() > 0:
            diff_data = diff_data.union(batch_diff_data)

        # Clean up to free memory
        joined.unpersist() if hasattr(joined, "unpersist") else None

    # Count total records with differences
    diff_count = len(diff_keys)
    logger.info(f"Total records with differences: {diff_count}")

    # Clean up
    common_keys.unpersist()

    return diff_data, diff_count


def create_column_diff_summary(diff_data, diff_count, name1, name2):
    """Create a summary of differences by column."""
    if diff_data.count() == 0:
        return diff_data.sparkSession.createDataFrame(
            [("No differences found", 0, 0)],
            ["column_name", "diff_count", "percentage"]
        )

    # Count differences by column
    summary = diff_data.groupBy("column_name").agg(
        count("*").alias("diff_count")
    )

    # Calculate percentage
    if diff_count > 0:
        summary = summary.withColumn(
            "percentage",
            (col("diff_count") * 100 / diff_count).cast("double")
        )
    else:
        summary = summary.withColumn("percentage", lit(0.0))

    # Sort by count in descending order
    summary = summary.orderBy(col("diff_count").desc())

    return summary


def write_results(result_dir, only_in_df1, only_in_df2, diff_data, column_diff_summary, name1, name2):
    """Write comparison results to files in a memory-efficient way."""
    # Write records only in first source
    logger.info(f"Writing records only in {name1}")
    only_in_df1_path = f"{result_dir}/only_in_{name1}.parquet"
    only_in_df1.write.mode("overwrite").parquet(only_in_df1_path)

    # Write records only in second source
    logger.info(f"Writing records only in {name2}")
    only_in_df2_path = f"{result_dir}/only_in_{name2}.parquet"
    only_in_df2.write.mode("overwrite").parquet(only_in_df2_path)

    # Write detailed differences - using smaller chunks for large datasets
    logger.info("Writing detailed differences")
    diff_count = diff_data.count()

    if diff_count > 1000000:  # For very large difference sets
        logger.info(f"Large difference set detected ({diff_count} rows), writing in chunks")
        diff_data_path = f"{result_dir}/detailed_differences"
        diff_data.write.mode("overwrite").partitionBy("column_name").parquet(diff_data_path)

        # Write a sample to CSV for easy viewing
        logger.info("Writing sample of differences to CSV")
        diff_sample = diff_data.limit(1000)
        diff_sample.write.mode("overwrite").option("header", "true").csv(
            f"{result_dir}/detailed_differences_sample.csv"
        )
    else:
        # For smaller sets, write as normal
        diff_data_path = f"{result_dir}/detailed_differences.parquet"
        diff_data.write.mode("overwrite").parquet(diff_data_path)

        # Also save as CSV for easier viewing
        diff_data.write.mode("overwrite").option("header", "true").csv(
            f"{result_dir}/detailed_differences.csv"
        )

    # Write column difference summary
    logger.info("Writing column difference summary")
    summary_path = f"{result_dir}/column_diff_summary.csv"
    column_diff_summary.write.mode("overwrite").option("header", "true").csv(summary_path)


def main():
    # Parse arguments
    args = parse_args()

    # Initialize Spark session with optimized configuration
    spark = SparkSession.builder \
        .appName("S3 File Comparison") \
        .config("spark.sql.broadcastTimeout", "3600") \
        .config("spark.sql.shuffle.partitions", str(args.num_partitions)) \
        .config("spark.default.parallelism", str(args.num_partitions)) \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.autoBroadcastJoinThreshold", "100mb") \
        .config("spark.cleaner.periodicGC.interval", "15min") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()

    # Set checkpoint directory for operations requiring it
    checkpoint_dir = "s3://your-bucket/checkpoint-dir"
    if args.output_dir.startswith("s3://"):
        # Extract bucket from output_dir
        bucket = args.output_dir.split("/")[2]
        checkpoint_dir = f"s3://{bucket}/spark-checkpoints"

    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    try:
        # Create output directory
        result_dir = create_output_directory(args.output_dir)

        # Read the Snowflake data from S3
        sf_df = read_snowflake_data_from_s3(
            spark, args.snowflake_data, args.snowflake_format, args.num_partitions
        )

        # Apply sampling if requested
        if args.sample_fraction < 1.0:
            logger.info(f"Sampling data at {args.sample_fraction * 100}% for testing")
            sf_df = sf_df.sample(args.sample_fraction)

        # Get column names from Snowflake data for the DAT file if none provided
        column_names = args.column_names
        if not column_names and args.file_type in ['dat', 'csv']:
            # Use Snowflake column names if we're reading a headerless file
            column_names = ','.join(sf_df.columns)
            logger.info(f"Using column names from Snowflake data: {column_names}")

        # Read the input file
        file_df = read_file_to_spark(
            spark, args.dat_file, args.file_type,
            args.delimiter, args.num_partitions, column_names
        )

        # Apply sampling if requested
        if args.sample_fraction < 1.0:
            file_df = file_df.sample(args.sample_fraction)

        # Parse key columns
        key_columns = [col.strip() for col in args.key_columns.split(',')]

        # Make sure key columns exist in both DataFrames
        for key_col in key_columns:
            if key_col not in file_df.columns:
                raise ValueError(f"Key column '{key_col}' not found in file DataFrame")
            if key_col not in sf_df.columns:
                raise ValueError(f"Key column '{key_col}' not found in Snowflake DataFrame")

        # Persist DataFrames as we'll use them multiple times
        logger.info("Persisting input DataFrames")
        file_df.persist()
        sf_df.persist()

        # Compare record counts
        file_count, sf_count = compare_record_counts(file_df, sf_df, "File", "Snowflake")

        # Find key differences
        only_in_file, only_in_sf, only_in_file_count, only_in_sf_count = find_key_differences(
            file_df, sf_df, key_columns, "File", "Snowflake"
        )

        # Compare column values (memory-efficient version)
        diff_data, diff_count = compare_column_values(
            file_df, sf_df, key_columns, "File", "Snowflake", args.column_batch_size
        )

        # Create column difference summary
        column_diff_summary = create_column_diff_summary(
            diff_data, diff_count, "File", "Snowflake"
        )

        # Write results to files
        write_results(
            result_dir, only_in_file, only_in_sf,
            diff_data, column_diff_summary, "File", "Snowflake"
        )

        # Generate HTML report
        report_path = generate_html_report(
            result_dir, file_count, sf_count,
            only_in_file_count, only_in_sf_count, diff_count,
            column_diff_summary, "File", "Snowflake"
        )

        logger.info(f"Comparison completed. Results saved to {result_dir}")
        logger.info(f"HTML Report available at: {report_path}")

        # Clean up persisted DataFrames
        file_df.unpersist()
        sf_df.unpersist()

    except Exception as e:
        logger.error(f"Error during comparison: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


def get_html_template():
    """Define the HTML template for the report."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Comparison Report</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        }
        h1 {
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }
        h2 {
            color: #2980b9;
            margin-top: 30px;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }
        .summary-box {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin: 20px 0;
        }
        .summary-card {
            flex: 1;
            min-width: 200px;
            background-color: #f8f9fa;
            border-left: 4px solid #3498db;
            padding: 15px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }
        .summary-card.warning {
            border-left-color: #e74c3c;
        }
        .summary-card.success {
            border-left-color: #2ecc71;
        }
        .card-title {
            font-weight: bold;
            margin-bottom: 5px;
            color: #555;
        }
        .card-value {
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
        }
        .card-description {
            margin-top: 5px;
            font-size: 12px;
            color: #7f8c8d;
        }
        table {
            width: 100%;
            margin: 20px 0;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
            color: #333;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .progress-container {
            height: 24px;
            background-color: #ecf0f1;
            border-radius: 4px;
            position: relative;
            overflow: hidden;
            margin-top: 8px;
        }
        .progress-bar {
            height: 100%;
            background-color: #3498db;
            border-radius: 4px;
            transition: width 0.5s ease;
        }
        .progress-bar.red {
            background-color: #e74c3c;
        }
        .progress-bar.yellow {
            background-color: #f1c40f;
        }
        .progress-bar.green {
            background-color: #2ecc71;
        }
        .progress-text {
            position: absolute;
            right: 10px;
            top: 50%;
            transform: translateY(-50%);
            color: #fff;
            font-weight: bold;
            text-shadow: 1px 1px 1px rgba(0, 0, 0, 0.3);
        }
        .footer {
            margin-top: 40px;
            border-top: 1px solid #eee;
            padding-top: 20px;
            color: #7f8c8d;
            font-size: 14px;
        }
        .chart-container {
            margin: 30px 0;
            height: 300px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Data Comparison Report</h1>
        <p>Comparison between {{ name1 }} and {{ name2 }}</p>
        <p><strong>Report Generated:</strong> {{ timestamp }}</p>

        <h2>Overview</h2>
        <div class="summary-box">
            <div class="summary-card">
                <div class="card-title">{{ name1 }} Records</div>
                <div class="card-value">{{ df1_count }}</div>
            </div>
            <div class="summary-card">
                <div class="card-title">{{ name2 }} Records</div>
                <div class="card-value">{{ df2_count }}</div>
            </div>
            <div class="summary-card {{ 'warning' if only_in_df1_count > 0 else 'success' }}">
                <div class="card-title">Only in {{ name1 }}</div>
                <div class="card-value">{{ only_in_df1_count }}</div>
                <div class="card-description">{{ "{:.2f}%".format(only_in_df1_count / df1_count * 100) if df1_count > 0 else "0.00%" }}</div>
            </div>
            <div class="summary-card {{ 'warning' if only_in_df2_count > 0 else 'success' }}">
                <div class="card-title">Only in {{ name2 }}</div>
                <div class="card-value">{{ only_in_df2_count }}</div>
                <div class="card-description">{{ "{:.2f}%".format(only_in_df2_count / df2_count * 100) if df2_count > 0 else "0.00%" }}</div>
            </div>
            <div class="summary-card {{ 'warning' if diff_count > 0 else 'success' }}">
                <div class="card-title">Records with Differences</div>
                <div class="card-value">{{ diff_count }}</div>
                <div class="card-description">{{ "{:.2f}%".format(diff_count / min(df1_count, df2_count) * 100) if min(df1_count, df2_count) > 0 else "0.00%" }}</div>
            </div>
            <div class="summary-card success">
                <div class="card-title">Exact Matches</div>
                <div class="card-value">{{ matching_count }}</div>
                <div class="card-description">{{ "{:.2f}%".format(match_percent) }}</div>
            </div>
        </div>

        <h2>Match Summary</h2>
        <div class="progress-container">
            <div class="progress-bar {{ 'red' if match_percent < 70 else ('yellow' if match_percent < 95 else 'green') }}" style="width: {{ match_percent }}%">
                <span class="progress-text">{{ "{:.2f}%".format(match_percent) }}</span>
            </div>
        </div>

        <h2>Column Differences</h2>
        {% if column_diff_summary %}
            <table>
                <thead>
                    <tr>
                        <th>Column Name</th>
                        <th>Difference Count</th>
                        <th>Percentage</th>
                        <th>Visualization</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in column_diff_summary %}
                        <tr>
                            <td>{{ row.column_name }}</td>
                            <td>{{ row.diff_count }}</td>
                            <td>{{ "{:.2f}%".format(row.percentage) }}</td>
                            <td>
                                <div class="progress-container" style="height: 15px;">
                                    <div class="progress-bar {{ 'red' if row.percentage > 10 else ('yellow' if row.percentage > 5 else 'green') }}" style="width: {{ min(row.percentage * 5, 100) }}%"></div>
                                </div>
                            </td>
                        </tr>
                    {% endfor %}
                </tbody>
            </table>
        {% else %}
            <p>No column differences found in matching records.</p>
        {% endif %}

        <h2>Output Files</h2>
        <ul>
            <li>Records only in {{ name1 }}: <code>only_in_{{ name1 }}.parquet</code></li>
            <li>Records only in {{ name2 }}: <code>only_in_{{ name2 }}.parquet</code></li>
            <li>Detailed differences: <code>detailed_differences.parquet</code>, <code>detailed_differences.csv</code></li>
            <li>Column difference summary: <code>column_diff_summary.csv</code></li>
            <li>This HTML report: <code>comparison_report.html</code></li>
        </ul>

        <div class="footer">
            <p>Generated by Data Comparison Tool | {{ timestamp }}</p>
        </div>
    </div>
</body>
</html>"""


def generate_html_report(result_dir, df1_count, df2_count, only_in_df1_count, only_in_df2_count,
                         diff_count, column_diff_summary_df, name1, name2):
    """Generate an HTML report with memory-efficient handling of large datasets."""
    logger.info("Generating HTML report")

    # Calculate matching count and percentage
    matching_count = min(df1_count - only_in_df1_count, df2_count - only_in_df2_count) - diff_count
    matching_count = max(0, matching_count)  # Ensure it's not negative
    match_percent = (matching_count / min(df1_count, df2_count) * 100) if min(df1_count, df2_count) > 0 else 0

    # Get the Jinja2 template
    template_str = get_html_template()
    template = jinja2.Template(template_str)

    # For large datasets, limit the summary rows to avoid memory issues
    if column_diff_summary_df.count() > 100:
        logger.info("Large number of column differences detected, limiting to top 100 for report")
        column_diff_summary_df = column_diff_summary_df.limit(100)

    # Convert the column_diff_summary to a list of dictionaries for easier template rendering
    if column_diff_summary_df.count() > 0 and "No differences found" not in [row.column_name for row in
                                                                             column_diff_summary_df.collect()]:
        summary_rows = [row.asDict() for row in column_diff_summary_df.collect()]
    else:
        summary_rows = []

    # Render the template with the data
    html_output = template.render(
        name1=name1,
        name2=name2,
        df1_count=df1_count,
        df2_count=df2_count,
        only_in_df1_count=only_in_df1_count,
        only_in_df2_count=only_in_df2_count,
        diff_count=diff_count,
        matching_count=matching_count,
        match_percent=match_percent,
        column_diff_summary=summary_rows,
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

    # Write the HTML to a file or S3
    if result_dir.startswith("s3://"):
        # For S3, we need to parse the bucket and key
        s3_parts = result_dir.replace("s3://", "").split("/")
        bucket_name = s3_parts[0]
        key_prefix = "/".join(s3_parts[1:])

        # Initialize S3 client
        s3_client = boto3.client('s3')

        # Upload the HTML file to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{key_prefix}/comparison_report.html",
            Body=html_output,
            ContentType='text/html'
        )

        logger.info(f"HTML report uploaded to s3://{bucket_name}/{key_prefix}/comparison_report.html")
        return f"s3://{bucket_name}/{key_prefix}/comparison_report.html"
    else:
        # For local file system
        with open(f"{result_dir}/comparison_report.html", "w") as f:
            f.write(html_output)

        logger.info(f"HTML report saved to {result_dir}/comparison_report.html")
        return f"{result_dir}/comparison_report.html"


if __name__ == "__main__":
    main()

"""
Usage example for large datasets:

spark-submit compare_datasets.py \
    --dat-file s3://your-bucket/input.dat \
    --snowflake-data s3://your-bucket/snowflake_data/ \
    --key-columns "id,date" \
    --num-partitions 400 \
    --column-batch-size 5

For testing with a sample first:

spark-submit compare_datasets.py \
    --dat-file s3://your-bucket/input.dat \
    --snowflake-data s3://your-bucket/snowflake_data/ \
    --key-columns "id,date" \
    --sample-fraction 0.01
"""