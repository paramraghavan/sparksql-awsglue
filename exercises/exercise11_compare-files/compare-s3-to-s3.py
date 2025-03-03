from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, md5, concat_ws, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import logging
import os
import sys
import time
from datetime import datetime

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


def read_snowflake_data_from_s3(spark, path, format_type):
    """Read Snowflake data that was exported to S3."""
    logger.info(f"Reading Snowflake data from S3: {path}, format: {format_type}")

    if format_type == 'parquet':
        return spark.read.parquet(path)
    elif format_type == 'csv':
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    else:
        raise ValueError(f"Unsupported format for Snowflake data: {format_type}")


def read_file_to_spark(spark, file_path, file_type, delimiter, column_names=None):
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

    # Return the DataFrame with properly named columns
    return df


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
    df1_with_key = df1.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))
    df2_with_key = df2.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))

    # Find keys only in df1
    keys_only_in_df1 = df1_with_key.select("composite_key").subtract(df2_with_key.select("composite_key"))
    only_in_df1 = df1_with_key.join(keys_only_in_df1, "composite_key").drop("composite_key")

    # Find keys only in df2
    keys_only_in_df2 = df2_with_key.select("composite_key").subtract(df1_with_key.select("composite_key"))
    only_in_df2 = df2_with_key.join(keys_only_in_df2, "composite_key").drop("composite_key")

    # Log results
    count_only_in_df1 = only_in_df1.count()
    count_only_in_df2 = only_in_df2.count()

    logger.info(f"Records only in {name1}: {count_only_in_df1}")
    logger.info(f"Records only in {name2}: {count_only_in_df2}")

    return only_in_df1, only_in_df2, count_only_in_df1, count_only_in_df2


def compare_column_values(df1, df2, key_columns, name1, name2):
    """Compare column values for records that exist in both DataFrames."""
    # Create a composite key column for the join
    df1_with_key = df1.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))
    df2_with_key = df2.withColumn("composite_key", concat_ws("~", *[col(c) for c in key_columns]))

    # Get the list of common columns for comparison
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))
    logger.info(f"Found {len(common_columns)} common columns for comparison")

    # Join DataFrames based on composite key
    joined_df = df1_with_key.join(df2_with_key, "composite_key", "inner")

    # Initialize a DataFrames to collect differences
    spark_session = df1.sparkSession
    diff_data = None

    # Create a schema for the differences DataFrame
    diff_schema = StructType([
        StructField("composite_key", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField(f"{name1}_value", StringType(), True),
        StructField(f"{name2}_value", StringType(), True)
    ])

    # Find records with any differences
    logger.info("Looking for records with column value differences")

    # Create conditions to identify rows with differences in any column
    diff_conditions = []

    for col_name in common_columns:
        # Skip the composite key column
        if col_name == "composite_key":
            continue

        # Create condition for this column
        col1 = col(f"{name1}.{col_name}")
        col2 = col(f"{name2}.{col_name}")

        # Add null-safe comparison
        diff_conditions.append(
            (col1.isNull() & col2.isNotNull()) |
            (col1.isNotNull() & col2.isNull()) |
            ((col1 != col2) & col1.isNotNull() & col2.isNotNull())
        )

    # Combine all conditions with OR
    from functools import reduce
    from pyspark.sql.functions import expr

    if diff_conditions:
        diff_condition = reduce(lambda a, b: a | b, diff_conditions)

        # Filter rows with differences
        different_rows = joined_df.filter(diff_condition)

        # Count records with differences
        diff_count = different_rows.count()
        logger.info(f"Found {diff_count} records with differences")

        if diff_count > 0:
            # Process each column to extract specific differences
            for col_name in common_columns:
                # Skip the composite key column
                if col_name == "composite_key":
                    continue

                # Create a DataFrame with just the differences for this column
                col_diff = joined_df.filter(
                    (col(f"{name1}.{col_name}").isNull() & col(f"{name2}.{col_name}").isNotNull()) |
                    (col(f"{name1}.{col_name}").isNotNull() & col(f"{name2}.{col_name}").isNull()) |
                    ((col(f"{name1}.{col_name}") != col(f"{name2}.{col_name}")) &
                     col(f"{name1}.{col_name}").isNotNull() &
                     col(f"{name2}.{col_name}").isNotNull())
                ).select(
                    col("composite_key"),
                    lit(col_name).alias("column_name"),
                    col(f"{name1}.{col_name}").cast("string").alias(f"{name1}_value"),
                    col(f"{name2}.{col_name}").cast("string").alias(f"{name2}_value")
                )

                # Append to main differences DataFrame
                if diff_data is None:
                    diff_data = col_diff
                else:
                    diff_data = diff_data.union(col_diff)
        else:
            # Create an empty DataFrame for no differences
            diff_data = spark_session.createDataFrame([], diff_schema)
    else:
        # Create an empty DataFrame if no columns to compare
        diff_data = spark_session.createDataFrame([], diff_schema)
        diff_count = 0

    return diff_data, diff_count


def create_column_diff_summary(diff_data, total_records, name1, name2):
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
    if total_records > 0:
        summary = summary.withColumn(
            "percentage",
            (col("diff_count") * 100 / total_records).cast("double")
        )
    else:
        summary = summary.withColumn("percentage", lit(0.0))

    # Sort by count in descending order
    summary = summary.orderBy(col("diff_count").desc())

    return summary


def write_results(result_dir, only_in_df1, only_in_df2, diff_data, column_diff_summary, name1, name2):
    """Write comparison results to files."""
    # Write records only in first source
    logger.info(f"Writing records only in {name1}")
    only_in_df1_path = f"{result_dir}/only_in_{name1}.parquet"
    only_in_df1.write.mode("overwrite").parquet(only_in_df1_path)

    # Write records only in second source
    logger.info(f"Writing records only in {name2}")
    only_in_df2_path = f"{result_dir}/only_in_{name2}.parquet"
    only_in_df2.write.mode("overwrite").parquet(only_in_df2_path)

    # Write detailed differences
    logger.info("Writing detailed differences")
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

    # Write a simple text summary for quick viewing
    summary_text = column_diff_summary.toPandas()

    if not result_dir.startswith("s3://"):
        # Only try to write local file if not on S3
        with open(f"{result_dir}/column_summary.txt", "w") as f:
            f.write(f"Column Difference Summary:\n")
            f.write(f"------------------------\n")
            for _, row in summary_text.iterrows():
                f.write(f"{row['column_name']}: {row['diff_count']} differences ({row['percentage']:.2f}%)\n")


def generate_report(spark, result_dir, df1_count, df2_count, only_in_df1_count, only_in_df2_count,
                    diff_count, column_diff_summary, name1, name2):
    """Generate a comprehensive report."""
    # Convert the summary to pandas for easier reporting
    summary_pdf = column_diff_summary.toPandas()

    # Create report lines
    report_lines = [
        "DATA COMPARISON REPORT",
        "=====================\n",
        f"Comparison between {name1} and {name2}\n",
        "RECORD COUNT SUMMARY",
        "-------------------",
        f"Total records in {name1}: {df1_count}",
        f"Total records in {name2}: {df2_count}",
        f"Records only in {name1}: {only_in_df1_count}",
        f"Records only in {name2}: {only_in_df2_count}",
        f"Records with differences: {diff_count}"
    ]

    matching_count = df1_count - only_in_df1_count - diff_count
    match_percent = (matching_count / df1_count * 100) if df1_count > 0 else 0

    report_lines.extend([
        f"Completely matching records: {matching_count} ({match_percent:.2f}%)\n",
        "COLUMN DIFFERENCE SUMMARY",
        "-----------------------"
    ])

    if len(summary_pdf) > 0 and "No differences found" not in summary_pdf["column_name"].values:
        report_lines.append(f"{'Column Name':<30} {'Difference Count':<15} {'Percentage':<10}")
        report_lines.append(f"{'-' * 30} {'-' * 15} {'-' * 10}")

        for _, row in summary_pdf.iterrows():
            report_lines.append(f"{row['column_name']:<30} {row['diff_count']:<15} {row['percentage']:.2f}%")
    else:
        report_lines.append("No column differences found in matching records.")

    report_lines.extend([
        "\nOUTPUT FILES",
        "-----------",
        f"Records only in {name1}: only_in_{name1}.parquet",
        f"Records only in {name2}: only_in_{name2}.parquet",
        "Detailed differences: detailed_differences.parquet, detailed_differences.csv",
        "Column difference summary: column_diff_summary.csv"
    ])

    # Convert report to DataFrame for saving to S3
    report_df = spark.createDataFrame([(line,) for line in report_lines], ["line"])
    report_df.coalesce(1).write.mode("overwrite").text(f"{result_dir}/comparison_report")

    logger.info("Report generated successfully")

def convert_date_column_data_type_to_str(df):
    # Alternative approach using a single expression
    df_converted = df
    for column_name in df.columns:
        if column_name.endswith("DT") or column_name.endswith("DATE"):
            df_converted = df_converted.withColumn(column_name, col(column_name).cast("string"))

    return df_converted

def main():
    # Parse arguments
    args = parse_args()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("S3 File Comparison") \
        .config("spark.sql.broadcastTimeout", "3600") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    try:
        # Create output directory
        result_dir = create_output_directory(args.output_dir)

        # Read the Snowflake data from S3
        sf_df = read_snowflake_data_from_s3(
            spark, args.snowflake_data, args.snowflake_format
        )

        # Get column names from Snowflake data for the DAT file if none provided
        column_names = args.column_names
        if not column_names and args.file_type in ['dat', 'csv']:
            # Use Snowflake column names if we're reading a headerless file
            column_names = ','.join(sf_df.columns)
            logger.info(f"Using column names from Snowflake data: {column_names}")

        # Read the input file
        file_df = read_file_to_spark(
            spark, args.dat_file, args.file_type,
            args.delimiter, column_names
        )

        # Parse key columns
        key_columns = [col.strip() for col in args.key_columns.split(',')]

        # Make sure key columns exist in both DataFrames
        for key_col in key_columns:
            if key_col not in file_df.columns:
                raise ValueError(f"Key column '{key_col}' not found in file DataFrame")
            if key_col not in sf_df.columns:
                raise ValueError(f"Key column '{key_col}' not found in Snowflake DataFrame")

        # Compare record counts
        file_count, sf_count = compare_record_counts(file_df, sf_df, "File", "Snowflake")

        # Find key differences
        only_in_file, only_in_sf, only_in_file_count, only_in_sf_count = find_key_differences(
            file_df, sf_df, key_columns, "File", "Snowflake"
        )

        # Compare column values
        diff_data, diff_count = compare_column_values(
            file_df, sf_df, key_columns, "File", "Snowflake"
        )

        # Create column difference summary
        matching_record_count = min(file_count, sf_count) - diff_count
        column_diff_summary = create_column_diff_summary(
            diff_data, matching_record_count, "File", "Snowflake"
        )

        # Write results to files
        write_results(
            result_dir, only_in_file, only_in_sf,
            diff_data, column_diff_summary, "File", "Snowflake"
        )

        # Generate final report
        generate_report(
            spark, result_dir, file_count, sf_count,
            only_in_file_count, only_in_sf_count, diff_count,
            column_diff_summary, "File", "Snowflake"
        )

        logger.info(f"Comparison completed. Results saved to {result_dir}")

    except Exception as e:
        logger.error(f"Error during comparison: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
