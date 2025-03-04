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
    from pyspark.sql.functions import col, concat_ws, lit
    from pyspark.sql.types import StructType, StructField, StringType
    from functools import reduce

    # Create composite keys with different names to avoid ambiguity
    df1_with_key = df1.withColumn(f"{name1}_composite_key", concat_ws("~", *[col(c) for c in key_columns]))
    df2_with_key = df2.withColumn(f"{name2}_composite_key", concat_ws("~", *[col(c) for c in key_columns]))

    # Get common columns for comparison
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))
    print(f"Found {len(common_columns)} common columns for comparison")

    # Rename all columns except the key columns to avoid ambiguity
    for column in df1.columns:
        if column not in key_columns:
            df1_with_key = df1_with_key.withColumnRenamed(column, f"{name1}_{column}")

    for column in df2.columns:
        if column not in key_columns:
            df2_with_key = df2_with_key.withColumnRenamed(column, f"{name2}_{column}")

    # Join the DataFrames on the composite keys
    joined_df = df1_with_key.join(
        df2_with_key,
        df1_with_key[f"{name1}_composite_key"] == df2_with_key[f"{name2}_composite_key"],
        "inner"
    )

    # Save one version of the composite key for results
    joined_df = joined_df.withColumn("composite_key", df1_with_key[f"{name1}_composite_key"])

    # Initialize differences DataFrame
    spark_session = df1.sparkSession
    diff_data = None

    # Create schema for results
    diff_schema = StructType([
        StructField("composite_key", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField(f"{name1}_value", StringType(), True),
        StructField(f"{name2}_value", StringType(), True)
    ])

    # Build list of differences for each common column
    all_diffs = []

    for column in common_columns:
        # Skip key columns
        if column in key_columns:
            continue

        col1_name = f"{name1}_{column}"
        col2_name = f"{name2}_{column}"

        # Create a separate DataFrame for each column with differences
        col_diff = joined_df.filter(
            (col(col1_name).isNull() & col(col2_name).isNotNull()) |
            (col(col1_name).isNotNull() & col(col2_name).isNull()) |
            ((col(col1_name) != col(col2_name)) & col(col1_name).isNotNull() & col(col2_name).isNotNull())
        ).select(
            col("composite_key"),
            lit(column).alias("column_name"),
            col(col1_name).cast("string").alias(f"{name1}_value"),
            col(col2_name).cast("string").alias(f"{name2}_value")
        )

        all_diffs.append(col_diff)

    # Union all differences if any were found
    if all_diffs:
        diff_data = all_diffs[0]
        for diff in all_diffs[1:]:
            diff_data = diff_data.union(diff)
        diff_count = diff_data.count()
    else:
        diff_data = spark_session.createDataFrame([], diff_schema)
        diff_count = 0

    print(f"Found {diff_count} differences")
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
    only_in_df1_path = f"{result_dir}/only_in_{name1}.csv"
    only_in_df1.write.mode("overwrite").option("header", "true").csv(only_in_df1_path)

    # Write records only in second source
    logger.info(f"Writing records only in {name2}")
    only_in_df2_path = f"{result_dir}/only_in_{name2}.csv"
    only_in_df2.write.mode("overwrite").option("header", "true").csv(only_in_df2_path)

    # Write detailed differences
    logger.info("Writing detailed differences")
    diff_data_path = f"{result_dir}/detailed_differences.csv"
    diff_data.write.mode("overwrite").option("header", "true").csv(diff_data_path)

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


def generate_html_report(spark, result_dir, df1_count, df2_count, only_in_df1_count, only_in_df2_count,
                         diff_count, column_diff_summary, name1, name2):
    """Generate a comprehensive HTML report."""
    # Convert the summary to pandas for easier reporting
    summary_pdf = column_diff_summary.toPandas()

    # Calculate matching records
    matching_count = df1_count - only_in_df1_count - diff_count
    match_percent = (matching_count / df1_count * 100) if df1_count > 0 else 0

    # Create HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Comparison Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                line-height: 1.6;
            }}
            h1, h2 {{
                color: #333;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            .summary-box {{
                background-color: #f8f8f8;
                border: 1px solid #ddd;
                padding: 15px;
                border-radius: 5px;
                margin-bottom: 20px;
            }}
            .file-list {{
                background-color: #f0f7ff;
                padding: 10px 15px;
                border-radius: 3px;
            }}
            .match-info {{
                font-weight: bold;
                color: {('#4CAF50' if match_percent > 90 else '#FFC107' if match_percent > 70 else '#F44336')};
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Data Comparison Report</h1>
            <p>Comparison between <strong>{name1}</strong> and <strong>{name2}</strong></p>

            <div class="summary-box">
                <h2>Record Count Summary</h2>
                <table>
                    <tr>
                        <th>Metric</th>
                        <th>Count</th>
                        <th>Percentage</th>
                    </tr>
                    <tr>
                        <td>Total records in {name1}</td>
                        <td>{df1_count:,}</td>
                        <td>100%</td>
                    </tr>
                    <tr>
                        <td>Total records in {name2}</td>
                        <td>{df2_count:,}</td>
                        <td>{(df2_count / df1_count * 100):.2f}% of {name1}</td>
                    </tr>
                    <tr>
                        <td>Records only in {name1}</td>
                        <td>{only_in_df1_count:,}</td>
                        <td>{(only_in_df1_count / df1_count * 100):.2f}%</td>
                    </tr>
                    <tr>
                        <td>Records only in {name2}</td>
                        <td>{only_in_df2_count:,}</td>
                        <td>{(only_in_df2_count / df2_count * 100 if df2_count > 0 else 0):.2f}%</td>
                    </tr>
                    <tr>
                        <td>Records with differences</td>
                        <td>{diff_count:,}</td>
                        <td>{(diff_count / df1_count * 100):.2f}%</td>
                        <td>{(diff_count/df1_count*100):.2f}%</td>
                    </tr>
                    <tr>
                        <td>Completely matching records</td>
                        <td>{matching_count:,}</td>
                        <td class="match-info">{match_percent:.2f}%</td>
                    </tr>
                </table>
            </div>
            
            <h2>Column Difference Summary</h2>
    """

    # Add column difference table
    if len(summary_pdf) > 0 and "No differences found" not in summary_pdf["column_name"].values:
        html_content += """
            <table>
                <tr>
                    <th>Column Name</th>
                    <th>Difference Count</th>
                    <th>Percentage</th>
                </tr>
        """

        for _, row in summary_pdf.iterrows():
            html_content += f"""
                <tr>
                    <td>{row['column_name']}</td>
                    <td>{row['diff_count']:,}</td>
                    <td>{row['percentage']:.2f}%</td>
                </tr>
            """

        html_content += """
            </table>
        """
    else:
        html_content += """
            <p>No column differences found in matching records.</p>
        """

    # Add output files section
    html_content += f"""
            <h2>Output Files</h2>
            <div class="file-list">
                <p>Records only in {name1}: <code>only_in_{name1}.csv</code></p>
                <p>Records only in {name2}: <code>only_in_{name2}.csv</code></p>
                <p>Detailed differences: <code>detailed_differences.csv</code></p>
                <p>Column difference summary: <code>column_diff_summary.csv</code></p>
            </div>
            
            <p>Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </body>
    </html>
    """

    # Save the HTML report
    html_df = spark.createDataFrame([(html_content,)], ["html"])

    # Write as a single file
    html_df.coalesce(1).write.mode("overwrite").text(f"{result_dir}/comparison_report.html")

    logger.info("HTML Report generated successfully")

    return html_content


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
        f"Records only in {name1}: only_in_{name1}.csv",
        f"Records only in {name2}: only_in_{name2}.csv",
        "Detailed differences: detailed_differences.csv",
        "Column difference summary: column_diff_summary.csv"
    ])

    # Convert report to DataFrame for saving to S3
    report_df = spark.createDataFrame([(line,) for line in report_lines], ["line"])
    report_df.coalesce(1).write.mode("overwrite").text(f"{result_dir}/comparison_report.txt")

    # Generate HTML report
    generate_html_report(
        spark, result_dir, df1_count, df2_count,
        only_in_df1_count, only_in_df2_count, diff_count,
        column_diff_summary, name1, name2
    )

    logger.info("Reports generated successfully")


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
