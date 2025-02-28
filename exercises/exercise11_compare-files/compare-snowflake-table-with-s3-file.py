from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, md5, concat_ws, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import logging
import configparser
import os
import sys
import time
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

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
    parser = argparse.ArgumentParser(description='Compare file (DAT/CSV/Parquet) with Snowflake table using PySpark')
    parser.add_argument('--config', type=str, default='config.ini', help='Path to config file')
    parser.add_argument('--file-path', type=str, required=True, help='Path to input file (DAT/CSV/Parquet)')
    parser.add_argument('--file-type', type=str, choices=['dat', 'csv', 'parquet'], default='dat',
                        help='Format of the input file')
    parser.add_argument('--delimiter', type=str, default='|', help='Delimiter used in DAT/CSV file')
    parser.add_argument('--key-columns', type=str, required=True,
                        help='Comma-separated list of column names to use as keys for comparison')
    parser.add_argument('--output-dir', type=str, default='comparison_results',
                        help='Directory to store comparison results')
    parser.add_argument('--download-snowflake', action='store_true',
                        help='Download Snowflake data to file instead of direct comparison')
    parser.add_argument('--download-format', type=str, choices=['csv', 'parquet'], default='parquet',
                        help='Format to use when downloading Snowflake data')
    parser.add_argument('--temp-dir', type=str, default='/tmp/snowflake_data',
                        help='Temporary directory to store downloaded Snowflake data')
    return parser.parse_args()


def get_snowflake_connection(config_file):
    """Create a connection to Snowflake using configuration file."""
    try:
        config = configparser.ConfigParser()
        config.read(config_file)

        conn = snowflake.connector.connect(
            user=config['snowflake']['user'],
            password=config['snowflake']['password'],
            account=config['snowflake']['account'],
            warehouse=config['snowflake']['warehouse'],
            database=config['snowflake']['database'],
            schema=config['snowflake']['schema'],
            role=config['snowflake']['role']
        )
        return conn, config['snowflake']['table_name']
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def get_snowflake_schema(conn, table_name):
    """Get column names and data types from Snowflake table."""
    try:
        cursor = conn.cursor()

        # Get table schema information using INFORMATION_SCHEMA
        query = f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_NAME = '{table_name.split('.')[-1]}'
        ORDER BY 
            ORDINAL_POSITION
        """

        cursor.execute(query)
        columns_info = cursor.fetchall()

        # Create a list of column names
        columns = [col[0].upper() for col in columns_info]

        # Create a dictionary of data types
        column_types = {}
        for col in columns_info:
            col_name = col[0].upper()
            data_type = col[1]
            column_types[col_name] = data_type

        cursor.close()
        return columns, column_types
    except Exception as e:
        logger.error(f"Failed to get schema from Snowflake table: {e}")
        # Fall back to basic column retrieval if INFORMATION_SCHEMA query fails
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            columns = [desc[0].upper() for desc in cursor.description]

            # Create basic types
            column_types = {col: "VARCHAR" for col in columns}

            cursor.close()
            logger.warning("Using basic column types due to error with INFORMATION_SCHEMA")
            return columns, column_types
        except Exception as e2:
            logger.error(f"Failed in fallback schema retrieval: {e2}")
            raise


def create_output_directory(output_dir):
    """Create output directory if it doesn't exist."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    result_dir = os.path.join(output_dir, f"comparison_{timestamp}")
    os.makedirs(result_dir)

    return result_dir


def download_snowflake_to_file(spark, conn, table_name, columns, output_format, output_path):
    """Download data from Snowflake to a file."""
    logger.info(f"Downloading Snowflake table {table_name} to {output_format} file")

    # Create temp directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    # Use Spark to read from Snowflake
    # Set up Snowflake connection properties
    sf_options = {
        "sfURL": f"{conn.account}.snowflakecomputing.com",
        "sfUser": conn.user,
        "sfPassword": conn.password,
        "sfDatabase": conn.database,
        "sfSchema": conn.schema,
        "sfWarehouse": conn.warehouse,
        "sfRole": conn.role,
        "dbtable": table_name
    }

    # Read from Snowflake
    logger.info("Reading data from Snowflake using Spark")
    sf_df = spark.read.format("snowflake").options(**sf_options).load()

    # Write to specified format
    output_file = os.path.join(output_path, f"snowflake_data.{output_format}")
    logger.info(f"Writing Snowflake data to {output_file}")

    if output_format == 'parquet':
        sf_df.write.mode("overwrite").parquet(output_file)
    else:  # csv
        sf_df.write.mode("overwrite").option("header", "false").option("delimiter", "|").csv(output_file)

    logger.info(f"Snowflake data downloaded to {output_file}")
    return output_file


def read_file_to_spark(spark, file_path, file_type, delimiter, schema_columns):
    """Read input file into Spark DataFrame."""
    logger.info(f"Reading {file_type} file: {file_path}")

    if file_type == 'parquet':
        df = spark.read.parquet(file_path)
    else:  # csv or dat
        df = spark.read.option("delimiter", delimiter) \
            .option("header", "false") \
            .csv(file_path)

        # Rename columns
        for i, col_name in enumerate(schema_columns):
            df = df.withColumnRenamed(f"_c{i}", col_name)

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

    # Get the list of columns to compare (excluding key columns if desired)
    all_columns = df1.columns
    # For efficiency, you might want to include key columns in the comparison
    compare_columns = all_columns

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

    for col_name in compare_columns:
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
            for col_name in compare_columns:
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
    only_in_df1_path = os.path.join(result_dir, f"only_in_{name1}.parquet")
    only_in_df1.write.mode("overwrite").parquet(only_in_df1_path)

    # Write records only in second source
    logger.info(f"Writing records only in {name2}")
    only_in_df2_path = os.path.join(result_dir, f"only_in_{name2}.parquet")
    only_in_df2.write.mode("overwrite").parquet(only_in_df2_path)

    # Write detailed differences
    logger.info("Writing detailed differences")
    diff_data_path = os.path.join(result_dir, "detailed_differences.parquet")
    diff_data.write.mode("overwrite").parquet(diff_data_path)

    # Also save as CSV for easier viewing
    diff_data.write.mode("overwrite").option("header", "true").csv(
        os.path.join(result_dir, "detailed_differences.csv")
    )

    # Write column difference summary
    logger.info("Writing column difference summary")
    summary_path = os.path.join(result_dir, "column_diff_summary.csv")
    column_diff_summary.write.mode("overwrite").option("header", "true").csv(summary_path)


def generate_report(result_dir, df1_count, df2_count, only_in_df1_count, only_in_df2_count,
                    diff_count, column_diff_summary, name1, name2):
    """Generate a comprehensive report."""
    # Convert the summary to pandas for easier reporting
    summary_pdf = column_diff_summary.toPandas()

    with open(os.path.join(result_dir, "comparison_report.txt"), "w") as f:
        f.write("DATA COMPARISON REPORT\n")
        f.write("=====================\n\n")

        f.write(f"Comparison between {name1} and {name2}\n\n")

        f.write("RECORD COUNT SUMMARY\n")
        f.write("-------------------\n")
        f.write(f"Total records in {name1}: {df1_count}\n")
        f.write(f"Total records in {name2}: {df2_count}\n")
        f.write(f"Records only in {name1}: {only_in_df1_count}\n")
        f.write(f"Records only in {name2}: {only_in_df2_count}\n")
        f.write(f"Records with differences: {diff_count}\n")

        matching_count = df1_count - only_in_df1_count - diff_count
        match_percent = (matching_count / df1_count * 100) if df1_count > 0 else 0

        f.write(f"Completely matching records: {matching_count} ({match_percent:.2f}%)\n\n")

        f.write("COLUMN DIFFERENCE SUMMARY\n")
        f.write("-----------------------\n")
        if len(summary_pdf) > 0 and "No differences found" not in summary_pdf["column_name"].values:
            f.write(f"{'Column Name':<30} {'Difference Count':<15} {'Percentage':<10}\n")
            f.write(f"{'-' * 30} {'-' * 15} {'-' * 10}\n")

            for _, row in summary_pdf.iterrows():
                f.write(f"{row['column_name']:<30} {row['diff_count']:<15} {row['percentage']:.2f}%\n")
        else:
            f.write("No column differences found in matching records.\n")

        f.write("\nOUTPUT FILES\n")
        f.write("-----------\n")
        f.write(f"Records only in {name1}: only_in_{name1}.parquet\n")
        f.write(f"Records only in {name2}: only_in_{name2}.parquet\n")
        f.write("Detailed differences: detailed_differences.parquet, detailed_differences.csv\n")
        f.write("Column difference summary: column_diff_summary.csv\n")


def main():
    # Parse arguments
    args = parse_args()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Snowflake File Comparison") \
        .config("spark.sql.broadcastTimeout", "3600") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    try:
        # Get Snowflake connection
        conn, table_name = get_snowflake_connection(args.config)

        # Create output directory
        result_dir = create_output_directory(args.output_dir)

        # Get schema information from Snowflake
        schema_columns, column_types = get_snowflake_schema(conn, table_name)
        logger.info(f"Found {len(schema_columns)} columns in Snowflake table")

        # Parse key columns
        key_columns = [col.upper() for col in args.key_columns.split(',')]

        # Make sure key columns exist in schema
        for key_col in key_columns:
            if key_col not in schema_columns:
                raise ValueError(f"Key column '{key_col}' not found in Snowflake schema")

        # Download Snowflake data to file if requested
        if args.download_snowflake:
            snowflake_file = download_snowflake_to_file(
                spark, conn, table_name, schema_columns,
                args.download_format, args.temp_dir
            )
            # Read Snowflake data from downloaded file
            sf_df = read_file_to_spark(
                spark, snowflake_file, args.download_format,
                args.delimiter, schema_columns
            )
        else:
            # Read directly from Snowflake
            sf_options = {
                "sfURL": f"{conn.account}.snowflakecomputing.com",
                "sfUser": conn.user,
                "sfPassword": conn.password,
                "sfDatabase": conn.database,
                "sfSchema": conn.schema,
                "sfWarehouse": conn.warehouse,
                "sfRole": conn.role,
                "dbtable": table_name
            }
            sf_df = spark.read.format("snowflake").options(**sf_options).load()

        # Read the input file
        file_df = read_file_to_spark(
            spark, args.file_path, args.file_type,
            args.delimiter, schema_columns
        )

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
            result_dir, file_count, sf_count,
            only_in_file_count, only_in_sf_count, diff_count,
            column_diff_summary, "File", "Snowflake"
        )

        logger.info(f"Comparison completed. Results saved to {result_dir}")

    except Exception as e:
        logger.error(f"Error during comparison: {e}", exc_info=True)
        raise
    finally:
        if 'conn' in locals():
            conn.close()
        spark.stop()


if __name__ == "__main__":
    main()
