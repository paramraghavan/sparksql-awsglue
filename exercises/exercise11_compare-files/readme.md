A PySpark solution for comparing a large 25GB file with a Snowflake table using AWS EMR. This approach will be highly
scalable and efficient for such large datasets.

A comprehensive solution for comparing your 25GB file with a Snowflake table using PySpark on AWS EMR.
This approach is highly scalable and efficient for large datasets.

## Solution Overview

The solution performs:

1. Row-level comparison to identify missing/extra records
2. Column-level comparison to identify value and data type differences
3. Detailed reporting with statistics and specific differences

## Key Features

1. **Full Scalability with Spark**:
    - Uses PySpark's distributed processing capabilities
    - Configures EMR cluster for optimal performance with large datasets
    - Automatically partitions data for efficient processing

2. **Automated Schema Handling**:
    - Extracts column names and data types directly from Snowflake
    - Applies them to the headerless file automatically
    - Ensures accurate comparison with correct data types

3. **Comprehensive Comparison Logic**:
    - Identifies records missing from either source
    - Pinpoints exact column differences between matching records
    - Handles null values properly in comparisons

4. **Detailed Reporting**:
    - Generates statistics of matching and non-matching records
    - Produces column-level reports showing which columns have the most differences
    - Creates both machine-readable (Parquet) and human-readable (CSV) outputs

## How to Use

1. **Set up the configuration files**:
    - Use the provided `config.ini` template for Snowflake credentials
    - Update the S3 paths in the launch script to your buckets

2. **Launch the EMR cluster**:
    - Run the `launch-emr.sh` script to create the EMR cluster with all necessary configurations
    - The script uploads your code to S3 and configures the cluster to run the comparison job

3. **Monitor and retrieve results**:
    - The comparison results will be saved to your specified S3 output directory
    - Results include both Parquet files (for further analysis) and CSV/text files (for easy viewing)

## Understanding the Results

The comparison produces several output files:

1. **comparison_report.txt** - A comprehensive summary showing:
    - Total record counts in both sources
    - Number of records found only in each source
    - Number of records with differences
    - Percentage of completely matching records
    - Summary of which columns have the most differences

2. **only_in_File.parquet** - Records present in your file but missing from Snowflake

3. **only_in_Snowflake.parquet** - Records present in Snowflake but missing from your file

4. **detailed_differences.csv** - Row-by-column breakdown of exactly what differs:
    - The key columns to identify the record
    - The column name where the difference occurs
    - The value from each source

5. **column_diff_summary.csv** - Analysis of which columns have the most differences:
    - Column names sorted by number of differences
    - Count and percentage of differences for each column

## Performance Considerations

The script is optimized for large datasets by:

- Configuring appropriate memory settings for Spark
- Using efficient join algorithms for the comparisons
- Executing SQL operations directly in Snowflake when possible
- First identifying records with differences, then drilling down to specific columns

This approach should handle your 25GB dataset efficiently on a properly sized EMR cluster.
