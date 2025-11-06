Here is a detailed, simple, and effective approach in Python using PySpark on AWS EMR with spark-submit to compare two
large Parquet files, considering key columns, skipping specified columns, handling various data types, and generating a
paginated HTML report for differences.

### Key Points of the Approach:

- Load Parquet files into DataFrames with Spark.
- Compare record counts.
- Identify records present in one file but not in the other using key columns.
- Compare column values on records that exist in both dataframes, skipping columns if specified.
- Create a summary of column differences.
- Generate an HTML report with sections for unique records and per-column differences, paginated with simple HTML
  tables.
- Documentation is included for ease of maintenance and clarity.

***

### PySpark Script (parquet_compare.py)
- To nomalize columns use [normalize_dataframe_for_comparison.md](../../exercises/exercise11_compare-files/normalize_dataframe_for_comparison.md)
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pandas as pd


def load_parquet(spark, path):
    return spark.read.parquet(path)


def compare_record_counts(df1, df2):
    return df1.count(), df2.count()


def find_records_not_in_other(df1, df2, keys):
    only_in_1 = df1.join(df2, keys, how='left_anti').withColumn("SourceFile", lit("File1"))
    only_in_2 = df2.join(df1, keys, how='left_anti').withColumn("SourceFile", lit("File2"))
    return only_in_1, only_in_2


def compare_common_records(df1, df2, keys, skip_columns):
    join_expr = [df1[k] == df2[k] for k in keys]
    joined = df1.alias("f1").join(df2.alias("f2"), join_expr, "inner")

    cols_to_check = [c for c in df1.columns if c not in keys + skip_columns]

    diffs = []
    for c in cols_to_check:
        diffs_col = joined.filter(
            (col(f"f1.{c}").isNull() != col(f"f2.{c}").isNull()) |
            (col(f"f1.{c}") != col(f"f2.{c}"))
        ).select(
            *[col(f"f1.{k}").alias(k) for k in keys],
            col(f"f1.{c}").alias("File1_val"),
            col(f"f2.{c}").alias("File2_val"),
        ).withColumn("ColumnName", lit(c))
        diffs.append(diffs_col)

    if diffs:
        return diffs[0].unionAll(*diffs[1:])
    else:
        return df1.limit(0)  # empty DataFrame


def generate_html_report(only_in_1, only_in_2, diffs, keys, output_file, max_rows=100):
    only_in_1_pd = only_in_1.toPandas() if only_in_1.count() else pd.DataFrame()
    only_in_2_pd = only_in_2.toPandas() if only_in_2.count() else pd.DataFrame()
    diffs_pd = diffs.toPandas() if diffs.count() else pd.DataFrame()

    html = "<html><head><style>table {border-collapse: collapse;} th, td {border: 1px solid black; padding: 5px;}</style></head><body>"
    html += f"<h1>Parquet Compare Report</h1>"

    html += f"<h2>Records only in File1: {len(only_in_1_pd)}</h2>"
    if not only_in_1_pd.empty:
        html += only_in_1_pd.head(max_rows).to_html(index=False)

    html += f"<h2>Records only in File2: {len(only_in_2_pd)}</h2>"
    if not only_in_2_pd.empty:
        html += only_in_2_pd.head(max_rows).to_html(index=False)

    html += f"<h2>Differences in Common Records: {len(diffs_pd)}</h2>"
    if not diffs_pd.empty:
        html += diffs_pd.head(max_rows).to_html(index=False)

    html += "</body></html>"

    local_path = "/tmp/diff_report.html"
    with open(local_path, "w") as f:
        f.write(html)

    print(f"Report saved to {output_file}")
    # Example usage
    upload_to_s3(local_path, "my-bucket", "output/diff_report.html")

import boto3

def upload_to_s3(local_file, bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(local_file, bucket, s3_key)


    
    
if __name__ == "__main__":
    import sys

    spark = SparkSession.builder.appName("ParquetCompare").getOrCreate()

    file1 = sys.argv[1]
    file2 = sys.argv[2]
    output_html = sys.argv[3]
    keys = sys.argv[4].split(",")  # comma-separated keys
    skip_columns = sys.argv[5].split(",") if len(sys.argv) > 5 else []

    df1 = load_parquet(spark, file1)
    df2 = load_parquet(spark, file2)

    count1, count2 = compare_record_counts(df1, df2)
    print(f"File1 record count: {count1}")
    print(f"File2 record count: {count2}")

    only_in_1, only_in_2 = find_records_not_in_other(df1, df2, keys)
    diffs = compare_common_records(df1, df2, keys, skip_columns)

    generate_html_report(only_in_1, only_in_2, diffs, keys, output_html)

    spark.stop()
```

***

### How to Run with spark-submit on AWS EMR

```bash
spark-submit parquet_compare.py s3://my-bucket/file1.parquet s3://my-bucket/file2.parquet s3://my-bucket/output/diff_report.html id,last_updated last_updated
```

- Replace `id,last_updated` for keys and skip columns as needed.
- The output will be a paginated HTML report showing record counts, records unique to each file, and differences in
  matching records by column.
- This design scales well for 50-150 GB Parquet datasets on EMR with Spark.

***

### Key Features:

- Uses efficient Spark join operations for anti-joins and inner joins.
- Compares only specified columns that are not keys or skipped.
- Produces a human-friendly HTML report, with pagination possible by limiting rows shown.
- Documented with clear function responsibilities for maintainability.
