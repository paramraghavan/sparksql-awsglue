from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import re
from typing import Dict, List


def check_dataframe_quality(df: DataFrame) -> Dict:
    """
    Comprehensive data quality check for PySpark DataFrame before Parquet write.
    Returns a dictionary with all detected issues.
    """
    issues = {
        'column_name_issues': [],
        'data_type_issues': [],
        'null_issues': [],
        'value_issues': [],
        'schema_issues': []
    }

    # 1. Check column names
    for field in df.schema.fields:
        col_name = field.name

        # Check for spaces
        if ' ' in col_name:
            issues['column_name_issues'].append(f"Column '{col_name}' contains spaces")

        # Check for dots (problematic in Parquet)
        if '.' in col_name:
            issues['column_name_issues'].append(f"Column '{col_name}' contains dots")

        # Check for special characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col_name):
            issues['column_name_issues'].append(f"Column '{col_name}' has special characters")

        # Check for empty column names
        if col_name.strip() == '':
            issues['column_name_issues'].append("Found empty column name")

    # Check for duplicate column names
    col_names = [field.name for field in df.schema.fields]
    if len(col_names) != len(set(col_names)):
        duplicates = [name for name in col_names if col_names.count(name) > 1]
        issues['column_name_issues'].append(f"Duplicate columns: {list(set(duplicates))}")

    # 2. Check null counts
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                             for c in df.columns]).collect()[0].asDict()

    total_rows = df.count()
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            pct = (null_count / total_rows) * 100 if total_rows > 0 else 0
            issues['null_issues'].append(
                f"Column '{col_name}': {null_count} nulls ({pct:.2f}%)"
            )

    # 3. Check for problematic data types and values
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType

        # Check for NaN in numeric columns
        if isinstance(col_type, (DoubleType, FloatType)):
            nan_count = df.filter(F.isnan(col_name)).count()
            if nan_count > 0:
                issues['value_issues'].append(
                    f"Column '{col_name}' has {nan_count} NaN values"
                )

        # Check for null strings in string columns
        if isinstance(col_type, StringType):
            null_string_count = df.filter(
                F.col(col_name).isin(['NaN', 'nan', 'NULL', 'null', 'None', ''])
            ).count()
            if null_string_count > 0:
                issues['value_issues'].append(
                    f"Column '{col_name}' has {null_string_count} string representations of null"
                )

            # Check for very large strings
            max_length = df.select(F.max(F.length(col_name)).alias('max_len')).collect()[0]['max_len']
            if max_length and max_length > 1000000:
                issues['value_issues'].append(
                    f"Column '{col_name}' has very large strings (max: {max_length} chars)"
                )

    # 4. Schema validation
    if df.count() == 0:
        issues['schema_issues'].append("DataFrame is empty (0 rows)")

    if len(df.columns) == 0:
        issues['schema_issues'].append("DataFrame has no columns")

    # Check for nested columns that might cause issues
    for field in df.schema.fields:
        if isinstance(field.dataType, (ArrayType, MapType)):
            issues['schema_issues'].append(
                f"Column '{field.name}' has complex type: {field.dataType}"
            )

    # Print summary
    print("=" * 70)
    print("PYSPARK DATA QUALITY CHECK REPORT")
    print("=" * 70)
    print(f"Total Rows: {total_rows:,}")
    print(f"Total Columns: {len(df.columns)}")

    total_issues = sum(len(v) for v in issues.values())
    print(f"\nTotal Issues Found: {total_issues}\n")

    for category, issue_list in issues.items():
        if issue_list:
            print(f"\n{category.upper().replace('_', ' ')}:")
            for issue in issue_list:
                print(f"  ❌ {issue}")

    if total_issues == 0:
        print("\n✅ No data quality issues detected!")

    print("\n" + "=" * 70)

    return issues


def clean_dataframe_for_parquet(df: DataFrame,
                                fix_column_names: bool = True,
                                handle_nulls: bool = True,
                                handle_nans: bool = True,
                                fix_string_nulls: bool = True) -> DataFrame:
    """
    Clean PySpark DataFrame to ensure it can be written to Parquet successfully.
    Returns a cleaned DataFrame.
    """
    df_clean = df

    print("Cleaning PySpark DataFrame for Parquet write...")
    print("-" * 70)

    # 1. Fix column names
    if fix_column_names:
        new_columns = []
        column_mapping = {}

        for col_name in df_clean.columns:
            # Replace spaces and dots with underscores
            new_col = col_name.replace(' ', '_').replace('.', '_')
            # Remove special characters
            new_col = re.sub(r'[^a-zA-Z0-9_]', '', new_col)
            # Ensure it starts with letter or underscore
            if new_col and not new_col[0].isalpha() and new_col[0] != '_':
                new_col = '_' + new_col
            # Handle empty column names
            if not new_col:
                new_col = f'column_{len(new_columns)}'

            new_columns.append(new_col)
            column_mapping[col_name] = new_col

        # Handle duplicate column names
        seen = {}
        final_columns = []
        for col in new_columns:
            if col in seen:
                seen[col] += 1
                final_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                final_columns.append(col)

        # Rename columns
        for old_name, new_name in zip(df_clean.columns, final_columns):
            if old_name != new_name:
                df_clean = df_clean.withColumnRenamed(old_name, new_name)

        print(f"  ✓ Fixed {len([1 for o, n in zip(df.columns, final_columns) if o != n])} column names")

    # 2. Handle NaN values in numeric columns
    if handle_nans:
        nan_cols = []
        for field in df_clean.schema.fields:
            if isinstance(field.dataType, (DoubleType, FloatType)):
                # Replace NaN with null
                df_clean = df_clean.withColumn(
                    field.name,
                    F.when(F.isnan(field.name), None).otherwise(F.col(field.name))
                )
                nan_cols.append(field.name)

        if nan_cols:
            print(f"  ✓ Handled NaN values in {len(nan_cols)} numeric columns")

    # 3. Handle string representations of null
    if fix_string_nulls:
        string_cols = []
        for field in df_clean.schema.fields:
            if isinstance(field.dataType, StringType):
                df_clean = df_clean.withColumn(
                    field.name,
                    F.when(
                        F.col(field.name).isin(['NaN', 'nan', 'NULL', 'null', 'None', '']),
                        None
                    ).otherwise(F.col(field.name))
                )
                string_cols.append(field.name)

        if string_cols:
            print(f"  ✓ Handled null strings in {len(string_cols)} string columns")

    # 4. Handle null values (optional - depends on your needs)
    if handle_nulls:
        # You can add custom null handling here
        # Example: df_clean = df_clean.na.fill({"numeric_col": 0, "string_col": "UNKNOWN"})
        print(f"  ℹ Null handling: keeping nulls as-is (can be customized)")

    print("-" * 70)
    print("✅ DataFrame cleaning complete!\n")

    return df_clean


def safe_write_parquet(df: DataFrame,
                       path: str,
                       mode: str = "overwrite",
                       partition_by: List[str] = None,
                       check_quality: bool = True,
                       auto_clean: bool = True,
                       compression: str = "snappy") -> None:
    """
    Safely write DataFrame to Parquet with quality checks and automatic cleaning.

    Parameters:
    - df: PySpark DataFrame to write
    - path: Output path for Parquet files
    - mode: Write mode ('overwrite', 'append', 'error', 'ignore')
    - partition_by: List of columns to partition by
    - check_quality: Whether to check data quality before writing
    - auto_clean: Whether to automatically clean the DataFrame
    - compression: Compression codec ('snappy', 'gzip', 'lzo', 'none')
    """
    df_to_write = df

    # Check quality
    if check_quality:
        print("\n" + "=" * 70)
        print("STEP 1: CHECKING DATA QUALITY")
        print("=" * 70)
        issues = check_dataframe_quality(df)

        total_issues = sum(len(v) for v in issues.values())

        if total_issues > 0 and not auto_clean:
            raise ValueError(
                f"Found {total_issues} data quality issues. "
                "Set auto_clean=True to automatically fix them."
            )

    # Clean if requested
    if auto_clean:
        print("\n" + "=" * 70)
        print("STEP 2: CLEANING DATAFRAME")
        print("=" * 70)
        df_to_write = clean_dataframe_for_parquet(df)

    # Write to Parquet
    print("=" * 70)
    print("STEP 3: WRITING TO PARQUET")
    print("=" * 70)
    print(f"  Path: {path}")
    print(f"  Mode: {mode}")
    print(f"  Compression: {compression}")
    if partition_by:
        print(f"  Partitions: {partition_by}")

    writer = df_to_write.write.mode(mode).option("compression", compression)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(path)

    print("  ✅ Successfully wrote Parquet file!")
    print("=" * 70)


# Example usage
if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("DataQualityChecker") \
        .getOrCreate()

    # Create a problematic DataFrame for demonstration
    data = [
        (1, "A", 1.5, "data"),
        (2, "B", float('nan'), "NaN"),
        (3, None, 3.5, "NULL"),
        (None, "D", 4.5, "value")
    ]

    sample_df = spark.createDataFrame(data, ["id", "category.name", "value with spaces", "status"])

    print("\n" + "=" * 70)
    print("EXAMPLE: CHECKING AND CLEANING DATAFRAME")
    print("=" * 70)

    # Method 1: Check and clean separately
    print("\nMETHOD 1: Manual check and clean")
    issues = check_dataframe_quality(sample_df)
    clean_df = clean_dataframe_for_parquet(sample_df)

    # Verify cleaned DataFrame
    print("\nVerifying cleaned DataFrame:")
    check_dataframe_quality(clean_df)

    # Method 2: Use safe_write_parquet (recommended)
    print("\n" + "=" * 70)
    print("METHOD 2: Safe Write (Recommended)")
    print("=" * 70)
    # safe_write_parquet(sample_df, "/tmp/output.parquet", auto_clean=True)

    print("\nTo write the cleaned DataFrame:")
    print("clean_df.write.mode('overwrite').parquet('/path/to/output.parquet')")