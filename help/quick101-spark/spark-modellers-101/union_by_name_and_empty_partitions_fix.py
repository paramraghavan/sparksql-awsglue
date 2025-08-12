from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit
from functools import reduce

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("UnionByName with Empty Partition Fix") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Sample schema for demonstration
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", IntegerType(), True)
])


# Create sample DataFrames (some might be empty)
def create_sample_dataframes():
    """Create multiple DataFrames including some empty ones"""

    # DataFrame 1 - Non-empty
    data1 = [(1, "Alice", "A", 100), (2, "Bob", "B", 200)]
    df1 = spark.createDataFrame(data1, schema)

    # DataFrame 2 - Non-empty
    data2 = [(3, "Charlie", "A", 150), (4, "David", "C", 300)]
    df2 = spark.createDataFrame(data2, schema)

    # DataFrame 3 - Empty DataFrame with same schema
    df3 = spark.createDataFrame([], schema)

    # DataFrame 4 - Non-empty
    data4 = [(5, "Eve", "B", 250)]
    df4 = spark.createDataFrame(data4, schema)

    # DataFrame 5 - Another empty DataFrame
    df5 = spark.createDataFrame([], schema)

    return [df1, df2, df3, df4, df5]


def union_dataframes_safe(dataframes, allow_missing_columns=True):
    """
    Safely union multiple DataFrames by name, handling empty DataFrames

    Args:
        dataframes: List of DataFrames to union
        allow_missing_columns: Whether to allow missing columns in unionByName

    Returns:
        Unioned DataFrame
    """
    # Filter out completely empty DataFrames
    non_empty_dfs = []

    for df in dataframes:
        # Check if DataFrame has any rows
        if df.count() > 0:
            non_empty_dfs.append(df)
        else:
            print(f"Skipping empty DataFrame with schema: {df.schema}")

    # If all DataFrames are empty, return an empty DataFrame with the first schema
    if not non_empty_dfs:
        print("All DataFrames are empty, returning empty DataFrame")
        return dataframes[0] if dataframes else spark.createDataFrame([], schema)

    # If only one non-empty DataFrame, return it
    if len(non_empty_dfs) == 1:
        return non_empty_dfs[0]

    # Union all non-empty DataFrames
    try:
        if allow_missing_columns:
            # Use unionByName with allowMissingColumns=True (Spark 3.1+)
            result_df = reduce(
                lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
                non_empty_dfs
            )
        else:
            # Standard unionByName
            result_df = reduce(
                lambda df1, df2: df1.unionByName(df2),
                non_empty_dfs
            )

        return result_df

    except Exception as e:
        print(f"Error during union: {e}")
        raise


def write_with_empty_partition_fix(df, output_path, partition_cols=None):
    """
    Write DataFrame to Parquet with fix for empty partition error

    Args:
        df: DataFrame to write
        output_path: Output path for parquet files
        partition_cols: Columns to partition by (optional)
    """

    # Check if DataFrame is empty
    if df.count() == 0:
        print("DataFrame is empty, skipping write operation")
        return

    # Method 1: Use coalesce to reduce partitions before writing
    df_coalesced = df.coalesce(1)  # Combine into single partition

    try:
        if partition_cols:
            # Check if partitioning columns have any non-null values
            partition_check = True
            for col_name in partition_cols:
                non_null_count = df_coalesced.filter(col(col_name).isNotNull()).count()
                if non_null_count == 0:
                    print(f"Warning: Partition column '{col_name}' has all null values")
                    partition_check = False

            if partition_check:
                df_coalesced.write \
                    .mode('overwrite') \
                    .option('compression', 'snappy') \
                    .partitionBy(*partition_cols) \
                    .parquet(output_path)
            else:
                # Write without partitioning if partition columns are problematic
                print("Writing without partitioning due to null partition values")
                df_coalesced.write \
                    .mode('overwrite') \
                    .option('compression', 'snappy') \
                    .parquet(output_path)
        else:
            df_coalesced.write \
                .mode('overwrite') \
                .option('compression', 'snappy') \
                .parquet(output_path)

        print(f"Successfully wrote {df.count()} records to {output_path}")

    except Exception as e:
        print(f"Error writing to parquet: {e}")

        # Alternative approach: Add a dummy partition column if needed
        if partition_cols and "empty partition" in str(e).lower():
            print("Attempting fix by adding default partition value...")
            df_with_default = df_coalesced

            for col_name in partition_cols:
                df_with_default = df_with_default.withColumn(
                    col_name,
                    col(col_name).cast(StringType()).otherwise(lit("unknown"))
                )

            df_with_default.write \
                .mode('overwrite') \
                .option('compression', 'snappy') \
                .partitionBy(*partition_cols) \
                .parquet(output_path)

            print("Successfully wrote with default partition values")


# Alternative method: Filter empty partitions before writing
def filter_empty_partitions(df, partition_cols):
    """
    Filter out rows that would create empty partitions

    Args:
        df: Input DataFrame
        partition_cols: List of partition columns

    Returns:
        Filtered DataFrame
    """
    if not partition_cols:
        return df

    # Create filter condition to exclude rows where all partition cols are null
    filter_condition = None
    for col_name in partition_cols:
        condition = col(col_name).isNotNull()
        if filter_condition is None:
            filter_condition = condition
        else:
            filter_condition = filter_condition | condition

    return df.filter(filter_condition) if filter_condition else df


def main():
    """Main execution function"""
    print("Creating sample DataFrames...")
    dataframes = create_sample_dataframes()

    # Print info about each DataFrame
    for i, df in enumerate(dataframes, 1):
        count = df.count()
        print(f"DataFrame {i}: {count} rows")
        if count > 0:
            df.show(5)

    print("\n" + "=" * 50)
    print("Performing unionByName operation...")

    # Union all DataFrames safely
    result_df = union_dataframes_safe(dataframes)

    print(f"\nUnion result: {result_df.count()} total rows")
    result_df.show()

    print("\n" + "=" * 50)
    print("Writing to Parquet with partition fix...")

    # Method 1: Write without partitioning
    output_path_1 = "/tmp/union_result_no_partition"
    write_with_empty_partition_fix(result_df, output_path_1)

    # Method 2: Write with partitioning
    output_path_2 = "/tmp/union_result_with_partition"
    write_with_empty_partition_fix(result_df, output_path_2, partition_cols=["category"])

    # Method 3: Filter empty partitions first
    filtered_df = filter_empty_partitions(result_df, ["category"])
    output_path_3 = "/tmp/union_result_filtered"
    write_with_empty_partition_fix(filtered_df, output_path_3, partition_cols=["category"])

    print("\nAll operations completed successfully!")


# Additional utility functions for common scenarios
def handle_schema_evolution(dataframes):
    """
    Handle DataFrames with slightly different schemas
    """
    if not dataframes:
        return spark.createDataFrame([], schema)

    # Get all unique column names across all DataFrames
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)

    # Add missing columns to each DataFrame
    normalized_dfs = []
    for df in dataframes:
        df_normalized = df
        for col_name in all_columns:
            if col_name not in df.columns:
                df_normalized = df_normalized.withColumn(col_name, lit(None))
        normalized_dfs.append(df_normalized)

    return union_dataframes_safe(normalized_dfs)


def create_checkpoint_safe_union(dataframes, checkpoint_dir="/tmp/spark_checkpoint"):
    """
    Create union with checkpointing to avoid deep lineage issues
    """
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    result_df = union_dataframes_safe(dataframes)

    # Checkpoint if result is large
    if result_df.count() > 10000:  # Adjust threshold as needed
        result_df = result_df.checkpoint()

    return result_df


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        spark.stop()
