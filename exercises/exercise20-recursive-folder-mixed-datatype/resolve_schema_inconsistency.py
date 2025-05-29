"""
Forces all columns to be read as strings:
"""
from pyspark.sql.types import *
from pyspark.sql.functions import *

# First, get the column names from any non-empty parquet file
sample_df = spark.read.parquet("model_out/iter_2")  # or any non-empty iteration
column_names = sample_df.columns

# Create a schema with all columns as StringType
string_schema = StructType([
    StructField(col_name, StringType(), True) for col_name in column_names
])

# Read with the string schema
df = spark.read.schema(string_schema) \
    .option("recursiveFileLookup", "true") \
    .parquet("model_out/")
###############################################################################

"""
Cast numeric data type to a common type like DoubleType
"""
# Read one non-empty file to get column names
sample_df = spark.read.parquet("model_out/iter_2")
columns = sample_df.columns

# Read with mergeSchema but then cast numeric columns
df = spark.read.option("recursiveFileLookup", "true") \
    .option("mergeSchema", "true") \
    .parquet("model_out/")

# Cast all numeric-looking columns to double
for col_name in columns:
    col_type = dict(df.dtypes)[col_name]
    if col_type in ['int', 'bigint', 'long', 'float', 'double']:
        df = df.withColumn(col_name, col(col_name).cast("double"))

###############################################################################

"""
Handle schema conflicts manually and iter_1 is typically empty, you can skip it.
"""

import os
from pyspark.sql.functions import *


def read_parquet_with_consistent_schema(base_path):
    # Get all iteration folders
    iter_folders = [f for f in os.listdir(base_path) if f.startswith('iter_')]

    dataframes = []
    target_schema = None

    for folder in iter_folders:
        folder_path = f"{base_path}/{folder}"
        try:
            # Try to read the folder
            temp_df = spark.read.parquet(folder_path)

            # Skip if empty
            if temp_df.count() == 0:
                continue

            # Set target schema from first non-empty dataset
            if target_schema is None:
                target_schema = temp_df.schema
                dataframes.append(temp_df)
            else:
                # Cast columns to match target schema
                aligned_df = align_schema(temp_df, target_schema)
                dataframes.append(aligned_df)

        except Exception as e:
            print(f"Skipping {folder}: {e}")
            continue

    # Union all dataframes
    if dataframes:
        return dataframes[0].unionByName(*dataframes[1:], allowMissingColumns=True)
    else:
        return spark.createDataFrame([], schema=target_schema)


def align_schema(df, target_schema):
    """Convert DataFrame columns to match target schema types"""
    for field in target_schema.fields:
        col_name = field.name
        target_type = field.dataType

        if col_name in df.columns:
            current_type = dict(df.dtypes)[col_name]
            if current_type != str(target_type):
                df = df.withColumn(col_name, col(col_name).cast(target_type))

    return df


# Usage
df = read_parquet_with_consistent_schema("model_out")
###############################################################################
