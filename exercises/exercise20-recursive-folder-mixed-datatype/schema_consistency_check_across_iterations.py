"""
WEa re assuming the path passed in the root folder- s3 path with s3 bucket and prefix
def function sceham_check(bucket, base_prefix)

The subfolder  are created per model iteration and these have parquest file
Our function will iterate over one sub-folder at a time
perform spark.read.parquet, read the dataframe columns - df.columns

df.columns returns a list of columns,
it will first will sort the columns in ascending order
 and then pre-append the subfolder name to the columns
and add this list to a data frame

it will loop over each sub-folder and perform the above step and add it to the dataframe

At the end will save the dataframe as a csv file

the path is s3 path and we need to use boto3 api _ s3 service to get subfolders adn loop over it
"""

import boto3
import pandas as pd
from pyspark.sql import SparkSession
from typing import List, Tuple
import logging


def schema_check(bucket: str, base_prefix: str, output_path: str = None) -> pd.DataFrame:
 """
 Reads parquet files from S3 subfolders, extracts column schemas, and returns/saves as CSV.

 Args:
     bucket (str): S3 bucket name
     base_prefix (str): Base prefix/path in S3 bucket
     output_path (str, optional): Path to save the CSV file. If None, returns DataFrame only.

 Returns:
     pd.DataFrame: DataFrame containing subfolder names and their corresponding columns
 """

 # Initialize Spark session
 spark = SparkSession.builder \
  .appName("SchemaCheck") \
  .config("spark.sql.adaptive.enabled", "true") \
  .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
  .getOrCreate()

 # Initialize boto3 S3 client
 s3_client = boto3.client('s3')

 # List to store schema information
 schema_data = []

 try:
  # Get all subfolders in the base prefix
  subfolders = get_s3_subfolders(s3_client, bucket, base_prefix)

  logging.info(f"Found {len(subfolders)} subfolders to process")

  # Process each subfolder
  for subfolder in subfolders:
   try:
    # Construct S3 path for parquet files
    s3_path = f"s3a://{bucket}/{subfolder}"

    logging.info(f"Processing subfolder: {subfolder}")

    # Read parquet files from the subfolder
    df = spark.read.parquet(s3_path)

    # Get column names and sort them
    columns = sorted(df.columns)

    # Create records with subfolder name prepended to each column
    for column in columns:
     schema_data.append({
      'subfolder': subfolder,
      'column': f"{subfolder}_{column}"
     })

    logging.info(f"Processed {len(columns)} columns from {subfolder}")

   except Exception as e:
    logging.error(f"Error processing subfolder {subfolder}: {str(e)}")
    continue

  # Create DataFrame from schema data
  result_df = pd.DataFrame(schema_data)

  # Save to CSV if output path is provided
  if output_path:
   result_df.to_csv(output_path, index=False)
   logging.info(f"Schema information saved to: {output_path}")

  return result_df

 except Exception as e:
  logging.error(f"Error in schema_check function: {str(e)}")
  raise

 finally:
  # Stop Spark session
  spark.stop()


def get_s3_subfolders(s3_client, bucket: str, base_prefix: str) -> List[str]:
 """
 Get all subfolders (prefixes) under the base prefix in S3 bucket.

 Args:
     s3_client: Boto3 S3 client
     bucket (str): S3 bucket name
     base_prefix (str): Base prefix to search under

 Returns:
     List[str]: List of subfolder prefixes
 """
 subfolders = []

 try:
  # Ensure base_prefix ends with '/' for proper prefix matching
  if not base_prefix.endswith('/'):
   base_prefix += '/'

  paginator = s3_client.get_paginator('list_objects_v2')

  # Use delimiter to get only direct subfolders
  page_iterator = paginator.paginate(
   Bucket=bucket,
   Prefix=base_prefix,
   Delimiter='/'
  )

  for page in page_iterator:
   # Get common prefixes (subfolders)
   if 'CommonPrefixes' in page:
    for prefix_info in page['CommonPrefixes']:
     subfolder = prefix_info['Prefix']
     # Remove the base prefix to get relative subfolder name
     relative_subfolder = subfolder[len(base_prefix):].rstrip('/')
     if relative_subfolder:  # Ensure it's not empty
      subfolders.append(subfolder.rstrip('/'))

  return subfolders

 except Exception as e:
  logging.error(f"Error getting S3 subfolders: {str(e)}")
  raise


def schema_check_alternative(bucket: str, base_prefix: str, output_path: str = None) -> pd.DataFrame:
 """
 Alternative implementation that creates a more structured output with one row per subfolder.

 Args:
     bucket (str): S3 bucket name
     base_prefix (str): Base prefix/path in S3 bucket
     output_path (str, optional): Path to save the CSV file

 Returns:
     pd.DataFrame: DataFrame with subfolders and their column lists
 """

 spark = SparkSession.builder \
  .appName("SchemaCheckAlternative") \
  .config("spark.sql.adaptive.enabled", "true") \
  .getOrCreate()

 s3_client = boto3.client('s3')
 schema_data = []

 try:
  subfolders = get_s3_subfolders(s3_client, bucket, base_prefix)

  for subfolder in subfolders:
   try:
    s3_path = f"s3a://{bucket}/{subfolder}"
    df = spark.read.parquet(s3_path)

    # Get sorted columns
    columns = sorted(df.columns)

    # Create prefixed column names
    prefixed_columns = [f"{subfolder.split('/')[-1]}_{col}" for col in columns]

    # Add row with subfolder and its columns
    schema_data.append({
     'subfolder': subfolder,
     'column_count': len(columns),
     'columns': ', '.join(prefixed_columns)
    })

   except Exception as e:
    logging.error(f"Error processing {subfolder}: {str(e)}")
    continue

  result_df = pd.DataFrame(schema_data)

  if output_path:
   result_df.to_csv(output_path, index=False)
   logging.info(f"Schema saved to: {output_path}")

  return result_df

 finally:
  spark.stop()


def schema_check_column_and_data_type(bucket: str, base_prefix: str, output_path: str = None) -> pd.DataFrame:
 """
 Alternative implementation that creates a more structured output with one row per subfolder.
 Includes column names with their corresponding data types.

 Args:
     bucket (str): S3 bucket name
     base_prefix (str): Base prefix/path in S3 bucket
     output_path (str, optional): Path to save the CSV file

 Returns:
     pd.DataFrame: DataFrame with subfolders, their column lists, and data types
 """

 spark = SparkSession.builder \
  .appName("SchemaCheckAlternative") \
  .config("spark.sql.adaptive.enabled", "true") \
  .getOrCreate()

 s3_client = boto3.client('s3')
 schema_data = []

 try:
  subfolders = get_s3_subfolders(s3_client, bucket, base_prefix)

  for subfolder in subfolders:
   try:
    s3_path = f"s3a://{bucket}/{subfolder}"
    df = spark.read.parquet(s3_path)

    # Get schema information (column name and data type)
    schema_info = [(field.name, str(field.dataType)) for field in df.schema.fields]

    # Sort by column name
    schema_info_sorted = sorted(schema_info, key=lambda x: x[0])

    # Create prefixed column names with data types
    subfolder_name = subfolder.split('/')[-1]
    columns_with_types = [f"{subfolder_name}_{col_name} ({data_type})"
                          for col_name, data_type in schema_info_sorted]

    # Create separate lists for just column names and just data types
    column_names = [col_name for col_name, _ in schema_info_sorted]
    data_types = [data_type for _, data_type in schema_info_sorted]
    prefixed_columns = [f"{subfolder_name}_{col}" for col in column_names]

    # Add row with subfolder and its schema information
    schema_data.append({
     'subfolder': subfolder,
     'column_count': len(column_names),
     'columns': ', '.join(prefixed_columns),
     'data_types': ', '.join(data_types),
     'columns_with_types': ', '.join(columns_with_types)
    })

   except Exception as e:
    logging.error(f"Error processing {subfolder}: {str(e)}")
    continue

  result_df = pd.DataFrame(schema_data)

  if output_path:
   result_df.to_csv(output_path, index=False)
   logging.info(f"Schema saved to: {output_path}")

  return result_df

 finally:
  spark.stop()


# Example usage
if __name__ == "__main__":
 # Configure logging
 logging.basicConfig(level=logging.INFO)

 # Example usage
 bucket_name = "your-s3-bucket"
 base_prefix = "path/to/model/iterations"
 output_csv = "schema_check_results.csv"

 try:
  # Run schema check
  df = schema_check(bucket_name, base_prefix, output_csv)
  print(f"Processed {len(df)} column entries")
  print(df.head())

 except Exception as e:
  print(f"Error: {str(e)}")

