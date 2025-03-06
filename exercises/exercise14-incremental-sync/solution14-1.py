import os
import re
import boto3
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType

# 🔹 Initialize PySpark Session
spark = SparkSession.builder \
    .appName("Incremental DataFrame Update") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# 🔹 Function to Read Config from YAML
def read_config(config_file="config1.yaml"):
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


# Load Configuration
config = read_config()
SOURCE_TYPE = config["source"].strip().lower()  # "s3" or "local"
BASE_INPUT_PATH = config["input_path"].strip()  # Base input path
COLUMNS = config["columns"]  # List of column names
KEY_COLUMNS = config["keys"]  # Key column(s) for updates
INPUT_DELIMITER = config["input_delimiter"].strip()  # Input file delimiter

# Output Configuration
OUTPUT_TYPE = config["output"]["type"].strip().lower()  # "s3" or "local"
OUTPUT_PATH = config["output"]["path"].strip()  # Output path (file or S3 key)
OUTPUT_FORMAT = config["output"]["format"].strip().lower()  # "csv" or "parquet"
OUTPUT_DELIMITER = config["output"]["delimiter"].strip()  # Output delimiter


# 🔹 Define Schema Dynamically
def infer_schema(columns):
    return StructType([StructField(col, StringType(), True) for col in columns])


schema = infer_schema(COLUMNS)


# 🔹 Function to Get Sorted File List
def get_file_list():
    """
    Get all files in the correct ascending date order (yyyy/month/day) and then
    sort files within each date folder by timestamp.
    """
    pattern = r"_(\d{8}_\d{6})_"  # Extracts timestamp (yyyymmdd_HHmmss)

    file_list = []

    if SOURCE_TYPE == "s3":
        #  S3 File Listing
        s3_client = boto3.client("s3")
        bucket, prefix = BASE_INPUT_PATH.split("/", 1)
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if re.search(r"year=\d+/month=\d+/day=\d+/", key) and key.endswith(".DAT"):
                        file_list.append(key)

        #  Sort files first by date (year/month/day) and then by timestamp in filename
        file_list.sort(key=lambda f: (
            int(re.search(r"year=(\d+)/month=(\d+)/day=(\d+)/", f).group(1)),  # Year
            int(re.search(r"year=(\d+)/month=(\d+)/day=(\d+)/", f).group(2)),  # Month
            int(re.search(r"year=(\d+)/month=(\d+)/day=(\d+)/", f).group(3)),  # Day
            re.search(pattern, f).group(1) if re.search(pattern, f) else "0"  # Timestamp
        ))

        return [f"s3a://{bucket}/{file}" for file in file_list]

    elif SOURCE_TYPE == "local":
        #  Local File Listing
        for root, _, files in os.walk(BASE_INPUT_PATH):
            if re.search(r"year=\d+/month=\d+/day=\d+", root):
                for file in files:
                    if file.endswith(".DAT"):
                        file_list.append(os.path.join(root, file))

        #  Sort files first by date (yyyy/month/day) and then by timestamp in filename
        file_list.sort(key=lambda f: (
            int(re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", f).group(1)),  # Year
            int(re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", f).group(2)),  # Month
            int(re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", f).group(3)),  # Day
            re.search(pattern, f).group(1) if re.search(pattern, f) else "0"  # Timestamp
        ))

        return file_list

    else:
        raise ValueError("Invalid source type. Use 's3' or 'local'.")


# Get Sorted List of Files
file_list = get_file_list()

# 🔹 Initialize an Empty Base DataFrame
df_main = spark.createDataFrame([], schema=schema)

# 🔹 Process Each File Incrementally
for file_path in file_list:
    try:
        #  Read New File into DataFrame with Custom Input Delimiter
        df_new = spark.read.option("header", True).option("delimiter", INPUT_DELIMITER).schema(schema).csv(file_path)

        #  Identify Existing Records (Same Key)
        df_existing = df_main.join(df_new, KEY_COLUMNS, "inner").select(df_new["*"])

        #  Identify New Records (Not in df_main)
        df_new_records = df_new.join(df_main, KEY_COLUMNS, "left_anti")

        #  Merge Updated and New Records
        df_main = df_existing.union(df_new_records)

        print(f"Processed file: {file_path}")

    except AnalysisException as e:
        print(f"Error reading file {file_path}: {str(e)}")

# 🔹 Write Final DataFrame to Local or S3 with Configurable Output Delimiter
if OUTPUT_TYPE == "s3":
    output_s3_path = f"s3a://{OUTPUT_PATH}"
    df_main.write.option("header", True).option("delimiter", OUTPUT_DELIMITER).mode("overwrite").csv(output_s3_path)
    print(f"Final result written to S3: {OUTPUT_PATH}")

elif OUTPUT_TYPE == "local":
    df_main.write.option("header", True).option("delimiter", OUTPUT_DELIMITER).mode("overwrite").csv(OUTPUT_PATH)
    print(f"Final result written locally: {OUTPUT_PATH}")

else:
    raise ValueError("Invalid output type. Use 's3' or 'local'.")

# 🔹 Show Final DataFrame After Processing
df_main.show()
