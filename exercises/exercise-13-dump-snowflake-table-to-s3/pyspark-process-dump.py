from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def process_s3_data():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("S3 Data Processor") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.profile", "your-aws-profile-name") \
        .getOrCreate()

    # S3 path
    s3_bucket = "your-s3-bucket-name"
    s3_prefix = "your/prefix/path/"
    file_format = "parquet"  # or "csv"

    # Construct the full S3 path
    s3_path = f"s3a://{s3_bucket}/{s3_prefix}"

    # Read data based on format
    if file_format == "parquet":
        df = spark.read.parquet(s3_path)
    elif file_format == "csv":
        df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(s3_path)
    else:  # Handle pipe-delimited CSV
        df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", "|") \
            .csv(s3_path)

    # Print schema and count
    print("Data Schema:")
    df.printSchema()

    print(f"Total rows: {df.count()}")

    # Example transformations
    # 1. Filter data
    filtered_df = df.filter(col("some_column") > 0)

    # 2. Add new columns
    processed_df = filtered_df.withColumn("processing_date", lit("2025-03-01"))

    # 3. Group and aggregate
    summary_df = processed_df.groupBy("category_column") \
        .agg({"numeric_column": "sum", "other_column": "count"})

    # Show results
    print("Sample of processed data:")
    processed_df.show(5)

    print("Summary statistics:")
    summary_df.show()

    # Write results back to S3 if needed
    output_path = f"s3a://{s3_bucket}/{s3_prefix}processed/"

    processed_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Results written to: {output_path}")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    process_s3_data()