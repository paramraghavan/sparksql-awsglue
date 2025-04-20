#!/usr/bin/env python
# Customer Segmentation Production Pipeline
# Purpose: Segment customers based on transaction history
# Author: Data Science Team
# Date: 2023-01-15

import os
import sys
from pyspark.sql import SparkSession

# Import project modules
sys.path.append("../../")  # Add project root to path
from src.utils.spark_utils import create_spark_session
from src.data.preprocessing import clean_transaction_data
from src.features.build_features import create_rfm_features
from src.models.clustering import train_kmeans_model, apply_clustering
from src.utils.model_io import save_model


def main():
    # Initialize Spark - if using spark-submit, you shouldn't create a session
    # as it will be created for you, so we can modify this part
    spark = SparkSession.builder \
        .appName("CustomerSegmentation") \
        .getOrCreate()

    # Log application start
    spark.sparkContext.setLogLevel("INFO")

    # Controls whether Hadoop creates "success marker" files (_SUCCESS files) after a job completes successfully.
    # Setting it to false tells Hadoop not to create these _SUCCESS marker files, reduces clutter
    spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    print("Starting Customer Segmentation Pipeline")

    # Load data
    input_path = "s3://my-bucket/data/raw/transactions.parquet"
    df = spark.read.parquet(input_path)
    print(f"Loaded {df.count()} transactions")

    # Preprocess data using project modules
    df_clean = clean_transaction_data(df)

    # Create RFM features
    df_features = create_rfm_features(df_clean)

    # Train segmentation model
    model = train_kmeans_model(df_features, num_clusters=5, seed=42)

    # Apply model
    df_segmented = apply_clustering(df_features, model)

    # Save results
    output_path = "s3://my-bucket/data/processed/customer_segments.parquet"
    df_segmented.write.mode("overwrite").parquet(output_path)
    print(f"Saved customer segments to {output_path}")

    # Also save model for future use
    model_path = "s3://my-bucket/models/customer_segmentation_model"
    save_model(model, model_path)
    print(f"Saved model to {model_path}")

    # Stop Spark session
    spark.stop()
    print("Customer Segmentation Pipeline completed successfully")


if __name__ == "__main__":
    main()
